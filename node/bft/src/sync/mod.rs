// Copyright (c) 2019-2025 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    Gateway,
    MAX_FETCH_TIMEOUT_IN_MS,
    PRIMARY_PING_IN_MS,
    Transport,
    helpers::{BFTSender, Pending, Storage, SyncReceiver, fmt_id, max_redundant_requests},
    spawn_blocking,
};
use snarkos_node_bft_events::{CertificateRequest, CertificateResponse, Event};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_sync::{BLOCK_REQUEST_BATCH_DELAY, BlockSync, Ping, PrepareSyncRequest, locators::BlockLocators};
use snarkvm::{
    console::{network::Network, types::Field},
    ledger::{PendingBlock, authority::Authority, block::Block, narwhal::BatchCertificate},
    prelude::{cfg_into_iter, cfg_iter},
};

use anyhow::{Result, anyhow, bail, ensure};
use indexmap::IndexMap;
#[cfg(feature = "locktick")]
use locktick::{parking_lot::Mutex, tokio::Mutex as TMutex};
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
#[cfg(not(feature = "serial"))]
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex as TMutex;
use tokio::{
    sync::{OnceCell, oneshot},
    task::JoinHandle,
};

/// Block synchronization logic for validators.
///
/// Synchronization works differently for nodes that act as validators in AleoBFT;
/// In the common case, validators generate blocks after receiving an anchor block that has been accepted
/// by a supermajority of the committee instead of fetching entire blocks from other nodes.
/// However, if a validator does not have an up-to-date DAG, it might still fetch entire blocks from other nodes.
///
/// This struct also manages fetching certificates from other validators during normal operation,
/// and blocks when falling behind.
///
/// Finally, `Sync` handles synchronization of blocks with the validator's local storage:
/// it loads blocks from the storage on startup and writes new blocks to the storage after discovering them.
#[derive(Clone)]
pub struct Sync<N: Network> {
    /// The gateway enables communication with other validators.
    gateway: Gateway<N>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The block synchronization logic.
    block_sync: Arc<BlockSync<N>>,
    /// The pending certificates queue.
    pending: Arc<Pending<Field<N>, BatchCertificate<N>>>,
    /// The BFT sender.
    bft_sender: Arc<OnceCell<BFTSender<N>>>,
    /// Handles to the spawned background tasks.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The response lock.
    response_lock: Arc<TMutex<()>>,
    /// The sync lock. Ensures that only one task syncs the ledger at a time.
    sync_lock: Arc<TMutex<()>>,
    /// The latest block responses.
    ///
    /// This is used in [`Sync::sync_storage_with_block()`] to accumulate blocks
    /// whose addition to the ledger is deferred until certain checks pass.
    /// Blocks need to be processed in order, hence a BTree map.
    ///
    /// Whenever a new block is added to this map, BlockSync::set_sync_height needs to be called.
    pending_blocks: Arc<TMutex<VecDeque<PendingBlock<N>>>>,
}

impl<N: Network> Sync<N> {
    /// Initializes a new sync instance.
    pub fn new(
        gateway: Gateway<N>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        block_sync: Arc<BlockSync<N>>,
    ) -> Self {
        // Return the sync instance.
        Self {
            gateway,
            storage,
            ledger,
            block_sync,
            pending: Default::default(),
            bft_sender: Default::default(),
            handles: Default::default(),
            response_lock: Default::default(),
            sync_lock: Default::default(),
            pending_blocks: Default::default(),
        }
    }

    /// Initializes the sync module and sync the storage with the ledger at bootup.
    pub async fn initialize(&self, bft_sender: Option<BFTSender<N>>) -> Result<()> {
        // If a BFT sender was provided, set it.
        if let Some(bft_sender) = bft_sender {
            self.bft_sender.set(bft_sender).expect("BFT sender already set in gateway");
        }

        info!("Syncing storage with the ledger...");

        // Sync the storage with the ledger.
        self.sync_storage_with_ledger_at_bootup().await?;

        debug!("Finished initial block synchronization at startup");
        Ok(())
    }

    /// Sends the given batch of block requests to peers.
    ///
    /// Responses to block requests will eventually be processed by `Self::try_advancing_block_synchronization`.
    #[inline]
    pub async fn issue_block_requests(&self) -> Result<()> {
        // First see if any peers need removal
        let peers_to_ban = self.block_sync.remove_timed_out_block_requests();
        for peer_ip in peers_to_ban {
            trace!("Banning peer {peer_ip} for timing out on block requests");

            let tcp = self.gateway.tcp().clone();
            tcp.banned_peers().update_ip_ban(peer_ip.ip());

            tokio::spawn(async move {
                tcp.disconnect(peer_ip).await;
            });
        }

        // Prepare the block requests, if any.
        // In the process, we update the state of `is_block_synced` for the sync module.
        let block_requests = self.block_sync.prepare_block_requests()?;
        trace!("Prepared {} block requests", block_requests.len());

        // Sends the block requests to the sync peers.
        for request in block_requests {
            if !self.block_sync.send_block_request(&self.gateway, request).await {
                // Stop if we fail to process a request.
                break;
            }

            // Sleep to avoid triggering spam detection.
            tokio::time::sleep(BLOCK_REQUEST_BATCH_DELAY).await;
        }

        Ok(())
    }

    /// Starts the sync module.
    ///
    /// When this function returns successfully, the sync module will have spawned background tasks
    /// that fetch blocks from other validators.
    pub async fn run(&self, ping: Option<Arc<Ping<N>>>, sync_receiver: SyncReceiver<N>) -> Result<()> {
        info!("Starting the sync module...");

        // Start the block sync loop.
        let self_ = self.clone();
        self.spawn(async move {
            // Sleep briefly to allow an initial primary ping to come in prior to entering the loop.
            // Ideally, a node does not consider itself synced when it has not received
            // any block locators from peers. However, in the initial bootup of validators,
            // this needs to happen, so we use this additional sleep as a grace period.
            tokio::time::sleep(Duration::from_millis(PRIMARY_PING_IN_MS)).await;
            loop {
                // Sleep briefly to avoid triggering spam detection.
                tokio::time::sleep(Duration::from_millis(PRIMARY_PING_IN_MS)).await;

                let new_blocks = self_.try_block_sync().await;
                if new_blocks {
                    if let Some(ping) = &ping {
                        let height = self_.ledger.latest_block_height();
                        ping.update_block_height(height);
                    }
                }
            }
        });

        // Start the pending queue expiration loop.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly.
                tokio::time::sleep(Duration::from_millis(MAX_FETCH_TIMEOUT_IN_MS)).await;

                // Remove the expired pending transmission requests.
                let self__ = self_.clone();
                let _ = spawn_blocking!({
                    self__.pending.clear_expired_callbacks();
                    Ok(())
                });
            }
        });

        /* Set up callbacks for events from the Gateway */

        // Retrieve the sync receiver.
        let SyncReceiver {
            mut rx_block_sync_advance_with_sync_blocks,
            mut rx_block_sync_remove_peer,
            mut rx_block_sync_get_block_locators,
            mut rx_block_sync_update_peer_block_height,
            mut rx_block_sync_update_peer_block_locators,
            mut rx_certificate_request,
            mut rx_certificate_response,
        } = sync_receiver;

        // Process the block sync request to advance with sync blocks.
        // Each iteration of this loop is triggered by an incoming [`BlockResponse`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`SyncSender::advance_with_sync_blocks()`],
        // which calls [`tx_block_sync_advance_with_sync_blocks.send()`],
        // which causes the `rx_block_sync_advance_with_sync_blocks.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, blocks, callback)) = rx_block_sync_advance_with_sync_blocks.recv().await {
                callback.send(self_.advance_with_sync_blocks(peer_ip, blocks).await).ok();
            }
        });

        // Fetch block locators for a specified range.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((_start, end, callback)) = rx_block_sync_get_block_locators.recv().await {
                let result = self_.get_block_locators(end);
                callback.send(result).ok();
            }
        });

        // Process the block sync request to remove the peer.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some(peer_ip) = rx_block_sync_remove_peer.recv().await {
                self_.remove_peer(peer_ip);
            }
        });

        // Process each block sync request to update peer locators.
        // Each iteration of this loop is triggered by an incoming [`PrimaryPing`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`SyncSender::update_peer_locators()`],
        // which calls [`tx_block_sync_update_peer_locators.send()`],
        // which causes the `rx_block_sync_update_peer_locators.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, block_locators, callback)) = rx_block_sync_update_peer_block_locators.recv().await
            {
                let self_ = self_.clone();
                tokio::spawn(async move {
                    callback.send(self_.update_peer_block_locators(peer_ip, block_locators)).ok();
                });
            }
        });

        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, peer_height, callback)) = rx_block_sync_update_peer_block_height.recv().await {
                let self_ = self_.clone();
                tokio::spawn(async move {
                    let result = self_.update_peer_block_height(peer_ip, peer_height).await;
                    callback.send(result).ok();
                });
            }
        });

        // Process each certificate request.
        // Each iteration of this loop is triggered by an incoming [`CertificateRequest`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`tx_certificate_request.send()`],
        // which causes the `rx_certificate_request.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, certificate_request)) = rx_certificate_request.recv().await {
                self_.send_certificate_response(peer_ip, certificate_request);
            }
        });

        // Process each certificate response.
        // Each iteration of this loop is triggered by an incoming [`CertificateResponse`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`tx_certificate_response.send()`],
        // which causes the `rx_certificate_response.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, certificate_response)) = rx_certificate_response.recv().await {
                self_.finish_certificate_request(peer_ip, certificate_response);
            }
        });

        Ok(())
    }

    /// Execute one iteration of block synchronization.
    ///
    /// Returns true if we made progress. Returning true does *not* imply being fully synced.
    ///
    /// This is called periodically by a tokio background task spawned in `Self::run`.
    /// Some unit tests also call this function directly to manually trigger block synchronization.
    pub(crate) async fn try_block_sync(&self) -> Result<()> {
        self.issue_block_requests().await?;

        // Sync the storage with the blocks.
        if let Err(err) = self.try_advancing_block_synchronization().await {
            bail!("Block synchronization failed - {err}");
        }

        /*
        // If the node is synced, clear the `latest_block_responses`.
        if self.is_synced() {
            self.latest_block_responses.lock().await.clear();
        }*/

        Ok(())
    }
}

// Callbacks used when receiving messages from the Gateway
impl<N: Network> Sync<N> {
    /// We received a block response and can (possibly) advance synchronization.
    async fn advance_with_sync_blocks(&self, peer_ip: SocketAddr, blocks: Vec<Block<N>>) -> Result<()> {
        // Verify that the response is valid and add it to block sync.
        self.block_sync.insert_block_responses(peer_ip, blocks)?;

        // Try to process responses stored in BlockSync.
        // Note: Do not call `self.block_sync.try_advancing_block_synchronziation` here as it will process
        // and remove any completed requests, which means the call to `sync_storage_with_blocks` will not process
        // them as expected.
        self.try_advancing_block_synchronization().await?;

        Ok(())
    }

    /// We received new peer locators during a Ping.
    async fn update_peer_block_height(&self, peer_ip: SocketAddr, new_advertised: u32) -> Result<()> {
        self.block_sync.update_peer_block_height(peer_ip, new_advertised).await
    }

    /// We received new peer locators during a Ping.
    fn update_peer_block_locators(&self, peer_ip: SocketAddr, block_locators: BlockLocators<N>) -> Result<()> {
        self.block_sync.update_peer_block_locators(peer_ip, block_locators)
    }

    /// A peer disconnected.
    fn remove_peer(&self, peer_ip: SocketAddr) {
        self.block_sync.remove_peer(&peer_ip);
    }

    #[cfg(test)]
    pub fn test_update_peer_locators(&self, peer_ip: SocketAddr, locators: BlockLocators<N>) -> Result<()> {
        self.update_peer_locators(peer_ip, locators)
    }
}

// Methods to manage storage.
impl<N: Network> Sync<N> {
    /// Syncs the storage with the ledger at bootup.
    ///
    /// This is called when first starting the validator.
    async fn sync_storage_with_ledger_at_bootup(&self) -> Result<()> {
        // Retrieve the latest block in the ledger.
        let latest_block = self.ledger.latest_block();

        // Retrieve the block height.
        let block_height = latest_block.height();
        // Determine the maximum number of blocks corresponding to rounds
        // that would not have been garbage collected, i.e. that would be kept in storage.
        // Since at most one block is created every two rounds,
        // this is half of the maximum number of rounds kept in storage.
        let max_gc_blocks = u32::try_from(self.storage.max_gc_rounds())?.saturating_div(2);
        // Determine the earliest height of blocks corresponding to rounds kept in storage,
        // conservatively set to the block height minus the maximum number of blocks calculated above.
        // By virtue of the BFT protocol, we can guarantee that all GC range blocks will be loaded.
        let gc_height = block_height.saturating_sub(max_gc_blocks);
        // Retrieve the blocks.
        let blocks = self.ledger.get_blocks(gc_height..block_height.saturating_add(1))?;

        // Acquire the sync lock.
        let _lock = self.sync_lock.lock().await;

        debug!("Syncing storage with the ledger from block {} to {}...", gc_height, block_height.saturating_add(1));

        /* Sync storage */

        // Sync the height with the block.
        self.storage.sync_height_with_block(latest_block.height());
        // Sync the round with the block.
        self.storage.sync_round_with_block(latest_block.round());
        // Perform GC on the latest block round.
        self.storage.garbage_collect_certificates(latest_block.round());
        // Iterate over the blocks.
        for block in &blocks {
            // If the block authority is a sub-DAG, then sync the batch certificates with the block.
            // Note that the block authority is always a sub-DAG in production;
            // beacon signatures are only used for testing,
            // and as placeholder (irrelevant) block authority in the genesis block.
            if let Authority::Quorum(subdag) = block.authority() {
                // Reconstruct the unconfirmed transactions.
                let unconfirmed_transactions = cfg_iter!(block.transactions())
                    .filter_map(|tx| {
                        tx.to_unconfirmed_transaction().map(|unconfirmed| (unconfirmed.id(), unconfirmed)).ok()
                    })
                    .collect::<HashMap<_, _>>();

                // Iterate over the certificates.
                for certificates in subdag.values().cloned() {
                    cfg_into_iter!(certificates).for_each(|certificate| {
                        self.storage.sync_certificate_with_block(block, certificate, &unconfirmed_transactions);
                    });
                }

                // Update the validator telemetry.
                #[cfg(feature = "telemetry")]
                self.gateway.validator_telemetry().insert_subdag(subdag);
            }
        }

        /* Sync the BFT DAG */

        // Construct a list of the certificates.
        let certificates = blocks
            .iter()
            .flat_map(|block| {
                match block.authority() {
                    // If the block authority is a beacon, then skip the block.
                    Authority::Beacon(_) => None,
                    // If the block authority is a subdag, then retrieve the certificates.
                    Authority::Quorum(subdag) => Some(subdag.values().flatten().cloned().collect::<Vec<_>>()),
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        // If a BFT sender was provided, send the certificates to the BFT.
        if let Some(bft_sender) = self.bft_sender.get() {
            // Await the callback to continue.
            if let Err(e) = bft_sender.tx_sync_bft_dag_at_bootup.send(certificates).await {
                bail!("Failed to update the BFT DAG from sync: {e}");
            }
        }

        self.block_sync.set_sync_height(block_height);

        Ok(())
    }

    /// Returns which height we are synchronized to.
    /// If there are queued block responses, this might be higher than the latest block in the ledger.
    async fn compute_sync_height(&self) -> u32 {
        let ledger_height = self.ledger.latest_block_height();
        let mut pending_blocks = self.pending_blocks.lock().await;

        // Remove any old responses.
        while let Some(b) = pending_blocks.front()
            && b.height() <= ledger_height
        {
            pending_blocks.pop_front();
        }

        // Ensure the returned value is always greater or equal than ledger height.
        pending_blocks.back().map(|b| b.height()).unwrap_or(0).max(ledger_height)
    }

    /// Aims to advance synchronization using any recent block responses received from peers.
    ///
    /// This is the validator's version of `BlockSync::try_advancing_block_synchronization`
    /// and is called periodically at runtime.
    ///
    /// This returns Ok(true) if we successfully advanced the ledger by at least one new block.
    ///
    /// A key difference to `BlockSync`'s versions is that it will only add blocks to the ledger once they have been confirmed by the network.
    /// If blocks are not confirmed yet, they will be kept in [`Self::pending_blocks`].
    /// It will also pass certificates from synced blocks to the BFT module so that consensus can progress as expected
    /// (see [`Self::sync_storage_with_block`] for more details).
    ///
    /// If the node falls behind more than GC rounds, this function calls [`Self::sync_storage_without_bft`] instead,
    /// which syncs without updating the BFT state.
    async fn try_advancing_block_synchronization(&self) -> Result<bool> {
        // Acquire the response lock.
        let _lock = self.response_lock.lock().await;

        // For sanity, set the sync height again.
        // (if the sync height is already larger or equal, this is a noop)
        let ledger_height = self.ledger.latest_block_height();
        self.block_sync.set_sync_height(ledger_height);

        // Retrieve the maximum block height of the peers.
        let tip = self.block_sync.find_sync_peers().map(|(x, _)| x.into_values().max().unwrap_or(0)).unwrap_or(0);

        // Updates sync state and returns the error (if any).
        let cleanup = |start_height, current_height, error| {
            let new_blocks = current_height > start_height;

            // Make the underlying `BlockSync` instance aware of the new sync height.
            if new_blocks {
                self.block_sync.set_sync_height(current_height);
            }

            if let Some(err) = error { Err(err) } else { Ok(new_blocks) }
        };

        // Retrieve the current height, based on the ledger height and the
        // (unconfirmed) blocks that are already queued up.
        let start_height = self.compute_sync_height().await;

        // For sanity, update the sync height before starting.
        // (if this is lower or equal to the current sync height, this is a noop)
        self.block_sync.set_sync_height(start_height);

        // The height is incremented as blocks are added.
        let mut current_height = start_height;
        trace!("Try advancing with block responses (at block {current_height})");

        // If we already were within GC or successfully caught up with GC, try to advance BFT normally again.
        loop {
            let next_height = current_height + 1;
            let Some(block) = self.block_sync.peek_next_block(next_height) else {
                break;
            };

            info!("Syncing the BFT to block {}...", block.height());
            // Sync the storage with the block.
            match self.sync_storage_with_block(block).await {
                Ok(_) => {
                    // Update the current height if sync succeeds.
                    current_height += 1;
                }
                Err(e) => {
                    // Mark the current height as processed in block_sync.
                    self.block_sync.remove_block_response(current_height);
                    return Err(e);
                }
            }
        }

        cleanup(start_height, current_height, None)
    }

    /// Syncs the storage with the given block.
    //
    /// This must be called *after* the block was added to the ledger.
    /// It also updates the DAG because the synced block might be within GC.
    async fn sync_storage_with_block(&self, block: &Block<N>) -> Result<()> {
        // Acquire the sync lock.
        let _lock = self.sync_lock.lock().await;

        // If the block authority is a sub-DAG, then sync the batch certificates with the block.
        // Note that the block authority is always a sub-DAG in production;
        // beacon signatures are only used for testing,
        // and as placeholder (irrelevant) block authority in the genesis block.
        if let Authority::Quorum(subdag) = block.authority() {
            // Reconstruct the unconfirmed transactions.
            let unconfirmed_transactions = cfg_iter!(block.transactions())
                .filter_map(|tx| {
                    tx.to_unconfirmed_transaction().map(|unconfirmed| (unconfirmed.id(), unconfirmed)).ok()
                })
                .collect::<HashMap<_, _>>();

            // Iterate over the certificates.
            for certificates in subdag.values().cloned() {
                cfg_into_iter!(certificates.clone()).for_each(|certificate| {
                    // Sync the batch certificate with the block.
                    self.storage.sync_certificate_with_block(block, certificate.clone(), &unconfirmed_transactions);
                });

                // Sync the BFT DAG with the certificates.
                for certificate in certificates {
                    // If a BFT sender was provided, send the certificate to the BFT.
                    // For validators, BFT spawns a receiver task in `BFT::start_handlers`.
                    if let Some(bft_sender) = self.bft_sender.get() {
                        // Await the callback to continue.
                        if let Err(err) = bft_sender.send_sync_bft(certificate).await {
                            bail!("Failed to sync certificate - {err}");
                        };
                    }
                }
            }

            ensure!(tail.height() + 1 == new_block.height(), "Got an out-of-order block");
        }

        Ok(())
    }
}

// Methods to assist with the block sync module.
impl<N: Network> Sync<N> {
    /// Returns `true` if the node is synced and has connected peers.
    pub fn is_synced(&self) -> bool {
        // Ensure the validator is connected to other validators,
        // not just clients.
        if self.gateway.number_of_connected_peers() == 0 {
            return false;
        }

        self.block_sync.is_block_synced()
    }

    /// Like `is_synced` but returns more information.
    pub fn check_synced(&self) -> Result<()> {
        if self.gateway.number_of_connected_peers() == 0 {
            bail!("Not connected to any other validators yet");
        }

        if !self.block_sync.is_block_synced() {
            bail!("Validator is still syncing");
        }

        Ok(())
    }

    /// Returns the number of blocks the node is behind the greatest peer height.
    pub fn num_blocks_behind(&self) -> Option<u32> {
        self.block_sync.num_blocks_behind()
    }

    /// Returns the current block locators of the node.
    pub fn get_block_locators(&self, latest_height: u32) -> Result<BlockLocators<N>> {
        self.block_sync.get_block_locators(latest_height)
    }
}

// Methods to assist with fetching batch certificates from peers.
impl<N: Network> Sync<N> {
    /// Sends a certificate request to the specified peer.
    pub async fn send_certificate_request(
        &self,
        peer_ip: SocketAddr,
        certificate_id: Field<N>,
    ) -> Result<BatchCertificate<N>> {
        // Initialize a oneshot channel.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Determine how many sent requests are pending.
        let num_sent_requests = self.pending.num_sent_requests(certificate_id);
        // Determine if we've already sent a request to the peer.
        let contains_peer_with_sent_request = self.pending.contains_peer_with_sent_request(certificate_id, peer_ip);
        // Determine the maximum number of redundant requests.
        let num_redundant_requests = max_redundant_requests(self.ledger.clone(), self.storage.current_round())?;
        // Determine if we should send a certificate request to the peer.
        // We send at most `num_redundant_requests` requests and each peer can only receive one request at a time.
        let should_send_request = num_sent_requests < num_redundant_requests && !contains_peer_with_sent_request;

        // Insert the certificate ID into the pending queue.
        self.pending.insert(certificate_id, peer_ip, Some((callback_sender, should_send_request)));

        // If the number of requests is less than or equal to the redundancy factor, send the certificate request to the peer.
        if should_send_request {
            // Send the certificate request to the peer.
            if self.gateway.send(peer_ip, Event::CertificateRequest(certificate_id.into())).await.is_none() {
                bail!("Unable to fetch batch certificate {certificate_id} - failed to send request")
            }
        } else {
            debug!(
                "Skipped sending request for certificate {} to '{peer_ip}' ({num_sent_requests} redundant requests)",
                fmt_id(certificate_id)
            );
        }
        // Wait for the certificate to be fetched.
        // TODO (raychu86): Consider making the timeout dynamic based on network traffic and/or the number of validators.
        match tokio::time::timeout(Duration::from_millis(MAX_FETCH_TIMEOUT_IN_MS), callback_receiver).await {
            // If the certificate was fetched, return it.
            Ok(result) => Ok(result?),
            // If the certificate was not fetched, return an error.
            Err(e) => bail!("Unable to fetch certificate {} - (timeout) {e}", fmt_id(certificate_id)),
        }
    }

    /// Handles the incoming certificate request.
    fn send_certificate_response(&self, peer_ip: SocketAddr, request: CertificateRequest<N>) {
        // Attempt to retrieve the certificate.
        if let Some(certificate) = self.storage.get_certificate(request.certificate_id) {
            // Send the certificate response to the peer.
            let self_ = self.clone();
            tokio::spawn(async move {
                let _ = self_.gateway.send(peer_ip, Event::CertificateResponse(certificate.into())).await;
            });
        }
    }

    /// Handles the incoming certificate response.
    /// This method ensures the certificate response is well-formed and matches the certificate ID.
    fn finish_certificate_request(&self, peer_ip: SocketAddr, response: CertificateResponse<N>) {
        let certificate = response.certificate;
        // Check if the peer IP exists in the pending queue for the given certificate ID.
        let exists = self.pending.get_peers(certificate.id()).unwrap_or_default().contains(&peer_ip);
        // If the peer IP exists, finish the pending request.
        if exists {
            // TODO: Validate the certificate.
            // Remove the certificate ID from the pending queue.
            self.pending.remove(certificate.id(), Some(certificate));
        }
    }
}

impl<N: Network> Sync<N> {
    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the primary.
    pub async fn shut_down(&self) {
        info!("Shutting down the sync module...");
        // Acquire the response lock.
        let _lock = self.response_lock.lock().await;
        // Acquire the sync lock.
        let _lock = self.sync_lock.lock().await;
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{helpers::now, ledger_service::CoreLedgerService, storage_service::BFTMemoryService};

    use snarkos_account::Account;
    use snarkos_node_sync::BlockSync;
    use snarkvm::{
        console::{
            account::{Address, PrivateKey},
            network::MainnetV0,
        },
        ledger::{
            narwhal::{BatchCertificate, BatchHeader, Subdag},
            store::{ConsensusStore, helpers::memory::ConsensusMemory},
        },
        prelude::{Ledger, VM},
        utilities::TestRng,
    };

    use aleo_std::StorageMode;
    use indexmap::IndexSet;
    use rand::Rng;
    use std::collections::BTreeMap;

    type CurrentNetwork = MainnetV0;
    type CurrentLedger = Ledger<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;
    type CurrentConsensusStore = ConsensusStore<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;

    /// Tests that commits work as expected when some anchors are not committed immediately.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_commit_chain() -> anyhow::Result<()> {
        let rng = &mut TestRng::default();
        // Initialize the round parameters.
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;

        // The first round of the first block.
        let first_round = 1;
        // The total number of blocks we test
        let num_blocks = 3;
        // The number of certificate rounds needed.
        // There is one additional round to provide availability for the inal block.
        let num_rounds = first_round + num_blocks * 2 + 1;
        // The first round that has at least N-f certificates referencing the anchor from the previous round.
        // This is also the last round we use in the test.
        let first_committed_round = num_rounds - 1;

        // Initialize the store.
        let store = CurrentConsensusStore::open(StorageMode::new_test(None)).unwrap();
        let account: Account<CurrentNetwork> = Account::new(rng)?;

        // Create a genesis block with a seeded RNG to reproduce the same genesis private keys.
        let seed: u64 = rng.r#gen();
        let genesis_rng = &mut TestRng::from_seed(seed);
        let genesis = VM::from(store).unwrap().genesis_beacon(account.private_key(), genesis_rng).unwrap();

        // Extract the private keys from the genesis committee by using the same RNG to sample private keys.
        let genesis_rng = &mut TestRng::from_seed(seed);
        let private_keys = [
            *account.private_key(),
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
        ];

        // Initialize the ledger with the genesis block.
        let ledger = CurrentLedger::load(genesis.clone(), StorageMode::new_test(None)).unwrap();
        // Initialize the ledger.
        let core_ledger = Arc::new(CoreLedgerService::new(ledger.clone(), Default::default()));

        // Sample 5 rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            let addresses = vec![
                Address::try_from(private_keys[0])?,
                Address::try_from(private_keys[1])?,
                Address::try_from(private_keys[2])?,
                Address::try_from(private_keys[3])?,
            ];

            let committee = ledger.latest_committee().unwrap();

            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: HashMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> =
                HashMap::new();
            let mut previous_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::with_capacity(4);

            for round in first_round..=first_committed_round {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };

                let committee_id = committee.id();
                let prev_leader = committee.get_leader(round - 1).unwrap();

                // For the first two blocks non-leaders will not reference the leader certificate.
                // This means, while there is an anchor, it is isn't committed
                // until later.
                for (i, private_key) in private_keys.iter().enumerate() {
                    let leader_index = addresses.iter().position(|&address| address == prev_leader).unwrap();
                    let is_certificate_round = round % 2 == 1;
                    let is_leader = i == leader_index;

                    let previous_certs = if round < first_committed_round && is_certificate_round && !is_leader {
                        previous_certificate_ids
                            .iter()
                            .cloned()
                            .enumerate()
                            .filter(|(idx, _)| *idx != leader_index)
                            .map(|(_, id)| id)
                            .collect()
                    } else {
                        previous_certificate_ids.clone()
                    };

                    let batch_header = BatchHeader::new(
                        private_key,
                        round,
                        now(),
                        committee_id,
                        Default::default(),
                        previous_certs,
                        rng,
                    )
                    .unwrap();

                    // Sign the batch header.
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    current_certificates.insert(BatchCertificate::from(batch_header, signatures).unwrap());
                }

                // Update the map of certificates.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates;
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the storage.
        let storage = Storage::new(core_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds);
        // Insert all certificates into storage.
        let mut certificates: Vec<BatchCertificate<CurrentNetwork>> = Vec::new();
        for i in first_round..=first_committed_round {
            let c = (*round_to_certificates_map.get(&i).unwrap()).clone();
            certificates.extend(c);
        }
        for certificate in certificates.clone().iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        // Create the blocks
        let mut previous_leader_cert = None;
        let mut blocks = vec![];

        for block_height in 1..=num_blocks {
            let leader_round = block_height * 2;

            let leader = committee.get_leader(leader_round).unwrap();
            let leader_certificate = storage.get_certificate_for_round_with_author(leader_round, leader).unwrap();

            let mut subdag_map: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
            let mut leader_cert_map = IndexSet::new();
            leader_cert_map.insert(leader_certificate.clone());

            let previous_cert_map = storage.get_certificates_for_round(leader_round - 1);

            subdag_map.insert(leader_round, leader_cert_map.clone());
            subdag_map.insert(leader_round - 1, previous_cert_map.clone());

            if leader_round > 2 {
                let previous_commit_cert_map: IndexSet<_> = storage
                    .get_certificates_for_round(leader_round - 2)
                    .into_iter()
                    .filter(|cert| {
                        if let Some(previous_leader_cert) = &previous_leader_cert {
                            cert != previous_leader_cert
                        } else {
                            true
                        }
                    })
                    .collect();
                subdag_map.insert(leader_round - 2, previous_commit_cert_map);
            }

            let subdag = Subdag::from(subdag_map.clone())?;
            let block = core_ledger.prepare_advance_to_next_quorum_block(subdag, Default::default())?;

            previous_leader_cert = Some(leader_certificate);

            core_ledger.advance_to_next_block(&block)?;
            blocks.push(block);
        }

        // ### Test that sync works as expected ###

        // Create a new ledger to test with, but use the existing storage
        // so that the certificates exist.
        let syncing_ledger = Arc::new(CoreLedgerService::new(
            CurrentLedger::load(genesis, StorageMode::new_test(None)).unwrap(),
            Default::default(),
        ));

        // Set up sync and its dependencies.
        let gateway = Gateway::new(account.clone(), storage.clone(), syncing_ledger.clone(), None, &[], None)?;
        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone()));
        let sync = Sync::new(gateway.clone(), storage.clone(), syncing_ledger.clone(), block_sync);

        let mut block_iter = blocks.into_iter();

        // Insert the blocks into the new sync module
        for _ in 0..num_blocks - 1 {
            let block = block_iter.next().unwrap();
            sync.sync_storage_with_block(block).await?;

            // Availability threshold is not met, so we should not advance yet.
            assert_eq!(syncing_ledger.latest_block_height(), 0);
        }

        // Only for the final block, the availability threshold is met,
        // because certificates for the subsequent round are already in storage.
        sync.sync_storage_with_block(block_iter.next().unwrap()).await?;
        assert_eq!(syncing_ledger.latest_block_height(), 3);

        // Ensure blocks 1 and 2 were added to the ledger.
        assert!(syncing_ledger.contains_block_height(1));
        assert!(syncing_ledger.contains_block_height(2));

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_pending_certificates() -> anyhow::Result<()> {
        let rng = &mut TestRng::default();
        // Initialize the round parameters.
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let commit_round = 2;

        // Initialize the store.
        let store = CurrentConsensusStore::open(StorageMode::new_test(None)).unwrap();
        let account: Account<CurrentNetwork> = Account::new(rng)?;

        // Create a genesis block with a seeded RNG to reproduce the same genesis private keys.
        let seed: u64 = rng.r#gen();
        let genesis_rng = &mut TestRng::from_seed(seed);
        let genesis = VM::from(store).unwrap().genesis_beacon(account.private_key(), genesis_rng).unwrap();

        // Extract the private keys from the genesis committee by using the same RNG to sample private keys.
        let genesis_rng = &mut TestRng::from_seed(seed);
        let private_keys = [
            *account.private_key(),
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
        ];
        // Initialize the ledger with the genesis block.
        let ledger = CurrentLedger::load(genesis.clone(), StorageMode::new_test(None)).unwrap();
        // Initialize the ledger.
        let core_ledger = Arc::new(CoreLedgerService::new(ledger.clone(), Default::default()));
        // Sample rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            // Initialize the committee.
            let committee = ledger.latest_committee().unwrap();
            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: HashMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> =
                HashMap::new();
            let mut previous_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::with_capacity(4);

            for round in 0..=commit_round + 8 {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };
                let committee_id = committee.id();
                // Create a certificate for each validator.
                for (i, private_key_1) in private_keys.iter().enumerate() {
                    let batch_header = BatchHeader::new(
                        private_key_1,
                        round,
                        now(),
                        committee_id,
                        Default::default(),
                        previous_certificate_ids.clone(),
                        rng,
                    )
                    .unwrap();
                    // Sign the batch header.
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    current_certificates.insert(BatchCertificate::from(batch_header, signatures).unwrap());
                }

                // Update the map of certificates.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates.clone();
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the storage.
        let storage = Storage::new(core_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds);
        // Insert certificates into storage.
        let mut certificates: Vec<BatchCertificate<CurrentNetwork>> = Vec::new();
        for i in 1..=commit_round + 8 {
            let c = (*round_to_certificates_map.get(&i).unwrap()).clone();
            certificates.extend(c);
        }
        for certificate in certificates.clone().iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }
        // Create block 1.
        let leader_round_1 = commit_round;
        let leader_1 = committee.get_leader(leader_round_1).unwrap();
        let leader_certificate = storage.get_certificate_for_round_with_author(commit_round, leader_1).unwrap();
        let mut subdag_map: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
        let block_1 = {
            let mut leader_cert_map = IndexSet::new();
            leader_cert_map.insert(leader_certificate.clone());
            let mut previous_cert_map = IndexSet::new();
            for cert in storage.get_certificates_for_round(commit_round - 1) {
                previous_cert_map.insert(cert);
            }
            subdag_map.insert(commit_round, leader_cert_map.clone());
            subdag_map.insert(commit_round - 1, previous_cert_map.clone());
            let subdag = Subdag::from(subdag_map.clone())?;
            core_ledger.prepare_advance_to_next_quorum_block(subdag, Default::default())?
        };
        // Insert block 1.
        core_ledger.advance_to_next_block(&block_1)?;

        // Create block 2.
        let leader_round_2 = commit_round + 2;
        let leader_2 = committee.get_leader(leader_round_2).unwrap();
        let leader_certificate_2 = storage.get_certificate_for_round_with_author(leader_round_2, leader_2).unwrap();
        let mut subdag_map_2: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
        let block_2 = {
            let mut leader_cert_map_2 = IndexSet::new();
            leader_cert_map_2.insert(leader_certificate_2.clone());
            let mut previous_cert_map_2 = IndexSet::new();
            for cert in storage.get_certificates_for_round(leader_round_2 - 1) {
                previous_cert_map_2.insert(cert);
            }
            subdag_map_2.insert(leader_round_2, leader_cert_map_2.clone());
            subdag_map_2.insert(leader_round_2 - 1, previous_cert_map_2.clone());
            let subdag_2 = Subdag::from(subdag_map_2.clone())?;
            core_ledger.prepare_advance_to_next_quorum_block(subdag_2, Default::default())?
        };
        // Insert block 2.
        core_ledger.advance_to_next_block(&block_2)?;

        // Create block 3
        let leader_round_3 = commit_round + 4;
        let leader_3 = committee.get_leader(leader_round_3).unwrap();
        let leader_certificate_3 = storage.get_certificate_for_round_with_author(leader_round_3, leader_3).unwrap();
        let mut subdag_map_3: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
        let block_3 = {
            let mut leader_cert_map_3 = IndexSet::new();
            leader_cert_map_3.insert(leader_certificate_3.clone());
            let mut previous_cert_map_3 = IndexSet::new();
            for cert in storage.get_certificates_for_round(leader_round_3 - 1) {
                previous_cert_map_3.insert(cert);
            }
            subdag_map_3.insert(leader_round_3, leader_cert_map_3.clone());
            subdag_map_3.insert(leader_round_3 - 1, previous_cert_map_3.clone());
            let subdag_3 = Subdag::from(subdag_map_3.clone())?;
            core_ledger.prepare_advance_to_next_quorum_block(subdag_3, Default::default())?
        };
        // Insert block 3.
        core_ledger.advance_to_next_block(&block_3)?;

        /*
            Check that the pending certificates are computed correctly.
        */

        // Retrieve the pending certificates.
        let pending_certificates = storage.get_pending_certificates();
        // Check that all of the pending certificates are not contained in the ledger.
        for certificate in pending_certificates.clone() {
            assert!(!core_ledger.contains_certificate(&certificate.id()).unwrap_or(false));
        }
        // Initialize an empty set to be populated with the committed certificates in the block subdags.
        let mut committed_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::new();
        {
            let subdag_maps = [&subdag_map, &subdag_map_2, &subdag_map_3];
            for subdag in subdag_maps.iter() {
                for subdag_certificates in subdag.values() {
                    committed_certificates.extend(subdag_certificates.iter().cloned());
                }
            }
        };
        // Create the set of candidate pending certificates as the set of all certificates minus the set of the committed certificates.
        let mut candidate_pending_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::new();
        for certificate in certificates.clone() {
            if !committed_certificates.contains(&certificate) {
                candidate_pending_certificates.insert(certificate);
            }
        }
        // Check that the set of pending certificates is equal to the set of candidate pending certificates.
        assert_eq!(pending_certificates, candidate_pending_certificates);
        Ok(())
    }
}
