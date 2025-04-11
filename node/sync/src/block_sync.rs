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
    helpers::{BlockId, BlockRequestId, BlockSyncRequest},
    locators::BlockLocators,
    pending_chain::PendingChain,
};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_router::messages::DataBlocks;
use snarkos_node_sync_communication_service::CommunicationService;
use snarkvm::prelude::{Network, block::Block};

use snarkvm::{
    console::network::Network,
    ledger::{
        PendingBlock,
        authority::Authority,
        block::Block,
        narwhal::{BatchHeader, Subdag},
    },
};

use anyhow::{Result, bail};
use indexmap::IndexMap;
use itertools::Itertools;
#[cfg(feature = "locktick")]
use locktick::{parking_lot::RwLock, tokio::Mutex as TMutex};
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
use rand::seq::{IteratorRandom, SliceRandom};
use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex as TMutex;
use tokio::sync::Notify;

mod helpers;
use helpers::rangify_heights;

mod sync_state;
use sync_state::SyncState;

/// The time nodes wait between issuing batches of block requests to avoid triggering spam detection.
// TODO (kaimast): Document why 10ms (not 1 or 100)
pub const BLOCK_REQUEST_BATCH_DELAY: Duration = Duration::from_millis(10);

/// The maximum number of peers we attempt to sync from.
const NUM_SYNC_CANDIDATE_PEERS: usize = 5;

const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// The maximum number of outstanding block requests.
/// Once a node hits this limit, it will not issue any new requests until existing requests time out or receive responses.
const MAX_BLOCK_REQUESTS: usize = 50; // 50 requests

/// The maximum number of blocks tolerated before the primary is considered behind its peers.
/// This is set to two because the most recent block will not be confirmed until the next one.
pub const MAX_BLOCKS_BEHIND: u32 = 2; // blocks

/// This is a dummy IP address that is used to represent the local node.
/// Note: This here does not need to be a real IP address, but it must be unique/distinct from all other connections.
pub const DUMMY_SELF_IP: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

/// Handle to an outstanding requested, containing the request itself and its timestamp.
/// This does not contain the response so that checking for responses does not require iterating over all requests.
#[derive(Clone)]
struct OutstandingRequest<N: Network> {
    request: SyncRequest<N>,
    timestamp: Instant,
    /// The corresponding response (if any).
    /// This is guaranteed to be Some if sync_ips for the given request are empty.
    response: Option<Block<N>>,
}

/// Information about a block request (used for the REST API).
#[derive(Clone, serde::Serialize)]
pub struct BlockRequestInfo {
    /// Seconds since the request was created
    elapsed: u64,
    /// Has the request been responded to?
    done: bool,
}

/// Summary of completed all in-flight requests.
#[derive(Clone, serde::Serialize)]
pub struct BlockRequestsSummary {
    outstanding: String,
    completed: String,
}

impl<N: Network> OutstandingRequest<N> {
    /// Get a reference to the IPs of peers that have not responded to the request (yet).
    fn sync_ips(&self) -> &IndexSet<SocketAddr> {
        let (_, _, sync_ips) = &self.request;
        sync_ips
    }

    /// Get a mutable reference to the IPs of peers that have not responded to the request (yet).
    fn sync_ips_mut(&mut self) -> &mut IndexSet<SocketAddr> {
        let (_, _, sync_ips) = &mut self.request;
        sync_ips
    }
}

struct BlockHeights {
    /// Advertised block height and last requested sync height for each peers.
    peer_heights: HashMap<SocketAddr, (u32, u32)>,
    /// The position at which we are syncing right now.
    sync_height: u32,
}

/// Struct that tracks oustanding requests.
///
/// This is wrapped in a single lock in BlockSync, so that it
/// can be updated atomically.
#[derive(Clone)]
struct BlockSyncState<N: Network> {
    /// Removing an entry from this map must remove the corresponding entry from the requests map.
    responses: HashMap<BlockRequestId, Vec<Block<N>>>,
}

impl<N: Network> Default for BlockSyncState<N> {
    fn default() -> Self {
        Self { requests: Default::default(), block_to_request: Default::default(), responses: Default::default() }
    }
}

/// A struct that keeps track of synchronizing blocks with other nodes.
///
/// It generates requests to send to other peers and processes responses to those requests.
/// The struct also keeps track of block locators, which indicate which peers it can fetch blocks from.
///
/// # Notes
/// - The actual network communication happens in `snarkos_node::Client` (for clients and provers) and in `snarkos_node_bft::Sync` (for validators).
///
/// - Validators only sync from other nodes using this struct if they fall behind, e.g.,
///   because they experience a network partition.
///   In the common case, validators will generate blocks from the DAG after an anchor certificate has been approved
///   by a supermajority of the committee.
pub struct BlockSync<N: Network> {
    /// The ledger.
    ledger: Arc<dyn LedgerService<N>>,

    /// The map of peer IP to their block locators.
    /// The block locators are consistent with the ledger and every other peer's block locators.
    locators: RwLock<HashMap<SocketAddr, BlockLocators<N>>>,

    /// The map of peer-to-peer to their common ancestor.
    /// This map is used to determine which peers to request blocks from.
    ///
    /// Lock ordering: when locking both, `common_ancestors` and `locators`, `common_ancestors` must be locked first.
    common_ancestors: RwLock<IndexMap<PeerPair, u32>>,

    /// The block requests in progress and their responses.
    requests: RwLock<BTreeMap<u32, OutstandingRequest<N>>>,

    /// Allows tracking if specific block has already been requested.
    block_to_request: RwLock<HashMap<N::BlockHash, BlockRequestId>>,

    /// The boolean indicator of whether the node is synced up to the latest block (within the given tolerance).
    ///
    /// Lock ordering: if you lock `sync_state` and `requests`, you must lock `sync_state` first.
    sync_state: RwLock<SyncState>,

    /// The lock used to ensure that [`Self::advance_with_sync_blocks()`] is called by one task at a time.
    advance_with_sync_blocks_lock: TMutex<()>,

    /// Gets notified when there was an update to the locators, a peer disconnected, or we received a new block response.
    notify: Notify,
    /// The peer heights and current sync height.
    block_heights: Arc<TMutex<BlockHeights>>,

    /// The set of pending chains (blocks that have not received sufficient votes yet).
    pending_chains: RwLock<Vec<PendingChain<N>>>,
}

impl<N: Network> BlockSync<N> {
    /// Initializes a new block sync module.
    pub fn new(ledger: Arc<dyn LedgerService<N>>) -> Self {
        let block_heights = BlockHeights { peer_heights: Default::default(), sync_height: 0 };

        Self {
            ledger,
            sync_state: Default::default(),
            notify: Default::default(),
            block_heights: Arc::new(TMutex::new(block_heights)),
            locators: Default::default(),
            requests: Default::default(),
            blocks_to_request: Default::default(),
            common_ancestors: Default::default(),
            advance_with_sync_blocks_lock: Default::default(),
        }
    }

    pub async fn wait_for_update(&self) {
        self.notify.notified().await
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    #[inline]
    pub fn is_block_synced(&self) -> bool {
        self.sync_state.read().is_block_synced()
    }

    /// Returns `true` if there a blocks to fetch or responses to process.
    ///
    /// This will always return true if [`Self::is_block_synced`] returns false,
    /// but it can return true when [`Self::is_block_synced`] returns true
    /// (due to the latter having a tolerance of one block).
    #[inline]
    pub fn can_block_sync(&self) -> bool {
        self.sync_state.read().can_block_sync() || self.has_pending_responses()
    }

    /// Returns the number of blocks the node is behind the greatest peer height,
    /// or `None` if no peers are connected yet.
    #[inline]
    pub fn num_blocks_behind(&self) -> Option<u32> {
        self.sync_state.read().num_blocks_behind()
    }

    /// Returns the greatest block height of any connected peer.
    #[inline]
    pub fn greatest_peer_block_height(&self) -> Option<u32> {
        self.sync_state.read().get_greatest_peer_height()
    }

    /// Returns the current sync height of this node.
    /// The sync height is always greater or equal to the ledger height.
    #[inline]
    pub fn get_sync_height(&self) -> u32 {
        self.sync_state.read().get_sync_height()
    }

    /// Returns the number of blocks we requested from peers, but have not received yet.
    #[inline]
    pub fn num_outstanding_block_requests(&self) -> usize {
        self.requests.read().iter().filter(|(_, e)| !e.sync_ips().is_empty()).count()
    }

    /// The total number of block request, including the ones that have been answered already but not processed yet.
    #[inline]
    pub fn num_total_block_requests(&self) -> usize {
        self.requests.read().len()
    }

    //// Returns the latest locator height for all known peers.
    pub fn get_peer_heights(&self) -> HashMap<SocketAddr, u32> {
        self.locators.read().iter().map(|(addr, locators)| (*addr, locators.latest_locator_height())).collect()
    }

    //// Returns information about all in-flight block requests.
    pub fn get_block_requests_info(&self) -> BTreeMap<u32, BlockRequestInfo> {
        self.requests
            .read()
            .iter()
            .map(|(height, request)| {
                (*height, BlockRequestInfo {
                    done: request.sync_ips().is_empty(),
                    elapsed: request.timestamp.elapsed().as_secs(),
                })
            })
            .collect()
    }

    /// Returns a summary of all in-flight requests.
    pub fn get_block_requests_summary(&self) -> BlockRequestsSummary {
        let completed = self
            .requests
            .read()
            .iter()
            .filter_map(|(h, e)| if e.sync_ips().is_empty() { Some(*h) } else { None })
            .collect::<Vec<_>>();

        let outstanding = self
            .requests
            .read()
            .iter()
            .filter_map(|(h, e)| if !e.sync_ips().is_empty() { Some(*h) } else { None })
            .collect::<Vec<_>>();

        BlockRequestsSummary { completed: rangify_heights(&completed), outstanding: rangify_heights(&outstanding) }
    }
}

// Helper functions needed for testing
#[cfg(test)]
impl<N: Network> BlockSync<N> {
    /// Returns the latest block height of the given peer IP.
    fn get_peer_height(&self, peer_ip: &SocketAddr) -> Option<u32> {
        self.locators.read().get(peer_ip).map(|locators| locators.latest_locator_height())
    }

    /// Returns the block request for the given height, if it exists.
    fn get_block_request(&self, height: u32) -> Option<SyncRequest<N>> {
        self.requests.read().get(&height).map(|e| e.request.clone())
    }

    /// Returns the timestamp of the last time the block was requested, if it exists.
    fn get_block_request_timestamp(&self, height: u32) -> Option<Instant> {
        self.requests.read().get(&height).map(|e| e.timestamp)
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns the block locators.
    #[inline]
    pub fn get_block_locators(&self, latest_height: u32) -> Result<BlockLocators<N>> {
        // Initialize the recents map.
        let mut recents = IndexMap::with_capacity(NUM_RECENT_BLOCKS);
        // Retrieve the recent block hashes.
        for height in latest_height.saturating_sub((NUM_RECENT_BLOCKS - 1) as u32)..=latest_height {
            recents.insert(height, self.ledger.get_block_hash(height)?);
        }

        // Initialize the checkpoints map.
        let mut checkpoints = IndexMap::with_capacity((latest_height / CHECKPOINT_INTERVAL + 1).try_into()?);
        // Retrieve the checkpoint block hashes.
        for height in (0..=latest_height).step_by(CHECKPOINT_INTERVAL as usize) {
            checkpoints.insert(height, self.ledger.get_block_hash(height)?);
        }

        // Construct the block locators.
        BlockLocators::new(recents, checkpoints)
    }

    /// Returns true if there are pending responses to block requests that need to be processed.
    pub fn has_pending_responses(&self) -> bool {
        self.requests.read().iter().filter(|(_, req)| req.response.is_some() && req.sync_ips().is_empty()).count() > 0
    }

    /// Send a batch of block requests.
    #[must_use]
    pub async fn send_block_request<C: CommunicationService>(
        &self,
        communication: &C,
        request: BlockSyncRequest<N>,
    ) -> bool {
        let request_id = (request.sync_peer, request.start_height());

        // Construct the message.
        let message = C::prepare_block_request(start_height, end_height);

        // Send the message to the peers.
        let mut tasks = Vec::with_capacity(sync_ips.len());
        for sync_ip in sync_ips {
            let sender = communication.send(sync_ip, message.clone()).await;
            let task = tokio::spawn(async move {
                // Ensure the request is sent successfully.
                match sender {
                    Some(sender) => {
                        if let Err(err) = sender.await {
                            warn!("Failed to send block request to peer '{sync_ip}': {err}");
                            false
                        } else {
                            true
                        }
                    }
                    None => {
                        warn!("Failed to send block request to peer '{sync_ip}': no such peer");
                        false
                    }
                }
            });

            tasks.push(task);
        }

        // Wait for all sends to finish at the same time.
        for result in futures::future::join_all(tasks).await {
            let success = match result {
                Ok(success) => success,
                Err(err) => {
                    error!("tokio join error: {err}");
                    false
                }
            };

            // If sending fails for any peer, remove the block request from the sync pool.
            if !success {
                // Remove the entire block request from the sync pool.
                for height in start_height..end_height {
                    self.remove_block_request(height);
                }
                // Break out of the loop.
                return false;
            }
        }

        // Send the message to the peers.
        let sender = communication.send(sync_peer, message).await;

        // If sending fails, remove the block request from the sync pool.
        if sender.is_none() {
            warn!("Failed to send block request to peer '{}'", sync_peer);
            // Remove the entire block request from the sync pool.
            self.remove_block_request(&request_id);
            return false;
        }

        true
    }

    /// Inserts a new block response from the given peer IP.
    ///
    /// Returns an error if the block was malformed, or we already received a different block for this height.
    /// This function also removes all block requests from the given peer IP on failure.
    ///
    /// Note, that this only queues the response. After this, you most likely want to call `Self::try_advancing_block_synchronization`.
    ///
    #[inline]
    pub fn insert_block_responses(&self, peer_ip: SocketAddr, blocks: Vec<Block<N>>) -> Result<()> {
        // Insert the candidate blocks into the sync pool.
        for block in blocks {
            if let Err(error) = self.insert_block_response(peer_ip, block) {
                self.remove_block_requests_to_peer(&peer_ip);
                bail!("{error}");
            }
        }
        Ok(())
    }

    /// Returns the next block for the given `next_height` if the request is complete,
    /// or `None` otherwise. This does not remove the block from the `responses` map.
    #[inline]
    pub fn peek_next_blocks(&self) -> Vec<(BlockRequestId, Vec<Block<N>>)> {
        // Note: This lock must be held across the entire scope, due to asynchronous block responses
        let requests = self.requests.read();

        requests
            .responses
            .iter()
            .filter_map(|(request_id, blocks)| {
                let Some(first_block) = blocks.first() else {
                    // This should never happen.
                    warn!("Response is empty");
                    return None;
                };

                if first_block.height() <= self.ledger.latest_block_height() {
                    return Some((*request_id, blocks.clone()));
                }

                if self.ledger.latest_block().hash() == first_block.previous_hash() {
                    return Some((*request_id, blocks.clone()));
                }

                for pending_chain in self.pending_chains.read().iter() {
                    if pending_chain.contains(&first_block.previous_hash()) {
                        return Some((*request_id, blocks.clone()));
                    }
                }

                None
            })
            .collect()
    }

    fn check_votes(&self, dag: &Subdag<N>, height: u32, previous_dag: &Subdag<N>) -> Result<bool> {
        let leader_certificate = previous_dag.leader_certificate();

        let commit_round = leader_certificate.round();
        let certificate_round = commit_round + 1;

        let authors: HashSet<_> = dag
            .certificates()
            .filter_map(|cert| {
                if cert.round() == certificate_round
                    && cert.previous_certificate_ids().contains(&leader_certificate.id())
                {
                    Some(cert.author())
                } else {
                    None
                }
            })
            .collect();

        let certificate_committee_lookback = self.ledger.get_committee_lookback_for_round(certificate_round)?;

        debug!("Validating sync block {height} at round {commit_round}...");
        // Check if the leader is ready to be committed.

        Ok(certificate_committee_lookback.is_availability_threshold_reached(&authors))
    }

    /// Attempts to advance synchronization by processing completed block responses.
    ///
    /// Validators will not call this function, but instead execute `snarkos_node_bft::Sync::try_advancing_block_synchronization`
    /// which also updates the BFT state.
    ///
    /// Returns the list of blocks that were newly applied to the ledger.
    #[inline]
    pub fn try_advancing_block_synchronization(&self) -> Vec<Block<N>> {
        // Acquire the lock to ensure this function is called only once at a time.
        // If the lock is already acquired, return early.
        let Some(_lock) = self.advance_with_sync_blocks_lock.try_lock() else {
            trace!("Skipping attempt to advance block synchronziation as it is already in progress");
            return vec![];
        };

        let mut ledger_height = self.ledger.latest_block_height();
        let mut pending_chains = self.pending_chains.write();

        // New blocks that have been confirmed.
        let mut advanced_by = vec![];

        for (request_id, blocks) in self.peek_next_blocks() {
            for block in blocks {
                let block_height = block.height();

                // First, find the pending chain to trueappend to.
                let chain_idx = if block_height == ledger_height + 1 {
                    let pending_chain = PendingChain::new();
                    pending_chains.push(pending_chain);
                    pending_chains.len() - 1
                } else {
                    let mut pending_idx = None;

                    for idx in 0..pending_chains.len() {
                        let chain = &pending_chains[idx];
                        if chain.contains(&block.previous_hash()) {
                            let Some(chain_height) = chain.current_height() else {
                                warn!("There is an empty pending chain.");
                                continue;
                            };

                            // Either extend or fork the chain.
                            if chain_height == block.height() {
                                pending_idx = Some(idx);
                            } else {
                                let new_chain = chain.fork_at(block.height());
                                pending_chains.push(new_chain);
                                pending_idx = Some(pending_chains.len() - 1);
                            }
                        }
                    }

                    match pending_idx {
                        Some(idx) => idx,
                        None => {
                            error!(
                                "Cannot find a suitable prefix for block {} at height {}",
                                block.hash(),
                                block.height()
                            );
                            break;
                        }
                    }
                };

                let pending_chain = &mut pending_chains[chain_idx];

                let block = match self.ledger.check_block_subdag(block, pending_chain.blocks()) {
                    Ok(pending) => pending,
                    Err(err) => {
                        warn!("Discarding invalid block - {err}");
                        //TODO remove empty pending chain here, if needed.
                        continue;
                    }
                };

                // Check if we can confirm blocks
                let has_votes = match block.authority() {
                    Authority::Beacon(_) => true,
                    Authority::Quorum(dag) => match pending_chain.head() {
                        Some(previous_block) => {
                            let Authority::Quorum(previous_dag) = previous_block.authority() else {
                                error!("Invalid authority for previous block");
                                continue;
                            };

                            match self.check_votes(dag, previous_block.height(), previous_dag) {
                                Ok(b) => b,
                                Err(err) => {
                                    warn!("Unexpected problem - {err}");
                                    continue;
                                }
                            }
                        }
                        None => {
                            // Pending chain is empty. Nothing to confirm.
                            false
                        }
                    },
                };

                let mut advanced = false;
                if has_votes {
                    for previous_block in pending_chain.blocks() {
                        let previous_block = previous_block.clone();
                        if let Some(pblock) = self.try_to_confirm_block(previous_block) {
                            advanced_by.push(pblock);
                            ledger_height += 1;
                            advanced = true;
                        } else {
                            break;
                        }
                    }
                }

                let block_hash = block.hash();

                if !pending_chain.append(block) {
                    error!("Pending chain already contained block?");
                }

                debug!("Added new pending block {} at height {}", &block_hash, block_height);

                if pending_chain.len() > 10 {
                    warn!("Pending chain is very long: {} blocks", pending_chain.len());
                }

                // If we successfully confirmed blocks, get rid of unneeded blocks.
                if advanced {
                    // Truncate chains.
                    for pending_chain in pending_chains.iter_mut() {
                        pending_chain.truncate_prefix(ledger_height);
                    }

                    // Remove any obsolete chain.
                    pending_chains.retain(|c| !c.is_empty());
                }
            }

            self.remove_block_request(&request_id);
        }

        if advanced_by.is_empty() {
            let mut last_update = self.last_update.lock();
            let elapsed = Instant::now() - *last_update;

            // Print debug message if we are (possibly) stuck.
            if elapsed > Duration::from_secs(60) && !self.is_block_synced() {
                error!("Block synchornization has not made progress for over a minute");
                // Update so we don't immediately print again.
                *last_update = Instant::now();
            }
        } else {
            debug!("Advanced by {} blocks", advanced_by.len());
            *self.last_update.lock() = Instant::now();
        }

        advanced_by
    }

    /// Try to apply the next pending block to the ledger.
    fn try_to_confirm_block(&self, pending_block: PendingBlock<N>) -> Option<Block<N>> {
        let block = match self.ledger.check_block_content(pending_block) {
            Ok(block) => block,
            Err(err) => {
                warn!("Failed to verify block contents: {err}");
                return None;
            }
        };

        info!("Syncing the ledger to block at height {}", block.height());

        match self.ledger.advance_to_next_block(&block) {
            Ok(_) => Some(block),
            Err(err) => {
                warn!("Failed to advance to next block (height: {}, hash: '{}'): {err}", block.height(), block.hash());
                None
            }
        }
    }
}

// Functionality related to sync peers.
impl<N: Network> BlockSync<N> {
    /// Returns the sync peers with their latest heights, and their minimum common ancestor, if the node can sync.
    /// This function returns peers that are consistent with each other, and have a block height
    /// that is greater than the ledger height of this node.
    pub fn find_sync_peers(&self) -> IndexMap<SocketAddr, u32> {
        self.find_sync_peers_at_height(self.ledger.latest_block_height())
    }

    /// Same as `Self::find_sync_peers`, but allows specifiying a custom height
    /// (must be greater than the ledger height).
    pub fn find_sync_peers_at_height(&self, current_height: u32) -> IndexMap<SocketAddr, u32> {
        let sync_peers = self.find_sync_peers_inner(current_height);
        // Map the locators into the latest height.
        sync_peers.into_iter().map(|(ip, locators)| (ip, locators.latest_locator_height())).collect()
    }

    /// Attempts to advance synchronization by processing completed block responses.
    ///
    /// Returns true, if new blocks were added to the ledger.
    ///
    /// # Usage
    /// This is only called in [`Client::try_block_sync`] and should not be called concurrently by multiple tasks.
    /// Validators do not call this function, and instead invoke
    /// [`snarkos_node_bft::Sync::try_advancing_block_synchronization`] which also updates the BFT state.
    #[inline]
    pub async fn try_advancing_block_synchronization(&self) -> Result<bool> {
        // Acquire the lock to ensure this function is called only once at a time.
        // If the lock is already acquired, return early.
        //
        // Note: This lock should not be needed anymore as there is only one place we call it from,
        // but we keep it for now out of caution.
        // TODO(kaimast): remove this eventually.
        let Ok(_lock) = self.advance_with_sync_blocks_lock.try_lock() else {
            trace!("Skipping attempt to advance block synchronziation as it is already in progress");
            return Ok(false);
        };

        // Start with the current height.
        let mut current_height = self.ledger.latest_block_height();
        let start_height = current_height;
        trace!("Try advancing with block responses (at block {current_height})");

        loop {
            let next_height = current_height + 1;

            let Some(block) = self.peek_next_block(next_height) else {
                break;
            };

            // Ensure the block height matches.
            if block.height() != next_height {
                warn!("Block height mismatch: expected {}, found {}", current_height + 1, block.height());
                break;
            }

            let ledger = self.ledger.clone();
            let advanced = tokio::task::spawn_blocking(move || {
                // Try to check the next block and advance to it.
                match ledger.check_next_block(&block) {
                    Ok(_) => match ledger.advance_to_next_block(&block) {
                        Ok(_) => true,
                        Err(err) => {
                            warn!(
                                "Failed to advance to next block (height: {}, hash: '{}'): {err}",
                                block.height(),
                                block.hash()
                            );
                            false
                        }
                    },
                    Err(err) => {
                        warn!(
                            "The next block (height: {}, hash: '{}') is invalid - {err}",
                            block.height(),
                            block.hash()
                        );
                        false
                    }
                }
            })
            .await?;

            // Remove the block response.
            self.remove_block_response(next_height);

            // If advancing failed, exit the loop.
            if !advanced {
                break;
            }

            // Update the latest height.
            current_height = next_height;
        }

        if current_height > start_height {
            self.set_sync_height(current_height);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<N: Network> BlockSync<N> {
    /// Updates the block locators and common ancestors for the given peer IP.
    ///
    /// This function does not need to check that the block locators are well-formed,
    /// because that is already done in [`BlockLocators::new()`], as noted in [`BlockLocators`].
    ///
    /// This function does **not** check
    /// that the block locators are consistent with the peer's previous block locators or other peers' block locators.
    pub fn update_peer_locators(&self, peer_ip: SocketAddr, locators: BlockLocators<N>) -> Result<()> {
        // Update the locators entry for the given peer IP.
        self.locators.write().insert(peer_ip, locators.clone());

        // Update `is_synced`.
        if let Some(greatest_peer_height) = self.locators.read().values().map(|l| l.latest_locator_height()).max() {
            self.sync_state.write().set_greatest_peer_height(greatest_peer_height);
        }

        // Notify the sync loop that something changed.
        self.notify.notify_one();

        Ok(())
    }

    /// Removes the peer from the sync pool, if they exist.
    pub fn remove_peer(&self, peer_ip: &SocketAddr) {
        trace!("Removing peer {peer_ip} from block sync");

        // Remove the locators entry for the given peer IP.
        self.locators.write().remove(peer_ip);
        // Remove all common ancestor entries for this peers.
        self.common_ancestors.write().retain(|pair, _| !pair.contains(peer_ip));
        // Remove all block requests to the peer.
        self.remove_block_requests_to_peer(peer_ip);

        // Notify the sync loop that something changed.
        self.notify.notify_one();
    }
}

// Functionality related to requests.
impl<N: Network> BlockSync<N> {
    /// Returns a list of block requests and the sync peers, if the node needs to sync.
    ///
    /// You usually want to call `remove_timed_out_block_requests` before invoking this function.
    ///
    /// # Concurrency
    /// This should be called by at most one task at a time.
    ///
    /// # Usage
    ///  - For validators, the primary spawns one task that periodically calls `bft::Sync::try_block_sync`. There is no possibility of multiple calls to it at a time.
    ///  - For clients, `Client::initialize_sync` also spawns exactly one task that periodically calls this function.
    ///  - Provers do not call this function.
    pub fn prepare_block_requests(&self) -> BlockRequestBatch<N> {
        // Used to print more information when we max out on requests.
        let print_requests = || {
            if tracing::enabled!(tracing::Level::TRACE) {
                let summary = self.get_block_requests_summary();

                trace!("The following requests are complete but not processed yet: {:?}", summary.completed);
                trace!("The following requests are still outstanding: {:?}", summary.outstanding);
            }
        };

        // Do not hold lock here as, currently, `find_sync_peers_inner` can take a while.
        let current_height = self.get_sync_height();

        // Ensure to not exceed the maximum number of outstanding block requests.
        let max_outstanding_block_requests =
            (MAX_BLOCK_REQUESTS as u32) * (DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32);
        let max_total_requests = 4 * max_outstanding_block_requests;
        let max_new_blocks_to_request =
            max_outstanding_block_requests.saturating_sub(self.num_outstanding_block_requests() as u32);

        // Prepare the block requests.
        if self.num_total_block_requests() >= max_total_requests as usize {
            trace!(
                "We are already requested at least {max_total_requests} blocks that have not been fully processed yet. Will not issue more."
            );

            print_requests();

            // Return an empty list of block requests.
            (Default::default(), Default::default())
        } else if max_new_blocks_to_request == 0 {
            trace!(
                "Already reached the maximum number of outstanding blocks ({max_outstanding_block_requests}). Will not issue more."
            );
            print_requests();

            // Return an empty list of block requests.
            (Default::default(), Default::default())
        } else if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers(current_height) {
            // Retrieve the highest block height.
            let greatest_peer_height = sync_peers.values().map(|l| l.latest_locator_height()).max().unwrap_or(0);
            // Update the state of `is_block_synced` for the sync module.
            self.sync_state.write().set_greatest_peer_height(greatest_peer_height);
            // Return the list of block requests.
            (
                self.construct_requests(
                    &sync_peers,
                    current_height,
                    min_common_ancestor,
                    max_new_blocks_to_request,
                    greatest_peer_height,
                ),
                sync_peers,
            )
        } else {
            // Update `is_block_synced` if there are no pending requests or responses.
            if self.requests.read().is_empty() && self.responses.read().is_empty() {
                trace!("All requests have been processed. Will set block synced to true.");
                // Update the state of `is_block_synced` for the sync module.
                self.sync_state.write().set_greatest_peer_height(0);
            } else {
                trace!("No new blocks can be requests, but there are still outstanding requests.");
            }

            // Return an empty list of block requests.
            return Ok(vec![]);
        };

        // Retrieve the highest block height.
        let greatest_peer_height = sync_peers.iter().map(|(_, l)| l.latest_locator_height()).max().unwrap_or(0);
        // Update the state of `is_block_synced` for the sync module.
        self.update_is_block_synced(greatest_peer_height, current_height, MAX_BLOCKS_BEHIND);

        let mut result = vec![];

        // The set of all blocks we are about to request
        let mut queued_requests = HashSet::default();

        for (peer_ip, locators) in sync_peers.iter() {
            let max_height = locators.latest_locator_height();
            let Some(hash) = locators.get_hash(max_height) else {
                bail!("Missing block hash in peer \"{peer_ip}\"'s locator at height {max_height}");
            };

            // First check if we should even create a request
            // (need at least one block that is not know/requested yet).
            if state.block_to_request.contains_key(&hash) {
                continue;
            }

            if queued_requests.contains(&(max_height, hash)) {
                continue;
            }

            for pending_chain in pending_chains.iter() {
                if pending_chain.contains(&hash) {
                    continue;
                }
            }

            // Then, find starting point.
            let mut start_height = max_height;
            let mut start_hash = hash;

            while start_height > ledger_height && start_height > 1 {
                let parent_height = start_height - 1;

                let Some(parent_hash) = locators.get_hash(parent_height) else {
                    bail!("Missing block hash in peer \"{peer_ip}\"'s locator at height {parent_height}");
                };

                let block_id = BlockId { hash: start_hash, height: start_height, previous_hash: parent_hash };

                // Stop if we detect a known ancestor.
                if self.is_previous_block_known(&block_id, &queued_requests)? {
                    break;
                }

                start_height -= 1;
                start_hash = parent_hash;
            }

            // Create request in chunks and ensure we don't exceed the set maximum.
            while start_height < max_height && result.len() < MAX_BLOCK_REQUESTS {
                let max_request_height =
                    (start_height + DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32).min(max_height);
                let req =
                    self.construct_request(start_height, max_request_height, peer_ip, locators, &queued_requests)?;

                // Make sure we don't request the same block again.
                for block in req.blocks.iter() {
                    queued_requests.insert((block.height, block.hash));
                }

                trace!(
                    "Generated new block request to peer \"{}\" for blocks from height {} to {}",
                    req.sync_peer,
                    req.start_height(),
                    req.end_height()
                );

                start_height = req.end_height();
                result.push(req);
            }
        }

        // Can we advance with block locators?
        if block_requests.is_empty() {
            self.fetch_new_block_locators(communication).await;
        }

        Ok(result)
    }

    async fn fetch_new_block_locators<C: CommunicationService>(&self, communication: &C) {
        let mut lock = self.block_heights.lock().await;
        let max_peer_height = *lock.peer_heights.values().map(|(advertised, _)| advertised).max().unwrap_or(&0);
        let ledger_height = self.ledger.latest_block_height();

        // Check if we are synced with current block locators and can advance.
        if lock.sync_height > ledger_height {
            // Not ready yet.
            return;
        }

        let new_sync_height = (lock.sync_height + 100).min(max_peer_height);
        trace!("Moving from sync_height {} to {new_sync_height}", lock.sync_height);

        // The number of peers we successfully request new block locators from.
        let mut count = 0;

        for (peer_ip, (advertised, last_sync)) in lock.peer_heights.iter_mut() {
            if *last_sync < new_sync_height && *advertised > *last_sync {
                let new_sync = new_sync_height.min(*advertised);
                let msg =
                    C::prepare_block_locators_request(new_sync.saturating_sub(NUM_RECENT_BLOCKS as u32), new_sync);

                let Some(fut) = communication.send(*peer_ip, msg).await else {
                    error!("Failed to send message to peer {peer_ip}");
                    continue;
                };

                match fut.await {
                    Ok(_) => {
                        *last_sync = new_sync;
                        count += 1;
                    }
                    Err(err) => {
                        error!("Failed to request block locators: {err}");
                    }
                }
            }
        }

        //TODO (kaimast): can count be zero here, ever?
        if count > 0 {
            debug!("Requested new block locators from {count} peers");
        }

        lock.sync_height = new_sync_height;
    }

    /// Set the sync height to a the given value.
    /// This is a no-op if `new_height` is equal or less to the current sync height.
    pub fn set_sync_height(&self, new_height: u32) {
        self.sync_state.write().set_sync_height(new_height);
    }

    /// Inserts a block request for the given height.
    fn insert_block_request(&self, request: BlockSyncRequest<N>) -> Result<()> {
        let mut state = self.state.write();
        let req_id = request.get_identifier();

        // Check for conflicts (even though we already did this earlier).
        if state.requests.contains_key(&req_id) {
            bail!("Pending request already existed");
        }

        for block_id in &request.blocks {
            if state.block_to_request.contains_key(&block_id.hash) {
                bail!("Request for block {block_id:?} already existed");
            }
        }

        // Now insert.
        for block_id in &request.blocks {
            state.block_to_request.insert(block_id.hash, req_id);
        }

        state.requests.insert(req_id, request);

        Ok(())
    }

    /// Inserts the given block response, after checking that the request exists and the response is well-formed.
    /// On success, this function removes the peer IP from the request sync peers and inserts the response.
    fn insert_block_response(&self, peer_ip: SocketAddr, block: Block<N>) -> Result<()> {
        // Retrieve the block height.
        let height = block.height();
        let mut requests = self.requests.write();

        if self.ledger.contains_block_height(height) {
            bail!("The sync request was removed because we already advanced");
        }

        let Some(entry) = requests.get_mut(&height) else { bail!("The sync pool did not request block {height}") };

        // Retrieve the request entry for the candidate block.
        let (expected_hash, expected_previous_hash, sync_ips) = &entry.request;

        // Ensure the candidate block hash matches the expected hash.
        if let Some(expected_hash) = expected_hash {
            if block.hash() != *expected_hash {
                bail!("The block hash for candidate block {height} from '{peer_ip}' is incorrect")
            }
        }
        // Ensure the previous block hash matches if it exists.
        if let Some(expected_previous_hash) = expected_previous_hash {
            if block.previous_hash() != *expected_previous_hash {
                bail!("The previous block hash in candidate block {height} from '{peer_ip}' is incorrect")
            }
        }
        // Ensure the sync pool requested this block from the given peer.
        if !sync_ips.contains(&peer_ip) {
            bail!("The sync pool did not request block {height} from '{peer_ip}'")
        }

        // Remove the peer IP from the request entry.
        entry.sync_ips_mut().swap_remove(&peer_ip);

        if let Some(existing_block) = &entry.response {
            // If the candidate block was already present, ensure it is the same block.
            if block != *existing_block {
                bail!("Candidate block {height} from '{peer_ip}' is malformed");
            }
        } else {
            entry.response = Some(block.clone());
        }

        // Notify the sync loop that something changed.
        self.notify.notify_one();

        Ok(())
    }

    pub async fn update_peer_block_height(&self, peer_ip: SocketAddr, new_advertised: u32) -> Result<()> {
        let mut lock = self.block_heights.lock().await;

        match lock.peer_heights.entry(peer_ip) {
            hash_map::Entry::Occupied(mut e) => {
                let (last_advertised, last_sync) = e.get();
                ensure!(new_advertised >= *last_advertised, "Peer height cannot decrease!");
                e.insert((new_advertised, *last_sync));
            }
            hash_map::Entry::Vacant(e) => {
                e.insert((new_advertised, 0));
            }
        }
    }

    /// On success, this function removes the peer IP from the requests map.
    /// On failure, this function removes all block requests from the given peer IP.
    fn insert_block_response(&self, peer_ip: SocketAddr, blocks: Vec<Block<N>>) -> Result<()> {
        let Some(first_block) = blocks.first() else {
            bail!("Block response contained no blocks");
        };

        // Build the request identifier
        let start_height = first_block.height();
        let num_blocks = blocks.len() as u32;
        let req_id = (peer_ip, start_height);

        // Ensure the block (response) from the peer is well-formed. On failure, remove all block requests to the peer.
        if let Err(error) = self.check_block_response(&peer_ip, first_block) {
            // Remove all block requests to the peer.
            self.remove_block_requests_to_peer(&peer_ip);
            return Err(error);
        }

        let mut state = self.state.write();

        // Insert the candidate block into the responses map.
        let prev = state.responses.insert(req_id, blocks);

        if prev.is_some() {
            bail!("Already received the same response");

            /* TODO decide what to do here
            // Remove the candidate block.
            responses.remove(&height);
            // Drop the write lock on the responses map.
            drop(responses);
            // Remove all block requests to the peer.
            self.remove_block_requests_to_peer(&peer_ip);
            bail!("Candidate block {height} from '{peer_ip}' is malformed"); */
        }

        trace!(
            "Got block response from \"{peer_ip}\" for blocks from height {start_height} to {}",
            start_height + num_blocks
        );
        Ok(())
    }

    /// For a request, check that it builds on a block that we already know about
    /// or that we are currently requesting.
    /// This will also fail if the ledger already confirmed a previous block with a different hash.
    fn is_previous_block_known(
        &self,
        block_id: &BlockId<N>,
        queued_requests: &HashSet<(u32, N::BlockHash)>,
    ) -> Result<bool> {
        let prev_height = block_id.height.saturating_sub(1);

        // Did the ledger already commit a previous block?
        if let Ok(hash) = self.ledger.get_block_hash(prev_height) {
            if hash == block_id.previous_hash {
                return Ok(true);
            } else {
                //TODO block peer here
                bail!("Previous block hash is incompatible with ledger");
            }
        }

        // Are we about to issue a new request for this?
        if queued_requests.contains(&(prev_height, block_id.previous_hash)) {
            return Ok(true);
        }

        // Did we already issue a request for this?
        if self.state.read().block_to_request.contains_key(&block_id.previous_hash) {
            return Ok(true);
        }

        Ok(false)
    }

    /* TODO is this still needed?
    /// Checks that a block request for the given height does not already exist.
    fn check_block_request(&self, height: u32) -> Result<()> {
        // Ensure the block height is not already in the ledger.
        if self.ledger.contains_block_height(height) {
            bail!("Failed to add block request, as block {height} exists in the ledger");
        }
        // Ensure the block height is not already requested.
        if self.requests.read().contains_key(&height) {
            bail!("Failed to add block request, as block {height} exists in the requests map");
        }

            if self.state.read().block_to_request.contains_key(&block_id.hash) {
                bail!("Failed to add block request, as an identical request already exists in the requests map");
            }

        Ok(())
    }*/

    /// Removes the entire block request for the given height, if it exists.
    fn remove_block_request(&self, height: u32) {
        // Remove the request entry for the given height.
        self.requests.write().remove(&height);
    }

    /// Removes the block request and response for the given height
    /// This may only be called after `peek_next_block`, which checked if the request for the given height was complete.
    ///
    /// Precondition: This may only be called after `peek_next_block` has returned `Some`,
    /// which has checked if the request for the given height is complete
    /// and there is a block with the given `height` in the `responses` map.
    pub fn remove_block_response(&self, height: u32) {
        // Remove the request entry for the given height.
        if let Some(e) = self.requests.write().remove(&height) {
            trace!("Block request for height {height} was completed in {}ms", e.timestamp.elapsed().as_millis());
        }
    }

    /// Checks the given block (response) from a peer against the expected block hash and previous block hash.
    ///
    /// Postcondition: If this function returns `Ok`, then `self.requests` has `height` as a key.
    fn check_block_response(&self, peer_ip: &SocketAddr, block: &Block<N>) -> Result<()> {
        // Retrieve the block height.
        let height = block.height();

        // Retrieve the request entry for the candidate block.
        if let Some(request) = self.state.read().requests.get(&(*peer_ip, block.height())) {
            let Some(request_previous_hash) = request.previous_block_hash() else {
                bail!("Request is empty");
            };

            // Ensure the previous block hash matches if it exists.
            if block.previous_hash() != request_previous_hash {
                bail!("The previous block hash in candidate block {height} from '{peer_ip}' is incorrect")
            }
            // Ensure the sync pool requested this block from the given peer.
            if request.sync_peer != *peer_ip {
                bail!("The sync pool did not request block {height} from '{peer_ip}'")
            }
            return Ok(());
        } else if self.ledger.contains_block_height(height) {
            bail!("The sync request was removed because we already advanced")
        }

        bail!("The sync pool did not request block {height}")
    }

    /// Removes all block requests for the given peer IP.
    ///
    /// This is used when disconnecting from a peer or when a peer sends invalid block responses.
    fn remove_block_requests_to_peer(&self, peer_ip: &SocketAddr) {
        trace!("Block sync is removing all block requests to peer {peer_ip}...");

        // Remove the peer IP from the requests map. If any request entry is now empty,
        // and its corresponding response entry is also empty, then remove that request entry altogether.
        self.requests.write().retain(|height, e| {
            let had_peer = e.sync_ips_mut().swap_remove(peer_ip);

            // Only remove requests that were sent to this peer, that have no other peer that can respond instead,
            // and that were not completed yet.
            let retain = !had_peer || !e.sync_ips().is_empty() || e.response.is_some();
            if !retain {
                trace!("Removed block request timestamp for {peer_ip} at height {height}");
            }
            !remove
        });

        // No need to remove responses here, because requests with responses will be retained.
    }

    /// Removes block requests that have timed out, i.e, requests we sent that did not receive a response in time, and requests that are obsolete, i.e., the ledger already has all corresponding blocks.
    ///
    /// This removes the corresponding block responses and returns the set of peers/addresses that timed out.
    /// It will ask the communication service to ban any timed-out peers.
    ///
    /// Finally, it will return a set of new of block requests that replaced the timed-out requests (if needed).
    pub fn handle_block_request_timeouts<C: CommunicationService>(
        &self,
        communication: &C,
    ) -> Option<BlockRequestBatch<N>> {
        // Acquire the write lock on the requests map.
        let mut requests = self.requests.write();

        // Retrieve the current time.
        let now = Instant::now();

        // Retrieve the current block height
        let current_height = self.ledger.latest_block_height();

        // Track the number of timed out block requests (only used to print a log message).
        let mut timed_out_requests = vec![];

        // Track which peers should be banned due to unresponsiveness.
        let mut peers_to_ban: HashSet<SocketAddr> = HashSet::new();
        let mut removed_requests = HashSet::new();

        // Remove timed out block requests.
        requests.retain(|height, e| {
            let is_obsolete = *height <= current_height;
            // Determine if the duration since the request timestamp has exceeded the request timeout.
            let timer_elapsed = now.duration_since(e.timestamp) > BLOCK_REQUEST_TIMEOUT;
            // Determine if the request is incomplete.
            let is_complete = e.sync_ips().is_empty();

            // Determine if the request has timed out.
            let is_timeout = timer_elapsed && !is_complete;

            // Retain if this is not a timeout and is not obsolete.
            let retain = !is_timeout && !is_obsolete;

            if is_timeout {
                trace!("Block request at height {height} has timed out: timer_elapsed={timer_elapsed}, is_complete={is_complete}, is_obsolete={is_obsolete}");

                // Increment the number of timed out block requests.
                timed_out_requests.push(*height);
            } else if is_obsolete {
                trace!("Block request at height {height} became obsolete (current_height={current_height})");
            }

            // If the request timed out, also remove and ban given peer.
            if is_timeout {
                for peer_ip in e.sync_ips().iter() {
                    peers_to_ban.insert(*peer_ip);
                }
            }

            retain
        });

        self.block_to_request.retain(|_, req_id| !removed_requests.contains(req_id));

        if !timed_out_requests.is_empty() {
            debug!("{num} block requests timed out", num = timed_out_requests.len());
        }

        let next_request_height = requests.iter().next().map(|(h, _)| *h);

        // Avoid locking `locators` and `requests` at the same time.
        drop(requests);

        // Now remove and ban any unresponsive peers
        for peer_ip in peers_to_ban {
            self.remove_peer(&peer_ip);
            communication.ban_peer(peer_ip);
        }

        // Re-issue any timed-out requests.
        //
        // Do this even if timed_out_requests is empty, because we might not be able to re-issue
        // requests immediately if there are no other peers at a given time.
        // Further, this only closes the first gap. So multiple calls to this might be needed.
        let sync_height = self.get_sync_height();
        if let Some(next_height) = next_request_height {
            let start = sync_height + 1;

            // Is there a gap?
            if next_height > start {
                // Only request the given range, so there are no overlaps with other requests.
                let end = next_height; // exclusive
                let max_new_blocks_to_request = end - start;

                let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(start) else {
                    warn!("Block requests timed out, but found no other peers to re-request from");
                    return None;
                };

                // Retrieve the highest block height.
                let greatest_peer_height = sync_peers.values().map(|l| l.latest_locator_height()).max().unwrap_or(0);

                debug!("Re-requesting blocks starting at height {start}");

                return Some((
                    self.construct_requests(
                        &sync_peers,
                        sync_height,
                        min_common_ancestor,
                        max_new_blocks_to_request,
                        greatest_peer_height,
                    ),
                    sync_peers,
                ));
            }
        }

        None
    }

    /// Returns the sync peers and their minimum common ancestor, if the node needs to sync.
    fn find_sync_peers_inner(&self, current_height: u32) -> Vec<(SocketAddr, BlockLocators<N>)> {
        // Pick a set of peers above the latest ledger height, and include their locators.
        // This will sort the peers by locator height in descending order.
        let mut sync_peers: Vec<_> = self
            .locators
            .read()
            .iter()
            .filter(|(_, locators)| locators.latest_locator_height() > current_height)
            .sorted_by_key(|(_, a)| a.latest_locator_height())
            .take(NUM_SYNC_CANDIDATE_PEERS)
            .map(|(peer_ip, locators)| (*peer_ip, locators.clone()))
            .collect();

        // Case 0: If there are no candidate peers, return `None`.
        if candidate_locators.is_empty() {
            trace!("Found no sync peers with height greater {current_height}");
            return None;
        }

        // Ensure we don't sync from the same peers in the same order every time.
        sync_peers.shuffle(&mut rand::thread_rng());

        sync_peers
    }

    // Given the sync peers, return a list of block requests.
    fn construct_request(
        &self,
        start_height: u32,
        max_height: u32,
        sync_peer: &SocketAddr,
        peer_locators: &BlockLocators<N>,
        _queued_requests: &HashSet<(u32, N::BlockHash)>,
    ) {
        // Compute the end height for the block request.
        // TODO restore this
        let _max_blocks_to_request = MAX_BLOCK_REQUESTS as u32 * DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32;
        // We can only fetch/verfiy blocks within GC
        let end_height = (start_height + BatchHeader::<N>::MAX_GC_ROUNDS as u32).min(max_height);

        if end_height <= start_height {
            bail!("Invalid range for blcok request");
        }

        // Construct the block hashes to request.
        let mut blocks = vec![];

        for height in start_height..=end_height {
            let Some(hash) = peer_locators.get_hash(height) else {
                bail!("Missing hash in block locator");
            };

            let Some(previous_hash) = peer_locators.get_hash(height.saturating_sub(1)) else {
                bail!("Missing previous block hash");
            };

            let block_id = BlockId { height, previous_hash, hash };
            blocks.push(block_id);
        }

        Ok(BlockSyncRequest { sync_peer: *sync_peer, blocks, timestamp: Instant::now() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::locators::{
        NUM_RECENT_BLOCKS,
        test_helpers::{
            sample_block_hash,
            sample_block_locators,
            sample_block_locators_with_fork,
            sample_forked_block_hash,
        },
    };

    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_sync_communication_service::test_helpers::DummyCommunicationService;
    use snarkvm::{
        ledger::committee::Committee,
        prelude::{Field, TestRng},
    };

    use indexmap::{IndexSet, indexset};
    use rand::Rng;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU32, Ordering},
        },
    };

    use snarkos_node_sync_communication_service::test_helpers::DummyCommunicationService;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Returns the peer IP for the sync pool.
    fn sample_peer_ip(id: u16) -> SocketAddr {
        assert_ne!(id, 0, "The peer ID must not be 0 (reserved for local IP in testing)");
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), id)
    }

    /// Returns a sample committee.
    fn sample_committee() -> Committee<CurrentNetwork> {
        let rng = &mut TestRng::default();
        snarkvm::ledger::committee::test_helpers::sample_committee(rng)
    }

    /// Returns the ledger service, initialized to the given height.
    fn sample_ledger_service(height: u32) -> MockLedgerService<CurrentNetwork> {
        MockLedgerService::new_at_height(sample_committee(), height)
    }

    /// Returns the sync pool, with the ledger initialized to the given height.
    fn sample_sync_at_height(height: u32) -> BlockSync<CurrentNetwork> {
        BlockSync::<CurrentNetwork>::new(Arc::new(sample_ledger_service(height)))
    }

    /// Returns a vector of randomly sampled block heights in [0, max_height].
    ///
    /// The maximum value will always be included in the result.
    fn generate_block_heights(max_height: u32, num_values: usize) -> Vec<u32> {
        assert!(num_values > 0, "Cannot generate an empty vector");
        assert!((max_height as usize) >= num_values);

        let mut rng = TestRng::default();

        let mut heights: Vec<u32> = (0..(max_height - 1)).choose_multiple(&mut rng, num_values);

        heights.push(max_height);

        heights
    }

    /// Returns a duplicate (deep copy) of the sync pool with a different ledger height.
    fn duplicate_sync_at_new_height(sync: &BlockSync<CurrentNetwork>, height: u32) -> BlockSync<CurrentNetwork> {
        BlockSync::<CurrentNetwork> {
            notify: Notify::new(),
            block_heights: sync.block_heights.clone(),
            ledger: Arc::new(sample_ledger_service(height)),
            locators: RwLock::new(sync.locators.read().clone()),
            common_ancestors: RwLock::new(sync.common_ancestors.read().clone()),
            requests: RwLock::new(sync.requests.read().clone()),
            sync_state: RwLock::new(sync.sync_state.read().clone()),
            advance_with_sync_blocks_lock: Default::default(),
        }
    }

    /// Checks that the sync pool (starting at genesis) returns the correct requests.
    fn check_prepare_block_requests(sync: BlockSync<CurrentNetwork>, peers: IndexSet<SocketAddr>) {
        // Check test assumptions are met.
        assert_eq!(sync.ledger.latest_block_height(), 0, "This test assumes the sync pool is at genesis");

        // Determine the number of peers within range of this sync pool.
        let _num_peers_within_recent_range_of_ledger =
            peers.iter().filter(|peer_ip| sync.get_peer_height(peer_ip).unwrap() < NUM_RECENT_BLOCKS as u32).count();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests().unwrap();

        // If there are no peers, then there should be no requests.
        if peers.is_empty() {
            assert!(requests.is_empty());
            return;
        }

        // Otherwise, there should be requests.
        assert!(requests.len() <= MAX_BLOCK_REQUESTS);

        let mut ids = HashSet::new();

        for request in requests.into_iter() {
            for block_id in request.blocks {
                let is_new = ids.insert(block_id.clone());
                assert!(is_new, "Duplicate request for the same block");

                // assert_eq!(block_id.height, 1 + idx as u32);
                assert_eq!(block_id.hash, sample_block_hash(block_id.height));
                assert_eq!(block_id.previous_hash, sample_block_hash(block_id.height - 1));
            }

            /* TODO
            if num_peers_within_recent_range_of_ledger >= REDUNDANCY_FACTOR {
                assert_eq!(sync_peers.len(), 1);
            } else {
                assert_eq!(sync_peers.len(), num_peers_within_recent_range_of_ledger);
                assert_eq!(sync_peers, peers);
            }*/
        }
    }

    /// Tests that height and hash values are set correctly using many different maximum block heights.
    #[test]
    fn test_latest_block_height() {
        for height in generate_block_heights(100_001, 5000) {
            let sync = sample_sync_at_height(height);
            // Check that the latest block height is the maximum height.
            assert_eq!(sync.ledger.latest_block_height(), height);
        }
    }

    #[test]
    fn test_get_block_height() {
        for height in generate_block_heights(100_001, 5000) {
            let sync = sample_sync_at_height(height);
            assert_eq!(sync.ledger.get_block_height(&(Field::<CurrentNetwork>::from_u32(0)).into()).unwrap(), 0);
            assert_eq!(
                sync.ledger.get_block_height(&(Field::<CurrentNetwork>::from_u32(height)).into()).unwrap(),
                height
            );
        }
    }

    #[test]
    fn test_get_block_hash() {
        for height in generate_block_heights(100_001, 5000) {
            let sync = sample_sync_at_height(height);
            assert_eq!(sync.ledger.get_block_hash(0).unwrap(), (Field::<CurrentNetwork>::from_u32(0)).into());
            assert_eq!(sync.ledger.get_block_hash(height).unwrap(), (Field::<CurrentNetwork>::from_u32(height)).into());
        }
    }

    #[tokio::test]
    async fn test_prepare_block_requests() {
        for num_peers in 0..111 {
            println!("Testing with {num_peers} peers");

            let sync = sample_sync_at_height(0);

            let mut peers = indexset![];

            for peer_id in 1..=num_peers {
                // Add a peer.
                sync.update_peer_block_locators(sample_peer_ip(peer_id), sample_block_locators(10)).unwrap();
                // Add the peer to the set of peers.
                peers.insert(sample_peer_ip(peer_id));
            }

            // If all peers are ahead, then requests should be prepared.
            let comm = DummyCommunicationService::default();
            check_prepare_block_requests(&comm, sync, peers);
        }
    }

    #[tokio::test]
    async fn test_prepare_block_requests_with_leading_fork_at_11() {
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is above peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they don't even realize that peer 1 is on a fork.
        // Thus, you can sync up to block 10 from any of the 3 peers.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 11,
        // then the sync pool should request blocks 1..=10 from the NUM_REDUNDANCY peers.
        // This is safe because the leading fork is at 11, and the sync pool is at 0,
        // so all candidate peers are at least 10 blocks ahead of the sync pool.

        const MAX_HEIGHT: u32 = 20;
        const FORK_HEIGHT: u32 = 11;

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, sample_block_locators_with_fork(MAX_HEIGHT, FORK_HEIGHT)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, sample_block_locators(FORK_HEIGHT - 1)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, sample_block_locators(FORK_HEIGHT - 1)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests().unwrap();

        assert!(requests.len() > 1);

        // Check the requests.
        let mut total_num_blocks = 0;
        //
        for request in requests.into_iter() {
            total_num_blocks += request.blocks.len() as u32;

            for block_id in request.blocks {
                if block_id.height >= FORK_HEIGHT {
                    assert_eq!(block_id.hash, sample_forked_block_hash(block_id.height));
                } else {
                    assert_eq!(block_id.hash, sample_block_hash(block_id.height));
                }

                if block_id.height > FORK_HEIGHT {
                    assert_eq!(block_id.previous_hash, sample_forked_block_hash(block_id.height - 1));
                } else {
                    assert_eq!(block_id.previous_hash, sample_block_hash(block_id.height - 1));
                }

                //TODO assert_eq!(sync_peers.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
            }
        }

        // Make sure all blocks are requested.
        assert_eq!(MAX_HEIGHT, total_num_blocks);
    }

    #[tokio::test]
    async fn test_prepare_block_requests_with_leading_fork_at_10() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is at peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they recognize that peer 1 has forked.
        // Thus, you don't have NUM_REDUNDANCY peers to sync to block 10.
        //
        // Now, while you could in theory sync up to block 9 from any of the 3 peers,
        // we choose not to do this as either side is likely to disconnect from us,
        // and we would rather wait for enough redundant peers before syncing.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 10,
        // then the sync pool should not request blocks as 1 peer conflicts with the other NUM_REDUNDANCY-1 peers.
        // We choose to sync with a cohort of peers that are *consistent* with each other,
        // and prioritize from descending heights (so the highest peer gets priority).

        const FORK_HEIGHT: u32 = 10;
        const MAX_HEIGHT: u32 = 20;

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, sample_block_locators_with_fork(MAX_HEIGHT, FORK_HEIGHT)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, sample_block_locators(FORK_HEIGHT)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, sample_block_locators(FORK_HEIGHT)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests().unwrap();

        // There is a duplicate block at FORK_HEIGHT.
        assert!(requests.len() > 1);

        // Add a peer.
        let peer_4 = sample_peer_ip(4);
        sync.update_peer_block_locators(peer_4, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, sync_peers) = sync.prepare_block_requests(&comm).await;
        assert_eq!(requests.len(), 10);

        let mut total_num_blocks = 0;

        // Check the requests.
        for request in requests.into_iter() {
            total_num_blocks += request.blocks.len() as u32;

            for block_id in request.blocks {
                match block_id.height.cmp(&FORK_HEIGHT) {
                    std::cmp::Ordering::Less => {
                        assert_eq!(block_id.hash, sample_block_hash(block_id.height));
                    }
                    std::cmp::Ordering::Greater => {
                        assert_eq!(request.sync_peer, peer_1);
                        assert_eq!(block_id.hash, sample_forked_block_hash(block_id.height));
                    }
                    std::cmp::Ordering::Equal => {
                        if request.sync_peer == peer_1 {
                            assert_eq!(block_id.hash, sample_forked_block_hash(block_id.height));
                        } else {
                            assert_eq!(block_id.hash, sample_block_hash(block_id.height));
                        }
                    }
                }

                if block_id.height > FORK_HEIGHT {
                    assert_eq!(block_id.previous_hash, sample_forked_block_hash(block_id.height - 1));
                } else {
                    assert_eq!(block_id.previous_hash, sample_block_hash(block_id.height - 1));
                }

                //TODO assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
                //TODO assert_ne!(sync_ips[0], peer_1); // It should never be the forked peer.
            }
        }

        // Make sure all blocks are requested.
        // (There are two blocks at FORK_HEIGHT)
        assert_eq!(MAX_HEIGHT + 1, total_num_blocks);
    }

    /*
        #[tokio::test]
        async fn test_prepare_block_requests_with_trailing_fork_at_9() {
            let rng = &mut TestRng::default();
        #[test]
        fn test_prepare_block_requests_with_trailing_fork_at_9() {
    >>>>>>> 6303b86fb (Redesign BlockSync to fully verify blockchain)
            let sync = sample_sync_at_height(0);

            // Peer 1 and 2 diverge from peer 3 at block 10.
            const MAX_HEIGHT: usize = 20;
            const FORK_HEIGHT: usize = 10;

            // Add a peer.
            let peer_1 = sample_peer_ip(1);
    <<<<<<< HEAD
            sync.update_peer_block_locators(peer_1, sample_block_locators(10)).unwrap();

            // Add a peer.
            let peer_2 = sample_peer_ip(2);
            sync.update_peer_block_locators(peer_2, sample_block_locators(10)).unwrap();
    =======
            sync.update_peer_locators(peer_1, sample_block_locators(MAX_HEIGHT)).unwrap();

            // Add a peer.
            let peer_2 = sample_peer_ip(2);
            sync.update_peer_locators(peer_2, sample_block_locators(MAX_HEIGHT)).unwrap();
    >>>>>>> 6303b86fb (Redesign BlockSync to fully verify blockchain)

            // Add a peer (fork).
            let peer_3 = sample_peer_ip(3);
    <<<<<<< HEAD
            sync.update_peer_block_locators(peer_3, sample_block_locators_with_fork(20, 10)).unwrap();

            // Prepare the block requests.
            let comm = DummyCommunicationService::default();
            let (requests, _) = sync.prepare_block_requests(&comm).await;
            assert_eq!(requests.len(), 0);

            // When there are NUM_REDUNDANCY+1 peers ahead, and peer 3 is on a fork, then there should be block requests.
    =======
            sync.update_peer_locators(peer_3, sample_block_locators_with_fork(MAX_HEIGHT, FORK_HEIGHT)).unwrap();
    >>>>>>> 6303b86fb (Redesign BlockSync to fully verify blockchain)

            // Add a peer.
            let peer_4 = sample_peer_ip(4);
            sync.update_peer_block_locators(peer_4, sample_block_locators(10)).unwrap();

            // Prepare the block requests.
    <<<<<<< HEAD
            let (requests, sync_peers) = sync.prepare_block_requests(&comm).await;
            assert_eq!(requests.len(), 10);
    =======
            let requests = sync.prepare_block_requests();
            assert!(requests.len() > 0);
    >>>>>>> 6303b86fb (Redesign BlockSync to fully verify blockchain)

            // Check the requests.
            for (idx, (block_id, sync_ips)) in requests.into_iter().enumerate() {
                // Construct the sync IPs.
                assert_eq!(block_id.height, 1 + idx as u32);
                assert_eq!(block_id.block_hash, Field::<CurrentNetwork>::from_u32(block_id.height).into());
                assert_eq!(block_id.previous_block_hash, Field::<CurrentNetwork>::from_u32(block_id.height - 1).into());
                assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
                assert_ne!(sync_ips[0], peer_3); // It should never be the forked peer.
            }
        }*/

    #[tokio::test]
    async fn test_insert_block_requests() {
        let rng = &mut TestRng::default();

        const LOCATOR_HEIGHT: u32 = 14;
        const START_HEIGHT: u32 = 0;

        // Ensure there will be more than one request.
        assert!(LOCATOR_HEIGHT > DataBlocks::<CurrentNetwork>::MAXIMUM_NUMBER_OF_BLOCKS as u32);

        let sync = sample_sync_at_height(START_HEIGHT);

        // Add a peer.
        sync.update_peer_locators(sample_peer_ip(1), sample_block_locators(LOCATOR_HEIGHT)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests().unwrap();
        assert!(requests.len() > 1);

        for request in requests.clone() {
            let req_id = request.get_identifier();
            let sync_peer = request.sync_peer;

            // Insert the block request.
            sync.insert_block_request(request).unwrap();
            // Check that the block requests were inserted.
            let request = sync.get_block_request(&req_id).unwrap();
            assert_eq!(request.sync_peer, sync_peer);
        }

        for request in requests.clone() {
            let req_id = request.get_identifier();
            let sync_peer = request.sync_peer;

            // Check that the block requests are still inserted.
            let request = sync.get_block_request(&req_id).unwrap();
            assert_eq!(request.sync_peer, sync_peer);
        }

        for request in requests {
            let req_id = request.get_identifier();
            let sync_peer = request.sync_peer;

            // Check that block requests cannot be inserted twice.
            assert!(sync.insert_block_request(request).is_err());
            // Check that the block requests are still inserted.
            let request = sync.get_block_request(&req_id).unwrap();
            assert_eq!(request.sync_peer, sync_peer);
        }
    }

    /* TODO
    #[test]
    fn test_insert_block_requests_fails() {
        let sync = sample_sync_at_height(9);

        // Add a peer.
        sync.update_peer_block_locators(sample_peer_ip(1), sample_block_locators(10)).unwrap();

        // Inserting a block height that is already in the ledger should fail.
        sync.insert_block_request(9, (None, None, indexset![sample_peer_ip(1)])).unwrap_err();
        // Inserting a block height that is not in the ledger should succeed.
        sync.insert_block_request(10, (None, None, indexset![sample_peer_ip(1)])).unwrap();
    }*/

    /* TODO
        #[test]
        fn test_update_peer_locators() {
            let sync = sample_sync_at_height(0);

    <<<<<<< HEAD
            // Test 2 peers.
            let peer1_ip = sample_peer_ip(1);
            for peer1_height in 0..500u32 {
                sync.update_peer_block_locators(peer1_ip, sample_block_locators(peer1_height)).unwrap();
                assert_eq!(sync.get_peer_height(&peer1_ip), Some(peer1_height));

                let peer2_ip = sample_peer_ip(2);
                for peer2_height in 0..500u32 {
                    println!("Testing peer 1 height at {peer1_height} and peer 2 height at {peer2_height}");

                    sync.update_peer_block_locators(peer2_ip, sample_block_locators(peer2_height)).unwrap();
                    assert_eq!(sync.get_peer_height(&peer2_ip), Some(peer2_height));

                    // Compute the distance between the peers.
                    let distance = peer1_height.abs_diff(peer2_height);

                    // Check the common ancestor.
                    if distance < NUM_RECENT_BLOCKS as u32 {
                        let expected_ancestor = core::cmp::min(peer1_height, peer2_height);
                        assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                        assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                    } else {
                        let min_checkpoints =
                            core::cmp::min(peer1_height / CHECKPOINT_INTERVAL, peer2_height / CHECKPOINT_INTERVAL);
                        let expected_ancestor = min_checkpoints * CHECKPOINT_INTERVAL;
                        assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                        assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                    }
                }
    =======
            let peer_ip = sample_peer_ip(1);
            for peer_height in 0..500u32 {
                sync.update_peer_locators(peer_ip, sample_block_locators(peer_height)).unwrap();
                assert_eq!(sync.get_peer_height(&peer_ip), Some(peer_height));
    >>>>>>> 6303b86fb (Redesign BlockSync to fully verify blockchain)
            }
        }*/

    #[test]
    fn test_remove_peer() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_block_locators(peer_ip, sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_block_locators(peer_ip, sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);
    }

    #[test]
    fn test_locators_insert_remove_insert() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_block_locators(peer_ip, sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_block_locators(peer_ip, sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));
    }

    #[tokio::test]
    async fn test_requests_insert_remove_insert() {
        let rng = &mut TestRng::default();
        let peer_height = 3;

        // Ensure all blocks fit in  a single request.
        assert!(peer_height <= DataBlocks::<CurrentNetwork>::MAXIMUM_NUMBER_OF_BLOCKS as usize);

        let sync = sample_sync_at_height(0);

        // Add a peer.
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_block_locators(peer_ip, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        assert_eq!(requests.len(), 10);
        sync.update_peer_locators(peer_ip, sample_block_locators(peer_height as u32)).unwrap();

        // Prepare the block requests.
        let comm = DummyCommunicationService::default();
        let requests = sync.prepare_block_requests(&comm).unwrap();
        assert_eq!(requests.len(), 1);

        for request in requests.clone() {
            assert_eq!(request.start_height(), 1);
            assert_eq!(request.blocks.len(), peer_height);

            // Insert the block request.
            sync.insert_block_request(request.clone()).unwrap();
            // Check that the block requests were inserted.
            let stored_request = sync.get_block_request(&request.get_identifier()).unwrap();
            assert_eq!(request.sync_peer, stored_request.sync_peer);
        }

        // Remove the peer.
        sync.remove_peer(&peer_ip);

        for request in requests {
            // Check that the block requests were removed.
            assert!(sync.get_block_request(&request.get_identifier()).is_none());
        }

        // As there is no peer, it should not be possible to prepare block requests.
        let requests = sync.prepare_block_requests().unwrap();
        assert_eq!(requests.len(), 0);

        // Add the peer again.
        sync.update_peer_locators(peer_ip, sample_block_locators(peer_height as u32)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests().unwrap();
        assert_eq!(requests.len(), 1);

        let request = requests.first().unwrap();

        assert_eq!(request.start_height(), 1);
        assert_eq!(request.blocks.len(), peer_height);
        // Insert the block request.
        sync.insert_block_request(request.clone()).unwrap();
        // Check that the block requests were inserted.
        let stored_request = sync.get_block_request(&request.get_identifier()).unwrap();
        assert_eq!(request.sync_peer, stored_request.sync_peer);
    }

    #[tokio::test]
    async fn test_obsolete_block_requests() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Set the height to some multiple of the maximum message size.i
        let min_height = 2 * DataBlocks::<CurrentNetwork>::MAXIMUM_NUMBER_OF_BLOCKS as u32;
        let max_height = 21 * DataBlocks::<CurrentNetwork>::MAXIMUM_NUMBER_OF_BLOCKS as u32;

        let locator_height = rng.gen_range(min_height..max_height);

        // Add a peer.
        let locators = sample_block_locators(locator_height);
        sync.update_peer_block_locators(sample_peer_ip(1), locators.clone()).unwrap();

        // Construct block requests
        let comm = DummyCommunicationService::default();
        let requests = sync.prepare_block_requests().unwrap();

        // The blocks cannot fit in a single request.
        assert!(requests.len() as u32 > 1);

        // Add the block requests to the sync module.
        for request in requests.clone() {
            // Insert the block request.
            sync.insert_block_request(request.clone()).unwrap();
            // Check that the block requests were inserted.
            let stored_request = sync.get_block_request(&request.get_identifier()).unwrap();
            assert_eq!(stored_request.sync_peer, request.sync_peer);
        }

        // Duplicate a new sync module with a different height to simulate block advancement.
        // This range needs to be inclusive, so that the range is never empty,
        // even with a locator height of 0.
        let ledger_height = rng.gen_range(0..=locator_height);
        let new_sync = duplicate_sync_at_new_height(&sync, ledger_height);

        // Check that the number of requests is the same.
        assert_eq!(new_sync.state.read().requests.len(), requests.len());

        // Remove timed out block requests.
        let c = DummyCommunicationService::default();
        new_sync.handle_block_request_timeouts(&c);

        // Check that the number of requests is reduced based on the ledger height.
        for (_, request) in new_sync.state.read().requests.iter() {
            assert!(request.end_height() > ledger_height);
        }
    }

    #[test]
    fn test_timed_out_block_request() {
        let sync = sample_sync_at_height(0);
        let peer_ip = sample_peer_ip(1);
        let locators = sample_block_locators(10);
        let block_hash = locators.get_hash(1);

        sync.update_peer_locators(peer_ip, locators.clone()).unwrap();

        let timestamp = Instant::now() - BLOCK_REQUEST_TIMEOUT - Duration::from_secs(1);

        // Add a timed-out request
        sync.requests.write().insert(1, OutstandingRequest {
            request: (block_hash, None, [peer_ip].into()),
            timestamp,
            response: None,
        });

        assert_eq!(sync.requests.read().len(), 1);
        assert_eq!(sync.locators.read().len(), 1);

        // Remove timed out block requests.
        let c = DummyCommunicationService::default();
        sync.handle_block_request_timeouts(&c);

        let ban_list = c.peers_to_ban.lock();
        assert_eq!(ban_list.len(), 1);
        assert_eq!(ban_list.iter().next(), Some(&peer_ip));

        assert!(sync.requests.read().is_empty());
        assert!(sync.locators.read().is_empty());
    }

    #[test]
    fn test_reissue_timed_out_block_request() {
        let sync = sample_sync_at_height(0);
        let peer_ip1 = sample_peer_ip(1);
        let peer_ip2 = sample_peer_ip(2);
        let peer_ip3 = sample_peer_ip(3);

        let locators = sample_block_locators(10);
        let block_hash1 = locators.get_hash(1);
        let block_hash2 = locators.get_hash(2);

        sync.update_peer_locators(peer_ip1, locators.clone()).unwrap();
        sync.update_peer_locators(peer_ip2, locators.clone()).unwrap();
        sync.update_peer_locators(peer_ip3, locators.clone()).unwrap();

        assert_eq!(sync.locators.read().len(), 3);

        let timestamp = Instant::now() - BLOCK_REQUEST_TIMEOUT - Duration::from_secs(1);

        // Add a timed-out request
        sync.requests.write().insert(1, OutstandingRequest {
            request: (block_hash1, None, [peer_ip1].into()),
            timestamp,
            response: None,
        });

        // Add a timed-out request
        sync.requests.write().insert(2, OutstandingRequest {
            request: (block_hash2, None, [peer_ip2].into()),
            timestamp: Instant::now(),
            response: None,
        });

        assert_eq!(sync.requests.read().len(), 2);

        // Remove timed out block requests.
        let c = DummyCommunicationService::default();
        let re_requests = sync.handle_block_request_timeouts(&c);

        let ban_list = c.peers_to_ban.lock();
        assert_eq!(ban_list.len(), 1);
        assert_eq!(ban_list.iter().next(), Some(&peer_ip1));

        assert_eq!(sync.requests.read().len(), 1);
        assert_eq!(sync.locators.read().len(), 2);

        let (new_requests, new_sync_ips) = re_requests.unwrap();
        assert_eq!(new_requests.len(), 1);

        let (height, (hash, _, _)) = new_requests.first().unwrap();
        assert_eq!(*height, 1);
        assert_eq!(*hash, block_hash1);
        assert_eq!(new_sync_ips.len(), 2);

        // Make sure the removed peer is not in the sync_peer set.
        let mut iter = new_sync_ips.iter();
        assert_ne!(iter.next().unwrap().0, &peer_ip1);
        assert_ne!(iter.next().unwrap().0, &peer_ip1);
    }
}
