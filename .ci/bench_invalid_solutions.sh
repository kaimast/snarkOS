#!/bin/bash

#################################################################
# Benchmarks how many invalid solutions per second a node can
# reject (provers issue many invalid solutions; we measure
# rejection throughput via REST with check_solution=true).
# Use --baseline to run without issuing invalid solutions (metrics only, for comparison).
# Use --valid-solutions to test accept-until-limit: build with accept_any_solution,
# POST solutions until prover hits per-epoch limit (accepted then 422).
#################################################################

set -eo pipefail

# Option: run without invalid solution load to get a baseline metrics CSV
skip_invalid_txs=false
# Option: build/run with accept_any_solution and count accepted vs limit rejections
valid_solutions=false
for arg in "$@"; do
  if [ "$arg" = "--baseline" ] || [ "$arg" = "--no-invalid-txs" ]; then
    skip_invalid_txs=true
  elif [ "$arg" = "--valid-solutions" ]; then
    valid_solutions=true
  fi
done

network_id=1
num_validators=4
min_height=50
# Issue fewer solutions so workers don't hit verification limit
num_workers=2
reqs_per_worker=25
# Metrics port for validator 0 (validator i uses base_metrics_port + i)
base_metrics_port=9000
max_wait_height=600
poll_interval=5

log_filter="info,snarkos_node_sync=warn,snarkos_node_tcp=warn,snarkos_node_rest=warn,snarkos_node_bft=warn"

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

init_log_dir

branch_name=$(git rev-parse --abbrev-ref HEAD)
log "On branch: ${branch_name}"

network_name=$(get_network_name $network_id)
log "Using network: $network_name (ID: $network_id)"

if [ -f info.txt ]; then
  snapshot_info=$(<info.txt)
  log "Snapshot_info: ${snapshot_info}"
else
  snapshot_info=""
fi

trap stop_nodes EXIT
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# When --valid-solutions: build snarkos with accept_any_solution and use that binary
snarkos_cmd="snarkos"
if $valid_solutions; then
  log "Building snarkos with --features accept_any_solution,disable_solution_rate_limit..."
  if ! cargo build --release -p snarkos --features accept_any_solution,disable_solution_rate_limit --quiet 2>"$log_dir/build_snarkos.err"; then
    log "Build failed. Ensure root Cargo.toml has accept_any_solution and disable_solution_rate_limit for snarkos-node."
    cat "$log_dir/build_snarkos.err" 1>&2
    exit 1
  fi
  snarkos_cmd="./target/release/snarkos"
  log "Using $snarkos_cmd for validators"
fi

common_flags=(
  --nodisplay --nobanner --noupdater
  "--log-filter=$log_filter"
  "--network=$network_id"
  --nocdn
  "--dev-num-validators=$num_validators"
  "--dev-num-clients=0"
  --no-dev-txs
  "--rest-rps=1000000"
)

# Build trusted validators list for BFT (all four so each can connect to the others).
validators=""
for i in $(seq 0 $((num_validators - 1))); do
  if [ -n "$validators" ]; then validators="$validators,"; fi
  validators="${validators}127.0.0.1:$((5000 + i))"
done

# Start full committee of 4 validators so the network can produce blocks.
for validator_index in $(seq 0 $((num_validators - 1))); do
  $snarkos_cmd clean "--dev=$validator_index" "--network=$network_id" 2>/dev/null || true

  metrics_ip="127.0.0.1:$((base_metrics_port + validator_index))"
  rest_port=$((3030 + validator_index))
  log_file="$log_dir/validator-$validator_index.log"

  if (( validator_index == 0 )); then
    # shellcheck disable=SC2086
    run_with_prefix "validator-$validator_index" $TASKSET1 $snarkos_cmd start "--dev=$validator_index" --validator \
      "${common_flags[@]}" "--validators=$validators" --metrics --metrics-ip "$metrics_ip" \
      "--logfile=$log_file" "--rest=127.0.0.1:$rest_port"
  else
    # shellcheck disable=SC2086
    run_with_prefix "validator-$validator_index" $TASKSET2 $snarkos_cmd start "--dev=$validator_index" --validator \
      "${common_flags[@]}" "--validators=$validators" --metrics --metrics-ip "$metrics_ip" \
      "--logfile=$log_file" "--rest=127.0.0.1:$rest_port"
  fi
  PIDS[validator_index]=$!
  sleep 1
done

wait_for_nodes "$num_validators" 0 "$network_name" 180

# Wait for validators to be fully connected via BFT
log "Waiting for validators to be fully connected..."
for validator_index in $(seq 0 $((num_validators - 1))); do
  if ! wait_for_bft_connections "$validator_index" $((num_validators - 1)) "$network_name"; then
    exit 1
  fi
done
log "All validators are fully connected"

# Wait for at least min_height blocks before running the solution benchmark
log "Waiting for committee to reach at least $min_height blocks..."
SECONDS=0
while (( SECONDS < max_wait_height )); do
  if check_heights 0 "$num_validators" $min_height "$network_name" "$SECONDS"; then
    log "Committee reached height $min_height after ${SECONDS}s"
    break
  fi
  sleep $poll_interval
done
if (( SECONDS >= max_wait_height )); then
  log "Committee did not reach height $min_height within ${max_wait_height}s"
  exit 1
fi

base_url="http://127.0.0.1:3030/v2/$network_name"

# Collect time-series metrics in the background (single CSV for plotting).
metrics_csv="${log_dir}/invalid_solutions_metrics.csv"
collect_interval_sec=5
collect_duration_sec=120
python ./.ci/collect_invalid_solutions_metrics.py "$num_validators" "$base_metrics_port" "$metrics_csv" "$collect_interval_sec" "$collect_duration_sec" &
COLLECT_PID=$!

if $skip_invalid_txs; then
  log "Baseline mode: not issuing invalid solutions (collecting metrics only)"
else
  solution_json="$log_dir/invalid_solution.json"
  log "Generating solution JSON..."
  if ! cargo run --example gen_invalid_solution --features bench -p snarkos-cli --quiet 2>"$log_dir/gen_invalid_solution.err" >"$solution_json"; then
    log "Failed to generate solution. Build with: cargo build -p snarkos-cli --features bench --example gen_invalid_solution"
    cat "$log_dir/gen_invalid_solution.err" 1>&2
    exit 1
  fi
  if [ ! -s "$solution_json" ]; then
    log "Generated solution file is empty"
    exit 1
  fi
  if $valid_solutions; then
    log "Running valid-solutions (accept-until-limit) benchmark ($num_workers workers x $reqs_per_worker requests)..."
    python ./.ci/invalid_solutions_helper.py --valid-solutions "$base_url" "$num_workers" "$reqs_per_worker" "$solution_json"
  else
    log "Running invalid solutions benchmark ($num_workers workers x $reqs_per_worker requests)..."
    python ./.ci/invalid_solutions_helper.py "$base_url" "$num_workers" "$reqs_per_worker" "$solution_json"
  fi
fi

# Wait for metrics collector to finish (or kill after a short timeout if it has not)
wait "$COLLECT_PID" 2>/dev/null || true
if kill -0 "$COLLECT_PID" 2>/dev/null; then
  kill "$COLLECT_PID" 2>/dev/null || true
  wait "$COLLECT_PID" 2>/dev/null || true
fi

# Copy metrics CSV to current dir so it's easy to find for plotting
if $skip_invalid_txs; then
  metrics_fixed_name="invalid_solutions_metrics_baseline.csv"
elif $valid_solutions; then
  metrics_fixed_name="invalid_solutions_metrics_valid_solutions.csv"
else
  metrics_fixed_name="invalid_solutions_metrics.csv"
fi
if [ -f "$metrics_csv" ]; then
  cp "$metrics_csv" "$metrics_fixed_name"
  log "Metrics CSV: $metrics_csv (also ./$metrics_fixed_name)"
else
  log "Metrics CSV not produced (collector may have failed; ensure nodes are built with --features metrics)"
fi

log "To update plots: python ./.ci/plot_invalid_solutions_metrics.py $metrics_fixed_name [output_dir]"
log "🎉 Invalid solutions benchmark done!"
exit 0
