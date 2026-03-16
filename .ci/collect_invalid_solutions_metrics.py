#!/usr/bin/env python3
"""
Collects Prometheus metrics from validators periodically and appends time-series
rows to a CSV (timestamp_sec, validator_index, metric values). Run during the
benchmark or standalone to produce data for plot_invalid_solutions_metrics.py.

Usage:
  collect_invalid_solutions_metrics.py <num_validators> <base_metrics_port> <output_csv> <interval_sec> <duration_sec>
  duration_sec=0 means run until SIGINT (Ctrl+C).
"""

import csv
import signal
import sys
import time
import urllib.request

NEW_HISTOGRAM_METRICS = [
    "snarkos_bft_commit_leader_certificate_latency_secs",
    "snarkos_consensus_prepare_advance_to_next_quorum_block_latency_secs",
    "snarkos_consensus_check_next_block_latency_secs",
    "snarkos_consensus_advance_to_next_block_latency_secs",
]


def fetch_metrics(url):
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def parse_histogram_values(text, base_names):
    result = {name: {"count": None, "sum": None} for name in base_names}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0]
        try:
            value = float(parts[-1])
        except ValueError:
            continue
        for base in base_names:
            for suffix in ("_count", "_sum"):
                full = base + suffix
                if key == full or key.startswith(full + "{"):
                    result[base][suffix[1:]] = value
                    break
    return result


def main():
    if len(sys.argv) < 6:
        print(
            "Usage: collect_invalid_solutions_metrics.py <num_validators> <base_metrics_port> <output_csv> <interval_sec> <duration_sec>",
            file=sys.stderr,
        )
        print("  duration_sec=0: run until Ctrl+C", file=sys.stderr)
        sys.exit(1)

    num_validators = int(sys.argv[1])
    base_metrics_port = int(sys.argv[2])
    output_csv = sys.argv[3]
    interval_sec = float(sys.argv[4])
    duration_sec = float(sys.argv[5])

    fieldnames = ["timestamp_sec", "validator_index", "metrics_url"]
    for base in NEW_HISTOGRAM_METRICS:
        fieldnames.extend([f"{base}_count", f"{base}_sum", f"{base}_mean"])
    fieldnames.append("error")

    start_time = time.perf_counter()
    first_write = True

    def write_header_and_rows(rows):
        nonlocal first_write
        with open(output_csv, "w" if first_write else "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if first_write:
                writer.writeheader()
                first_write = False
            for row in rows:
                writer.writerow(row)

    # Create file and write header immediately so the file exists even if first scrape fails
    write_header_and_rows([])

    stop = False

    def on_signal(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    sample = 0
    while not stop:
        if duration_sec > 0 and (time.perf_counter() - start_time) >= duration_sec:
            break
        t = time.time()
        rows = []
        for v in range(num_validators):
            port = base_metrics_port + v
            url = f"http://127.0.0.1:{port}/metrics"
            text = fetch_metrics(url)
            row = {"timestamp_sec": round(t, 2), "validator_index": v, "metrics_url": url, "error": ""}
            for base in NEW_HISTOGRAM_METRICS:
                row[f"{base}_count"] = ""
                row[f"{base}_sum"] = ""
                row[f"{base}_mean"] = ""
            if text is None:
                row["error"] = "fetch_failed"
                rows.append(row)
                continue
            values = parse_histogram_values(text, NEW_HISTOGRAM_METRICS)
            for base in NEW_HISTOGRAM_METRICS:
                c = values[base]["count"]
                s = values[base]["sum"]
                row[f"{base}_count"] = c if c is not None else ""
                row[f"{base}_sum"] = s if s is not None else ""
                if c is not None and s is not None and c > 0:
                    row[f"{base}_mean"] = round(s / c, 6)
                else:
                    row[f"{base}_mean"] = ""
            rows.append(row)
        write_header_and_rows(rows)
        sample += 1
        if sample % 10 == 0:
            print(f"Collected {sample} samples to {output_csv}", file=sys.stderr)
        if not stop and duration_sec > 0 and (time.perf_counter() - start_time + interval_sec) >= duration_sec:
            break
        time.sleep(interval_sec)

    print(f"Stopped. Wrote {sample} sample(s) to {output_csv}", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
