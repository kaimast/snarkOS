#!/usr/bin/env python3
"""
Scrapes Prometheus /metrics from each validator and writes a CSV of the
metrics added in the last commit (BFT/consensus latency histograms) per validator.
Run at the end of bench_invalid_solutions.sh (validators must be started with --metrics).
"""

import csv
import sys
import urllib.request

# New histogram metrics added in the last commit (base names; we read _count and _sum).
NEW_HISTOGRAM_METRICS = [
    "snarkos_bft_commit_leader_certificate_latency_secs",
    "snarkos_consensus_prepare_advance_to_next_quorum_block_latency_secs",
    "snarkos_consensus_check_next_block_latency_secs",
    "snarkos_consensus_advance_to_next_block_latency_secs",
]

def fetch_metrics(url):
    """Fetch Prometheus text from url; return as string or None on error."""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def parse_histogram_values(text, base_names):
    """
    Parse Prometheus text and return a dict: base_name -> {"count": float, "sum": float}.
    Handles: metric_name value  or  metric_name{label="x"} value
    """
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
    if len(sys.argv) < 4:
        print(
            "Usage: export_invalid_solutions_metrics.py <num_validators> <base_metrics_port> <output_csv>",
            file=sys.stderr,
        )
        print(
            "  e.g. export_invalid_solutions_metrics.py 1 9000 invalid_solutions_metrics.csv",
            file=sys.stderr,
        )
        sys.exit(1)

    num_validators = int(sys.argv[1])
    base_metrics_port = int(sys.argv[2])
    output_csv = sys.argv[3]

    fieldnames = ["validator_index", "metrics_url"]
    for base in NEW_HISTOGRAM_METRICS:
        fieldnames.extend([f"{base}_count", f"{base}_sum", f"{base}_mean"])
    fieldnames.append("error")

    rows = []
    for v in range(num_validators):
        port = base_metrics_port + v
        url = f"http://127.0.0.1:{port}/metrics"
        text = fetch_metrics(url)
        row = {"validator_index": v, "metrics_url": url, "error": ""}
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

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Wrote metrics for {num_validators} validator(s) to {output_csv}", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
