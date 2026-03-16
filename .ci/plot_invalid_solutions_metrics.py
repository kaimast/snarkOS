#!/usr/bin/env python3
"""
Generates one graph per BFT/consensus metric from a time-series metrics CSV.
X-axis = time, Y-axis = metric value, one line per validator.
Reads the CSV produced by collect_invalid_solutions_metrics.py (run during
the benchmark or separately).

Usage:
  plot_invalid_solutions_metrics.py <timeseries_metrics_csv> [output_dir]
"""

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict

NEW_HISTOGRAM_METRICS = [
    "snarkos_bft_commit_leader_certificate_latency_secs",
    "snarkos_consensus_prepare_advance_to_next_quorum_block_latency_secs",
    "snarkos_consensus_check_next_block_latency_secs",
    "snarkos_consensus_advance_to_next_block_latency_secs",
]


def metric_label(base_name):
    return base_name.replace("snarkos_bft_", "").replace("snarkos_consensus_", "").replace("_secs", " (secs)")


def safe_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: plot_invalid_solutions_metrics.py <timeseries_metrics_csv> [output_dir]",
            file=sys.stderr,
        )
        print(
            "  timeseries_metrics_csv  path to CSV from collect_invalid_solutions_metrics.py",
            file=sys.stderr,
        )
        print(
            "  output_dir              directory for PNG files (default: same as CSV)",
            file=sys.stderr,
        )
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else csv_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as e:
        print(f"matplotlib is required: {e}", file=sys.stderr)
        print("Install with: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if "timestamp_sec" not in fieldnames:
            print(
                "CSV must have 'timestamp_sec' column. Use the output of collect_invalid_solutions_metrics.py "
                "(run during the benchmark or: python .ci/collect_invalid_solutions_metrics.py <num_validators> <base_port> <out.csv> <interval_sec> <duration_sec>).",
                file=sys.stderr,
            )
            sys.exit(1)
        for row in reader:
            if row.get("error") == "fetch_failed":
                continue
            rows.append(row)

    if not rows:
        print("No rows to plot.", file=sys.stderr)
        sys.exit(0)

    # Group by validator_index -> list of (timestamp_sec, row)
    by_validator = defaultdict(list)
    for r in rows:
        t = safe_float(r.get("timestamp_sec"))
        if t is None:
            continue
        v = r.get("validator_index")
        if v == "":
            continue
        try:
            vidx = int(v)
        except ValueError:
            continue
        by_validator[vidx].append((t, r))

    for vidx in by_validator:
        by_validator[vidx].sort(key=lambda x: x[0])

    validators = sorted(by_validator.keys())
    colors = plt.cm.tab10.colors if len(validators) <= 10 else plt.cm.tab20.colors

    for base in NEW_HISTOGRAM_METRICS:
        mean_col = f"{base}_mean"
        fig, ax = plt.subplots()
        for i, vidx in enumerate(validators):
            points = by_validator.get(vidx, [])
            if not points:
                continue
            times = [p[0] for p in points]
            raw = [safe_float(p[1].get(mean_col)) for p in points]
            values = [float("nan") if v is None else v for v in raw]
            if all(v != v for v in values):
                continue
            t0 = times[0]
            times_rel = [t - t0 for t in times]
            color = colors[i % len(colors)]
            ax.plot(
                times_rel,
                values,
                marker="o",
                markersize=4,
                linestyle="-",
                linewidth=1.5,
                label=f"Validator {vidx}",
                color=color,
            )
        ax.set_xlabel("Time (sec since first sample)")
        ax.set_ylabel("Mean latency (secs)")
        ax.set_title(metric_label(base))
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", base)
        fig.tight_layout()
        fig.savefig(output_dir / f"{safe_name}.png", dpi=150)
        plt.close(fig)
        print(f"Saved {output_dir / f'{safe_name}.png'}", file=sys.stderr)

    print(f"Generated {len(NEW_HISTOGRAM_METRICS)} graph(s) in {output_dir}", file=sys.stderr)
    sys.exit(0)


if __name__ == "__main__":
    main()
