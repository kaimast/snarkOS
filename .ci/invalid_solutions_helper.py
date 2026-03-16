#!/usr/bin/env python3
"""
Benchmark driver for invalid solution rejection throughput.
POSTs many invalid solutions (with check_solution=true) to the node's REST API
and measures how many rejections (422) per second the node can handle.
"""

import asyncio
import json
import sys
import time

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp is required. Install with: pip install aiohttp", file=sys.stderr)
    sys.exit(1)


SOLUTION_BROADCAST_PATH = "/solution/broadcast"
# Expect 422 Unprocessable Entity for invalid solutions
EXPECTED_STATUS = 422


def load_solution_json(path_or_file):
    """Load JSON from file or file-like, tolerating extra lines (e.g. cargo output)."""
    if hasattr(path_or_file, "read"):
        raw = path_or_file.read()
    else:
        with open(path_or_file, "r") as f:
            raw = f.read()
    raw = raw.strip()
    # If the file has extra content (e.g. "Running ..." from cargo), extract the JSON object.
    start = raw.find("{")
    if start < 0:
        raise ValueError("No JSON object found in solution file")
    depth = 0
    for i in range(start, len(raw)):
        if raw[i] == "{":
            depth += 1
        elif raw[i] == "}":
            depth -= 1
            if depth == 0:
                return json.loads(raw[start : i + 1])
    raise ValueError("Unterminated JSON object in solution file")


def write_results(num_requests, total_wait, rejections, base_url):
    """Append benchmark result to results.json (same format as rest_api_helper.py)."""
    throughput = rejections / total_wait if total_wait > 0 else 0
    print(
        f"Invalid solutions benchmark done! {rejections} rejections in {total_wait:.1f}s. "
        f"Throughput: {throughput:.1f} rejections/s."
    )
    try:
        with open("info.txt", "r") as f:
            snapshot_info = f.read().replace("\n", "")
    except FileNotFoundError:
        snapshot_info = ""

    with open("results.json", "a") as f:
        f.write(
            f'{{ "name": "invalid-solutions-rejected", "unit": "rejections/s", '
            f'"value": {throughput}, "extra": "num_requests={num_requests}, '
            f'rejections={rejections}, total_wait={total_wait:.1f}, '
            f'base_url={base_url}, {snapshot_info}" }},\n'
        )


async def post_invalid_solution(session, url, solution_json, worker_id, req_index):
    """POST one invalid solution; return True if we got 422 (rejected as invalid)."""
    try:
        async with session.post(
            url,
            json=solution_json,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            body = await response.read()
            if response.status == EXPECTED_STATUS:
                return True
            if response.status in (200, 203):
                print(
                    f"Worker {worker_id} request {req_index}: expected 422, got {response.status}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"Worker {worker_id} request {req_index}: status {response.status} body={body[:200]}",
                    file=sys.stderr,
                )
            return False
    except asyncio.TimeoutError:
        print(f"Worker {worker_id} request {req_index}: timeout", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Worker {worker_id} request {req_index}: {e}", file=sys.stderr)
        return False


async def worker(session, base_url, solution_json, worker_id, reqs_per_worker):
    """Worker coroutine: POST invalid solution reqs_per_worker times."""
    url = f"{base_url.rstrip('/')}{SOLUTION_BROADCAST_PATH}?check_solution=true"
    rejections = 0
    for i in range(reqs_per_worker):
        if await post_invalid_solution(session, url, solution_json, worker_id, i + 1):
            rejections += 1
    return rejections


async def main():
    if len(sys.argv) < 4:
        print(
            "Usage: invalid_solutions_helper.py <base_url> <num_workers> <reqs_per_worker> [solution.json]",
            file=sys.stderr,
        )
        print(
            "  base_url         e.g. http://127.0.0.1:3030/v2/testnet",
            file=sys.stderr,
        )
        print(
            "  num_workers      number of concurrent worker tasks",
            file=sys.stderr,
        )
        print(
            "  reqs_per_worker  number of POSTs per worker",
            file=sys.stderr,
        )
        print(
            "  solution.json    path to invalid solution JSON (default: read from stdin)",
            file=sys.stderr,
        )
        sys.exit(1)

    base_url = sys.argv[1]
    num_workers = int(sys.argv[2])
    reqs_per_worker = int(sys.argv[3])

    if len(sys.argv) >= 5:
        solution_json = load_solution_json(sys.argv[4])
    else:
        solution_json = load_solution_json(sys.stdin)

    print(
        f"Starting {num_workers} workers, {reqs_per_worker} requests each "
        f"(total {num_workers * reqs_per_worker} invalid solution POSTs)..."
    )
    print(f"Base URL: {base_url}")
    start = time.perf_counter()

    connector = aiohttp.TCPConnector(
        limit=min(100, num_workers * 2),
        limit_per_host=min(50, num_workers * 2),
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            worker(session, base_url, solution_json, w, reqs_per_worker)
            for w in range(1, num_workers + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    total_wait = time.perf_counter() - start
    rejections = sum(r for r in results if isinstance(r, int))
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        for e in errors:
            print(f"Worker error: {e}", file=sys.stderr)
        sys.exit(1)

    write_results(num_workers * reqs_per_worker, total_wait, rejections, base_url)
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
