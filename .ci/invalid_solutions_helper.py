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


def write_results_invalid(num_requests, total_wait, rejections, base_url):
    """Append benchmark result for invalid-solutions mode."""
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


def write_results_valid(num_requests, total_wait, accepted, rejected, base_url):
    """Append benchmark result for valid-solutions mode (accept until limit)."""
    accept_throughput = accepted / total_wait if total_wait > 0 else 0
    reject_throughput = rejected / total_wait if total_wait > 0 else 0
    print(
        f"Valid solutions (accept-until-limit) benchmark done! "
        f"{accepted} accepted, {rejected} limit rejections in {total_wait:.1f}s. "
        f"Throughput: {accept_throughput:.1f} accepted/s, {reject_throughput:.1f} limit_rejections/s."
    )
    try:
        with open("info.txt", "r") as f:
            snapshot_info = f.read().replace("\n", "")
    except FileNotFoundError:
        snapshot_info = ""

    with open("results.json", "a") as f:
        f.write(
            f'{{ "name": "valid-solutions-accepted", "unit": "accepted/s", '
            f'"value": {accept_throughput}, "extra": "num_requests={num_requests}, '
            f'accepted={accepted}, limit_rejections={rejected}, total_wait={total_wait:.1f}, '
            f'base_url={base_url}, {snapshot_info}" }},\n'
        )


def post_invalid_solution_result(status):
    """Return 'rejected' (422), 'accepted' (200/203), or None for other."""
    if status == EXPECTED_STATUS:
        return "rejected"
    if status in (200, 203):
        return "accepted"
    return None


async def post_solution(
    session, url, solution_json, worker_id, req_index, valid_solutions_mode
):
    """POST one solution. In invalid-solutions mode return True iff 422. In valid-solutions mode return ('accepted'|'rejected'|None)."""
    try:
        async with session.post(
            url,
            json=solution_json,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:
            body = await response.read()
            result = post_invalid_solution_result(response.status)
            if result is not None:
                return (result if valid_solutions_mode else (result == "rejected"))
            if valid_solutions_mode:
                print(
                    f"Worker {worker_id} request {req_index}: status {response.status} body={body[:200]}",
                    file=sys.stderr,
                )
                return None
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
        return None if valid_solutions_mode else False
    except Exception as e:
        print(f"Worker {worker_id} request {req_index}: {e}", file=sys.stderr)
        return None if valid_solutions_mode else False


async def worker(
    session, base_url, solution_json, worker_id, reqs_per_worker, valid_solutions_mode
):
    """Worker coroutine: POST solution reqs_per_worker times."""
    url = f"{base_url.rstrip('/')}{SOLUTION_BROADCAST_PATH}?check_solution=true"
    if valid_solutions_mode:
        accepted, rejected = 0, 0
        for i in range(reqs_per_worker):
            r = await post_solution(
                session, url, solution_json, worker_id, i + 1, True
            )
            if r == "accepted":
                accepted += 1
            elif r == "rejected":
                rejected += 1
        return ("valid_solutions", accepted, rejected)
    rejections = 0
    for i in range(reqs_per_worker):
        if await post_solution(
            session, url, solution_json, worker_id, i + 1, False
        ):
            rejections += 1
    return rejections


async def main():
    args = [a for a in sys.argv[1:] if a != "--valid-solutions"]
    valid_solutions_mode = len(args) != len(sys.argv) - 1

    if len(args) < 4:
        print(
            "Usage: invalid_solutions_helper.py [--valid-solutions] <base_url> <num_workers> <reqs_per_worker> [solution.json]",
            file=sys.stderr,
        )
        print(
            "  --valid-solutions  count 2xx as accepted, 422 as limit rejections (nodes built with accept_any_solution)",
            file=sys.stderr,
        )
        print(
            "  base_url           e.g. http://127.0.0.1:3030/v2/testnet",
            file=sys.stderr,
        )
        print(
            "  num_workers        number of concurrent worker tasks",
            file=sys.stderr,
        )
        print(
            "  reqs_per_worker    number of POSTs per worker",
            file=sys.stderr,
        )
        print(
            "  solution.json      path to solution JSON (default: read from stdin)",
            file=sys.stderr,
        )
        sys.exit(1)

    base_url = args[0]
    num_workers = int(args[1])
    reqs_per_worker = int(args[2])

    if len(args) >= 4:
        solution_json = load_solution_json(args[3])
    else:
        solution_json = load_solution_json(sys.stdin)

    total_requests = num_workers * reqs_per_worker
    mode_desc = "valid-solutions (accept until limit)" if valid_solutions_mode else "invalid solution"
    print(
        f"Starting {num_workers} workers, {reqs_per_worker} requests each "
        f"(total {total_requests} {mode_desc} POSTs)..."
    )
    print(f"Base URL: {base_url}")
    start = time.perf_counter()

    connector = aiohttp.TCPConnector(
        limit=min(100, num_workers * 2),
        limit_per_host=min(50, num_workers * 2),
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            worker(
                session,
                base_url,
                solution_json,
                w,
                reqs_per_worker,
                valid_solutions_mode,
            )
            for w in range(1, num_workers + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    total_wait = time.perf_counter() - start
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        for e in errors:
            print(f"Worker error: {e}", file=sys.stderr)
        sys.exit(1)

    if valid_solutions_mode:
        accepted = sum(r[1] for r in results if isinstance(r, tuple) and r[0] == "valid_solutions")
        rejected = sum(r[2] for r in results if isinstance(r, tuple) and r[0] == "valid_solutions")
        write_results_valid(
            total_requests, total_wait, accepted, rejected, base_url
        )
    else:
        rejections = sum(r for r in results if isinstance(r, int))
        write_results_invalid(
            total_requests, total_wait, rejections, base_url
        )
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
