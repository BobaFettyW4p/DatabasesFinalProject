"""Shared helpers for timing query scripts."""

import time

MAX_QUERY_SECONDS = 2.0


def start_query_timer():
    return time.perf_counter()


def end_query_timer(start_time, label):
    elapsed_seconds = time.perf_counter() - start_time
    passed = elapsed_seconds < MAX_QUERY_SECONDS
    status = "PASS" if passed else "FAIL"
    print(
        f"\n[TIMING] {label} runtime: {elapsed_seconds:.4f}s "
        f"({status} - target < {MAX_QUERY_SECONDS:.1f}s)"
    )
    if not passed:
        raise RuntimeError(
            f"{label} exceeded runtime target: {elapsed_seconds:.4f}s >= {MAX_QUERY_SECONDS:.1f}s"
        )
    return elapsed_seconds
