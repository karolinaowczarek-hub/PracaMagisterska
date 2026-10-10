"""Workload generator: deterministic keys, thread pool, hard op timeouts, no retries."""

from __future__ import annotations

import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Callable, Optional

from .config import Clock
from .errors import ErrorCategory, classify, driver_errors, is_expected
from .output import CsvSink, sanitize_message, summarize

log = logging.getLogger(__name__)


@dataclass
class Operation:
    op_id: int
    op_type: str  # read | write
    key: str
    started_at: str = ""
    latency_ms: float = 0.0
    success: bool = False
    error_category: str = ""
    error_class: str = ""
    error_message: str = ""
    phase: str = ""
    client_side: str = "majority"
    target_node: str = ""
    elapsed_since_fault_ms: float | None = None


def keys(seed: int, space: int) -> Callable[[], str]:
    rng = random.Random(seed)

    def next_key() -> str:
        return f"k{rng.randrange(space):08d}"

    return next_key


def value(size: int, seed: int = 0) -> Callable[[], str]:
    rng = random.Random(seed + 1)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

    def next_value() -> str:
        return "".join(rng.choice(alphabet) for _ in range(size))

    return next_value


class _PhaseState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.phase = "baseline"
        self.fault_mark: float | None = None  # perf_counter at fault
        self.stop = False
        self.op_counter = 0


def run_phase(
    client: Any,
    duration_s: float,
    phase: str,
    sink: CsvSink,
    *,
    cfg: dict,
    run_meta: dict,
    client_side: str = "majority",
    fault_mark: float | None = None,
    rw_ratio: float | None = None,
) -> dict[str, Any]:
    """Run workload for duration_s; write ops to sink. Returns summary stats."""
    workload = cfg["workload"]
    threads = int(workload["threads"])
    timeout_ms = float(workload["op_timeout_ms"])
    ratio = rw_ratio if rw_ratio is not None else float(workload["read_write_ratio"])
    next_key = keys(int(cfg["random_seed"]), int(workload["key_space_size"]))
    next_val = value(int(workload["value_size_bytes"]), int(cfg["random_seed"]))

    state = _PhaseState()
    state.phase = phase
    state.fault_mark = fault_mark

    latencies: list[float] = []
    successes = 0
    lat_lock = threading.Lock()
    t0 = perf_counter()
    deadline = t0 + duration_s

    def one_op() -> None:
        nonlocal successes
        if perf_counter() >= deadline or state.stop:
            return
        with state.lock:
            state.op_counter += 1
            op_id = state.op_counter
        is_read = random.Random(op_id).random() < ratio
        op_type = "read" if is_read else "write"
        key = next_key()
        started = Clock.utc_now_iso()
        start = perf_counter()
        success = False
        err_cat = ""
        err_cls = ""
        err_msg = ""
        try:
            if is_read:
                client.read(key)
            else:
                client.write(key, next_val())
            success = True
        except driver_errors(run_meta["db"]) as exc:
            cat, err_cls, raw = classify(exc, run_meta["db"])
            err_cat = cat.value
            err_msg = sanitize_message(raw)
        latency = (perf_counter() - start) * 1000.0
        # label slow ops as TIMEOUT
        if not success and latency >= timeout_ms and not err_cat:
            err_cat = ErrorCategory.TIMEOUT.value
        elif not success and latency >= timeout_ms and err_cat == ErrorCategory.OTHER.value:
            err_cat = ErrorCategory.TIMEOUT.value

        elapsed_fault = None
        if state.fault_mark is not None:
            elapsed_fault = (start - state.fault_mark) * 1000.0

        expected = ""
        if err_cat:
            try:
                expected = str(
                    is_expected(
                        ErrorCategory(err_cat),
                        run_meta["db"],
                        run_meta["scenario"],
                        client_side=client_side,
                        phase=phase,
                    )
                ).lower()
            except ValueError:
                expected = "false"

        row = {
            "run_id": run_meta["run_id"],
            "db": run_meta["db"],
            "scenario": run_meta["scenario"],
            "phase": phase,
            "fault_level": run_meta.get("fault_level", ""),
            "client_side": client_side,
            "target_node": getattr(client, "last_target_node", "") or "",
            "consistency_level": run_meta.get("consistency_level", ""),
            "op_id": op_id,
            "op_type": op_type,
            "key": key,
            "started_at": started,
            "elapsed_since_fault_ms": elapsed_fault if elapsed_fault is not None else "",
            "latency_ms": round(latency, 3),
            "success": str(success).lower(),
            "error_category": err_cat,
            "error_class": err_cls,
            "error_message": err_msg,
            "expected_under_cap": expected,
            "started_perf": start,
        }
        sink.write(row)
        with lat_lock:
            latencies.append(latency)
            if success:
                successes += 1

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = set()
        while perf_counter() < deadline and not state.stop:
            # fill the pool
            while len(futures) < threads * 2 and perf_counter() < deadline:
                futures.add(pool.submit(one_op))
            done, futures = wait(futures, timeout=0.05, return_when=FIRST_COMPLETED)
            futures = set(futures)
        # Drain
        wait(futures, timeout=timeout_ms / 1000.0 + 1.0)

    duration = perf_counter() - t0
    return summarize(latencies, successes, duration)


def run_continuous(
    client: Any,
    get_phase: Callable[[], str],
    should_stop: Callable[[], bool],
    sink: CsvSink,
    *,
    cfg: dict,
    run_meta: dict,
    client_side: str = "majority",
    get_fault_mark: Callable[[], float | None] | None = None,
    on_op: Callable[[dict], None] | None = None,
) -> None:
    """Continuous load across phases; phase name read dynamically."""
    workload = cfg["workload"]
    threads = int(workload["threads"])
    timeout_ms = float(workload["op_timeout_ms"])
    ratio = float(workload["read_write_ratio"])
    next_key = keys(int(cfg["random_seed"]), int(workload["key_space_size"]))
    next_val = value(int(workload["value_size_bytes"]), int(cfg["random_seed"]))
    counter = {"n": 0}
    lock = threading.Lock()

    def one_op() -> None:
        if should_stop():
            return
        with lock:
            counter["n"] += 1
            op_id = counter["n"]
        phase = get_phase()
        is_read = random.Random(op_id).random() < ratio
        op_type = "read" if is_read else "write"
        key = next_key()
        started = Clock.utc_now_iso()
        start = perf_counter()
        success = False
        err_cat = err_cls = err_msg = ""
        try:
            if is_read:
                client.read(key)
            else:
                client.write(key, next_val())
            success = True
        except driver_errors(run_meta["db"]) as exc:
            cat, err_cls, raw = classify(exc, run_meta["db"])
            err_cat = cat.value
            err_msg = sanitize_message(raw)
        latency = (perf_counter() - start) * 1000.0
        if not success and latency >= timeout_ms:
            if not err_cat or err_cat == ErrorCategory.OTHER.value:
                err_cat = ErrorCategory.TIMEOUT.value

        fault_mark = get_fault_mark() if get_fault_mark else None
        elapsed_fault = ((start - fault_mark) * 1000.0) if fault_mark is not None else ""

        expected = ""
        if err_cat:
            try:
                expected = str(
                    is_expected(
                        ErrorCategory(err_cat),
                        run_meta["db"],
                        run_meta["scenario"],
                        client_side=client_side,
                        phase=phase,
                    )
                ).lower()
            except ValueError:
                expected = "false"

        row = {
            "run_id": run_meta["run_id"],
            "db": run_meta["db"],
            "scenario": run_meta["scenario"],
            "phase": phase,
            "fault_level": run_meta.get("fault_level", ""),
            "client_side": client_side,
            "target_node": getattr(client, "last_target_node", "") or "",
            "consistency_level": run_meta.get("consistency_level", ""),
            "op_id": op_id,
            "op_type": op_type,
            "key": key,
            "started_at": started,
            "elapsed_since_fault_ms": elapsed_fault,
            "latency_ms": round(latency, 3),
            "success": str(success).lower(),
            "error_category": err_cat,
            "error_class": err_cls,
            "error_message": err_msg,
            "expected_under_cap": expected,
            "started_perf": start,
        }
        sink.write(row)
        if on_op:
            on_op(row)

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures: set = set()
        while not should_stop():
            while len(futures) < threads * 2 and not should_stop():
                futures.add(pool.submit(one_op))
            done, futures = wait(futures, timeout=0.05, return_when=FIRST_COMPLETED)
            futures = set(futures)
        wait(futures, timeout=timeout_ms / 1000.0 + 1.0)
