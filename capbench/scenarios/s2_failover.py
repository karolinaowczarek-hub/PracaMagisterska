"""S2 — primary failure and recovery."""

from __future__ import annotations

import logging
import threading
import time
from time import perf_counter
from typing import Any

from .. import infra
from ..config import Clock
from ..databases.base import Database
from ..output import CsvSink, sanitize_message
from ..workload import run_continuous

log = logging.getLogger(__name__)


def run(
    db: Database,
    cfg: dict,
    sinks: dict[str, CsvSink],
    run_meta: dict,
) -> dict[str, Any]:
    clock = Clock()
    phases = cfg["phase_durations"]
    poll_ms = float(cfg.get("poll_interval_ms", 100))
    poll_s = poll_ms / 1000.0
    metrics: dict[str, Any] = {"hypothesis": db.expected_behaviour("s2")}
    run_meta = {**run_meta, "fault_level": "kill", "fault_mode": "sigkill"}

    infra.preflight(db.name, cfg)

    phase_box = {"phase": "warmup"}
    stop_box = {"stop": False}
    fault_mark_box: dict[str, float | None] = {"t": None}
    first_error_t: list[float | None] = [None]
    last_ok_write_before: list[float | None] = [None]
    first_ok_write_after: list[float | None] = [None]
    failed_ops = {"read": 0, "write": 0}
    lock = threading.Lock()

    def _op_start(row: dict) -> float:
        started = row.get("started_perf")
        if isinstance(started, (int, float)):
            return float(started)
        return perf_counter()

    def on_op(row: dict) -> None:
        with lock:
            ok = row["success"] == "true"
            op_start = _op_start(row)
            if phase_box["phase"] == "warmup" and ok and row["op_type"] == "write":
                last_ok_write_before[0] = op_start
            if fault_mark_box["t"] is not None:
                if not ok and first_error_t[0] is None:
                    first_error_t[0] = op_start
                if phase_box["phase"] == "during_fault" and not ok:
                    failed_ops[row["op_type"]] = failed_ops.get(row["op_type"], 0) + 1
                if (
                    phase_box["phase"] in ("during_fault", "after_election")
                    and ok
                    and row["op_type"] == "write"
                    and first_ok_write_after[0] is None
                    and first_error_t[0] is not None
                ):
                    first_ok_write_after[0] = op_start

    worker_done = threading.Event()

    def workload() -> None:
        try:
            run_continuous(
                db,
                get_phase=lambda: phase_box["phase"],
                should_stop=lambda: stop_box["stop"],
                sink=sinks["operations"],
                cfg=cfg,
                run_meta=run_meta,
                client_side="majority",
                get_fault_mark=lambda: fault_mark_box["t"],
                on_op=on_op,
            )
        finally:
            worker_done.set()

    t_worker = threading.Thread(target=workload, daemon=True)
    t_worker.start()

    try:
        time.sleep(float(phases["s2_warmup_s"]))

        victim = db.victim_for_failover()
        old_primary = victim
        log.info("Killing primary %s", victim)
        phase_box["phase"] = "during_fault"
        t_kill = perf_counter()
        fault_mark_box["t"] = t_kill
        clock.mark("fault")
        infra.stop_container(victim, timeout=0)

        t_new_primary = None
        promote_started = False
        deadline = t_kill + float(phases["s2_after_failover_s"]) + 30

        while perf_counter() < deadline:
            prim = db.primary()
            if prim and prim != old_primary:
                t_new_primary = perf_counter()
                phase_box["phase"] = "after_election"
                break
            if not promote_started and (perf_counter() - t_kill) > 0.5:
                candidates = [n for n in db.nodes() if n != old_primary]
                for c in candidates:
                    r = infra._run(
                        ["docker", "inspect", "-f", "{{.State.Running}}", c], timeout=15
                    )
                    if (r.stdout or "").strip() == "true":
                        db.note_failover(c)
                        db.promote(c)
                        promote_started = True
                        break
            time.sleep(poll_s)

        if t_new_primary is None:
            prim = db.primary()
            if prim and prim != old_primary:
                t_new_primary = perf_counter()
                phase_box["phase"] = "after_election"

        detection_ms = None
        if first_error_t[0] is not None:
            detection_ms = (first_error_t[0] - t_kill) * 1000.0
        election_ms = None
        if t_new_primary is not None:
            election_ms = (t_new_primary - t_kill) * 1000.0

        write_unavail_ms = None
        if last_ok_write_before[0] and first_ok_write_after[0]:
            write_unavail_ms = (first_ok_write_after[0] - last_ok_write_before[0]) * 1000.0

        time.sleep(float(phases["s2_after_failover_s"]) * 0.25)

        phase_box["phase"] = "recovery"
        method = db.rejoin(old_primary)
        timeout = float(phases["s2_recovery_timeout_s"])

        def two_streaming() -> bool:
            return db.all_nodes_running() and db.streaming_replica_count() >= 2

        ok, _ = infra.wait_for(two_streaming, timeout=timeout, interval=1.0)
        if ok:
            db.enable_sync_replication()
            ok, _ = infra.wait_for(
                lambda: db.sync_replica_count() >= 1,
                timeout=min(30.0, timeout),
                interval=1.0,
            )
        full_recovery_ms = (perf_counter() - t_kill) * 1000.0 if ok else None

        def _metric(name, value, unit, context=""):
            sinks["scenario_metrics"].write({
                "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s2",
                "metric_name": name, "metric_value": value if value is not None else "",
                "unit": unit, "context": context,
            })

        _metric("detection_time", detection_ms, "ms")
        _metric("election_time", election_ms, "ms", db.election_metric_context())
        _metric("write_unavailability", write_unavail_ms, "ms")
        _metric("failed_ops_read", failed_ops.get("read", 0), "count")
        _metric("failed_ops_write", failed_ops.get("write", 0), "count")
        _metric("full_recovery_time", full_recovery_ms, "ms", f"rejoin={method}")
        _metric("poll_interval", poll_ms, "ms", "measurement_uncertainty")

        metrics.update({
            "detection_time_ms": detection_ms,
            "election_time_ms": election_ms,
            "write_unavailability_ms": write_unavail_ms,
            "full_recovery_time_ms": full_recovery_ms,
            "rejoin_method": method,
            "status": "ok" if t_new_primary and ok else "aborted",
        })
        if not t_new_primary:
            metrics["abort_reason"] = "no new primary elected/promoted"
            raise RuntimeError(metrics["abort_reason"])
        if not ok:
            metrics["abort_reason"] = "cluster not fully recovered after rejoin"
            raise RuntimeError(metrics["abort_reason"])
        return metrics
    except Exception as exc:
        metrics["status"] = "aborted"
        metrics["abort_reason"] = sanitize_message(str(exc), 500)
        log.exception("S2 aborted")
        raise
    finally:
        stop_box["stop"] = True
        worker_done.wait(timeout=30)
        infra.cleanup(db.name, cfg)
