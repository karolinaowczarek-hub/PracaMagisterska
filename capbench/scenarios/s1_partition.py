"""S1 — network partition: majority vs minority clients."""

from __future__ import annotations

import logging
import threading
from time import perf_counter
from typing import Any

from .. import infra
from ..config import Clock
from ..databases.base import Database
from ..output import CsvSink, sanitize_message
from ..workload import run_phase

log = logging.getLogger(__name__)


def run(
    db: Database,
    cfg: dict,
    sinks: dict[str, CsvSink],
    run_meta: dict,
) -> dict[str, Any]:
    clock = Clock()
    victim = db.victim_for_partition()
    meta = {n["name"]: n for n in cfg[db.name]["nodes"]}
    phases = cfg["phase_durations"]
    metrics: dict[str, Any] = {"hypothesis": db.expected_behaviour("s1")}

    infra.preflight(db.name, cfg)
    clock.mark("start")

    try:
        # --- baseline ---
        run_meta = {**run_meta, "fault_level": "0"}
        base = run_phase(
            db, float(phases["s1_baseline_s"]), "baseline", sinks["operations"],
            cfg=cfg, run_meta=run_meta, client_side="majority",
        )
        _write_phase(sinks, run_meta, "baseline", "majority", "", base)

        # --- partition ---
        clock.mark("fault")
        fault_t = perf_counter()
        infra.isolate_cluster(victim, meta[victim]["ipv4_cluster"])
        log.info("Partitioned %s from cluster peers", victim)

        minority = db.minority_client()
        # majority client
        db.connect()

        maj_result: dict = {}
        min_result: dict = {}

        def majority_load() -> None:
            nonlocal maj_result
            maj_result = run_phase(
                db, float(phases["s1_partition_s"]), "during_fault", sinks["operations"],
                cfg=cfg, run_meta=run_meta, client_side="majority", fault_mark=fault_t,
            )

        def minority_load() -> None:
            nonlocal min_result
            min_result = run_phase(
                minority, float(phases["s1_partition_s"]), "during_fault", sinks["operations"],
                cfg=cfg, run_meta=run_meta, client_side="minority", fault_mark=fault_t,
            )

        t1 = threading.Thread(target=majority_load)
        t2 = threading.Thread(target=minority_load)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        _write_phase(sinks, run_meta, "during_fault", "majority", "", maj_result)
        _write_phase(sinks, run_meta, "during_fault", "minority", "", min_result)

        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s1",
            "metric_name": "majority_success_rate", "metric_value": maj_result.get("success_rate", 0),
            "unit": "ratio", "context": "during_fault",
        })
        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s1",
            "metric_name": "minority_success_rate", "metric_value": min_result.get("success_rate", 0),
            "unit": "ratio", "context": "during_fault",
        })

        # --- heal ---
        infra.restore_cluster(victim, meta[victim]["ipv4_cluster"])
        db.note_healed()
        clock.mark("heal")
        after = run_phase(
            db, float(phases["s1_baseline_s"]), "after_heal", sinks["operations"],
            cfg=cfg, run_meta=run_meta, client_side="majority", fault_mark=fault_t,
        )
        _write_phase(sinks, run_meta, "after_heal", "majority", "", after)
        minority.close()
        metrics["status"] = "ok"
        return metrics
    except Exception as exc:
        metrics["status"] = "aborted"
        metrics["abort_reason"] = sanitize_message(str(exc), 500)
        log.exception("S1 aborted")
        raise
    finally:
        infra.cleanup(db.name, cfg)


def _write_phase(sinks, run_meta, phase, client_side, op_type_filter, summary):
    # one summary row for the phase
    sinks["phase_summary"].write({
        "run_id": run_meta["run_id"],
        "db": run_meta["db"],
        "scenario": run_meta["scenario"],
        "phase": phase,
        "fault_level": run_meta.get("fault_level", ""),
        "client_side": client_side,
        "op_type": op_type_filter or "all",
        "consistency_level": run_meta.get("consistency_level", ""),
        **{k: summary.get(k) for k in (
            "attempted", "success_count", "error_count", "success_rate",
            "throughput_ops_per_sec", "latency_mean_ms", "latency_median_ms",
            "latency_p95_ms", "latency_p99_ms", "latency_min_ms", "latency_max_ms",
            "phase_duration_ms",
        )},
    })
