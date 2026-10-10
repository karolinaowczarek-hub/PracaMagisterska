"""S3 — injected inter-node latency via tc/netem."""

from __future__ import annotations

import logging
import time
from typing import Any

from .. import infra
from ..databases.base import Database
from ..output import CsvSink, sanitize_message
from ..workload import run_phase
from .s1_partition import _write_phase

log = logging.getLogger(__name__)


def run(
    db: Database,
    cfg: dict,
    sinks: dict[str, CsvSink],
    run_meta: dict,
) -> dict[str, Any]:
    phases = cfg["phase_durations"]
    levels = list(cfg["netem_levels_ms"])
    metrics: dict[str, Any] = {"hypothesis": db.expected_behaviour("s3")}

    infra.preflight(db.name, cfg)

    try:
        peer_specs = db.latency_peers()

        for delay_ms in levels:
            log.info("S3 delay level %sms", delay_ms)
            run_meta_l = {**run_meta, "fault_level": str(delay_ms)}

            measured = None
            if delay_ms > 0:
                for container, peer_ips in peer_specs:
                    infra.apply_delay(container, int(delay_ms), peer_ips)
                    # verify against first peer
                    measured = infra.verify_delay(container, peer_ips[0], int(delay_ms))
                    sinks["scenario_metrics"].write({
                        "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s3",
                        "metric_name": "measured_rtt", "metric_value": measured,
                        "unit": "ms", "context": f"injected_delay={delay_ms}ms",
                    })
            else:
                for container, _ in peer_specs:
                    infra.remove_delay(container)

            summary = run_phase(
                db, float(phases["s3_load_s"]), "load", sinks["operations"],
                cfg=cfg, run_meta=run_meta_l, client_side="majority",
            )
            _write_phase(sinks, run_meta_l, "load", "majority", "all", summary)

            sinks["scenario_metrics"].write({
                "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s3",
                "metric_name": "latency_median", "metric_value": summary.get("latency_median_ms"),
                "unit": "ms", "context": f"injected_delay={delay_ms}ms",
            })
            sinks["scenario_metrics"].write({
                "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s3",
                "metric_name": "throughput", "metric_value": summary.get("throughput_ops_per_sec"),
                "unit": "ops/s", "context": f"injected_delay={delay_ms}ms",
            })

            for container, _ in peer_specs:
                infra.remove_delay(container)
            time.sleep(float(phases["s3_stabilize_s"]))

        metrics["status"] = "ok"
        return metrics
    except Exception as exc:
        metrics["status"] = "aborted"
        metrics["abort_reason"] = sanitize_message(str(exc), 500)
        log.exception("S3 aborted")
        raise
    finally:
        infra.cleanup(db.name, cfg)
