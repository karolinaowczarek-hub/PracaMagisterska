"""S4 — partition, divergent writes, reconnect, sync timeline."""

from __future__ import annotations

import logging
import time
from collections import Counter
from time import perf_counter
from typing import Any

from .. import infra
from ..databases.base import Database
from ..errors import classify, driver_errors
from ..output import CsvSink, sanitize_message
from ..workload import value

log = logging.getLogger(__name__)


def run(
    db: Database,
    cfg: dict,
    sinks: dict[str, CsvSink],
    run_meta: dict,
) -> dict[str, Any]:
    phases = cfg["phase_durations"]
    meta = {n["name"]: n for n in cfg[db.name]["nodes"]}
    metrics: dict[str, Any] = {"hypothesis": db.expected_behaviour("s4")}
    n_ref = 5000 if cfg.get("profile") != "fast" else 500
    n_new = int(phases["s4_majority_writes"])

    infra.preflight(db.name, cfg)

    try:
        next_val = value(int(cfg["workload"]["value_size_bytes"]), int(cfg["random_seed"]) + 99)
        refs: list[tuple[str, str]] = [(f"ref_{i:08d}", next_val()) for i in range(n_ref)]
        db.write_batch(refs)
        log.info("Wrote %s reference records", len(refs))

        expected_refs = dict(refs)
        ref_keys = [k for k, _ in refs]
        mismatches: list[str] = []
        seen = {key: False for key in ref_keys}
        for node in db.nodes():
            got = db.read_keys_from_node(node, ref_keys)
            for key, exp in expected_refs.items():
                if key not in got:
                    continue
                seen[key] = True
                if got[key] != exp:
                    mismatches.append(f"{node}:{key}")
        missing_everywhere = [key for key, present in seen.items() if not present]
        if mismatches or missing_everywhere:
            sample = mismatches or [f"missing:{key}" for key in missing_everywhere]
            raise RuntimeError(
                "pre-partition replica mismatch on "
                + ", ".join(sample[:8])
                + (f" (+{len(sample) - 8} more)" if len(sample) > 8 else "")
            )

        victim = db.victim_for_partition()
        infra.isolate_cluster(victim, meta[victim]["ipv4_cluster"])
        log.info("S4 partitioned %s from cluster peers", victim)

        minority = db.minority_client()
        db.connect()

        new_keys: list[tuple[str, str]] = []
        majority_items: list[tuple[str, str, str]] = []
        for i in range(n_new):
            k = f"new_{i:08d}"
            v = next_val()
            try:
                db.write(k, v)
                new_keys.append((k, v))
            except driver_errors(db.name) as exc:
                cat, cls_name, raw = classify(exc, db.name)
                majority_items.append((cat.value, cls_name, sanitize_message(raw)))

        minority_ok = 0
        minority_attempts = min(100, n_new)
        minority_items: list[tuple[str, str, str]] = []
        for i in range(minority_attempts):
            k = f"min_{i:08d}"
            try:
                minority.write(k, next_val())
                minority_ok += 1
            except driver_errors(db.name) as exc:
                cat, cls_name, raw = classify(exc, db.name)
                minority_items.append((cat.value, cls_name, sanitize_message(raw)))

        _write_error_groups(sinks, run_meta, "during_fault", "majority", "write", majority_items)
        _write_error_groups(sinks, run_meta, "during_fault", "minority", "write", minority_items)

        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
            "metric_name": "minority_writes_succeeded", "metric_value": minority_ok,
            "unit": "count", "context": f"attempts={minority_attempts}",
        })
        minority_errors = Counter(cat for cat, _, _ in minority_items)
        majority_write_errors = Counter(cat for cat, _, _ in majority_items)
        for cat, count in minority_errors.items():
            sinks["scenario_metrics"].write({
                "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
                "metric_name": "minority_write_error", "metric_value": count,
                "unit": "count", "context": cat,
            })
        for cat, count in majority_write_errors.items():
            sinks["scenario_metrics"].write({
                "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
                "metric_name": "majority_write_error", "metric_value": count,
                "unit": "count", "context": cat,
            })

        infra.restore_cluster(victim, meta[victim]["ipv4_cluster"])
        db.note_healed()
        t_re = perf_counter()
        db.repair_hooks()

        poll_s = float(phases["s4_poll_interval_ms"]) / 1000.0
        timeout = float(phases["s4_sync_timeout_s"])
        check_pairs = refs[:200] + new_keys[:200]
        check_keys = [k for k, _ in check_pairs]
        expected = {k: v for k, v in refs + new_keys}

        consecutive_ok = 0
        sync_time_ms = None
        stale_total = 0

        while perf_counter() - t_re < timeout:
            elapsed_ms = (perf_counter() - t_re) * 1000.0
            all_match = True
            reported: set[str] = set()
            for node in db.nodes():
                matching = stale = missing = 0
                try:
                    got_map = db.read_keys_from_node(node, check_keys)
                except driver_errors(db.name):
                    got_map = {k: None for k in check_keys}
                relevant = [k for k in check_keys if k in got_map]
                if not relevant:
                    continue
                reported.update(relevant)
                for k in relevant:
                    got = got_map.get(k)
                    exp = expected.get(k)
                    if got is None:
                        missing += 1
                        all_match = False
                    elif exp is not None and got != exp:
                        stale += 1
                        stale_total += 1
                        all_match = False
                    else:
                        matching += 1
                ratio = matching / len(relevant) if relevant else 1.0
                sinks["consistency_timeline"].write({
                    "run_id": run_meta["run_id"],
                    "db": run_meta["db"],
                    "node": node,
                    "elapsed_since_reconnect_ms": round(elapsed_ms, 3),
                    "keys_checked": len(relevant),
                    "keys_matching": matching,
                    "keys_stale": stale,
                    "keys_missing": missing,
                    "consistency_ratio": round(ratio, 6),
                })
            if reported != set(check_keys):
                all_match = False
            if all_match:
                consecutive_ok += 1
                if consecutive_ok >= 3:
                    sync_time_ms = elapsed_ms
                    break
            else:
                consecutive_ok = 0
            time.sleep(poll_s)

        final_keys = [k for k, _ in new_keys[:500]]
        by_key: dict[str, set] = {k: set() for k in final_keys}
        for node in db.nodes():
            try:
                got_map = db.read_keys_from_node(node, final_keys)
            except driver_errors(db.name):
                got_map = {k: None for k in final_keys}
            for k in final_keys:
                if k not in got_map:
                    continue
                by_key[k].add(got_map.get(k))
        divergent = sum(1 for vals in by_key.values() if len(vals) != 1)

        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
            "metric_name": "sync_time", "metric_value": sync_time_ms if sync_time_ms is not None else "",
            "unit": "ms", "context": "",
        })
        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
            "metric_name": "records_to_sync", "metric_value": len(new_keys),
            "unit": "count", "context": "",
        })
        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
            "metric_name": "divergent_after_sync", "metric_value": divergent,
            "unit": "count", "context": "",
        })
        sinks["scenario_metrics"].write({
            "run_id": run_meta["run_id"], "db": run_meta["db"], "scenario": "s4",
            "metric_name": "stale_reads", "metric_value": stale_total,
            "unit": "count", "context": "during_convergence",
        })

        minority.close()
        metrics["status"] = "ok" if sync_time_ms is not None else "aborted"
        if sync_time_ms is None:
            metrics["abort_reason"] = "sync timeout"
            raise RuntimeError("S4 sync timeout")
        return metrics
    except Exception as exc:
        metrics["status"] = "aborted"
        metrics["abort_reason"] = sanitize_message(str(exc), 500)
        log.exception("S4 aborted")
        raise
    finally:
        infra.cleanup(db.name, cfg)


def _write_error_groups(
    sinks: dict[str, CsvSink],
    run_meta: dict,
    phase: str,
    client_side: str,
    op_type: str,
    items: list[tuple[str, str, str]],
) -> None:
    counts: Counter[tuple[str, str]] = Counter()
    examples: dict[tuple[str, str], str] = {}
    for cat, cls_name, msg in items:
        key = (cat, cls_name)
        counts[key] += 1
        examples.setdefault(key, msg)
    total = sum(counts.values()) or 1
    for (cat, cls_name), n in counts.items():
        sinks["errors_breakdown"].write({
            "run_id": run_meta["run_id"],
            "db": run_meta["db"],
            "scenario": "s4",
            "phase": phase,
            "client_side": client_side,
            "op_type": op_type,
            "error_category": cat,
            "error_class": cls_name,
            "count": n,
            "share_of_errors": round(n / total, 6),
            "example_message": examples[(cat, cls_name)],
        })

