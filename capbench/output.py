"""CSV sinks, latency summaries, aggregation."""

from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

OPERATIONS_HEADER = [
    "run_id", "db", "scenario", "phase", "fault_level", "client_side",
    "target_node", "consistency_level", "op_id", "op_type", "key",
    "started_at", "elapsed_since_fault_ms", "latency_ms", "success",
    "error_category", "error_class", "error_message", "expected_under_cap",
]

PHASE_SUMMARY_HEADER = [
    "run_id", "db", "scenario", "phase", "fault_level", "client_side",
    "op_type", "consistency_level", "attempted", "success_count", "error_count",
    "success_rate", "throughput_ops_per_sec", "latency_mean_ms",
    "latency_median_ms", "latency_p95_ms", "latency_p99_ms",
    "latency_min_ms", "latency_max_ms", "phase_duration_ms",
]

SCENARIO_METRICS_HEADER = [
    "run_id", "db", "scenario", "metric_name", "metric_value", "unit", "context",
]

ERRORS_BREAKDOWN_HEADER = [
    "run_id", "db", "scenario", "phase", "client_side", "op_type",
    "error_category", "error_class", "count", "share_of_errors", "example_message",
]

CONSISTENCY_TIMELINE_HEADER = [
    "run_id", "db", "node", "elapsed_since_reconnect_ms", "keys_checked",
    "keys_matching", "keys_stale", "keys_missing", "consistency_ratio",
]

RUNS_HEADER = [
    "run_id", "db", "scenario", "started_at", "finished_at", "status",
    "image_tag", "driver_version", "consistency_config", "retry_config",
    "phase_config", "random_seed", "threads", "value_size_bytes",
    "key_space_size", "fault_mode", "poll_interval_ms", "docker_version",
    "wsl_memory_gb", "cpu_count", "abort_reason", "profile",
]


def _percentile(sorted_vals: Sequence[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    return float(sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f))


def summarize(latencies: Sequence[float], successes: int, duration_s: float) -> dict[str, Any]:
    attempted = len(latencies)
    errors = attempted - successes
    sorted_l = sorted(latencies) if latencies else []
    return {
        "attempted": attempted,
        "success_count": successes,
        "error_count": errors,
        "success_rate": (successes / attempted) if attempted else 0.0,
        "throughput_ops_per_sec": (attempted / duration_s) if duration_s > 0 else 0.0,
        "latency_mean_ms": statistics.fmean(sorted_l) if sorted_l else None,
        "latency_median_ms": statistics.median(sorted_l) if sorted_l else None,
        "latency_p95_ms": _percentile(sorted_l, 95),
        "latency_p99_ms": _percentile(sorted_l, 99),
        "latency_min_ms": min(sorted_l) if sorted_l else None,
        "latency_max_ms": max(sorted_l) if sorted_l else None,
        "phase_duration_ms": duration_s * 1000.0,
    }


class CsvSink:
    """Incremental CSV writer; buffer 1000 rows; newline='' for Windows."""

    def __init__(self, path: Path, header: Sequence[str], buffer_size: int = 1000) -> None:
        self.path = path
        self.header = list(header)
        self.buffer_size = buffer_size
        self._buf: list[dict[str, Any]] = []
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists() or path.stat().st_size == 0
        self._fh = path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._fh, fieldnames=self.header, extrasaction="ignore")
        if write_header:
            self._writer.writeheader()
            self._fh.flush()

    def write(self, row: dict[str, Any]) -> None:
        self._buf.append(row)
        if len(self._buf) >= self.buffer_size:
            self.flush()

    def write_many(self, rows: Iterable[dict[str, Any]]) -> None:
        for r in rows:
            self.write(r)

    def flush(self) -> None:
        if not self._buf:
            return
        self._writer.writerows(self._buf)
        self._fh.flush()
        self._buf.clear()

    def close(self) -> None:
        self.flush()
        self._fh.close()

    def __enter__(self) -> "CsvSink":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def ensure_result_sinks(results_dir: Path, run_id: str) -> dict[str, CsvSink]:
    """Per-run operations file + shared summary files."""
    results_dir.mkdir(parents=True, exist_ok=True)
    ops_path = results_dir / f"operations_{run_id}.csv"
    return {
        "operations": CsvSink(ops_path, OPERATIONS_HEADER),
        "phase_summary": CsvSink(results_dir / "phase_summary.csv", PHASE_SUMMARY_HEADER),
        "scenario_metrics": CsvSink(results_dir / "scenario_metrics.csv", SCENARIO_METRICS_HEADER),
        "errors_breakdown": CsvSink(results_dir / "errors_breakdown.csv", ERRORS_BREAKDOWN_HEADER),
        "consistency_timeline": CsvSink(
            results_dir / "consistency_timeline.csv", CONSISTENCY_TIMELINE_HEADER
        ),
        "runs": CsvSink(results_dir / "runs.csv", RUNS_HEADER),
    }


def close_sinks(sinks: dict[str, CsvSink]) -> None:
    for s in sinks.values():
        s.close()


def sanitize_message(msg: str, limit: int = 200) -> str:
    cleaned = (msg or "").replace("\n", " ").replace("\r", " ").replace(",", ";")
    return cleaned[:limit]


def write_errors_breakdown_from_operations(ops_path: Path, sink: CsvSink, run_id: str) -> None:
    """One row per (phase, side, op, category, class) for a single run."""
    if not ops_path.exists() or ops_path.stat().st_size == 0:
        return
    groups: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    totals: dict[tuple[str, str, str], int] = {}
    db = ""
    scenario = ""
    with ops_path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("run_id") != run_id:
                continue
            db = row.get("db") or db
            scenario = row.get("scenario") or scenario
            if (row.get("success") or "").lower() == "true":
                continue
            cat = row.get("error_category") or ""
            if not cat:
                continue
            phase = row.get("phase") or ""
            side = row.get("client_side") or ""
            op_type = row.get("op_type") or ""
            cls_name = row.get("error_class") or ""
            bucket = (phase, side, op_type)
            totals[bucket] = totals.get(bucket, 0) + 1
            key = (phase, side, op_type, cat, cls_name)
            if key not in groups:
                groups[key] = {"count": 0, "example": row.get("error_message") or ""}
            groups[key]["count"] += 1
    for (phase, side, op_type, cat, cls_name), data in groups.items():
        total = totals.get((phase, side, op_type), 1) or 1
        sink.write({
            "run_id": run_id,
            "db": db,
            "scenario": scenario,
            "phase": phase,
            "client_side": side,
            "op_type": op_type,
            "error_category": cat,
            "error_class": cls_name,
            "count": data["count"],
            "share_of_errors": round(data["count"] / total, 6),
            "example_message": data["example"],
        })



def aggregate(results_dir: Path, include_fast: bool = False) -> Path:
    """Raw CSVs → results/aggregated/. Skips profile=fast by default."""
    import pandas as pd

    results_dir = Path(results_dir)
    out_dir = results_dir / "aggregated"
    out_dir.mkdir(parents=True, exist_ok=True)

    runs_path = results_dir / "runs.csv"
    if not runs_path.exists():
        raise FileNotFoundError(f"No runs.csv in {results_dir}")

    runs = pd.read_csv(runs_path)
    if "profile" in runs.columns and not include_fast:
        runs = runs[runs["profile"] != "fast"]

    ok_runs = set(runs.loc[runs["status"] == "ok", "run_id"].astype(str))

    phase_path = results_dir / "phase_summary.csv"
    metrics_path = results_dir / "scenario_metrics.csv"

    if phase_path.exists():
        phase = pd.read_csv(phase_path)
        phase = phase[phase["run_id"].astype(str).isin(ok_runs)]
        if not phase.empty:
            grouped = (
                phase.groupby(["db", "scenario", "phase", "client_side", "op_type"], dropna=False)
                .agg(
                    n_runs=("run_id", "nunique"),
                    success_rate_median=("success_rate", "median"),
                    success_rate_min=("success_rate", "min"),
                    success_rate_max=("success_rate", "max"),
                    latency_median_ms_median=("latency_median_ms", "median"),
                    throughput_median=("throughput_ops_per_sec", "median"),
                )
                .reset_index()
            )
            grouped.to_csv(out_dir / "by_db_scenario.csv", index=False, newline="")

    if metrics_path.exists():
        metrics = pd.read_csv(metrics_path)
        metrics = metrics[metrics["run_id"].astype(str).isin(ok_runs)]
        # latency vs delay for S3
        s3 = metrics[metrics["scenario"] == "s3"]
        if not s3.empty:
            s3.to_csv(out_dir / "latency_vs_delay.csv", index=False, newline="")

        # CAP summary
        rows = []
        for (db, scenario), g in metrics.groupby(["db", "scenario"]):
            for metric_name, mg in g.groupby("metric_name"):
                vals = pd.to_numeric(mg["metric_value"], errors="coerce").dropna()
                if vals.empty:
                    continue
                rows.append(
                    {
                        "db": db,
                        "scenario": scenario,
                        "hypothesis": "",
                        "observed": f"median={vals.median():.3f} min={vals.min():.3f} max={vals.max():.3f}",
                        "hypothesis_confirmed": "",
                        "evidence_metric": metric_name,
                        "evidence_value": vals.median(),
                        "n_runs": mg["run_id"].nunique(),
                    }
                )
        if rows:
            pd.DataFrame(rows).to_csv(out_dir / "cap_summary.csv", index=False, newline="")

    return out_dir
