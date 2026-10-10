#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import os
import re
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capbench import infra
from capbench.config import SUPPORTED_DBS, SUPPORTED_SCENARIOS, Clock, load_config, setup_logging
from capbench.databases import get_database
from capbench.output import (
    aggregate,
    close_sinks,
    ensure_result_sinks,
    write_errors_breakdown_from_operations,
)
from capbench.scenarios import SCENARIOS


def _run_id(db: str, scenario: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{db}_{scenario}_{ts}_{uuid.uuid4().hex[:6]}"


def cmd_verify(_args: argparse.Namespace) -> int:
    try:
        lines = infra.verify_environment()
        for line in lines:
            print(f"OK  {line}")
        print("verify: PASS")
        return 0
    except Exception as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1


def cmd_up(args: argparse.Namespace) -> int:
    db = args.db
    _check_db(db)
    cfg = load_config(args.profile)
    print(f"Starting {db} cluster...")
    if args.purge_volumes:
        infra.compose_down(db, purge_volumes=True)
    infra.compose_up(db, build=args.build)
    infra.wait_healthy(db, timeout=300, cfg=cfg)
    print(f"{db} is healthy.")
    return 0


def cmd_down(args: argparse.Namespace) -> int:
    _check_db(args.db)
    infra.compose_down(args.db, purge_volumes=args.purge_volumes)
    print("down: done")
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    _check_db(args.db)
    cfg = load_config(args.profile)
    db = get_database(args.db, cfg)
    db.connect()
    try:
        key = f"smoke_{uuid.uuid4().hex[:8]}"
        db.write(key, "ok")
        got = db.read(key)
        state = db.cluster_state()
        print(f"write/read: {got!r}")
        print(f"primary: {db.primary()}")
        print(f"cluster_state: {state}")
        if got != "ok":
            print("FAIL smoke read mismatch", file=sys.stderr)
            return 1
        if not db.is_healthy():
            print("FAIL cluster not healthy", file=sys.stderr)
            return 1
        print("smoke: PASS")
        return 0
    finally:
        db.close()


def cmd_rescue(args: argparse.Namespace) -> int:
    cfg = load_config(args.profile)
    dbs = [args.db] if args.db else list(SUPPORTED_DBS)
    for db in dbs:
        print(f"rescue {db}...")
        try:
            infra.cleanup(db, cfg)
            infra.reconnect_networks(db, cfg)
        except Exception as exc:
            print(f"  warn: {exc}")
    print("rescue: done")
    return 0


def _cassandra_levels(cfg: dict) -> list[str]:
    block = (cfg.get("consistency") or {}).get("cassandra") or {}
    raw = block.get("consistency_levels") or [block.get("consistency_level", "quorum")]
    levels = []
    for item in raw:
        name = str(item).lower()
        if name not in ("one", "quorum"):
            raise SystemExit(f"Unknown Cassandra consistency level: {item}")
        if name not in levels:
            levels.append(name)
    return levels or ["quorum"]


def _recorded_cassandra_level(text: str) -> str:
    match = re.search(
        r"consistency_level['\"]?\s*[:=]\s*['\"]?(one|quorum)",
        (text or "").lower(),
    )
    if match:
        return match.group(1)
    return "quorum"


def _redis_level(cfg: dict, scenario: str) -> list[str]:
    if scenario in ("s1", "s3"):
        return ["async", "wait"]
    return ["async"]


def _recorded_redis_level(text: str) -> str:
    if re.search(r"wait_replicas['\"]?\s*[:=]\s*['\"]?1\b", (text or "").lower()):
        return "wait"
    return "async"


def _cfg_for_level(cfg: dict, db_name: str, level: str | None) -> dict:
    if db_name == "cassandra" and level:
        run_cfg = copy.deepcopy(cfg)
        run_cfg.setdefault("cassandra", {})["consistency_level"] = level
        run_cfg.setdefault("consistency", {}).setdefault("cassandra", {})["consistency_level"] = level
        recorded = run_cfg["consistency"]["cassandra"]
        recorded.pop("consistency_levels", None)
        return run_cfg
    if db_name == "redis" and level:
        if level not in ("async", "wait"):
            raise SystemExit(f"Unknown Redis replication mode: {level}")
        run_cfg = copy.deepcopy(cfg)
        wait = 1 if level == "wait" else 0
        run_cfg.setdefault("redis", {})["wait_replicas"] = wait
        run_cfg.setdefault("consistency", {}).setdefault("redis", {})["wait_replicas"] = wait
        return run_cfg
    return cfg


def cmd_scenario(args: argparse.Namespace) -> int:
    _check_db(args.db)
    if args.scenario not in SUPPORTED_SCENARIOS:
        print(f"Unknown scenario: {args.scenario}", file=sys.stderr)
        return 2
    cfg = load_config(args.profile)
    cfg = _cfg_for_level(cfg, args.db, getattr(args, "consistency", None))
    runs = args.runs or int(cfg["runs_per_scenario"][args.scenario])
    rc = 0
    for i in range(runs):
        level = ""
        if args.db == "cassandra":
            level = str(cfg["cassandra"].get("consistency_level", "quorum"))
        elif args.db == "redis":
            level = "wait" if int(cfg.get("redis", {}).get("wait_replicas", 0)) else "async"
        label = f" {level}" if level else ""
        print(f"=== {args.db} {args.scenario}{label} run {i+1}/{runs} (profile={cfg['profile']}) ===")
        try:
            _one_scenario(args.db, args.scenario, cfg, purge=True)
        except Exception as exc:
            print(f"Run failed: {exc}", file=sys.stderr)
            traceback.print_exc()
            rc = 1
            # continue remaining runs
    return rc


def _one_scenario(db_name: str, scenario: str, cfg: dict, purge: bool = True) -> None:
    run_id = _run_id(db_name, scenario)
    setup_logging(run_id)
    results_dir = ROOT / "results"
    sinks = ensure_result_sinks(results_dir, run_id)

    consistency = cfg.get("consistency", {}).get(db_name, {})
    if db_name == "cassandra":
        level_name = str(cfg.get("cassandra", {}).get("consistency_level", "quorum"))
        consistency = {
            "consistency_level": level_name,
            "replication_factor": consistency.get("replication_factor", 3),
        }
        run_level = level_name
    elif db_name == "redis":
        wait = int(cfg.get("redis", {}).get("wait_replicas", 0))
        consistency = {"replication": "async", "wait_replicas": wait}
        run_level = "wait" if wait else "async"
    else:
        run_level = str(consistency)
    run_meta = {
        "run_id": run_id,
        "db": db_name,
        "scenario": scenario,
        "consistency_level": run_level,
        "fault_level": "",
        "fault_mode": "",
    }

    started = Clock.utc_now_iso()
    status = "ok"
    abort_reason = ""
    image_tag = f"capbench-{db_name}:local"
    driver_version = ""
    try:
        if db_name == "mongo":
            import pymongo
            driver_version = getattr(pymongo, "version", "")
        elif db_name == "cassandra":
            import cassandra
            driver_version = getattr(cassandra, "__version__", "")
        elif db_name == "redis":
            import redis as redis_mod
            driver_version = getattr(redis_mod, "__version__", "")
        else:
            import psycopg2
            driver_version = getattr(psycopg2, "__version__", "")
    except Exception:
        pass

    try:
        if purge:
            infra.compose_down(db_name, purge_volumes=True)
            infra.compose_up(db_name)
            infra.wait_healthy(db_name, timeout=300, cfg=cfg)

        db = get_database(db_name, cfg)
        db.connect()
        mod = SCENARIOS[scenario]
        result = mod.run(db, cfg, sinks, run_meta)
        status = result.get("status", "ok")
        abort_reason = result.get("abort_reason", "")
        db.close()
    except Exception as exc:
        status = "aborted"
        abort_reason = str(exc)[:500]
        try:
            infra.dump_logs(db_name, ROOT / "logs" / run_id, cfg=cfg)
        except Exception:
            pass
        raise
    finally:
        try:
            sinks["operations"].flush()
            write_errors_breakdown_from_operations(
                results_dir / f"operations_{run_id}.csv",
                sinks["errors_breakdown"],
                run_id,
            )
        except Exception:
            pass
        finished = Clock.utc_now_iso()
        ok_d, docker_ver = infra.docker_available()
        sinks["runs"].write({
            "run_id": run_id,
            "db": db_name,
            "scenario": scenario,
            "started_at": started,
            "finished_at": finished,
            "status": status,
            "image_tag": image_tag,
            "driver_version": driver_version,
            "consistency_config": str(consistency),
            "retry_config": "disabled",
            "phase_config": str(cfg.get("phase_durations")),
            "random_seed": cfg.get("random_seed"),
            "threads": cfg["workload"]["threads"],
            "value_size_bytes": cfg["workload"]["value_size_bytes"],
            "key_space_size": cfg["workload"]["key_space_size"],
            "fault_mode": run_meta.get("fault_mode", ""),
            "poll_interval_ms": cfg.get("poll_interval_ms", 100),
            "docker_version": docker_ver if ok_d else "",
            "wsl_memory_gb": infra.docker_memory_gb(),
            "cpu_count": os.cpu_count() or "",
            "abort_reason": abort_reason,
            "profile": cfg.get("profile", "full"),
        })
        close_sinks(sinks)


def cmd_matrix(args: argparse.Namespace) -> int:
    cfg = load_config(args.profile)
    import csv
    from collections import Counter
    import random

    rng = random.Random(int(cfg["random_seed"]))
    done_counts: Counter[tuple[str, str, str]] = Counter()
    if args.resume:
        runs_path = ROOT / "results" / "runs.csv"
        if runs_path.exists():
            with runs_path.open(newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if row.get("status") != "ok" or row.get("profile") != cfg["profile"]:
                        continue
                    level = ""
                    if row.get("db") == "cassandra":
                        level = _recorded_cassandra_level(row.get("consistency_config", ""))
                    elif row.get("db") == "redis":
                        level = _recorded_redis_level(row.get("consistency_config", ""))
                    done_counts[(row["db"], row["scenario"], level)] += 1

    dbs = [args.db]
    combos: list[tuple[str, str, str, int]] = []
    for db in dbs:
        for sc in SUPPORTED_SCENARIOS:
            if db == "cassandra":
                levels = _cassandra_levels(cfg)
            elif db == "redis":
                levels = _redis_level(cfg, sc)
            else:
                levels = [""]
            n = int(cfg["runs_per_scenario"][sc])
            for level in levels:
                already = done_counts[(db, sc, level)] if args.resume else 0
                for i in range(already, n):
                    combos.append((db, sc, level, i))
    rng.shuffle(combos)

    if not combos:
        print("matrix: nothing left to run")
        return 0

    rc = 0
    for db, sc, level, i in combos:
        label = f" {level}" if level else ""
        print(f"matrix: {db} {sc}{label} #{i+1}")
        try:
            _one_scenario(db, sc, _cfg_for_level(cfg, db, level or None), purge=True)
        except Exception as exc:
            print(f"failed: {exc}", file=sys.stderr)
            rc = 1
    return rc


def cmd_aggregate(args: argparse.Namespace) -> int:
    out = aggregate(ROOT / "results", include_fast=args.include_fast)
    print(f"Wrote aggregates to {out}")
    return 0


def _check_db(db: str) -> None:
    if db not in SUPPORTED_DBS:
        raise SystemExit(
            f"Database '{db}' not available yet. This phase supports only: {', '.join(SUPPORTED_DBS)}"
        )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="CAP theorem benchmark (PostgreSQL phase)")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_profile(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--profile", default="full", choices=["full", "fast"])

    sp = sub.add_parser("verify", help="Environment gate")
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser("up", help="Start cluster")
    add_profile(sp)
    sp.add_argument("--db", required=True)
    sp.add_argument("--purge-volumes", action="store_true")
    sp.add_argument("--build", action="store_true", help="Rebuild images (hits Docker Hub)")
    sp.set_defaults(func=cmd_up)

    sp = sub.add_parser("down", help="Stop cluster")
    sp.add_argument("--db", required=True)
    sp.add_argument("--purge-volumes", action="store_true")
    sp.set_defaults(func=cmd_down)

    sp = sub.add_parser("smoke", help="One write + read + cluster state")
    add_profile(sp)
    sp.add_argument("--db", required=True)
    sp.set_defaults(func=cmd_smoke)

    sp = sub.add_parser("scenario", help="Run one scenario")
    add_profile(sp)
    sp.add_argument("--db", required=True)
    sp.add_argument("--scenario", required=True, choices=list(SUPPORTED_SCENARIOS))
    sp.add_argument("--runs", type=int, default=None)
    sp.add_argument("--consistency", choices=["one", "quorum", "async", "wait"], default=None)
    sp.set_defaults(func=cmd_scenario)

    sp = sub.add_parser("matrix", help="Full matrix for supported DBs")
    add_profile(sp)
    sp.add_argument("--db", required=True, choices=list(SUPPORTED_DBS))
    sp.add_argument("--resume", action="store_true")
    sp.set_defaults(func=cmd_matrix)

    sp = sub.add_parser("aggregate", help="Build aggregated tables")
    sp.add_argument("--include-fast", action="store_true")
    sp.set_defaults(func=cmd_aggregate)

    sp = sub.add_parser("rescue", help="Emergency cleanup of tc/networks")
    add_profile(sp)
    sp.add_argument("--db", default=None)
    sp.set_defaults(func=cmd_rescue)

    args = p.parse_args(argv)
    if not hasattr(args, "profile"):
        args.profile = "full"
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
