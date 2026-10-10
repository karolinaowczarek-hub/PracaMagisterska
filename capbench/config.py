"""Load and validate config.yaml; profiles fast/full; logging; Clock."""

from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT / "config.yaml"

SUPPORTED_DBS = ("postgres", "mongo", "cassandra", "redis")
SUPPORTED_SCENARIOS = ("s1", "s2", "s3", "s4")


class ConfigError(Exception):
    pass


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_config(profile: str = "full", path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or DEFAULT_CONFIG_PATH
    if not cfg_path.exists():
        raise ConfigError(f"Missing config: {cfg_path}")
    with cfg_path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    required = [
        "phase_durations",
        "workload",
        "runs_per_scenario",
        "netem_levels_ms",
        "postgres",
        "random_seed",
    ]
    missing = [k for k in required if k not in raw]
    if missing:
        raise ConfigError(f"config.yaml missing keys: {missing}")

    profiles = raw.get("profiles") or {}
    if profile not in ("full", "fast") and profile not in profiles:
        raise ConfigError(f"Unknown profile: {profile}")

    cfg = copy.deepcopy(raw)
    # Apply profile overlays (full is identity; fast merges overrides)
    overlay = profiles.get(profile) or {}
    if overlay:
        cfg = _deep_merge(cfg, overlay)

    cfg["profile"] = profile
    cfg["config_path"] = str(cfg_path)
    return cfg


def setup_logging(run_id: str, logs_dir: Path | None = None) -> Path:
    logs_dir = logs_dir or (ROOT / "logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"{run_id}.log"

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Force UTC timestamps
    fmt.converter = lambda ts: datetime.fromtimestamp(ts, tz=timezone.utc).timetuple()

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path


class Clock:
    """perf_counter for durations; UTC datetime only for presentation marks."""

    def __init__(self) -> None:
        self._marks: dict[str, float] = {}

    def mark(self, name: str) -> float:
        t = perf_counter()
        self._marks[name] = t
        return t

    def elapsed_since(self, name: str) -> float:
        if name not in self._marks:
            raise KeyError(f"Unknown mark: {name}")
        return perf_counter() - self._marks[name]

    def elapsed_ms_since(self, name: str) -> float:
        return self.elapsed_since(name) * 1000.0

    @staticmethod
    def utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    def has(self, name: str) -> bool:
        return name in self._marks
