"""PostgreSQL adapter: sync replication, external promote, pg_rewind rejoin."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Sequence

import psycopg2

from .. import infra
from .base import Database

log = logging.getLogger(__name__)

_PG_ERRORS = (psycopg2.Error, OSError, TimeoutError)

PGDATA = "/var/lib/postgresql/data"
PG_CTL = "/usr/lib/postgresql/16/bin/pg_ctl"
PG_REWIND = "/usr/lib/postgresql/16/bin/pg_rewind"
PG_BASEBACKUP = "/usr/lib/postgresql/16/bin/pg_basebackup"


def _checksum(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


_UPSERT_SQL = """
INSERT INTO kv(key, value, checksum, updated_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (key) DO UPDATE
  SET value = EXCLUDED.value,
      checksum = EXCLUDED.checksum,
      updated_at = now()
"""


class PostgresDB(Database):
    name = "postgres"

    def __init__(self, cfg: dict, *, hosts: list[str] | None = None, minority_of: str | None = None) -> None:
        self.cfg = cfg
        self.pg = cfg["postgres"]
        self._nodes_meta = {n["name"]: n for n in self.pg["nodes"]}
        self._all_names = [n["name"] for n in self.pg["nodes"]]
        # host ports
        if hosts is None:
            self.hosts = [
                ("127.0.0.1", int(n["host_port"]), n["name"]) for n in self.pg["nodes"]
            ]
        else:
            self.hosts = hosts
        self._minority_of = minority_of
        self._conn = None
        self.last_target_node = ""
        self._timeout_s = float(cfg["workload"]["op_timeout_ms"]) / 1000.0

    def _connect_kwargs(self, host: str, port: int) -> dict:
        return {
            "host": host,
            "port": port,
            "user": self.pg["user"],
            "password": self.pg["password"],
            "dbname": self.pg["database"],
            "connect_timeout": max(1, int(self._timeout_s)),
            "options": f"-c statement_timeout={int(self._timeout_s * 1000)}",
        }

    def connect(self) -> None:
        self.close()
        last_err = None
        # Prefer current primary if discoverable; else try all hosts
        order = list(self.hosts)
        for host, port, name in order:
            try:
                conn = psycopg2.connect(**self._connect_kwargs(host, port))
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery()")
                    in_recovery = cur.fetchone()[0]
                if in_recovery and self._minority_of is None:
                    conn.close()
                    continue
                self._conn = conn
                self.last_target_node = name
                return
            except _PG_ERRORS as exc:
                last_err = exc
                continue
        # any reachable node
        for host, port, name in order:
            try:
                conn = psycopg2.connect(**self._connect_kwargs(host, port))
                conn.autocommit = True
                self._conn = conn
                self.last_target_node = name
                return
            except _PG_ERRORS as exc:
                last_err = exc
        raise last_err or RuntimeError("No PostgreSQL host reachable")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except psycopg2.Error:
                pass
            self._conn = None

    def _ensure(self):
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn

    def write(self, key: str, value: str) -> None:
        conn = self._ensure()
        try:
            with conn.cursor() as cur:
                cur.execute(_UPSERT_SQL, (key, value, _checksum(value)))
        except psycopg2.Error:
            if self._conn is not None and self._conn.closed:
                self._conn = None
            raise

    def read(self, key: str) -> str | None:
        conn = self._ensure()
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM kv WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None

    def write_batch(self, records: Sequence[tuple[str, str]]) -> None:
        conn = self._ensure()
        with conn.cursor() as cur:
            for key, value in records:
                cur.execute(_UPSERT_SQL, (key, value, _checksum(value)))

    def read_from_node(self, node: str, key: str) -> str | None:
        meta = self._nodes_meta[node]
        conn = psycopg2.connect(
            **self._connect_kwargs("127.0.0.1", int(meta["host_port"]))
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM kv WHERE key = %s", (key,))
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            conn.close()

    def read_keys_from_node(self, node: str, keys: Sequence[str]) -> dict[str, str | None]:
        meta = self._nodes_meta[node]
        conn = psycopg2.connect(
            **self._connect_kwargs("127.0.0.1", int(meta["host_port"]))
        )
        conn.autocommit = True
        out: dict[str, str | None] = {}
        try:
            with conn.cursor() as cur:
                for key in keys:
                    cur.execute("SELECT value FROM kv WHERE key = %s", (key,))
                    row = cur.fetchone()
                    out[key] = row[0] if row else None
            return out
        finally:
            conn.close()

    def minority_client(self) -> "PostgresDB":
        victim = self.victim_for_partition()
        meta = self._nodes_meta[victim]
        hosts = [("127.0.0.1", int(meta["host_port"]), victim)]
        client = PostgresDB(self.cfg, hosts=hosts, minority_of=victim)
        client.connect()
        return client

    def nodes(self) -> list[str]:
        return list(self._all_names)

    def primary(self) -> str | None:
        for name in self._all_names:
            meta = self._nodes_meta[name]
            try:
                conn = psycopg2.connect(
                    **self._connect_kwargs("127.0.0.1", int(meta["host_port"]))
                )
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery()")
                    in_recovery = cur.fetchone()[0]
                conn.close()
                if not in_recovery:
                    return name
            except _PG_ERRORS:
                continue
        return None

    def cluster_state(self) -> dict[str, Any]:
        state: dict[str, Any] = {"primary": self.primary(), "replication": {}}
        prim = state["primary"]
        if not prim:
            return state
        meta = self._nodes_meta[prim]
        try:
            conn = psycopg2.connect(
                **self._connect_kwargs("127.0.0.1", int(meta["host_port"]))
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT application_name, state, sync_state, write_lag, flush_lag, replay_lag "
                    "FROM pg_stat_replication"
                )
                rows = cur.fetchall()
                state["replication"] = [
                    {
                        "application_name": r[0],
                        "state": r[1],
                        "sync_state": r[2],
                        "write_lag": str(r[3]),
                        "flush_lag": str(r[4]),
                        "replay_lag": str(r[5]),
                    }
                    for r in rows
                ]
            conn.close()
        except psycopg2.Error as exc:
            state["error"] = str(exc)
        return state

    def is_healthy(self) -> bool:
        try:
            return infra._pg_sync_replicas(self.cfg) >= 1 and self.primary() is not None
        except (psycopg2.Error, infra.InfraError):
            return False

    def enable_sync_replication(self) -> None:
        infra._pg_enable_sync_standbys(self.cfg)

    def streaming_replica_count(self) -> int:
        return infra._pg_connected_replicas(self.cfg)

    def sync_replica_count(self) -> int:
        return infra._pg_sync_replicas(self.cfg)

    def all_nodes_running(self) -> bool:
        return infra.postgres_nodes_running(self.cfg)

    def record_count(self) -> int:
        conn = self._ensure()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM kv")
            return int(cur.fetchone()[0])

    def victim_for_partition(self) -> str:
        replicas = [n["name"] for n in self.pg["nodes"] if n.get("role") == "replica"]
        if not replicas:
            raise RuntimeError("no replica in config to isolate")
        return replicas[-1]

    def victim_for_failover(self) -> str:
        return self.primary() or self._all_names[0]

    def latency_peers(self) -> list[tuple[str, list[str]]]:
        prim = self.primary() or self._all_names[0]
        peers = [meta["ipv4_cluster"] for name, meta in self._nodes_meta.items() if name != prim]
        return [(prim, peers)]

    def _app_name(self, node: str) -> str:
        app = self._nodes_meta[node].get("application_name")
        if not app:
            raise RuntimeError(f"missing application_name for {node}")
        return app

    def _pg_exec(self, node: str, args: list[str], timeout: float = 60) -> tuple[int, str, str]:
        """Run a Postgres server tool as the postgres user (not a login shell)."""
        return infra.exec_in(node, ["gosu", "postgres", *args], timeout=timeout)

    def _cluster_ip(self, node: str) -> str:
        return self._nodes_meta[node]["ipv4_cluster"]

    def promote(self, node: str) -> None:
        """External failover: pg_ctl promote inside container; re-point remaining replicas."""
        log.info("Promoting %s via pg_ctl promote", node)
        rc, out, err = self._pg_exec(
            node,
            [PG_CTL, "promote", "-D", PGDATA, "-W"],
            timeout=60,
        )
        if rc != 0:
            raise RuntimeError(f"promote failed on {node}: {err or out}")

        def promoted() -> bool:
            return self.primary() == node

        ok, _ = infra.wait_for(promoted, timeout=60, interval=0.2)
        if not ok:
            raise RuntimeError(f"{node} did not become primary")

        new_ip = self._cluster_ip(node)
        for name in self._all_names:
            if name == node:
                continue
            running = infra._run(
                ["docker", "inspect", "-f", "{{.State.Running}}", name], timeout=15
            )
            if (running.stdout or "").strip() != "true":
                continue
            app = self._app_name(name)
            rc, out, err = infra.exec_in(
                name,
                [
                    "bash", "-c",
                    "sed -i '/^primary_conninfo/d' "
                    f"{PGDATA}/postgresql.auto.conf && "
                    f"echo \"primary_conninfo = 'host={new_ip} port=5432 user=replicator "
                    f"password=repl application_name={app}'\" >> {PGDATA}/postgresql.auto.conf",
                ],
                timeout=30,
            )
            if rc != 0:
                raise RuntimeError(f"failed to repoint {name}: {err or out}")
            restarted = infra._run(["docker", "restart", name], timeout=120)
            if restarted.returncode != 0:
                raise RuntimeError(f"failed to restart {name} after repoint: {restarted.stderr}")

        ok, _ = infra.wait_for(lambda: infra._pg_connected_replicas(self.cfg) >= 1, timeout=60, interval=1.0)
        if not ok:
            raise RuntimeError("no streaming replica after promote")
        infra._pg_enable_sync_standbys(self.cfg)

    def _rewind_offline(self, node: str, volume: str, script: str, timeout: float) -> tuple[int, str, str]:
        """Run rewind/basebackup while the node container is stopped (postgres is PID 1)."""
        infra._run(["docker", "stop", "-t", "1", node], timeout=30)
        result = infra._run(
            [
                "docker", "run", "--rm",
                "--entrypoint", "bash",
                "--network", self.pg["networks"]["cluster"],
                "-v", f"{volume}:{PGDATA}",
                "capbench-postgres:local",
                "-c", script,
            ],
            timeout=timeout,
        )
        return result.returncode, result.stdout or "", result.stderr or ""

    def rejoin(self, node: str) -> str:
        """Bring old primary back as standby via pg_rewind; fallback pg_basebackup."""
        new_primary = self.primary()
        if not new_primary or new_primary == node:
            raise RuntimeError("No new primary to rejoin against")

        source_ip = self._cluster_ip(new_primary)
        app = self._app_name(node)
        volume = f"capbench_postgres_{node}-data"
        source = (
            f"host={source_ip} port=5432 user=cap password=cap dbname=capbench"
        )
        conninfo = (
            f"host={source_ip} port=5432 user=replicator password=repl application_name={app}"
        )
        rewind_script = (
            f"gosu postgres {PG_REWIND} --target-pgdata={PGDATA} --source-server='{source}' "
            f"&& touch {PGDATA}/standby.signal "
            f"&& sed -i '/^primary_conninfo/d' {PGDATA}/postgresql.auto.conf "
            f"&& echo \"primary_conninfo = '{conninfo}'\" >> {PGDATA}/postgresql.auto.conf "
            f"&& chown -R postgres:postgres {PGDATA}"
        )
        rc, out, err = self._rewind_offline(node, volume, rewind_script, timeout=180)
        method = "pg_rewind"
        if rc != 0:
            log.warning("pg_rewind failed (%s); falling back to pg_basebackup", err or out)
            method = "pg_basebackup"
            backup_script = (
                f"find {PGDATA} -mindepth 1 -delete && "
                f"gosu postgres env PGPASSWORD=repl {PG_BASEBACKUP} -h {source_ip} -U replicator "
                f"-D {PGDATA} -Fp -Xs -P -R && "
                f"sed -i '/^primary_conninfo/d' {PGDATA}/postgresql.auto.conf && "
                f"echo \"primary_conninfo = '{conninfo}'\" >> {PGDATA}/postgresql.auto.conf && "
                f"touch {PGDATA}/standby.signal && chown -R postgres:postgres {PGDATA}"
            )
            rc2, out2, err2 = self._rewind_offline(node, volume, backup_script, timeout=300)
            if rc2 != 0:
                raise RuntimeError(f"rejoin failed: {err2 or out2}")

        infra.start_container(node)
        return method

    def expected_behaviour(self, scenario: str) -> str:
        return {
            "s1": "CP: minority rejects/times out writes; majority remains available with sync commit",
            "s2": "External promote; detection_time system-comparable; election_time includes script reaction",
            "s3": "Write latency rises with inter-node delay under synchronous_commit=on",
            "s4": "After rejoin via pg_rewind/basebackup, nodes converge; no stale majority divergence",
        }.get(scenario, "")
