"""Cassandra adapter: 3 nodes, replication factor 3."""

from __future__ import annotations

import hashlib
import logging
import re
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Sequence

from cassandra import (
    ConsistencyLevel,
    OperationTimedOut,
    ReadFailure,
    ReadTimeout,
    Unavailable,
    WriteFailure,
    WriteTimeout,
)
from cassandra.cluster import Cluster, NoHostAvailable, Session
from cassandra.policies import HostDistance, RoundRobinPolicy
from cassandra.query import SimpleStatement

from .. import infra
from .base import Database

log = logging.getLogger(__name__)

_DRIVER_ERRORS = (
    NoHostAvailable,
    OperationTimedOut,
    ReadTimeout,
    WriteTimeout,
    ReadFailure,
    WriteFailure,
    Unavailable,
    OSError,
    TimeoutError,
)

_STATUS_RE = re.compile(r"^(UN|DN|UJ|UL|UM|DJ|DL)\s+(\d+\.\d+\.\d+\.\d+)", re.M)


def _checksum(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _host_port(host) -> int:
    for attr in ("broadcast_rpc_port", "broadcast_port", "port"):
        value = getattr(host, attr, None)
        if isinstance(value, int) and value:
            return value
    endpoint = getattr(host, "endpoint", None)
    port = getattr(endpoint, "port", None) if endpoint is not None else None
    if isinstance(port, int) and port:
        return port
    return 9042


class _OnePortPolicy(RoundRobinPolicy):
    """Route only to the node published on this host port."""

    def __init__(self, port: int) -> None:
        super().__init__()
        self._port = int(port)

    def distance(self, host) -> HostDistance:
        if _host_port(host) == self._port:
            return HostDistance.LOCAL
        return HostDistance.IGNORED


class CassandraDB(Database):
    name = "cassandra"

    def __init__(self, cfg: dict, *, hosts: list[tuple] | None = None) -> None:
        self.cfg = cfg
        self.cs = cfg["cassandra"]
        self._nodes_meta = {n["name"]: n for n in self.cs["nodes"]}
        self._all_names = [n["name"] for n in self.cs["nodes"]]
        if hosts is None:
            self.hosts = [
                ("127.0.0.1", int(n["host_port"]), n["name"]) for n in self.cs["nodes"]
            ]
        else:
            self.hosts = hosts
        self._lock = threading.Lock()
        self._cluster: Cluster | None = None
        self._session: Session | None = None
        self.last_target_node = ""
        self._timeout_s = int(cfg["workload"]["op_timeout_ms"]) / 1000.0
        level = str(self.cs.get("consistency_level", "quorum")).upper()
        self._consistency = getattr(ConsistencyLevel, level)

    def _open(self, port: int) -> tuple[Cluster, Session]:
        cluster = Cluster(
            contact_points=["127.0.0.1"],
            port=port,
            protocol_version=4,
            connect_timeout=self._timeout_s,
            control_connection_timeout=self._timeout_s,
            load_balancing_policy=_OnePortPolicy(port),
        )
        session = cluster.connect("capbench")
        session.default_timeout = self._timeout_s
        session.default_consistency_level = self._consistency
        return cluster, session

    def connect(self) -> None:
        with self._lock:
            self._close_locked()
            last_err: BaseException | None = None
            for _host, port, name in self.hosts:
                if not _port_open(port, self._timeout_s):
                    continue
                try:
                    self._cluster, self._session = self._open(port)
                    self.last_target_node = name
                    return
                except Exception as exc:
                    last_err = exc
                    self._close_locked()
            raise last_err or RuntimeError("No Cassandra node reachable")

    def _close_locked(self) -> None:
        if self._cluster is not None:
            try:
                self._cluster.shutdown()
            except Exception:
                pass
        self._cluster = None
        self._session = None

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    def _session_or_connect(self) -> Session:
        with self._lock:
            session = self._session
        if session is not None:
            return session
        self.connect()
        assert self._session is not None
        return self._session

    def _execute(self, cql: str, params: tuple, *, consistency: ConsistencyLevel | None = None):
        stmt = SimpleStatement(cql, consistency_level=consistency or self._consistency)
        try:
            return self._session_or_connect().execute(stmt, params, timeout=self._timeout_s)
        except _DRIVER_ERRORS:
            self.close()
            raise

    def write(self, key: str, value: str) -> None:
        self._execute(
            "INSERT INTO kv (id, value, checksum, updated_at) VALUES (%s, %s, %s, %s)",
            (key, value, _checksum(value), datetime.now(timezone.utc).replace(tzinfo=None)),
        )

    def read(self, key: str) -> str | None:
        rows = self._execute("SELECT value FROM kv WHERE id = %s", (key,))
        row = rows.one() if rows is not None else None
        if row is None:
            return None
        return row.value

    def write_batch(self, records: Sequence[tuple[str, str]]) -> None:
        for key, value in records:
            self.write(key, value)
        if not records:
            return
        last_key, last_value = records[-1]

        def caught_up() -> bool:
            try:
                return all(
                    self.read_from_node(node, last_key) == last_value for node in self.nodes()
                )
            except Exception:
                return False

        ok, _ = infra.wait_for(caught_up, timeout=120, interval=0.5)
        if not ok:
            raise RuntimeError("replicas did not catch up after write_batch")

    def read_from_node(self, node: str, key: str) -> str | None:
        return self.read_keys_from_node(node, [key]).get(key)

    def read_keys_from_node(self, node: str, keys: Sequence[str]) -> dict[str, str | None]:
        out: dict[str, str | None] = {key: None for key in keys}
        if not keys:
            return out
        port = int(self._nodes_meta[node]["host_port"])
        cluster, session = self._open(port)
        try:
            step = 100
            for start in range(0, len(keys), step):
                part = list(keys[start:start + step])
                marks = ", ".join(["%s"] * len(part))
                stmt = SimpleStatement(
                    f"SELECT id, value FROM kv WHERE id IN ({marks})",
                    consistency_level=ConsistencyLevel.LOCAL_ONE,
                )
                for row in session.execute(stmt, part, timeout=self._timeout_s):
                    out[row.id] = row.value
            return out
        finally:
            cluster.shutdown()

    def minority_client(self) -> "CassandraDB":
        victim = self.victim_for_partition()
        meta = self._nodes_meta[victim]
        client = CassandraDB(
            self.cfg,
            hosts=[("127.0.0.1", int(meta["host_port"]), victim)],
        )
        client.connect()
        return client

    def nodes(self) -> list[str]:
        return list(self._all_names)

    def primary(self) -> str | None:
        for name in self._all_names:
            port = int(self._nodes_meta[name]["host_port"])
            if _port_open(port, 0.4):
                return name
        return None

    def _status_by_name(self) -> dict[str, str]:
        target = self.primary() or self._all_names[0]
        rc, out, err = infra.exec_in(target, ["nodetool", "status"], timeout=30)
        if rc != 0:
            raise RuntimeError(err or out or "nodetool status failed")
        by_ip = {ip: state for state, ip in _STATUS_RE.findall(out or "")}
        found: dict[str, str] = {}
        for name, meta in self._nodes_meta.items():
            state = by_ip.get(meta["ipv4_cluster"])
            if state:
                found[name] = state
        return found

    def cluster_state(self) -> dict[str, Any]:
        try:
            return {"coordinator": self.primary(), "status": self._status_by_name()}
        except (RuntimeError, OSError) as exc:
            return {"coordinator": self.primary(), "error": str(exc)}

    def _up_normal(self) -> int:
        try:
            return sum(1 for state in self._status_by_name().values() if state == "UN")
        except (RuntimeError, OSError):
            return 0

    def is_healthy(self) -> bool:
        return self._up_normal() >= 3

    def streaming_replica_count(self) -> int:
        return max(0, self._up_normal() - 1)

    def sync_replica_count(self) -> int:
        return self.streaming_replica_count()

    def all_nodes_running(self) -> bool:
        for name in self._all_names:
            r = infra._run(["docker", "inspect", "-f", "{{.State.Running}}", name], timeout=15)
            if (r.stdout or "").strip() != "true":
                return False
        return True

    def record_count(self) -> int:
        rows = self._execute("SELECT COUNT(*) AS n FROM kv", (), consistency=ConsistencyLevel.LOCAL_ONE)
        row = rows.one()
        return int(row.n) if row is not None else 0

    def victim_for_partition(self) -> str:
        coord = self.primary()
        others = [n for n in self._all_names if n != coord]
        if not others:
            raise RuntimeError("no node to isolate")
        return others[-1]

    def victim_for_failover(self) -> str:
        return self.primary() or self._all_names[0]

    def latency_peers(self) -> list[tuple[str, list[str]]]:
        coord = self.primary() or self._all_names[0]
        peers = [meta["ipv4_cluster"] for name, meta in self._nodes_meta.items() if name != coord]
        return [(coord, peers)]

    def external_promote(self) -> bool:
        return False

    def note_failover(self, candidate: str) -> None:
        log.info("Coordinator switch")

    def election_metric_context(self) -> str:
        return "coordinator_switch"

    def promote(self, node: str) -> None:
        return None

    def repair_hooks(self) -> None:
        target = self.primary() or self._all_names[0]
        rc, out, err = infra.exec_in(target, ["nodetool", "netstats"], timeout=30)
        text = (out or err or "").strip().splitlines()
        log.info("nodetool netstats rc=%s %s", rc, " | ".join(text[:6]))

    def rejoin(self, node: str) -> str:
        infra.start_container(node)
        return "gossip_catchup"

    def expected_behaviour(self, scenario: str) -> str:
        level = self._consistency
        if level == ConsistencyLevel.ONE:
            text = {
                "s1": "ONE can commit on one node, including the isolated one; copies may differ until heal",
                "s2": "There is no election. ONE still commits on a single remaining node",
                "s3": "ONE write latency stays lower than QUORUM because the coordinator does not wait for another node",
                "s4": "A write committed only on the isolated node must converge after rejoin",
            }
        else:
            text = {
                "s1": "QUORUM needs two of three nodes: the isolated node cannot commit, the other two can",
                "s2": "There is no election. The client switches coordinator; election_time is that switch",
                "s3": "QUORUM write latency rises with delay between the nodes",
                "s4": "After the node rejoins, copies converge; quorum writes did not commit on the isolated node",
            }
        return text.get(scenario, "")


def _port_open(port: int, timeout: float) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False
