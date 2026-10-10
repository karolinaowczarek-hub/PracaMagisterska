"""Redis Cluster adapter: 3 primaries, 3 replicas, async replication."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

import redis
from redis.backoff import NoBackoff
from redis.cluster import ClusterNode, RedisCluster
from redis.crc import key_slot
from redis.exceptions import AskError, ConnectionError as RedisConnectionError
from redis.exceptions import MovedError, RedisClusterException, RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from .. import infra
from .base import Database

log = logging.getLogger(__name__)

_LINK_ERRORS = (RedisConnectionError, RedisTimeoutError, RedisClusterException, OSError, TimeoutError)
_DATA_TAG = b"capbench"
_DATA_SLOT = key_slot(_DATA_TAG)


def _stored_key(key: str) -> str:
    return "{capbench}" + key


def _redirect(exc: BaseException) -> bool:
    text = str(exc).lstrip().upper()
    return text.startswith("MOVED") or text.startswith("ASK")


@dataclass
class _NodeRow:
    node_id: str
    name: str
    flags: set[str]
    master_id: str
    link: str
    slots: list[str]

    @property
    def is_primary(self) -> bool:
        return "master" in self.flags

    @property
    def is_replica(self) -> bool:
        return "slave" in self.flags or "replica" in self.flags

    @property
    def failed(self) -> bool:
        return "fail" in self.flags

    def owns(self, slot: int) -> bool:
        for token in self.slots:
            if token.startswith("["):
                continue
            if "-" in token:
                start, end = token.split("-", 1)
                if int(start) <= slot <= int(end):
                    return True
            elif token.isdigit() and int(token) == slot:
                return True
        return False


class RedisDB(Database):
    name = "redis"

    def __init__(self, cfg: dict, *, hosts: list[tuple] | None = None, minority: bool = False) -> None:
        self.cfg = cfg
        self.rd = cfg["redis"]
        self._nodes_meta = {n["name"]: n for n in self.rd["nodes"]}
        self._by_ip = {n["ipv4_cluster"]: n["name"] for n in self.rd["nodes"]}
        self._all_names = [n["name"] for n in self.rd["nodes"]]
        if hosts is None:
            self.hosts = [("127.0.0.1", int(n["host_port"]), n["name"]) for n in self.rd["nodes"]]
        else:
            self.hosts = hosts
        self._minority = minority
        self._wait = int(self.rd.get("wait_replicas", 0)) > 0
        self._timeout_s = int(cfg["workload"]["op_timeout_ms"]) / 1000.0
        self._client: redis.Redis | RedisCluster | None = None
        self._isolated: str | None = None
        self._excluded: str | None = None
        self._arm_exclude = False
        self._watched_name: str | None = None
        self._watched_slot: int | None = None
        self._majority: dict[str, redis.Redis] = {}
        self.last_target_node = ""

    def connect(self) -> None:
        self.close()
        if self._arm_exclude and self._isolated:
            self._excluded = self._isolated
        if self._minority:
            _host, port, name = self.hosts[0]
            self._client = self._open(port)
            self._client.ping()
            self.last_target_node = name
            return
        startup = [
            ClusterNode(host, port)
            for host, port, name in self.hosts
            if name != self._excluded
        ]
        if not startup:
            raise RuntimeError("no Redis node left for the majority client")
        if self._excluded:
            self._ping_majority()
            return
        self._client = RedisCluster(
            startup_nodes=startup,
            address_remap=self._remap,
            decode_responses=True,
            require_full_coverage=False,
            cluster_error_retry_attempts=0,
            retry=Retry(NoBackoff(), 0),
            dynamic_startup_nodes=False,
            reinitialize_steps=1,
            socket_timeout=self._timeout_s,
            socket_connect_timeout=self._timeout_s,
        )
        self.last_target_node = self.primary() or ""

    def note_healed(self) -> None:
        self._arm_exclude = False
        self._excluded = None
        if not self._minority:
            self.connect()

    def _healthy_names(self) -> list[str]:
        return [name for name in self._all_names if name != self._excluded]

    def _ping_majority(self) -> None:
        last: BaseException | None = None
        self._majority = {}
        for name in self._healthy_names():
            client = self._open(int(self._nodes_meta[name]["host_port"]))
            try:
                client.ping()
            except _LINK_ERRORS as exc:
                last = exc
                client.close()
                continue
            self._majority[name] = client
        if not self._majority:
            raise last or RedisClusterException("no majority node reachable")
        self.last_target_node = next(iter(self._majority))

    def _on_majority(self, fn):
        if not self._majority:
            self._ping_majority()
        last: BaseException | None = None
        for name, client in self._majority.items():
            try:
                self.last_target_node = name
                return self._follow_once(client, fn)
            except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
                last = exc
        raise last or RedisClusterException("no majority node reachable")

    def _follow_once(self, client: redis.Redis, fn):
        try:
            return fn(client)
        except (MovedError, AskError) as exc:
            target = self._by_ip.get(exc.host)
            if target is None or target == self._excluded:
                raise
            other = self._majority.get(target)
            if other is None:
                raise
            self.last_target_node = target
            return fn(other)

    def close(self) -> None:
        for client in self._majority.values():
            client.close()
        self._majority = {}
        if self._client is not None:
            self._client.close()
            self._client = None

    def _open(self, port: int) -> redis.Redis:
        return redis.Redis(
            host="127.0.0.1",
            port=port,
            socket_connect_timeout=self._timeout_s,
            socket_timeout=self._timeout_s,
            decode_responses=True,
            max_connections=8,
        )

    def _remap(self, address: tuple[str, int]) -> tuple[str, int]:
        host, port = address
        name = self._by_ip.get(host)
        if name is None:
            return ("127.0.0.1", port)
        return ("127.0.0.1", int(self._nodes_meta[name]["host_port"]))

    def _db(self) -> redis.Redis | RedisCluster:
        if self._client is None:
            self.connect()
        assert self._client is not None
        return self._client

    def _put(self, client: redis.Redis, key: str, value: str) -> None:
        client.hset(key, mapping={"value": value, "checksum": value[:16]})
        if not self._wait:
            return
        acked = int(client.execute_command("WAIT", 1, int(self._timeout_s * 1000)))
        if acked < 1:
            raise TimeoutError("WAIT 1 not enough good replicas")

    def write(self, key: str, value: str) -> None:
        stored = _stored_key(key)
        try:
            if self._minority:
                client = self._db()
                assert isinstance(client, redis.Redis)
                self._put(client, stored, value)
                return
            if self._excluded:
                self._on_majority(lambda client: self._put(client, stored, value))
                return
            client = self._db()
            assert isinstance(client, RedisCluster)
            node = client.get_node_from_key(stored)
            self.last_target_node = self._by_ip.get(node.host, node.host)
            self._put(client.get_redis_connection(node), stored, value)
        except (MovedError, AskError):
            raise
        except _LINK_ERRORS:
            self.close()
            raise
        except RedisError:
            if not self._excluded:
                self.close()
            raise

    def read(self, key: str) -> str | None:
        stored = _stored_key(key)
        try:
            if self._minority:
                client = self._db()
                assert isinstance(client, redis.Redis)
                return client.hget(stored, "value")
            if self._excluded:
                return self._on_majority(lambda client: client.hget(stored, "value"))
            client = self._db()
            assert isinstance(client, RedisCluster)
            node = client.get_node_from_key(stored)
            self.last_target_node = self._by_ip.get(node.host, node.host)
            return client.get_redis_connection(node).hget(stored, "value")
        except (MovedError, AskError):
            raise
        except _LINK_ERRORS:
            self.close()
            raise
        except RedisError:
            if not self._excluded:
                self.close()
            raise

    def write_batch(self, records: Sequence[tuple[str, str]]) -> None:
        for key, value in records:
            self.write(key, value)
        if not records:
            return
        last_key, last_value = records[-1]
        replica = self._replica_of_slot(_DATA_SLOT)

        def caught_up() -> bool:
            if replica is None:
                return False
            try:
                return self.read_from_node(replica, last_key) == last_value
            except RedisError:
                return False

        ok, _ = infra.wait_for(caught_up, timeout=120, interval=0.5)
        if not ok:
            raise RuntimeError("replica did not catch up after write_batch")

    def read_from_node(self, node: str, key: str) -> str | None:
        found = self.read_keys_from_node(node, [key])
        if key not in found:
            return None
        return found[key]

    def read_keys_from_node(self, node: str, keys: Sequence[str]) -> dict[str, str | None]:
        out: dict[str, str | None] = {}
        if not keys:
            return out
        client = self._open(int(self._nodes_meta[node]["host_port"]))
        try:
            try:
                client.execute_command("READONLY")
            except RedisError:
                pass
            probe = keys[0]
            try:
                first = client.hget(_stored_key(probe), "value")
            except (MovedError, AskError):
                return {}
            except ResponseError as exc:
                if _redirect(exc):
                    return {}
                raise
            out[probe] = first
            rest = list(keys[1:])
            step = 500
            for start in range(0, len(rest), step):
                part = rest[start:start + step]
                pipe = client.pipeline(transaction=False)
                for key in part:
                    pipe.hget(_stored_key(key), "value")
                for key, value in zip(part, pipe.execute()):
                    out[key] = value
            return out
        finally:
            client.close()

    def minority_client(self) -> "RedisDB":
        victim = self._isolated or self.victim_for_partition()
        meta = self._nodes_meta[victim]
        client = RedisDB(
            self.cfg,
            hosts=[("127.0.0.1", int(meta["host_port"]), victim)],
            minority=True,
        )
        client._wait = self._wait
        client.connect()
        return client

    def nodes(self) -> list[str]:
        return list(self._all_names)

    def _rows(self) -> list[_NodeRow]:
        last: BaseException | None = None
        ordered = [name for name in self._all_names if name != self._excluded]
        ordered += [name for name in self._all_names if name == self._excluded]
        for name in ordered:
            if self._watched_name == name and not self._running(name):
                continue
            port = int(self._nodes_meta[name]["host_port"])
            client = self._open(port)
            try:
                text = client.execute_command("CLUSTER", "NODES")
            except _LINK_ERRORS as exc:
                last = exc
                continue
            finally:
                client.close()
            return self._parse_nodes(str(text))
        raise RuntimeError(f"CLUSTER NODES failed: {last}")

    def _parse_nodes(self, text: str) -> list[_NodeRow]:
        rows: list[_NodeRow] = []
        for line in text.splitlines():
            parts = line.split()
            if len(parts) < 8:
                continue
            ip = parts[1].split("@", 1)[0].rsplit(":", 1)[0]
            name = self._by_ip.get(ip)
            if name is None:
                continue
            rows.append(
                _NodeRow(
                    node_id=parts[0],
                    name=name,
                    flags=set(parts[2].split(",")),
                    master_id=parts[3],
                    link=parts[7],
                    slots=parts[8:],
                )
            )
        return rows

    def _owner_name(self, slot: int) -> str | None:
        for row in self._rows():
            if row.is_primary and not row.failed and row.owns(slot):
                return row.name
        return None

    def _replica_of_slot(self, slot: int) -> str | None:
        rows = self._rows()
        owner_id = ""
        for row in rows:
            if row.is_primary and row.owns(slot):
                owner_id = row.node_id
                break
        for row in rows:
            if row.is_replica and row.master_id == owner_id and not row.failed:
                return row.name
        return None

    def _cluster_info(self) -> dict[str, str]:
        last: BaseException | None = None
        for name in self._all_names:
            port = int(self._nodes_meta[name]["host_port"])
            client = self._open(port)
            try:
                text = str(client.execute_command("CLUSTER", "INFO"))
            except _LINK_ERRORS as exc:
                last = exc
                continue
            finally:
                client.close()
            info: dict[str, str] = {}
            for line in text.splitlines():
                if ":" in line:
                    key, value = line.split(":", 1)
                    info[key.strip()] = value.strip()
            return info
        raise RuntimeError(f"CLUSTER INFO failed: {last}")

    def _connected_replicas(self) -> int:
        try:
            rows = self._rows()
        except (RuntimeError, OSError):
            return 0
        return sum(1 for row in rows if row.is_replica and not row.failed and row.link == "connected")

    def primary(self) -> str | None:
        slot = self._watched_slot if self._watched_slot is not None else _DATA_SLOT
        try:
            owner = self._owner_name(slot)
        except (RuntimeError, OSError):
            return None
        if owner is None:
            return None
        if self._watched_name and owner == self._watched_name and not self._running(owner):
            return None
        return owner

    def cluster_state(self) -> dict[str, Any]:
        try:
            rows = self._rows()
            info = self._cluster_info()
        except (RuntimeError, OSError) as exc:
            return {"error": str(exc)}
        nodes = {
            row.name: {
                "role": "primary" if row.is_primary else "replica",
                "failed": row.failed,
                "link": row.link,
            }
            for row in rows
        }
        return {
            "cluster_state": info.get("cluster_state"),
            "slots_ok": info.get("cluster_slots_ok"),
            "primary": self.primary(),
            "nodes": nodes,
        }

    def _replicated(self) -> bool:
        try:
            info = self._cluster_info()
        except (RuntimeError, OSError):
            return False
        return (
            info.get("cluster_state") == "ok"
            and info.get("cluster_slots_ok") == "16384"
            and self._connected_replicas() >= 3
        )

    def is_healthy(self) -> bool:
        return self._replicated()

    def streaming_replica_count(self) -> int:
        return 3 if self._replicated() else 0

    def sync_replica_count(self) -> int:
        return self.streaming_replica_count()

    def _running(self, name: str) -> bool:
        result = infra._run(["docker", "inspect", "-f", "{{.State.Running}}", name], timeout=15)
        return (result.stdout or "").strip() == "true"

    def all_nodes_running(self) -> bool:
        return all(self._running(name) for name in self._all_names)

    def record_count(self) -> int:
        owner = self.primary()
        if owner is None:
            return 0
        client = self._open(int(self._nodes_meta[owner]["host_port"]))
        try:
            return int(client.dbsize())
        finally:
            client.close()

    def victim_for_partition(self) -> str:
        name = self._owner_name(_DATA_SLOT)
        if not name:
            raise RuntimeError("no primary owns the measured slot")
        self._isolated = name
        self._arm_exclude = True
        return name

    def victim_for_failover(self) -> str:
        name = self._owner_name(_DATA_SLOT)
        if not name:
            raise RuntimeError("no primary owns the measured slot")
        self._watched_name = name
        self._watched_slot = _DATA_SLOT
        return name

    def latency_peers(self) -> list[tuple[str, list[str]]]:
        peers: list[tuple[str, list[str]]] = []
        for name, meta in self._nodes_meta.items():
            others = [
                other["ipv4_cluster"]
                for other_name, other in self._nodes_meta.items()
                if other_name != name
            ]
            peers.append((name, others))
        return peers

    def external_promote(self) -> bool:
        return False

    def note_failover(self, candidate: str) -> None:
        log.info("Cluster failover")

    def election_metric_context(self) -> str:
        return "cluster_failover"

    def promote(self, node: str) -> None:
        return None

    def rejoin(self, node: str) -> str:
        infra.start_container(node)
        return "replica_catchup"

    def repair_hooks(self) -> None:
        try:
            info = self._cluster_info()
        except (RuntimeError, OSError) as exc:
            log.info("CLUSTER INFO failed: %s", exc)
            return
        log.info(
            "CLUSTER INFO state=%s slots_ok=%s",
            info.get("cluster_state"),
            info.get("cluster_slots_ok"),
        )

    def expected_behaviour(self, scenario: str) -> str:
        if self._wait:
            text = {
                "s1": "WAIT 1 cannot finish on the isolated primary; the majority can write after its replica is elected",
                "s2": "The replica of the killed primary takes its slots",
                "s3": "WAIT 1 makes write latency follow the delay to the replica",
                "s4": "After the old primary rejoins as a replica, the shard copies converge",
            }
        else:
            text = {
                "s1": "The isolated primary can accept writes until it loses majority; those writes can be lost after failover",
                "s2": "The replica of the killed primary takes its slots; election_time is that failover",
                "s3": "Async write latency stays low because the primary does not wait for a replica",
                "s4": "After the old primary rejoins as a replica, the shard copies converge",
            }
        return text.get(scenario, "")
