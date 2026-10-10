"""MongoDB adapter: replica set, majority write concern, automatic election."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Sequence

from pymongo import MongoClient, ReadPreference
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from .. import infra
from .base import Database

log = logging.getLogger(__name__)

_MONGO_ERRORS = (PyMongoError, OSError, TimeoutError)


def _checksum(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


class MongoDB(Database):
    name = "mongo"

    def __init__(self, cfg: dict, *, hosts: list[tuple] | None = None, minority_of: str | None = None) -> None:
        self.cfg = cfg
        self.mg = cfg["mongo"]
        self._nodes_meta = {n["name"]: n for n in self.mg["nodes"]}
        self._all_names = [n["name"] for n in self.mg["nodes"]]
        if hosts is None:
            self.hosts = [
                ("127.0.0.1", int(n["host_port"]), n["name"]) for n in self.mg["nodes"]
            ]
        else:
            self.hosts = hosts
        self._minority_of = minority_of
        self._client: MongoClient | None = None
        self.last_target_node = ""
        self._timeout_ms = int(cfg["workload"]["op_timeout_ms"])
        concern = self.mg.get("write_concern", "majority")
        self._write_concern = WriteConcern(w=1 if minority_of else concern, wtimeout=self._timeout_ms)
        self._read_pref = ReadPreference.SECONDARY if minority_of else ReadPreference.PRIMARY

    def _client_for(self, port: int) -> MongoClient:
        return MongoClient(
            host="127.0.0.1",
            port=port,
            directConnection=True,
            serverSelectionTimeoutMS=self._timeout_ms,
            connectTimeoutMS=self._timeout_ms,
            socketTimeoutMS=self._timeout_ms,
        )

    def _db(self):
        if self._client is None:
            self.connect()
        assert self._client is not None
        return self._client.get_database(
            self.mg["database"],
            write_concern=self._write_concern,
            read_preference=self._read_pref,
            read_concern=ReadConcern("local") if self._minority_of else ReadConcern("majority"),
        )

    def _hello(self, port: int) -> dict:
        client = self._client_for(port)
        try:
            return client.admin.command("hello")
        finally:
            client.close()

    def connect(self) -> None:
        self.close()
        last_err: BaseException | None = None
        for host, port, name in self.hosts:
            try:
                hello = self._hello(port)
                if not self._minority_of and not hello.get("isWritablePrimary"):
                    continue
                self._client = self._client_for(port)
                self._client.admin.command("ping")
                self.last_target_node = name
                return
            except _MONGO_ERRORS as exc:
                last_err = exc
        raise last_err or RuntimeError("No MongoDB host reachable")

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _upsert(self, key: str, value: str) -> None:
        try:
            self._db()["kv"].replace_one(
                {"_id": key},
                {
                    "_id": key,
                    "value": value,
                    "checksum": _checksum(value),
                    "updated_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )
        except PyMongoError:
            self.close()
            raise

    def write(self, key: str, value: str) -> None:
        self._upsert(key, value)

    def read(self, key: str) -> str | None:
        try:
            doc = self._db()["kv"].find_one({"_id": key}, {"value": 1})
        except PyMongoError:
            self.close()
            raise
        if not doc:
            return None
        return doc.get("value")

    def write_batch(self, records: Sequence[tuple[str, str]]) -> None:
        for key, value in records:
            self._upsert(key, value)
        if not records:
            return
        last = records[-1][0]

        def caught_up() -> bool:
            try:
                return all(
                    self.read_from_node(node, last) == records[-1][1] for node in self.nodes()
                )
            except _MONGO_ERRORS:
                return False

        ok, _ = infra.wait_for(caught_up, timeout=60, interval=0.5)
        if not ok:
            raise RuntimeError("replicas did not catch up after write_batch")

    def read_from_node(self, node: str, key: str) -> str | None:
        return self.read_keys_from_node(node, [key]).get(key)

    def read_keys_from_node(self, node: str, keys: Sequence[str]) -> dict[str, str | None]:
        out: dict[str, str | None] = {key: None for key in keys}
        if not keys:
            return out
        meta = self._nodes_meta[node]
        client = self._client_for(int(meta["host_port"]))
        try:
            coll = client.get_database(
                self.mg["database"],
                read_preference=ReadPreference.PRIMARY_PREFERRED,
            )["kv"]
            step = 500
            for start in range(0, len(keys), step):
                part = list(keys[start:start + step])
                for doc in coll.find({"_id": {"$in": part}}, {"value": 1}):
                    out[doc["_id"]] = doc.get("value")
            return out
        finally:
            client.close()

    def minority_client(self) -> "MongoDB":
        victim = self.victim_for_partition()
        meta = self._nodes_meta[victim]
        client = MongoDB(
            self.cfg,
            hosts=[("127.0.0.1", int(meta["host_port"]), victim)],
            minority_of=victim,
        )
        client.connect()
        return client

    def nodes(self) -> list[str]:
        return list(self._all_names)

    def primary(self) -> str | None:
        for name in self._all_names:
            meta = self._nodes_meta[name]
            try:
                hello = self._hello(int(meta["host_port"]))
                if hello.get("isWritablePrimary"):
                    return name
            except _MONGO_ERRORS:
                continue
        return None

    def _status_members(self) -> list[dict]:
        prim = self.primary()
        target = prim or self._all_names[0]
        rc, out, err = infra.exec_in(
            target,
            ["mongosh", "--quiet", "--eval",
             "JSON.stringify(rs.status().members.map(m => ({name:m.name, state:m.stateStr})))"],
            timeout=20,
        )
        if rc != 0:
            raise RuntimeError(err or out or "rs.status failed")
        return json.loads(out)

    def cluster_state(self) -> dict[str, Any]:
        try:
            return {"primary": self.primary(), "members": self._status_members()}
        except (PyMongoError, RuntimeError, json.JSONDecodeError) as exc:
            return {"primary": self.primary(), "error": str(exc)}

    def _secondaries(self) -> int:
        try:
            return sum(1 for m in self._status_members() if m.get("state") == "SECONDARY")
        except (PyMongoError, RuntimeError, json.JSONDecodeError):
            return 0

    def is_healthy(self) -> bool:
        return self.primary() is not None and self._secondaries() >= 1

    def streaming_replica_count(self) -> int:
        return self._secondaries()

    def sync_replica_count(self) -> int:
        return self._secondaries()

    def all_nodes_running(self) -> bool:
        for name in self._all_names:
            r = infra._run(["docker", "inspect", "-f", "{{.State.Running}}", name], timeout=15)
            if (r.stdout or "").strip() != "true":
                return False
        return True

    def record_count(self) -> int:
        return int(self._db()["kv"].count_documents({}))

    def victim_for_partition(self) -> str:
        prim = self.primary()
        secondaries = [n for n in self._all_names if n != prim]
        if not secondaries:
            raise RuntimeError("no secondary to isolate")
        return secondaries[-1]

    def victim_for_failover(self) -> str:
        return self.primary() or self._all_names[0]

    def latency_peers(self) -> list[tuple[str, list[str]]]:
        prim = self.primary() or self._all_names[0]
        peers = [meta["ipv4_cluster"] for name, meta in self._nodes_meta.items() if name != prim]
        return [(prim, peers)]

    def external_promote(self) -> bool:
        return False

    def promote(self, node: str) -> None:
        return None

    def rejoin(self, node: str) -> str:
        infra.start_container(node)
        return "replica_set_catchup"

    def expected_behaviour(self, scenario: str) -> str:
        return {
            "s1": "CP: isolated secondary rejects writes; majority stays available with w=majority",
            "s2": "Replica set elects a new primary; election_time is the set, not an external promote",
            "s3": "Write latency rises with inter-node delay under write concern majority",
            "s4": "After the secondary rejoins, nodes converge; minority writes do not commit",
        }.get(scenario, "")
