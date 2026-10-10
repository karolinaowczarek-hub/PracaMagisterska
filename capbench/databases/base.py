"""Database adapter interface + scenario hooks."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Sequence


class Database(ABC):
    name: str = "base"

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    def write(self, key: str, value: str) -> None: ...

    @abstractmethod
    def read(self, key: str) -> str | None: ...

    @abstractmethod
    def write_batch(self, records: Sequence[tuple[str, str]]) -> None: ...

    @abstractmethod
    def read_from_node(self, node: str, key: str) -> str | None: ...

    def read_keys_from_node(self, node: str, keys: Sequence[str]) -> dict[str, str | None]:
        return {key: self.read_from_node(node, key) for key in keys}

    @abstractmethod
    def minority_client(self) -> "Database": ...

    @abstractmethod
    def nodes(self) -> list[str]: ...

    @abstractmethod
    def primary(self) -> str | None: ...

    @abstractmethod
    def cluster_state(self) -> Any: ...

    @abstractmethod
    def is_healthy(self) -> bool: ...

    @abstractmethod
    def record_count(self) -> int: ...

    @abstractmethod
    def victim_for_partition(self) -> str: ...

    @abstractmethod
    def victim_for_failover(self) -> str: ...

    @abstractmethod
    def latency_peers(self) -> list[tuple[str, list[str]]]: ...

    @abstractmethod
    def promote(self, node: str) -> None: ...

    @abstractmethod
    def rejoin(self, node: str) -> str: ...

    def repair_hooks(self) -> None:
        return None

    def enable_sync_replication(self) -> None:
        return None

    def streaming_replica_count(self) -> int:
        return 0

    def sync_replica_count(self) -> int:
        return 0

    def all_nodes_running(self) -> bool:
        return True

    def external_promote(self) -> bool:
        return True

    def note_healed(self) -> None:
        return None

    def note_failover(self, candidate: str) -> None:
        log = logging.getLogger("capbench.scenarios.s2_failover")
        if self.external_promote():
            log.info("External promote of %s", candidate)
        else:
            log.info("Replica set will elect a new primary")

    def election_metric_context(self) -> str:
        if self.external_promote():
            return "includes_script_promote"
        return "replica_set_election"

    @abstractmethod
    def expected_behaviour(self, scenario: str) -> str: ...
