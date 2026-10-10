"""Error taxonomy and driver exception mapping."""

from __future__ import annotations

from enum import Enum

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]

try:
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    PyMongoError = None  # type: ignore[assignment,misc]

try:
    from cassandra import (
        OperationTimedOut,
        ReadFailure,
        ReadTimeout,
        Unavailable,
        WriteFailure,
        WriteTimeout,
    )
    from cassandra.cluster import NoHostAvailable
    _CASSANDRA_ERRORS: tuple[type[BaseException], ...] = (
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
except ImportError:  # pragma: no cover
    _CASSANDRA_ERRORS = (OSError, TimeoutError)

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import RedisClusterException, RedisError
    from redis.exceptions import TimeoutError as RedisTimeoutError
    _REDIS_ERRORS: tuple[type[BaseException], ...] = (
        RedisError,
        RedisClusterException,
        RedisConnectionError,
        RedisTimeoutError,
        OSError,
        TimeoutError,
    )
except ImportError:  # pragma: no cover
    _REDIS_ERRORS = (OSError, TimeoutError)


class ErrorCategory(str, Enum):
    TIMEOUT = "TIMEOUT"
    NO_QUORUM = "NO_QUORUM"
    NOT_PRIMARY = "NOT_PRIMARY"
    CONNECTION_REFUSED = "CONNECTION_REFUSED"
    CONNECTION_RESET = "CONNECTION_RESET"
    UNAVAILABLE = "UNAVAILABLE"
    CLUSTER_DOWN = "CLUSTER_DOWN"
    MOVED_ASK = "MOVED_ASK"
    READ_ONLY = "READ_ONLY"
    OTHER = "OTHER"


def driver_errors(db: str) -> tuple[type[BaseException], ...]:
    """Exceptions that a database adapter may raise for a single operation."""
    if db == "postgres" and psycopg2 is not None:
        return (psycopg2.Error, OSError, TimeoutError)
    if db == "mongo" and PyMongoError is not None:
        return (PyMongoError, OSError, TimeoutError)
    if db == "cassandra":
        return _CASSANDRA_ERRORS
    if db == "redis":
        return _REDIS_ERRORS
    return (OSError, TimeoutError)


def classify(exc: BaseException, db: str) -> tuple[ErrorCategory, str, str]:
    """Map driver exception → (category, class_name, short_message)."""
    cls_name = type(exc).__name__
    msg = str(exc) or ""
    low = msg.lower()

    if db == "postgres":
        if "timeout" in low or "canceling statement" in low or "querycanceled" in cls_name.lower():
            return ErrorCategory.TIMEOUT, cls_name, msg
        if "read-only" in low or "read only" in low:
            return ErrorCategory.READ_ONLY, cls_name, msg
        if "terminating connection" in low or "server closed the connection" in low:
            return ErrorCategory.CONNECTION_RESET, cls_name, msg
        if "connection refused" in low or "could not connect" in low:
            return ErrorCategory.CONNECTION_REFUSED, cls_name, msg
        if "connection reset" in low or "broken pipe" in low:
            return ErrorCategory.CONNECTION_RESET, cls_name, msg
        if "not accepting connections" in low or "the database system is starting" in low:
            return ErrorCategory.UNAVAILABLE, cls_name, msg
        if "cannot execute" in low and "recovery" in low:
            return ErrorCategory.READ_ONLY, cls_name, msg
        if cls_name in ("OperationalError", "InterfaceError", "DatabaseError"):
            return ErrorCategory.UNAVAILABLE, cls_name, msg

    if db == "mongo":
        if "timeout" in low or "timed out" in low:
            return ErrorCategory.TIMEOUT, cls_name, msg
        if "not primary" in low or "notwritableprimary" in low or cls_name == "NotPrimaryError":
            return ErrorCategory.NOT_PRIMARY, cls_name, msg
        if "write concern" in low or "waiting for replication" in low:
            return ErrorCategory.NO_QUORUM, cls_name, msg
        if "connection refused" in low:
            return ErrorCategory.CONNECTION_REFUSED, cls_name, msg
        if "connection reset" in low or "connection closed" in low:
            return ErrorCategory.CONNECTION_RESET, cls_name, msg
        if cls_name in ("ServerSelectionTimeoutError", "NetworkTimeout", "AutoReconnect"):
            return ErrorCategory.UNAVAILABLE, cls_name, msg
        if cls_name.endswith("Error"):
            return ErrorCategory.UNAVAILABLE, cls_name, msg

    if db == "cassandra":
        if "timeout" in low or cls_name in ("OperationTimedOut", "ReadTimeout", "WriteTimeout"):
            return ErrorCategory.TIMEOUT, cls_name, msg
        if cls_name == "Unavailable" or "cannot achieve consistency" in low:
            return ErrorCategory.NO_QUORUM, cls_name, msg
        if "connection refused" in low:
            return ErrorCategory.CONNECTION_REFUSED, cls_name, msg
        if "connection reset" in low or "connection closed" in low:
            return ErrorCategory.CONNECTION_RESET, cls_name, msg
        if cls_name in ("NoHostAvailable", "WriteFailure", "ReadFailure"):
            return ErrorCategory.UNAVAILABLE, cls_name, msg

    if db == "redis":
        if "timeout" in low or cls_name in ("TimeoutError", "Timeout"):
            return ErrorCategory.TIMEOUT, cls_name, msg
        if "clusterdown" in low or cls_name in ("ClusterDownError", "MasterDownError", "RedisClusterException"):
            return ErrorCategory.CLUSTER_DOWN, cls_name, msg
        if cls_name in ("MovedError", "AskError") or low.startswith("moved") or low.startswith("ask"):
            return ErrorCategory.MOVED_ASK, cls_name, msg
        if "tryagain" in low or cls_name == "TryAgainError":
            return ErrorCategory.UNAVAILABLE, cls_name, msg
        if "noreplicas" in low or "not enough good replicas" in low:
            return ErrorCategory.NO_QUORUM, cls_name, msg
        if "readonly" in low or "read only" in low:
            return ErrorCategory.READ_ONLY, cls_name, msg
        if "connection refused" in low:
            return ErrorCategory.CONNECTION_REFUSED, cls_name, msg
        if "connection reset" in low or "connection closed" in low:
            return ErrorCategory.CONNECTION_RESET, cls_name, msg
        if cls_name in ("ConnectionError", "BusyLoadingError"):
            return ErrorCategory.UNAVAILABLE, cls_name, msg

    return ErrorCategory.OTHER, cls_name, msg


_S1_MINORITY = {
    ErrorCategory.READ_ONLY,
    ErrorCategory.TIMEOUT,
    ErrorCategory.CONNECTION_REFUSED,
    ErrorCategory.CONNECTION_RESET,
    ErrorCategory.UNAVAILABLE,
}
_S2_FAULT = {
    ErrorCategory.TIMEOUT,
    ErrorCategory.CONNECTION_REFUSED,
    ErrorCategory.CONNECTION_RESET,
    ErrorCategory.UNAVAILABLE,
    ErrorCategory.NOT_PRIMARY,
    ErrorCategory.READ_ONLY,
}


def is_expected(
    category: ErrorCategory,
    db: str,
    scenario: str,
    *,
    client_side: str = "",
    phase: str = "",
) -> bool:
    if db == "postgres":
        if scenario == "s1":
            return client_side == "minority" and category in _S1_MINORITY
        if scenario == "s2":
            return phase in ("during_fault", "recovery") and category in _S2_FAULT
        if scenario == "s4":
            return client_side == "minority" and category in _S1_MINORITY
        return False
    if db == "mongo":
        minority = _S1_MINORITY | {ErrorCategory.NOT_PRIMARY, ErrorCategory.NO_QUORUM}
        if scenario in ("s1", "s4"):
            return client_side == "minority" and category in minority
        if scenario == "s2":
            return phase in ("during_fault", "recovery") and category in (_S2_FAULT | {ErrorCategory.NO_QUORUM})
        return False
    if db == "cassandra":
        minority = _S1_MINORITY | {ErrorCategory.NO_QUORUM}
        if scenario in ("s1", "s4"):
            return client_side == "minority" and category in minority
        if scenario == "s2":
            return phase in ("during_fault", "recovery") and category in (_S2_FAULT | {ErrorCategory.NO_QUORUM})
    if db == "redis":
        minority = _S1_MINORITY | {
            ErrorCategory.NO_QUORUM,
            ErrorCategory.READ_ONLY,
            ErrorCategory.CLUSTER_DOWN,
            ErrorCategory.MOVED_ASK,
        }
        if scenario in ("s1", "s4"):
            return client_side == "minority" and category in minority
        if scenario == "s2":
            return phase in ("during_fault", "recovery") and category in (
                _S2_FAULT | {ErrorCategory.NO_QUORUM, ErrorCategory.CLUSTER_DOWN, ErrorCategory.MOVED_ASK}
            )
    return False
