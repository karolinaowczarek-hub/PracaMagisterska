from .base import Database
from .cassandra import CassandraDB
from .mongo import MongoDB
from .postgres import PostgresDB
from .redis import RedisDB

__all__ = ["Database", "CassandraDB", "MongoDB", "PostgresDB", "RedisDB"]


def get_database(db: str, cfg: dict) -> Database:
    if db == "postgres":
        return PostgresDB(cfg)
    if db == "mongo":
        return MongoDB(cfg)
    if db == "cassandra":
        return CassandraDB(cfg)
    if db == "redis":
        return RedisDB(cfg)
    raise ValueError(f"Database not available: {db}")
