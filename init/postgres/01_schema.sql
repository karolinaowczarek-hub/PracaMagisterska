-- Schema + replication user for CAP benchmark (PostgreSQL).
-- Mounted into /docker-entrypoint-initdb.d on the primary only.
-- Host-based auth is appended by 02_hba.sh in the same directory.

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    checksum TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE USER replicator WITH REPLICATION PASSWORD 'repl' LOGIN;
