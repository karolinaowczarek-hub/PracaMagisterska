#!/bin/bash
set -e
# Append replication + network trust rules (runs only on empty data dir).
{
  echo "host all all 0.0.0.0/0 trust"
  echo "host all all ::/0 trust"
  echo "host replication replicator 0.0.0.0/0 trust"
  echo "host replication replicator ::/0 trust"
} >> "${PGDATA}/pg_hba.conf"
# wal_log_hints
if ! grep -q '^wal_log_hints' "${PGDATA}/postgresql.conf"; then
  echo "wal_log_hints = on" >> "${PGDATA}/postgresql.conf"
fi
