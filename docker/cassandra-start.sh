#!/bin/bash
set -eu
if [ -n "${NATIVE_PORT:-}" ]; then
  sed -i "s/^native_transport_port:.*/native_transport_port: ${NATIVE_PORT}/" /etc/cassandra/cassandra.yaml
fi
exec /usr/local/bin/docker-entrypoint.sh cassandra -f
