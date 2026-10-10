#!/bin/bash
set -eu
HOSTS=(
  172.34.0.11
  172.34.0.12
  172.34.0.13
  172.34.0.14
  172.34.0.15
  172.34.0.16
)
for host in "${HOSTS[@]}"; do
  ready=0
  for _ in $(seq 1 90); do
    if redis-cli -h "$host" -p 6379 ping 2>/dev/null | grep -q PONG; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "redis node $host did not answer" >&2
    exit 1
  fi
done

state=$(redis-cli -h "${HOSTS[0]}" -p 6379 cluster info | awk -F: '/^cluster_state:/ {gsub(/\r/, "", $2); print $2}')
if [ "$state" = "ok" ]; then
  exit 0
fi

redis-cli --cluster create \
  "${HOSTS[0]}:6379" \
  "${HOSTS[1]}:6379" \
  "${HOSTS[2]}:6379" \
  "${HOSTS[3]}:6379" \
  "${HOSTS[4]}:6379" \
  "${HOSTS[5]}:6379" \
  --cluster-replicas 1 \
  --cluster-yes
