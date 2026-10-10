#!/bin/bash
set -eu
mkdir -p /data
cp /usr/local/etc/redis/redis.conf /data/redis.conf
printf '\ncluster-announce-ip %s\ncluster-announce-port 6379\ncluster-announce-bus-port 16379\n' "$ANNOUNCE_IP" >> /data/redis.conf
exec redis-server /data/redis.conf
