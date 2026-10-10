"""Scenario package."""

from . import s1_partition, s2_failover, s3_latency, s4_recovery

SCENARIOS = {
    "s1": s1_partition,
    "s2": s2_failover,
    "s3": s3_latency,
    "s4": s4_recovery,
}
