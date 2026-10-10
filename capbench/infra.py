"""Docker compose, network partition, tc/netem, health checks, preflight, cleanup."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Callable

log = logging.getLogger(__name__)

_UN_NODE = re.compile(r"^UN\s+\d+\.\d+\.\d+\.\d+", re.M)

ROOT = Path(__file__).resolve().parent.parent
DOCKER_DIR = ROOT / "docker"


def _cfg(cfg: dict | None = None) -> dict:
    if cfg is not None:
        return cfg
    from .config import load_config
    return load_config()


class InfraError(Exception):
    pass


def _run(cmd: list[str], timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    log.debug("exec: %s", " ".join(cmd))
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def compose_file(db: str) -> Path:
    path = DOCKER_DIR / f"compose.{db}.yml"
    if not path.exists():
        raise InfraError(f"Compose file missing: {path}")
    return path


def project_name(db: str) -> str:
    return f"capbench_{db}"


def compose_up(db: str, *, build: bool = False) -> None:
    cf = compose_file(db)
    cmd = [
        "docker", "compose",
        "-f", str(cf),
        "-p", project_name(db),
        "up", "-d",
    ]
    if build:
        cmd.append("--build")
    r = _run(cmd, timeout=900 if db in ("cassandra", "redis") else 600)
    if r.returncode != 0:
        raise InfraError(f"compose up failed:\n{r.stdout}\n{r.stderr}")


def compose_down(db: str, purge_volumes: bool = False) -> None:
    cf = compose_file(db)
    cmd = [
        "docker", "compose",
        "-f", str(cf),
        "-p", project_name(db),
        "down",
        "--remove-orphans",
    ]
    if purge_volumes:
        cmd.append("--volumes")
    r = _run(cmd, timeout=300)
    if r.returncode != 0:
        log.warning("compose down rc=%s: %s", r.returncode, r.stderr)
    # leftover containers
    from .config import ConfigError
    try:
        names = node_names(db, _cfg())
    except (InfraError, ConfigError):
        names = []
    for name in names:
        _run(["docker", "rm", "-f", name], timeout=60)


def stop_container(name: str, timeout: int = 0) -> None:
    """timeout=0 → SIGKILL (hard fault)."""
    if timeout == 0:
        r = _run(["docker", "kill", name], timeout=60)
    else:
        r = _run(["docker", "stop", "-t", str(timeout), name], timeout=timeout + 60)
    if r.returncode != 0:
        raise InfraError(f"stop/kill {name} failed: {r.stderr}")


def start_container(name: str) -> None:
    r = _run(["docker", "start", name], timeout=120)
    if r.returncode != 0:
        raise InfraError(f"start {name} failed: {r.stderr}")


def _iface_for_ip(container: str, ip: str) -> str:
    """NIC that owns `ip`, including when the link is already down."""
    rc, out, err = exec_in(
        container,
        [
            "sh", "-c",
            f"ip -o addr show | awk -v ip='{ip}' "
            "'index($4, ip \"/\") == 1 {split($2, a, \"@\"); print a[1]; exit}'",
        ],
        timeout=15,
    )
    iface = (out or "").strip()
    if rc != 0 or not iface:
        raise InfraError(f"cannot find NIC for {ip} on {container}: {err or out}")
    return iface


def _nodes_owning(container: str, cfg: dict | None = None) -> list[dict]:
    cfg = _cfg(cfg)
    for key in ("postgres", "mongo", "cassandra", "redis"):
        nodes = (cfg.get(key) or {}).get("nodes") or []
        if any(n.get("name") == container for n in nodes):
            return nodes
    raise InfraError(f"no cluster owns {container}")


def isolate_cluster(victim: str, cluster_ip: str) -> None:
    """Block cluster traffic to the victim on the other nodes."""
    for node in _nodes_owning(victim):
        if node["name"] == victim:
            continue
        _block_peer(node["name"], node["ipv4_cluster"], cluster_ip)


def _block_peer(container: str, local_ip: str, victim_ip: str) -> None:
    iface = _iface_for_ip(container, local_ip)
    script = (
        f"tc qdisc del dev {iface} root 2>/dev/null; "
        f"tc qdisc del dev {iface} ingress 2>/dev/null; "
        f"tc qdisc add dev {iface} root handle 1: prio && "
        f"tc qdisc add dev {iface} parent 1:1 handle 10: netem loss 100% && "
        f"tc filter add dev {iface} protocol ip parent 1: prio 1 u32 "
        f"match ip dst {victim_ip}/32 flowid 1:1 && "
        f"tc qdisc add dev {iface} ingress && "
        f"tc filter add dev {iface} parent ffff: protocol ip u32 "
        f"match ip src {victim_ip}/32 action drop"
    )
    rc, out, err = exec_in(container, ["sh", "-c", script], timeout=20)
    if rc != 0:
        raise InfraError(f"block {victim_ip} on {container} {iface}: {err or out}")


def restore_cluster(victim: str, _cluster_ip: str) -> None:
    for node in _nodes_owning(victim):
        remove_delay(node["name"])
        try:
            exec_in(
                node["name"],
                ["ip", "link", "set", "dev", _iface_for_ip(node["name"], node["ipv4_cluster"]), "up"],
                timeout=15,
            )
        except InfraError:
            pass


def connect(
    container: str,
    network: str,
    *,
    aliases: list[str] | None = None,
    ipv4: str | None = None,
) -> None:
    cmd = ["docker", "network", "connect"]
    if ipv4:
        cmd += ["--ip", ipv4]
    for a in aliases or []:
        cmd += ["--alias", a]
    cmd += [network, container]
    r = _run(cmd, timeout=60)
    if r.returncode != 0:
        # Already connected is OK
        if "already exists" in (r.stderr or "").lower() or "already connected" in (r.stderr or "").lower():
            return
        raise InfraError(f"connect {container} to {network}: {r.stderr}")
    if ipv4:
        actual = container_ip(container, network)
        if actual != ipv4:
            raise InfraError(
                f"IP mismatch after reconnect: expected {ipv4}, got {actual} — abort run"
            )


def container_ip(container: str, network: str) -> str | None:
    r = _run(
        [
            "docker", "inspect",
            "-f",
            f'{{{{(index .NetworkSettings.Networks "{network}").IPAddress}}}}',
            container,
        ],
        timeout=30,
    )
    ip = (r.stdout or "").strip()
    return ip or None


def exec_in(name: str, cmd: list[str], timeout: float = 60) -> tuple[int, str, str]:
    full = ["docker", "exec", name, *cmd]
    r = _run(full, timeout=timeout)
    return r.returncode, r.stdout or "", r.stderr or ""


def _iface_toward(container: str, peer_ip: str) -> str:
    """Interface toward a cluster peer."""
    rc, out, err = exec_in(
        container,
        ["sh", "-c", f"ip route get {peer_ip} | awk '{{for (i = 1; i <= NF; i++) if ($i == \"dev\") {{print $(i + 1); exit}}}}'"],
        timeout=15,
    )
    iface = (out or "").strip().split()[0] if (out or "").strip() else ""
    if rc != 0 or not iface:
        raise InfraError(f"cannot find interface toward {peer_ip} on {container}: {err or out}")
    return iface


def apply_delay(container: str, ms: int, peer_ips: list[str]) -> None:
    """Delay egress on the cluster NIC."""
    if ms <= 0:
        remove_delay(container)
        return
    if not peer_ips:
        raise InfraError(f"apply_delay on {container} has no peer addresses")
    iface = _iface_toward(container, peer_ips[0])
    exec_in(container, ["tc", "qdisc", "del", "dev", iface, "root"], timeout=30)
    rc, _, err = exec_in(
        container,
        ["tc", "qdisc", "replace", "dev", iface, "root", "netem", "delay", f"{ms}ms"],
        timeout=30,
    )
    if rc != 0:
        raise InfraError(f"tc netem failed on {container} {iface}: {err}")


def _default_iface(container: str) -> str:
    rc, out, _ = exec_in(
        container,
        ["sh", "-c", "ip route | awk '/default/ {print $5; exit}'"],
        timeout=15,
    )
    iface = (out or "").strip()
    return iface or "eth0"


def verify_delay(
    container: str,
    peer_ip: str,
    expected_ms: int,
    tolerance: float = 0.4,
) -> float:
    """ping -c 5; fail if measured RTT is below the injected one-way delay."""
    if expected_ms == 0:
        return 0.0
    rc, out, err = exec_in(
        container,
        ["ping", "-c", "5", "-W", "2", peer_ip],
        timeout=30,
    )
    if rc != 0:
        raise InfraError(f"ping {peer_ip} from {container} failed: {err or out}")
    # parse rtt min/avg/max
    avg = None
    for line in out.splitlines():
        if "rtt" in line or "round-trip" in line:
            # rtt min/avg/max/mdev = 0.1/0.2/0.3/0.0 ms
            try:
                part = line.split("=")[1].strip().split()[0]
                avg = float(part.split("/")[1])
            except (IndexError, ValueError):
                pass
    if avg is None:
        raise InfraError(f"Could not parse ping RTT from: {out}")
    lower = expected_ms * (1.0 - tolerance)
    if avg < lower:
        raise InfraError(
            f"verify_delay failed: expected ~{expected_ms}ms one-way, "
            f"measured avg RTT={avg:.1f}ms (lower bound {lower:.1f})"
        )
    return avg


def remove_delay(container: str) -> None:
    for iface in (_default_iface(container), "eth0", "eth1"):
        exec_in(container, ["tc", "qdisc", "del", "dev", iface, "root"], timeout=15)
        exec_in(container, ["tc", "qdisc", "del", "dev", iface, "ingress"], timeout=15)


def wait_for(
    fn: Callable[[], bool],
    timeout: float,
    interval: float = 0.1,
) -> tuple[bool, float]:
    t0 = time.perf_counter()
    while True:
        if fn():
            return True, time.perf_counter() - t0
        if time.perf_counter() - t0 >= timeout:
            return False, time.perf_counter() - t0
        time.sleep(interval)


def _pg_primary_node(cfg: dict | None = None) -> str | None:
    for node in node_names("postgres", _cfg(cfg)):
        rc, out, _ = exec_in(
            node,
            ["psql", "-U", "cap", "-d", "capbench", "-tAc", "SELECT NOT pg_is_in_recovery();"],
            timeout=15,
        )
        if rc == 0 and (out or "").strip().lower() in ("t", "true"):
            return node
    return None


def _pg_exec_sql(sql: str, node: str | None = None, cfg: dict | None = None) -> tuple[int, str, str]:
    names = node_names("postgres", _cfg(cfg))
    target = node or _pg_primary_node(cfg) or names[0]
    return exec_in(
        target,
        ["psql", "-U", "cap", "-d", "capbench", "-tAc", sql],
        timeout=30,
    )


def _pg_count(sql: str, cfg: dict | None = None) -> int:
    rc, out, _ = _pg_exec_sql(sql, cfg=cfg)
    if rc != 0:
        return 0
    try:
        return int((out or "0").strip() or "0")
    except ValueError:
        return 0


def _pg_connected_replicas(cfg: dict | None = None) -> int:
    return _pg_count("SELECT count(*) FROM pg_stat_replication;", cfg)


def _pg_sync_replicas(cfg: dict | None = None) -> int:
    return _pg_count(
        "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum');",
        cfg,
    )


def _sync_standby_names(cfg: dict | None = None) -> str:
    return str(_cfg(cfg)["consistency"]["postgres"]["synchronous_standby_names"]).strip()


def _pg_sync_names_applied(cfg: dict | None = None, node: str | None = None) -> bool:
    rc, out, _ = _pg_exec_sql("SHOW synchronous_standby_names;", node=node, cfg=cfg)
    if rc != 0:
        return False
    current = " ".join((out or "").split())
    expected = " ".join(_sync_standby_names(cfg).split())
    return bool(expected) and expected in current


def _pg_enable_sync_standbys(cfg: dict | None = None) -> bool:
    """Set synchronous_standby_names from config after standbys are streaming."""
    cfg = _cfg(cfg)
    primary = _pg_primary_node(cfg)
    if not primary:
        return False
    if _pg_sync_names_applied(cfg, node=primary):
        return True
    names = _sync_standby_names(cfg).replace("'", "''")
    rc, out, err = _pg_exec_sql(
        f"ALTER SYSTEM SET synchronous_standby_names = '{names}';",
        node=primary,
        cfg=cfg,
    )
    if rc != 0:
        raise InfraError(f"synchronous_standby_names: {err or out}")
    rc2, out2, err2 = _pg_exec_sql("SELECT pg_reload_conf();", node=primary, cfg=cfg)
    if rc2 != 0:
        raise InfraError(f"pg_reload_conf: {err2 or out2}")
    log.info("Enabled synchronous_standby_names on %s", primary)
    return True


def wait_healthy_postgres(timeout: float = 300, cfg: dict | None = None) -> None:
    cfg = _cfg(cfg)
    names = node_names("postgres", cfg)

    def ready() -> bool:
        for n in names:
            r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
            if (r.stdout or "").strip() != "true":
                return False
        if _pg_connected_replicas(cfg) < 1:
            return False
        if not _pg_enable_sync_standbys(cfg):
            return False
        return _pg_sync_replicas(cfg) >= 1

    ok, elapsed = wait_for(ready, timeout=timeout, interval=2.0)
    if not ok:
        raise InfraError(f"PostgreSQL cluster not healthy after {elapsed:.1f}s")
    log.info("PostgreSQL healthy in %.1fs (sync replicas=%s)", elapsed, _pg_sync_replicas(cfg))


def postgres_nodes_running(cfg: dict | None = None) -> bool:
    for n in node_names("postgres", _cfg(cfg)):
        r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
        if (r.stdout or "").strip() != "true":
            return False
    return True


def wait_healthy(db: str, timeout: float = 300, cfg: dict | None = None) -> None:
    if db == "postgres":
        wait_healthy_postgres(timeout=timeout, cfg=cfg)
    elif db == "mongo":
        wait_healthy_mongo(timeout=timeout, cfg=cfg)
    elif db == "cassandra":
        wait_healthy_cassandra(timeout=max(timeout, 600), cfg=cfg)
    elif db == "redis":
        wait_healthy_redis(timeout=timeout, cfg=cfg)
    else:
        raise InfraError(f"wait_healthy not implemented for {db}")


def wait_healthy_mongo(timeout: float = 300, cfg: dict | None = None) -> None:
    cfg = _cfg(cfg)
    names = node_names("mongo", cfg)
    members = ", ".join(
        f"{{_id: {i}, host: '{n['ipv4_cluster']}:27017'}}"
        for i, n in enumerate(cfg["mongo"]["nodes"])
    )
    initiate = (
        "try { rs.status(); } catch (e) { "
        f"rs.initiate({{_id: 'rs0', members: [{members}]}}); "
        "}"
    )
    started = False

    def ready() -> bool:
        nonlocal started
        for n in names:
            r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
            if (r.stdout or "").strip() != "true":
                return False
        if not started:
            rc, out, err = exec_in(names[0], ["mongosh", "--quiet", "--eval", initiate], timeout=30)
            if rc != 0:
                log.debug("rs.initiate: %s %s", out, err)
                return False
            started = True
            return False
        rc, out, err = exec_in(
            names[0],
            ["mongosh", "--quiet", "--eval",
             "const m = rs.status().members; "
             "const p = m.filter(x => x.stateStr == 'PRIMARY').length; "
             "const s = m.filter(x => x.stateStr == 'SECONDARY').length; "
             "print(p + ':' + s);"],
            timeout=20,
        )
        if rc != 0:
            return False
        parts = (out or "").strip().split(":")
        return len(parts) == 2 and parts[0] == "1" and parts[1].isdigit() and int(parts[1]) >= 2

    ok, elapsed = wait_for(ready, timeout=timeout, interval=2.0)
    if not ok:
        raise InfraError(f"MongoDB replica set not healthy after {elapsed:.1f}s")
    log.info("MongoDB healthy in %.1fs", elapsed)


def wait_healthy_cassandra(timeout: float = 420, cfg: dict | None = None) -> None:
    cfg = _cfg(cfg)
    names = node_names("cassandra", cfg)
    schema = (
        "CREATE KEYSPACE IF NOT EXISTS capbench WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': 3}; "
        "CREATE TABLE IF NOT EXISTS capbench.kv ("
        "id text PRIMARY KEY, value text, checksum text, updated_at timestamp);"
    )

    def ready() -> bool:
        for n in names:
            r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
            if (r.stdout or "").strip() != "true":
                return False
        rc, out, err = exec_in(names[0], ["nodetool", "status"], timeout=30)
        if rc != 0:
            log.debug("nodetool: %s %s", out, err)
            return False
        up = len(_UN_NODE.findall(out or ""))
        if up < 3:
            return False
        rc, out, err = exec_in(names[0], ["cqlsh", "-e", schema], timeout=60)
        if rc != 0:
            log.debug("schema: %s %s", out, err)
            return False
        return True

    ok, elapsed = wait_for(ready, timeout=timeout, interval=3.0)
    if not ok:
        raise InfraError(f"Cassandra cluster not healthy after {elapsed:.1f}s")
    log.info("Cassandra healthy in %.1fs", elapsed)


def wait_healthy_redis(timeout: float = 180, cfg: dict | None = None) -> None:
    cfg = _cfg(cfg)
    names = node_names("redis", cfg)

    def init_ok() -> bool:
        result = _run(
            ["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", "redis-init"],
            timeout=15,
        )
        text = (result.stdout or "").strip()
        return result.returncode == 0 and text.startswith("exited 0")

    def cluster_ok() -> bool:
        rc, out, err = exec_in(names[0], ["redis-cli", "CLUSTER", "INFO"], timeout=15)
        if rc != 0:
            log.debug("CLUSTER INFO: %s %s", out, err)
            return False
        info = {}
        for line in (out or "").splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                info[key.strip()] = value.strip()
        if info.get("cluster_state") != "ok" or info.get("cluster_slots_ok") != "16384":
            return False
        rc, nodes, err = exec_in(names[0], ["redis-cli", "CLUSTER", "NODES"], timeout=15)
        if rc != 0:
            log.debug("CLUSTER NODES: %s %s", nodes, err)
            return False
        replicas = 0
        for line in (nodes or "").splitlines():
            parts = line.split()
            if len(parts) < 8:
                continue
            flags = set(parts[2].split(","))
            if ("slave" in flags or "replica" in flags) and "fail" not in flags and parts[7] == "connected":
                replicas += 1
        return replicas >= 3

    def ready() -> bool:
        for name in names:
            result = _run(["docker", "inspect", "-f", "{{.State.Running}}", name], timeout=15)
            if (result.stdout or "").strip() != "true":
                return False
        return init_ok() and cluster_ok()

    ok, elapsed = wait_for(ready, timeout=timeout, interval=2.0)
    if not ok:
        raise InfraError(f"Redis cluster not healthy after {elapsed:.1f}s")
    log.info("Redis healthy in %.1fs", elapsed)


def node_names(db: str, cfg: dict) -> list[str]:
    if db in ("postgres", "mongo", "cassandra", "redis"):
        return [n["name"] for n in cfg[db]["nodes"]]
    raise InfraError(f"Unknown db: {db}")


def preflight(db: str, cfg: dict) -> None:
    """Check the cluster before a run."""
    nodes = node_names(db, cfg)
    cluster_net = cfg[db]["networks"]["cluster"]
    client_net = cfg[db]["networks"]["client"]
    by_name = {node["name"]: node for node in cfg[db]["nodes"]}

    for n in nodes:
        r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
        if (r.stdout or "").strip() != "true":
            raise InfraError(f"preflight: {n} not running")
        for net in (cluster_net, client_net):
            ip = container_ip(n, net)
            if not ip:
                raise InfraError(f"preflight: {n} missing from {net}")
        cluster_iface = _iface_for_ip(n, by_name[n]["ipv4_cluster"])
        rc, link, _ = exec_in(n, ["ip", "-o", "link", "show", "dev", cluster_iface], timeout=15)
        if rc != 0 or "state DOWN" in (link or ""):
            raise InfraError(f"preflight: cluster NIC {cluster_iface} down on {n}")
        for iface in {_default_iface(n), cluster_iface}:
            rc, out, _ = exec_in(n, ["tc", "qdisc", "show", "dev", iface], timeout=15)
            if "netem" in (out or ""):
                raise InfraError(f"preflight: leftover netem on {n} {iface}: {out}")


def cleanup(db: str, cfg: dict) -> None:
    """Undo scenario faults: tc/netem, cluster isolation, stopped containers."""
    if db not in ("postgres", "mongo", "cassandra", "redis"):
        log.warning("cleanup: unsupported db %s", db)
        return
    nodes = node_names(db, cfg)
    by_name = {n["name"]: n for n in cfg[db]["nodes"]}

    for n in nodes:
        try:
            remove_delay(n)
        except InfraError as exc:
            log.debug("remove_delay %s: %s", n, exc)
        try:
            restore_cluster(n, by_name[n]["ipv4_cluster"])
        except InfraError as exc:
            log.debug("restore_cluster %s: %s", n, exc)
        r = _run(["docker", "inspect", "-f", "{{.State.Running}}", n], timeout=15)
        if (r.stdout or "").strip() != "true":
            try:
                start_container(n)
            except InfraError as exc:
                log.warning("start %s during cleanup: %s", n, exc)


def reconnect_networks(db: str, cfg: dict) -> None:
    """Reattach both Docker networks."""
    nodes = node_names(db, cfg)
    cluster_net = cfg[db]["networks"]["cluster"]
    client_net = cfg[db]["networks"]["client"]
    by_name = {n["name"]: n for n in cfg[db]["nodes"]}
    for n in nodes:
        meta = by_name[n]
        try:
            connect(n, cluster_net, aliases=[n], ipv4=meta["ipv4_cluster"])
        except InfraError as exc:
            log.debug("connect cluster %s: %s", n, exc)
        try:
            connect(n, client_net, aliases=[n], ipv4=meta["ipv4_client"])
        except InfraError as exc:
            log.debug("connect client %s: %s", n, exc)


def dump_logs(db: str, dest: Path, since: str | None = None, cfg: dict | None = None) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    from .config import ConfigError
    try:
        nodes = node_names(db, _cfg(cfg))
    except (InfraError, ConfigError):
        nodes = []
    for n in nodes:
        cmd = ["docker", "logs"]
        if since:
            cmd += ["--since", since]
        cmd.append(n)
        r = _run(cmd, timeout=120)
        (dest / f"{n}.log").write_text((r.stdout or "") + (r.stderr or ""), encoding="utf-8")


def docker_available() -> tuple[bool, str]:
    if not shutil.which("docker"):
        return False, "docker CLI not found"
    r = _run(["docker", "version", "--format", "{{.Server.Version}}"], timeout=30)
    if r.returncode != 0:
        return False, r.stderr or "cannot talk to Docker engine"
    return True, (r.stdout or "").strip()


def verify_environment() -> list[str]:
    """Return list of human-readable OK lines; raise InfraError on hard fail."""
    lines: list[str] = []
    ok, ver = docker_available()
    if not ok:
        raise InfraError(f"Docker not available: {ver}")
    lines.append(f"Docker engine OK ({ver})")

    # WSL2 backend hint (Windows)
    r = _run(["docker", "info", "--format", "{{.OperatingSystem}}"], timeout=30)
    lines.append(f"Docker OS: {(r.stdout or '').strip()}")

    # Dummy container for NET_ADMIN + tc
    name = "capbench-tc-probe"
    _run(["docker", "rm", "-f", name], timeout=30)
    r = _run(
        [
            "docker", "run", "-d", "--name", name,
            "--cap-add", "NET_ADMIN",
            "alpine:3.20", "sleep", "60",
        ],
        timeout=120,
    )
    if r.returncode != 0:
        raise InfraError(f"Failed to start probe container: {r.stderr}")
    try:
        # install iproute2/iputils in probe
        _run(["docker", "exec", name, "sh", "-c", "apk add --no-cache iproute2 iputils >/dev/null"], timeout=120)
        rc, _, err = exec_in(name, ["tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", "100ms"], timeout=30)
        if rc != 0:
            raise InfraError(f"tc not usable with NET_ADMIN: {err}")
        lines.append("tc/netem + NET_ADMIN OK (probe)")
        exec_in(name, ["tc", "qdisc", "del", "dev", "eth0", "root"], timeout=15)
    finally:
        _run(["docker", "rm", "-f", name], timeout=30)

    cfg = _cfg()
    ports = [int(n["host_port"]) for n in cfg["postgres"]["nodes"]]
    lines.append("Postgres host ports from config: " + ", ".join(str(p) for p in ports))
    return lines


def docker_memory_gb() -> str:
    """Docker engine memory in GB. Empty if unknown."""
    r = _run(["docker", "info", "--format", "{{.MemTotal}}"], timeout=30)
    if r.returncode != 0:
        return ""
    try:
        total = int((r.stdout or "0").strip() or 0)
    except ValueError:
        return ""
    if total <= 0:
        return ""
    return f"{total / (1024 ** 3):.1f}"
