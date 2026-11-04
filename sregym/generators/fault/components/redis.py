"""Redis-protocol component driver (Redis and Valkey): run CLI commands inside the server's pod.

Every primitive takes a ``RedisInstance`` binding row; ``cli`` selects redis-cli or valkey-cli.
"""

from __future__ import annotations

from sregym.conductor.problems.composition.app_bindings import RedisInstance
from sregym.generators.fault.components.kube import ComponentError, Kube, exec_in, pod_for


def cli(kube: Kube, namespace: str, inst: RedisInstance, *args: str) -> str:
    pod = pod_for(kube, namespace, inst.kind, inst.workload)
    out = exec_in(kube, namespace, pod, [inst.cli, "-p", str(inst.port), *args], container=inst.container)
    if out.startswith("(error)") or out.startswith("ERR"):
        raise ComponentError(f"{inst.workload} in {namespace}: {inst.cli} {' '.join(args)}: {out.strip()[:300]}")
    return out


def replication_info(kube: Kube, namespace: str, inst: RedisInstance) -> dict[str, str]:
    return parse_info(cli(kube, namespace, inst, "INFO", "replication"))


def parse_info(text: str) -> dict[str, str]:
    fields = {}
    for line in text.splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.strip().split(":", 1)
            fields[k] = v
    return fields


def make_replica_of(kube: Kube, namespace: str, inst: RedisInstance, host: str, port: int) -> None:
    """``REPLICAOF host port``: the server turns read-only (writes fail with READONLY) and keeps
    retrying a master that never answers; existing data stays served until a sync succeeds."""
    cli(kube, namespace, inst, "REPLICAOF", host, str(port))


def promote(kube: Kube, namespace: str, inst: RedisInstance) -> None:
    """``REPLICAOF NO ONE``: back to a writable master, keeping the data set."""
    cli(kube, namespace, inst, "REPLICAOF", "NO", "ONE")


def dbsize(kube: Kube, namespace: str, inst: RedisInstance) -> int:
    return int(cli(kube, namespace, inst, "DBSIZE").strip().split()[-1])
