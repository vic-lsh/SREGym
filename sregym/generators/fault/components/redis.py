"""Redis-protocol component driver (Redis and Valkey): run CLI commands inside the server's pod.

Every primitive takes a ``RedisInstance`` binding row; ``cli`` selects redis-cli or valkey-cli.
"""

from __future__ import annotations

from dataclasses import dataclass

from sregym.conductor.problems.composition.app_bindings import RedisInstance
from sregym.generators.fault.components.kube import ComponentError, Kube, exec_in, pod_for

# Error reply prefixes. Without a tty redis-cli prints an error reply raw (``READONLY You can't
# write against a read only replica.``) and exits 0, so the prefix is the only signal. Verified on
# redis:6.2.4: ERR, WRONGTYPE, OOM, READONLY, NOPERM, WRONGPASS, BUSY (tests pin the exact lines).
ERROR_PREFIXES = frozenset(
    {
        "ERR",
        "WRONGTYPE",
        "READONLY",
        "NOPERM",
        "NOAUTH",
        "WRONGPASS",
        "OOM",
        "BUSY",
        "BUSYKEY",
        "LOADING",
        "MISCONF",
        "MASTERDOWN",
        "NOREPLICAS",
        "EXECABORT",
        "NOSCRIPT",
        "NOTBUSY",
        "UNKILLABLE",
        "CROSSSLOT",
        "CLUSTERDOWN",
    }
)


def error_reply(out: str) -> str | None:
    """The error line if ``out`` (raw redis-cli output) is an error reply, else None."""
    first = out.lstrip("\n").split("\n", 1)[0].strip()
    if first.startswith("(error) "):
        first = first[len("(error) ") :]
    word = first.split(" ", 1)[0]
    return first if word in ERROR_PREFIXES else None


def cli(kube: Kube, namespace: str, inst: RedisInstance, *args: str) -> str:
    """Run one command; raises ComponentError on an error reply (any ``ERROR_PREFIXES`` line)."""
    pod = pod_for(kube, namespace, inst.kind, inst.workload)
    out = exec_in(kube, namespace, pod, [inst.cli, "-p", str(inst.port), *args], container=inst.container)
    err = error_reply(out)
    if err is not None:
        raise ComponentError(f"{inst.workload} in {namespace}: {inst.cli} {' '.join(args)}: {err[:300]}")
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


SAMPLE_KEYS = 20


@dataclass(frozen=True)
class KeySample:
    """What the server held before a fault: its size and a sample of key names."""

    dbsize: int
    keys: tuple[str, ...]


def key_sample(kube: Kube, namespace: str, inst: RedisInstance) -> KeySample:
    """``DBSIZE`` and up to ``SAMPLE_KEYS`` key names (one SCAN page)."""
    lines = cli(kube, namespace, inst, "SCAN", "0", "COUNT", str(SAMPLE_KEYS * 5)).splitlines()
    keys = tuple(k for k in (line.strip() for line in lines[1:]) if k)[:SAMPLE_KEYS]
    return KeySample(dbsize(kube, namespace, inst), keys)


def keys_intact(kube: Kube, namespace: str, inst: RedisInstance, sample: KeySample) -> tuple[bool, str]:
    """Every sampled key still exists."""
    if not sample.keys:
        return True, f"{inst.workload}: no keys sampled"
    present = int(cli(kube, namespace, inst, "EXISTS", *sample.keys).strip().split()[-1])
    return present == len(sample.keys), (
        f"{inst.workload}: {present}/{len(sample.keys)} sampled keys present (dbsize was {sample.dbsize})"
    )
