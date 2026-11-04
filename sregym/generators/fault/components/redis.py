"""Redis-protocol component driver (Redis and Valkey): run CLI commands inside the server's pod.

Every primitive takes a ``RedisInstance`` binding row; ``cli`` selects redis-cli or valkey-cli.
"""

from __future__ import annotations

import re
import secrets
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
    return int(_lines(check_exec(kube, namespace, inst, "DBSIZE"))[-1].split()[-1])


SAMPLE_KEYS = 20


@dataclass(frozen=True)
class KeySample:
    """What the server held before a fault: its size and a sample of key names."""

    dbsize: int
    keys: tuple[str, ...]


def key_sample(kube: Kube, namespace: str, inst: RedisInstance) -> KeySample:
    """``DBSIZE`` and up to ``SAMPLE_KEYS`` key names (one SCAN page)."""
    lines = check_exec(kube, namespace, inst, "SCAN", "0", "COUNT", str(SAMPLE_KEYS * 5)).splitlines()
    keys = tuple(k for k in (line.strip() for line in lines[1:]) if k)[:SAMPLE_KEYS]
    return KeySample(dbsize(kube, namespace, inst), keys)


def keys_intact(kube: Kube, namespace: str, inst: RedisInstance, sample: KeySample) -> tuple[bool, str]:
    """Every sampled key still exists (read through the check channel, whatever the fault did to ``default``)."""
    if not sample.keys:
        return True, f"{inst.workload}: no keys sampled"
    present = int(_lines(check_exec(kube, namespace, inst, "EXISTS", *sample.keys))[-1].split()[-1])
    return present == len(sample.keys), (
        f"{inst.workload}: {present}/{len(sample.keys)} sampled keys present (dbsize was {sample.dbsize})"
    )


# === Incident-fault primitives (FR family) =======================================================
# ACL revoke, maxmemory, client pause, poisoned/dangling sorted-set members, key rename and stale
# followers. Self-contained: they use ``_exec`` (not ``cli``) so a command can time out (a paused server
# never answers) or return an error reply without raising.


ACL_MODES = ("write", "read")
_PROBE_KEY = "__sregym_pause_probe__"


def _exec(
    kube: Kube, namespace: str, inst: RedisInstance, *args: str, timeout_s: int | None = None, check: bool = True
) -> str:
    """Run one redis-cli command in the server's pod. ``timeout_s`` wraps it in ``timeout`` (a paused
    server never answers; exit code 124 reads as an empty reply). ``check`` raises on an error reply."""
    pod = pod_for(kube, namespace, inst.kind, inst.workload)
    argv = [inst.cli, "-p", str(inst.port), *args]
    if timeout_s is not None:
        argv = ["timeout", str(timeout_s), *argv]
    try:
        out = exec_in(kube, namespace, pod, argv, container=inst.container)
    except Exception as e:  # kubectl exec exits non-zero when the wrapped command was killed
        if timeout_s is not None and "124" in str(e):
            return ""
        raise
    err = error_reply(out)
    if check and err:
        raise ComponentError(f"{inst.workload} in {namespace}: {inst.cli} {' '.join(args)}: {err[:300]}")
    return out


def _lines(text: str) -> list[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


# --- check channel -----------------------------------------------------------------------------
# Every verdict, witness and integrity read of the framework goes through a dedicated read-only
# ACL user, never through the identity a fault may restrict (``default``: an ACL fault revokes its
# read category, and the oracle would then see NOPERM instead of the data). The user is created
# lazily by ``default`` (ACL administration still works under -@read/-@write), with a random
# password held only in this process (ACL LIST shows a hash), so nothing in the cluster, and no
# agent, can use it to read around the fault; ``remove_check_user`` deletes it at recover.

CHECK_USER = "sregym_check"
_CHECK_RULES = ("reset", "on", "~*", "&*", "-@all", "+@read", "+ping")
_check_passwords: dict[tuple[str, str], str] = {}


def _auth_failed(text: str) -> bool:
    return any(m in text for m in ("AUTH failed", "WRONGPASS", "NOAUTH", "invalid username-password"))


def ensure_check_user(kube: Kube, namespace: str, inst: RedisInstance) -> None:
    """(Re)create the check user with a fresh password (a pod restart wipes runtime ACLs)."""
    password = secrets.token_urlsafe(24)
    _exec(kube, namespace, inst, "ACL", "SETUSER", CHECK_USER, f">{password}", *_CHECK_RULES)
    _check_passwords[(namespace, inst.workload)] = password


def remove_check_user(kube: Kube, namespace: str, inst: RedisInstance) -> None:
    """``ACL DELUSER``: the channel is gone (deleting a missing user is fine)."""
    _check_passwords.pop((namespace, inst.workload), None)
    _exec(kube, namespace, inst, "ACL", "DELUSER", CHECK_USER)


def check_exec(kube: Kube, namespace: str, inst: RedisInstance, *args: str) -> str:
    """One read-only command as the check user. Re-creates the user once if it is missing or its
    password is unknown (restart, another process). Raises ComponentError on an error reply."""
    key = (namespace, inst.workload)
    if key not in _check_passwords:
        ensure_check_user(kube, namespace, inst)
    for attempt in (0, 1):
        pod = pod_for(kube, namespace, inst.kind, inst.workload)
        argv = [
            "env",
            f"REDISCLI_AUTH={_check_passwords[key]}",
            inst.cli,
            "--user",
            CHECK_USER,
            "-p",
            str(inst.port),
            *args,
        ]
        try:
            out = exec_in(kube, namespace, pod, argv, container=inst.container)
        except Exception as e:
            if attempt == 0 and _auth_failed(str(e)):
                ensure_check_user(kube, namespace, inst)
                continue
            raise
        if attempt == 0 and _auth_failed(out):
            ensure_check_user(kube, namespace, inst)
            continue
        err = error_reply(out)
        if err:
            raise ComponentError(f"{inst.workload} in {namespace}: check read {' '.join(args)}: {err[:300]}")
        return out
    raise ComponentError(f"{inst.workload} in {namespace}: check user authentication failed twice")


# --- ACL ---------------------------------------------------------------------------------------


def acl_revoke(kube: Kube, namespace: str, inst: RedisInstance, mode: str) -> None:
    """``ACL SETUSER default -@write`` (mode ``write``) or ``-@read``: the next such command on any
    connection, pooled ones included, fails with NOPERM. Runtime only (a pod restart resets it)."""
    if mode not in ACL_MODES:
        raise ValueError(f"unknown ACL mode {mode!r}; expected one of {ACL_MODES}")
    _exec(kube, namespace, inst, "ACL", "SETUSER", "default", f"-@{mode}")


def acl_restore(kube: Kube, namespace: str, inst: RedisInstance) -> None:
    """``ACL SETUSER default +@all``: the user's command rules are back to the image default."""
    _exec(kube, namespace, inst, "ACL", "SETUSER", "default", "+@all")


def acl_revoked_modes(kube: Kube, namespace: str, inst: RedisInstance) -> tuple[str, ...]:
    """Which of ``-@write`` / ``-@read`` the default user's rules currently contain."""
    rules = _exec(kube, namespace, inst, "ACL", "LIST")
    return tuple(m for m in ACL_MODES if re.search(rf"(?:^|\s)-@{m}(?:\s|$)", rules))


# --- maxmemory ---------------------------------------------------------------------------------


def used_memory(kube: Kube, namespace: str, inst: RedisInstance) -> int:
    for line in _lines(_exec(kube, namespace, inst, "INFO", "memory")):
        if line.startswith("used_memory:"):
            return int(line.split(":", 1)[1])
    raise ComponentError(f"{inst.workload} in {namespace}: INFO memory has no used_memory")


def get_maxmemory(kube: Kube, namespace: str, inst: RedisInstance) -> int:
    return int(_lines(_exec(kube, namespace, inst, "CONFIG", "GET", "maxmemory"))[-1])


def set_maxmemory(kube: Kube, namespace: str, inst: RedisInstance, limit_bytes: int) -> None:
    """``CONFIG SET maxmemory``: with the default ``noeviction`` policy every command that would grow
    the data set fails with OOM once used memory exceeds the limit; reads keep working. 0 is unlimited."""
    if limit_bytes < 0:
        raise ValueError("maxmemory must be >= 0")
    _exec(kube, namespace, inst, "CONFIG", "SET", "maxmemory", str(limit_bytes))


def oom_limit(used_bytes: int, fraction: float = 0.5) -> int:
    """A maxmemory of ``fraction`` of current use. Live: every write fails deterministically at or below
    50% of used memory; near or above 100% it flips between success and OOM as client buffers move."""
    if not 0 < fraction <= 0.5:
        raise ValueError("fraction must be in (0, 0.5]: higher limits do not trip writes deterministically")
    return max(1, int(used_bytes * fraction))


# --- client pause ------------------------------------------------------------------------------


def client_pause_write(kube: Kube, namespace: str, inst: RedisInstance, duration_ms: int) -> None:
    """``CLIENT PAUSE <ms> WRITE``: writes block until unpause or expiry, reads are served. Never ALL:
    ``CLIENT PAUSE ALL`` also blocks ``CLIENT UNPAUSE``, so no command can undo it."""
    if duration_ms <= 0:
        raise ValueError("duration_ms must be positive")
    _exec(kube, namespace, inst, "CLIENT", "PAUSE", str(duration_ms), "WRITE")


def client_unpause(kube: Kube, namespace: str, inst: RedisInstance) -> None:
    _exec(kube, namespace, inst, "CLIENT", "UNPAUSE")


def writes_paused(kube: Kube, namespace: str, inst: RedisInstance, probe_timeout_s: int = 3) -> bool:
    """True if a write-flagged no-op (DEL of a key that never exists) does not answer in time."""
    out = _exec(kube, namespace, inst, "DEL", _PROBE_KEY, timeout_s=probe_timeout_s, check=False)
    return out.strip() == ""


# --- sorted-set members ------------------------------------------------------------------------

POISON_KINDS = ("non_numeric", "dangling_id")
DEFAULT_POISON_SCORE = 1e18  # above any post timestamp score, so zrevrange puts it on the first page


def poison_member(kind: str, token: str) -> str:
    """The member to insert: ``non_numeric`` fails the service's ``stoul`` (HTTP 500), ``dangling_id``
    is a numeric post id no post-storage record has (``Return set incomplete``)."""
    if kind == "non_numeric":
        return f"corrupt-{token}"
    if kind == "dangling_id":
        if not token.isdigit():
            raise ValueError("dangling_id token must be digits")
        return token
    raise ValueError(f"unknown poison kind {kind!r}; expected one of {POISON_KINDS}")


def zadd_member(kube: Kube, namespace: str, inst: RedisInstance, key: str, member: str, score: float) -> None:
    _exec(kube, namespace, inst, "ZADD", key, f"{score:.0f}", member)


def zrem_member(kube: Kube, namespace: str, inst: RedisInstance, key: str, *members: str) -> int:
    """Surgical repair: remove only these members. Returns how many were present."""
    out = _exec(kube, namespace, inst, "ZREM", key, *members)
    return int(_lines(out)[-1].split()[-1])


def has_member(kube: Kube, namespace: str, inst: RedisInstance, key: str, member: str) -> bool:
    out = _lines(check_exec(kube, namespace, inst, "ZSCORE", key, member))
    return bool(out) and out[-1] not in ("(nil)", "")


def zset_members(
    kube: Kube, namespace: str, inst: RedisInstance, key: str, start: int = 0, stop: int = -1
) -> list[str]:
    return _lines(check_exec(kube, namespace, inst, "ZREVRANGE", key, str(start), str(stop)))


def zcard(kube: Kube, namespace: str, inst: RedisInstance, key: str) -> int:
    return int(_lines(check_exec(kube, namespace, inst, "ZCARD", key))[-1].split()[-1])


# --- key rename --------------------------------------------------------------------------------


def renamed_key(key: str, token: str) -> str:
    return f"{key}:bak-{token}"


def rename_key(kube: Kube, namespace: str, inst: RedisInstance, key: str, new_key: str) -> None:
    """``RENAME key new_key``: the old key reads as empty (a timeline read returns ``[]`` with HTTP 200).
    Refuses a missing source (RENAME would error) and a destination that exists (it would be overwritten)."""
    if key_exists(kube, namespace, inst, new_key):
        raise ComponentError(f"{inst.workload} in {namespace}: refusing to overwrite existing key {new_key}")
    _exec(kube, namespace, inst, "RENAME", key, new_key)


def key_exists(kube: Kube, namespace: str, inst: RedisInstance, key: str) -> bool:
    return _lines(check_exec(kube, namespace, inst, "EXISTS", key))[-1].split()[-1] == "1"


def merge_back(kube: Kube, namespace: str, inst: RedisInstance, key: str, backup_key: str) -> None:
    """Undo a rename without losing writes made since: compose re-creates ``key`` with newer posts, so a
    plain RENAME back would drop them. Union the two sorted sets (max score per member) and delete the backup."""
    if not key_exists(kube, namespace, inst, backup_key):
        raise ComponentError(f"{inst.workload} in {namespace}: backup key {backup_key} does not exist")
    _exec(kube, namespace, inst, "ZUNIONSTORE", key, "2", key, backup_key, "AGGREGATE", "MAX")
    _exec(kube, namespace, inst, "DEL", backup_key)


# --- stale cache -------------------------------------------------------------------------------


def make_stale(
    kube: Kube, namespace: str, inst: RedisInstance, key: str, *, drop: tuple[str, ...], add: tuple[str, ...] = ()
) -> None:
    """Make a cached set stale: remove ``drop`` and insert ``add``. Keep the key non-empty (a reader
    only goes to the database when the key is empty). ``add`` entries get score 0."""
    if not drop:
        raise ValueError("drop must name at least one member")
    zrem_member(kube, namespace, inst, key, *drop)
    for m in add:
        zadd_member(kube, namespace, inst, key, m, 0)


def repair_stale(kube: Kube, namespace: str, inst: RedisInstance, key: str) -> None:
    """``DEL key``: the next read refills it from the database (valid only for a cache in front of one)."""
    _exec(kube, namespace, inst, "DEL", key)


# --- write probe -------------------------------------------------------------------------------

PAUSED = "PAUSED"
_WRITE_PROBE_KEY = "__sregym_write_probe__"


def probe_write(kube: Kube, namespace: str, inst: RedisInstance, timeout_s: int = 3) -> str | None:
    """Try one growing write. Returns the error class that rejected it (``NOPERM``, ``OOM``,
    ``READONLY``, ...), ``PAUSED`` if it did not answer in time, or None when it landed (the probe key
    is removed again, best effort)."""
    out = _exec(kube, namespace, inst, "ZADD", _WRITE_PROBE_KEY, "1", "x", timeout_s=timeout_s, check=False)
    if out.strip() == "":
        return PAUSED
    err = error_reply(out)
    if err is not None:
        return err.split(" ", 1)[0]
    _exec(kube, namespace, inst, "DEL", _WRITE_PROBE_KEY, check=False)
    return None
