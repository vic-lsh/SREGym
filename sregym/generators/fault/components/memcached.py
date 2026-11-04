"""Memcached component driver: text-protocol commands from a client pod in the namespace.

App-neutral: talks to ``<workload>:<port>`` (the Service of the same name) with busybox ``nc``,
so it works for any memcached whatever its image ships. Values travel base64-encoded.
Value corruption (``corrupt``) is pure and deterministic in its seed.
"""

from __future__ import annotations

import base64
import json
import random
import re
import shlex
from typing import Any

from sregym.conductor.problems.composition.app_bindings import MemcachedInstance
from sregym.generators.fault.components.kube import ComponentError, Kube, run_client_pod

CORRUPTION_MODES = ("garbage_bytes", "wrong_type", "stale_value")
_KEY = re.compile(r"^[!-~]{1,250}$")  # memcached keys: printable, no spaces or control chars


def _check_key(key: str) -> str:
    if not _KEY.match(key):
        raise ValueError(f"invalid memcached key {key!r}")
    return key


def _send(host: str, port: int, payload_b64: str, marker: str) -> str:
    """Shell that pipes a base64 payload to memcached and prints ``marker <base64 reply>``."""
    return (
        f'echo "{marker} $(echo {shlex.quote(payload_b64)} | base64 -d | nc -w 3 {host} {port} '
        f"| base64 | tr -d '\\n')\""
    )


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _run(kube: Kube, namespace: str, inst: MemcachedInstance, payloads: list[bytes]) -> list[bytes]:
    """Send each payload on its own connection; return each raw reply."""
    host = f"{inst.workload}.{namespace}.svc.cluster.local"
    script = "; ".join(_send(host, inst.port, _b64(p + b"quit\r\n"), f"@@R{i}") for i, p in enumerate(payloads))
    log = run_client_pod(kube, namespace, script + "; echo @@END", 60 + 5 * len(payloads), prefix="sregym-memcached")
    replies: dict[int, bytes] = {}
    for line in log.splitlines():
        m = re.match(r"^@@R(\d+) ?(\S*)$", line.strip())
        if m:
            replies[int(m.group(1))] = base64.b64decode(m.group(2)) if m.group(2) else b""
    if "@@END" not in log or len(replies) != len(payloads):
        raise ComponentError(f"memcached {inst.workload} in {namespace}: incomplete replies: {log[-300:]!r}")
    return [replies[i] for i in range(len(payloads))]


def parse_get(reply: bytes) -> bytes | None:
    """The value in a ``get`` reply, or None on a miss. Raises on an error reply."""
    if reply.startswith(b"END"):
        return None
    m = re.match(rb"^VALUE \S+ \d+ (\d+)\r\n", reply)
    if not m:
        raise ComponentError(f"unexpected memcached get reply: {reply[:120]!r}")
    start = m.end()
    return reply[start : start + int(m.group(1))]


def get_many(kube: Kube, namespace: str, inst: MemcachedInstance, keys: tuple[str, ...]) -> dict[str, bytes | None]:
    replies = _run(kube, namespace, inst, [f"get {_check_key(k)}\r\n".encode() for k in keys])
    return {k: parse_get(r) for k, r in zip(keys, replies, strict=True)}


def set_many(kube: Kube, namespace: str, inst: MemcachedInstance, items: dict[str, bytes], ttl_s: int) -> None:
    payloads = [f"set {_check_key(k)} 0 {int(ttl_s)} {len(v)}\r\n".encode() + v + b"\r\n" for k, v in items.items()]
    for key, reply in zip(items, _run(kube, namespace, inst, payloads), strict=True):
        if not reply.startswith(b"STORED"):
            raise ComponentError(f"memcached {inst.workload}: set {key} replied {reply[:80]!r}")


def delete_many(kube: Kube, namespace: str, inst: MemcachedInstance, keys: tuple[str, ...]) -> None:
    for key, reply in zip(
        keys, _run(kube, namespace, inst, [f"delete {_check_key(k)}\r\n".encode() for k in keys]), strict=True
    ):
        if not (reply.startswith(b"DELETED") or reply.startswith(b"NOT_FOUND")):
            raise ComponentError(f"memcached {inst.workload}: delete {key} replied {reply[:80]!r}")


def flush_all(kube: Kube, namespace: str, inst: MemcachedInstance) -> None:
    (reply,) = _run(kube, namespace, inst, [b"flush_all\r\n"])
    if not reply.startswith(b"OK"):
        raise ComponentError(f"memcached {inst.workload}: flush_all replied {reply[:80]!r}")


def corrupt(value: bytes, mode: str, seed: int) -> bytes:
    """A poisoned version of a cached value, deterministic in ``seed``.

    * ``garbage_bytes``: random non-UTF-8 bytes of the same length (any decoder fails);
    * ``wrong_type``: the JSON value with every scalar wrapped in a list (typed readers fail);
    * ``stale_value``: well-formed JSON whose every string is changed (readers succeed but serve
      wrong data: a mismatched password hash, a post with other text).
    Non-JSON values get the same treatment as raw text. Always differs from ``value``.
    """
    rng = random.Random(seed)
    if mode == "garbage_bytes":
        n = max(len(value), 8)
        return bytes([0xFF, 0xFE]) + bytes(rng.randrange(0x80, 0x100) for _ in range(n - 2))
    try:
        doc: Any = json.loads(value)
    except ValueError:
        doc = value.decode(errors="replace")
    tag = f"stale-{rng.randrange(16**6):06x}"
    if mode == "wrong_type":
        out = _map_scalars(doc, lambda v: [v])
    elif mode == "stale_value":
        out = _map_scalars(doc, lambda v: f"{v}~{tag}" if isinstance(v, str) else v)
        if out == doc:  # no strings: change numbers instead
            out = _map_scalars(doc, lambda v: v + 1 if isinstance(v, int) and not isinstance(v, bool) else v)
    else:
        raise ValueError(f"unknown corruption mode {mode!r}; expected one of {CORRUPTION_MODES}")
    data = json.dumps(out, separators=(",", ":")).encode()
    return data if data != value else data + b" "


def _map_scalars(v: Any, f) -> Any:
    if isinstance(v, dict):
        return {k: _map_scalars(x, f) for k, x in v.items()}
    if isinstance(v, list):
        return [_map_scalars(x, f) for x in v]
    return f(v)
