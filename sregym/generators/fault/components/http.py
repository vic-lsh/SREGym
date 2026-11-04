"""App-neutral HTTP from inside the namespace: fetch entry-point responses, seed and snapshot data.

Implements the binding's ``IntegrityCheck``: ``capture`` takes the seeded-data snapshot before
injection, ``compare`` checks the same reads return the same data after mitigation. Pure parts
(script, log parsing, normalization) are separate from the one client-pod call.
"""

from __future__ import annotations

import base64
import json
import shlex
import time
from dataclasses import dataclass
from typing import Any

from sregym.conductor.problems.composition.app_bindings import AppBinding
from sregym.conductor.problems.composition.grading import Request, wget_command
from sregym.generators.fault.components.kube import ComponentError, Kube, run_client_pod

_BEGIN, _END = "@@REQ", "@@END"
SEED_ATTEMPTS = 3
READ_RETRIES = 2


@dataclass(frozen=True)
class Response:
    ok: bool  # 2xx (after redirects)
    body: str


def fetch_script(base: str, requests: tuple[Request, ...]) -> str:
    """One line per request: ``@@REQ <i> OK|FAIL <base64 body>``, then ``@@END``."""
    parts = []
    for i, req in enumerate(requests):
        cmd = wget_command(base, req, output="/tmp/body")
        parts.append(
            f"rm -f /tmp/body; if {cmd} 2>/dev/null; then s=OK; else s=FAIL; fi; "
            f"echo \"{_BEGIN} {i} $s $(base64 /tmp/body 2>/dev/null | tr -d '\\n')\""
        )
    return "; ".join(parts) + f"; echo {_END}"


def parse_fetch_log(log: str, n: int) -> list[Response]:
    out: dict[int, Response] = {}
    complete = False
    for line in log.splitlines():
        if line.strip() == _END:
            complete = True
            continue
        fields = line.strip().split(" ", 3)
        if len(fields) >= 3 and fields[0] == _BEGIN:
            body = base64.b64decode(fields[3]).decode(errors="replace") if len(fields) == 4 else ""
            out[int(fields[1])] = Response(fields[2] == "OK", body)
    if not complete or len(out) != n:
        raise ComponentError(f"fetch log incomplete ({len(out)}/{n} responses): {log[-300:]!r}")
    return [out[i] for i in range(n)]


def fetch(kube: Kube, binding: AppBinding, namespace: str, requests: tuple[Request, ...]) -> list[Response]:
    if not requests:
        return []
    script = fetch_script(binding.entry.base_url(namespace), requests)
    deadline = 30 + 6 * len(requests)
    return parse_fetch_log(run_client_pod(kube, namespace, script, deadline, prefix="sregym-fetch"), len(requests))


def normalize(body: str, volatile_keys: tuple[str, ...] = ()) -> Any:
    """Parsed JSON with ``volatile_keys`` dropped from every object; non-JSON bodies stay text."""
    try:
        value = json.loads(body)
    except ValueError:
        return body.strip()

    def strip(v: Any) -> Any:
        if isinstance(v, dict):
            return {k: strip(x) for k, x in v.items() if k not in volatile_keys}
        if isinstance(v, list):
            return [strip(x) for x in v]
        return v

    return strip(value)


def _empty(v: Any) -> bool:
    return v is None or v in ("", [], {})


@dataclass(frozen=True)
class Snapshot:
    reads: tuple[str, ...]
    values: tuple[Any, ...]  # normalized bodies, one per read

    def json_field_values(self, name: str) -> tuple[str, ...]:
        """Every value of JSON key ``name`` anywhere in the snapshot, as strings, first-seen order."""
        return json_field_values(self.values, name)


def json_field_values(values: Any, name: str) -> tuple[str, ...]:
    """Every scalar value of JSON key ``name`` anywhere in ``values``, as strings, first-seen order."""
    found: list[str] = []

    def walk(v: Any) -> None:
        if isinstance(v, dict):
            for k, x in v.items():
                if k == name and not isinstance(x, (dict, list)) and str(x) not in found:
                    found.append(str(x))
                walk(x)
        elif isinstance(v, (list, tuple)):
            for x in v:
                walk(x)

    walk(values)
    return tuple(found)


def capture(kube: Kube, binding: AppBinding, namespace: str) -> Snapshot:
    """Snapshot the binding's integrity reads, seeding first if any read is empty or failing.
    Raises if the reads still fail or are empty after seeding."""
    check = binding.integrity
    if check is None:
        raise ComponentError(f"{binding.app} has no integrity check in its binding")
    reads = tuple(Request(p) for p in check.reads)
    responses = fetch(kube, binding, namespace, reads)
    for attempt in range(SEED_ATTEMPTS):
        if not any(not r.ok or _empty(normalize(r.body, check.volatile_keys)) for r in responses):
            break
        # A freshly deployed app fails its first requests while connection pools warm up: retry.
        time.sleep(attempt * 5)
        fetch(kube, binding, namespace, check.seed)
        responses = fetch(kube, binding, namespace, reads)
    values = tuple(normalize(r.body, check.volatile_keys) for r in responses)
    bad = [p for p, r, v in zip(check.reads, responses, values, strict=True) if not r.ok or _empty(v)]
    if bad:
        raise ComponentError(f"{binding.app} integrity reads failing or empty after seeding: {bad}")
    return Snapshot(check.reads, values)


def compare(kube: Kube, binding: AppBinding, namespace: str, snapshot: Snapshot) -> tuple[bool, str]:
    """Do the snapshot's reads return the same data now? ``(ok, detail)``."""
    check = binding.integrity
    assert check is not None
    reads = tuple(Request(p) for p in snapshot.reads)
    responses = fetch(kube, binding, namespace, reads)
    for _ in range(READ_RETRIES):
        # The first request after a pod restart can fail once on a stale pooled connection; a
        # failed read is retried, changed data never is.
        if all(r.ok for r in responses):
            break
        time.sleep(3)
        responses = fetch(kube, binding, namespace, reads)
    return diff(snapshot, responses, check.volatile_keys)


def diff(snapshot: Snapshot, responses: list[Response], volatile_keys: tuple[str, ...] = ()) -> tuple[bool, str]:
    """Pure comparison of current responses against a snapshot."""
    for path, want, got in zip(snapshot.reads, snapshot.values, responses, strict=True):
        if not got.ok:
            return False, f"integrity read failed: {path}"
        now = normalize(got.body, volatile_keys)
        if now != want:
            return False, f"seeded data changed at {path}: was {_short(want)}, now {_short(now)}"
    return True, f"{len(snapshot.reads)} seeded reads unchanged"


def _short(v: Any) -> str:
    text = json.dumps(v) if not isinstance(v, str) else v
    return shlex.quote(text[:160])
