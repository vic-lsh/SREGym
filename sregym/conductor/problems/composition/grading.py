# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Pure grading rules for a co-located multi-fault composite.

Standard library only. The composite performs the I/O (listing pods, running the probe
pod) and uses these functions to decide:

* `collateral_verdict`: one namespace-wide pod health walk over raw pod JSON;
* the end-to-end traffic probe: `ENTRY_POINTS` (the one per-app table of entry
  endpoints), `probe_pod_manifest` (a short-lived busybox pod that requests each
  endpoint) and `parse_probe_log` (2xx/3xx fraction against `PROBE_MIN_OK_FRACTION`);
* `mitigation_success`: collateral AND traffic AND every per-fault verdict.

Why a dedicated probe instead of the apps' own load generators: the wrk2 jobs
(hotel_reservation, social_network) report per-window aggregates and fail a whole
window on any non-2xx, the Locust generators (astronomy_shop, train_ticket) report
cumulative stats since start (the faulty period included), and their log streams belong
to one app instance. A fresh probe measures the state at evaluation time only.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntryPoint:
    service: str  # Service name in the app namespace
    port: int
    paths: tuple[str, ...]  # GET paths a healthy app answers with 2xx/3xx


# The one table of per-app entry points, keyed by `Application.name`: the "Name" field of
# `sregym/service/metadata/<namespace>.json`, which is not always the short app name
# (astronomy_shop's is "OpenTelemetry Demo Astronomy Shop"). Enforced for every catalog app by
# tests/composition/test_colocated_grading.py, which reads those files. Paths are the user-facing reads each app's own load generator exercises:
#   hotel_reservation: DeathStarBench wrk2 mixed-workload_type_1.lua (/hotels, /recommendations)
#   social_network:    nginx-thrift index plus the wrk2-api timeline reads (nginx.conf)
#   astronomy_shop:    the frontend-proxy routes the upstream Locust file browses
#   train_ticket:      the UI dashboard (nginx) and its unauthenticated verify-code API
ENTRY_POINTS: Mapping[str, EntryPoint] = {
    "Hotel Reservation": EntryPoint(
        service="frontend",
        port=5000,
        paths=(
            "/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095",
            "/recommendations?require=rate&lat=38.0235&lon=-122.095",
            "/recommendations?require=dis&lat=37.7749&lon=-122.4194",
        ),
    ),
    "Social Network": EntryPoint(
        service="nginx-thrift",
        port=8080,
        paths=(
            "/",
            "/wrk2-api/home-timeline/read?user_id=1&start=0&stop=10",
            "/wrk2-api/user-timeline/read?user_id=1&start=0&stop=10",
        ),
    ),
    "OpenTelemetry Demo Astronomy Shop": EntryPoint(
        service="frontend-proxy",
        port=8080,
        paths=(
            "/",
            "/api/products",
            "/api/products/OLJCESPC7Z",
            "/api/recommendations?productIds=OLJCESPC7Z",
            "/api/data?contextKeys=telescopes",
        ),
    ),
    "Train Ticket": EntryPoint(
        service="ts-ui-dashboard",
        port=8080,
        paths=("/index.html", "/api/v1/verifycode/generate"),
    ),
}

PROBE_IMAGE = "busybox:1.36"  # already used by SREGym probe pods (oracles/*_mitigation.py)
PROBE_ROUNDS = 6  # every path is requested this many times
PROBE_REQUEST_TIMEOUT_S = 5
PROBE_MIN_OK_FRACTION = 0.9
PROBE_POD_LABEL = {"app.kubernetes.io/managed-by": "sregym-composite-probe"}

_OK, _FAIL, _DONE = "PROBE_OK", "PROBE_FAIL", "PROBE_DONE"


def probe_script(namespace: str, entry: EntryPoint, rounds: int = PROBE_ROUNDS) -> str:
    """A busybox sh script printing one PROBE_OK/PROBE_FAIL line per request, then PROBE_DONE.

    busybox wget follows redirects and exits non-zero on 4xx/5xx, timeouts, and
    connection errors, so PROBE_OK means a final 2xx (reached directly or via 3xx).
    """
    base = f"http://{entry.service}.{namespace}.svc.cluster.local:{entry.port}"
    lines = []
    for path in entry.paths:
        url = shlex.quote(base + path)
        tag = shlex.quote(path)
        lines.append(
            f"if wget -q -T {PROBE_REQUEST_TIMEOUT_S} -O /dev/null {url}; "
            f"then echo {_OK} {tag}; else echo {_FAIL} {tag}; fi"
        )
    body = "; ".join(lines)
    return f"i=0; while [ $i -lt {int(rounds)} ]; do {body}; i=$((i+1)); done; echo {_DONE}"


def probe_deadline_s(entry: EntryPoint, rounds: int = PROBE_ROUNDS) -> int:
    """Upper bound on the probe's run time (every request timing out) plus scheduling slack."""
    return rounds * len(entry.paths) * PROBE_REQUEST_TIMEOUT_S + 60


def probe_pod_manifest(name: str, namespace: str, entry: EntryPoint, rounds: int = PROBE_ROUNDS) -> dict[str, Any]:
    """The probe pod. It satisfies the `restricted` Pod Security Standard and declares
    small requests and limits, so a healthy namespace (PSA labels or quotas restored)
    admits it."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "labels": dict(PROBE_POD_LABEL)},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "activeDeadlineSeconds": probe_deadline_s(entry, rounds),
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 65534,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "probe",
                    "image": PROBE_IMAGE,
                    "command": ["sh", "-c", probe_script(namespace, entry, rounds)],
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "16Mi"},
                        "limits": {"cpu": "100m", "memory": "32Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                }
            ],
        },
    }


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    ok_count: int
    total: int
    failed_paths: tuple[str, ...]
    complete: bool

    def detail(self) -> str:
        if not self.complete:
            return f"probe incomplete ({self.ok_count}/{self.total} ok before it stopped)"
        text = f"{self.ok_count}/{self.total} requests 2xx/3xx (need >= {PROBE_MIN_OK_FRACTION:.0%})"
        if self.failed_paths:
            text += "; failing: " + ", ".join(self.failed_paths)
        return text


_LINE = re.compile(rf"^({_OK}|{_FAIL}) (.*)$")


def parse_probe_log(log: str, min_ok_fraction: float = PROBE_MIN_OK_FRACTION) -> ProbeResult:
    """Count the probe's per-request lines. A log without PROBE_DONE (pod killed, deadline
    hit, never started) or without any request fails."""
    ok_count = total = 0
    failed: list[str] = []
    complete = False
    for raw in log.splitlines():
        line = raw.strip()
        if line == _DONE:
            complete = True
            continue
        m = _LINE.match(line)
        if not m:
            continue
        total += 1
        if m.group(1) == _OK:
            ok_count += 1
        elif m.group(2) not in failed:
            failed.append(m.group(2))
    ok = complete and total > 0 and ok_count >= min_ok_fraction * total
    return ProbeResult(ok=ok, ok_count=ok_count, total=total, failed_paths=tuple(failed), complete=complete)


def collateral_verdict(pods: Iterable[Mapping[str, Any]]) -> tuple[bool, str]:
    """One namespace-wide health walk over raw pod JSON: did anything stay or get broken?

    Fails on an empty namespace, a pod not Running/Succeeded, a waiting container with a
    reason, a container terminated other than Completed, or a not-ready container in a
    Running pod. Probe pods of this module are ignored.
    """
    pods = [p for p in pods if not _is_probe_pod(p)]
    if not pods:
        return False, "no pods in namespace"
    for pod in pods:
        name = (pod.get("metadata") or {}).get("name", "?")
        status = pod.get("status") or {}
        phase = status.get("phase")
        if phase not in ("Running", "Succeeded"):
            return False, f"pod {name} phase={phase}"
        for cs in status.get("containerStatuses") or []:
            state = cs.get("state") or {}
            waiting = state.get("waiting") or {}
            terminated = state.get("terminated") or {}
            if waiting.get("reason"):
                return False, f"container {cs.get('name')} waiting: {waiting['reason']}"
            if terminated and terminated.get("reason") not in (None, "Completed"):
                return False, f"container {cs.get('name')} terminated: {terminated.get('reason')}"
            if not cs.get("ready") and phase == "Running":
                return False, f"container {cs.get('name')} not ready"
    return True, "ok"


def _is_probe_pod(pod: Mapping[str, Any]) -> bool:
    labels = (pod.get("metadata") or {}).get("labels") or {}
    return all(labels.get(k) == v for k, v in PROBE_POD_LABEL.items())


def mitigation_success(fault_verdicts: Iterable[bool], collateral_ok: bool, traffic_ok: bool) -> bool:
    """A composite is mitigated only if every fault is fixed, nothing else is broken, and
    end-to-end traffic succeeds."""
    verdicts = list(fault_verdicts)
    return bool(verdicts) and all(verdicts) and collateral_ok and traffic_ok
