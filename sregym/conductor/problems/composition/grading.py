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
from urllib.parse import urlencode


@dataclass(frozen=True)
class Request:
    """One HTTP request to an app entry point: a GET, or a form POST when `form` is set."""

    path: str
    form: tuple[tuple[str, str], ...] | None = None  # urlencoded POST fields, in order; None = GET
    # Read-back for a write whose HTTP status does not prove it landed (social-network compose
    # answers 200 when the database dropped the write): the probe appends a fresh marker to the
    # value of form field `marker_field`, then GETs `readback`, which must contain the marker.
    marker_field: str | None = None
    readback: str | None = None

    def __post_init__(self) -> None:
        if (self.marker_field is None) != (self.readback is None):
            raise ValueError(f"{self.path}: marker_field and readback go together")
        if self.marker_field is not None and self.marker_field not in dict(self.form or ()):
            raise ValueError(f"{self.path}: marker_field {self.marker_field!r} is not a form field")

    @property
    def method(self) -> str:
        return "GET" if self.form is None else "POST"

    @property
    def tag(self) -> str:
        """The label the probe prints for this request (a GET path alone, for older logs)."""
        return self.path if self.form is None else f"POST {self.path}"

    def post_data(self) -> str | None:
        if self.form is None:
            return None
        return urlencode(self.form)


@dataclass(frozen=True)
class EntryPoint:
    """An app's user-facing entry: GET reads, plus optional write requests.

    `writes` are requested every round like `paths` (a healthy app answers 2xx/3xx; busybox
    wget follows a redirect with a GET). `setup` requests run once before the rounds, their
    outcome ignored: they create what the writes need (a user to log in as), idempotently.
    """

    service: str  # Service name in the app namespace
    port: int
    paths: tuple[str, ...]  # GET paths a healthy app answers with 2xx/3xx
    writes: tuple[Request, ...] = ()
    setup: tuple[Request, ...] = ()

    def requests(self) -> tuple[Request, ...]:
        """Every counted request of one probe round, reads first."""
        return tuple(Request(p) for p in self.paths) + self.writes

    def base_url(self, namespace: str) -> str:
        return f"http://{self.service}.{namespace}.svc.cluster.local:{self.port}"


# Social Network probe identities: user ids outside the wrk2 workload's 1..962 range, so the probe's
# own writes never touch timelines the workload or a fault's seeded-data snapshot reads. The follower
# makes every probe compose also write a home timeline (home-timeline-redis).
SOCIAL_PROBE_USER_ID = 9200
SOCIAL_PROBE_USERNAME = "sregym_probe_9200"
SOCIAL_PROBE_FOLLOWER_ID = 9201
SOCIAL_PROBE_FOLLOWER_USERNAME = "sregym_probe_9201"

# The one table of per-app entry points, keyed by `Application.name`: the "Name" field of
# `sregym/service/metadata/<namespace>.json`, which is not always the short app name
# (astronomy_shop's is "OpenTelemetry Demo Astronomy Shop"). Enforced for every catalog app by
# tests/composition/test_colocated_grading.py, which reads those files. Paths are the user-facing reads each app's own load generator exercises:
#   hotel_reservation: DeathStarBench wrk2 mixed-workload_type_1.lua (/hotels, /recommendations)
#   social_network:    nginx-thrift index plus the wrk2-api timeline reads (nginx.conf), and the
#                      write paths: compose-post, follow (wrk2-api) and login (/api/user/login) as a
#                      dedicated probe user (SOCIAL_PROBE_*), so per-component write faults are visible
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
        writes=(
            Request(
                "/wrk2-api/post/compose",
                (
                    ("user_id", str(SOCIAL_PROBE_USER_ID)),
                    ("username", SOCIAL_PROBE_USERNAME),
                    ("post_type", "0"),
                    ("text", "sregym traffic probe "),
                ),
                marker_field="text",
                readback=f"/wrk2-api/user-timeline/read?user_id={SOCIAL_PROBE_USER_ID}&start=0&stop=5",
            ),
            Request(
                "/wrk2-api/user/follow",
                (("user_id", str(SOCIAL_PROBE_FOLLOWER_ID)), ("followee_id", str(SOCIAL_PROBE_USER_ID))),
            ),
            Request("/api/user/login", (("username", SOCIAL_PROBE_USERNAME), ("password", SOCIAL_PROBE_USERNAME))),
        ),
        setup=tuple(
            Request(
                "/wrk2-api/user/register",
                (
                    ("first_name", "probe"),
                    ("last_name", str(uid)),
                    ("username", name),
                    ("password", name),
                    ("user_id", str(uid)),
                ),
            )
            for uid, name in (
                (SOCIAL_PROBE_USER_ID, SOCIAL_PROBE_USERNAME),
                (SOCIAL_PROBE_FOLLOWER_ID, SOCIAL_PROBE_FOLLOWER_USERNAME),
            )
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
    base = entry.base_url(namespace)
    lines = []
    for req in entry.requests():
        tag = shlex.quote(req.tag)
        lines.append(f"if {wget_command(base, req)}; then echo {_OK} {tag}; else echo {_FAIL} {tag}; fi")
    body = "; ".join(lines)
    setup = "".join(f"{wget_command(base, req)} >/dev/null 2>&1; " for req in entry.setup)
    return f"{setup}i=0; while [ $i -lt {int(rounds)} ]; do {body}; i=$((i+1)); done; echo {_DONE}"


_MARKER_SLOT = "MARKERSLOT"


def wget_command(base: str, req: Request, output: str = "/dev/null") -> str:
    """The busybox wget invocation for one request; it succeeds only on a 2xx/3xx answer.

    A GET exits non-zero on 4xx/5xx and timeouts (redirects followed). A POST succeeds when the
    first status line is 2xx/3xx: a write answered with a redirect (social-network login replies
    302 to a relative ``../../main.html`` busybox cannot follow) has already succeeded.
    """
    url = shlex.quote(base + req.path)
    post = req.post_data()
    if post is None:
        return f"wget -q -T {PROBE_REQUEST_TIMEOUT_S} -O {output} {url}"
    data = shlex.quote(post)
    if req.marker_field is not None:
        # The marker ($m, set per request by the probe) goes at the end of the marker field's value.
        form = tuple((k, v + _MARKER_SLOT if k == req.marker_field else v) for k, v in req.form or ())
        data = '"' + urlencode(form).replace(_MARKER_SLOT, "$m") + '"'
    status = (
        f"wget -q -S -T {PROBE_REQUEST_TIMEOUT_S} -O {output} --post-data {data} {url} 2>&1 "
        "| grep -m1 'HTTP/1' | grep -q ' [23][0-9][0-9] '"
    )
    if req.readback is None:
        return status
    back = shlex.quote(base + req.readback)
    return (
        f'{{ m="pm$$$(date +%s)$i"; {status} && sleep 1 && '
        f'wget -q -T {PROBE_REQUEST_TIMEOUT_S} -O - {back} 2>/dev/null | grep -q "$m"; }}'
    )


def probe_deadline_s(entry: EntryPoint, rounds: int = PROBE_ROUNDS) -> int:
    """Upper bound on the probe's run time (every request timing out) plus scheduling slack."""
    per_round = sum(2 + (2 if r.readback else 0) for r in entry.requests()) / 2
    return int((rounds * per_round + len(entry.setup)) * PROBE_REQUEST_TIMEOUT_S) + 60


def probe_pod_manifest(name: str, namespace: str, entry: EntryPoint, rounds: int = PROBE_ROUNDS) -> dict[str, Any]:
    """The probe pod. It satisfies the `restricted` Pod Security Standard and declares
    small requests and limits, so a healthy namespace (PSA labels or quotas restored)
    admits it."""
    return client_pod_manifest(name, namespace, probe_script(namespace, entry, rounds), probe_deadline_s(entry, rounds))


def client_pod_manifest(name: str, namespace: str, script: str, deadline_s: int) -> dict[str, Any]:
    """A short-lived busybox pod running `script` with `sh -c`, labelled as a probe pod (so the
    collateral walk ignores it). Used by the traffic probe and by component drivers that talk to
    an app's services from inside the namespace (seeded-data snapshots, memcached commands)."""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "labels": dict(PROBE_POD_LABEL)},
        "spec": {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "activeDeadlineSeconds": int(deadline_s),
            "securityContext": {
                "runAsNonRoot": True,
                "runAsUser": 65534,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "containers": [
                {
                    "name": "probe",
                    "image": PROBE_IMAGE,
                    "command": ["sh", "-c", script],
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
