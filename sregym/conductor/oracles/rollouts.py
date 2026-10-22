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
"""The settle wait that mitigation oracles run before judging a namespace.

One implementation for every oracle (they each carried a copy), so a caller that already
waited can tell its oracles to skip it (``Oracle.namespace_settled``). Standard library only.

A namespace is settled when nothing is still rolling out: every Deployment and StatefulSet
has observed its latest spec and has every desired replica updated and ready with no old
replica left, and no pod is being deleted (``metadata.deletionTimestamp``). The last
condition matters because a Deployment reports itself complete while its old ReplicaSet's
pod is still Terminating, and pod-walking oracles fail that pod.

The wait is bounded and filters nothing: on timeout the caller grades the state as it
stands, so a pod stuck Terminating because a fault is unfixed (a finalizer that never
clears) still fails the oracle that checks it. Waiting does change *when* grading looks:
a crash-looping or flapping pod can be caught in a brief all-ready window and pass a
one-shot pod walk such as MitigationOracle's. That exposure predates this wait (the old
60 s one polled the same way) and grows slightly with the longer bound.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

# Bound on the settle wait after the agent's submission. An old pod gets its
# terminationGracePeriodSeconds (30 s by default) before it is killed, and the kubelet may
# be slow under load, so the bound leaves room for one grace period plus a rollout.
ROLLOUT_SETTLE_S = 90.0
ROLLOUT_POLL_S = 5.0

# How many in-flight objects a reason names before summarising the rest.
_REASON_ITEMS = 5

Obj = Mapping[str, Any]


@dataclass(frozen=True)
class Workloads:
    """One read of a namespace's rollout state, as API JSON objects."""

    deployments: tuple[Obj, ...] = ()
    stateful_sets: tuple[Obj, ...] = ()
    pods: tuple[Obj, ...] = ()


@dataclass(frozen=True)
class Settle:
    """Outcome of one settle wait. ``reason`` is empty when settled, else names what was
    still in flight at the last read (or why the namespace could not be read)."""

    settled: bool
    waited_s: float
    reason: str = ""


def _get(obj: Obj, *path: str) -> Any:
    for key in path:
        if not isinstance(obj, Mapping):
            return None
        obj = obj.get(key)
    return obj


def _name(obj: Obj) -> str:
    return _get(obj, "metadata", "name") or "?"


def _desired(obj: Obj) -> int:
    replicas = _get(obj, "spec", "replicas")
    return 1 if replicas is None else replicas  # unset means the API default, 1


def _stale(obj: Obj) -> bool:
    """The controller has not yet acted on the latest spec (its status describes an older one)."""
    return (_get(obj, "status", "observedGeneration") or 0) < (_get(obj, "metadata", "generation") or 0)


def _deployment_pending(dep: Obj) -> str | None:
    desired, status = _desired(dep), _get(dep, "status") or {}
    updated = status.get("updatedReplicas") or 0
    if _stale(dep):
        return f"deployment/{_name(dep)}: spec change not yet observed"
    if updated < desired or (status.get("readyReplicas") or 0) < desired or (status.get("unavailableReplicas") or 0):
        return f"deployment/{_name(dep)}: {status.get('readyReplicas') or 0}/{desired} ready, {updated} updated"
    if (status.get("replicas") or 0) > updated:
        return f"deployment/{_name(dep)}: {(status.get('replicas') or 0) - updated} old replica(s) left"
    return None


def _stateful_set_pending(sts: Obj) -> str | None:
    """As ``kubectl rollout status``: with a partition, the replicas at or above it are
    updated; without one, the current revision has caught up with the update revision."""
    desired, status = _desired(sts), _get(sts, "status") or {}
    if _stale(sts):
        return f"statefulset/{_name(sts)}: spec change not yet observed"
    if (status.get("readyReplicas") or 0) < desired:
        return f"statefulset/{_name(sts)}: {status.get('readyReplicas') or 0}/{desired} ready"
    strategy = _get(sts, "spec", "updateStrategy") or {}
    if strategy.get("type", "RollingUpdate") != "RollingUpdate":
        return None  # OnDelete: nothing rolls out until someone deletes a pod
    partition = _get(strategy, "rollingUpdate", "partition") or 0
    if partition:
        if (status.get("updatedReplicas") or 0) < desired - partition:
            return f"statefulset/{_name(sts)}: {status.get('updatedReplicas') or 0}/{desired - partition} updated"
    elif status.get("updateRevision") != status.get("currentRevision"):
        return f"statefulset/{_name(sts)}: revision {status.get('updateRevision')} still rolling out"
    return None


def _selects(dep: Obj, pod: Obj) -> bool:
    match = _get(dep, "spec", "selector", "matchLabels") or {}
    labels = _get(pod, "metadata", "labels") or {}
    return bool(match) and all(labels.get(k) == v for k, v in match.items())


def pending(workloads: Workloads, only: str | None = None) -> list[str]:
    """What is still in flight, one description per object; empty when settled.

    With ``only``, just that Deployment and the pods its selector matches."""
    deployments = [d for d in workloads.deployments if only is None or _name(d) == only]
    stateful_sets = workloads.stateful_sets if only is None else ()
    pods = workloads.pods if only is None else [p for p in workloads.pods if any(_selects(d, p) for d in deployments)]
    found = [_deployment_pending(d) for d in deployments]
    found += [_stateful_set_pending(s) for s in stateful_sets]
    found += [f"pod/{_name(p)}: terminating" for p in pods if _get(p, "metadata", "deletionTimestamp")]
    return [f for f in found if f]


def _summary(items: list[str]) -> str:
    more = len(items) - _REASON_ITEMS
    return "; ".join(items[:_REASON_ITEMS]) + (f"; +{more} more" if more > 0 else "")


def wait_for_rollouts(
    read: Callable[[], Workloads],
    timeout_s: float = ROLLOUT_SETTLE_S,
    poll_s: float = ROLLOUT_POLL_S,
    only: str | None = None,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Settle:
    """Poll ``read()`` until nothing is ``pending`` or ``timeout_s`` has passed. Returns at
    once, without sleeping, if the namespace is already settled. A ``read`` that raises ends
    the wait as not settled, with the error as the reason."""
    start = clock()
    deadline = start + timeout_s
    while True:
        try:
            items = pending(read(), only)
        except Exception as e:  # an unreadable namespace is graded as it stands, never waited on
            return Settle(False, clock() - start, f"reading the namespace failed: {type(e).__name__}: {str(e)[:200]}")
        if not items:
            return Settle(True, clock() - start)
        if clock() >= deadline:
            return Settle(False, clock() - start, f"not settled after {timeout_s:g}s: {_summary(items)}")
        sleep(poll_s)


def namespace_reader(core_api: Any, apps_api: Any, namespace: str) -> Callable[[], Workloads]:
    """A ``read`` for ``wait_for_rollouts`` over Kubernetes ``CoreV1Api`` and ``AppsV1Api``
    clients: three list calls per read, each returning raw JSON."""

    def items(response: Any) -> tuple[Obj, ...]:
        return tuple(json.loads(response.data).get("items") or ())

    def read() -> Workloads:
        return Workloads(
            deployments=items(apps_api.list_namespaced_deployment(namespace, _preload_content=False)),
            stateful_sets=items(apps_api.list_namespaced_stateful_set(namespace, _preload_content=False)),
            pods=items(core_api.list_namespaced_pod(namespace, _preload_content=False)),
        )

    return read
