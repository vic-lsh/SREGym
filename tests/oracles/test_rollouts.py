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
"""The shared rollout-settle wait (``oracles/rollouts.py``), ``Oracle.settle_rollouts``, and
the pod-walking mitigation oracles that grade only after it: a Deployment reports itself
complete while its old ReplicaSet's pod is still Terminating, and those oracles failed that
pod. Over in-memory Kubernetes API clients (no cluster)."""

import itertools
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException

from sregym.conductor.oracles import rollouts
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.deployment_readiness import DeploymentReadinessOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.service.kubectl import KubeCtl

NS = "hotel-reservation"
_ORACLES = Path(__file__).resolve().parents[2] / "sregym" / "conductor" / "oracles"


def _dep(name, replicas=1, updated=1, ready=1, unavailable=None, total=None, generation=1, observed=1, labels=None):
    return {
        "metadata": {"name": name, "generation": generation},
        "spec": {"replicas": replicas, "selector": {"matchLabels": labels or {"app": name}}},
        "status": {
            "observedGeneration": observed,
            "replicas": updated if total is None else total,
            "updatedReplicas": updated,
            "readyReplicas": ready,
            "unavailableReplicas": unavailable,
        },
    }


def _sts(name, replicas=1, ready=1, updated=1, current="r1", update="r1", strategy=None, generation=1, observed=1):
    return {
        "metadata": {"name": name, "generation": generation},
        "spec": {"replicas": replicas, "updateStrategy": strategy or {"type": "RollingUpdate"}},
        "status": {
            "observedGeneration": observed,
            "readyReplicas": ready,
            "updatedReplicas": updated,
            "currentRevision": current,
            "updateRevision": update,
        },
    }


def _partition(n):
    return {"type": "RollingUpdate", "rollingUpdate": {"partition": n}}


def _pod(name, app="a", terminating=False, phase="Running", ready=True):
    pod = {
        "metadata": {"name": name, "labels": {"app": app}},
        "status": {
            "phase": phase,
            "containerStatuses": [{"name": app, "ready": ready, "state": {"running": {}} if ready else {}}],
        },
    }
    if terminating:
        pod["metadata"]["deletionTimestamp"] = "2026-10-22T19:25:30Z"
    return pod


def _w(deployments=(), stateful_sets=(), pods=()):
    return rollouts.Workloads(tuple(deployments), tuple(stateful_sets), tuple(pods))


# --- what counts as in flight -------------------------------------------------------------


@pytest.mark.parametrize(
    "workloads, in_flight",
    [
        (_w(), []),
        (_w([_dep("a")], [_sts("db")], [_pod("a-1")]), []),
        (_w([_dep("a", replicas=None)]), []),
        # Scaled to zero is settled, not "1 desired" (deployment_readiness used to count it as 1).
        (_w([_dep("a", replicas=0, updated=None, ready=None)]), []),
        (_w([_dep("a", replicas=2, ready=1, updated=2)]), ["deployment/a: 1/2 ready, 2 updated"]),
        (_w([_dep("a", updated=0)]), ["deployment/a: 1/1 ready, 0 updated"]),
        (_w([_dep("a", unavailable=1)]), ["deployment/a: 1/1 ready, 1 updated"]),
        # The status still describes the previous spec: the controller has not acted yet.
        (_w([_dep("a", generation=2, observed=1)]), ["deployment/a: spec change not yet observed"]),
        # `kubectl rollout status`: "1 old replicas are pending termination".
        (_w([_dep("a", total=2)]), ["deployment/a: 1 old replica(s) left"]),
        # The Deployment looks complete, but its old ReplicaSet's pod is still shutting down.
        (_w([_dep("a")], pods=[_pod("a-new"), _pod("a-old", terminating=True)]), ["pod/a-old: terminating"]),
        (_w(stateful_sets=[_sts("db", ready=0)]), ["statefulset/db: 0/1 ready"]),
        (_w(stateful_sets=[_sts("db", update="r2")]), ["statefulset/db: revision r2 still rolling out"]),
        (_w(stateful_sets=[_sts("db", generation=3, observed=2)]), ["statefulset/db: spec change not yet observed"]),
        (_w(stateful_sets=[_sts("db", update="r2", strategy={"type": "OnDelete"})]), []),
        (
            _w(stateful_sets=[_sts("db", replicas=3, ready=3, updated=1, update="r2", strategy=_partition(2))]),
            [],
        ),
        (
            _w(stateful_sets=[_sts("db", replicas=3, ready=3, updated=0, update="r2", strategy=_partition(2))]),
            ["statefulset/db: 0/1 updated"],
        ),
    ],
)
def test_pending_names_everything_still_in_flight(workloads, in_flight):
    assert rollouts.pending(workloads) == in_flight


def test_pending_for_one_deployment_ignores_other_workloads_and_their_pods():
    workloads = _w(
        [_dep("target"), _dep("other", ready=0)],
        [_sts("db", ready=0)],
        [_pod("other-old", app="other", terminating=True)],
    )

    assert rollouts.pending(workloads, only="target") == []
    target_old = _pod("target-old", app="target", terminating=True)
    assert rollouts.pending(_w(workloads.deployments, pods=[target_old]), only="target") == [
        "pod/target-old: terminating"
    ]


# --- the bounded wait ---------------------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.now += s


def test_an_old_pod_that_finishes_terminating_within_the_bound_is_waited_for():
    clock = FakeClock()
    old = _pod("a-old", terminating=True)

    def read():  # the old pod is gone once 10 s have passed
        return _w([_dep("a")], pods=[_pod("a-new")] + ([old] if clock.now < 10 else []))

    settle = rollouts.wait_for_rollouts(read, 90, 5, clock=clock, sleep=clock.sleep)

    assert settle == rollouts.Settle(True, 10.0)


def test_a_pod_stuck_terminating_ends_the_wait_at_the_bound_and_is_named():
    clock = FakeClock()

    settle = rollouts.wait_for_rollouts(
        lambda: _w(pods=[_pod("a-old", terminating=True)]), 60, 5, clock=clock, sleep=clock.sleep
    )

    assert settle == rollouts.Settle(False, 60.0, "not settled after 60s: pod/a-old: terminating")


def test_a_settled_namespace_is_read_once_and_never_waited_on():
    clock, reads = FakeClock(), []

    def read():
        reads.append(clock.now)
        return _w([_dep("a")], pods=[_pod("a-1")])

    def sleep(s):
        raise AssertionError("slept on a settled namespace")

    assert rollouts.wait_for_rollouts(read, 90, 5, clock=clock, sleep=sleep) == rollouts.Settle(True, 0.0)
    assert reads == [0.0]


def test_an_unreadable_namespace_ends_the_wait_with_the_error():
    def read():
        raise RuntimeError("Forbidden")

    settle = rollouts.wait_for_rollouts(read, 90, 5, clock=FakeClock(), sleep=FakeClock().sleep)

    assert settle == rollouts.Settle(False, 0.0, "reading the namespace failed: RuntimeError: Forbidden")


def test_a_long_reason_names_the_first_few_and_counts_the_rest():
    pods = [_pod(f"p{i}", terminating=True) for i in range(7)]

    settle = rollouts.wait_for_rollouts(lambda: _w(pods=pods), 0, 5, clock=FakeClock(), sleep=FakeClock().sleep)

    assert settle.reason.endswith("pod/p4: terminating; +2 more")


@pytest.mark.parametrize("poll_s", [1.0, 3.0, 5.0])
def test_the_wait_returns_at_the_first_settled_read_and_never_outlasts_its_bound(poll_s):
    """Every sequence of up to 6 read outcomes and every bound of 0..8 polls (exhaustive,
    where robust-skills samples the same space with hypothesis). Read k (at k * poll_s) sees
    state k (the last state repeats); True means settled."""
    for n in range(1, 7):
        for states in itertools.product([False, True], repeat=n):
            for timeout_polls in range(9):
                clock = FakeClock()
                timeout_s = timeout_polls * poll_s

                def read(clock=clock, states=states):
                    settled = states[min(round(clock.now / poll_s), len(states) - 1)]
                    return _w(pods=[] if settled else [_pod("old", terminating=True)])

                settle = rollouts.wait_for_rollouts(read, timeout_s, poll_s, clock=clock, sleep=clock.sleep)

                visible = [states[min(k, len(states) - 1)] for k in range(timeout_polls + 1)]
                case = (states, timeout_polls)
                assert settle.settled is any(visible), case
                assert settle.waited_s == (visible.index(True) if any(visible) else timeout_polls) * poll_s, case
                assert settle.waited_s <= timeout_s, case


# --- in-memory Kubernetes API clients -------------------------------------------------------


class _Model(SimpleNamespace):
    """A Kubernetes client model: attributes in snake_case; an unset field reads as None."""

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return None


def _snake(key):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower()


def _model(value):
    if isinstance(value, dict):
        return _Model(**{_snake(k): _model(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_model(v) for v in value]
    return value


class FakeApis:
    """The ``CoreV1Api``/``AppsV1Api`` calls the settle reader and ``KubeCtl`` make, over JSON
    objects: raw JSON with ``_preload_content=False``, models otherwise. A pod in
    ``gone_after`` disappears once the pod list has been read that many times (its grace
    period ends while the oracle waits). ``fail`` names a list method that raises, as an RBAC
    denial would."""

    def __init__(self, deployments=(), pods=(), stateful_sets=(), gone_after=None, fail=None):
        self.deployments, self.pods, self.stateful_sets = list(deployments), list(pods), list(stateful_sets)
        self.gone_after = dict(gone_after or {})
        self.fail = fail
        self.pod_reads = 0

    def _respond(self, method, namespace, items, preload):
        assert namespace == NS
        if method == self.fail:
            raise RuntimeError("Forbidden")
        if preload:
            return _model({"items": items})
        return SimpleNamespace(data=json.dumps({"items": items}))

    def list_namespaced_deployment(self, namespace, _preload_content=True):
        return self._respond("list_namespaced_deployment", namespace, self.deployments, _preload_content)

    def list_namespaced_stateful_set(self, namespace, _preload_content=True):
        return self._respond("list_namespaced_stateful_set", namespace, self.stateful_sets, _preload_content)

    def list_namespaced_pod(self, namespace, _preload_content=True):
        self.pod_reads += 1
        pods = [p for p in self.pods if self.pod_reads <= self.gone_after.get(p["metadata"]["name"], self.pod_reads)]
        return self._respond("list_namespaced_pod", namespace, pods, _preload_content)

    def read_namespaced_deployment(self, name, namespace):
        assert namespace == NS
        for dep in self.deployments:
            if dep["metadata"]["name"] == name:
                return _model(dep)
        raise ApiException(status=404, reason="Not Found")


def _kubectl(apis):
    # The real KubeCtl over fake API clients; its __init__ loads a kubeconfig.
    kubectl = object.__new__(KubeCtl)
    kubectl.core_v1_api = kubectl.apps_v1_api = apis
    return kubectl


@pytest.mark.parametrize(
    "apis, settled, reason",
    [
        (FakeApis([_dep("frontend")], [_pod("frontend-1", app="frontend")], [_sts("db")]), True, ""),
        (FakeApis(fail="list_namespaced_pod"), False, "reading the namespace failed: RuntimeError: Forbidden"),
        (
            FakeApis(stateful_sets=[_sts("db", ready=0)]),
            False,
            "not settled after 0s: statefulset/db: 0/1 ready",
        ),
    ],
)
def test_namespace_reader_reads_deployments_stateful_sets_and_pods(apis, settled, reason):
    read = rollouts.namespace_reader(apis, apis, NS)

    settle = rollouts.wait_for_rollouts(read, 0, 5, clock=FakeClock(), sleep=FakeClock().sleep)

    assert (settle.settled, settle.reason) == (settled, reason)


# --- Oracle.settle_rollouts -----------------------------------------------------------------


class Plain(Oracle):
    def evaluate(self):
        return {"success": True}


def test_presettled_oracle_skips_its_wait_and_others_still_wait():
    reads = []

    def read():
        reads.append(1)
        return _w([_dep("a")])

    oracle = Plain(problem=None)
    assert oracle.settle_rollouts(read=read).settled is True
    assert reads == [1]

    oracle.namespace_settled = True
    assert oracle.settle_rollouts(read=read) is None
    assert reads == [1]


def test_settle_reads_the_problems_namespace_through_its_kubectl():
    apis = FakeApis([_dep("a")], [_pod("a-old", terminating=True)])
    oracle = Plain(SimpleNamespace(namespace=NS, kubectl=_kubectl(apis)))

    settle = oracle.settle_rollouts(timeout_s=0)

    assert settle == rollouts.Settle(False, settle.waited_s, "not settled after 0s: pod/a-old: terminating")
    assert apis.pod_reads == 1


# --- the pod-walking oracles this fixes ----------------------------------------------------


def _with_quick_settle(oracle_cls, timeout_s):
    class Quick(oracle_cls):
        # The oracle's own settle call with the bound and poll the test can afford; the
        # wait's timing is covered above with an injected clock.
        def settle_rollouts(self, **kwargs):
            return super().settle_rollouts(**{**kwargs, "timeout_s": timeout_s, "poll_s": 0})

    return Quick


def _graded(oracle_cls, apis, timeout_s=rollouts.ROLLOUT_SETTLE_S):
    problem = SimpleNamespace(namespace=NS, kubectl=_kubectl(apis), faulty_service="recommendation")
    return _with_quick_settle(oracle_cls, timeout_s)(problem).evaluate()


def _rolled_out_with_old_pod_terminating():
    """The fix rolled out, but the old ReplicaSet's pod (stuck in its init container, so phase
    Pending) is still Terminating when grading begins."""
    old = _pod("recommendation-68f4ff69c6-gdwhh", app="recommendation", phase="Pending", ready=False, terminating=True)
    deployments = [_dep("frontend"), _dep("recommendation")]
    pods = [_pod("frontend-1", app="frontend"), _pod("recommendation-58c5ff98bf-mcjr8", app="recommendation"), old]
    return deployments, pods, old["metadata"]["name"]


@pytest.mark.parametrize("oracle_cls", [MitigationOracle, DeploymentReadinessOracle])
def test_an_old_pod_still_terminating_at_submission_is_waited_out_and_the_fix_passes(oracle_cls):
    deployments, pods, old = _rolled_out_with_old_pod_terminating()
    apis = FakeApis(deployments, pods, gone_after={old: 2})  # gone after two settle reads

    assert _graded(oracle_cls, apis)["success"] is True
    assert apis.pod_reads == 4  # three settle reads, then the oracle's own pod walk


@pytest.mark.parametrize("oracle_cls", [MitigationOracle, DeploymentReadinessOracle])
def test_a_pod_stuck_terminating_still_fails_after_the_bounded_wait(oracle_cls):
    # A finalizer the agent never cleared: the pod stays Terminating however long we wait.
    deployments, pods, _ = _rolled_out_with_old_pod_terminating()

    assert _graded(oracle_cls, FakeApis(deployments, pods), timeout_s=0)["success"] is False


@pytest.mark.parametrize("oracle_cls", [MitigationOracle, DeploymentReadinessOracle])
def test_a_settled_healthy_namespace_passes(oracle_cls):
    deployments, pods, old = _rolled_out_with_old_pod_terminating()
    apis = FakeApis(deployments, [p for p in pods if p["metadata"]["name"] != old])

    assert _graded(oracle_cls, apis, timeout_s=0)["success"] is True


# --- one implementation --------------------------------------------------------------------

# Oracles that keep a wait of their own, and why it is not a copy of the namespace settle.
_OWN_WAITS = {
    "calico_route_reflector_mitigation.py": "waits on two namespaces for 180 s",
}


def test_no_oracle_keeps_its_own_rollout_settle_copy():
    # One implementation (rollouts.py) is what lets a caller that already waited skip it.
    copies = {p.name for p in _ORACLES.rglob("*.py") if re.search(r"def _wait_for_rollout", p.read_text())}

    assert copies == set(_OWN_WAITS)
