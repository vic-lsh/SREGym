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
"""ColocatedComposite over fake sub-problems and an in-memory ClusterIO (no cluster, no LLM)."""

import ast
import copy
import inspect
import json
import threading
from pathlib import Path

import pytest

from sregym.conductor.oracles import rollouts
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.colocated_composite import ColocatedComposite, CompositeDiagnosisOracle, KubeClusterIO
from sregym.conductor.problems.composition import build_composite, grading, liveness
from sregym.conductor.problems.composition.generator import draw_plan, take_plan
from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint, Tier

NS = "hotel-reservation"


class FakeApp:
    def __init__(self, name="Hotel Reservation", wrk=None):
        self.name = name
        self.namespace = NS
        if wrk is not None:
            self.wrk = wrk


class FakeClock:
    """Simulated time for the settle wait. `at(t, fn)` runs `fn` once time reaches `t`, so a
    test can make cluster state change while the wait polls."""

    def __init__(self):
        self.now = 0.0
        self.events = []

    def __call__(self):
        return self.now

    def at(self, t, fn):
        self.events.append((t, fn))

    def sleep(self, s):
        self.now += s
        due = [e for e in self.events if e[0] <= self.now]
        self.events = [e for e in self.events if e[0] > self.now]
        for _, fn in due:
            fn()


class FakeCluster:
    """In-memory ClusterIO. Objects live in `objects[kind][name]`; reads return deep copies,
    like the API server. Kinds in `unreadable` fail to list, as an RBAC denial would.
    The settle wait is the real one (`rollouts.wait_for_rollouts`) over `deployments` (with
    status) and `pods`, on a simulated clock."""

    def __init__(self):
        self.kubectl = object()
        self.objects = {kind: {} for kind in liveness.ALL_KINDS}
        self.objects["Namespace"][NS] = {"metadata": {"name": NS, "labels": {}}}
        self.unreadable = set()
        self.pods = [_pod("frontend")]
        self.probe_log = "\n".join(["PROBE_OK /"] * 10 + ["PROBE_DONE"])
        self.probe_phase = "Succeeded"
        self.run_manifests = []
        self.deployments = []
        self.clock = FakeClock()
        self.settle_waits = []

    def put(self, kind, obj):
        self.objects[kind][obj["metadata"]["name"]] = copy.deepcopy(obj)

    def delete(self, kind, name):
        del self.objects[kind][name]

    def read_specs(self, namespace):
        assert namespace == NS
        raw = {k: [copy.deepcopy(o) for o in v.values()] for k, v in self.objects.items() if k not in self.unreadable}
        return raw, {k: "Forbidden" for k in self.unreadable}

    def list_pods(self, namespace):
        assert namespace == NS
        return copy.deepcopy(self.pods), ""

    def wait_for_rollouts(self, namespace, timeout_s):
        assert namespace == NS and timeout_s > 0
        self.settle_waits.append(timeout_s)

        def read():
            return rollouts.Workloads(
                deployments=tuple(copy.deepcopy(self.deployments)), pods=tuple(self.list_pods(NS)[0])
            )

        return rollouts.wait_for_rollouts(read, timeout_s, clock=self.clock, sleep=self.clock.sleep)

    def run_pod(self, manifest, timeout_s):
        assert manifest["metadata"]["namespace"] == NS and manifest["metadata"]["name"]
        assert timeout_s > 0
        self.run_manifests.append(manifest)
        return self.probe_phase, self.probe_log


def _pod(name, phase="Running", ready=True, terminating=False):
    pod = {
        "metadata": {"name": name},
        "status": {"phase": phase, "containerStatuses": [{"name": name, "ready": ready, "state": {"running": {}}}]},
    }
    if terminating:
        pod["metadata"]["deletionTimestamp"] = "2026-10-22T19:25:30Z"
    return pod


def _deployment(name, ready=1):
    return {
        "metadata": {"name": name, "generation": 2},
        "spec": {"replicas": 1},
        "status": {"observedGeneration": 2, "replicas": 1, "updatedReplicas": 1, "readyReplicas": ready},
    }


def _service(name, target_port=8080):
    return {"metadata": {"name": name}, "spec": {"ports": [{"name": "http", "port": 80, "targetPort": target_port}]}}


class FakeOracle(Oracle):
    def __init__(self, problem, verdict=True, error=None):
        super().__init__(problem)
        self.verdict = verdict
        self.error = error

    def evaluate(self):
        if self.error:
            raise self.error
        return {"success": self.verdict(self.problem) if callable(self.verdict) else self.verdict}


class FakeFault(Problem):
    """A sub-problem whose inject/recover are callables over the fake cluster."""

    def __init__(self, log, label, inject=None, recover=None, verdict=True, oracle_error=None):
        super().__init__(app=FakeApp(), namespace=NS)
        self.label = label
        self.log = log
        self._inject = inject or (lambda: None)
        self._recover = recover or (lambda: None)
        self.root_cause = f"root cause {label}"
        self.mitigation_oracle = FakeOracle(self, verdict, oracle_error)

    def inject_fault(self):
        self.log.append(("inject", self.label))
        self._inject()

    def recover_fault(self):
        self.log.append(("recover", self.label))
        self._recover()


def _composite(problems, cluster, **kwargs):
    c = ColocatedComposite(problems, cluster=cluster, **kwargs)
    c.liveness_settle_s = 0
    c.inject_pause_s = 0
    return c


def _target_port_fault(log, cluster, label, service, bad=9999, **kw):
    return FakeFault(log, label, inject=lambda: cluster.put("Service", _service(service, bad)), **kw)


# --- construction --------------------------------------------------------------


def test_safety_is_report_only_and_sub_problems_share_app_and_kubectl():
    cluster = FakeCluster()
    subs = [FakeFault([], "a"), FakeFault([], "b")]

    c = _composite(subs, cluster)

    assert c.safety_report_only is True
    assert all(p.app is c.app and p.kubectl is cluster.kubectl for p in subs)


def test_rejects_mismatched_namespaces_and_lengths():
    other = FakeFault([], "b")
    other.namespace = "elsewhere"
    with pytest.raises(ValueError, match="namespace"):
        _composite([FakeFault([], "a"), other], FakeCluster())
    with pytest.raises(ValueError, match="tiers"):
        _composite([FakeFault([], "a")], FakeCluster(), tiers=[Tier.ORDINARY, Tier.ORDINARY])


@pytest.mark.parametrize(
    "footprints",
    [
        [("Service", "user", "spec.ports.targetPort"), ("Deployment", "geo", "spec.template.spec.containers.image")],
        [("Service", "user", "spec.ports.targetPort"), ("Service", "rate", "<object>")],
        [(Footprint("Service", "user", "spec.ports"),), (Footprint("Service", "user", "spec.selector"),)],
        [("Service", "user", "spec.ports.targetPort"), (Footprint("ConfigMap", "c"), Footprint("Service", "rate"))],
    ],
)
def test_footprint_gate_accepts_disjoint_legacy_and_typed_footprints(footprints):
    _composite([FakeFault([], "a"), FakeFault([], "b")], FakeCluster(), footprints=footprints)


@pytest.mark.parametrize(
    "footprints",
    [
        [("Service", "rate", "<object>"), ("Service", "rate", "spec.ports.targetPort")],
        [(Footprint("Service", "user", "spec.ports"),), (Footprint("Service", "user", "spec.ports.targetPort"),)],
        [("Deployment", "geo", "spec.replicas"), (Footprint("ConfigMap", "c"), Footprint("Deployment", "geo"))],
    ],
)
def test_footprint_gate_rejects_overlaps(footprints):
    with pytest.raises(ValueError, match="Footprint collision"):
        _composite([FakeFault([], "a"), FakeFault([], "b")], FakeCluster(), footprints=footprints)


def test_footprint_gate_rejects_malformed_declarations():
    with pytest.raises(ValueError, match="unrecognized footprint"):
        _composite([FakeFault([], "a")], FakeCluster(), footprints=[("Service", "user")])


# --- injection order, liveness, validity ----------------------------------------


def test_injection_is_stable_sorted_by_tier_and_recovery_reverses_it():
    log = []
    labels = ["blocker", "ordinary1", "restarter", "ordinary2"]
    tiers = [Tier.BLOCKS_ADMISSION, Tier.ORDINARY, Tier.RESTARTS_PODS, Tier.ORDINARY]
    c = _composite([FakeFault(log, x, verdict=False) for x in labels], FakeCluster(), tiers=tiers)

    c.inject_fault()
    c.recover_fault()

    injected = [label for op, label in log if op == "inject"]
    recovered = [label for op, label in log if op == "recover"]
    assert injected == ["restarter", "ordinary1", "ordinary2", "blocker"]
    assert recovered == list(reversed(injected))
    assert c.report_fields()["composite_injection_order"] == "[2, 1, 3, 0]"


def test_spec_faults_that_still_hold_are_live_and_the_run_is_valid():
    cluster, log = FakeCluster(), []
    cluster.put("Service", _service("user"))
    cluster.put("Service", _service("rate"))
    subs = [
        _target_port_fault(log, cluster, "a", "user"),
        FakeFault(log, "b", inject=lambda: cluster.delete("Service", "rate")),
    ]
    c = _composite(subs, cluster)

    c.inject_fault()

    fields = c.report_fields()
    assert fields["fault_0_live"] and fields["fault_0_live_basis"] == "spec_diff"
    assert fields["fault_1_live"] and fields["fault_1_live_basis"] == "spec_diff"
    assert fields["composite_valid"] is True


def test_fault_erased_by_a_later_injection_makes_the_run_invalid():
    cluster, log = FakeCluster(), []
    cluster.put("Service", _service("user", 8080))
    subs = [
        _target_port_fault(log, cluster, "a", "user", bad=9999),
        _target_port_fault(log, cluster, "b", "user", bad=8080),  # overwrites a's write
    ]
    c = _composite(subs, cluster)

    c.inject_fault()

    fields = c.report_fields()
    # The retry re-injects fault_0, which now erases fault_1: still invalid.
    assert fields["composite_injection_retried"] == "[0]"
    assert fields["fault_0_live"] is True
    assert fields["fault_1_live"] is False and fields["fault_1_live_basis"] == "spec_diff"
    assert fields["composite_valid"] is False
    assert c.injection_failure() == fields


@pytest.mark.parametrize("oracle_passes, live", [(False, True), (True, False)])
def test_runtime_only_fault_is_judged_by_its_own_oracle(oracle_passes, live):
    c = _composite([FakeFault([], "valkey-auth", verdict=oracle_passes)], FakeCluster())

    c.inject_fault()

    fields = c.report_fields()
    assert fields["fault_0_live"] is live
    assert fields["fault_0_live_basis"] == "oracle"
    assert fields["fault_0_spec_changes"] == 0


def test_runtime_only_fault_with_crashing_oracle_has_basis_none():
    c = _composite([FakeFault([], "x", oracle_error=RuntimeError("down"))], FakeCluster())

    c.inject_fault()

    assert c.report_fields()["fault_0_live_basis"] == "none"
    assert c.report_fields()["composite_valid"] is False


def test_injection_error_is_recorded_and_later_faults_still_inject():
    cluster, log = FakeCluster(), []
    cluster.put("Service", _service("user"))

    def boom():
        raise TimeoutError("namespace never became ready")

    subs = [FakeFault(log, "a", inject=boom), _target_port_fault(log, cluster, "b", "user")]
    c = _composite(subs, cluster)

    c.inject_fault()  # does not raise

    fields = c.report_fields()
    assert ("inject", "b") in log
    assert [label for op, label in log if op == "inject"] == ["a", "b", "a"]  # a retried once
    assert "TimeoutError" in fields["fault_0_inject_error"]
    assert "fault_1_inject_error" not in fields
    assert fields["fault_1_live"] is True
    assert fields["composite_valid"] is False


def test_a_transient_injection_error_is_retried_once_and_the_run_becomes_valid():
    """Regression (pilot slot 2): a webhook rollout timed out once (image pull rate limit)."""
    cluster, log = FakeCluster(), []
    cluster.put("Service", _service("user"))
    cluster.put("Service", _service("rate"))
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError("rollout status timed out")
        cluster.delete("Service", "rate")

    subs = [_target_port_fault(log, cluster, "a", "user"), FakeFault(log, "b", inject=flaky)]
    c = _composite(subs, cluster)

    c.inject_fault()

    fields = c.report_fields()
    assert [label for op, label in log if op == "inject"] == ["a", "b", "b"]  # only b retried
    assert fields["composite_injection_retried"] == "[1]"
    assert "fault_1_inject_error" not in fields
    assert fields["fault_0_live"] and fields["fault_1_live"] and fields["fault_1_live_basis"] == "spec_diff"
    assert fields["composite_valid"] is True and c.injection_failure() is None


def test_a_valid_run_is_not_retried():
    cluster, log = FakeCluster(), []
    cluster.put("Service", _service("user"))
    c = _composite([_target_port_fault(log, cluster, "a", "user")], cluster)

    c.inject_fault()

    assert log == [("inject", "a")]
    assert "composite_injection_retried" not in c.report_fields()
    assert c.injection_failure() is None


def test_unreadable_kind_is_reported_and_its_faults_fall_back_to_the_oracle():
    cluster, log = FakeCluster(), []
    cluster.unreadable.add("Secret")
    c = _composite([FakeFault(log, "a", verdict=False)], cluster)

    c.inject_fault()

    fields = c.report_fields()
    assert "Secret" in fields["composite_snapshot_errors"]
    assert fields["fault_0_live_basis"] == "oracle"


def test_valid_is_false_before_injection():
    c = _composite([FakeFault([], "a")], FakeCluster())
    assert c.report_fields()["composite_valid"] is False
    assert c.injection_failure() is not None


# --- recovery ------------------------------------------------------------------


def test_recovery_never_raises_and_bounds_each_fault():
    log, release = [], threading.Event()

    def raise_runtime():
        raise RuntimeError("rollout status failed")

    def raise_exit():
        raise SystemExit(1)

    def hang():
        release.wait(timeout=30)  # bound only guards a broken test run

    subs = [
        FakeFault(log, "ok1"),
        FakeFault(log, "runtime", recover=raise_runtime),
        FakeFault(log, "exit", recover=raise_exit),
        FakeFault(log, "hang", recover=hang),
        FakeFault(log, "ok2"),
    ]
    c = _composite(subs, FakeCluster())
    c.recover_timeout_s = 0.2
    try:
        c.recover_fault()
    finally:
        release.set()

    assert [label for op, label in log if op == "recover"] == ["ok2", "hang", "exit", "runtime", "ok1"]
    assert c.recovery["fault_0_recover_error"] == ""
    assert "RuntimeError" in c.recovery["fault_1_recover_error"]
    assert "SystemExit" in c.recovery["fault_2_recover_error"]
    assert "timeout" in c.recovery["fault_3_recover_error"]


def _fp_draw(*footprints, tags=()):
    return FaultDraw(key="k", app="hotel_reservation", params=(), footprints=footprints, exclusive_tags=frozenset(tags))


def test_recovery_skips_namespace_only_faults_and_records_why():
    log = []
    draws = [
        _fp_draw(Footprint("Service", "user", "spec.ports.targetPort")),
        _fp_draw(Footprint("MutatingWebhookConfiguration", "w")),
        _fp_draw(Footprint("Deployment", "geo"), tags={"namespace:policy-system"}),
        _fp_draw(Footprint("Namespace", NS, "metadata.labels")),
    ]
    c = _composite([FakeFault(log, str(i)) for i in range(4)], FakeCluster(), draws=draws)
    c.namespace = f"{NS}-session1"  # the conductor renames after construction

    c.recover_fault()

    assert [label for op, label in log if op == "recover"] == ["2", "1"]
    assert [c.recovery[f"fault_{i}_recovery"] for i in range(4)] == ["skipped", "recovered", "recovered", "skipped"]
    assert "MutatingWebhookConfiguration/w" in c.recovery["fault_1_recovery_reason"]
    assert "namespace:policy-system" in c.recovery["fault_2_recovery_reason"]
    assert c.recovery["fault_1_recover_error"] == "" and "fault_0_recover_error" not in c.recovery


def test_recovery_runs_for_legacy_and_host_level_faults():
    log = []
    khaos = FakeFault(log, "khaos")
    khaos.requires_khaos = lambda: True
    c = _composite([FakeFault(log, "legacy"), khaos], FakeCluster())
    c.recover_fault()
    assert sorted(label for op, label in log if op == "recover") == ["khaos", "legacy"]

    log.clear()
    khaos2 = FakeFault(log, "khaos")
    khaos2.requires_khaos = lambda: True
    draws = [_fp_draw(Footprint("Service", "a")), _fp_draw(Footprint("Service", "b"))]
    c = _composite([FakeFault(log, "plain"), khaos2], FakeCluster(), draws=draws)
    c.recover_fault()
    assert [label for op, label in log if op == "recover"] == ["khaos"]
    assert c.recovery["fault_1_recovery_reason"] == "host-level (khaos) fault"


# --- mitigation grading --------------------------------------------------------


def _graded(verdicts, cluster=None, draws=None):
    cluster = cluster or FakeCluster()
    subs = [FakeFault([], f"f{i}", verdict=v) for i, v in enumerate(verdicts)]
    c = _composite(subs, cluster, draws=draws)
    return c, c.mitigation_oracle.evaluate()


def test_mitigation_succeeds_only_when_all_faults_collateral_and_traffic_pass():
    _, r = _graded([True, True])

    assert r["success"] is True and r["accuracy"] == 100.0
    assert r["faults_fixed"] == 2 and r["collateral_ok"] and r["traffic_ok"]


def test_one_unfixed_fault_fails_the_composite():
    _, r = _graded([True, False])

    assert r["success"] is False
    assert r["fault_1_verdict"] is False and r["faults_fixed"] == 1


def test_crashing_sub_oracle_is_a_per_fault_failure():
    cluster = FakeCluster()
    subs = [FakeFault([], "a"), FakeFault([], "b", oracle_error=RuntimeError("api down"))]
    r = _composite(subs, cluster).mitigation_oracle.evaluate()

    assert r["fault_0_verdict"] is True and r["fault_1_verdict"] is False and r["success"] is False


def test_broken_collateral_fails_the_composite():
    cluster = FakeCluster()
    cluster.pods = [_pod("frontend"), _pod("geo", phase="Pending")]

    _, r = _graded([True], cluster)

    assert r["collateral_ok"] is False and "geo" in r["collateral_reason"] and r["success"] is False


def test_failing_traffic_fails_the_composite():
    cluster = FakeCluster()
    cluster.probe_log = "\n".join(["PROBE_OK /"] * 5 + ["PROBE_FAIL /hotels"] * 5 + ["PROBE_DONE"])

    _, r = _graded([True], cluster)

    assert r["traffic_ok"] is False and "/hotels" in r["traffic_detail"] and r["success"] is False


def test_probe_that_could_not_run_fails_traffic():
    cluster = FakeCluster()
    cluster.probe_phase = "Error: Forbidden: exceeded quota"

    _, r = _graded([True], cluster)

    assert r["traffic_ok"] is False and "quota" in r["traffic_detail"]


def test_probe_targets_the_app_entry_point_in_the_app_namespace():
    cluster = FakeCluster()

    _graded([True], cluster)

    (manifest,) = cluster.run_manifests
    script = manifest["spec"]["containers"][0]["command"][-1]
    assert f"frontend.{NS}.svc.cluster.local:5000/hotels" in script


class FakeCoreApi:
    """The CoreV1Api calls KubeClusterIO.run_pod makes, over an in-memory pod map.
    `fail` names the one call that raises; a failing create still stores the pod, as a
    create that timed out client-side but reached the API server would."""

    def __init__(self, fail=None, phase="Succeeded"):
        self.pods, self.fail, self.phase = {}, fail, phase

    def _maybe_fail(self, call):
        if self.fail == call:
            raise RuntimeError(f"{call} failed")

    def create_namespaced_pod(self, namespace, body):
        self.pods[(namespace, body["metadata"]["name"])] = body
        self._maybe_fail("create")

    def read_namespaced_pod(self, name, namespace):
        self._maybe_fail("read")
        assert (namespace, name) in self.pods
        status = type("Status", (), {"phase": self.phase})()
        return type("Pod", (), {"status": status})()

    def read_namespaced_pod_log(self, name, namespace):
        self._maybe_fail("log")
        return "PROBE_DONE"

    def delete_namespaced_pod(self, name, namespace, grace_period_seconds=None):
        del self.pods[(namespace, name)]


@pytest.mark.parametrize("fail", [None, "create", "read", "log"])
def test_probe_pod_is_deleted_on_every_exit_path(fail):
    core = FakeCoreApi(fail=fail)
    io = KubeClusterIO(kubectl=object(), apis={"core": core})
    manifest = grading.probe_pod_manifest("probe-x", NS, grading.ENTRY_POINTS["Hotel Reservation"])

    phase, logs = io.run_pod(manifest, timeout_s=5)

    assert core.pods == {}
    assert phase.startswith("Error") == (fail in ("create", "read"))
    assert (logs == "PROBE_DONE") == (fail is None)


def test_unknown_app_fails_traffic_closed():
    cluster = FakeCluster()
    sub = FakeFault([], "a")
    sub.app = FakeApp(name="Mystery App")

    r = _composite([sub], cluster).mitigation_oracle.evaluate()

    assert r["traffic_ok"] is False and r["success"] is False


def test_results_carry_identity_from_draws():
    draws = [
        FaultDraw(
            key="k8s.target_port",
            app="hotel_reservation",
            params=(("service", "user"),),
            footprints=(Footprint("Service", "user"),),
        ),
    ]
    c, r = _graded([True], draws=draws)

    assert r["composite_n"] == 1
    assert r["fault_0_key"] == "k8s.target_port"
    assert json.loads(r["fault_0_params"]) == {"service": "user"}


def test_results_without_draws_use_the_class_name():
    _, r = _graded([True])

    assert r["fault_0_key"] == "FakeFault" and r["fault_0_params"] == "{}"


def test_draws_supply_footprints_and_tiers_and_must_agree_with_explicit_ones():
    draw = FaultDraw(
        key="k", app="hotel_reservation", params=(), footprints=(Footprint("Service", "user"),), tier=Tier.RESTARTS_PODS
    )
    c = _composite([FakeFault([], "a")], FakeCluster(), draws=[draw])
    assert c.footprints == [draw.footprints] and c.tiers == [Tier.RESTARTS_PODS]

    with pytest.raises(ValueError, match="disagree"):
        _composite([FakeFault([], "a")], FakeCluster(), draws=[draw], tiers=[Tier.ORDINARY])
    with pytest.raises(ValueError, match="disagree"):
        _composite([FakeFault([], "a")], FakeCluster(), draws=[draw], footprints=[("Service", "rate", "<object>")])


def test_non_json_draw_params_fail_at_construction():
    draw = FaultDraw(
        key="k", app="hotel_reservation", params=(("p", object()),), footprints=(Footprint("Service", "u"),)
    )
    with pytest.raises(TypeError):
        _composite([FakeFault([], "a")], FakeCluster(), draws=[draw])


# --- generator -> build_composite -> results (end to end, pure) --------------------

_PLAN_SERVICES = ("user", "search", "rate", "geo", "profile")


def _plan_entry(key, kind, field, tier):
    def sample(app, rng):
        service = rng.choice(_PLAN_SERVICES)
        return FaultDraw(
            key=key,
            app=app,
            params=(("bad_port", rng.randrange(9000, 9999)), ("faulty_service", service)),
            footprints=(Footprint(kind, service, field),),
            tier=tier,
        )

    def build(draw):
        return FakeFault([], f"{draw.key}:{draw.param_dict()['faulty_service']}")

    return CatalogEntry(key=key, apps=frozenset({"hotel_reservation"}), sample=sample, build=build)


_PLAN_CATALOG = (
    _plan_entry("target_port", "Service", "spec.ports.targetPort", Tier.ORDINARY),
    _plan_entry("selector", "Service", "spec.selector", Tier.ORDINARY),
    _plan_entry("image", "Deployment", "spec.template.spec.containers.image", Tier.RESTARTS_PODS),
    _plan_entry("quota", "ResourceQuota", None, Tier.BLOCKS_ADMISSION),
)


@pytest.mark.parametrize("n", [1, 2, 3, 5])
@pytest.mark.parametrize("seed", range(8))
def test_plan_built_into_a_composite_reports_every_draw(n, seed):
    plan = draw_plan("hotel_reservation", n, seed, _PLAN_CATALOG)

    c = build_composite(plan, entries=_PLAN_CATALOG, cluster=FakeCluster())
    c.liveness_settle_s = c.inject_pause_s = 0
    r = c.mitigation_oracle.evaluate()

    assert r["composite_n"] == n
    for i, draw in enumerate(plan.draws):
        identity = draw.to_json()
        assert r[f"fault_{i}_key"] == identity["key"]
        assert json.loads(r[f"fault_{i}_params"]) == identity["params"] == draw.param_dict()
        assert r[f"fault_{i}_tier"] == identity["tier"]
        assert r[f"fault_{i}_class"] == "FakeFault"
    assert c.footprints == [d.footprints for d in plan.draws]


@pytest.mark.parametrize("seed", range(8))
def test_take_k_composite_reports_base_plan_and_base_indices(seed):
    base = draw_plan("hotel_reservation", 4, seed, _PLAN_CATALOG)
    sub = take_plan(base, 2)

    c = build_composite(sub, entries=_PLAN_CATALOG, cluster=FakeCluster())
    c.liveness_settle_s = c.inject_pause_s = 0
    r = c.mitigation_oracle.evaluate()

    assert (r["composite_n"], r["composite_base_n"], r["composite_take_k"]) == (2, 4, 2)
    for i, base_index in enumerate(sub.base_index):
        assert r[f"fault_{i}_base_index"] == base_index
        assert r[f"fault_{i}_key"] == base.draws[base_index].key


def test_whole_plan_composite_reports_take_k_equal_to_n():
    fields = build_composite(
        draw_plan("hotel_reservation", 3, 0, _PLAN_CATALOG), entries=_PLAN_CATALOG, cluster=FakeCluster()
    ).report_fields()
    assert (fields["composite_base_n"], fields["composite_take_k"]) == (3, 3)
    assert [fields[f"fault_{i}_base_index"] for i in range(3)] == [0, 1, 2]


def test_build_composite_names_draws_missing_from_the_catalog():
    plan = draw_plan("hotel_reservation", 2, 0, _PLAN_CATALOG)
    with pytest.raises(KeyError, match="not in the catalog"):
        build_composite(plan, entries=_PLAN_CATALOG[:0], cluster=FakeCluster())


def _composite_calls(path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ColocatedComposite":
            yield node


# sdo2's registry builds no ColocatedComposite itself; every one comes from build_composite.
@pytest.mark.parametrize("relpath", ["sregym/conductor/problems/composition/__init__.py"])
def test_every_composite_construction_passes_accepted_keywords(relpath):
    """Static: the registry imports every problem class, so it cannot be imported here."""
    accepted = set(inspect.signature(ColocatedComposite).parameters)
    calls = list(_composite_calls(Path(__file__).resolve().parents[2] / relpath))

    assert calls, f"no ColocatedComposite(...) call found in {relpath}"
    for call in calls:
        assert not call.args, f"{relpath}:{call.lineno} passes positional arguments"
        assert {k.arg for k in call.keywords} <= accepted, f"{relpath}:{call.lineno}"
        assert "problems" in {k.arg for k in call.keywords}


# --- workload handle rebinding (audit hazard: never-deployed app's wrk) -------------


class FakeCompound(Oracle):
    def __init__(self, problem, **oracles):
        super().__init__(problem)
        self.oracles = oracles

    def evaluate(self):
        return {"success": all(o.evaluate()["success"] for o in self.oracles.values())}


class WrkOracle(Oracle):
    def __init__(self, problem, wrk):
        super().__init__(problem)
        self.wrk = wrk

    def evaluate(self):
        return {"success": self.wrk == "canonical"}


def test_sub_oracle_workload_handles_are_rebound_to_the_deployed_app():
    a, b = FakeFault([], "a"), FakeFault([], "b")
    a.mitigation_oracle = WrkOracle(a, wrk="a-own-instance")
    b.mitigation_oracle = FakeCompound(b, w=WrkOracle(b, wrk="b-own-instance"))
    c = _composite([a, b], FakeCluster())
    c.app.wrk = "canonical"  # the conductor deploys and starts the canonical app's workload

    r = c.mitigation_oracle.evaluate()

    assert r["fault_0_verdict"] is True and r["fault_1_verdict"] is True


# --- concurrent mitigation grading -------------------------------------------------


class Tracker:
    """Counts sub-oracles inside evaluate() at once; records each one's view on entry."""

    def __init__(self):
        self.lock = threading.Lock()
        self.active = 0
        self.peak = 0
        self.entries = []  # (label, active including itself, finished labels so far)
        self.finished = []

    def enter(self, label):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.entries.append((label, self.active, list(self.finished)))

    def leave(self, label):
        with self.lock:
            self.active -= 1
            self.finished.append(label)


class ScriptedOracle(Oracle):
    """Runs ``body()`` inside the tracker; ``safe`` and ``budget`` set the scheduling attributes."""

    def __init__(self, problem, tracker, body=lambda: {"success": True}, safe=True, budget=5.0):
        super().__init__(problem)
        self.tracker, self.body = tracker, body
        self.concurrent_safe = safe
        self.evaluation_budget_s = budget

    def evaluate(self):
        self.tracker.enter(self.problem.label)
        try:
            return self.body()
        finally:
            self.tracker.leave(self.problem.label)


def _scripted(specs, tracker, cluster=None):
    """``specs``: one (body, safe, budget) per fault, labelled f0, f1, ..."""
    subs = []
    for i, (body, safe, budget) in enumerate(specs):
        sub = FakeFault([], f"f{i}")
        sub.mitigation_oracle = ScriptedOracle(sub, tracker, body, safe, budget)
        subs.append(sub)
    return _composite(subs, cluster or FakeCluster())


def _meet(barrier):
    def body():
        barrier.wait(timeout=10)  # bound only guards a broken test run
        return {"success": True}

    return body


def test_sub_oracles_run_concurrently():
    # A serial loop would break the barrier, so passing proves wall time ~ max, not sum.
    tracker = Tracker()
    c = _scripted([(_meet(threading.Barrier(8)), True, 5.0)] * 8, tracker)

    r = c.mitigation_oracle.evaluate()

    assert r["faults_fixed"] == 8 and r["success"] is True
    assert tracker.peak == 8


def test_concurrency_is_bounded_by_max_workers():
    tracker = Tracker()
    c = _scripted([(_meet(threading.Barrier(3)), True, 5.0)] * 6, tracker)
    c.mitigation_oracle.max_workers = 3

    r = c.mitigation_oracle.evaluate()

    assert r["faults_fixed"] == 6 and tracker.peak == 3


def test_unsafe_sub_oracles_run_alone_after_the_safe_ones_and_results_keep_declared_order():
    tracker = Tracker()
    meet = _meet(threading.Barrier(3))
    specs = [
        (lambda: {"success": True}, False, 5.0),
        (meet, True, 5.0),
        (lambda: {"success": False}, False, 5.0),
        (meet, True, 5.0),
        (meet, True, 5.0),
    ]
    c = _scripted(specs, tracker)

    r = c.mitigation_oracle.evaluate()

    unsafe = [(label, active, done) for label, active, done in tracker.entries if label in ("f0", "f2")]
    assert [label for label, _, _ in unsafe] == ["f0", "f2"]
    assert all(active == 1 for _, active, _ in unsafe)
    assert all({"f1", "f3", "f4"} <= set(done) for _, _, done in unsafe)
    assert [o["name"] for o in r["oracles"]] == [f"fault_{i}:FakeFault" for i in range(5)]
    assert [r[f"fault_{i}_verdict"] for i in range(5)] == [True, True, False, True, True]


@pytest.mark.parametrize("error", [RuntimeError("api down"), SystemExit(3), KeyboardInterrupt()])
def test_a_crashing_sub_oracle_fails_only_its_fault(error):
    def crash():
        raise error

    tracker = Tracker()
    meet = _meet(threading.Barrier(2))
    c = _scripted([(meet, True, 5.0), (crash, True, 5.0), (meet, True, 5.0)], tracker)

    r = c.mitigation_oracle.evaluate()

    assert [r[f"fault_{i}_verdict"] for i in range(3)] == [True, False, True]
    assert type(error).__name__ in r["oracles"][1]["error"] and r["fault_1_timed_out"] is False


def test_sub_oracle_past_the_deadline_is_a_per_fault_timeout():
    release = threading.Event()

    def hang():
        release.wait(timeout=30)  # released below; the bound only guards a broken test run
        return {"success": True}

    tracker = Tracker()
    quick = (lambda: {"success": True}, True, 0.5)
    c = _scripted([quick, (hang, True, 0.5), quick, (lambda: {"success": True}, False, 0.5)], tracker)
    try:
        r = c.mitigation_oracle.evaluate()
    finally:
        release.set()

    assert [r[f"fault_{i}_timed_out"] for i in range(4)] == [False, True, False, True]
    assert [r[f"fault_{i}_verdict"] for i in range(4)] == [True, False, True, False]
    assert "did not finish within the 1s grading deadline" in r["oracles"][1]["error"]
    assert "was not started" in r["oracles"][3]["error"]
    assert r["success"] is False and r["traffic_ok"] is True  # collateral and traffic still ran


@pytest.mark.parametrize("settled", [True, False])
def test_namespace_settles_once_and_every_sub_oracle_skips_its_own_wait(settled):
    a, b = FakeFault([], "a"), FakeFault([], "b")
    sub_reads = []

    class Settling(Oracle):
        def evaluate(self):
            self.settle_rollouts(read=lambda: sub_reads.append(1) or rollouts.Workloads())
            return {"success": True}

    a.mitigation_oracle = Settling(a)
    b.mitigation_oracle = FakeCompound(b, inner=Settling(b))
    cluster = FakeCluster()
    cluster.deployments = [_deployment("frontend", ready=1 if settled else 0)]
    c = _composite([a, b], cluster)

    r = c.mitigation_oracle.evaluate()

    assert sub_reads == []  # no sub-oracle waited for what the composite already waited for
    assert cluster.settle_waits == [rollouts.ROLLOUT_SETTLE_S]
    assert r["fault_0_verdict"] is True and r["fault_1_verdict"] is True
    assert r["composite_settled"] is settled
    assert ("composite_settle_reason" in r) is (not settled)


def _pods_healthy(cluster):
    """A pod-walking verdict, as MitigationOracle and DeploymentReadinessOracle judge."""
    return lambda problem: grading.collateral_verdict(cluster.pods)[0]


def test_an_old_pod_still_terminating_at_submission_is_waited_out_and_the_fix_passes():
    # The kscale artifact: the fix rolled out, but the old ReplicaSet's pod (stuck in its
    # init container, so phase Pending) was still Terminating when grading began.
    cluster = FakeCluster()
    old = _pod("recommendation-68f4ff69c6-gdwhh", phase="Pending", ready=False, terminating=True)
    cluster.deployments = [_deployment("recommendation")]
    cluster.pods = [_pod("frontend"), _pod("recommendation-58c5ff98bf-mcjr8"), old]
    cluster.clock.at(20, lambda: cluster.pods.remove(old))  # its grace period ends
    c = _composite([FakeFault([], "init-hang", verdict=_pods_healthy(cluster))], cluster)

    r = c.mitigation_oracle.evaluate()

    assert r["composite_settled"] is True and r["composite_settle_s"] == 20
    assert r["fault_0_verdict"] is True and r["collateral_ok"] is True and r["success"] is True


def test_a_pod_stuck_terminating_on_an_unfixed_fault_still_fails_after_the_bounded_wait():
    # A finalizer the agent never cleared: the pod stays Terminating however long we wait.
    cluster = FakeCluster()
    stuck = _pod("payment-7d9c-x2v", ready=False, terminating=True)
    cluster.pods = [_pod("frontend"), stuck]

    def finalizer_cleared(problem):
        return not any(p["metadata"].get("deletionTimestamp") for p in cluster.pods)

    c = _composite([FakeFault([], "finalizer", verdict=finalizer_cleared)], cluster)

    r = c.mitigation_oracle.evaluate()

    assert r["composite_settled"] is False
    assert r["composite_settle_s"] == rollouts.ROLLOUT_SETTLE_S
    assert "pod/payment-7d9c-x2v: terminating" in r["composite_settle_reason"]
    assert r["fault_0_verdict"] is False and r["collateral_ok"] is False and r["success"] is False


def test_an_already_settled_namespace_is_graded_without_waiting():
    cluster = FakeCluster()
    cluster.deployments = [_deployment("frontend")]

    r = _composite([FakeFault([], "a")], cluster).mitigation_oracle.evaluate()

    assert r["composite_settled"] is True and r["composite_settle_s"] == 0
    assert cluster.clock.now == 0 and r["success"] is True


class ResultOracle(Oracle):
    def __init__(self, problem, result):
        super().__init__(problem)
        self.result = result

    def evaluate(self):
        return dict(self.result)


def _with_unjudged_fault(cluster, other_verdict=True):
    a, b, c = FakeFault([], "a"), FakeFault([], "hpa"), FakeFault([], "b", verdict=other_verdict)
    b.mitigation_oracle = ResultOracle(b, {"success": False, "infra_error": "metrics_server_unavailable"})
    return _composite([a, b, c], cluster).mitigation_oracle.evaluate()


def test_an_unjudged_fault_that_alone_decides_the_outcome_makes_the_row_an_infra_error():
    r = _with_unjudged_fault(FakeCluster())

    assert r["infra_error"] == "metrics_server_unavailable"
    assert r["fault_1_infra_error"] == "metrics_server_unavailable"
    assert [r[f"fault_{i}_verdict"] for i in range(3)] == [True, False, True]


def _collateral_broken():
    cluster = FakeCluster()
    cluster.pods = [_pod("frontend"), _pod("geo", phase="Failed")]
    return cluster


def _traffic_broken():
    cluster = FakeCluster()
    cluster.probe_log = "\n".join(["PROBE_FAIL /"] * 10 + ["PROBE_DONE"])
    return cluster


@pytest.mark.parametrize(
    "cluster, other_verdict, why",
    [
        (FakeCluster, False, "another fault is not fixed"),
        (_collateral_broken, True, "collateral failed"),
        (_traffic_broken, True, "traffic failed"),
    ],
)
def test_an_unjudged_fault_in_a_row_that_failed_anyway_stays_a_per_fault_note(cluster, other_verdict, why):
    r = _with_unjudged_fault(cluster(), other_verdict)

    assert r["success"] is False
    assert "infra_error" not in r, why
    assert r["fault_1_infra_error"] == "metrics_server_unavailable"


def test_a_composite_whose_sub_oracles_all_judged_carries_no_infra_error():
    r = _composite([FakeFault([], "a"), FakeFault([], "b", verdict=False)], FakeCluster()).mitigation_oracle.evaluate()

    assert "infra_error" not in r and not any(k.endswith("_infra_error") for k in r)


class FakeRolloutApis:
    """The list calls ``rollouts.namespace_reader`` makes, over in-memory JSON objects.
    ``fail`` names a list method that raises, as an RBAC denial would."""

    def __init__(self, deployments=(), stateful_sets=(), pods=(), fail=None):
        self.objects = {
            "list_namespaced_deployment": list(deployments),
            "list_namespaced_stateful_set": list(stateful_sets),
            "list_namespaced_pod": list(pods),
        }
        self.fail = fail

    def __getattr__(self, method):
        if method not in self.objects:
            raise AttributeError(method)

        def call(namespace, _preload_content=True):
            assert namespace == NS and _preload_content is False
            if method == self.fail:
                raise RuntimeError("Forbidden")
            return type("Response", (), {"data": json.dumps({"items": self.objects[method]})})()

        return call


@pytest.mark.parametrize(
    "apis, settled, reason",
    [
        (FakeRolloutApis([_deployment("frontend")], pods=[_pod("frontend")]), True, ""),
        (FakeRolloutApis(fail="list_namespaced_pod"), False, "reading the namespace failed: RuntimeError: Forbidden"),
    ],
)
def test_kube_cluster_io_settle_reads_deployments_stateful_sets_and_pods(apis, settled, reason):
    io = KubeClusterIO(kubectl=object(), apis={"core": apis, "apps": apis})

    settle = io.wait_for_rollouts(NS, timeout_s=90)

    assert (settle.settled, settle.reason) == (settled, reason)


def _with_oracles(specs, max_workers=None):
    c = _scripted([(lambda: {"success": True}, safe, budget) for safe, budget in specs], Tracker())
    if max_workers is not None:
        c.mitigation_oracle.max_workers = max_workers
    return c


def test_evaluation_timeout_scales_with_serial_oracles_and_waves_not_with_concurrent_n():
    one = _with_oracles([(True, 240.0)])
    eight = _with_oracles([(True, 240.0)] * 8)
    eight_with_two_serial = _with_oracles([(True, 240.0)] * 6 + [(False, 240.0), (False, 100.0)])
    nine = _with_oracles([(True, 240.0)] * 9, max_workers=8)

    assert one.evaluation_timeout_s == eight.evaluation_timeout_s
    assert eight_with_two_serial.evaluation_timeout_s == eight.evaluation_timeout_s + 340.0
    assert nine.evaluation_timeout_s == eight.evaluation_timeout_s + 240.0


def test_evaluation_timeout_covers_settle_deadline_and_probe_and_never_drops_below_the_default():
    c = _with_oracles([(True, 240.0)] * 8)
    oracle = c.mitigation_oracle
    probe_s = grading.probe_deadline_s(grading.ENTRY_POINTS["Hotel Reservation"])

    assert oracle.deadline_s() == 240.0
    assert c.evaluation_timeout_s >= oracle.settle_timeout_s + oracle.deadline_s() + probe_s
    small = _with_oracles([(True, 1.0)])
    small.app = FakeApp(name="Mystery App")  # no traffic probe: the derived bound is under the default
    assert small.mitigation_oracle.time_bound_s() < 300.0
    assert small.evaluation_timeout_s == Problem.evaluation_timeout_s == 300.0
    assert c.evaluation_timeout_s >= c.diagnosis_oracle.deadline_s


# --- diagnosis -------------------------------------------------------------------


class BarrierJudge:
    """Succeeds only if all judges run at the same time: a sequential loop breaks the barrier."""

    def __init__(self, barrier, problem):
        self.barrier = barrier
        self.problem = problem

    def evaluate(self, solution):
        self.barrier.wait(timeout=10)  # bound only guards a broken test run
        return {"success": self.problem.label in solution}


def test_diagnosis_judges_run_concurrently_and_count_per_fault():
    subs = [FakeFault([], f"f{i}") for i in range(8)]
    c = _composite(subs, FakeCluster())
    barrier = threading.Barrier(8)
    oracle = CompositeDiagnosisOracle(c, judge_factory=lambda p, others: BarrierJudge(barrier, p))

    r = oracle.evaluate("found f0 f1 f2 f3 f4 f5 f6")

    assert r["faults_diagnosed"] == 7 and r["fault_7_diagnosed"] is False and r["success"] is False
    assert r["composite_n"] == 8


def test_diagnosis_judge_crash_is_a_per_fault_failure():
    class Crashing:
        def evaluate(self, solution):
            raise RuntimeError("judge offline")

    c = _composite([FakeFault([], "a")], FakeCluster())

    r = CompositeDiagnosisOracle(c, judge_factory=lambda p, others: Crashing()).evaluate("a")

    assert r["fault_0_diagnosed"] is False and "judge offline" in r["oracles"][0]["error"]


def test_diagnosis_judge_reads_root_cause_at_evaluation_time():
    seen = []

    class Recording:
        def __init__(self, p, others):
            seen.append(p.root_cause)

        def evaluate(self, solution):
            return {"success": True}

    sub = FakeFault([], "a")
    c = _composite([sub], FakeCluster())
    oracle = CompositeDiagnosisOracle(c, judge_factory=Recording)
    sub.root_cause = "namespace=hotel-reservation-session1"  # the conductor's session rename

    oracle.evaluate("x")

    assert seen == ["namespace=hotel-reservation-session1"]


def test_each_diagnosis_judge_is_told_the_other_faults_root_causes_and_not_its_own():
    given = {}

    class Recording:
        def __init__(self, p, others):
            given[p.label] = others

        def evaluate(self, solution):
            return {"success": True}

    subs = [FakeFault([], label) for label in ("a", "b", "c")]
    subs[1].root_cause = ""  # a sub-problem without a root cause adds no context line
    c = _composite(subs, FakeCluster())

    CompositeDiagnosisOracle(c, judge_factory=Recording).evaluate("x")

    assert given == {
        "a": ("root cause c",),
        "b": ("root cause a", "root cause c"),
        "c": ("root cause a",),
    }


def test_a_single_fault_composite_judge_gets_no_other_faults():
    given = []

    class Recording:
        def __init__(self, p, others):
            given.append(others)

        def evaluate(self, solution):
            return {"success": True}

    CompositeDiagnosisOracle(_composite([FakeFault([], "a")], FakeCluster()), judge_factory=Recording).evaluate("x")

    assert given == [()]


def test_probe_manifest_is_admissible_under_restricted_psa_and_quotas():
    manifest = grading.probe_pod_manifest("p", NS, grading.ENTRY_POINTS["Hotel Reservation"])
    spec = manifest["spec"]
    container = spec["containers"][0]

    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    assert container["securityContext"] == {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
    assert set(container["resources"]) == {"requests", "limits"}
    assert spec["restartPolicy"] == "Never" and spec["activeDeadlineSeconds"] > 0
