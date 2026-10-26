"""Offline contract tests for composite fault problems (no cluster, no LLM)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import sregym.conductor.problems.composite as composite_module
import sregym.conductor.problems.registry as registry_module
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composite import (
    COMPOSITE_SPECS_PATH,
    BenignEnvDrift,
    ComponentSpec,
    CompositeFaultProblem,
    CompositeRecoveryError,
    CompositeSpec,
    load_composite_specs,
)
from sregym.utils.decorators import mark_fault_injected

EMPTY_INVENTORY = SimpleNamespace(list_objects=lambda namespaces: frozenset(), delete=lambda obj: None)


class _App:
    def __init__(self, namespace: str = "hotel-reservation") -> None:
        self.namespace = namespace
        self.name = "Hotel Reservation"


class _Oracle(Oracle):
    def __init__(self, problem, success: bool = True) -> None:
        super().__init__(problem)
        self.success = success

    def evaluate(self) -> dict:
        return {"success": self.success}


class _Judge:
    def __init__(self, problem, expected: str) -> None:
        self.problem = problem
        self.expected = expected


class _Fault(Problem):
    def __init__(self, name: str, events: list[str], *, app: _App | None = None, fail_recover: bool = False) -> None:
        super().__init__(app=app or _App())
        self.name = name
        self.events = events
        self.fail_recover = fail_recover
        self.root_cause = f"[fault_spec] component={name}; namespace=hotel-reservation || {name} is broken."
        self.mitigation_oracle = _Oracle(self)

    @mark_fault_injected
    def inject_fault(self) -> None:
        self.events.append(f"inject {self.name}")

    @mark_fault_injected
    def recover_fault(self) -> None:
        self.events.append(f"recover {self.name}")
        if self.fail_recover:
            raise RuntimeError(f"{self.name} recovery failed")


class _Decoy(Problem):
    def __init__(self, app, events: list[str], **params) -> None:
        super().__init__(app=app)
        self.events = events
        self.params = params
        self.root_cause = "LOG_LEVEL=debug on geo is benign."

    def inject_fault(self) -> None:
        self.events.append("inject decoy")

    def recover_fault(self) -> None:
        self.events.append("recover decoy")


def _spec(*components: ComponentSpec, problem_id: str = "composite_test_hotel_reservation") -> CompositeSpec:
    return CompositeSpec(problem_id=problem_id, components=components)


def _build(spec: CompositeSpec, faults: dict[str, _Fault], events: list[str]) -> CompositeFaultProblem:
    return CompositeFaultProblem(
        spec,
        component_factory=lambda problem_id: faults[problem_id],
        decoy_factories={"benign_env_drift": lambda app, **params: _Decoy(app, events, **params)},
    )


@pytest.fixture(autouse=True)
def _offline_judge(monkeypatch):
    monkeypatch.setattr(composite_module, "LLMAsAJudgeOracle", _Judge)


def _two_faults(events: list[str], **kwargs) -> tuple[CompositeFaultProblem, dict[str, _Fault]]:
    faults = {"a": _Fault("a", events), "b": _Fault("b", events, **kwargs)}
    spec = _spec(ComponentSpec(role="fault", problem="a"), ComponentSpec(role="fault", problem="b"))
    return _build(spec, faults, events), faults


def test_components_inject_in_catalog_order_and_recover_in_reverse() -> None:
    events: list[str] = []
    problem, _ = _two_faults(events)

    problem.inject_fault()
    problem.recover_fault()

    assert events == ["inject a", "inject b", "recover b", "recover a"]
    assert problem.fault_injected is False


def test_every_recovery_is_attempted_and_the_errors_are_raised_together() -> None:
    events: list[str] = []
    faults = {
        "a": _Fault("a", events, fail_recover=True),
        "b": _Fault("b", events, fail_recover=True),
        "c": _Fault("c", events),
    }
    spec = _spec(*(ComponentSpec(role="fault", problem=name) for name in ("a", "b", "c")))
    problem = _build(spec, faults, events)
    problem.inject_fault()

    with pytest.raises(CompositeRecoveryError) as raised:
        problem.recover_fault()

    assert events[-3:] == ["recover c", "recover b", "recover a"]
    assert [str(error) for error in raised.value.exceptions] == ["b recovery failed", "a recovery failed"]
    assert "a" in str(raised.value) and "b" in str(raised.value)


def test_mitigation_passes_only_when_every_fault_oracle_passes() -> None:
    events: list[str] = []
    problem, faults = _two_faults(events)
    problem.inject_fault()

    faults["b"].mitigation_oracle.success = False
    failing = problem.mitigation_oracle.evaluate()
    faults["b"].mitigation_oracle.success = True
    passing = problem.mitigation_oracle.evaluate()

    assert failing["success"] is False
    assert [part["success"] for part in failing["oracles"]] == [True, False]
    assert passing["success"] is True


def test_decoys_are_injected_in_order_recovered_but_never_graded() -> None:
    events: list[str] = []
    faults = {"a": _Fault("a", events)}
    spec = _spec(
        ComponentSpec(role="fault", problem="a"),
        ComponentSpec(role="decoy", decoy="benign_env_drift", params={"deployment": "geo"}),
    )
    problem = _build(spec, faults, events)

    problem.inject_fault()
    graded = problem.mitigation_oracle.evaluate()
    problem.recover_fault()

    assert events == ["inject a", "inject decoy", "recover decoy", "recover a"]
    assert [part["name"] for part in graded["oracles"]] == ["0-a"]
    assert problem.fault_indices == (0,)
    assert problem.components[1].params == {"deployment": "geo"}


def test_evaluation_timeout_sums_the_graded_faults_and_skips_decoys() -> None:
    events: list[str] = []
    faults = {"a": _Fault("a", events), "b": _Fault("b", events)}
    faults["b"].evaluation_timeout_s = 600.0
    spec = _spec(
        ComponentSpec(role="fault", problem="a"),
        ComponentSpec(role="decoy", decoy="benign_env_drift"),
        ComponentSpec(role="fault", problem="b"),
    )

    problem = _build(spec, faults, events)

    assert problem.evaluation_timeout_s == Problem.evaluation_timeout_s + 600.0


def test_diagnosis_ground_truth_lists_every_cause_and_names_each_decoy_as_not_a_cause() -> None:
    events: list[str] = []
    faults = {"a": _Fault("a", events), "b": _Fault("b", events)}
    spec = _spec(
        ComponentSpec(role="fault", problem="a"),
        ComponentSpec(role="fault", problem="b"),
        ComponentSpec(role="decoy", decoy="benign_env_drift"),
    )
    problem = _build(spec, faults, events)

    assert faults["a"].root_cause in problem.root_cause
    assert faults["b"].root_cause in problem.root_cause
    assert "LOG_LEVEL=debug on geo is benign." in problem.root_cause
    assert "NOT a root cause" in problem.root_cause
    assert problem.diagnosis_oracle.expected == problem.root_cause


def test_one_component_recovers_alone_and_is_not_recovered_again() -> None:
    events: list[str] = []
    problem, faults = _two_faults(events)
    problem.inject_fault()
    faults["a"].mitigation_oracle.success = False

    problem.recover_component(1)
    component = problem.component_oracle(0).evaluate()
    problem.recover_fault()

    assert events == ["inject a", "inject b", "recover b", "recover a"]
    assert component == {"success": False}
    with pytest.raises(ValueError, match="already recovered"):
        problem.recover_component(1)


def test_a_failed_injection_rolls_back_the_components_already_injected() -> None:
    events: list[str] = []
    problem, faults = _two_faults(events)

    def broken() -> None:
        raise RuntimeError("b cannot inject")

    faults["b"].inject_fault = broken
    with pytest.raises(RuntimeError, match="b cannot inject"):
        problem.inject_fault()

    assert events == ["inject a", "recover b", "recover a"]


def test_components_must_share_one_application_namespace() -> None:
    events: list[str] = []
    faults = {"a": _Fault("a", events), "b": _Fault("b", events, app=_App("social-network"))}
    spec = _spec(ComponentSpec(role="fault", problem="a"), ComponentSpec(role="fault", problem="b"))

    with pytest.raises(ValueError, match="one application namespace"):
        _build(spec, faults, events)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"role": "cause", "problem": "a"}, "role"),
        ({"role": "fault"}, "fault component names a registry problem"),
        ({"role": "fault", "problem": "a", "decoy": "benign_env_drift"}, "fault component names a registry problem"),
        ({"role": "decoy", "problem": "a"}, "decoy component names a decoy kind"),
    ],
)
def test_component_specs_are_validated(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        ComponentSpec(**kwargs)


def test_a_composite_needs_two_components_one_of_them_a_fault_and_no_repeats() -> None:
    fault = ComponentSpec(role="fault", problem="a")
    decoy = ComponentSpec(role="decoy", decoy="benign_env_drift")
    with pytest.raises(ValueError, match="at least two components"):
        _spec(fault)
    with pytest.raises(ValueError, match="at least one fault"):
        _spec(decoy, decoy)
    with pytest.raises(ValueError, match="more than once"):
        _spec(fault, fault)
    with pytest.raises(ValueError, match="composite_"):
        _spec(fault, decoy, problem_id="policy_and_rate")


def test_the_shipped_table_parses_and_names_the_assurance_composites() -> None:
    specs = load_composite_specs()

    assert set(specs) >= {
        "composite_policy_and_rate_configmap_hotel_reservation",
        "composite_frontend_selector_and_readiness_hotel_reservation",
        "composite_geo_configmap_with_log_drift_hotel_reservation",
        "composite_user_target_port_and_geo_image_hotel_reservation",
        "composite_rate_service_and_geo_image_hotel_reservation",
        "composite_user_target_port_and_rate_service_hotel_reservation",
    }
    k1 = specs["composite_policy_and_rate_configmap_hotel_reservation"]
    assert [component.problem for component in k1.components] == [
        "network_policy_block",
        "missing_configmap_mongodb_rate_hotel_reservation",
    ]
    k2 = specs["composite_frontend_selector_and_readiness_hotel_reservation"]
    assert [component.problem for component in k2.components] == [
        "wrong_service_selector_hotel_reservation",
        "readiness_probe_misconfiguration_hotel_reservation",
    ]
    k3 = specs["composite_geo_configmap_with_log_drift_hotel_reservation"]
    assert [(component.role, component.problem or component.decoy) for component in k3.components] == [
        ("fault", "missing_configmap_hotel_reservation"),
        ("decoy", "benign_env_drift"),
    ]
    assert k3.components[1].params == {"deployment": "geo", "env": {"LOG_LEVEL": "debug"}}


def test_the_table_rejects_unknown_keys(tmp_path: Path) -> None:
    table = tmp_path / "specs.json"
    table.write_text(
        json.dumps({"composite_x_hotel_reservation": {"components": [{"role": "fault", "problem": "a", "extra": 1}]}})
    )
    with pytest.raises(ValueError, match="extra"):
        load_composite_specs(table)


def _registry(monkeypatch):
    class _SelectionKubeCtl:
        def is_emulated_cluster(self):
            return False

    monkeypatch.setattr(registry_module, "KubeCtl", _SelectionKubeCtl)
    return registry_module.ProblemRegistry()


def test_the_registry_merges_every_composite_and_each_component_is_registered(monkeypatch) -> None:
    registry = _registry(monkeypatch)
    specs = load_composite_specs(COMPOSITE_SPECS_PATH)

    for problem_id, spec in specs.items():
        assert problem_id in registry.PROBLEM_REGISTRY
        for component in spec.components:
            if component.role == "fault":
                assert component.problem in registry.PROBLEM_REGISTRY
                assert not component.problem.startswith("composite_")
            else:
                assert component.decoy in composite_module.DECOY_FACTORIES


@pytest.mark.parametrize(
    ("short_name", "services"),
    [
        ("repeat05", ("profile", "user")),
        ("repeat08", ("search", "geo")),
        ("compose03", ("profile", "search")),
        ("compose05", ("rate", "geo")),
        ("compose06", ("reservation", "search")),
        ("compose08", ("user", "recommendation")),
        ("compose09", ("geo", "rate")),
        ("compose10", ("search", "reservation")),
    ],
)
def test_pbprog_composites_resolve_with_distinct_services(
    monkeypatch, short_name, services, offline_problem_dependencies
) -> None:
    problem_id = f"composite_pbprog_{short_name}_hotel_reservation"

    problem = _registry(monkeypatch).get_problem_instance(problem_id)

    assert isinstance(problem, CompositeFaultProblem)
    targets = tuple(
        component.faulty_service[0] if isinstance(component.faulty_service, list) else component.faulty_service
        for component in problem.components
    )
    assert targets == services
    assert len(set(targets)) == len(targets)


def test_the_registry_builds_components_through_its_own_factories(monkeypatch) -> None:
    registry = _registry(monkeypatch)
    events: list[str] = []
    built: list[str] = []

    def factory(name):
        def build():
            built.append(name)
            return _Fault(name, events)

        return build

    registry.PROBLEM_REGISTRY["network_policy_block"] = factory("network_policy_block")
    registry.PROBLEM_REGISTRY["missing_configmap_mongodb_rate_hotel_reservation"] = factory("rate")

    problem = registry.get_problem_instance("composite_policy_and_rate_configmap_hotel_reservation")

    assert isinstance(problem, CompositeFaultProblem)
    assert built == ["network_policy_block", "rate"]
    assert problem.app is problem.components[0].app


class _KubeCtl:
    def __init__(self, env: list[dict] | None) -> None:
        self.env = env
        self.commands: list[str] = []

    def exec_command(self, command: str) -> str:
        self.commands.append(command)
        if command.startswith("kubectl get deployment"):
            container = {"name": "hotel-reserv-geo"}
            if self.env is not None:
                container["env"] = self.env
            return json.dumps({"spec": {"template": {"spec": {"containers": [container]}}}})
        return "ok"


def test_benign_env_drift_sets_the_variable_and_unsets_it_when_it_was_absent() -> None:
    kubectl = _KubeCtl([{"name": "JAEGER_SAMPLE_RATIO", "value": "1"}])
    decoy = BenignEnvDrift(_App(), deployment="geo", env={"LOG_LEVEL": "debug"}, kubectl=kubectl)

    decoy.inject_fault()
    decoy.recover_fault()

    changes = [command for command in kubectl.commands if "set env" in command]
    assert changes == [
        "kubectl set env deployment/geo -n hotel-reservation LOG_LEVEL=debug",
        "kubectl set env deployment/geo -n hotel-reservation LOG_LEVEL-",
    ]
    assert any(command.startswith("kubectl rollout status deployment/geo") for command in kubectl.commands)
    assert decoy.mitigation_oracle is None
    assert "not a root cause" in decoy.root_cause.lower()


def test_benign_env_drift_restores_a_value_that_was_already_set() -> None:
    kubectl = _KubeCtl([{"name": "LOG_LEVEL", "value": "info"}])
    decoy = BenignEnvDrift(_App(), deployment="geo", env={"LOG_LEVEL": "debug"}, kubectl=kubectl)

    decoy.inject_fault()
    decoy.recover_fault()

    changes = [command for command in kubectl.commands if "set env" in command]
    assert changes[-1] == "kubectl set env deployment/geo -n hotel-reservation LOG_LEVEL=info"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"deployment": "", "env": {"LOG_LEVEL": "debug"}}, "deployment"),
        ({"deployment": "geo", "env": {}}, "at least one variable"),
        ({"deployment": "geo", "env": {"LOG LEVEL": "debug"}}, "variable name"),
        ({"deployment": "geo", "env": {"LOG_LEVEL": "a b"}}, "value"),
    ],
)
def test_benign_env_drift_validates_its_parameters(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        BenignEnvDrift(_App(), kubectl=_KubeCtl(None), **kwargs)


def test_full_worker_recovery_reports_fault_and_decoy_failures_and_retries():
    from sregym.fastloop.worker import FastloopWorker

    events = []
    faults = {"a": _Fault("a", events), "b": _Fault("b", events, fail_recover=True)}
    spec = _spec(
        ComponentSpec(role="fault", problem="a"),
        ComponentSpec(role="decoy", decoy="benign_env_drift", params={"deployment": "geo"}),
        ComponentSpec(role="fault", problem="b"),
    )
    p = _build(spec, faults, events)
    worker = FastloopWorker(
        problem_factory=lambda _: p,
        deployer=lambda *args: None,
        health_probe=lambda _: {},
        cluster_factory=lambda *args: ("fake", "fake"),
        cluster_inventory=EMPTY_INVENTORY,
    )
    injected = worker.handle({"op": "inject", "problem_id": spec.problem_id})
    assert injected["faults"] == [0, 1]
    assert [c["component_index"] for c in injected["components"]] == [0, 2]
    assert [c["injection_order"] for c in injected["components"]] == [0, 2]
    assert [c["identity"]["problem_id"] for c in injected["components"]] == ["a", "b"]
    # Component oracle/recovery never touches its sibling or the decoy.
    faults["b"].mitigation_oracle.success = False
    assert worker.handle({"op": "oracle", "fault": 0})["success"] is True
    assert worker.handle({"op": "oracle", "fault": 1})["success"] is False
    events.clear()
    p.components[1].recover_fault = lambda: (_ for _ in ()).throw(RuntimeError("decoy failed"))
    result = worker.handle({"op": "recover", "mode": "full"})
    assert not result["success"] and result["recovered"] == [0] and result["pending"] == [1]
    assert [e["component_index"] for e in result["errors"]] == [2, 1]
    assert p.pending_components == (2, 1)
    with pytest.raises(RuntimeError, match="recovery"):
        worker.handle({"op": "inject", "problem_id": "next"})
    faults["b"].fail_recover = False
    p.components[1].recover_fault = lambda: events.append("recover decoy")
    retry = worker.handle({"op": "recover", "mode": "full"})
    assert retry["success"] and retry["recovered"] == [1] and not p.pending_components
    assert events == ["recover b", "recover a", "recover b", "recover decoy"]


def test_catalog_full_recovery_can_address_only_one_component():
    events = []
    problem, _ = _two_faults(events)
    problem.inject_fault()
    result = problem.recovery_result(0)
    assert result["success"] and result["recovered"] == [0] and result["pending"] == [1]
    assert problem.pending_components == (1,)
    assert events == ["inject a", "inject b", "recover a"]


def test_partial_catalog_injection_is_invalid_and_restores_the_failed_component():
    from sregym.fastloop.worker import FastloopWorker

    events = []
    problem, faults = _two_faults(events)
    dirty = set()

    def fail_after_write():
        dirty.add("b")
        raise RuntimeError("b failed after write")

    faults["b"].inject_fault = fail_after_write
    original_recover = faults["b"].recover_fault

    def restore_b():
        original_recover()
        dirty.discard("b")

    faults["b"].recover_fault = restore_b
    worker = FastloopWorker(
        problem_factory=lambda _: problem,
        deployer=lambda *args: None,
        health_probe=lambda _: {},
        cluster_factory=lambda *args: ("fake", "fake"),
        cluster_inventory=EMPTY_INVENTORY,
    )
    result = worker.handle({"op": "inject", "problem_id": problem.problem_id})
    assert not result["injection"]["valid"]
    assert not dirty
    assert events == ["inject a", "recover b", "recover a"]
    assert worker.handle({"op": "recover", "mode": "full"})["success"]


def test_catalog_reports_a_nonraising_invalid_component():
    problem, faults = _two_faults([])
    faults["b"].injection_failure = lambda: {"synthetic": "invalid component b"}
    problem.inject_fault()
    assert problem.injection_failure() is not None
