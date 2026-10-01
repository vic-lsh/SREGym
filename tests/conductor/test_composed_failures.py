"""ComposedFailures: same-app composition with AND mitigation and per-fault diagnosis credit."""

from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.problems.composed_failures import COMPOSITE_SPECS, ComposedFailures, PerFaultDiagnosisOracle


class _App:
    name = "Hotel Reservation"
    namespace = "hotel-reservation"


class _Oracle(Oracle):
    def __init__(self, problem, ok):
        super().__init__(problem)
        self.ok = ok

    def evaluate(self, *args, **kwargs):
        return {"success": self.ok}


class _Sub:
    def __init__(self, target, *, app=None, mitigated=False, cls_name=None):
        self.app = app or _App()
        self.namespace = self.app.namespace
        self.faulty_service = target
        self.root_cause = f"root cause of {target}"
        self.diagnosis_oracle = None
        self.mitigation_oracle = _Oracle(self, mitigated)
        self.events = []
        if cls_name:
            self.__class__ = type(cls_name, (_Sub,), {})

    def inject_fault(self):
        self.events.append("inject")

    def recover_fault(self):
        self.events.append("recover")


def _judge(verdicts):
    """Judge factory: grades the expected text that mentions a component as listed in verdicts."""

    def factory(problem, expected):
        ok = any(component in expected.split("\n\n")[0] and found for component, found in verdicts.items())
        return _Oracle(problem, ok)

    return factory


def test_uses_one_shared_hotel_reservation_app():
    subs = [_Sub("geo"), _Sub("recommendation")]
    problem = ComposedFailures(subs, judge_factory=_judge({}))
    assert problem.app.name == "Hotel Reservation"
    assert all(sub.app is problem.app for sub in subs)
    assert problem.namespace == "hotel-reservation"


def test_rejects_two_faults_on_one_deployment():
    with pytest.raises(ValueError, match="geo"):
        ComposedFailures([_Sub("geo"), _Sub("geo")], judge_factory=_judge({}))


def test_rejects_overlapping_multi_target_deployments():
    multi = _Sub("mongodb-geo")
    del multi.faulty_service
    multi.faulty_services = ["mongodb-geo", "mongodb-rate"]
    with pytest.raises(ValueError, match="mongodb-geo"):
        ComposedFailures([multi, _Sub("mongodb-geo")], judge_factory=_judge({}))


def test_rejects_wrong_arity_and_foreign_apps():
    with pytest.raises(ValueError):
        ComposedFailures([_Sub("geo")], judge_factory=_judge({}))
    with pytest.raises(ValueError):
        ComposedFailures([_Sub(str(i)) for i in range(6)], judge_factory=_judge({}))
    other = SimpleNamespace(name="Social Network", namespace="social")
    with pytest.raises(ValueError, match="Hotel Reservation"):
        ComposedFailures([_Sub("geo"), _Sub("user", app=other)], judge_factory=_judge({}))


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [(True, True, True), (True, False, False), (False, True, False), (False, False, False)],
)
def test_mitigation_is_and_of_sub_oracles(first, second, expected):
    subs = [_Sub("geo", mitigated=first), _Sub("recommendation", mitigated=second)]
    problem = ComposedFailures(subs, judge_factory=_judge({}))
    result = problem.mitigation_oracle.evaluate()
    assert result["success"] is expected
    assert [item["success"] for item in result["oracles"]] == [first, second]


def test_diagnosis_gives_per_fault_credit():
    subs = [_Sub("geo"), _Sub("recommendation")]
    problem = ComposedFailures(subs, judge_factory=_judge({"geo": True, "recommendation": False}))
    assert isinstance(problem.diagnosis_oracle, PerFaultDiagnosisOracle)
    result = problem.diagnosis_oracle.evaluate("the geo deployment is broken")
    assert result["success"] is False
    assert result["faults_found"] == 1
    assert result["faults_total"] == 2
    assert [item["component"] for item in result["per_fault"]] == ["geo", "recommendation"]
    assert [item["success"] for item in result["per_fault"]] == [True, False]
    assert result["accuracy"] == 50.0


def test_diagnosis_succeeds_only_when_every_fault_found():
    subs = [_Sub("geo"), _Sub("recommendation")]
    problem = ComposedFailures(subs, judge_factory=_judge({"geo": True, "recommendation": True}))
    result = problem.diagnosis_oracle.evaluate("both")
    assert result["success"] is True
    assert result["accuracy"] == 100.0


def test_expected_text_names_other_faults_as_context():
    seen = []

    def factory(problem, expected):
        seen.append(expected)
        return _Oracle(problem, True)

    ComposedFailures([_Sub("geo"), _Sub("recommendation")], judge_factory=factory)
    assert seen[0].startswith("root cause of geo")
    assert "root cause of recommendation" in seen[0]
    assert "do not penalize" in seen[0].lower()


def test_inject_in_order_and_recover_in_reverse():
    order = []
    subs = [_Sub("geo"), _Sub("recommendation")]
    for sub in subs:
        sub.inject_fault = lambda s=sub: order.append(("inject", s.faulty_service))
        sub.recover_fault = lambda s=sub: order.append(("recover", s.faulty_service))
    problem = ComposedFailures(subs, judge_factory=_judge({}), settle_seconds=0)
    problem.inject_fault()
    problem.recover_fault()
    assert order == [
        ("inject", "geo"),
        ("inject", "recommendation"),
        ("recover", "recommendation"),
        ("recover", "geo"),
    ]


def test_registered_composites_are_hotel_reservation_groups():
    assert len(COMPOSITE_SPECS) == 5
    for problem_id, parts in COMPOSITE_SPECS.items():
        assert "__v_" not in problem_id
        assert 2 <= len(parts) <= 5


def test_registered_n_fault_composites_have_three_and_five_faults():
    sizes = {problem_id: len(parts) for problem_id, parts in COMPOSITE_SPECS.items()}
    assert sizes["composite3_hotel_geo_rate_recommendation"] == 3
    assert sizes["composite5_hotel_geo_rate_recommendation_frontend_user"] == 5


@pytest.mark.parametrize("size", [3, 4, 5])
def test_n_fault_mitigation_needs_every_sub_oracle(size):
    for failing in range(size):
        subs = [_Sub(f"svc{i}", mitigated=i != failing) for i in range(size)]
        result = ComposedFailures(subs, judge_factory=_judge({})).mitigation_oracle.evaluate()
        assert result["success"] is False
        assert [item["success"] for item in result["oracles"]].count(False) == 1
    subs = [_Sub(f"svc{i}", mitigated=True) for i in range(size)]
    assert ComposedFailures(subs, judge_factory=_judge({})).mitigation_oracle.evaluate()["success"] is True


def test_five_fault_diagnosis_counts_partial_credit():
    subs = [_Sub(f"svc{i}") for i in range(5)]
    verdicts = {"svc0": True, "svc1": True, "svc3": True}
    problem = ComposedFailures(subs, judge_factory=_judge(verdicts))
    result = problem.diagnosis_oracle.evaluate("svc0 svc1 svc3")
    assert result["faults_found"] == 3
    assert result["faults_total"] == 5
    assert result["accuracy"] == 60.0
    assert result["success"] is False


def test_registry_lists_composites(monkeypatch):
    from sregym.conductor.problems import registry as registry_module

    monkeypatch.setattr(registry_module, "KubeCtl", lambda: SimpleNamespace())
    registry = registry_module.ProblemRegistry()
    assert set(COMPOSITE_SPECS) <= set(registry.PROBLEM_REGISTRY)
    assert set(COMPOSITE_SPECS) <= set(registry.get_problem_ids(all=True))
