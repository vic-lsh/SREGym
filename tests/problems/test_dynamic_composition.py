"""Generated problem IDs resolve to reproducible, independently recoverable faults."""

from types import SimpleNamespace

import pytest

import sregym.conductor.problems.composite as composite_module
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composition import resolve
from sregym.conductor.problems.composition.catalog import entries_for
from sregym.conductor.problems.composition.generator import PlanError, PlanRef, draw_plan, parse_plan_id, plan_id
from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, Footprint, conflicts
from sregym.conductor.problems.registry import ProblemRegistry


class _Oracle(Oracle):
    def evaluate(self, *args, **kwargs):
        return {"success": True}


class _Fault(Problem):
    def __init__(self, draw, events):
        super().__init__(app=SimpleNamespace(namespace="hotel-reservation", name="Hotel Reservation"))
        self.root_cause = draw.key
        self.mitigation_oracle = _Oracle(self)
        self.events = events
        self.key = draw.key

    def inject_fault(self):
        self.events.append(f"inject {self.key}")

    def recover_fault(self):
        self.events.append(f"recover {self.key}")


def test_catalog_draws_are_deterministic_and_conflict_free():
    for app in ("hotel_reservation", "social_network", "astronomy_shop", "train_ticket"):
        first = draw_plan(app, 5, 23, entries_for(app))
        second = draw_plan(app, 5, 23, tuple(reversed(entries_for(app))))
        assert first == second
        assert len(first.draws) == 5
        assert all(
            not conflicts(left, right) for index, left in enumerate(first.draws) for right in first.draws[index + 1 :]
        )
        assert len({draw.param_dict()["faulty_service"] for draw in first.draws}) == 5


def _example_entry(events):
    services = ("user", "search", "rate")

    def sample(app, rng):
        service = rng.choice(services)
        return FaultDraw(
            key="example",
            app=app,
            params=(("faulty_service", service),),
            footprints=(Footprint("Service", service, "spec.ports.targetPort"),),
        )

    return CatalogEntry(
        key="example",
        apps=frozenset({"hotel_reservation"}),
        sample=sample,
        build=lambda draw: _Fault(draw, events),
    )


def _registry_with(monkeypatch, entry):
    monkeypatch.setattr("sregym.conductor.problems.composition.catalog.entries_for", lambda app: (entry,))
    monkeypatch.setattr(composite_module, "LLMAsAJudgeOracle", lambda **kwargs: SimpleNamespace())
    registry = object.__new__(ProblemRegistry)
    registry.PROBLEM_REGISTRY = {}
    return registry


def test_generated_problem_resolves_and_recovers_each_fault(monkeypatch):
    events = []
    registry = _registry_with(monkeypatch, _example_entry(events))
    problem_id = plan_id("hotel_reservation", 2, 11)
    problem = registry.get_problem_instance(problem_id)
    assert problem.problem_id == problem_id
    assert len(problem.components) == 2
    problem.inject_fault()
    problem.recover_fault()
    assert events == ["inject example", "inject example", "recover example", "recover example"]
    assert (problem.composite_base_n, problem.composite_take_k, problem.fault_base_index) == (2, 2, (0, 1))


def test_take_k_problems_are_nested_subsets_of_one_base_plan(monkeypatch):
    registry = _registry_with(monkeypatch, _example_entry([]))
    whole = registry.get_problem_instance(plan_id("hotel_reservation", 3, 5))
    base = dict(zip(whole.fault_base_index, whole.fault_draws, strict=True))
    previous: set[int] = set()
    for take in (1, 2, 3):
        problem_id = plan_id("hotel_reservation", 3, 5, take)
        problem = registry.get_problem_instance(problem_id)
        assert problem.problem_id == problem_id
        assert (problem.composite_base_n, problem.composite_take_k) == (3, take)
        assert len(problem.components) == take
        assert problem.fault_draws == tuple(base[i] for i in problem.fault_base_index)
        assert [name.split("-")[0] for name in (c.name for c in problem.spec.components)] == [
            str(i) for i in problem.fault_base_index
        ]
        assert previous < set(problem.fault_base_index)
        previous = set(problem.fault_base_index)


def test_invalid_plan_ids_and_impossible_counts_fail_clearly():
    assert parse_plan_id("mf__hotel_reservation__n2__s11__v_") == PlanRef("hotel_reservation", 2, 11)
    assert parse_plan_id("mf__hotel_reservation__n2__s11__k1__v_") == PlanRef("hotel_reservation", 2, 11, 1)
    with pytest.raises(ValueError, match="malformed"):
        resolve("mf__hotel_reservation__n0__s11__v_")
    with pytest.raises(ValueError, match="malformed"):
        resolve("mf__hotel_reservation__n2__s11__k3__v_")
    with pytest.raises(PlanError, match="no conflict-free plan"):
        draw_plan("hotel_reservation", 9, 1, entries_for("hotel_reservation"))
