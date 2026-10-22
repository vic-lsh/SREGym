"""Generated problem IDs resolve to reproducible plans (the composite they build:
tests/composition/test_colocated_composite.py)."""

import pytest

from sregym.conductor.problems.composition import resolve
from sregym.conductor.problems.composition.catalog import entries_for
from sregym.conductor.problems.composition.generator import PlanError, PlanRef, draw_plan, parse_plan_id
from sregym.conductor.problems.composition.spec import conflicts
from sregym.conductor.problems.registry import ProblemRegistry


def test_catalog_draws_are_deterministic_and_conflict_free():
    for app in ("hotel_reservation", "social_network", "astronomy_shop", "train_ticket"):
        first = draw_plan(app, 5, 23, entries_for(app))
        second = draw_plan(app, 5, 23, tuple(reversed(entries_for(app))))
        assert first == second
        assert len(first.draws) == 5
        assert all(
            not conflicts(left, right) for index, left in enumerate(first.draws) for right in first.draws[index + 1 :]
        )


def test_invalid_plan_ids_and_impossible_counts_fail_clearly():
    assert parse_plan_id("mf__hotel_reservation__n2__s11__v_") == PlanRef("hotel_reservation", 2, 11)
    assert parse_plan_id("mf__hotel_reservation__n2__s11__k1__v_") == PlanRef("hotel_reservation", 2, 11, 1)
    with pytest.raises(ValueError, match="malformed"):
        resolve("mf__hotel_reservation__n0__s11__v_")
    with pytest.raises(ValueError, match="malformed"):
        resolve("mf__hotel_reservation__n2__s11__k3__v_")
    with pytest.raises(PlanError, match="no conflict-free plan"):
        draw_plan("hotel_reservation", 99, 1, entries_for("hotel_reservation"))


def test_the_registry_resolves_multi_fault_ids_through_composition():
    registry = object.__new__(ProblemRegistry)
    registry.PROBLEM_REGISTRY = {}
    with pytest.raises(ValueError, match="malformed multi-fault problem id"):
        registry.get_problem_instance("mf__hotel_reservation__n2__s11__k3__v_")
    with pytest.raises(ValueError, match="not found in registry"):
        registry.get_problem_instance("no_such_problem")
