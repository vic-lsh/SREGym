"""A planned run can mix existing single faults with generated multi-fault problems."""

import json

import pytest

from sregym.conductor.problems.composition.generator import parse_plan_id, parse_weights
from sregym.parallel_runner import _args_for_tests, build_static_plan, save_sequence_plan


def test_weighted_sequence_is_reproducible_and_contains_one_and_many_faults():
    args = _args_for_tests(
        problem="missing_service_hotel_reservation",
        sequence_len=12,
        sequence_seed=17,
        fault_count_weights="1:1,2:1,3:1",
        composition_apps=["hotel_reservation"],
    )

    first = build_static_plan(args)
    assert first == build_static_plan(args)
    assert len(first) == 12
    assert any(task.problem_id == "missing_service_hotel_reservation" for task in first)
    assert any(parse_plan_id(task.problem_id) is not None for task in first)
    assert {parse_plan_id(task.problem_id)[1] for task in first if parse_plan_id(task.problem_id)} <= {2, 3}


def test_fault_count_weights_require_positive_counts_and_weights():
    assert parse_weights("1:3,2:2,5:1") == {1: 3.0, 2: 2.0, 5: 1.0}
    for invalid in ("", "0:1", "1:0", "1:-2", "2:nan", "1:1,1:2"):
        try:
            parse_weights(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted {invalid!r}")


def test_impossible_composition_fails_while_planning():
    args = _args_for_tests(
        problem="missing_service_hotel_reservation",
        sequence_len=1,
        fault_count_weights="9:1",
        composition_apps=["hotel_reservation"],
    )
    with pytest.raises(ValueError, match="at most 8"):
        build_static_plan(args)


def test_resume_rejects_a_changed_sequence(tmp_path):
    path = tmp_path / "sequence_plan.json"
    tasks = build_static_plan(_args_for_tests(problem="missing_service_hotel_reservation", sequence_len=2))
    save_sequence_plan(path, tasks)
    assert json.loads(path.read_text()) == [
        {"sequence": task.sequence, "problem_id": task.problem_id} for task in tasks
    ]
    save_sequence_plan(path, tasks)
    with pytest.raises(ValueError, match="does not match"):
        save_sequence_plan(path, [tasks[1], tasks[0]])


def test_single_fault_slots_reject_a_static_composite():
    args = _args_for_tests(
        problem="composite_example",
        sequence_len=1,
        fault_count_weights="1:1",
        composition_apps=["hotel_reservation"],
    )
    with pytest.raises(ValueError, match="single-fault pool is empty"):
        build_static_plan(args)
