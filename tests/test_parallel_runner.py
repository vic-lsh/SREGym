"""Unit tests for upstream-backed parallel scheduling."""

from __future__ import annotations

import csv
from pathlib import Path

from sregym.parallel_runner import (
    RunTask,
    _args_for_tests,
    _child_command,
    _read_solved,
    build_static_plan,
    select_problem_ids,
)


def test_repeat_expands_static_plan_with_stable_sequence():
    args = _args_for_tests(problem="target_port", n_attempts=3)
    plan = build_static_plan(args)
    assert plan == [
        RunTask(0, "target_port"),
        RunTask(1, "target_port"),
        RunTask(2, "target_port"),
    ]


def test_sequence_sampling_is_deterministic():
    args = _args_for_tests(suite="sregym-lite", sequence_len=5, sequence_seed=9)
    first = build_static_plan(args)
    second = build_static_plan(args)
    assert first == second
    assert len(first) == 5


def test_tasklist_and_problem_prefix_filter(tmp_path: Path):
    tasklist = tmp_path / "tasks.yml"
    tasklist.write_text(
        "all:\n  problems:\n    - wrong_dns_policy_social_network\n    - target_port\n",
        encoding="utf-8",
    )
    args = _args_for_tests(tasklist=str(tasklist), problem_spec=["wrong_dns_policy"])
    assert select_problem_ids(args) == ["wrong_dns_policy_social_network"]


def test_tasklist_accepts_upstream_stage_mapping(tmp_path: Path):
    tasklist = tmp_path / "tasks.yml"
    tasklist.write_text(
        "all:\n  problems:\n    target_port:\n      - diagnosis\n      - mitigation\n",
        encoding="utf-8",
    )

    args = _args_for_tests(tasklist=str(tasklist))

    assert select_problem_ids(args) == ["target_port"]


def test_variant_selection_is_deterministic_and_bounded():
    args = _args_for_tests(
        variants=True,
        variant_spec=["missing_env_variable"],
        variant_count=4,
        variant_seed=17,
    )
    first = select_problem_ids(args)
    second = select_problem_ids(args)
    assert first == second
    assert len(first) == 4
    assert all(problem.startswith("missing_env_variable__v_") for problem in first)


def test_child_command_uses_upstream_single_problem_cli():
    args = _args_for_tests(problem="target_port")
    args.agent = "codex"
    args.model = "gpt-5"
    args.agent_timeout = 90
    args.judge_model = "gpt-5-mini"
    args.reasoning_effort = "high"
    args.noise = False
    args.force_build = False
    args.use_external_harness = False
    command = _child_command(args, RunTask(2, "target_port"))
    assert "--worker-child" in command
    assert command[command.index("--problem") + 1] == "target_port"
    assert command[command.index("--n-attempts") + 1] == "1"
    assert command[command.index("--judge-model") + 1] == "gpt-5-mini"


def test_read_solved_uses_upstream_flattened_stage_results(tmp_path: Path):
    result_dir = tmp_path / "results" / "agent" / "problem"
    result_dir.mkdir(parents=True)
    result_path = result_dir / "problem_agent_results.csv"
    with result_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["Diagnosis.success", "Mitigation.success"])
        writer.writeheader()
        writer.writerow({"Diagnosis.success": "True", "Mitigation.success": "true"})
    assert _read_solved(tmp_path / "results") is True
