"""Unit tests for upstream-backed parallel scheduling."""

from __future__ import annotations

import csv
import queue
import threading
from pathlib import Path

from sregym.parallel_runner import (
    RunResult,
    RunTask,
    _args_for_tests,
    _child_command,
    _manifest_row_is_complete,
    _read_solved,
    _worker,
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


def test_child_command_preserves_source_workspace_mode():
    args = _args_for_tests(
        deploy_from_source=True,
        app_filter="hotel_reservation",
        application_workspace=True,
    )
    args.agent = "sdo_codex"
    args.model = "haiku"
    args.agent_timeout = 90
    args.judge_model = "judge"
    args.reasoning_effort = None
    args.noise = False
    args.force_build = False
    args.use_external_harness = False

    command = _child_command(args, RunTask(0, "missing_configmap_hotel_reservation"))

    assert "--deploy-from-source" in command
    assert command[command.index("--app-filter") + 1] == "hotel_reservation"
    assert "--application-workspace" in command


def test_read_solved_uses_upstream_flattened_stage_results(tmp_path: Path):
    result_dir = tmp_path / "results" / "agent" / "problem"
    result_dir.mkdir(parents=True)
    result_path = result_dir / "problem_agent_results.csv"
    with result_path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["Diagnosis.success", "Mitigation.success"])
        writer.writeheader()
        writer.writerow({"Diagnosis.success": "True", "Mitigation.success": "true"})
    assert _read_solved(tmp_path / "results") is True


def test_manifest_completion_requires_result_artifact(tmp_path: Path):
    result_dir = tmp_path / "run"
    result_dir.mkdir()
    row = {"returncode": "0", "result_dir": str(result_dir)}

    assert _manifest_row_is_complete(row) is False
    (result_dir / "attempt_results.csv").write_text(
        "problem_id,Diagnosis.success\nexample,false\n",
        encoding="utf-8",
    )
    assert _manifest_row_is_complete(row) is True


def test_worker_waits_for_supervisor_ack_before_next_task(tmp_path, monkeypatch):
    tasks = queue.Queue()
    results = queue.Queue()
    acknowledgement = queue.Queue()
    stop = threading.Event()
    task = RunTask(0, "target_port")
    tasks.put(task)
    tasks.put(None)
    monkeypatch.setattr("sregym.parallel_runner.create_worker_cluster", lambda *args: ("cluster", "kubeconfig"))
    monkeypatch.setattr("sregym.parallel_runner.delete_worker_cluster", lambda *args: None)
    monkeypatch.setattr(
        "sregym.parallel_runner._run_child",
        lambda *args: RunResult(task, 0, 0, 1.0, str(tmp_path), True),
    )

    worker = threading.Thread(
        target=_worker,
        args=(_args_for_tests(), 0, tmp_path, tasks, results, acknowledgement, stop),
    )
    worker.start()
    assert results.get(timeout=2).task == task
    assert worker.is_alive()
    acknowledgement.put(None)
    worker.join(timeout=2)
    assert not worker.is_alive()


def test_worker_id_offset_selects_cluster_and_host_ports(tmp_path, monkeypatch):
    """Concurrent single-worker runs on different clusters must not share a cluster or a host port."""

    import subprocess

    from sregym.parallel_runner import _run_child

    monkeypatch.setenv("SREGYM_WORKER_ID_OFFSET", "2")
    clusters = []
    monkeypatch.setattr(
        "sregym.parallel_runner.create_worker_cluster",
        lambda worker_id, root: clusters.append(worker_id) or ("luna-w2", "kubeconfig"),
    )
    monkeypatch.setattr("sregym.parallel_runner.delete_worker_cluster", lambda *args: None)
    seen = []
    monkeypatch.setattr(
        "sregym.parallel_runner._run_child",
        lambda args, task, worker_id, root, kubeconfig, cluster_name: seen.append((worker_id, cluster_name))
        or RunResult(task, worker_id, 0, 1.0, str(tmp_path), True),
    )
    tasks = queue.Queue()
    tasks.put(RunTask(0, "target_port"))
    tasks.put(None)
    acknowledgement = queue.Queue()
    acknowledgement.put(None)
    _worker(_args_for_tests(), 0, tmp_path, tasks, queue.Queue(), acknowledgement, threading.Event())
    assert clusters == [2]
    assert seen == [(0, "luna-w2")]

    envs = []
    monkeypatch.setattr("sregym.parallel_runner._child_command", lambda args, task: ["true"])
    monkeypatch.setattr(
        "sregym.parallel_runner.subprocess.run",
        lambda *args, env, **kwargs: envs.append(env) or subprocess.CompletedProcess(args, 1),
    )
    result = _run_child(_args_for_tests(), RunTask(0, "target_port"), 0, tmp_path, "kubeconfig", "luna-w2")

    assert envs[0]["SREGYM_WORKER_ID"] == "2"
    assert envs[0]["API_PORT"] == "8002"
    assert envs[0]["MCP_SERVER_PORT"] == "9956"
    # The local results layout is unchanged.
    assert result.result_dir.endswith("worker_0")


def test_without_offset_worker_ids_and_ports_are_unchanged(tmp_path, monkeypatch):
    import subprocess

    from sregym.parallel_runner import _run_child

    monkeypatch.delenv("SREGYM_WORKER_ID_OFFSET", raising=False)
    envs = []
    monkeypatch.setattr("sregym.parallel_runner._child_command", lambda args, task: ["true"])
    monkeypatch.setattr(
        "sregym.parallel_runner.subprocess.run",
        lambda *args, env, **kwargs: envs.append(env) or subprocess.CompletedProcess(args, 1),
    )
    _run_child(_args_for_tests(), RunTask(0, "target_port"), 1, tmp_path, "kubeconfig", "sregym-w1")

    assert (envs[0]["SREGYM_WORKER_ID"], envs[0]["API_PORT"], envs[0]["MCP_SERVER_PORT"]) == ("1", "8001", "9955")
