"""Parallel experiment supervisor built around upstream's single-run CLI.

Each worker owns a Kind cluster and invokes the upstream runner in a child
process for one problem at a time. This keeps upstream lifecycle, agent,
artifact, and grading implementations authoritative while preserving the
fork's isolated parallel scheduling and resumable experiment directories.
"""

from __future__ import annotations

import csv
import os
import queue
import random
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

from logger import console
from sregym.conductor.problem_sets import PROBLEM_SETS
from sregym.conductor.problems.variant_generator import (
    AdaptiveScheduler,
    filter_variant_ids_by_spec,
    generate_all_variants,
    generate_variant_stream,
    generate_variant_stream_by_class,
    generate_variant_stream_grouped,
)
from sregym.conductor.problems.variant_specs import get_all_variant_specs
from sregym.worker_infra import create_worker_cluster, delete_worker_cluster


@dataclass(frozen=True)
class RunTask:
    sequence: int
    problem_id: str


@dataclass(frozen=True)
class RunResult:
    task: RunTask
    worker_id: int
    returncode: int
    elapsed_seconds: float
    result_dir: str
    solved: bool
    error: str = ""


def _load_tasklist(path: Path) -> list[str]:
    with path.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    problems = document.get("all", {}).get("problems")
    if isinstance(problems, dict) and all(isinstance(item, str) for item in problems):
        return list(problems)
    if isinstance(problems, list) and all(isinstance(item, str) for item in problems):
        return problems
    raise ValueError(f"Task list {path} must contain all.problems as a stage mapping or list of strings")


def _filter_problem_specs(problem_ids: list[str], prefixes: list[str] | None) -> list[str]:
    if not prefixes:
        return problem_ids
    return [
        problem_id
        for problem_id in problem_ids
        if any(problem_id == prefix or problem_id.startswith(f"{prefix}_") for prefix in prefixes)
    ]


def _variant_pool(spec_names: list[str] | None) -> list[str]:
    specs = get_all_variant_specs()
    variant_ids = list(generate_all_variants(specs))
    if spec_names:
        variant_ids = filter_variant_ids_by_spec(
            variant_ids,
            spec_names,
            {spec.base_name for spec in specs},
        )
    return variant_ids


def _default_problem_ids() -> list[str]:
    """Read upstream's registry without requiring a configured kubeconfig."""
    from sregym.conductor.problems import registry as registry_module

    original_kubectl = registry_module.KubeCtl

    class _SelectionKubeCtl:
        def is_emulated_cluster(self) -> bool:
            return False

    try:
        registry_module.KubeCtl = _SelectionKubeCtl
        return registry_module.ProblemRegistry().get_problem_ids()
    finally:
        registry_module.KubeCtl = original_kubectl


def select_problem_ids(args: Any) -> list[str]:
    """Resolve static problem selection without requiring a live cluster."""
    if args.variants:
        pool = _variant_pool(args.variant_spec)
        count = args.variant_count or len(pool)
        if args.variant_order == "round-robin":
            return generate_variant_stream_by_class(pool, count, args.variant_offset, args.variant_seed)
        if args.variant_order == "grouped":
            return generate_variant_stream_grouped(
                pool,
                count,
                args.variant_offset,
                args.variant_seed,
                args.variant_max_per_class,
            )
        if args.variant_order == "adaptive":
            raise ValueError("adaptive selection is scheduled dynamically")
        return generate_variant_stream(pool, count, args.variant_offset, args.variant_seed)

    if args.problem:
        selected = [args.problem]
    elif args.suite:
        selected = list(PROBLEM_SETS[args.suite])
    elif not args.tasklist:
        selected = _default_problem_ids()
    else:
        selected = _load_tasklist(Path(args.tasklist))
    return _filter_problem_specs(selected, args.problem_spec)


def build_static_plan(args: Any) -> list[RunTask]:
    selected = select_problem_ids(args)
    if args.sequence_len:
        if not selected:
            raise ValueError("Cannot generate a sequence from an empty problem selection")
        generator = random.Random(args.sequence_seed)
        selected = [generator.choice(selected) for _ in range(args.sequence_len)]
    return [
        RunTask(sequence=index, problem_id=problem_id)
        for index, problem_id in enumerate(problem_id for problem_id in selected for _ in range(args.n_attempts))
    ]


def _child_command(args: Any, task: RunTask) -> list[str]:
    command = [
        sys.executable,
        str(Path(__file__).resolve().parent.parent / "main.py"),
        "--worker-child",
        "--parallel",
        "1",
        "--problem",
        task.problem_id,
        "--agent",
        args.agent,
        "--model",
        args.model,
        "--n-attempts",
        "1",
        "--agent-timeout",
        str(args.agent_timeout),
        "--judge-rounds",
        str(args.judge_rounds),
        "--judge-voting-temperature",
        str(args.judge_voting_temperature),
    ]
    if args.judge_model:
        command.extend(["--judge-model", args.judge_model])
    if args.reasoning_effort:
        command.extend(["--reasoning-effort", args.reasoning_effort])
    if args.noise:
        command.append("--noise")
    if args.force_build:
        command.append("--force-build")
    if args.use_external_harness:
        command.append("--use-external-harness")
    return command


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def _read_solved(result_dir: Path) -> bool:
    rows: list[dict[str, str]] = []
    for result_csv in result_dir.rglob("*_results.csv"):
        with result_csv.open(newline="", encoding="utf-8") as stream:
            rows.extend(csv.DictReader(stream))
    if not rows:
        return False
    row = rows[-1]
    diagnosis = _truthy(row.get("Diagnosis.success"))
    mitigation_value = row.get("Mitigation.success")
    return diagnosis and (mitigation_value is None or _truthy(mitigation_value))


def _run_child(args: Any, task: RunTask, worker_id: int, root: Path, kubeconfig: str) -> RunResult:
    task_dir = root / "runs" / f"{task.sequence:06d}_{task.problem_id}" / f"worker_{worker_id}"
    task_dir.mkdir(parents=True, exist_ok=True)
    log_path = task_dir / "worker.log"
    env = os.environ.copy()
    api_port = 8000 + worker_id
    mcp_port = 9954 + worker_id
    env.update(
        {
            "KUBECONFIG": kubeconfig,
            "SREGYM_BASE_KUBECONFIG": kubeconfig,
            "SREGYM_WORKER_ID": str(worker_id),
            "API_PORT": str(api_port),
            "MCP_SERVER_PORT": str(mcp_port),
            "MCP_SERVER_URL": f"http://127.0.0.1:{mcp_port}",
            "SREGYM_RESULTS_DIR": str((task_dir / "results").resolve()),
        }
    )
    memory_path = root / "long_term_summary.md"
    if args.enable_summary and not args.no_inject_summary and memory_path.exists():
        env["SREGYM_SUMMARY_FILE"] = str(memory_path)
    started = time.monotonic()
    try:
        with log_path.open("w", encoding="utf-8") as log_stream:
            completed = subprocess.run(
                _child_command(args, task),
                cwd=Path(__file__).resolve().parent.parent,
                env=env,
                stdout=log_stream,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        return RunResult(
            task=task,
            worker_id=worker_id,
            returncode=completed.returncode,
            elapsed_seconds=time.monotonic() - started,
            result_dir=str(task_dir),
            solved=completed.returncode == 0 and _read_solved(task_dir / "results"),
        )
    except Exception as exc:
        return RunResult(
            task=task,
            worker_id=worker_id,
            returncode=1,
            elapsed_seconds=time.monotonic() - started,
            result_dir=str(task_dir),
            solved=False,
            error=str(exc),
        )


def _worker(
    args: Any,
    worker_id: int,
    root: Path,
    tasks: queue.Queue[RunTask | None],
    results: queue.Queue[RunResult | tuple[str, int, str]],
    acknowledgement: queue.Queue[None],
    stop: threading.Event,
) -> None:
    cluster_name = ""
    try:
        cluster_name, kubeconfig = create_worker_cluster(worker_id, str(root))
        while not stop.is_set():
            task = tasks.get()
            if task is None:
                break
            results.put(_run_child(args, task, worker_id, root, kubeconfig))
            acknowledgement.get()
    except Exception as exc:
        results.put(("worker_error", worker_id, str(exc)))
    finally:
        delete_worker_cluster(cluster_name)


_MANIFEST_FIELDS = (
    "sequence",
    "problem_id",
    "worker_id",
    "returncode",
    "elapsed_seconds",
    "result_dir",
    "solved",
    "error",
)


def _load_manifest(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=_MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _result_row(result: RunResult) -> dict[str, Any]:
    return {
        "sequence": result.task.sequence,
        "problem_id": result.task.problem_id,
        "worker_id": result.worker_id,
        "returncode": result.returncode,
        "elapsed_seconds": f"{result.elapsed_seconds:.3f}",
        "result_dir": result.result_dir,
        "solved": str(result.solved).lower(),
        "error": result.error,
    }


def _update_operational_memory(root: Path, result: RunResult, args: Any) -> None:
    if not args.enable_summary:
        return
    log_path = Path(result.result_dir) / "worker.log"
    if not log_path.exists():
        return
    memory_path = root / "long_term_summary.md"
    current = memory_path.read_text(encoding="utf-8") if memory_path.exists() else ""
    trajectory = log_path.read_text(encoding="utf-8", errors="replace")[-100000:]
    model = args.summary_model or args.judge_model or args.model

    from llm_backend.get_llm_backend import LiteLLMBackend

    backend = LiteLLMBackend(
        model_name=model,
        api_base=os.environ.get("JUDGE_API_BASE") or os.environ.get("AGENT_API_BASE"),
        api_key=os.environ.get("JUDGE_API_KEY") or os.environ.get("AGENT_API_KEY"),
        temperature=0.0,
    )
    prompt = f"""Maintain a concise operational memory for repeated SRE benchmark runs.

Current memory:
{current or "(empty)"}

Latest run: {result.task.problem_id}; solved={result.solved}
Trajectory and benchmark output:
{trajectory}

Return only the updated Markdown memory. Preserve useful symptoms, diagnostic
signals, root causes, and mitigations; remove unsupported speculation and
merge duplicates.
"""
    response = backend.inference(prompt, system_prompt="You maintain evidence-based SRE operational memory.")
    updated = str(response.content).strip()
    temporary = memory_path.with_suffix(".tmp")
    temporary.write_text(updated, encoding="utf-8")
    temporary.replace(memory_path)


def _experiment_root(args: Any) -> Path:
    if args.experiment_dir:
        return Path(args.experiment_dir).resolve()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (Path("results") / f"parallel_{stamp}").resolve()


def _adaptive_scheduler(args: Any, completed: list[dict[str, str]]) -> AdaptiveScheduler:
    max_per_class = args.variant_max_per_class or 20
    scheduler = AdaptiveScheduler(
        _variant_pool(args.variant_spec),
        consec_solves_to_stop=args.variant_adaptive_consec_solves,
        max_per_class=max_per_class,
        seed=args.variant_seed,
        max_total=args.variant_count,
    )
    for row in sorted(completed, key=lambda item: int(item["sequence"])):
        scheduled = scheduler.next_problem()
        if scheduled is None:
            break
        _, expected_problem = scheduled
        if expected_problem != row["problem_id"]:
            raise ValueError("Existing adaptive manifest does not match the requested scheduler configuration")
        scheduler.record_completion(expected_problem, _truthy(row.get("solved")))
    return scheduler


def run_parallel(args: Any) -> int:
    root = _experiment_root(args)
    root.mkdir(parents=True, exist_ok=True)
    memory_path = root / "long_term_summary.md"
    if args.seed_summary and not memory_path.exists():
        shutil.copy2(args.seed_summary, memory_path)
    manifest_path = root / "parallel_results.csv"
    manifest = _load_manifest(manifest_path)
    completed_keys = {
        (int(row["sequence"]), row["problem_id"]) for row in manifest if int(row.get("returncode", "1")) == 0
    }

    task_queue: queue.Queue[RunTask | None] = queue.Queue()
    result_queue: queue.Queue[RunResult | tuple[str, int, str]] = queue.Queue()
    acknowledgements: list[queue.Queue[None]] = [queue.Queue() for _ in range(args.parallel)]
    stop = threading.Event()
    workers = [
        threading.Thread(
            target=_worker,
            args=(args, worker_id, root, task_queue, result_queue, acknowledgements[worker_id], stop),
            name=f"sregym-worker-{worker_id}",
            daemon=True,
        )
        for worker_id in range(args.parallel)
    ]
    for worker in workers:
        worker.start()

    adaptive = args.variants and args.variant_order == "adaptive"
    scheduler = (
        _adaptive_scheduler(args, [row for row in manifest if int(row.get("returncode", "1")) == 0])
        if adaptive
        else None
    )
    pending = 0
    total = args.variant_count if adaptive else None
    if scheduler is not None:
        for _ in workers:
            item = scheduler.next_problem()
            if item is None:
                break
            task_queue.put(RunTask(*item))
            pending += 1
    else:
        plan = [task for task in build_static_plan(args) if (task.sequence, task.problem_id) not in completed_keys]
        total = len(plan)
        for task in plan:
            task_queue.put(task)
            pending += 1

    failures = 0
    completed_now = 0
    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    )
    progress_task = progress.add_task("Running SREGym", total=total)
    progress.start()
    try:
        while pending:
            outcome = result_queue.get()
            if isinstance(outcome, tuple):
                _, worker_id, error = outcome
                console.print(f"[red]Worker {worker_id} failed during setup: {error}[/red]")
                failures += 1
                stop.set()
                break
            pending -= 1
            completed_now += 1
            failures += int(outcome.returncode != 0)
            manifest.append(_result_row(outcome))
            _write_manifest(manifest_path, manifest)
            try:
                _update_operational_memory(root, outcome, args)
            except Exception as exc:
                console.print(f"[yellow]Operational-memory update failed: {exc}[/yellow]")
            acknowledgements[outcome.worker_id].put(None)
            progress.advance(progress_task)
            progress.update(
                progress_task,
                description=f"{outcome.task.problem_id} ({'solved' if outcome.solved else 'failed'})",
            )

            if scheduler is not None:
                scheduler.record_completion(outcome.task.problem_id, outcome.solved)
                next_item = scheduler.next_problem()
                if next_item is not None:
                    task_queue.put(RunTask(*next_item))
                    pending += 1
    finally:
        progress.stop()
        for acknowledgement in acknowledgements:
            acknowledgement.put(None)
        for _ in workers:
            task_queue.put(None)
        for worker in workers:
            worker.join(timeout=30)

    console.print(
        f"Parallel experiment: {root} ({completed_now} new runs, {failures} failures, {len(completed_keys)} resumed)"
    )
    return 1 if failures else 0


def _args_for_tests(**overrides: Any) -> SimpleNamespace:
    """Small argument factory used by unit tests and downstream wrappers."""
    defaults = {
        "problem": None,
        "suite": None,
        "tasklist": None,
        "problem_spec": None,
        "variants": False,
        "variant_spec": None,
        "variant_count": None,
        "variant_offset": 0,
        "variant_seed": 42,
        "variant_order": "shuffled",
        "variant_max_per_class": None,
        "variant_adaptive_consec_solves": 3,
        "n_attempts": 1,
        "sequence_len": 0,
        "sequence_seed": 42,
        "judge_rounds": 3,
        "judge_voting_temperature": 0.7,
        "enable_summary": False,
        "no_inject_summary": False,
        "summary_model": None,
        "seed_summary": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)
