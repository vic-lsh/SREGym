import argparse
import asyncio
import csv
import fcntl
import glob
import json
import logging
import multiprocessing
import os
import platform
import queue
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil
import uvicorn
from rich.console import Console
from rich.markup import escape
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

# Ensure multiprocessing uses a local filesystem for temp files (fixes NFS busy errors)
# We use the current working directory if it's on /mnt/data (local disk), otherwise fallback to default
if os.getcwd().startswith("/mnt/data"):
    local_tmp = os.path.join(os.getcwd(), ".local_tmp")
    os.makedirs(local_tmp, exist_ok=True)
    os.environ["TMPDIR"] = local_tmp
    tempfile.tempdir = local_tmp

from logger import init_logger
from mcp_server.configs.load_all_cfg import mcp_server_cfg
from mcp_server.sregym_mcp_server import app as mcp_app
from sregym.agent_launcher import AgentLauncher
from sregym.agent_registry import get_agent, list_agents
from sregym.conductor.conductor import Conductor
from sregym.conductor.conductor_api import request_shutdown, run_api
from sregym.conductor.constants import StartProblemResult
from sregym.service.kubeconfig import require_kubeconfig_path

LAUNCHER = AgentLauncher()
# Ensure logger inherits from 'all' so handlers are attached
logger = logging.getLogger("all.main")

# Agents that support the summary system (accumulate learnings across runs).
# Maps agent name -> output filename used by that agent.
# Agents listed here use an external summarizer subprocess run by main.py after each problem.
AGENT_OUTPUT_FILES = {"gemini_cli": "gemini-cli.txt", "claudecode": "claude-code.txt"}

# Agents with a built-in long-term summary system (no external summarizer subprocess needed).
# They accept --summary-dir and --summary-model CLI args.
AGENT_LT_SUMMARY = {"crucible"}


def agent_supports_summary(agent_name: str) -> bool:
    """Return True if the given agent supports the shared summary system."""
    return agent_name in AGENT_OUTPUT_FILES or agent_name in AGENT_LT_SUMMARY


KIND_CLUSTER_PREFIX = "sregym-w"
WORKER_META_KEY_PREFIX = "__worker_meta__"
OPENEBS_PRELOAD_IMAGES = [
    "openebs/node-disk-manager:2.1.0",
    "openebs/node-disk-exporter:2.1.0",
    "openebs/node-disk-operator:2.1.0",
    "openebs/provisioner-localpv:3.4.0",
]
PRELOAD_IMAGE_ENV_VAR = "SREGYM_PRELOAD_IMAGES"
PRELOAD_IMAGE_PATTERN = re.compile(r"^\s*image:\s*['\"]?([^'\"\s]+)['\"]?\s*$", re.MULTILINE)

# Resource limits for parallel execution
# Calibrated based on container count (1 core per container).
# Social Network: ~27 containers -> 27 units
# Hotel Reservation: ~18 containers -> 18 units (approx)
# Astronomy Shop: ~14 containers -> 14 units
# Train Ticket: ~10 containers -> 10 units
# Light apps: ~5 units
#

def _resolve_progress_mode(stream, parallel_workers: int) -> str:
    """
    Determine progress rendering mode.
    Modes:
      - rich: animated rich Progress UI
      - plain: periodic plain-text summaries
      - off: no progress output
    """
    raw = os.getenv("SREGYM_PROGRESS_MODE", "auto").strip().lower()
    if raw in {"rich", "plain", "off"}:
        return raw

    # In parallel mode, default to plain output. Rich live rendering is fragile when
    # any external layer captures or rewrites terminal output.
    if parallel_workers > 1:
        return "plain"

    is_tty = hasattr(stream, "isatty") and stream.isatty()
    term = os.getenv("TERM", "").strip().lower()
    in_ci = os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}
    if not is_tty or term in {"", "dumb"} or in_ci:
        return "plain"
    return "rich"



def get_current_datetime_formatted():
    now = datetime.now()
    formatted_datetime = now.strftime("%m%d_%H%M")
    return formatted_datetime


def get_latest_log_dir():
    """Finds the most recently modified directory in the logs/ folder."""
    logs_root = os.path.abspath("logs")
    if not os.path.exists(logs_root):
        return None

    subdirs = [os.path.join(logs_root, d) for d in os.listdir(logs_root) if os.path.isdir(os.path.join(logs_root, d))]
    if not subdirs:
        return None

    # Sort by modification time
    return max(subdirs, key=os.path.getmtime)


def is_result_complete(csv_path):
    """Checks if a result CSV file contains evaluation results and is not just a header with problem_id."""
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return False

            # We expect at least one of these columns to exist to consider the result "complete"
            eval_columns = ["Diagnosis.success", "Mitigation.success", "Diagnosis.judgment", "Mitigation.judgment"]
            has_eval_column = any(col in reader.fieldnames for col in eval_columns)
            if not has_eval_column:
                return False

            # Also check if there's at least one data row and it has some values in these columns
            try:
                first_row = next(reader)
                return any(
                    first_row.get(col) is not None and first_row.get(col) != ""
                    for col in eval_columns
                    if col in reader.fieldnames
                )
            except StopIteration:
                return False
    except Exception:
        return False


def generate_sequence(problem_ids: list, n: int, seed: int) -> list:
    """Generate a deterministic sequence of n problem IDs sampled with replacement."""
    rng = random.Random(seed)
    return [rng.choice(problem_ids) for _ in range(n)]


def driver_loop(
    conductor: Conductor,
    experiment_log_dir: str,
    problem_filter: str = None,
    agent_to_run: str = None,
    use_external_harness: bool = False,
    repeat: int = 1,
    enable_summary: bool = False,
    inject_summary: bool = True,
    summary_model: str = None,
    problem_list: list = None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
    sequence: list = None,
    sequence_start_idx: int = 0,
):
    """
    Deploy each problem and wait for HTTP grading via POST /submit.
    Returns a list of flattened dicts with results per problem.

    Args:
        conductor: The Conductor instance
        experiment_log_dir: Directory to store logs and results.
        problem_filter: Optional problem ID to run. If specified, only this problem will be run.
        agent_to_run: Agent name to run (required unless use_external_harness is True).
        use_external_harness: If True, inject fault and exit without running evaluation logic.
        enable_summary: If True, pass --enable-summary to the agent.
        inject_summary: If True, pass summary to agent in prompt (default). If False, pass --no-inject-summary.
        problem_list: Optional list of problem IDs to run.
        status_dict: Shared dictionary for status updates (used in parallel mode).
        problem_queue: Optional multiprocessing.Queue to fetch problems from.
        worker_id: Optional ID of the worker process (for logging).
    """

    async def driver():
        # In parallel mode, we don't want the console to output to stdout directly
        # because it will be interleaved. We only use console for local logging
        # which will be redirected to a file.
        console = Console(force_terminal=sys.stdout.isatty()) if status_dict is None else Console(file=sys.stdout)

        # give the API a moment to bind
        await asyncio.sleep(1)

        # Verify agent exists in registry (skip if using external harness)
        if not use_external_harness:
            _default_registry = Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml"
            _registry_path = Path(os.environ.get("SREGYM_AGENT_REGISTRY", _default_registry))
            available_agents = list_agents(path=_registry_path).keys()
            if agent_to_run not in available_agents:
                console.log(f"⚠️ Agent '{agent_to_run}' not found in registry. Available agents: {available_agents}")
                sys.exit(1)

            console.log(f"Starting agent now: {agent_to_run}")
            conductor.register_agent(agent_to_run)

            # Start K8s API proxy to hide chaos engineering namespaces from the agent
            console.log("🔒 Starting Kubernetes API proxy to hide chaos namespaces...")
            conductor.start_k8s_proxy()
            LAUNCHER.set_agent_kubeconfig(conductor.get_agent_kubeconfig_path())

        all_results_for_agent = []

        def write_error_result(problem_id: str, error_message: str, sequence_index: int = None):
            """Write a structured result row even when execution fails before grading."""
            if not agent_to_run:
                return
            current_date_time = get_current_datetime_formatted()
            if sequence_index is not None:
                csv_path = os.path.join(
                    experiment_log_dir,
                    f"{current_date_time}_{sequence_index:05d}_{problem_id}_{agent_to_run}_results.csv",
                )
            else:
                csv_path = os.path.join(
                    experiment_log_dir, f"{current_date_time}_{problem_id}_{agent_to_run}_results.csv"
                )
            snapshot = {
                "problem_id": problem_id,
                "run_status": "Error",
                "error": str(error_message),
            }
            if sequence_index is not None:
                snapshot["sequence_index"] = sequence_index
            with open(csv_path, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted(snapshot.keys()), quoting=csv.QUOTE_NONNUMERIC)
                writer.writeheader()
                writer.writerow(snapshot)
            logger.info(f"❌ Problem {problem_id} for agent {agent_to_run} failed. Error result written to {csv_path}")

        # session_timestamp = get_current_datetime_formatted()

        if sequence is not None:
            # Sequence mode: iterate over pre-generated sequence with indices
            def sequence_gen():
                for seq_idx, pid in enumerate(sequence):
                    if seq_idx < sequence_start_idx:
                        continue
                    yield seq_idx, pid

            problem_iterator = sequence_gen()
            use_sequence_mode = True
        elif problem_queue:

            def problem_gen():
                # Yield pre-assigned problems first (e.g. popped by worker to check for work)
                if problem_list:
                    for p in problem_list:
                        yield p

                while True:
                    try:
                        # Blocking get() allows the main loop to wait for the scheduler
                        # to assign tasks via the queue.
                        task = problem_queue.get()
                        if task is None:  # Sentinel to stop
                            return
                        yield task
                    except queue.Empty:
                        return

            problem_iterator = problem_gen()
            use_sequence_mode = False
        else:
            # Get all problem IDs and filter if needed
            problem_ids = conductor.problems.get_problem_ids()

            all_problem_ids = conductor.problems.get_problem_ids(all=True)
            if problem_filter:
                if problem_filter not in all_problem_ids:
                    console.log(
                        f"⚠️  Problem '{problem_filter}' not found in registry. Available problems: {problem_ids}"
                    )
                    sys.exit(1)
                problem_ids = [problem_filter]
                console.log(f"🎯 Running single problem: {problem_filter}")
            elif problem_list:
                # Filter to intersection of available and requested
                problem_ids = [p for p in problem_ids if p in problem_list]
                console.log(f"🎯 Running {len(problem_ids)} problems from list")

            # sanity check: are there any specified problem ids that do not exist in the registry?
            unknown_problem_ids = set(problem_ids) - set(all_problem_ids)
            if unknown_problem_ids:
                console.log(
                    f"⚠️  These problem ids do not exist in the registry and they will be skipped: {unknown_problem_ids}"
                )
            for unknown_problem_id in unknown_problem_ids:
                problem_ids.remove(unknown_problem_id)

            problem_iterator = problem_ids
            use_sequence_mode = False

        for item in problem_iterator:
            if use_sequence_mode:
                seq_idx, pid = item
            else:
                seq_idx, pid = None, item

            # Unique key for this sequence slot (disambiguates repeated pids)
            seq_key = f"{seq_idx:05d}:{pid}" if seq_idx is not None else pid

            # Check for existing results (Resume capability)
            # We look for any timestamped file matching the pattern *_{pid}_{agent_to_run}_results.csv
            # Only checking if agent_to_run is specified (not external harness)
            completed_iterations = 0
            if agent_to_run and not use_external_harness:
                if seq_idx is not None:
                    # Sequence mode: match by seq_idx to disambiguate repeated problems
                    search_pattern = os.path.join(
                        experiment_log_dir, f"*_{seq_idx:05d}_{pid}_{agent_to_run}_results.csv"
                    )
                else:
                    search_pattern = os.path.join(experiment_log_dir, f"*_{pid}_{agent_to_run}_results.csv")
                existing_files = glob.glob(search_pattern)

                for f_path in existing_files:
                    if is_result_complete(f_path):
                        completed_iterations += 1

                if completed_iterations >= repeat:
                    label = f"[{seq_idx:05d}] {pid}" if seq_idx is not None else pid
                    console.log(
                        f"⏭️  Skipping problem '{label}': Found {completed_iterations}/{repeat} completed results."
                    )

                    if status_dict is not None:
                        status_dict[seq_key] = {
                            "status": "Completed (Resumed)",
                            "pid": pid,
                            "start_time": time.time(),
                            "elapsed": 0.0,
                            "worker_id": worker_id,
                        }
                    continue
                elif completed_iterations > 0:
                    label = f"[{seq_idx:05d}] {pid}" if seq_idx is not None else pid
                    console.log(
                        f"⏯️  Resuming problem '{label}': {completed_iterations}/{repeat} iterations already completed."
                    )

            # Prepare for logging redirection if in parallel mode
            redirect_ctx = (
                open(os.path.join(experiment_log_dir, f"{pid}.log"), "w") if status_dict is not None else None
            )
            original_stdout = sys.stdout
            original_stderr = sys.stderr

            if status_dict is not None:
                sys.stdout = redirect_ctx
                sys.stderr = redirect_ctx

                # Redirect logging handler to the file so logs don't go to the original stderr (which might be console or worker log)
                root_logger = logging.getLogger("all")
                for handler in root_logger.handlers:
                    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                        handler.setStream(redirect_ctx)

                # Update status to starting
                status_dict[seq_key] = {
                    "status": "Deploying App",
                    "pid": pid,
                    "start_time": time.time(),
                    "elapsed": 0.0,
                    "worker_id": worker_id,
                }

            try:
                for iteration in range(completed_iterations, repeat):
                    console.log(f"\n🔍 Starting problem: {pid} (Run {iteration + 1}/{repeat})")

                    conductor.problem_id = pid

                    # Define callback to update status from conductor
                    def update_conductor_status(status, _seq_key=seq_key, _pid=pid):
                        if status_dict is not None:
                            # Preserve start_time if it exists, otherwise use current time
                            current_info = status_dict.get(_seq_key, {})
                            start_time = current_info.get("start_time", time.time())
                            status_dict[_seq_key] = {
                                "status": status,
                                "pid": _pid,
                                "start_time": start_time,
                                "elapsed": time.time() - start_time,
                                "worker_id": worker_id,
                            }

                    conductor.set_status_callback(update_conductor_status)

                    result = await conductor.start_problem()
                    if result == StartProblemResult.SKIPPED_KHAOS_REQUIRED:
                        console.log(f"⏭️  Skipping problem '{pid}': requires Khaos but running on emulated cluster")
                        if status_dict is not None:
                            status_dict[seq_key] = {
                                "status": "Skipped (Khaos Req)",
                                "pid": pid,
                                "start_time": status_dict[seq_key]["start_time"],
                                "elapsed": time.time() - status_dict[seq_key]["start_time"],
                                "worker_id": worker_id,
                            }
                        continue

                    # If using external harness, fault is injected - exit now
                    if use_external_harness:
                        console.log(f"✅ Fault injected for problem '{pid}'. Exiting for external harness.")
                        return []

                    # Define agent log directory
                    # Use a unique directory per problem to avoid race conditions on instruction.txt/output files
                    agent_base_dir = os.path.join(experiment_log_dir, agent_to_run)
                    agent_log_dir = os.path.join(agent_base_dir, conductor.problem_id)

                    if not use_external_harness:
                        if status_dict is not None:
                            status_dict[seq_key] = {
                                "status": "Agent Running",
                                "pid": pid,
                                "start_time": status_dict[seq_key]["start_time"],
                                "elapsed": time.time() - status_dict[seq_key]["start_time"],
                                "worker_id": worker_id,
                            }

                        # Defensive: ensure no stale agent from previous problem before starting
                        LAUNCHER.cleanup_agent(agent_to_run)
                        _default_registry = Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml"
                        _registry_path = Path(os.environ.get("SREGYM_AGENT_REGISTRY", _default_registry))
                        reg = get_agent(agent_to_run, path=_registry_path)
                        if reg:
                            extra_args = ""
                            # Pass explicit log dir to external-summarizer agents (e.g. gemini_cli)
                            if agent_to_run in AGENT_OUTPUT_FILES:
                                extra_args += f" --logs-dir {agent_log_dir} --summary-dir {agent_base_dir}"

                            # Crucible handles summarization internally via --summary-dir
                            if agent_to_run in AGENT_LT_SUMMARY:
                                extra_args += f" --logs-dir {agent_log_dir}"
                            if agent_to_run in AGENT_LT_SUMMARY and enable_summary:
                                effective_summary_model = summary_model or os.environ.get("MODEL_ID", "gpt-4o")
                                extra_args += (
                                    f" --summary-dir {agent_base_dir} --summary-model {effective_summary_model}"
                                )

                            if enable_summary and agent_to_run in AGENT_OUTPUT_FILES:
                                extra_args += " --enable-summary"
                            if not inject_summary:
                                extra_args += " --no-inject-summary"

                            await LAUNCHER.ensure_started(reg, extra_args=extra_args.strip())

                    # Poll until grading completes or agent exits
                    agent_exit_code: int | None = None
                    while conductor.submission_stage != "done":
                        if status_dict is not None:
                            # Update stage
                            current_stage = conductor.submission_stage or "Running"
                            status_dict[seq_key] = {
                                "status": f"Agent: {current_stage}",
                                "pid": pid,
                                "start_time": status_dict[seq_key]["start_time"],
                                "elapsed": time.time() - status_dict[seq_key]["start_time"],
                                "worker_id": worker_id,
                            }

                        # Check if agent process has exited
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            agent_proc.proc.poll()
                            if agent_proc.proc.returncode is not None:
                                agent_exit_code = agent_proc.proc.returncode
                                console.log(f"⚠️  Agent process exited with return code {agent_exit_code}")
                                break
                        await asyncio.sleep(1)

                    if status_dict is not None:
                        status_dict[seq_key] = {
                            "status": "Cleaning Up",
                            "pid": pid,
                            "start_time": status_dict[seq_key]["start_time"],
                            "elapsed": time.time() - status_dict[seq_key]["start_time"],
                            "worker_id": worker_id,
                        }

                    console.log(f"✅ Completed {pid}: results={conductor.results}")

                    # Wait for agent process to complete naturally before cleanup
                    # This allows the agent to finish saving trajectories and other cleanup tasks
                    if not use_external_harness:
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            console.log("⏳ Waiting for agent process to complete...")
                            timeout = 30  # seconds
                            elapsed = 0
                            while elapsed < timeout:
                                agent_proc.proc.poll()
                                if agent_proc.proc.returncode is not None:
                                    console.log(
                                        f"✅ Agent process completed with return code {agent_proc.proc.returncode}"
                                    )
                                    break
                                await asyncio.sleep(1)
                                elapsed += 1
                            else:
                                console.log(f"⚠️  Agent process did not complete within {timeout}s, will force cleanup")

                    snapshot = {"problem_id": pid}
                    if seq_idx is not None:
                        snapshot["sequence_index"] = seq_idx
                    for stage, outcome in conductor.results.items():
                        if isinstance(outcome, dict):
                            for k, v in outcome.items():
                                snapshot[f"{stage}.{k}"] = v
                        else:
                            snapshot[stage] = outcome
                    if agent_exit_code is not None and agent_exit_code != 0:
                        snapshot["agent_error"] = True
                        snapshot["agent_exit_code"] = agent_exit_code
                    all_results_for_agent.append(snapshot)

                    fieldnames = sorted(snapshot.keys())
                    current_date_time = get_current_datetime_formatted()

                    # Write results to experiment_log_dir
                    if seq_idx is not None:
                        csv_path = os.path.join(
                            experiment_log_dir,
                            f"{current_date_time}_{seq_idx:05d}_{pid}_{agent_to_run}_results.csv",
                        )
                    else:
                        csv_path = os.path.join(
                            experiment_log_dir, f"{current_date_time}_{pid}_{agent_to_run}_results.csv"
                        )
                    with open(csv_path, "w", newline="") as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
                        writer.writeheader()
                        writer.writerows([snapshot])
                    if snapshot.get("agent_error"):
                        logger.warning(
                            f"⚠️  Problem {pid} for agent {agent_to_run} finished with agent crash "
                            f"(exit {agent_exit_code})! Results written to {csv_path}"
                        )
                    else:
                        logger.info(
                            f"✅ Problem {pid} for agent {agent_to_run} complete! Results written to {csv_path}"
                        )

                    # Cleanup agent process so a fresh one can be started for the next problem
                    if not use_external_harness:
                        LAUNCHER.cleanup_agent(agent_to_run)
                        console.log(f"🧹 Cleaned up agent process for {agent_to_run}")

                        # Run summarization if enabled.
                        # Agents in AGENT_LT_SUMMARY handle this themselves inside the driver.
                        if enable_summary and agent_to_run in AGENT_OUTPUT_FILES:
                            output_filename = AGENT_OUTPUT_FILES[agent_to_run]
                            effective_summary_model = summary_model or os.environ.get("MODEL_ID", "gemini-2.5-flash")
                            console.log(f"📝 Running external summarization for {agent_to_run}...")
                            try:
                                summarize_cmd = [
                                    sys.executable,
                                    "-m",
                                    "clients.common.summarize",
                                    "--logs-dir",
                                    agent_log_dir,
                                    "--summary-dir",
                                    agent_base_dir,
                                    "--model",
                                    effective_summary_model,
                                    "--output-filename",
                                    output_filename,
                                ]
                                result = subprocess.run(summarize_cmd, capture_output=True, text=True)
                                if result.returncode == 0:
                                    console.log("✅ External summarization step completed.")
                                else:
                                    console.log(f"⚠️ External summarization failed (exit code {result.returncode}):")
                                    console.log(result.stderr)
                            except Exception as e:
                                console.log(f"⚠️ External summarization failed to launch: {e}")

            except Exception as e:
                console.log(f"❌ Error running problem {pid}: {e}")
                if not use_external_harness:
                    write_error_result(pid, str(e), sequence_index=seq_idx)
                if status_dict is not None:
                    status_dict[seq_key] = {
                        "status": "Error",
                        "pid": pid,
                        "start_time": status_dict[seq_key]["start_time"],
                        "elapsed": time.time() - status_dict[seq_key]["start_time"],
                        "worker_id": worker_id,
                    }
                # Do not raise e; continue to next problem
            finally:
                # Ensure agent is cleaned up even if an error occurred
                if not use_external_harness:
                    LAUNCHER.cleanup_agent(agent_to_run)
                    await asyncio.sleep(1)  # Allow process group to fully tear down

                if status_dict is not None:
                    if status_dict[seq_key]["status"] != "Error":
                        status_dict[seq_key] = {
                            "status": "Completed",
                            "pid": pid,
                            "start_time": status_dict[seq_key]["start_time"],
                            "elapsed": time.time() - status_dict[seq_key]["start_time"],
                            "worker_id": worker_id,
                        }
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr

                    # Restore logging handler
                    root_logger = logging.getLogger("all")
                    for handler in root_logger.handlers:
                        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                            handler.setStream(original_stderr)

                    if redirect_ctx:
                        redirect_ctx.close()

        # Stop K8s API proxy when all problems are done
        if not use_external_harness:
            console.log("🔓 Stopping Kubernetes API proxy...")
            conductor.stop_k8s_proxy()

        return [{agent_to_run: all_results_for_agent}]

    return asyncio.run(driver())


def start_mcp_server_after_api():
    # Small delay so the main API binds first (avoid port races if clients hit MCP immediately)
    time.sleep(1.0)

    host = "0.0.0.0" if mcp_server_cfg.expose_server else "127.0.0.1"
    port = int(os.getenv("MCP_SERVER_PORT", mcp_server_cfg.mcp_server_port))

    config = uvicorn.Config(
        app=mcp_app,
        host=host,
        port=port,
        log_level="info",
    )
    # IMPORTANT: we're not in the main thread
    config.install_signal_handlers = False

    server = uvicorn.Server(config)
    # This call blocks *this* thread; it's fine because we're daemonizing the thread
    try:
        logger.info(f"Starting MCP server on {host}:{port}")
        server.run()
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        raise e


def _run_driver_and_shutdown(
    conductor: Conductor,
    experiment_log_dir: str,
    problem_filter: str = None,
    agent_to_run: str = None,
    use_external_harness: bool = False,
    repeat: int = 1,
    enable_summary: bool = False,
    inject_summary: bool = True,
    summary_model: str = None,
    problem_list: list = None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
    sequence: list = None,
    sequence_start_idx: int = 0,
):
    """Run the benchmark driver, stash results, then tell the API to exit."""
    try:
        results = driver_loop(
            conductor,
            experiment_log_dir,
            problem_filter=problem_filter,
            agent_to_run=agent_to_run,
            use_external_harness=use_external_harness,
            repeat=repeat,
            enable_summary=enable_summary,
            inject_summary=inject_summary,
            summary_model=summary_model,
            problem_list=problem_list,
            status_dict=status_dict,
            problem_queue=problem_queue,
            worker_id=worker_id,
            sequence=sequence,
            sequence_start_idx=sequence_start_idx,
        )
        main.results = results
    except Exception as e:
        logger.error(f"Driver loop crashed: {e}")
    finally:
        # ⬇️ Ask the API server (running in main thread) to stop so we can write CSV
        request_shutdown()


def _worker_kind_config_path() -> str:
    """Choose the kind config file based on host architecture."""
    arch = platform.machine().lower()
    config_name = "kind-config-arm.yaml" if ("arm" in arch or "aarch" in arch) else "kind-config-x86.yaml"
    return os.path.abspath(os.path.join("kind", config_name))


def _should_preload_infra_images() -> bool:
    return os.getenv("SREGYM_PRELOAD_INFRA_IMAGES", "1").strip().lower() not in {"0", "false", "no"}


def _extract_images_from_text(text: str) -> set[str]:
    images: set[str] = set()
    for match in PRELOAD_IMAGE_PATTERN.findall(text):
        image = match.strip()
        if not image or "{{" in image or "}}" in image:
            continue
        images.add(image)
    return images


def _iter_yaml_files(root_path: Path):
    if not root_path.exists():
        return
    for suffix in ("*.yaml", "*.yml"):
        for file_path in root_path.rglob(suffix):
            if not file_path.is_file():
                continue
            yield file_path


def _collect_images_from_yaml_path(path: Path) -> set[str]:
    images: set[str] = set()
    if not path.exists():
        return images

    if path.is_file():
        candidates = [path]
    else:
        candidates = list(_iter_yaml_files(path))

    for file_path in candidates:
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        images.update(_extract_images_from_text(text))
    return images


def _collect_images_from_helm_chart(chart_path: Path) -> set[str]:
    if not chart_path.exists():
        return set()

    rendered = subprocess.run(
        ["helm", "template", "sregym-preload-scan", str(chart_path), "--include-crds"],
        check=False,
        capture_output=True,
        text=True,
    )
    if rendered.returncode == 0 and rendered.stdout:
        return _extract_images_from_text(rendered.stdout)

    logger.warning(f"Helm template failed for preload scan ({chart_path}), falling back to static YAML scan.")
    return _collect_images_from_yaml_path(chart_path)


def _discover_benchmark_images() -> list[str]:
    metadata_root = Path("sregym/service/metadata")
    benchmark_images: set[str] = set()

    if metadata_root.exists():
        for metadata_file in metadata_root.glob("*.json"):
            try:
                metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue

            helm_cfg = metadata.get("Helm Config") or {}
            chart_path = helm_cfg.get("chart_path")
            if chart_path and not helm_cfg.get("remote_chart", False):
                local_chart_path = Path("SREGym-applications") / chart_path
                benchmark_images.update(_collect_images_from_helm_chart(local_chart_path))

            for key in ("K8S Deploy Path", "K8S Workload Job Path"):
                deploy_path = metadata.get(key)
                if deploy_path:
                    local_path = Path("SREGym-applications") / deploy_path
                    benchmark_images.update(_collect_images_from_yaml_path(local_path))

    # Infra resources used by multiple problems.
    benchmark_images.update(_collect_images_from_yaml_path(Path("sregym/service/khaos.yaml")))
    benchmark_images.update(_collect_images_from_yaml_path(Path("sregym/observer/prometheus")))

    return sorted(benchmark_images)


def _get_preload_images() -> list[str]:
    override = os.getenv(PRELOAD_IMAGE_ENV_VAR, "").strip()
    if not override:
        discovered = _discover_benchmark_images()
        # Persist the resolved list so worker processes reuse the exact same image set.
        images = sorted(set(OPENEBS_PRELOAD_IMAGES).union(discovered))
        os.environ[PRELOAD_IMAGE_ENV_VAR] = ",".join(images)
        return images
    images = [img.strip() for img in override.split(",") if img.strip()]
    return images if images else OPENEBS_PRELOAD_IMAGES


def _docker_image_exists(image: str) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.returncode == 0


def _docker_pull_with_retries(image: str) -> bool:
    retries = int(os.getenv("SREGYM_IMAGE_PULL_RETRIES", "4"))
    backoff = int(os.getenv("SREGYM_IMAGE_PULL_BACKOFF_SECONDS", "5"))

    for attempt in range(1, retries + 1):
        logger.info(f"Pulling image ({attempt}/{retries}): {image}")
        result = subprocess.run(
            ["docker", "pull", image],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        if attempt < retries:
            time.sleep(backoff * attempt)

    logger.warning(f"Failed to pre-pull image: {image}")
    return False


def _prefetch_infra_images_once() -> None:
    if not _should_preload_infra_images():
        logger.info("Infra image preloading disabled via SREGYM_PRELOAD_INFRA_IMAGES.")
        return

    images = _get_preload_images()
    if not images:
        return
    logger.info(f"Preloading benchmark images on host: {len(images)} image(s).")

    # Cross-process lock to ensure only one process pulls shared images.
    lock_path = os.path.join(tempfile.gettempdir(), "sregym-image-prefetch.lock")
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        for image in images:
            if _docker_image_exists(image):
                logger.info(f"Image already cached: {image}")
                continue
            _docker_pull_with_retries(image)


def _load_preloaded_images_into_cluster(cluster_name: str) -> None:
    if not _should_preload_infra_images():
        return

    images = _get_preload_images()
    logger.info(f"Loading pre-pulled benchmark images into cluster {cluster_name}: {len(images)} image(s).")
    for image in images:
        if not _docker_image_exists(image):
            logger.warning(f"Skipping kind load for missing local image: {image}")
            continue
        try:
            subprocess.run(
                ["kind", "load", "docker-image", "--name", cluster_name, image],
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info(f"Loaded cached image into {cluster_name}: {image}")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to load image into {cluster_name}: {image} ({e})")
            logger.warning(f"Stdout: {e.stdout}")
            logger.warning(f"Stderr: {e.stderr}")
            # Continue loading other images; do not fail the cluster setup
            pass


def _build_kind_config_with_registry_auth(base_config_path: str, docker_user: str, docker_password: str) -> str:
    """Return path to a temp kind config with Docker Hub auth injected into containerdConfigPatches."""
    import tempfile

    import yaml

    with open(base_config_path) as f:
        config = yaml.safe_load(f)

    auth_patch = (
        '[plugins."io.containerd.grpc.v1.cri".registry.configs."registry-1.docker.io".auth]\n'
        f'  username = "{docker_user}"\n'
        f'  password = "{docker_password}"\n'
    )
    config.setdefault("containerdConfigPatches", [])
    config["containerdConfigPatches"].append(auth_patch)

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.dump(config, tmp)
    tmp.flush()
    return tmp.name


def _create_worker_cluster(worker_id: int, experiment_log_dir: str) -> tuple[str, str]:
    """Create a dedicated kind cluster for one worker and return (cluster_name, kubeconfig_path)."""
    cluster_name = f"{KIND_CLUSTER_PREFIX}{worker_id}"
    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    config_path = _worker_kind_config_path()

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Kind config file not found: {config_path}")

    docker_user = os.environ.get("DOCKER_USERNAME")
    docker_password = os.environ.get("DOCKER_PASSWORD")
    patched_config_path = None
    if docker_user and docker_password:
        patched_config_path = _build_kind_config_with_registry_auth(config_path, docker_user, docker_password)
        config_path = patched_config_path
        logger.info("Docker Hub credentials will be injected into containerd on all kind nodes.")
    else:
        logger.warning(
            "DOCKER_USERNAME/DOCKER_PASSWORD not set. Kind nodes will pull Docker Hub images unauthenticated."
        )

    logger.info(f"Preparing isolated kind cluster for worker {worker_id}: {cluster_name}")

    # Force cleanup of any lingering docker containers for this worker
    # kind delete cluster sometimes misses these if the cluster creation was interrupted
    try:
        # distinct name filter to avoid deleting other workers' containers (e.g. w1 vs w10)
        # Using name=^cluster_name- ensures we target only this cluster's nodes
        cmd = ["docker", "ps", "-a", "-q", "--filter", f"name=^{cluster_name}-"]
        container_ids = subprocess.check_output(cmd, text=True).strip().split()
        if container_ids:
            logger.info(f"Force removing lingering containers for {cluster_name}: {container_ids}")
            subprocess.run(
                ["docker", "rm", "-f"] + container_ids,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception as e:
        logger.warning(f"Failed to force cleanup containers for {cluster_name}: {e}")

    # Best-effort cleanup in case a previous run crashed and left this worker cluster behind.
    subprocess.run(
        ["kind", "delete", "cluster", "--name", cluster_name],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    subprocess.run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            cluster_name,
            "--config",
            config_path,
            "--kubeconfig",
            kubeconfig_path,
            "--wait",
            "180s",
        ],
        check=True,
    )
    if patched_config_path and os.path.exists(patched_config_path):
        os.unlink(patched_config_path)
    _load_preloaded_images_into_cluster(cluster_name)

    os.environ["KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_KIND_CLUSTER_NAME"] = cluster_name

    # Validate that the worker kubeconfig is immediately usable.
    subprocess.run(
        ["kubectl", "config", "current-context", "--kubeconfig", kubeconfig_path],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    logger.info(f"Worker {worker_id} cluster ready: {cluster_name}, kubeconfig={kubeconfig_path}")
    return cluster_name, kubeconfig_path


def _delete_worker_cluster(cluster_name: str) -> None:
    """Delete a worker's dedicated kind cluster."""
    if not cluster_name:
        return
    logger.info(f"Tearing down worker kind cluster: {cluster_name}")
    subprocess.run(
        ["kind", "delete", "cluster", "--name", cluster_name],
        check=False,
    )


def _worker_meta_key(worker_id: int) -> str:
    return f"{WORKER_META_KEY_PREFIX}{worker_id}"


def _kill_process_tree(pid: int, sig: int = signal.SIGTERM) -> None:
    """Kill a process and all its descendants. Ensures worker subprocesses (agents, kind, kubectl) are terminated."""
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    for child in proc.children(recursive=True):
        try:
            child.send_signal(sig)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    try:
        proc.send_signal(sig)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass


def worker_main(args, worker_id, problem_queue, experiment_log_dir, status_dict, sequence=None, sequence_start_idx=0):
    """Worker function for parallel execution."""

    def _shutdown_handler(signum, frame):
        """On SIGTERM/SIGINT, clean up agent subprocesses before exiting."""
        LAUNCHER.cleanup_all_agents(timeout=3)
        os._exit(0)

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    os.environ["SREGYM_WORKER_ID"] = str(worker_id)
    os.environ["API_PORT"] = str(8000 + worker_id)
    os.environ["MCP_SERVER_PORT"] = str(9000 + worker_id)
    _sregym_dir = os.path.dirname(os.path.abspath(__file__))
    os.environ["SREGYM_EXP_ENV"] = os.path.join(_sregym_dir, "exp_env", f"exp_env_{worker_id}")

    # Append worker ID to log file to avoid conflicts
    session_timestamp = get_current_datetime_formatted()
    os.environ["SREGYM_LOG_FILE"] = os.path.join(experiment_log_dir, f"sregym_{session_timestamp}_w{worker_id}.log")

    # Reset logging handlers to avoid writing to the supervisor's log (inherited via fork)
    root_logger = logging.getLogger("all")
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    # Re-initialize logger with the new SREGYM_LOG_FILE
    init_logger()

    # In parallel mode, redirect all output to a worker log file to prevent console interleaving
    worker_log_path = os.path.join(experiment_log_dir, f"worker_{worker_id}.log")

    # Use os.dup2 to redirect ALL output (stdout/stderr) to the file descriptor of the log file.
    # This captures output from subprocesses (like kubectl) and C libraries that would otherwise
    # bypass sys.stdout and print to the terminal, causing mangled output in the parallel view.
    with open(worker_log_path, "w") as f:
        # Flush python buffers before redirecting
        sys.stdout.flush()
        sys.stderr.flush()

        # Redirect FD 1 (stdout) and FD 2 (stderr) to the file
        os.dup2(f.fileno(), 1)
        os.dup2(f.fileno(), 2)

        cluster_name = ""
        try:
            # Note: We do NOT check for work here anymore. We want the worker to start up,
            # create the cluster, and then wait for tasks from the scheduler.

            status_dict[_worker_meta_key(worker_id)] = {
                "status": "Creating cluster",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            }
            cluster_name, _ = _create_worker_cluster(worker_id, experiment_log_dir)
            status_dict[_worker_meta_key(worker_id)] = {
                "status": f"Cluster ready ({cluster_name})",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            }
            # Run main with the private queue. It will block until tasks arrive.
            main(
                args,
                problem_queue=problem_queue if sequence is None else None,
                problem_list=None,  # No pre-assigned list, everything via queue
                experiment_log_dir=experiment_log_dir,
                status_dict=status_dict,
                worker_id=worker_id,
                sequence=sequence,
                sequence_start_idx=sequence_start_idx,
            )
        except Exception as e:
            status_dict[_worker_meta_key(worker_id)] = {
                "status": f"Worker setup failed: {e}",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            }
            raise
        finally:
            _delete_worker_cluster(cluster_name)


def run_parallel(args):
    """Split problems and run in parallel workers."""

    from sregym.conductor.problems.registry import ProblemRegistry

    registry = ProblemRegistry()
    # Use the same logic as conductor to get problem IDs
    # If args.problem is set, run only that (but parallel doesn't make much sense unless repeat > 1)
    if args.problem:
        all_problems = [args.problem]
    else:
        all_problems = registry.get_problem_ids()
        # tasklist.yml may contain stale problem IDs; filter early to avoid runtime failures in workers
        all_problem_ids = set(registry.get_problem_ids(all=True))
        unknown_problem_ids = sorted(set(all_problems) - all_problem_ids)
        if unknown_problem_ids:
            logger.warning(f"These problem IDs are not in the registry and will be skipped: {unknown_problem_ids}")
            all_problems = [pid for pid in all_problems if pid in all_problem_ids]

    if not all_problems:
        logger.error("No problems found to run.")
        sys.exit(1)

    # Create shared experiment log directory
    if args.resume_last:
        latest = get_latest_log_dir()
        if not latest:
            logger.error("No previous log directory found to resume from.")
            sys.exit(1)
        experiment_log_dir = latest
        logger.info(f"Resuming experiment from latest: {experiment_log_dir}")
    elif args.resume_from:
        experiment_log_dir = os.path.abspath(args.resume_from)
        if not os.path.exists(experiment_log_dir):
            logger.error(f"Resume directory {experiment_log_dir} does not exist.")
            sys.exit(1)
        logger.info(f"Resuming experiment from: {experiment_log_dir}")
    else:
        session_timestamp = get_current_datetime_formatted()
        os.makedirs("logs", exist_ok=True)
        dir_name = session_timestamp
        if args.agent:
            dir_name = f"{session_timestamp}_{args.agent}"
        experiment_log_dir = os.path.abspath(f"logs/{dir_name}")
        os.makedirs(experiment_log_dir, exist_ok=True)
        logger.info(f"Parallel experiment logs will be stored in: {experiment_log_dir}")

        # Copy seed summary into agent summary dir
        if args.seed_summary:
            agent_base_dir = os.path.join(experiment_log_dir, args.agent)
            os.makedirs(agent_base_dir, exist_ok=True)
            dest_path = os.path.join(agent_base_dir, "long_term_summary.txt")
            shutil.copy2(args.seed_summary, dest_path)
            logger.info(f"Copied seed summary to {dest_path}")

        # Set log file for parallel runner
        log_file_path = os.path.join(experiment_log_dir, f"sregym_supervisor_{session_timestamp}.log")
        os.environ["SREGYM_LOG_FILE"] = log_file_path
        init_logger()

    # Handle sequence mode
    sequence = None
    sequence_start_idx = 0
    if getattr(args, "sequence_len", 0) > 0:
        sequence_state_path = os.path.join(experiment_log_dir, "sequence_state.json")
        is_resuming = args.resume_last or args.resume_from

        if is_resuming and os.path.exists(sequence_state_path):
            # Load existing sequence state
            with open(sequence_state_path) as f:
                state = json.load(f)
            stored_seed = state["seed"]
            stored_sequence = state["sequence"]

            if args.sequence_len > len(stored_sequence):
                # Extend: regenerate with same seed to new length, verify prefix
                new_sequence = generate_sequence(all_problems, args.sequence_len, stored_seed)
                if new_sequence[: len(stored_sequence)] != stored_sequence:
                    logger.error("Sequence prefix mismatch on extension — seed/problem pool changed?")
                    sys.exit(1)
                sequence = new_sequence
                with open(sequence_state_path, "w") as f:
                    json.dump({"seed": stored_seed, "sequence": sequence}, f)
                logger.info(f"Extended sequence from {len(stored_sequence)} to {args.sequence_len} problems.")
            else:
                sequence = stored_sequence
                logger.info(f"Loaded existing sequence of {len(sequence)} problems from {sequence_state_path}.")
        else:
            # New sequence run
            seed = getattr(args, "sequence_seed", 42)
            sequence = generate_sequence(all_problems, args.sequence_len, seed)
            with open(sequence_state_path, "w") as f:
                json.dump({"seed": seed, "sequence": sequence}, f)
            logger.info(f"Generated new sequence of {args.sequence_len} problems (seed={seed}).")

        # Determine start index: first position without a completed result file
        agent_to_run = args.agent
        sequence_start_idx = 0
        for idx, pid in enumerate(sequence):
            search_pattern = os.path.join(experiment_log_dir, f"*_{idx:05d}_{pid}_{agent_to_run}_results.csv")
            existing_files = glob.glob(search_pattern)
            completed = any(is_result_complete(f) for f in existing_files)
            if completed:
                sequence_start_idx = idx + 1
            else:
                break
        logger.info(f"Sequence mode: starting from index {sequence_start_idx}/{len(sequence)}.")

    manager = multiprocessing.Manager()
    status_dict = manager.dict()
    # Replace single shared queue with private queues for each worker
    worker_queues = [manager.Queue() for _ in range(args.parallel)]

    # Filter problems if resuming (non-sequence mode)
    problems_to_run = []
    if sequence is not None:
        # In sequence mode, problems_to_run is just a placeholder (not used for queue scheduling)
        problems_to_run = []
    elif args.resume_last or args.resume_from:
        agent_to_run = args.agent
        for pid in all_problems:
            completed_iterations = 0
            if agent_to_run:
                search_pattern = os.path.join(experiment_log_dir, f"*_{pid}_{agent_to_run}_results.csv")
                existing_files = glob.glob(search_pattern)
                for f_path in existing_files:
                    if is_result_complete(f_path):
                        completed_iterations += 1

            if completed_iterations < args.repeat:
                problems_to_run.append(pid)
            else:
                status_dict[pid] = {
                    "status": "Completed (Resumed)",
                    "start_time": time.time(),
                    "elapsed": 0.0,
                    "worker_id": None,
                }
    else:
        problems_to_run = all_problems

    # Do not populate queues upfront. We will schedule them dynamically.
    pending_problems = list(problems_to_run)
    # Sort pending problems to run heaviest first? Or mixed?
    # Heaviest first is usually better for packing, but we have a simple limit.
    # Let's keep original order or shuffle. Original order is fine.

    _prefetch_infra_images_once()

    processes = []
    worker_map = {}  # Map process to worker ID
    if sequence is not None:
        logger.info(
            f"Running sequence of {len(sequence)} problems (starting at {sequence_start_idx}) with {args.parallel} workers."
        )
    else:
        logger.info(f"Running {len(problems_to_run)} problems with {args.parallel} workers.")

    for i in range(args.parallel):
        # Pass the PRIVATE queue for this worker
        p = multiprocessing.Process(
            target=worker_main,
            args=(args, i, worker_queues[i], experiment_log_dir, status_dict),
            kwargs={"sequence": sequence, "sequence_start_idx": sequence_start_idx},
        )
        p.start()
        processes.append(p)
        worker_map[p] = i

    assigned_tasks = {}  # worker_id -> problem_id
    shutdown_sent = False

    # Monitoring loop
    try:
        # Redirect stdout/stderr to suppress unwanted output during Progress display
        # We keep a reference to the original stdout for the Console to use
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        # Use devnull for unwanted output
        null_out = open(os.devnull, "w")
        sys.stdout = null_out
        sys.stderr = null_out

        # Remove StreamHandler from logger to prevent interference with Rich
        # We also need to check the true root logger, as third-party libraries might attach there
        loggers_to_check = [logging.getLogger("all"), logging.getLogger()]
        removed_handlers_by_logger = []
        for logger_obj in loggers_to_check:
            removed = [h for h in logger_obj.handlers if isinstance(h, logging.StreamHandler)]
            for h in removed:
                logger_obj.removeHandler(h)
            removed_handlers_by_logger.append((logger_obj, removed))

        try:
            progress_mode = _resolve_progress_mode(original_stdout, args.parallel)
            logger.info(f"Progress output mode: {progress_mode}")

            console = Console(file=original_stdout, force_terminal=(progress_mode == "rich"))
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console,
                disable=(progress_mode != "rich"),
                transient=(progress_mode == "rich"),
            ) as progress:
                # Main overall progress
                if sequence is not None:
                    total_problems = len(sequence) - sequence_start_idx
                else:
                    total_problems = len(all_problems)
                main_task = progress.add_task("[bold green]Overall Progress", total=total_problems)

                # Worker tasks - one per worker
                worker_tasks = {}
                for i in range(args.parallel):
                    # Initial state for workers
                    t_id = progress.add_task(f"Worker {i}: Idle", total=100, visible=True)
                    worker_tasks[i] = t_id

                failed_workers_logged = set()
                last_plain_print_ts = 0.0
                last_plain_snapshot = None
                while any(p.is_alive() for p in processes) or (
                    status_dict and any(info.get("worker_id") is not None for info in status_dict.values())
                ):
                    # --- SCHEDULING LOGIC ---
                    if not shutdown_sent:
                        # 1. Update Assigned Tasks (Check completion)
                        # Make a copy of keys to modify dict
                        for wid in list(assigned_tasks.keys()):
                            pid = assigned_tasks[wid]
                            info = status_dict.get(pid)
                            if info:
                                status = info.get("status", "")
                                # Check for terminal states
                                if (
                                    status.startswith("Completed")
                                    or status.startswith("Error")
                                    or status.startswith("Skipped")
                                ):
                                    del assigned_tasks[wid]

                        # 2. Assign New Tasks to Idle Workers
                        idle_workers = []
                        for i in range(args.parallel):
                            # Worker must be running, not assigned a task, and not failed
                            if (i not in assigned_tasks) and (i not in failed_workers_logged):
                                # Verify process is alive
                                if processes[i].is_alive():
                                    idle_workers.append(i)

                        for wid in idle_workers:
                            if not pending_problems:
                                break

                            problem_to_assign = pending_problems.pop(0)
                            assigned_tasks[wid] = problem_to_assign
                            worker_queues[wid].put(problem_to_assign)

                        # 4. Check Termination
                        if sequence is not None:
                            # In sequence mode, the single worker handles its own termination.
                            # Send shutdown only when all workers have exited.
                            if not any(p.is_alive() for p in processes) and not shutdown_sent:
                                shutdown_sent = True
                        elif not pending_problems and not assigned_tasks:
                            # Done!
                            logger.info("All tasks completed or assigned. Sending shutdown signals.")
                            for q in worker_queues:
                                q.put(None)
                            shutdown_sent = True

                    # --- END SCHEDULING LOGIC ---

                    # Check for dead workers and update status
                    active_workers = set()
                    for idx in range(len(processes)):
                        p = processes[idx]
                        wid = idx  # processes list is indexed by worker_id

                        if p.is_alive():
                            active_workers.add(wid)
                        else:
                            # Worker died
                            if p.exitcode != 0 and wid not in failed_workers_logged:
                                msg = f"Worker {wid} failed with exit code {p.exitcode}. Check worker_{wid}.log for details."
                                logger.error(msg)
                                progress.console.print(f"[bold red]❌ {msg}[/bold red]")
                                failed_workers_logged.add(wid)

                            # Find problems assigned to this worker that are not terminal
                            for pid, info in status_dict.items():
                                if str(pid).startswith(WORKER_META_KEY_PREFIX):
                                    continue
                                if info.get("worker_id") == wid:
                                    status = info.get("status")
                                    if not (
                                        status.startswith("Completed")
                                        or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]
                                    ):
                                        status_dict[pid] = {
                                            "status": "Error (Worker Died)",
                                            "start_time": info["start_time"],
                                            "elapsed": time.time() - info["start_time"],
                                            "worker_id": wid,
                                        }

                            # Restart Logic: If there is still work to do, restart the worker
                            if pending_problems and not shutdown_sent:
                                logger.info(
                                    f"Restarting Worker {wid} to handle {len(pending_problems)} pending problems."
                                )
                                progress.console.print(f"[bold yellow]🔄 Restarting Worker {wid}[/bold yellow]")

                                if wid in failed_workers_logged:
                                    failed_workers_logged.remove(wid)

                                # Update status to indicate restart (helps UI)
                                meta_key = _worker_meta_key(wid)
                                status_dict[meta_key] = {
                                    "status": "Restarting...",
                                    "start_time": time.time(),
                                    "elapsed": 0.0,
                                    "worker_id": wid,
                                }

                                # Remove old process from map
                                if p in worker_map:
                                    del worker_map[p]

                                # Start new process
                                new_p = multiprocessing.Process(
                                    target=worker_main,
                                    args=(args, wid, worker_queues[wid], experiment_log_dir, status_dict),
                                    kwargs={"sequence": sequence, "sequence_start_idx": sequence_start_idx},
                                )
                                new_p.start()

                                # Update references
                                processes[idx] = new_p
                                worker_map[new_p] = wid
                                active_workers.add(wid)

                    completed_count = 0
                    error_count = 0
                    skipped_count = 0

                    # Track what each worker is doing
                    # Initialize with None
                    current_worker_status = {i: None for i in range(args.parallel)}

                    for pid, info in status_dict.items():
                        if str(pid).startswith(WORKER_META_KEY_PREFIX):
                            continue
                        status = info.get("status", "Unknown")
                        wid = info.get("worker_id")

                        # Counts for overall
                        if status.startswith("Completed"):
                            completed_count += 1
                        elif status in ["Error", "Error (Worker Died)"]:
                            error_count += 1
                        elif status == "Skipped (Khaos Req)":
                            skipped_count += 1

                        # Worker status (if active)
                        if wid is not None:
                            # Check if this is an active state
                            is_active = not (
                                status.startswith("Completed")
                                or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]
                            )
                            if is_active:
                                start_t = info.get("start_time", time.time())
                                display_pid = info.get("pid", str(pid))  # real pid from value, fallback to key
                                current_worker_status[wid] = (status, display_pid, start_t)

                    # Update main task
                    finished_count = completed_count + error_count + skipped_count
                    status_text = f"[bold green]Overall Progress[/bold green] (Completed: [green]{completed_count}[/green], Errors: [red]{error_count}[/red]"
                    if skipped_count > 0:
                        status_text += f", Skipped: [yellow]{skipped_count}[/yellow]"
                    status_text += ")"
                    progress.update(main_task, completed=finished_count, description=status_text)

                    # Update worker tasks
                    for i in range(args.parallel):
                        if i not in active_workers:
                            # Worker is dead or finished
                            progress.update(
                                worker_tasks[i], description=f"Worker {i}: [dim]Finished[/dim]", completed=100
                            )
                        elif current_worker_status[i]:
                            status, pid, start_t = current_worker_status[i]
                            elapsed = int(time.time() - start_t)

                            # Map status to approximate progress
                            completed_pct = 0
                            if status == "Deploying App":
                                completed_pct = 10
                            elif status == "Injecting Faults":
                                completed_pct = 20
                            elif status == "Agent Running":
                                completed_pct = 30
                            elif status.startswith("Agent:"):
                                if "diagnosis" in status.lower():
                                    completed_pct = 50
                                elif "mitigation" in status.lower():
                                    completed_pct = 70
                                else:
                                    completed_pct = 40

                                if "verifying" in status.lower():
                                    completed_pct += 10
                            elif status == "Cleaning Up":
                                completed_pct = 90
                            elif status.startswith("Completed") or status.startswith("Error"):
                                completed_pct = 100

                            desc = f"Worker {i}: [cyan]{escape(str(pid))}[/cyan] - {escape(str(status))} [yellow]({elapsed}s)[/yellow]"
                            progress.update(worker_tasks[i], description=desc, completed=completed_pct)
                        else:
                            # Worker is alive but idle (or between tasks)
                            meta = status_dict.get(_worker_meta_key(i))
                            if meta and meta.get("status"):
                                start_t = meta.get("start_time", time.time())
                                elapsed = int(time.time() - start_t)
                                progress.update(
                                    worker_tasks[i],
                                    description=f"Worker {i}: [blue]{escape(str(meta.get('status')))}[/blue] [yellow]({elapsed}s)[/yellow]",
                                    completed=0,
                                )
                            else:
                                progress.update(worker_tasks[i], description=f"Worker {i}: Idle", completed=0)

                    if progress_mode == "plain":
                        now_ts = time.time()
                        snapshot = (
                            finished_count,
                            completed_count,
                            error_count,
                            skipped_count,
                            len(pending_problems),
                            len(assigned_tasks),
                        )
                        if snapshot != last_plain_snapshot or now_ts - last_plain_print_ts >= 15:
                            print(
                                (
                                    f"[progress] done={finished_count}/{total_problems} "
                                    f"ok={completed_count} err={error_count} skip={skipped_count} "
                                    f"pending={len(pending_problems)} active_workers={len(assigned_tasks)}"
                                ),
                                file=original_stdout,
                                flush=True,
                            )
                            last_plain_snapshot = snapshot
                            last_plain_print_ts = now_ts

                    if not any(p.is_alive() for p in processes):
                        break

                    time.sleep(0.5)

        finally:
            # Restore stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            null_out.close()

            # Restore handlers
            for logger_obj, handlers in removed_handlers_by_logger:
                for h in handlers:
                    logger_obj.addHandler(h)

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user. Terminating workers...")

    finally:
        pass  # Nothing to restore here anymore

    logger.info("Waiting for workers to cleanup...")
    # Wait for workers to cleanup (parallel wait)
    start_wait = time.time()
    while time.time() - start_wait < 5:
        if not any(p.is_alive() for p in processes):
            break
        time.sleep(0.1)

    for p in processes:
        if p.is_alive():
            logger.warning(f"Worker {worker_map.get(p)} did not exit, killing process tree...")
            _kill_process_tree(p.pid, signal.SIGTERM)
            p.join(timeout=2)
            if p.is_alive():
                _kill_process_tree(p.pid, signal.SIGKILL)
                p.join(timeout=1)
        else:
            p.join()


def main(
    args,
    problem_list=None,
    experiment_log_dir=None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
    sequence=None,
    sequence_start_idx=0,
):
    # Generate session ID and log directory
    session_timestamp = get_current_datetime_formatted()
    # Ensure logs root exists
    os.makedirs("logs", exist_ok=True)

    if experiment_log_dir is None:
        if args.resume_last:
            latest = get_latest_log_dir()
            if not latest:
                logger.error("No previous log directory found to resume from.")
                sys.exit(1)
            experiment_log_dir = latest
            logger.info(f"Resuming experiment from latest: {experiment_log_dir}")
        elif getattr(args, "resume_from", None):
            experiment_log_dir = os.path.abspath(args.resume_from)
            if not os.path.exists(experiment_log_dir):
                logger.error(f"Resume directory {experiment_log_dir} does not exist.")
                sys.exit(1)
            logger.info(f"Resuming experiment from: {experiment_log_dir}")
        else:
            # Create experiment directory
            dir_name = session_timestamp
            if args.agent:
                dir_name = f"{session_timestamp}_{args.agent}"
            experiment_log_dir = os.path.abspath(f"logs/{dir_name}")

    os.makedirs(experiment_log_dir, exist_ok=True)

    # Set log file path for init_logger if not already set by worker
    if "SREGYM_LOG_FILE" not in os.environ:
        log_file_path = os.path.join(experiment_log_dir, f"sregym_{session_timestamp}.log")
        os.environ["SREGYM_LOG_FILE"] = log_file_path

    # set up the logger
    init_logger()
    logger.info(f"Experiment logs will be stored in: {experiment_log_dir}")

    # Initialize Noise Manager if config is provided or default config exists
    nm = None
    noise_config_path = args.noise_config
    default_noise_config = "sregym/generators/noise/noise_config.yaml"

    # Use default path if no argument provided but default file exists
    if not noise_config_path and os.path.exists(default_noise_config):
        noise_config_path = default_noise_config

    if noise_config_path:
        try:
            from sregym.generators.noise.manager import get_noise_manager

            nm = get_noise_manager()
            nm.load_config(noise_config_path)
            logger.info(f"✅ Noise manager initialized with config: {noise_config_path}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize noise manager: {e}")

    os.environ["MODEL_ID"] = args.model
    if getattr(args, "judge_model", None):
        os.environ["JUDGE_MODEL_ID"] = args.judge_model

    # Enforce explicit kubeconfig selection for every process and worker.
    base_kubeconfig = require_kubeconfig_path()
    os.environ["KUBECONFIG"] = base_kubeconfig
    os.environ["SREGYM_BASE_KUBECONFIG"] = base_kubeconfig

    conductor = Conductor()

    # Start the driver in the background; it will call request_shutdown() when finished
    driver_thread = threading.Thread(
        target=_run_driver_and_shutdown,
        kwargs=dict(
            conductor=conductor,
            experiment_log_dir=experiment_log_dir,
            problem_filter=args.problem,
            agent_to_run=args.agent,
            use_external_harness=args.use_external_harness,
            repeat=args.repeat,
            enable_summary=args.enable_summary,
            inject_summary=not args.no_inject_summary,
            summary_model=getattr(args, "summary_model", None),
            problem_list=problem_list,
            status_dict=status_dict,
            problem_queue=problem_queue,
            worker_id=worker_id,
            sequence=sequence,
            sequence_start_idx=sequence_start_idx,
        ),
        name="driver",
        daemon=True,
    )
    driver_thread.start()

    # Start the MCP server in the background (lets the main thread run the Conductor API)
    if not args.use_external_harness:  # No need for MCP if using external harness
        mcp_thread = threading.Thread(
            target=start_mcp_server_after_api,
            name="mcp-server",
            daemon=True,
        )
        mcp_thread.start()

    # Start the Conductor HTTP API in the MAIN thread (blocking)
    join_driver = True
    try:
        run_api(conductor)
    except KeyboardInterrupt:
        # If interrupted, still try to shut down cleanly but quickly
        logger.info("\n🛑 Interrupted by user. Exiting immediately...")
        request_shutdown()
        join_driver = False
    finally:
        # Stop noise manager if it was initialized
        if nm:
            try:
                logger.info("Stopping noise manager...")
                nm.stop()
            except Exception as e:
                logger.error(f"⚠️ Error stopping noise manager: {e}")

        # Give driver a moment to finish setting results, unless interrupted
        if join_driver:
            driver_thread.join(timeout=5)

    # When API shuts down, collect results from driver
    results = getattr(main, "results", [])

    if not results:
        logger.warning("⚠️ No results to write.")

    if __name__ == "__main__":
        # separate run, use exit
        sys.exit(0)
    else:
        # function call run, return results
        return results


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run SREGym benchmark suite")
    parser.add_argument(
        "--problem",
        type=str,
        default=None,
        help="Run only a specific problem by its ID (e.g., 'target_port')",
    )
    parser.add_argument(
        "--agent",
        type=str,
        default=None,
        help="Agent to run by its name (e.g., 'stratus')",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o",
        help="Run only a specific model backend (e.g., 'gpt-4o', 'gemini-2.5-pro', 'claude-sonnet-4', 'moonshot', 'vertex-ai-gemini-1.5-pro')",
    )
    parser.add_argument(
        "--use-external-harness", action="store_true", help="For use in external harnesses, deploy the fault and exit."
    )
    parser.add_argument(
        "--noise-config",
        type=str,
        default=None,
        help="Path to noise configuration YAML file",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to repeat each problem",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel workers to run",
    )
    parser.add_argument(
        "--enable-summary",
        action="store_true",
        help="Enable summarization of results using an LLM",
    )
    parser.add_argument(
        "--no-inject-summary",
        action="store_true",
        help="Build summaries but do not pass them to the agent",
    )
    parser.add_argument(
        "--summary-model",
        type=str,
        default=None,
        help="Model ID for summarization LLM (default: same as --model / MODEL_ID). "
        "Useful to use a cheaper model for summarization, e.g. gemini-2.5-flash.",
    )
    parser.add_argument(
        "--judge-model",
        type=str,
        default=None,
        help="Model ID for the LLM-as-a-judge (default: same as --model / MODEL_ID). "
        "Useful to use a different model for evaluation, e.g. 'gpt-4o'.",
    )
    parser.add_argument(
        "--resume-from",
        type=str,
        default=None,
        help="Resume experiment from an existing log directory (skips completed problems)",
    )
    parser.add_argument(
        "--resume-last",
        action="store_true",
        help="Resume experiment from the most recent log directory",
    )
    parser.add_argument(
        "--seed-summary",
        type=str,
        default=None,
        metavar="PATH",
        help="Initial summary file (copied to summary dir before first run). Works with gemini_cli and claudecode.",
    )
    parser.add_argument(
        "--sequence-len",
        type=int,
        default=0,
        help="Sequence mode: run N randomly sampled problems in order (0=disabled)",
    )
    parser.add_argument(
        "--sequence-seed",
        type=int,
        default=42,
        help="Random seed for deterministic sequence generation (default: 42)",
    )
    args = parser.parse_args()

    # Validate that --agent is provided when not using external harness
    if not args.use_external_harness and args.agent is None:
        parser.error("--agent is required when --use-external-harness is not set")

    # Validate sequence mode constraints
    if args.sequence_len > 0 and args.parallel > 1:
        parser.error("--sequence-len requires --parallel 1 (sequential execution)")
    if args.sequence_len > 0 and args.problem:
        parser.error("--sequence-len and --problem are mutually exclusive")

    # Validate --seed-summary
    if args.seed_summary:
        if args.resume_last or args.resume_from:
            parser.error("--seed-summary cannot be combined with --resume-last or --resume-from")
        if not agent_supports_summary(args.agent):
            parser.error(
                f"--seed-summary can only be used with agents that support summaries: {sorted(set(AGENT_OUTPUT_FILES) | AGENT_LT_SUMMARY)}"
            )
        seed_path = Path(args.seed_summary)
        if not seed_path.is_file():
            parser.error(f"--seed-summary: path does not exist or is not a file: {args.seed_summary}")

    # Always run through the parallel wrapper to ensure consistent logging and behavior
    # even for single-worker runs (capture stdout/stderr, etc.)
    run_parallel(args)
