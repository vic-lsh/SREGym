import argparse
import asyncio
import csv
import fcntl
import glob
import logging
import multiprocessing
import os
import platform
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
import tempfile
import queue

import uvicorn
from rich.console import Console, Group
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
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

KIND_CLUSTER_PREFIX = "sregym-w"
WORKER_META_KEY_PREFIX = "__worker_meta__"
OPENEBS_PRELOAD_IMAGES = [
    "openebs/node-disk-manager:2.1.0",
    "openebs/node-disk-exporter:2.1.0",
    "openebs/node-disk-operator:2.1.0",
    "openebs/provisioner-localpv:3.4.0",
]


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
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
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


def driver_loop(
    conductor: Conductor,
    experiment_log_dir: str,
    problem_filter: str = None,
    agent_to_run: str = None,
    use_external_harness: bool = False,
    repeat: int = 1,
    enable_summary: bool = False,
    problem_list: list = None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
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
        problem_list: Optional list of problem IDs to run.
        status_dict: Shared dictionary for status updates (used in parallel mode).
        problem_queue: Optional multiprocessing.Queue to fetch problems from.
        worker_id: Optional ID of the worker process (for logging).
    """

    async def driver():
        # In parallel mode, we don't want the console to output to stdout directly
        # because it will be interleaved. We only use console for local logging
        # which will be redirected to a file.
        console = Console(force_terminal=True) if status_dict is None else Console(file=sys.stdout)
        
        # give the API a moment to bind
        await asyncio.sleep(1)

        # Verify agent exists in registry (skip if using external harness)
        if not use_external_harness:
            available_agents = list_agents(path=Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml").keys()
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

        def write_error_result(problem_id: str, error_message: str):
            """Write a structured result row even when execution fails before grading."""
            if not agent_to_run:
                return
            current_date_time = get_current_datetime_formatted()
            csv_path = os.path.join(experiment_log_dir, f"{current_date_time}_{problem_id}_{agent_to_run}_results.csv")
            snapshot = {
                "problem_id": problem_id,
                "run_status": "Error",
                "error": str(error_message),
            }
            with open(csv_path, "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted(snapshot.keys()))
                writer.writeheader()
                writer.writerow(snapshot)
            logger.info(f"❌ Problem {problem_id} for agent {agent_to_run} failed. Error result written to {csv_path}")
        # session_timestamp = get_current_datetime_formatted()

        if problem_queue:
            def problem_gen():
                while True:
                    try:
                        yield problem_queue.get_nowait()
                    except queue.Empty:
                        return
            problem_iterator = problem_gen()
        else:
            # Get all problem IDs and filter if needed
            problem_ids = conductor.problems.get_problem_ids()

            all_problem_ids = conductor.problems.get_problem_ids(all=True)
            if problem_filter:
                if problem_filter not in all_problem_ids:
                    console.log(f"⚠️  Problem '{problem_filter}' not found in registry. Available problems: {problem_ids}")
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

        for pid in problem_iterator:
            # Check for existing results (Resume capability)
            # We look for any timestamped file matching the pattern *_{pid}_{agent_to_run}_results.csv
            # Only checking if agent_to_run is specified (not external harness)
            completed_iterations = 0
            if agent_to_run and not use_external_harness:
                search_pattern = os.path.join(experiment_log_dir, f"*_{pid}_{agent_to_run}_results.csv")
                existing_files = glob.glob(search_pattern)

                for f_path in existing_files:
                    if is_result_complete(f_path):
                        completed_iterations += 1

                if completed_iterations >= repeat:
                    console.log(f"⏭️  Skipping problem '{pid}': Found {completed_iterations}/{repeat} completed results.")

                    if status_dict is not None:
                        status_dict[pid] = {
                            "status": "Completed (Resumed)",
                            "start_time": time.time(),
                            "elapsed": 0.0,
                            "worker_id": worker_id,
                        }
                    continue
                elif completed_iterations > 0:
                    console.log(
                        f"⏯️  Resuming problem '{pid}': {completed_iterations}/{repeat} iterations already completed."
                    )

            # Prepare for logging redirection if in parallel mode
            redirect_ctx = open(os.path.join(experiment_log_dir, f"{pid}.log"), "w") if status_dict is not None else None
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
                status_dict[pid] = {
                    "status": "Deploying",
                    "start_time": time.time(),
                    "elapsed": 0.0,
                    "worker_id": worker_id
                }

            try:
                for iteration in range(completed_iterations, repeat):
                    console.log(f"\n🔍 Starting problem: {pid} (Run {iteration+1}/{repeat})")

                    conductor.problem_id = pid

                    result = await conductor.start_problem()
                    if result == StartProblemResult.SKIPPED_KHAOS_REQUIRED:
                        console.log(f"⏭️  Skipping problem '{pid}': requires Khaos but running on emulated cluster")
                        if status_dict is not None:
                            status_dict[pid] = {
                                "status": "Skipped (Khaos Req)",
                                "start_time": status_dict[pid]["start_time"],
                                "elapsed": time.time() - status_dict[pid]["start_time"],
                                "worker_id": worker_id
                            }
                        continue

                    # If using external harness, fault is injected - exit now
                    if use_external_harness:
                        console.log(f"✅ Fault injected for problem '{pid}'. Exiting for external harness.")
                        return []

                    # Define agent log directory
                    agent_log_dir = os.path.join(experiment_log_dir, agent_to_run)

                    if not use_external_harness:
                        if status_dict is not None:
                            status_dict[pid] = {
                                "status": "Agent Running",
                                "start_time": status_dict[pid]["start_time"],
                                "elapsed": time.time() - status_dict[pid]["start_time"],
                                "worker_id": worker_id
                            }

                        reg = get_agent(agent_to_run, path=Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml")
                        if reg:
                            extra_args = ""
                            # Pass explicit log dir to supported agents (e.g. gemini_cli)
                            if agent_to_run == "gemini_cli":
                                 extra_args += f" --logs-dir {agent_log_dir}"
                            
                            if enable_summary:
                                 extra_args += " --enable-summary"
                                 
                            await LAUNCHER.ensure_started(reg, extra_args=extra_args.strip())

                    # Poll until grading completes or agent exits
                    while conductor.submission_stage != "done":
                        if status_dict is not None:
                            # Update stage
                            current_stage = conductor.submission_stage or "Running"
                            status_dict[pid] = {
                                "status": f"Agent: {current_stage}",
                                "start_time": status_dict[pid]["start_time"],
                                "elapsed": time.time() - status_dict[pid]["start_time"],
                                "worker_id": worker_id
                            }

                        # Check if agent process has exited
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            agent_proc.proc.poll()
                            if agent_proc.proc.returncode is not None:
                                console.log(f"⚠️  Agent process exited with return code {agent_proc.proc.returncode}")
                                break
                        await asyncio.sleep(1)

                    if status_dict is not None:
                        status_dict[pid] = {
                            "status": "Cleaning Up",
                            "start_time": status_dict[pid]["start_time"],
                            "elapsed": time.time() - status_dict[pid]["start_time"],
                            "worker_id": worker_id
                        }

                    console.log(f"✅ Completed {pid}: results={conductor.results}")

                    # Wait for agent process to complete naturally before cleanup
                    # This allows the agent to finish saving trajectories and other cleanup tasks
                    if not use_external_harness:
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            console.log(f"⏳ Waiting for agent process to complete...")
                            timeout = 30  # seconds
                            elapsed = 0
                            while elapsed < timeout:
                                agent_proc.proc.poll()
                                if agent_proc.proc.returncode is not None:
                                    console.log(f"✅ Agent process completed with return code {agent_proc.proc.returncode}")
                                    break
                                await asyncio.sleep(1)
                                elapsed += 1
                            else:
                                console.log(f"⚠️  Agent process did not complete within {timeout}s, will force cleanup")

                    snapshot = {"problem_id": pid}
                    for stage, outcome in conductor.results.items():
                        if isinstance(outcome, dict):
                            for k, v in outcome.items():
                                snapshot[f"{stage}.{k}"] = v
                        else:
                            snapshot[stage] = outcome
                    all_results_for_agent.append(snapshot)

                    fieldnames = sorted(snapshot.keys())
                    current_date_time = get_current_datetime_formatted()
                    
                    # Write results to experiment_log_dir
                    csv_path = os.path.join(experiment_log_dir, f"{current_date_time}_{pid}_{agent_to_run}_results.csv")
                    with open(csv_path, "w", newline="") as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows([snapshot])
                    logger.info(f"✅ Problem {pid} for agent {agent_to_run} complete! Results written to {csv_path}")

                    # Cleanup agent process so a fresh one can be started for the next problem
                    if not use_external_harness:
                        LAUNCHER.cleanup_agent(agent_to_run)
                        console.log(f"🧹 Cleaned up agent process for {agent_to_run}")

                        # Run summarization if enabled (specifically for gemini_cli)
                        if enable_summary and agent_to_run == "gemini_cli":
                            console.log("📝 Running external summarization for Gemini CLI...")
                            try:
                                # Run summarization script
                                summarize_cmd = [
                                    sys.executable,
                                    "clients/gemini_cli/summarize_results.py",
                                    "--logs-dir", agent_log_dir,
                                    "--model", os.environ.get("MODEL_ID", "gemini-2.0-flash")
                                ]
                                result = subprocess.run(
                                    summarize_cmd,
                                    capture_output=True,
                                    text=True
                                )
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
                    write_error_result(pid, str(e))
                if status_dict is not None:
                    status_dict[pid] = {
                        "status": "Error",
                        "start_time": status_dict[pid]["start_time"],
                        "elapsed": time.time() - status_dict[pid]["start_time"],
                        "worker_id": worker_id
                    }
                # Do not raise e; continue to next problem
            finally:
                # Ensure agent is cleaned up even if an error occurred
                if not use_external_harness:
                    LAUNCHER.cleanup_agent(agent_to_run)
                
                if status_dict is not None:
                    if status_dict[pid]["status"] != "Error":
                        status_dict[pid] = {
                            "status": "Completed",
                            "start_time": status_dict[pid]["start_time"],
                            "elapsed": time.time() - status_dict[pid]["start_time"],
                            "worker_id": worker_id
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
    problem_list: list = None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
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
            problem_list=problem_list,
            status_dict=status_dict,
            problem_queue=problem_queue,
            worker_id=worker_id,
        )
        setattr(main, "results", results)
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


def _get_preload_images() -> list[str]:
    override = os.getenv("SREGYM_PRELOAD_IMAGES", "").strip()
    if not override:
        return OPENEBS_PRELOAD_IMAGES
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
    for image in images:
        if not _docker_image_exists(image):
            logger.warning(f"Skipping kind load for missing local image: {image}")
            continue
        try:
            subprocess.run(
                ["kind", "load", "docker-image", "--name", cluster_name, image],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            logger.info(f"Loaded cached image into {cluster_name}: {image}")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to load image into {cluster_name}: {image} ({e})")


def _create_worker_cluster(worker_id: int, experiment_log_dir: str) -> tuple[str, str]:
    """Create a dedicated kind cluster for one worker and return (cluster_name, kubeconfig_path)."""
    cluster_name = f"{KIND_CLUSTER_PREFIX}{worker_id}"
    kubeconfig_dir = os.path.join(experiment_log_dir, "kubeconfigs")
    os.makedirs(kubeconfig_dir, exist_ok=True)
    kubeconfig_path = os.path.join(kubeconfig_dir, f"worker_{worker_id}.kubeconfig")
    config_path = _worker_kind_config_path()

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Kind config file not found: {config_path}")

    logger.info(f"Preparing isolated kind cluster for worker {worker_id}: {cluster_name}")

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


def worker_main(args, worker_id, problem_queue, experiment_log_dir, status_dict):
    """Worker function for parallel execution."""
    os.environ["SREGYM_WORKER_ID"] = str(worker_id)
    os.environ["API_PORT"] = str(8000 + worker_id)
    os.environ["MCP_SERVER_PORT"] = str(9000 + worker_id)
    os.environ["SREGYM_EXP_ENV"] = f"exp_env_{worker_id}"
    
    # Append worker ID to log file to avoid conflicts
    session_timestamp = get_current_datetime_formatted()
    os.environ["SREGYM_LOG_FILE"] = os.path.join(experiment_log_dir, f"sregym_{session_timestamp}_w{worker_id}.log")
    
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
            # Run main with the specific list of problems
            main(
                args,
                problem_queue=problem_queue,
                experiment_log_dir=experiment_log_dir,
                status_dict=status_dict,
                worker_id=worker_id,
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
    import math

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
        experiment_log_dir = os.path.abspath(f"logs/{session_timestamp}")
        os.makedirs(experiment_log_dir, exist_ok=True)
        logger.info(f"Parallel experiment logs will be stored in: {experiment_log_dir}")
        
        # Set log file for parallel runner
        log_file_path = os.path.join(experiment_log_dir, f"sregym_parallel_{session_timestamp}.log")
        os.environ["SREGYM_LOG_FILE"] = log_file_path
        init_logger()

    manager = multiprocessing.Manager()
    status_dict = manager.dict()
    problem_queue = manager.Queue()
    
    # Filter problems if resuming
    problems_to_run = []
    if args.resume_last or args.resume_from:
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
                    "worker_id": None
                }
    else:
        problems_to_run = all_problems

    for pid in problems_to_run:
        problem_queue.put(pid)

    _prefetch_infra_images_once()
        
    processes = []
    worker_map = {} # Map process to worker ID
    logger.info(f"Running {len(problems_to_run)} problems with {args.parallel} workers.")
    
    for i in range(args.parallel):
        p = multiprocessing.Process(target=worker_main, args=(args, i, problem_queue, experiment_log_dir, status_dict))
        p.start()
        processes.append(p)
        worker_map[p] = i
        
    # Monitoring loop
    try:
        # Redirect stdout/stderr to suppress unwanted output during Progress display
        # We keep a reference to the original stdout for the Console to use
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        # Use devnull for unwanted output
        null_out = open(os.devnull, 'w')
        sys.stdout = null_out
        sys.stderr = null_out
        
        try:
            console = Console(file=original_stdout, force_terminal=True)
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                # Main overall progress
                total_problems = len(all_problems)
                main_task = progress.add_task("[bold green]Overall Progress", total=total_problems)
                
                # Worker tasks - one per worker
                worker_tasks = {}
                for i in range(args.parallel):
                    # Initial state for workers
                    t_id = progress.add_task(f"Worker {i}: Idle", total=100, visible=True) 
                    worker_tasks[i] = t_id
                
                while any(p.is_alive() for p in processes) or (status_dict and any(info.get("worker_id") is not None for info in status_dict.values())):
                    # Check for dead workers and update status
                    active_workers = set()
                    for p in processes:
                        if p.is_alive():
                            active_workers.add(worker_map.get(p))
                        else:
                            # Worker died
                            wid = worker_map.get(p)
                            # Find problems assigned to this worker that are not terminal
                            for pid, info in status_dict.items():
                                if str(pid).startswith(WORKER_META_KEY_PREFIX):
                                    continue
                                if info.get("worker_id") == wid:
                                    status = info.get("status")
                                    if not (status.startswith("Completed") or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]):
                                        status_dict[pid] = {
                                            "status": "Error (Worker Died)",
                                            "start_time": info["start_time"],
                                            "elapsed": time.time() - info["start_time"],
                                            "worker_id": wid
                                        }

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
                            is_active = not (status.startswith("Completed") or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"])
                            if is_active:
                                start_t = info.get("start_time", time.time())
                                current_worker_status[wid] = (status, pid, start_t)

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
                             progress.update(worker_tasks[i], description=f"Worker {i}: [dim]Finished[/dim]", completed=100)
                        elif current_worker_status[i]:
                            status, pid, start_t = current_worker_status[i]
                            elapsed = int(time.time() - start_t)
                            
                            # Map status to approximate progress
                            completed_pct = 0
                            if status == "Deploying":
                                completed_pct = 10
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
                                
                            desc = f"Worker {i}: [cyan]{pid}[/cyan] - {status} [yellow]({elapsed}s)[/yellow]"
                            progress.update(worker_tasks[i], description=desc, completed=completed_pct)
                        else:
                            # Worker is alive but idle (or between tasks)
                            meta = status_dict.get(_worker_meta_key(i))
                            if meta and meta.get("status"):
                                start_t = meta.get("start_time", time.time())
                                elapsed = int(time.time() - start_t)
                                progress.update(
                                    worker_tasks[i],
                                    description=f"Worker {i}: [blue]{meta.get('status')}[/blue] [yellow]({elapsed}s)[/yellow]",
                                    completed=0,
                                )
                            else:
                                progress.update(worker_tasks[i], description=f"Worker {i}: Idle", completed=0)
                    
                    if not any(p.is_alive() for p in processes):
                        break
                        
                    time.sleep(0.5)
        
        finally:
            # Restore stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            null_out.close()

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user. Terminating workers...")

    finally:
        pass # Nothing to restore here anymore

    logger.info("Waiting for workers to cleanup...")
    # Wait for workers to cleanup (parallel wait)
    start_wait = time.time()
    while time.time() - start_wait < 5:
        if not any(p.is_alive() for p in processes):
            break
        time.sleep(0.1)

    for p in processes:
        if p.is_alive():
            logger.warning(f"Worker {worker_map.get(p)} did not exit, forcing termination...")
            p.terminate()
            p.join(timeout=1)
        else:
            p.join()


def main(args, problem_list=None, experiment_log_dir=None, status_dict=None, problem_queue=None, worker_id=None):
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
            experiment_log_dir = os.path.abspath(f"logs/{session_timestamp}")
    
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

    # Enforce explicit kubeconfig selection for every process and worker.
    base_kubeconfig = require_kubeconfig_path()
    os.environ["KUBECONFIG"] = base_kubeconfig
    os.environ["SREGYM_BASE_KUBECONFIG"] = base_kubeconfig

    conductor = Conductor()

    # Start the driver in the background; it will call request_shutdown() when finished
    driver_thread = threading.Thread(
        target=_run_driver_and_shutdown,
        args=(conductor, experiment_log_dir, args.problem, args.agent, args.use_external_harness, args.repeat, args.enable_summary, problem_list, status_dict, problem_queue, worker_id),
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
    args = parser.parse_args()

    # Validate that --agent is provided when not using external harness
    if not args.use_external_harness and args.agent is None:
        parser.error("--agent is required when --use-external-harness is not set")

    # Always run through the parallel wrapper to ensure consistent logging and behavior
    # even for single-worker runs (capture stdout/stderr, etc.)
    run_parallel(args)
