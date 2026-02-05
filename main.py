import argparse
import asyncio
import csv
import glob
import logging
import multiprocessing
import os
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
from rich.live import Live
from rich.table import Table

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

LAUNCHER = AgentLauncher()
logger = logging.getLogger(__name__)


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
                                "elapsed": time.time() - status_dict[pid]["start_time"]
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
                                "elapsed": time.time() - status_dict[pid]["start_time"]
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
                                "elapsed": time.time() - status_dict[pid]["start_time"]
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
                            "elapsed": time.time() - status_dict[pid]["start_time"]
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
                if status_dict is not None:
                    status_dict[pid] = {
                        "status": "Error",
                        "start_time": status_dict[pid]["start_time"],
                        "elapsed": time.time() - status_dict[pid]["start_time"]
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
                            "elapsed": time.time() - status_dict[pid]["start_time"]
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
    server.run()


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

        # Run main with the specific list of problems
        main(args, problem_queue=problem_queue, experiment_log_dir=experiment_log_dir, status_dict=status_dict, worker_id=worker_id)


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
        console = Console(force_terminal=True)
        with Live(console=console, auto_refresh=False) as live:
            while any(p.is_alive() for p in processes) or (status_dict and any(info.get("worker_id") is not None for info in status_dict.values())):
                # Check for dead workers and update status
                for p in processes:
                    if not p.is_alive():
                        # Worker died
                        wid = worker_map.get(p)
                        # Find problems assigned to this worker that are not terminal
                        for pid, info in status_dict.items():
                            if info.get("worker_id") == wid:
                                status = info.get("status")
                                if not (status.startswith("Completed") or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]):
                                    status_dict[pid] = {
                                        "status": "Error (Worker Died)",
                                        "start_time": info["start_time"],
                                        "elapsed": time.time() - info["start_time"],
                                        "worker_id": wid
                                    }

                total_problems = len(all_problems)
                completed_count = 0
                error_count = 0
                skipped_count = 0
                active_tasks = []
                
                started_pids = set(status_dict.keys())
                sorted_keys = sorted(status_dict.keys())
                
                erred_tasks = []
                for pid in sorted_keys:
                    info = status_dict[pid]
                    status = info.get("status", "Unknown")
                    start_time = info.get("start_time", 0)
                    elapsed = 0
                    
                    is_active = True
                    
                    if status.startswith("Completed"):
                        completed_count += 1
                        is_active = False
                    elif status in ["Error", "Error (Worker Died)"]:
                        error_count += 1
                        is_active = False
                        erred_tasks.append((pid, status, start_time + info.get("elapsed", 0)))
                    elif status == "Skipped (Khaos Req)":
                         skipped_count += 1
                         is_active = False
                    else:
                        elapsed = time.time() - start_time
                    
                    if is_active:
                        active_tasks.append((pid, status, elapsed))

                queued_count = total_problems - len(started_pids)
                running_count = len(active_tasks)

                table = Table(title=f"Parallel Execution ({total_problems} problems)")
                table.add_column("Problem ID", style="cyan")
                table.add_column("Status", style="magenta")
                table.add_column("Elapsed", style="green")
                
                for pid, status, elapsed in active_tasks:
                    table.add_row(pid, status, f"{elapsed:.1f}s")
                
                summary_parts = [
                    f"Progress: {completed_count + error_count + skipped_count}/{total_problems}",
                    f"Running: {running_count}",
                    f"Queued: {queued_count}",
                    f"[green]Completed: {completed_count}[/green]",
                    f"[red]Errors: {error_count}[/red]",
                ]
                if skipped_count > 0:
                    summary_parts.append(f"[yellow]Skipped: {skipped_count}[/yellow]")

                table.caption = " | ".join(summary_parts)
                
                renderable = table
                if erred_tasks:
                    erred_tasks.sort(key=lambda x: x[2], reverse=True)
                    latest_errors = erred_tasks[:5]
                    error_table = Table(title="Latest Errors (Max 5)", show_header=True, header_style="bold red")
                    error_table.add_column("Problem ID", style="cyan")
                    error_table.add_column("Status", style="red")
                    error_table.add_column("Time", style="dim")
                    
                    for pid, status, end_time in latest_errors:
                         t_str = datetime.fromtimestamp(end_time).strftime("%H:%M:%S")
                         error_table.add_row(pid, status, t_str)
                    
                    renderable = Group(table, error_table)

                live.update(renderable, refresh=True)
                
                # If all workers are dead, we are done.
                if not any(p.is_alive() for p in processes):
                     break
                     
                time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted by user. Terminating workers...")

    logger.info("Waiting for workers to cleanup...")
    for p in processes:
        if p.is_alive():
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"Worker {worker_map.get(p)} did not exit, forcing termination...")
                p.terminate()
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

    if args.parallel > 1:
        run_parallel(args)
    else:
        main(args)
