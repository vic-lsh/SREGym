import argparse
import asyncio
import csv
import logging
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import uvicorn
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.text import Text

from logger import init_logger
from mcp_server.configs.load_all_cfg import mcp_server_cfg
from mcp_server.configs.mcp_tool_cfg import McpToolCfg
from mcp_server.kubectl_server_helper.kubectl import set_exp_env_dir
from mcp_server.sregym_mcp_server import create_mcp_app
from sregym.agent_launcher import AgentLauncher
from sregym.agent_registry import get_agent, list_agents
from sregym.conductor.conductor import Conductor
from sregym.conductor.conductor_api import ApiServer
from sregym.conductor.constants import StartProblemResult
from sregym.conductor.problems.registry import ProblemRegistry

LAUNCHER = AgentLauncher()
logger = logging.getLogger(__name__)


def get_current_datetime_formatted():
    now = datetime.now()
    formatted_datetime = now.strftime("%m%d_%H%M")
    return formatted_datetime


class ParallelProgressManager:
    def __init__(self, total_runs: int):
        self._lock = threading.Lock()
        self._progress = Progress(
            TextColumn("{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TextColumn("{task.fields[stage]}"),
            TimeElapsedColumn(),
            refresh_per_second=4,
        )
        self._overall_task = self._progress.add_task("Experiments", total=total_runs, stage="running")
        self._run_tasks: dict[str, int] = {}
        self._run_stage_order: dict[str, list[str]] = {}
        self._run_stage_label: dict[str, str] = {}

    def start(self):
        self._progress.start()

    def stop(self):
        self._progress.stop()

    def add_run(self, run_id: str, description: str, stage_order: list[str] | None = None):
        if stage_order is None:
            stage_order = ["setup", "diagnosis", "mitigation", "done"]
        with self._lock:
            task_id = self._progress.add_task(description, total=len(stage_order), stage=self._label_stage(stage_order[0]))
            self._run_tasks[run_id] = task_id
            self._run_stage_order[run_id] = stage_order
            self._run_stage_label[run_id] = stage_order[0]

    def set_stage_order(self, run_id: str, stage_order: list[str]):
        if not stage_order:
            stage_order = ["done"]
        with self._lock:
            self._run_stage_order[run_id] = stage_order
            task_id = self._run_tasks.get(run_id)
            if task_id is None:
                return
            current_stage = self._run_stage_label.get(run_id, stage_order[0])
            completed = self._stage_index(stage_order, current_stage)
            self._progress.update(task_id, total=len(stage_order), completed=completed)

    def update_stage(self, run_id: str, stage: str):
        stage = stage or "setup"
        with self._lock:
            task_id = self._run_tasks.get(run_id)
            if task_id is None:
                return
            stage_order = self._run_stage_order.get(run_id, ["setup", "diagnosis", "mitigation", "done"])
            self._run_stage_label[run_id] = stage
            completed = self._stage_index(stage_order, stage)
            self._progress.update(task_id, completed=completed, stage=self._label_stage(stage))

    def mark_done(self, run_id: str):
        with self._lock:
            task_id = self._run_tasks.get(run_id)
            if task_id is not None:
                stage_order = self._run_stage_order.get(run_id, ["done"])
                self._progress.update(task_id, completed=len(stage_order), stage=self._label_stage("done"))
            self._progress.update(self._overall_task, advance=1)

    def mark_failed(self, run_id: str):
        with self._lock:
            task_id = self._run_tasks.get(run_id)
            if task_id is not None:
                stage_order = self._run_stage_order.get(run_id, ["done"])
                self._progress.update(task_id, completed=len(stage_order), stage=Text("failed", style="bold red"))
            self._progress.update(self._overall_task, advance=1)

    def _stage_index(self, stage_order: list[str], stage: str) -> int:
        try:
            idx = stage_order.index(stage)
        except ValueError:
            return 0
        return min(idx + 1, len(stage_order))

    def _label_stage(self, stage: str):
        labels = {
            "setup": "deployment",
            "diagnosis": "diagnose",
            "mitigation": "mitigation",
            "done": "done",
        }
        return labels.get(stage, stage)


def _disable_console_logging():
    root_logger = logging.getLogger("all")
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.FileHandler):
            continue
        if getattr(handler, "stream", None) in (sys.stdout, sys.stderr):
            root_logger.removeHandler(handler)


def driver_loop(
    conductor: Conductor,
    experiment_log_dir: str,
    problem_filter: str = None,
    agent_to_run: str = None,
    use_external_harness: bool = False,
    repeat: int = 1,
    enable_summary: bool = False,
    launcher: AgentLauncher | None = None,
    run_log_dir: str | None = None,
    run_id: str | None = None,
    mcp_tool_cfg: McpToolCfg | None = None,
    agent_env: dict | None = None,
    progress_mgr: ParallelProgressManager | None = None,
    quiet: bool = False,
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
    """

    async def driver():
        console = Console()
        def _log(msg: str):
            if not quiet:
                console.log(msg)
        # give the API a moment to bind
        await asyncio.sleep(1)

        # Verify agent exists in registry (skip if using external harness)
        if not use_external_harness:
            available_agents = list_agents(path=Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml").keys()
            if agent_to_run not in available_agents:
                msg = f"Agent '{agent_to_run}' not found in registry. Available agents: {available_agents}"
                if quiet:
                    raise RuntimeError(msg)
                console.log(f"⚠️ {msg}")
                sys.exit(1)

            _log(f"Starting agent now: {agent_to_run}")
            conductor.register_agent(agent_to_run)

            # Start K8s API proxy to hide chaos engineering namespaces from the agent
            _log("🔒 Starting Kubernetes API proxy to hide chaos namespaces...")
            conductor.start_k8s_proxy()
            (launcher or LAUNCHER).set_agent_kubeconfig(conductor.get_agent_kubeconfig_path())

        all_results_for_agent = []
        # session_timestamp = get_current_datetime_formatted()

        # Get all problem IDs and filter if needed
        problem_ids = conductor.problems.get_problem_ids()


        all_problem_ids = conductor.problems.get_problem_ids(all=True)
        if problem_filter:
            if problem_filter not in all_problem_ids:
                console.log(f"⚠️  Problem '{problem_filter}' not found in registry. Available problems: {problem_ids}")
                sys.exit(1)
            problem_ids = [problem_filter]
            console.log(f"🎯 Running single problem: {problem_filter}")

        # sanity check: are there any specified problem ids that do not exist in the registry?
        unknown_problem_ids = set(problem_ids) - set(all_problem_ids)
        if unknown_problem_ids:
            console.log(
                f"⚠️  These problem ids do not exist in the registry and they will be skipped: {unknown_problem_ids}"
            )
        for unknown_problem_id in unknown_problem_ids:
            problem_ids.remove(unknown_problem_id)

        for pid in problem_ids:
            for iteration in range(repeat):
                _log(f"\n🔍 Starting problem: {pid} (Run {iteration+1}/{repeat})")

                conductor.problem_id = pid

                result = await conductor.start_problem()
                if result == StartProblemResult.SKIPPED_KHAOS_REQUIRED:
                    _log(f"⏭️  Skipping problem '{pid}': requires Khaos but running on emulated cluster")
                    continue

                if progress_mgr and run_id:
                    stage_order = ["setup"] + [stage["name"] for stage in conductor.stage_sequence] + ["done"]
                    progress_mgr.set_stage_order(run_id, stage_order)
                    progress_mgr.update_stage(run_id, conductor.submission_stage or "setup")

                if mcp_tool_cfg is not None:
                    prometheus_port = getattr(conductor.prometheus, "port", None)
                    if prometheus_port:
                        mcp_tool_cfg.prometheus_url = f"http://localhost:{prometheus_port}"
                    trace_api = getattr(conductor.app, "trace_api", None)
                    if trace_api is not None:
                        mcp_tool_cfg.jaeger_base_url = getattr(trace_api, "base_url", None)

                # If using external harness, fault is injected - exit now
                if use_external_harness:
                    _log(f"✅ Fault injected for problem '{pid}'. Exiting for external harness.")
                    return []

                # Define agent log directory
                agent_log_root = run_log_dir or experiment_log_dir
                agent_log_dir = os.path.join(agent_log_root, agent_to_run)
                os.makedirs(agent_log_dir, exist_ok=True)

                if not use_external_harness:
                    reg = get_agent(agent_to_run, path=Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml")
                    if reg:
                        extra_args = ""
                        # Pass explicit log dir to supported agents (e.g. gemini_cli)
                        if agent_to_run == "gemini_cli":
                             extra_args += f" --logs-dir {agent_log_dir}"
                        
                        if enable_summary:
                             extra_args += " --enable-summary"
                             
                        await (launcher or LAUNCHER).ensure_started(
                            reg, extra_args=extra_args.strip(), extra_env=agent_env
                        )

                # Poll until grading completes or agent exits
                last_stage = None
                while conductor.submission_stage != "done":
                    if progress_mgr and run_id:
                        current_stage = conductor.submission_stage or "setup"
                        if current_stage != last_stage:
                            progress_mgr.update_stage(run_id, current_stage)
                            last_stage = current_stage
                    # Check if agent process has exited
                    agent_proc = (launcher or LAUNCHER)._procs.get(agent_to_run)
                    if agent_proc:
                        agent_proc.proc.poll()
                        if agent_proc.proc.returncode is not None:
                            _log(f"⚠️  Agent process exited with return code {agent_proc.proc.returncode}")
                            break
                    await asyncio.sleep(1)

                _log(f"✅ Completed {pid}: results={conductor.results}")

                # Wait for agent process to complete naturally before cleanup
                # This allows the agent to finish saving trajectories and other cleanup tasks
                if not use_external_harness:
                    agent_proc = (launcher or LAUNCHER)._procs.get(agent_to_run)
                    if agent_proc:
                        _log(f"⏳ Waiting for agent process to complete...")
                        timeout = 30  # seconds
                        elapsed = 0
                        while elapsed < timeout:
                            agent_proc.proc.poll()
                            if agent_proc.proc.returncode is not None:
                                _log(f"✅ Agent process completed with return code {agent_proc.proc.returncode}")
                                break
                            await asyncio.sleep(1)
                            elapsed += 1
                        else:
                            _log(f"⚠️  Agent process did not complete within {timeout}s, will force cleanup")

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
                run_suffix = f"_{run_id}" if run_id else ""
                csv_path = os.path.join(
                    experiment_log_dir, f"{current_date_time}_{pid}_{agent_to_run}{run_suffix}_results.csv"
                )
                with open(csv_path, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows([snapshot])
                logger.info(f"✅ Problem {pid} for agent {agent_to_run} complete! Results written to {csv_path}")

                # Cleanup agent process so a fresh one can be started for the next problem
                if not use_external_harness:
                    (launcher or LAUNCHER).cleanup_agent(agent_to_run)
                    _log(f"🧹 Cleaned up agent process for {agent_to_run}")

                    # Run summarization if enabled (specifically for gemini_cli)
                    if enable_summary and agent_to_run == "gemini_cli":
                        _log("📝 Running external summarization for Gemini CLI...")
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
                                _log("✅ External summarization step completed.")
                            else:
                                _log(f"⚠️ External summarization failed (exit code {result.returncode}):")
                                _log(result.stderr)
                        except Exception as e:
                            _log(f"⚠️ External summarization failed to launch: {e}")

        # Stop K8s API proxy when all problems are done
        if not use_external_harness:
            _log("🔓 Stopping Kubernetes API proxy...")
            conductor.stop_k8s_proxy()

        return [{agent_to_run: all_results_for_agent}]

    return asyncio.run(driver())


@dataclass
class RunContext:
    problem_id: str
    run_id: str
    api_port: int
    mcp_port: int
    proxy_port: int
    log_dir: str
    namespace_suffix: str


class McpServer:
    def __init__(self, app, host: str, port: int, exp_env_dir: str | None = None):
        self.app = app
        self.host = host
        self.port = port
        self.exp_env_dir = exp_env_dir
        self._shutdown_event = threading.Event()
        self._server = None

    def run(self):
        if self.exp_env_dir:
            set_exp_env_dir(self.exp_env_dir)
        config = uvicorn.Config(app=self.app, host=self.host, port=self.port, log_level="info")
        config.install_signal_handlers = False
        server = uvicorn.Server(config)
        self._server = server

        def _watch():
            self._shutdown_event.wait()
            server.should_exit = True

        threading.Thread(target=_watch, name=f"mcp-shutdown-watcher-{self.port}", daemon=True).start()
        try:
            server.run()
        finally:
            self._shutdown_event.clear()
            self._server = None

    def shutdown(self):
        self._shutdown_event.set()
        if self._server is not None:
            self._server.should_exit = True


def _run_problem(
    ctx: RunContext,
    args,
    experiment_log_dir: str,
    progress_mgr: ParallelProgressManager | None = None,
    quiet: bool = False,
):
    run_log_dir = ctx.log_dir
    os.makedirs(run_log_dir, exist_ok=True)

    exp_env_dir = os.path.join("exp_env", secrets.token_hex(4))
    os.makedirs(exp_env_dir, exist_ok=True)

    conductor = Conductor(namespace_suffix=ctx.namespace_suffix, k8s_proxy_port=ctx.proxy_port)
    launcher = AgentLauncher(clean_exp_env=False)

    api_host = os.getenv("API_HOSTNAME", "0.0.0.0")
    api_server = ApiServer(conductor, host=api_host, port=ctx.api_port)
    api_thread = threading.Thread(target=api_server.run, name=f"api-{ctx.run_id}", daemon=True)
    api_thread.start()

    mcp_server = None
    mcp_thread = None
    mcp_cfg = None
    if not args.use_external_harness:
        mcp_cfg = McpToolCfg(benchmark_submit_url=f"http://localhost:{ctx.api_port}/submit")
        mcp_app = create_mcp_app(mcp_cfg)
        mcp_host = "0.0.0.0" if mcp_server_cfg.expose_server else "127.0.0.1"
        mcp_server = McpServer(mcp_app, host=mcp_host, port=ctx.mcp_port, exp_env_dir=exp_env_dir)
        mcp_thread = threading.Thread(target=mcp_server.run, name=f"mcp-{ctx.run_id}", daemon=True)
        mcp_thread.start()

    agent_env = {
        "API_PORT": str(ctx.api_port),
        "MCP_SERVER_PORT": str(ctx.mcp_port),
        "EXP_ENV_DIR": exp_env_dir,
    }

    try:
        results = driver_loop(
            conductor,
            experiment_log_dir,
            problem_filter=ctx.problem_id,
            agent_to_run=args.agent,
            use_external_harness=args.use_external_harness,
            repeat=args.repeat,
            enable_summary=args.enable_summary,
            launcher=launcher,
            run_log_dir=run_log_dir,
            run_id=ctx.run_id,
            mcp_tool_cfg=mcp_cfg,
            agent_env=agent_env,
            progress_mgr=progress_mgr,
            quiet=quiet,
        )
        return results
    finally:
        if mcp_server:
            mcp_server.shutdown()
        if api_server:
            api_server.shutdown()
        if mcp_thread:
            mcp_thread.join(timeout=5)
        api_thread.join(timeout=5)
        try:
            shutil.rmtree(exp_env_dir)
        except Exception:
            pass


def main(args):
    # Generate session ID and log directory
    session_timestamp = get_current_datetime_formatted()
    # Ensure logs root exists
    os.makedirs("logs", exist_ok=True)
    # Create experiment directory
    experiment_log_dir = os.path.abspath(f"logs/{session_timestamp}")
    os.makedirs(experiment_log_dir, exist_ok=True)
    
    # Set log file path for init_logger
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

    if nm and args.parallel > 1:
        logger.warning("⚠️ Noise manager is global; parallel runs may interfere with each other.")

    os.environ["MODEL_ID"] = args.model

    registry = ProblemRegistry()
    problem_ids = registry.get_problem_ids()
    if args.problem:
        if args.problem not in problem_ids:
            raise RuntimeError(f"Problem '{args.problem}' not found in registry.")
        problem_ids = [args.problem]

    if not problem_ids:
        logger.warning("⚠️ No problems to run.")
        return []

    base_api_port = int(os.getenv("API_PORT", "8000"))
    base_mcp_port = int(os.getenv("MCP_SERVER_PORT", "9954"))
    base_proxy_port = int(os.getenv("K8S_PROXY_PORT", "16443"))

    run_contexts: list[RunContext] = []
    for idx, pid in enumerate(problem_ids):
        run_id = f"run{idx}"
        run_log_dir = os.path.join(experiment_log_dir, f"{pid}_{run_id}")
        run_contexts.append(
            RunContext(
                problem_id=pid,
                run_id=run_id,
                api_port=base_api_port + idx,
                mcp_port=base_mcp_port + idx,
                proxy_port=base_proxy_port + idx,
                log_dir=run_log_dir,
                namespace_suffix=run_id,
            )
        )

    max_workers = max(1, args.parallel)
    progress_mgr = None
    quiet = False
    if args.parallel > 1:
        progress_mgr = ParallelProgressManager(total_runs=len(run_contexts))
        progress_mgr.start()
        _disable_console_logging()
        quiet = True
    results = []
    errors: list[tuple[RunContext, Exception]] = []
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for ctx in run_contexts:
                if progress_mgr:
                    progress_mgr.add_run(ctx.run_id, f"{ctx.problem_id} ({ctx.run_id})")
                future = executor.submit(_run_problem, ctx, args, experiment_log_dir, progress_mgr, quiet)
                futures[future] = ctx
            for future in as_completed(futures):
                ctx = futures[future]
                try:
                    results.append(future.result())
                    if progress_mgr:
                        progress_mgr.mark_done(ctx.run_id)
                except Exception as e:
                    errors.append((ctx, e))
                    logger.error(f"❌ Run {ctx.run_id} failed: {e}")
                    if progress_mgr:
                        progress_mgr.mark_failed(ctx.run_id)
    finally:
        if progress_mgr:
            progress_mgr.stop()
        if nm:
            try:
                logger.info("Stopping noise manager...")
                nm.stop()
            except Exception as e:
                logger.error(f"⚠️ Error stopping noise manager: {e}")

    if not results:
        logger.warning("⚠️ No results to write.")
    if errors:
        console = Console()
        for ctx, err in errors:
            console.print(f"[red]Run {ctx.run_id} ({ctx.problem_id}) failed:[/red] {err}")

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
        help="Maximum number of problems to run in parallel",
    )
    parser.add_argument(
        "--enable-summary",
        action="store_true",
        help="Enable summarization of agent runs (only supported by gemini_cli)",
    )
    args = parser.parse_args()

    # Validate that --agent is provided when not using external harness
    if not args.use_external_harness and args.agent is None:
        parser.error("--agent is required when --use-external-harness is not set")

    main(args)
