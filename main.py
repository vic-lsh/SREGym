import argparse
import asyncio
import csv
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
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from dataclasses import asdict, dataclass
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
from sregym.agent_exit import (
    DEFAULT_GRACEFUL_EXIT_TIMEOUT_SECONDS,
    resolve_graceful_exit_timeout_seconds,
    wait_for_process_exit,
)
from sregym.agent_registry import get_agent, list_agents
from sregym.conductor.conductor import Conductor
from sregym.conductor.conductor_api import request_shutdown, run_api
from sregym.conductor.constants import StartProblemResult
from sregym.service.kubeconfig import require_kubeconfig_path
from sregym.worker_infra import (
    KIND_CLUSTER_PREFIX as _WORKER_INFRA_KIND_CLUSTER_PREFIX,
    apply_worker_cpu_limit as _apply_worker_cpu_limit,
    create_kind_cluster as _create_kind_cluster,
    create_worker_cluster as _create_worker_cluster,
    delete_kind_cluster as _delete_kind_cluster,
    delete_worker_cluster as _delete_worker_cluster,
    existing_cluster_is_reusable as _existing_cluster_is_reusable,
    log_cpu_oversubscription as _log_cpu_oversubscription,
    reuse_cluster_enabled as _reuse_cluster_enabled,
    stable_kubeconfig_path as _stable_kubeconfig_path,
    worker_kind_config_path as _worker_kind_config_path,
)

LAUNCHER = AgentLauncher()
# Ensure logger inherits from 'all' so handlers are attached
logger = logging.getLogger("all.main")

# Agents that support the summary system (accumulate learnings across runs).
# Maps agent name -> output filename used by that agent.
# Agents listed here use an external summarizer subprocess run by main.py after each problem.
AGENT_OUTPUT_FILES = {"gemini_cli": "gemini-cli.txt", "claudecode": "claude-code.txt"}

# Agents with a built-in long-term summary system (no external summarizer subprocess needed).
# They accept --summary-dir and --summary-model CLI args.
AGENT_LT_SUMMARY = {"crucible", "crucible_deepagents", "crucible_simple"}


def agent_supports_summary(agent_name: str) -> bool:
    """Return True if the given agent supports the shared summary system."""
    return agent_name in AGENT_OUTPUT_FILES or agent_name in AGENT_LT_SUMMARY


KIND_CLUSTER_PREFIX = _WORKER_INFRA_KIND_CLUSTER_PREFIX
LIVE_CLUSTER_PREFIX = "sregym-live"
WORKER_META_KEY_PREFIX = "__worker_meta__"
LIVE_COMMANDS = {"deploy", "undeploy", "serve-k8s-proxy"}
CLI_APP_NAME_ALIASES = {
    "astronomy_shop": "Astronomy Shop",
    "hotel_reservation": "Hotel Reservation",
    "social_network": "Social Network",
    "fleet_cast": "Fleet Cast",
    "blueprint_hotel_reservation": "Blueprint Hotel Reservation",
}

# Exceptions raised when the multiprocessing Manager's IPC pipe is broken.
# When this happens, status_dict proxy operations fail — but that should not
# crash the driver loop or the supervisor, since status reporting is non-critical.
_MANAGER_PIPE_ERRORS = (BrokenPipeError, OSError, EOFError, ConnectionResetError)


def _safe_status_update(status_dict, key, value):
    """Update status_dict, ignoring errors from a dead Manager."""
    if status_dict is None:
        return
    try:
        status_dict[key] = value
    except _MANAGER_PIPE_ERRORS:
        pass


def _safe_status_read(status_dict, key, default=None):
    """Read from status_dict, returning default if Manager is dead."""
    if status_dict is None:
        return default
    try:
        return status_dict[key]
    except (*_MANAGER_PIPE_ERRORS, KeyError):
        return default


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


def _read_solved_from_result_csv(csv_path: str) -> bool:
    """Return True iff Diagnosis.success AND Mitigation.success are both true.

    Bools are written via csv.QUOTE_NONNUMERIC so they appear as the strings
    ``"True"`` / ``"False"``. Treat anything else (missing/empty/error rows)
    as not-solved.
    """
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            try:
                row = next(reader)
            except StopIteration:
                return False

            def _truthy(value: str | None) -> bool:
                if value is None:
                    return False
                return str(value).strip().lower() == "true"

            return _truthy(row.get("Diagnosis.success")) and _truthy(row.get("Mitigation.success"))
    except Exception:
        return False


_ADAPTIVE_RESULT_RE = re.compile(
    r"^(?P<ts>[^_]+_[^_]+)_(?P<seq_idx>\d{5})_(?P<pid>.+)_(?P<agent>[^_]+)_results\.csv$"
)


def _parse_adaptive_csv_filename(csv_path: str, agent_name: str) -> tuple[int, str] | None:
    """Parse a result CSV filename and extract ``(seq_idx, pid)``.

    Filenames look like ``{MMDD_HHMM}_{seq_idx:05d}_{pid}_{agent}_results.csv``.
    Because both the timestamp and the pid can contain underscores, we anchor
    the parse on the trailing ``_{agent}_results.csv`` suffix and the
    five-digit zero-padded ``seq_idx``.

    Returns None if the filename doesn't match the expected shape (e.g. a
    pre-adaptive result without a seq_idx).
    """
    name = os.path.basename(csv_path)
    suffix = f"_{agent_name}_results.csv"
    if not name.endswith(suffix):
        return None
    head = name[: -len(suffix)]
    # head = "{MMDD_HHMM}_{seq_idx:05d}_{pid}"; locate the 5-digit chunk
    # immediately preceded by an underscore. We iterate from the start
    # because the timestamp itself contains digits but is split as
    # "MMDD_HHMM" — its second token is 4 digits, not 5.
    for m in re.finditer(r"_(\d{5})_", head):
        seq_idx_str = m.group(1)
        pid = head[m.end():]
        # Sanity check: the prefix before the seq_idx must look like a
        # timestamp ("XXXX_XXXX") so we don't accidentally match digits
        # inside a pid.
        prefix = head[: m.start()]
        if "_" in prefix and not prefix.endswith("_"):
            try:
                return int(seq_idx_str), pid
            except ValueError:
                return None
    return None


def generate_sequence(problem_ids: list, n: int, seed: int) -> list:
    """Generate a deterministic sequence of n problem IDs sampled with replacement."""
    rng = random.Random(seed)
    return [rng.choice(problem_ids) for _ in range(n)]


@dataclass
class FrontendPortForwardInfo:
    local_port: int
    pid: int
    url: str


@dataclass
class K8sProxyInfo:
    port: int
    pid: int
    url: str
    kubeconfig_path: str


@dataclass
class LiveDeploymentState:
    deployment_name: str
    deployment_dir: str
    cluster_name: str
    kubeconfig_path: str
    shared_cluster: bool
    cluster_reused: bool
    app_name: str
    namespace: str
    problem_id: str | None
    frontend_service: str
    frontend_target_port: int
    frontend_local_port: int | None
    frontend_url: str | None
    frontend_port_forward_pid: int | None
    k8s_proxy_port: int | None
    k8s_proxy_pid: int | None
    k8s_proxy_url: str | None
    k8s_proxy_kubeconfig_path: str | None
    created_at: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "LiveDeploymentState":
        return cls(
            deployment_name=str(data["deployment_name"]),
            deployment_dir=str(data["deployment_dir"]),
            cluster_name=str(data["cluster_name"]),
            kubeconfig_path=str(data["kubeconfig_path"]),
            shared_cluster=bool(data.get("shared_cluster", False)),
            cluster_reused=bool(data.get("cluster_reused", False)),
            app_name=str(data["app_name"]),
            namespace=str(data["namespace"]),
            problem_id=data.get("problem_id"),
            frontend_service=str(data["frontend_service"]),
            frontend_target_port=int(data["frontend_target_port"]),
            frontend_local_port=(
                int(data["frontend_local_port"]) if data.get("frontend_local_port") is not None else None
            ),
            frontend_url=data.get("frontend_url"),
            frontend_port_forward_pid=(
                int(data["frontend_port_forward_pid"])
                if data.get("frontend_port_forward_pid") is not None
                else None
            ),
            k8s_proxy_port=int(data["k8s_proxy_port"]) if data.get("k8s_proxy_port") is not None else None,
            k8s_proxy_pid=int(data["k8s_proxy_pid"]) if data.get("k8s_proxy_pid") is not None else None,
            k8s_proxy_url=data.get("k8s_proxy_url"),
            k8s_proxy_kubeconfig_path=data.get("k8s_proxy_kubeconfig_path"),
            created_at=str(data["created_at"]),
        )

    @classmethod
    def load(cls, path: str | Path) -> "LiveDeploymentState":
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.from_dict(data)

    def write(self, path: str | Path) -> None:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=True)


@dataclass
class PersistedClusterBaseline:
    namespaces: set[str]
    cluster_roles: set[str]
    cluster_role_bindings: set[str]
    persistent_volumes: set[str]
    storage_classes: set[str]
    crds: set[str]
    node_labels: dict[str, dict[str, str]]
    node_taints: dict[str, list]
    coredns_configmap_data: dict[str, str]

    def to_dict(self) -> dict:
        return {
            "namespaces": sorted(self.namespaces),
            "cluster_roles": sorted(self.cluster_roles),
            "cluster_role_bindings": sorted(self.cluster_role_bindings),
            "persistent_volumes": sorted(self.persistent_volumes),
            "storage_classes": sorted(self.storage_classes),
            "crds": sorted(self.crds),
            "node_labels": self.node_labels,
            "node_taints": self.node_taints,
            "coredns_configmap_data": self.coredns_configmap_data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PersistedClusterBaseline":
        return cls(
            namespaces=set(data.get("namespaces", [])),
            cluster_roles=set(data.get("cluster_roles", [])),
            cluster_role_bindings=set(data.get("cluster_role_bindings", [])),
            persistent_volumes=set(data.get("persistent_volumes", [])),
            storage_classes=set(data.get("storage_classes", [])),
            crds=set(data.get("crds", [])),
            node_labels=dict(data.get("node_labels", {})),
            node_taints=dict(data.get("node_taints", {})),
            coredns_configmap_data=dict(data.get("coredns_configmap_data", {})),
        )

    @classmethod
    def from_cluster_baseline(cls, baseline) -> "PersistedClusterBaseline":
        return cls(
            namespaces=set(getattr(baseline, "namespaces", set())),
            cluster_roles=set(getattr(baseline, "cluster_roles", set())),
            cluster_role_bindings=set(getattr(baseline, "cluster_role_bindings", set())),
            persistent_volumes=set(getattr(baseline, "persistent_volumes", set())),
            storage_classes=set(getattr(baseline, "storage_classes", set())),
            crds=set(getattr(baseline, "crds", set())),
            node_labels=dict(getattr(baseline, "node_labels", {})),
            node_taints=dict(getattr(baseline, "node_taints", {})),
            coredns_configmap_data=dict(getattr(baseline, "coredns_configmap_data", {})),
        )


@dataclass
class SharedLiveClusterState:
    cluster_name: str
    kubeconfig_path: str
    baseline: PersistedClusterBaseline | None
    created_at: str

    def to_dict(self) -> dict:
        return {
            "cluster_name": self.cluster_name,
            "kubeconfig_path": self.kubeconfig_path,
            "baseline": self.baseline.to_dict() if self.baseline else None,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SharedLiveClusterState":
        baseline_data = data.get("baseline")
        return cls(
            cluster_name=str(data["cluster_name"]),
            kubeconfig_path=str(data["kubeconfig_path"]),
            baseline=PersistedClusterBaseline.from_dict(baseline_data) if baseline_data else None,
            created_at=str(data["created_at"]),
        )

    @classmethod
    def load(cls, path: str | Path) -> "SharedLiveClusterState":
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return cls.from_dict(data)

    def write(self, path: str | Path) -> None:
        os.makedirs(os.path.dirname(os.fspath(path)), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.to_dict(), handle, indent=2, sort_keys=True)


@dataclass
class LiveUndeployResult:
    state: LiveDeploymentState
    cluster_deleted: bool


class _LiveAppProblem:
    def __init__(self, app):
        self.app = app
        self.namespace = app.namespace

    def requires_khaos(self) -> bool:
        return False

    def inject_fault(self) -> None:
        return None

    def recover_fault(self) -> None:
        return None


def _default_live_deployments_root() -> str:
    return os.path.abspath(os.path.join("logs", "live_deployments"))


def _sanitize_deployment_name(name: str) -> str:
    sanitized = re.sub(r"[^a-z0-9._-]+", "-", name.strip().lower()).strip(".-_")
    if not sanitized:
        raise ValueError("Deployment name must contain at least one alphanumeric character.")
    return sanitized[:48]


def _default_deployment_name(problem_id: str | None, app_name: str | None) -> str:
    target = problem_id or app_name or "live"
    return _sanitize_deployment_name(f"{target}-{get_current_datetime_formatted()}")


def _deployment_dir_for_name(deployment_name: str, deployments_root: str | None = None) -> str:
    root = os.path.abspath(deployments_root or _default_live_deployments_root())
    return os.path.join(root, _sanitize_deployment_name(deployment_name))


def _deployment_state_path(deployment_name: str, deployments_root: str | None = None) -> str:
    return os.path.join(_deployment_dir_for_name(deployment_name, deployments_root), "deployment_state.json")


def _shared_live_cluster_root(deployments_root: str | None = None) -> str:
    return os.path.join(os.path.abspath(deployments_root or _default_live_deployments_root()), "_shared")


def _shared_live_cluster_state_path(deployments_root: str | None = None) -> str:
    return os.path.join(_shared_live_cluster_root(deployments_root), "cluster_state.json")


def _shared_live_cluster_name() -> str:
    return f"{LIVE_CLUSTER_PREFIX}-shared"


def _shared_live_cluster_kubeconfig_path(deployments_root: str | None = None) -> str:
    return os.path.join(_shared_live_cluster_root(deployments_root), "kubeconfigs", "shared.kubeconfig")


def _list_active_live_deployments(deployments_root: str | None = None) -> list[str]:
    root = os.path.abspath(deployments_root or _default_live_deployments_root())
    if not os.path.isdir(root):
        return []

    active_deployments: list[str] = []
    for entry in os.scandir(root):
        if not entry.is_dir():
            continue
        if entry.name == "_shared":
            continue
        state_path = os.path.join(entry.path, "deployment_state.json")
        if os.path.exists(state_path):
            active_deployments.append(entry.name)
    return sorted(active_deployments)


def _remove_shared_live_cluster_state(deployments_root: str | None = None) -> None:
    state_path = _shared_live_cluster_state_path(deployments_root)
    if os.path.exists(state_path):
        os.remove(state_path)


def _load_shared_live_cluster_state(deployments_root: str | None = None) -> SharedLiveClusterState | None:
    state_path = _shared_live_cluster_state_path(deployments_root)
    if not os.path.exists(state_path):
        return None
    return SharedLiveClusterState.load(state_path)


def _resolve_cli_app_name(app_name: str) -> str:
    normalized = app_name.strip()
    alias_key = normalized.lower().replace("-", "_").replace(" ", "_")
    if alias_key in CLI_APP_NAME_ALIASES:
        return CLI_APP_NAME_ALIASES[alias_key]

    for display_name in CLI_APP_NAME_ALIASES.values():
        if normalized.lower() == display_name.lower():
            return display_name

    raise ValueError(
        f"Unknown app '{app_name}'. Valid app names: {sorted(CLI_APP_NAME_ALIASES)}"
    )


def _cluster_name_for_live_deployment(deployment_name: str) -> str:
    cluster_safe_name = _sanitize_deployment_name(deployment_name).replace("_", "-")
    return f"{LIVE_CLUSTER_PREFIX}-{cluster_safe_name}"[:60]


def _pick_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _stop_trace_port_forward(app) -> None:
    trace_api = getattr(app, "trace_api", None)
    if trace_api and hasattr(trace_api, "stop_port_forward"):
        try:
            trace_api.stop_port_forward()
        except Exception as exc:
            logger.warning(f"Failed to stop trace port-forward for live deployment: {exc}")


def _start_frontend_port_forward(
    *,
    kubeconfig_path: str,
    namespace: str,
    service_name: str,
    target_port: int,
    local_port: int | None = None,
    log_path: str | None = None,
) -> FrontendPortForwardInfo | None:
    chosen_port = local_port or _pick_free_local_port()
    log_target = log_path or os.devnull
    env = os.environ.copy()
    env["KUBECONFIG"] = kubeconfig_path
    env["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path

    with open(log_target, "a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            [
                "kubectl",
                "--kubeconfig",
                kubeconfig_path,
                "-n",
                namespace,
                "port-forward",
                f"svc/{service_name}",
                f"{chosen_port}:{target_port}",
                "--address",
                "127.0.0.1",
            ],
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
            text=True,
            env=env,
        )

    time.sleep(2)
    if process.poll() is not None:
        logger.warning(
            "Frontend port-forward exited early for service "
            f"{service_name} in namespace {namespace}; see {log_target}"
        )
        return None

    return FrontendPortForwardInfo(
        local_port=chosen_port,
        pid=process.pid,
        url=f"http://127.0.0.1:{chosen_port}",
    )


def _terminate_port_forward(pid: int | None) -> None:
    _terminate_background_process(pid, description="frontend port-forward")


def _terminate_background_process(pid: int | None, *, description: str) -> None:
    if not pid:
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        logger.warning(f"Failed to terminate {description} process group {pid}: {exc}")


def _write_live_k8s_proxy_kubeconfig(*, listen_port: int, output_path: str) -> str:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    kubeconfig = f"""apiVersion: v1
kind: Config
current-context: sregym-agent
clusters:
- name: sregym-proxy
  cluster:
    server: http://127.0.0.1:{listen_port}
    insecure-skip-tls-verify: true
contexts:
- name: sregym-agent
  context:
    cluster: sregym-proxy
    user: sregym-agent
users:
- name: sregym-agent
  user: {{}}
"""
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(kubeconfig)
    return output_path


def _wait_for_local_listener(*, port: int, process: subprocess.Popen, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited with code {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)

    raise TimeoutError(f"Timed out waiting for local listener on port {port}")


def _start_live_k8s_proxy(
    *,
    kubeconfig_path: str,
    deployment_dir: str,
    listen_port: int | None = None,
    log_path: str | None = None,
) -> K8sProxyInfo:
    chosen_port = listen_port or _pick_free_local_port()
    log_target = log_path or os.devnull
    proxy_kubeconfig_path = os.path.join(deployment_dir, "kubeconfigs", "agent-proxy.kubeconfig")
    env = os.environ.copy()
    env["KUBECONFIG"] = kubeconfig_path
    env["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path
    env["PYTHONUNBUFFERED"] = "1"

    command = [
        sys.executable,
        os.path.abspath(__file__),
        "serve-k8s-proxy",
        "--kubeconfig-path",
        kubeconfig_path,
        "--listen-port",
        str(chosen_port),
    ]

    with open(log_target, "a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
            text=True,
            env=env,
        )

    try:
        _wait_for_local_listener(port=chosen_port, process=process)
    except Exception as exc:
        _terminate_background_process(process.pid, description="K8s proxy")
        raise RuntimeError(f"Failed to start K8s proxy on port {chosen_port}; see {log_target}") from exc

    _write_live_k8s_proxy_kubeconfig(listen_port=chosen_port, output_path=proxy_kubeconfig_path)
    return K8sProxyInfo(
        port=chosen_port,
        pid=process.pid,
        url=f"http://127.0.0.1:{chosen_port}",
        kubeconfig_path=proxy_kubeconfig_path,
    )


def _terminate_live_k8s_proxy(pid: int | None) -> None:
    _terminate_background_process(pid, description="K8s proxy")


def _attach_live_cluster(cluster_name: str, kubeconfig_path: str) -> None:
    os.environ["KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_BASE_KUBECONFIG"] = kubeconfig_path
    os.environ["SREGYM_KIND_CLUSTER_NAME"] = cluster_name
    logger.info(f"Live deployment reusing cluster {cluster_name}; kubeconfig={kubeconfig_path}")


def _create_or_reuse_shared_live_cluster(
    *,
    deployments_root: str | None = None,
    recreate: bool = False,
) -> tuple[SharedLiveClusterState, bool]:
    shared_state = _load_shared_live_cluster_state(deployments_root)
    if shared_state and not recreate:
        ok, reason = _existing_cluster_is_reusable(shared_state.cluster_name, shared_state.kubeconfig_path)
        if ok:
            _attach_live_cluster(shared_state.cluster_name, shared_state.kubeconfig_path)
            return shared_state, True
        logger.info(
            f"Shared live cluster {shared_state.cluster_name} is not reusable: {reason}. Recreating it."
        )

    cluster_name = _shared_live_cluster_name()
    kubeconfig_path = _shared_live_cluster_kubeconfig_path(deployments_root)
    os.makedirs(os.path.dirname(kubeconfig_path), exist_ok=True)
    _create_kind_cluster(cluster_name, kubeconfig_path)
    shared_state = SharedLiveClusterState(
        cluster_name=cluster_name,
        kubeconfig_path=kubeconfig_path,
        baseline=None,
        created_at=datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )
    shared_state.write(_shared_live_cluster_state_path(deployments_root))
    logger.info(f"Shared live cluster ready: {cluster_name}, kubeconfig={kubeconfig_path}")
    return shared_state, False


def _resolve_live_problem(conductor: Conductor, *, problem_id: str | None, app_name: str | None):
    if problem_id:
        problem = conductor.problems.get_problem_instance(problem_id)
        if app_name:
            expected_app_name = _resolve_cli_app_name(app_name)
            if problem.app.name != expected_app_name:
                raise ValueError(
                    f"Problem '{problem_id}' deploys '{problem.app.name}', not '{expected_app_name}'."
                )
        if problem.requires_khaos() and conductor.kubectl.is_emulated_cluster():
            raise RuntimeError(
                f"Problem '{problem_id}' requires Khaos and cannot be deployed on an emulated cluster."
            )
        return problem

    resolved_app_name = _resolve_cli_app_name(app_name or "")
    app = conductor.apps.get_app_instance(resolved_app_name)
    return _LiveAppProblem(app)


def _assign_live_problem(conductor: Conductor, *, problem_id: str | None, problem) -> None:
    conductor.problem_id = problem_id
    conductor.problem = problem
    conductor.app = problem.app


def _cleanup_live_problem_environment(
    *,
    conductor: Conductor,
    problem_id: str | None,
    problem,
    baseline: PersistedClusterBaseline | None,
) -> None:
    _assign_live_problem(conductor, problem_id=problem_id, problem=problem)
    if baseline is not None:
        conductor.cluster_state.baseline = baseline
        conductor._baseline_captured = True
    problem.recover_fault()
    conductor.undeploy_app()
    if baseline is not None:
        conductor.cluster_state.reconcile_to_baseline()


def _build_live_state(
    *,
    deployment_name: str,
    deployment_dir: str,
    cluster_name: str,
    kubeconfig_path: str,
    shared_cluster: bool,
    cluster_reused: bool,
    app,
    problem_id: str | None,
    port_forward: FrontendPortForwardInfo | None,
    k8s_proxy: K8sProxyInfo | None,
) -> LiveDeploymentState:
    return LiveDeploymentState(
        deployment_name=deployment_name,
        deployment_dir=deployment_dir,
        cluster_name=cluster_name,
        kubeconfig_path=kubeconfig_path,
        shared_cluster=shared_cluster,
        cluster_reused=cluster_reused,
        app_name=app.name,
        namespace=app.namespace,
        problem_id=problem_id,
        frontend_service=app.frontend_service,
        frontend_target_port=app.frontend_port,
        frontend_local_port=port_forward.local_port if port_forward else None,
        frontend_url=port_forward.url if port_forward else None,
        frontend_port_forward_pid=port_forward.pid if port_forward else None,
        k8s_proxy_port=k8s_proxy.port if k8s_proxy else None,
        k8s_proxy_pid=k8s_proxy.pid if k8s_proxy else None,
        k8s_proxy_url=k8s_proxy.url if k8s_proxy else None,
        k8s_proxy_kubeconfig_path=k8s_proxy.kubeconfig_path if k8s_proxy else None,
        created_at=datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )


def deploy_live_environment(
    *,
    problem_id: str | None,
    app_name: str | None,
    deployment_name: str | None = None,
    deployments_root: str | None = None,
    frontend_local_port: int | None = None,
    with_k8s_proxy: bool = False,
    recreate_cluster: bool = False,
) -> LiveDeploymentState:
    if not problem_id and not app_name:
        raise ValueError("deploy requires either problem_id or app_name")

    resolved_name = _sanitize_deployment_name(deployment_name or _default_deployment_name(problem_id, app_name))
    deployment_dir = _deployment_dir_for_name(resolved_name, deployments_root)
    os.makedirs(deployment_dir, exist_ok=True)
    state_path = os.path.join(deployment_dir, "deployment_state.json")
    if os.path.exists(state_path):
        raise FileExistsError(
            f"Live deployment '{resolved_name}' already exists. Use undeploy first or choose a different name."
        )
    active_deployments = _list_active_live_deployments(deployments_root)
    if active_deployments:
        raise FileExistsError(
            "Only one live deployment can be active at a time. "
            f"Undeploy {active_deployments[0]!r} before creating another one."
        )

    cluster_name = ""
    cluster_reused = False
    shared_cluster_state: SharedLiveClusterState | None = None
    conductor: Conductor | None = None
    problem = None
    port_forward: FrontendPortForwardInfo | None = None
    k8s_proxy: K8sProxyInfo | None = None
    try:
        shared_cluster_state, cluster_reused = _create_or_reuse_shared_live_cluster(
            deployments_root=deployments_root,
            recreate=recreate_cluster,
        )
        cluster_name = shared_cluster_state.cluster_name
        kubeconfig_path = shared_cluster_state.kubeconfig_path
        conductor = Conductor()
        problem = _resolve_live_problem(conductor, problem_id=problem_id, app_name=app_name)
        _assign_live_problem(conductor, problem_id=problem_id, problem=problem)
        conductor.deploy_app()
        if conductor.cluster_state.baseline is not None:
            shared_cluster_state.baseline = PersistedClusterBaseline.from_cluster_baseline(
                conductor.cluster_state.baseline
            )
            shared_cluster_state.write(_shared_live_cluster_state_path(deployments_root))
        if problem_id:
            problem.inject_fault()
            problem.verify_fault_applied()
        _stop_trace_port_forward(problem.app)

        port_forward = _start_frontend_port_forward(
            kubeconfig_path=kubeconfig_path,
            namespace=problem.app.namespace,
            service_name=problem.app.frontend_service,
            target_port=problem.app.frontend_port,
            local_port=frontend_local_port,
            log_path=os.path.join(deployment_dir, "frontend-port-forward.log"),
        )
        if with_k8s_proxy:
            k8s_proxy = _start_live_k8s_proxy(
                kubeconfig_path=kubeconfig_path,
                deployment_dir=deployment_dir,
                log_path=os.path.join(deployment_dir, "k8s-proxy.log"),
            )
        state = _build_live_state(
            deployment_name=resolved_name,
            deployment_dir=deployment_dir,
            cluster_name=cluster_name,
            kubeconfig_path=kubeconfig_path,
            shared_cluster=True,
            cluster_reused=cluster_reused,
            app=problem.app,
            problem_id=problem_id,
            port_forward=port_forward,
            k8s_proxy=k8s_proxy,
        )
        state.write(state_path)
        return state
    except Exception:
        _terminate_live_k8s_proxy(k8s_proxy.pid if k8s_proxy else None)
        _terminate_port_forward(port_forward.pid if port_forward else None)
        if cluster_name:
            if cluster_reused and conductor is not None and problem is not None:
                try:
                    _cleanup_live_problem_environment(
                        conductor=conductor,
                        problem_id=problem_id,
                        problem=problem,
                        baseline=shared_cluster_state.baseline if shared_cluster_state else None,
                    )
                except Exception as cleanup_exc:
                    logger.warning(f"Failed to clean up reused shared cluster after live deploy error: {cleanup_exc}")
            else:
                _delete_live_cluster(cluster_name)
                _remove_shared_live_cluster_state(deployments_root)
        raise


def undeploy_live_environment(
    deployment_name: str,
    *,
    deployments_root: str | None = None,
    delete_cluster: bool = False,
) -> LiveUndeployResult:
    state_path = _deployment_state_path(deployment_name, deployments_root)
    if not os.path.exists(state_path):
        raise FileNotFoundError(
            f"Live deployment '{_sanitize_deployment_name(deployment_name)}' not found at {state_path}"
        )

    state = LiveDeploymentState.load(state_path)
    _terminate_port_forward(state.frontend_port_forward_pid)
    _terminate_live_k8s_proxy(state.k8s_proxy_pid)
    cluster_deleted = False

    if state.shared_cluster and not delete_cluster:
        shared_cluster_state = _load_shared_live_cluster_state(deployments_root)
        if shared_cluster_state and shared_cluster_state.baseline is not None:
            ok, reason = _existing_cluster_is_reusable(
                shared_cluster_state.cluster_name,
                shared_cluster_state.kubeconfig_path,
            )
            if ok:
                try:
                    _attach_live_cluster(shared_cluster_state.cluster_name, shared_cluster_state.kubeconfig_path)
                    conductor = Conductor()
                    problem = _resolve_live_problem(
                        conductor,
                        problem_id=state.problem_id,
                        app_name=state.app_name,
                    )
                    _cleanup_live_problem_environment(
                        conductor=conductor,
                        problem_id=state.problem_id,
                        problem=problem,
                        baseline=shared_cluster_state.baseline,
                    )
                except Exception as exc:
                    logger.warning(
                        f"Shared-cluster cleanup failed for deployment {state.deployment_name}: {exc}. "
                        "Deleting the cluster instead."
                    )
                    _delete_live_cluster(state.cluster_name)
                    _remove_shared_live_cluster_state(deployments_root)
                    cluster_deleted = True
            else:
                logger.warning(
                    f"Shared-cluster cleanup fallback: cluster {shared_cluster_state.cluster_name} "
                    f"is not reusable ({reason}). Deleting it."
                )
                _delete_live_cluster(state.cluster_name)
                _remove_shared_live_cluster_state(deployments_root)
                cluster_deleted = True
        else:
            logger.warning("Shared live cluster state is missing or incomplete. Deleting the cluster instead.")
            _delete_live_cluster(state.cluster_name)
            _remove_shared_live_cluster_state(deployments_root)
            cluster_deleted = True
    else:
        _delete_live_cluster(state.cluster_name)
        if state.shared_cluster:
            _remove_shared_live_cluster_state(deployments_root)
        cluster_deleted = True

    os.remove(state_path)
    return LiveUndeployResult(state=state, cluster_deleted=cluster_deleted)


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

        def write_error_result(problem_id: str, error_message: str, sequence_index: int = None,
                              start_date_time: str = None, problem_run_dir: str = None):
            """Write a structured result row even when execution fails before grading."""
            if not agent_to_run:
                return
            current_date_time = start_date_time or get_current_datetime_formatted()
            if problem_run_dir is None:
                raise RuntimeError("write_error_result requires problem_run_dir")
            csv_path = os.path.join(problem_run_dir, f"results_{current_date_time}.csv")
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

        if problem_queue:

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

        for item in problem_iterator:
            if isinstance(item, tuple):
                seq_idx, pid = item
            else:
                seq_idx, pid = None, item

            # Unique key for this sequence slot (disambiguates repeated pids)
            seq_key = f"{seq_idx:05d}:{pid}" if seq_idx is not None else pid

            # Resolve per-problem run directory (reuse existing if any, else create a new one).
            # Layout: <experiment_log_dir>/problem_runs/<MMDD_HHMM>_[<seq:05d>_]<pid>/
            pid_suffix = f"{seq_idx:05d}_{pid}" if seq_idx is not None else pid
            runs_root = os.path.join(experiment_log_dir, "problem_runs")
            existing_run_dirs = sorted(glob.glob(os.path.join(runs_root, f"*_{pid_suffix}")))
            if existing_run_dirs:
                problem_run_dir = existing_run_dirs[-1]
            else:
                problem_dir_ts = get_current_datetime_formatted()
                problem_run_dir = os.path.join(runs_root, f"{problem_dir_ts}_{pid_suffix}")
            os.makedirs(problem_run_dir, exist_ok=True)
            agent_log_dir = os.path.join(problem_run_dir, "agent")
            os.makedirs(agent_log_dir, exist_ok=True)

            # Check for existing results (Resume capability) under the problem run dir.
            completed_iterations = 0
            if agent_to_run and not use_external_harness:
                existing_files = glob.glob(os.path.join(problem_run_dir, "results_*.csv"))

                for f_path in existing_files:
                    if is_result_complete(f_path):
                        completed_iterations += 1

                if completed_iterations >= repeat:
                    label = f"[{seq_idx:05d}] {pid}" if seq_idx is not None else pid
                    console.log(
                        f"⏭️  Skipping problem '{label}': Found {completed_iterations}/{repeat} completed results."
                    )

                    _safe_status_update(status_dict, seq_key, {
                        "status": "Completed (Resumed)",
                        "pid": pid,
                        "start_time": time.time(),
                        "elapsed": 0.0,
                        "worker_id": worker_id,
                    })
                    continue
                elif completed_iterations > 0:
                    label = f"[{seq_idx:05d}] {pid}" if seq_idx is not None else pid
                    console.log(
                        f"⏯️  Resuming problem '{label}': {completed_iterations}/{repeat} iterations already completed."
                    )

            # Prepare for logging redirection if in parallel mode
            redirect_ctx = (
                open(os.path.join(problem_run_dir, "run.log"), "w") if status_dict is not None else None
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
                _safe_status_update(status_dict, seq_key, {
                    "status": "Deploying App",
                    "pid": pid,
                    "start_time": time.time(),
                    "elapsed": 0.0,
                    "worker_id": worker_id,
                })

            # Whether the *latest* iteration of this problem was a full success
            # (diagnosis AND mitigation). Surfaced into status_dict on Completed
            # so the supervisor's adaptive scheduler can react to outcomes.
            iteration_solved: bool = False

            try:
                for iteration in range(completed_iterations, repeat):
                    iteration_start_time = get_current_datetime_formatted()
                    console.log(f"\n🔍 Starting problem: {pid} (Run {iteration + 1}/{repeat})")

                    conductor.problem_id = pid

                    # Define callback to update status from conductor
                    def update_conductor_status(status, _seq_key=seq_key, _pid=pid):
                        if status_dict is not None:
                            current_info = _safe_status_read(status_dict, _seq_key, {})
                            start_time = current_info.get("start_time", time.time()) if isinstance(current_info, dict) else time.time()
                            _safe_status_update(status_dict, _seq_key, {
                                "status": status,
                                "pid": _pid,
                                "start_time": start_time,
                                "elapsed": time.time() - start_time,
                                "worker_id": worker_id,
                            })

                    conductor.set_status_callback(update_conductor_status)

                    result = await conductor.start_problem()
                    if result == StartProblemResult.SKIPPED_KHAOS_REQUIRED:
                        console.log(f"⏭️  Skipping problem '{pid}': requires Khaos but running on emulated cluster")
                        _info = _safe_status_read(status_dict, seq_key, {})
                        _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                        _safe_status_update(status_dict, seq_key, {
                            "status": "Skipped (Khaos Req)",
                            "pid": pid,
                            "start_time": _st,
                            "elapsed": time.time() - _st,
                            "worker_id": worker_id,
                        })
                        continue

                    # If using external harness, fault is injected - exit now
                    if use_external_harness:
                        console.log(f"✅ Fault injected for problem '{pid}'. Exiting for external harness.")
                        return []

                    # Agent base dir is retained for AGENT_OUTPUT_FILES summary-dir plumbing only.
                    # Per-problem agent_log_dir is resolved above (problem_run_dir/agent).
                    agent_base_dir = os.path.join(experiment_log_dir, agent_to_run)
                    agent_registration = None

                    if not use_external_harness:
                        _info = _safe_status_read(status_dict, seq_key, {})
                        _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                        _safe_status_update(status_dict, seq_key, {
                            "status": "Agent Running",
                            "pid": pid,
                            "start_time": _st,
                            "elapsed": time.time() - _st,
                            "worker_id": worker_id,
                        })

                        # Defensive: ensure no stale agent from previous problem before starting
                        LAUNCHER.cleanup_agent(agent_to_run)
                        _default_registry = Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml"
                        _registry_path = Path(os.environ.get("SREGYM_AGENT_REGISTRY", _default_registry))
                        agent_registration = get_agent(agent_to_run, path=_registry_path)
                        if agent_registration:
                            extra_args = ""
                            # Pass explicit log dir to external-summarizer agents (e.g. gemini_cli)
                            if agent_to_run in AGENT_OUTPUT_FILES:
                                extra_args += f" --logs-dir {agent_log_dir} --summary-dir {agent_base_dir}"

                            # Crucible handles summarization internally via --summary-dir
                            if agent_to_run in AGENT_LT_SUMMARY:
                                extra_args += f" --logs-dir {agent_log_dir}"
                            if agent_to_run in AGENT_LT_SUMMARY and enable_summary:
                                effective_summary_model = summary_model or os.environ.get("MODEL_ID", "gpt-4o")
                                kb_dir = os.path.join(experiment_log_dir, "kb")
                                extra_args += (
                                    f" --summary-dir {kb_dir} --summary-model {effective_summary_model}"
                                )

                            if enable_summary and agent_to_run in AGENT_OUTPUT_FILES:
                                extra_args += " --enable-summary"
                            if not inject_summary:
                                extra_args += " --no-inject-summary"

                            await LAUNCHER.ensure_started(agent_registration, extra_args=extra_args.strip())

                    # Poll until grading completes or agent exits.
                    # "awaiting_cleanup" is a terminal-for-the-agent state when deferred
                    # cleanup is enabled — the agent is expected to do post-submit work and
                    # then POST /cleanup (handled below after the natural-exit wait).
                    agent_exit_code: int | None = None
                    _terminal_stages = {"done", "awaiting_cleanup"}
                    while conductor.submission_stage not in _terminal_stages:
                        if status_dict is not None:
                            current_stage = conductor.submission_stage or "Running"
                            _info = _safe_status_read(status_dict, seq_key, {})
                            _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                            _safe_status_update(status_dict, seq_key, {
                                "status": f"Agent: {current_stage}",
                                "pid": pid,
                                "start_time": _st,
                                "elapsed": time.time() - _st,
                                "worker_id": worker_id,
                            })

                        # Check if agent process has exited
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            agent_proc.proc.poll()
                            if agent_proc.proc.returncode is not None:
                                agent_exit_code = agent_proc.proc.returncode
                                console.log(f"⚠️  Agent process exited with return code {agent_exit_code}")
                                break
                        await asyncio.sleep(1)

                    _info = _safe_status_read(status_dict, seq_key, {})
                    _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                    _safe_status_update(status_dict, seq_key, {
                        "status": "Cleaning Up",
                        "pid": pid,
                        "start_time": _st,
                        "elapsed": time.time() - _st,
                        "worker_id": worker_id,
                    })

                    console.log(f"✅ Completed {pid}: results={conductor.results}")

                    # Wait for agent process to complete naturally before cleanup
                    # This allows the agent to finish saving trajectories and other cleanup tasks
                    if not use_external_harness:
                        agent_proc = LAUNCHER._procs.get(agent_to_run)
                        if agent_proc:
                            timeout = resolve_graceful_exit_timeout_seconds(agent_registration)
                            if timeout is None:
                                console.log(
                                    "⏳ Waiting for agent process to complete "
                                    "(no timeout for post-run finalization)..."
                                )
                            else:
                                console.log(
                                    f"⏳ Waiting for agent process to complete "
                                    f"(timeout: {int(DEFAULT_GRACEFUL_EXIT_TIMEOUT_SECONDS)}s)..."
                                )
                            completed = await wait_for_process_exit(
                                agent_proc.proc,
                                timeout_seconds=timeout,
                            )
                            if completed:
                                console.log(
                                    f"✅ Agent process completed with return code {agent_proc.proc.returncode}"
                                )
                            else:
                                console.log(
                                    f"⚠️  Agent process did not complete within {int(timeout)}s, "
                                    "will force cleanup"
                                )

                    # Safety-net: if the conductor is still holding at "awaiting_cleanup"
                    # (deferred-cleanup path) OR was left mid-stage by a crashed agent,
                    # run teardown now so the cluster is clean before the next problem.
                    # Idempotent via conductor._cleanup_lock.
                    if conductor.submission_stage != "done":
                        try:
                            await asyncio.to_thread(conductor.force_cleanup)
                        except Exception as e:
                            console.log(f"⚠️  Post-agent force_cleanup failed: {e}")

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
                    iteration_solved = bool(snapshot.get("Diagnosis.success")) and bool(
                        snapshot.get("Mitigation.success")
                    )
                    all_results_for_agent.append(snapshot)

                    fieldnames = sorted(snapshot.keys())
                    current_date_time = iteration_start_time

                    # Write results into the per-problem run dir.
                    csv_path = os.path.join(problem_run_dir, f"results_{current_date_time}.csv")
                    with open(csv_path, "w", newline="") as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
                        writer.writeheader()
                        writer.writerows([snapshot])
                    if snapshot.get("agent_error"):
                        logger.warning(
                            f"⚠️  Problem {pid} for agent {agent_to_run} finished with agent crash "
                            f"(exit {agent_exit_code})! Results written to {csv_path}"
                        )
                        _info = _safe_status_read(status_dict, seq_key, {})
                        _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                        _safe_status_update(status_dict, seq_key, {
                            "status": "Error",
                            "pid": pid,
                            "start_time": _st,
                            "elapsed": time.time() - _st,
                            "worker_id": worker_id,
                        })
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
                logger.error(f"Error running problem {pid}:", exc_info=True)
                # Attempt cleanup so stale cluster-scoped resources don't poison the next problem
                try:
                    conductor.undeploy_app()
                    if conductor._baseline_captured:
                        conductor.cluster_state.reconcile_to_baseline()
                except Exception as cleanup_err:
                    console.log(f"⚠️  Post-error cleanup also failed: {cleanup_err}")
                if not use_external_harness:
                    write_error_result(pid, str(e), sequence_index=seq_idx,
                                       start_date_time=iteration_start_time,
                                       problem_run_dir=problem_run_dir)
                _info = _safe_status_read(status_dict, seq_key, {})
                _st = _info.get("start_time", time.time()) if isinstance(_info, dict) else time.time()
                _safe_status_update(status_dict, seq_key, {
                    "status": "Error",
                    "pid": pid,
                    "start_time": _st,
                    "elapsed": time.time() - _st,
                    "worker_id": worker_id,
                })
                # Do not raise e; continue to next problem
            finally:
                # Ensure agent is cleaned up even if an error occurred
                if not use_external_harness:
                    LAUNCHER.cleanup_agent(agent_to_run)
                    await asyncio.sleep(1)  # Allow process group to fully tear down

                _current = _safe_status_read(status_dict, seq_key, {})
                _cur_status = _current.get("status", "") if isinstance(_current, dict) else ""
                if _cur_status != "Error":
                    _st = _current.get("start_time", time.time()) if isinstance(_current, dict) else time.time()
                    _safe_status_update(status_dict, seq_key, {
                        "status": "Completed",
                        "pid": pid,
                        "start_time": _st,
                        "elapsed": time.time() - _st,
                        "worker_id": worker_id,
                        "solved": iteration_solved,
                    })
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
        )
        main.results = results
    except Exception as e:
        logger.error(f"Driver loop crashed: {e}")
    finally:
        # ⬇️ Ask the API server (running in main thread) to stop so we can write CSV
        request_shutdown()


def _delete_live_cluster(cluster_name: str) -> None:
    _delete_kind_cluster(cluster_name)


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


def worker_main(args, worker_id, problem_queue, experiment_log_dir, status_dict):
    """Worker function for parallel execution."""

    def _ignore_signal_handler(signum, frame):
        """Ignore SIGTERM/SIGHUP so workers survive process-group signals.
        Workers exit cleanly via the None sentinel on their queue."""
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.info(f"Worker {worker_id} received {sig_name} — ignoring")

    def _sigint_handler(signum, frame):
        """On SIGINT (Ctrl-C), clean up agent subprocesses and exit."""
        LAUNCHER.cleanup_all_agents(timeout=3)
        os._exit(0)

    signal.signal(signal.SIGTERM, _ignore_signal_handler)
    signal.signal(signal.SIGINT, _sigint_handler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _ignore_signal_handler)

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

            _safe_status_update(status_dict, _worker_meta_key(worker_id), {
                "status": "Creating cluster",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            })
            cluster_name, _ = _create_worker_cluster(worker_id, experiment_log_dir)
            _safe_status_update(status_dict, _worker_meta_key(worker_id), {
                "status": f"Cluster ready ({cluster_name})",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            })
            # Run main with the private queue. It will block until tasks arrive.
            main(
                args,
                problem_queue=problem_queue,
                problem_list=None,  # No pre-assigned list, everything via queue
                experiment_log_dir=experiment_log_dir,
                status_dict=status_dict,
                worker_id=worker_id,
            )
        except Exception as e:
            _safe_status_update(status_dict, _worker_meta_key(worker_id), {
                "status": f"Worker setup failed: {e}",
                "start_time": time.time(),
                "elapsed": 0.0,
                "worker_id": worker_id,
            })
            raise
        finally:
            _delete_worker_cluster(cluster_name)


def run_parallel(args):
    """Split problems and run in parallel workers."""

    from sregym.conductor.problems.registry import ProblemRegistry

    registry = ProblemRegistry()
    # Use the same logic as conductor to get problem IDs
    # If args.problem is set, run only that (but parallel doesn't make much sense unless repeat > 1)
    tasklist_path = getattr(args, "tasklist", None)

    if args.problem:
        all_problems = [args.problem]
    else:
        all_problems = registry.get_problem_ids(tasklist_path=tasklist_path)
        # tasklist.yml may contain stale problem IDs; filter early to avoid runtime failures in workers
        all_problem_ids = set(registry.get_problem_ids(all=True))
        unknown_problem_ids = sorted(set(all_problems) - all_problem_ids)
        if unknown_problem_ids:
            logger.warning(f"These problem IDs are not in the registry and will be skipped: {unknown_problem_ids}")
            all_problems = [pid for pid in all_problems if pid in all_problem_ids]

    problem_specs = getattr(args, "problem_spec", None)
    if problem_specs:
        specs = set(problem_specs)
        filtered = [p for p in all_problems if any(p == s or p.startswith(s + "_") for s in specs)]
        unmatched = sorted(s for s in specs if not any(p == s or p.startswith(s + "_") for p in all_problems))
        if unmatched:
            logger.warning(f"--problem-spec: no problems matched spec(s): {unmatched}")
        all_problems = filtered

    if not all_problems:
        logger.error("No problems found to run.")
        sys.exit(1)

    # Create or reuse experiment log directory
    is_resuming = False
    if args.experiment_dir:
        experiment_log_dir = os.path.abspath(args.experiment_dir)
        if os.path.exists(experiment_log_dir):
            # Auto-resume: existing dir with results
            is_resuming = True
            logger.info(f"Resuming experiment from: {experiment_log_dir}")
        else:
            os.makedirs(experiment_log_dir, exist_ok=True)
            logger.info(f"Experiment logs will be stored in: {experiment_log_dir}")
    else:
        session_timestamp = get_current_datetime_formatted()
        os.makedirs("logs", exist_ok=True)
        dir_name = session_timestamp
        if args.agent:
            dir_name = f"{session_timestamp}_{args.agent}"
        experiment_log_dir = os.path.abspath(f"logs/{dir_name}")
        os.makedirs(experiment_log_dir, exist_ok=True)
        logger.info(f"Experiment logs will be stored in: {experiment_log_dir}")

    if not is_resuming:
        # Copy seed summary into agent summary dir
        if args.seed_summary:
            agent_kb_dir = os.path.join(experiment_log_dir, "kb")
            os.makedirs(agent_kb_dir, exist_ok=True)
            dest_path = os.path.join(agent_kb_dir, "long_term_summary.md")
            shutil.copy2(args.seed_summary, dest_path)
            logger.info(f"Copied seed summary to {dest_path}")
            lessons_src = os.path.join(os.path.dirname(args.seed_summary), "operational_lessons.md")
            if os.path.isfile(lessons_src):
                dest_lessons = os.path.join(agent_kb_dir, "operational_lessons.md")
                shutil.copy2(lessons_src, dest_lessons)
                logger.info(f"Copied operational lessons to {dest_lessons}")

    # Set log file for parallel runner (always, even when resuming — the supervisor
    # redirects stdout/stderr to /dev/null for the progress display, so file logging
    # is the only way to diagnose supervisor-level issues).
    session_timestamp = get_current_datetime_formatted()
    log_file_path = os.path.join(experiment_log_dir, f"sregym_supervisor_{session_timestamp}.log")
    os.environ["SREGYM_LOG_FILE"] = log_file_path
    init_logger()

    # Handle variant stream mode or sequence mode
    sequence = None
    sequence_start_idx = 0
    adaptive_scheduler = None
    if getattr(args, "variants", False):
        from sregym.conductor.problems.variant_generator import (
            AdaptiveScheduler,
            generate_variant_stream,
            generate_variant_stream_by_class,
            generate_variant_stream_grouped,
        )

        try:
            variant_ids = registry.get_variant_ids(spec_names=args.variant_spec)
        except ValueError as exc:
            logger.error(str(exc))
            sys.exit(1)
        if not variant_ids:
            logger.error("No variant problems found in registry.")
            sys.exit(1)

        if args.variant_order == "adaptive":
            # Adaptive mode: no pre-generated sequence. The scheduler dispatches
            # problems on demand based on outcomes recorded by the supervisor.
            adaptive_scheduler = AdaptiveScheduler(
                variant_ids=variant_ids,
                consec_solves_to_stop=args.variant_adaptive_consec_solves,
                max_per_class=args.variant_max_per_class,
                seed=args.variant_seed,
                max_total=args.variant_count if args.variant_count > 0 else None,
            )
            variant_state_path = os.path.join(experiment_log_dir, "variant_state.json")
            with open(variant_state_path, "w") as f:
                json.dump({
                    "seed": args.variant_seed,
                    "order": args.variant_order,
                    "max_per_class": args.variant_max_per_class,
                    "consec_solves_to_stop": args.variant_adaptive_consec_solves,
                    "max_total": args.variant_count if args.variant_count > 0 else None,
                    "variant_spec": args.variant_spec,
                    "total_variant_pool": len(variant_ids),
                    "class_order": adaptive_scheduler.class_order,
                }, f)
            spec_filter_msg = f", spec_filter={args.variant_spec}" if args.variant_spec else ""
            max_total_msg = f", max_total={args.variant_count}" if args.variant_count > 0 else ""
            logger.info(
                f"Variant stream (adaptive): {len(adaptive_scheduler.class_order)} classes "
                f"(seed={args.variant_seed}, max_per_class={args.variant_max_per_class}, "
                f"consec_solves_to_stop={args.variant_adaptive_consec_solves}"
                f"{max_total_msg}, pool={len(variant_ids)} variants{spec_filter_msg})."
            )

            # Resume: replay completed result CSVs through the scheduler so its
            # per-class state and seq_idx counter pick up where they left off.
            agent_to_run = args.agent
            replay_pattern = os.path.join(
                experiment_log_dir, f"*_*_*_{agent_to_run}_results.csv"
            )
            replay_entries: list[tuple[int, str, bool]] = []
            for f_path in glob.glob(replay_pattern):
                if not is_result_complete(f_path):
                    continue
                parsed = _parse_adaptive_csv_filename(f_path, agent_to_run)
                if parsed is None:
                    continue
                seq_idx, pid = parsed
                solved = _read_solved_from_result_csv(f_path)
                replay_entries.append((seq_idx, pid, solved))
            replay_entries.sort(key=lambda t: t[0])
            for _, pid, solved in replay_entries:
                adaptive_scheduler.record_completion(pid, solved=solved)
            if replay_entries:
                next_idx = replay_entries[-1][0] + 1
                adaptive_scheduler.set_next_seq_idx(next_idx)
                logger.info(
                    f"Adaptive resume: replayed {len(replay_entries)} completions; "
                    f"next seq_idx={next_idx}."
                )
            else:
                logger.info("Adaptive resume: no prior completions found.")
        else:
            stream_kwargs = dict(
                variant_ids=variant_ids,
                count=args.variant_count,
                offset=args.variant_offset,
                seed=args.variant_seed,
            )
            if args.variant_order == "flat":
                sequence = generate_variant_stream(**stream_kwargs)
            elif args.variant_order == "round_robin":
                sequence = generate_variant_stream_by_class(**stream_kwargs)
            else:  # grouped
                sequence = generate_variant_stream_grouped(
                    max_per_class=args.variant_max_per_class,
                    **stream_kwargs,
                )
            variant_state_path = os.path.join(experiment_log_dir, "variant_state.json")
            with open(variant_state_path, "w") as f:
                json.dump({
                    "seed": args.variant_seed,
                    "offset": args.variant_offset,
                    "count": args.variant_count,
                    "order": args.variant_order,
                    "max_per_class": args.variant_max_per_class,
                    "variant_spec": args.variant_spec,
                    "total_variant_pool": len(variant_ids),
                    "sequence": sequence,
                }, f)
            spec_filter_msg = f", spec_filter={args.variant_spec}" if args.variant_spec else ""
            logger.info(
                f"Variant stream: {len(sequence)} problems "
                f"(order={args.variant_order}, offset={args.variant_offset}, "
                f"seed={args.variant_seed}, pool={len(variant_ids)} variants{spec_filter_msg})."
            )

            # Determine start index by scanning for completed results
            agent_to_run = args.agent
            for idx, pid in enumerate(sequence):
                search_pattern = os.path.join(experiment_log_dir, f"*_{idx:05d}_{pid}_{agent_to_run}_results.csv")
                existing_files = glob.glob(search_pattern)
                completed = any(is_result_complete(f) for f in existing_files)
                if completed:
                    sequence_start_idx = idx + 1
                else:
                    break
            logger.info(f"Variant mode: starting from index {sequence_start_idx}/{len(sequence)}.")

    elif getattr(args, "sequence_len", 0) > 0:
        sequence_state_path = os.path.join(experiment_log_dir, "sequence_state.json")

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
    # Use plain multiprocessing.Queue (not manager.Queue) so that scheduling
    # survives a Manager crash — the queues use OS-level pipes, independent of
    # the Manager server process that backs status_dict.
    worker_queues = [multiprocessing.Queue() for _ in range(args.parallel)]

    # Filter problems if resuming (non-sequence mode)
    problems_to_run = []
    if adaptive_scheduler is not None:
        # Adaptive mode: dispatch is dynamic via the scheduler. We pass an empty
        # pending list and let the supervisor pull from the scheduler.
        problems_to_run = []
    elif sequence is not None:
        # Build pending list with (seq_idx, pid) tuples for queue-based dispatch
        problems_to_run = [(idx, pid) for idx, pid in enumerate(sequence) if idx >= sequence_start_idx]
    elif is_resuming:
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
                _safe_status_update(status_dict, pid, {
                    "status": "Completed (Resumed)",
                    "start_time": time.time(),
                    "elapsed": 0.0,
                    "worker_id": None,
                })
    else:
        problems_to_run = all_problems

    # Do not populate queues upfront. We will schedule them dynamically.
    pending_problems = list(problems_to_run)
    # Sort pending problems to run heaviest first? Or mixed?
    # Heaviest first is usually better for packing, but we have a simple limit.
    # Let's keep original order or shuffle. Original order is fine.

    _log_cpu_oversubscription(args.parallel)

    processes = []
    worker_map = {}  # Map process to worker ID
    if adaptive_scheduler is not None:
        logger.info(
            f"Running adaptive variant stream over {len(adaptive_scheduler.class_order)} "
            f"classes with {args.parallel} workers."
        )
    elif sequence is not None:
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
        )
        p.start()
        processes.append(p)
        worker_map[p] = i

    assigned_tasks = {}  # worker_id -> problem_id
    shutdown_sent = False

    # Make the supervisor resilient to SIGTERM/SIGHUP — log and ignore so workers
    # can finish.  Only SIGINT (Ctrl-C) should abort the experiment.
    def _supervisor_signal_handler(signum, frame):
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.warning(f"Supervisor received {sig_name} ({signum}) — ignoring, workers will continue")

    signal.signal(signal.SIGTERM, _supervisor_signal_handler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _supervisor_signal_handler)

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
            removed = [h for h in logger_obj.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)]
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
                manager_dead = False
                last_heartbeat_ts = 0.0
                while True:
                    # Exit when all worker processes have terminated
                    if not any(p.is_alive() for p in processes):
                        logger.info("All worker processes have terminated. Exiting monitoring loop.")
                        break

                    # When Manager is dead we lose status reporting but workers
                    # (using independent multiprocessing.Queue) keep running.
                    if manager_dead:
                        time.sleep(1)
                        continue

                    # --- SCHEDULING LOGIC ---
                    if not shutdown_sent:
                        # 1. Update Assigned Tasks (Check completion)
                        for wid in list(assigned_tasks.keys()):
                            pid = assigned_tasks[wid]
                            info = _safe_status_read(status_dict, pid)
                            if info:
                                status = info.get("status", "")
                                if (
                                    status.startswith("Completed")
                                    or status.startswith("Error")
                                    or status.startswith("Skipped")
                                ):
                                    # Adaptive mode: feed solved/not-solved into the
                                    # scheduler so it can decide what to dispatch
                                    # next. Only "Completed" counts as a real
                                    # attempt — Error/Skipped reflect infrastructure
                                    # issues that shouldn't burn the per-class
                                    # budget.
                                    if (
                                        adaptive_scheduler is not None
                                        and status.startswith("Completed")
                                    ):
                                        actual_pid = pid.split(":", 1)[1] if ":" in pid else pid
                                        solved = bool(info.get("solved", False))
                                        adaptive_scheduler.record_completion(actual_pid, solved=solved)
                                        logger.info(
                                            f"Adaptive: recorded completion for {actual_pid} "
                                            f"(solved={solved}). class_state="
                                            f"{adaptive_scheduler.class_state_summary()}"
                                        )
                                    del assigned_tasks[wid]

                        # 2. Assign New Tasks to Idle Workers
                        idle_workers = []
                        for i in range(args.parallel):
                            if (i not in assigned_tasks) and (i not in failed_workers_logged):
                                if processes[i].is_alive():
                                    idle_workers.append(i)

                        for wid in idle_workers:
                            if adaptive_scheduler is not None:
                                problem_to_assign = adaptive_scheduler.next_problem()
                                if problem_to_assign is None:
                                    break
                            else:
                                if not pending_problems:
                                    break
                                problem_to_assign = pending_problems.pop(0)

                            if isinstance(problem_to_assign, tuple):
                                s_idx, s_pid = problem_to_assign
                                assigned_tasks[wid] = f"{s_idx:05d}:{s_pid}"
                            else:
                                assigned_tasks[wid] = problem_to_assign
                            worker_queues[wid].put(problem_to_assign)

                        # 4. Check Termination
                        if adaptive_scheduler is not None:
                            scheduler_exhausted = adaptive_scheduler.is_done()
                        else:
                            scheduler_exhausted = not pending_problems
                        if scheduler_exhausted and not assigned_tasks:
                            logger.info(
                                "All tasks completed or assigned. Sending shutdown signals. "
                                f"pending={len(pending_problems)} assigned={dict(assigned_tasks)} "
                                f"alive_workers={[i for i in range(args.parallel) if processes[i].is_alive()]}"
                            )
                            for q in worker_queues:
                                q.put(None)
                            shutdown_sent = True

                    # --- END SCHEDULING LOGIC ---

                    # Check for dead workers and update status
                    active_workers = set()
                    for idx in range(len(processes)):
                        p = processes[idx]
                        wid = idx

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
                            try:
                                sd_items = list(status_dict.items())
                            except _MANAGER_PIPE_ERRORS:
                                sd_items = []
                            for pid, info in sd_items:
                                if str(pid).startswith(WORKER_META_KEY_PREFIX):
                                    continue
                                if info.get("worker_id") == wid:
                                    status = info.get("status", "")
                                    if not (
                                        status.startswith("Completed")
                                        or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]
                                    ):
                                        _safe_status_update(status_dict, pid, {
                                            "status": "Error (Worker Died)",
                                            "start_time": info.get("start_time", time.time()),
                                            "elapsed": time.time() - info.get("start_time", time.time()),
                                            "worker_id": wid,
                                        })

                            # Restart Logic: If there is still work to do, restart the worker
                            has_more_work = (
                                (adaptive_scheduler is not None and not adaptive_scheduler.is_done())
                                or bool(pending_problems)
                            )
                            if has_more_work and not shutdown_sent:
                                logger.info(
                                    f"Restarting Worker {wid} to handle pending work "
                                    f"(adaptive={adaptive_scheduler is not None}, "
                                    f"pending={len(pending_problems)})."
                                )
                                progress.console.print(f"[bold yellow]🔄 Restarting Worker {wid}[/bold yellow]")

                                if wid in failed_workers_logged:
                                    failed_workers_logged.remove(wid)

                                meta_key = _worker_meta_key(wid)
                                _safe_status_update(status_dict, meta_key, {
                                    "status": "Restarting...",
                                    "start_time": time.time(),
                                    "elapsed": 0.0,
                                    "worker_id": wid,
                                })

                                if p in worker_map:
                                    del worker_map[p]

                                new_p = multiprocessing.Process(
                                    target=worker_main,
                                    args=(args, wid, worker_queues[wid], experiment_log_dir, status_dict),
                                )
                                new_p.start()

                                processes[idx] = new_p
                                worker_map[new_p] = wid
                                active_workers.add(wid)

                    # --- PROGRESS DISPLAY ---
                    completed_count = 0
                    error_count = 0
                    skipped_count = 0
                    current_worker_status = {i: None for i in range(args.parallel)}

                    try:
                        sd_items = list(status_dict.items())
                    except _MANAGER_PIPE_ERRORS:
                        sd_items = []
                        if not manager_dead:
                            logger.warning("Manager pipe broken; dispatching remaining work and waiting for workers")
                            manager_dead = True
                            # Dispatch any remaining pending problems round-robin to workers
                            # so they can finish without the supervisor's scheduling loop.
                            if not shutdown_sent:
                                # In adaptive mode we lose the feedback signal once
                                # the Manager dies, so we drain the scheduler greedily
                                # without recording outcomes — best-effort completion.
                                drain: list[Any] = []
                                if adaptive_scheduler is not None:
                                    while True:
                                        nxt = adaptive_scheduler.next_problem()
                                        if nxt is None:
                                            break
                                        drain.append(nxt)
                                else:
                                    drain = list(pending_problems)
                                if drain:
                                    alive_workers = [i for i in range(args.parallel) if processes[i].is_alive()]
                                    if alive_workers:
                                        for pi, prob in enumerate(drain):
                                            wid = alive_workers[pi % len(alive_workers)]
                                            worker_queues[wid].put(prob)
                                        logger.info(
                                            f"Dispatched {len(drain)} remaining problems to "
                                            f"{len(alive_workers)} workers"
                                        )
                                pending_problems.clear()
                            # Send shutdown sentinels AFTER all remaining work so workers
                            # drain their queues before stopping.
                            if not shutdown_sent:
                                for q in worker_queues:
                                    q.put(None)
                                shutdown_sent = True

                    for pid, info in sd_items:
                        if str(pid).startswith(WORKER_META_KEY_PREFIX):
                            continue
                        status = info.get("status", "Unknown")
                        wid = info.get("worker_id")

                        if status.startswith("Completed"):
                            completed_count += 1
                        elif status in ["Error", "Error (Worker Died)"]:
                            error_count += 1
                        elif status == "Skipped (Khaos Req)":
                            skipped_count += 1

                        if wid is not None:
                            is_active = not (
                                status.startswith("Completed")
                                or status in ["Error", "Skipped (Khaos Req)", "Error (Worker Died)"]
                            )
                            if is_active:
                                start_t = info.get("start_time", time.time())
                                display_pid = info.get("pid", str(pid))
                                current_worker_status[wid] = (status, display_pid, start_t)

                    finished_count = completed_count + error_count + skipped_count
                    status_text = f"[bold green]Overall Progress[/bold green] (Completed: [green]{completed_count}[/green], Errors: [red]{error_count}[/red]"
                    if skipped_count > 0:
                        status_text += f", Skipped: [yellow]{skipped_count}[/yellow]"
                    status_text += ")"
                    progress.update(main_task, completed=finished_count, description=status_text)

                    for i in range(args.parallel):
                        if i not in active_workers:
                            progress.update(
                                worker_tasks[i], description=f"Worker {i}: [dim]Finished[/dim]", completed=100
                            )
                        elif current_worker_status[i]:
                            status, pid, start_t = current_worker_status[i]
                            elapsed = int(time.time() - start_t)

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
                            meta = _safe_status_read(status_dict, _worker_meta_key(i))
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

                    # Periodic heartbeat log (every 30s) for post-mortem debugging
                    now_ts = time.time()
                    if now_ts - last_heartbeat_ts >= 30:
                        alive = [i for i in range(args.parallel) if processes[i].is_alive()]
                        logger.debug(
                            f"[heartbeat] alive_workers={alive} pending={len(pending_problems)} "
                            f"assigned={dict(assigned_tasks)} shutdown_sent={shutdown_sent} "
                            f"manager_dead={manager_dead}"
                        )
                        last_heartbeat_ts = now_ts

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
        logger.info("\n🛑 Interrupted by user (Ctrl-C). Sending shutdown to workers...")
        if not shutdown_sent:
            for q in worker_queues:
                q.put(None)
            shutdown_sent = True
    except Exception:
        logger.error("Supervisor monitoring loop crashed:", exc_info=True)
        # Dispatch remaining work to workers before dying so they can finish
        if not shutdown_sent:
            try:
                alive_workers = [i for i in range(args.parallel) if processes[i].is_alive()]
                if alive_workers and pending_problems:
                    for pi, prob in enumerate(pending_problems):
                        wid = alive_workers[pi % len(alive_workers)]
                        worker_queues[wid].put(prob)
                    logger.info(f"Dispatched {len(pending_problems)} remaining problems to {len(alive_workers)} workers")
                    pending_problems.clear()
                for q in worker_queues:
                    q.put(None)
                shutdown_sent = True
            except Exception:
                logger.error("Failed to dispatch remaining work:", exc_info=True)

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
            logger.warning(f"Worker {worker_map.get(p)} did not exit, force-killing process tree...")
            # Workers ignore SIGTERM, so go straight to SIGKILL
            _kill_process_tree(p.pid, signal.SIGKILL)
            p.join(timeout=2)
        else:
            p.join()


def main(
    args,
    problem_list=None,
    experiment_log_dir=None,
    status_dict=None,
    problem_queue=None,
    worker_id=None,
):
    # Generate session ID and log directory
    session_timestamp = get_current_datetime_formatted()
    # Ensure logs root exists
    os.makedirs("logs", exist_ok=True)

    if experiment_log_dir is None:
        if getattr(args, "experiment_dir", None):
            experiment_log_dir = os.path.abspath(args.experiment_dir)
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
    os.environ["JUDGE_NUM_ROUNDS"] = str(args.judge_rounds)
    os.environ["JUDGE_VOTING_TEMPERATURE"] = str(args.judge_voting_temperature)

    # Enforce explicit kubeconfig selection for every process and worker.
    base_kubeconfig = require_kubeconfig_path()
    os.environ["KUBECONFIG"] = base_kubeconfig
    os.environ["SREGYM_BASE_KUBECONFIG"] = base_kubeconfig

    # Resolve whether this agent opts into deferred cleanup (see AgentRegistration).
    # When enabled, the conductor holds teardown until /cleanup is POSTed, and the
    # agent subprocess sees SREGYM_DEFER_CLEANUP=1 so it knows to make that call.
    defer_cleanup = False
    if args.agent and not args.use_external_harness:
        _default_registry = Path(os.path.dirname(os.path.abspath(__file__))) / "agents.yaml"
        _registry_path = Path(os.environ.get("SREGYM_AGENT_REGISTRY", _default_registry))
        _reg = get_agent(args.agent, path=_registry_path)
        if _reg and _reg.defer_cleanup:
            defer_cleanup = True
            os.environ["SREGYM_DEFER_CLEANUP"] = "1"

    conductor = Conductor(
        tasklist_path=getattr(args, "tasklist", None),
        defer_cleanup=defer_cleanup,
    )

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


def build_live_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage live SREGym application deployments")
    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy_parser = subparsers.add_parser(
        "deploy",
        help="Deploy an app or problem environment onto the reusable live kind cluster",
    )
    deploy_parser.add_argument(
        "--app",
        type=str,
        default=None,
        help="App to deploy without faults (e.g. hotel_reservation, astronomy_shop)",
    )
    deploy_parser.add_argument(
        "--problem",
        type=str,
        default=None,
        help="Problem ID to deploy and inject into the live cluster",
    )
    deploy_parser.add_argument(
        "--deployment-name",
        type=str,
        default=None,
        help="Stable name for the deployment state directory and cluster",
    )
    deploy_parser.add_argument(
        "--local-port",
        type=int,
        default=None,
        help="Optional localhost port to use for the frontend port-forward",
    )
    deploy_parser.add_argument(
        "--with-k8s-proxy",
        action="store_true",
        help="Start the filtered localhost Kubernetes API proxy and emit a proxy kubeconfig for agents",
    )
    deploy_parser.add_argument(
        "--recreate-cluster",
        action="store_true",
        help="Discard any existing reusable live cluster and recreate it before deployment",
    )

    undeploy_parser = subparsers.add_parser(
        "undeploy",
        help="Remove a live deployment from the reusable cluster",
    )
    undeploy_parser.add_argument(
        "--deployment-name",
        type=str,
        required=True,
        help="Deployment name returned by the deploy command",
    )
    undeploy_parser.add_argument(
        "--delete-cluster",
        action="store_true",
        help="Delete the reusable live kind cluster instead of keeping it warm for the next deploy",
    )

    proxy_parser = subparsers.add_parser("serve-k8s-proxy", help=argparse.SUPPRESS)
    proxy_parser.add_argument("--kubeconfig-path", type=str, required=True, help=argparse.SUPPRESS)
    proxy_parser.add_argument("--listen-port", type=int, required=True, help=argparse.SUPPRESS)
    return parser


def build_benchmark_parser() -> argparse.ArgumentParser:
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
        "--judge-rounds",
        type=int,
        default=3,
        help="Number of LLM judge rounds for majority voting (default: 3).",
    )
    parser.add_argument(
        "--judge-voting-temperature",
        type=float,
        default=0.7,
        help="Temperature for judge voting rounds (default: 0.7). "
        "Higher values increase diversity between rounds.",
    )
    parser.add_argument(
        "--experiment-dir",
        type=str,
        default=None,
        help="Explicit experiment directory. If it exists and has results, "
             "auto-resume (skip completed problems). If it doesn't exist, create it.",
    )
    parser.add_argument(
        "--tasklist",
        type=str,
        default=None,
        help="Path to a tasklist YAML file (overrides the default sregym/conductor/tasklist.yml)",
    )
    parser.add_argument(
        "--problem-spec",
        action="append",
        default=None,
        metavar="SPEC_NAME",
        help="Filter non-variant problems to those whose ID equals SPEC_NAME or starts with "
             "SPEC_NAME_ (repeat the flag to include multiple specs).",
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
    parser.add_argument(
        "--variants",
        action="store_true",
        help="Run auto-generated problem variants (ignores tasklist.yml)",
    )
    parser.add_argument(
        "--variant-count",
        type=int,
        default=0,
        help="Number of variant problems to run (required with --variants)",
    )
    parser.add_argument(
        "--variant-offset",
        type=int,
        default=0,
        help="Starting offset in the variant stream (default: 0)",
    )
    parser.add_argument(
        "--variant-seed",
        type=int,
        default=42,
        help="Seed for deterministic variant stream ordering (default: 42)",
    )
    parser.add_argument(
        "--variant-order",
        choices=["flat", "round_robin", "grouped", "adaptive"],
        default="round_robin",
        help="Variant stream ordering: flat=epoch shuffle of all variants, "
             "round_robin=one per class per round (default), "
             "grouped=drain one class before moving to the next, "
             "adaptive=stay in a class until N consecutive solves or "
             "X attempts (requires --variant-adaptive-consec-solves and "
             "--variant-max-per-class).",
    )
    parser.add_argument(
        "--variant-max-per-class",
        type=int,
        default=None,
        help="When --variant-order=grouped or adaptive, take at most N variants "
             "per class before moving on. Default for grouped: take all. "
             "Required for adaptive.",
    )
    parser.add_argument(
        "--variant-adaptive-consec-solves",
        type=int,
        default=None,
        help="Adaptive mode only: advance to the next class once the last N "
             "completions in the current class are all solved. Required when "
             "--variant-order=adaptive.",
    )
    parser.add_argument(
        "--variant-spec",
        action="append",
        default=None,
        metavar="BASE_NAME",
        help="Restrict variant generation to specific spec base_names "
             "(e.g. readiness_probe_misconfiguration). Repeat the flag to "
             "select multiple specs. Default: all specs.",
    )
    return parser


def _validate_live_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.command == "deploy":
        if not args.app and not args.problem:
            parser.error("deploy requires --app or --problem")
        if args.local_port is not None and args.local_port <= 0:
            parser.error("--local-port must be > 0")
        return

    if args.command == "serve-k8s-proxy" and args.listen_port <= 0:
        parser.error("--listen-port must be > 0")


def _validate_benchmark_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not args.use_external_harness and args.agent is None:
        parser.error("--agent is required when --use-external-harness is not set")

    if args.sequence_len > 0 and args.problem:
        parser.error("--sequence-len and --problem are mutually exclusive")

    if args.variants:
        if args.variant_order != "adaptive" and args.variant_count <= 0:
            parser.error("--variant-count is required and must be > 0 when --variants is set")
        if args.problem:
            parser.error("--variants and --problem are mutually exclusive")
        if args.sequence_len > 0:
            parser.error("--variants and --sequence-len are mutually exclusive")
        if args.variant_max_per_class is not None:
            if args.variant_order not in ("grouped", "adaptive"):
                parser.error("--variant-max-per-class requires --variant-order=grouped or adaptive")
            if args.variant_max_per_class <= 0:
                parser.error("--variant-max-per-class must be > 0")
        if args.variant_order == "adaptive":
            if args.variant_max_per_class is None:
                parser.error("--variant-max-per-class is required when --variant-order=adaptive")
            if args.variant_adaptive_consec_solves is None:
                parser.error("--variant-adaptive-consec-solves is required when --variant-order=adaptive")
            if args.variant_adaptive_consec_solves <= 0:
                parser.error("--variant-adaptive-consec-solves must be > 0")
            if args.variant_count < 0:
                parser.error("--variant-count must be >= 0 when --variant-order=adaptive (0 = no global cap)")
    if args.variant_adaptive_consec_solves is not None and args.variant_order != "adaptive":
        parser.error("--variant-adaptive-consec-solves requires --variant-order=adaptive")
    if args.variant_spec and not args.variants:
        parser.error("--variant-spec requires --variants")
    if args.variant_spec:
        from sregym.conductor.problems.variant_specs import get_all_variant_specs

        known_specs = {spec.base_name for spec in get_all_variant_specs()}
        unknown = [n for n in args.variant_spec if n not in known_specs]
        if unknown:
            parser.error(
                f"--variant-spec: unknown spec name(s): {unknown}. "
                f"Valid names: {sorted(known_specs)}"
            )

    if args.seed_summary:
        if args.experiment_dir and os.path.exists(args.experiment_dir):
            parser.error("--seed-summary cannot be combined with resuming an existing --experiment-dir")
        seed_path = Path(args.seed_summary)
        if not seed_path.is_file():
            parser.error(f"--seed-summary: path does not exist or is not a file: {args.seed_summary}")


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    effective_argv = list(argv if argv is not None else sys.argv[1:])
    if effective_argv and effective_argv[0] in LIVE_COMMANDS:
        parser = build_live_parser()
        args = parser.parse_args(effective_argv)
        _validate_live_args(parser, args)
        return args

    parser = build_benchmark_parser()
    args = parser.parse_args(effective_argv)
    _validate_benchmark_args(parser, args)
    return args


def run_live_command(args: argparse.Namespace) -> int:
    if args.command == "serve-k8s-proxy":
        return run_live_k8s_proxy(args)

    if args.command == "deploy":
        deployment_name = _sanitize_deployment_name(
            args.deployment_name or _default_deployment_name(args.problem, args.app)
        )
        deployment_dir = _deployment_dir_for_name(deployment_name)
        os.makedirs(deployment_dir, exist_ok=True)
        os.environ["SREGYM_LOG_FILE"] = os.path.join(deployment_dir, "live_deploy.log")
        init_logger()
        state = deploy_live_environment(
            problem_id=args.problem,
            app_name=args.app,
            deployment_name=deployment_name,
            frontend_local_port=args.local_port,
            with_k8s_proxy=args.with_k8s_proxy,
            recreate_cluster=args.recreate_cluster,
        )
        print(f"Deployment '{state.deployment_name}' is ready.")
        print(f"Cluster: {state.cluster_name}")
        print(f"Cluster Reused: {'yes' if state.cluster_reused else 'no'}")
        print(f"Kubeconfig: {state.kubeconfig_path}")
        if state.problem_id:
            print(f"Injected problem: {state.problem_id}")
        else:
            print(f"Application: {state.app_name}")
        if state.frontend_url:
            print(f"Frontend URL: {state.frontend_url}")
        else:
            print("Frontend URL: unavailable (frontend port-forward failed to start)")
        if state.k8s_proxy_url and state.k8s_proxy_kubeconfig_path:
            print(f"K8s Proxy URL: {state.k8s_proxy_url}")
            print(f"K8s Proxy Kubeconfig: {state.k8s_proxy_kubeconfig_path}")
        print(f"Undeploy with: python main.py undeploy --deployment-name {state.deployment_name}")
        return 0

    if args.command == "undeploy":
        deployment_dir = _deployment_dir_for_name(args.deployment_name)
        os.makedirs(deployment_dir, exist_ok=True)
        os.environ["SREGYM_LOG_FILE"] = os.path.join(deployment_dir, "live_undeploy.log")
        init_logger()
        result = undeploy_live_environment(args.deployment_name, delete_cluster=args.delete_cluster)
        print(f"Deployment '{result.state.deployment_name}' removed.")
        if result.cluster_deleted:
            print(f"Deleted cluster: {result.state.cluster_name}")
        else:
            print(f"Cluster retained for reuse: {result.state.cluster_name}")
        return 0

    raise ValueError(f"Unsupported live command: {args.command}")


def run_live_k8s_proxy(args: argparse.Namespace) -> int:
    from sregym.service.k8s_proxy import KubernetesAPIProxy

    stop_event = threading.Event()

    def _handle_signal(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    proxy = KubernetesAPIProxy(listen_port=args.listen_port, kubeconfig_path=args.kubeconfig_path)
    proxy.start()
    try:
        stop_event.wait()
    finally:
        proxy.stop()
    return 0


if __name__ == "__main__":
    cli_args = parse_cli_args()
    if getattr(cli_args, "command", None) in LIVE_COMMANDS:
        sys.exit(run_live_command(cli_args))

    # Always run through the parallel wrapper to ensure consistent logging and behavior
    # even for single-worker runs (capture stdout/stderr, etc.)
    run_parallel(cli_args)
