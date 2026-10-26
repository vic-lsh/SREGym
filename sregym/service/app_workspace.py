"""Persistent per-experiment application workspaces for agents."""

from __future__ import annotations

import os
import runpy
import shutil
import subprocess
from pathlib import Path

from sregym.paths import TARGET_MICROSERVICES

APP_SOURCE_SUBDIRS = {
    "hotel_reservation": "hotelReservation",
    "social_network": "socialNetwork",
    "astronomy_shop": "astronomy-shop",
}

_INITIAL_COMMIT_MESSAGE = "Initial application workspace snapshot"
_WORKSPACE_ENV_VAR = "SREGYM_APP_SOURCE_DIR"
_AGENT_WORKDIR_ENV_VAR = "SREGYM_AGENT_WORKDIR"
_WORKSPACE_SEED_ENV_VAR = "SREGYM_APP_WORKSPACE_SEED_DIR"


def _target_microservices_root() -> Path:
    return TARGET_MICROSERVICES


def _astronomy_source_root() -> Path:
    # astronomy-shop in SREGym-applications is a Helm chart, not demo source.
    return Path(__file__).resolve().parents[2] / "source_apps" / "astronomy_shop"


def prepare_astronomy_source(source_dir: Path) -> None:
    """Keep historical dependency audit findings visible without changing pins.

    Only newly created evaluation workspaces call this before their initial Git
    snapshot. Seeded accepted repositories are copied unchanged.
    """
    for component in ("accounting", "cart"):
        props = source_dir / "src" / component / "Directory.Build.props"
        if not props.is_file():
            continue
        content = props.read_text(encoding="utf-8")
        marker = "<!-- SREGym pinned-version audit policy -->"
        if marker in content:
            continue
        policy = (
            "\n  " + marker + "\n  <PropertyGroup>\n"
            "    <WarningsNotAsErrors>$(WarningsNotAsErrors);NU1901;NU1902;NU1903;NU1904</WarningsNotAsErrors>\n"
            "  </PropertyGroup>\n"
        )
        props.write_text(content.replace("</Project>", policy + "</Project>"), encoding="utf-8")


def hotel_cleanup_controller_enabled() -> bool:
    """Opt-in before the initial source snapshot; never mutate seeded workspaces."""
    return os.getenv("SREGYM_HOTEL_CLEANUP_CONTROLLER", "").lower() in {"1", "true", "yes", "on"}


def application_workspace_dir(experiment_dir: str | Path) -> Path:
    return Path(experiment_dir) / "application_workspace"


def application_source_override() -> Path | None:
    raw = os.getenv(_WORKSPACE_ENV_VAR, "").strip()
    if not raw:
        return None
    return Path(raw)


def agent_workdir_env_var() -> str:
    return _AGENT_WORKDIR_ENV_VAR


def application_workspace_seed_override() -> Path | None:
    raw = os.getenv(_WORKSPACE_SEED_ENV_VAR, "").strip()
    if not raw:
        return None
    return Path(raw)


def should_replay_completed_run(
    *,
    application_workspace_enabled: bool,
    is_resuming: bool,
    pending_problems: list[object],
) -> bool:
    return application_workspace_enabled and is_resuming and not pending_problems


def resolve_app_source_subdir(app_filter: str) -> str:
    normalized = app_filter.strip().lower()
    if normalized in APP_SOURCE_SUBDIRS:
        return APP_SOURCE_SUBDIRS[normalized]
    raise ValueError(f"Application workspace is not supported for app filter {app_filter!r}")


def prepare_application_workspace(
    *,
    experiment_dir: str | Path,
    app_filter: str,
    resume: bool,
    seed_from: str | Path | None = None,
) -> Path:
    workspace_dir = application_workspace_dir(experiment_dir)
    if resume:
        if not workspace_dir.is_dir():
            raise FileNotFoundError(f"Application workspace missing for resumed experiment: {workspace_dir}")
        return workspace_dir

    if workspace_dir.exists():
        shutil.rmtree(workspace_dir)

    if seed_from is not None:
        seed_dir = Path(seed_from)
        if not seed_dir.is_dir():
            raise FileNotFoundError(f"Seeded application workspace does not exist: {seed_dir}")
        shutil.copytree(seed_dir, workspace_dir)
        return workspace_dir

    subdir = resolve_app_source_subdir(app_filter)
    source_dir = _astronomy_source_root() if subdir == "astronomy-shop" else _target_microservices_root() / subdir
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Application source directory does not exist: {source_dir}")

    shutil.copytree(
        source_dir,
        workspace_dir,
        ignore=shutil.ignore_patterns(".git", ".gitmodules"),
    )
    if subdir == "astronomy-shop":
        prepare_astronomy_source(workspace_dir)
    if subdir == "hotelReservation" and hotel_cleanup_controller_enabled():
        component = workspace_dir / "optional/cleanup-controller/enable.py"
        runpy.run_path(str(component))["enable"](workspace_dir)
    _initialize_git_repository(workspace_dir)
    return workspace_dir


def resolve_app_relative_path(app_filter: str, relative_path: str | Path) -> Path:
    workspace_dir = application_source_override()
    source_subdir = resolve_app_source_subdir(app_filter)
    rel_path = Path(relative_path)
    if workspace_dir is not None:
        try:
            if rel_path.parts and rel_path.parts[0] == source_subdir:
                return workspace_dir / Path(*rel_path.parts[1:])
        except IndexError:
            pass
    return _target_microservices_root() / rel_path


def resolve_workspace_path(relative_path: str | Path) -> Path:
    workspace_dir = application_source_override()
    rel_path = Path(relative_path)
    # Astronomy's chart and application source are independently pinned. Its
    # upstream source checkout has no Helm chart; always use the harness chart.
    if rel_path.parts[:3] == ("astronomy-shop", "charts", "opentelemetry-demo"):
        return _target_microservices_root() / rel_path
    if workspace_dir is not None:
        source_roots = set(APP_SOURCE_SUBDIRS.values())
        if rel_path.parts and rel_path.parts[0] in source_roots:
            rel_path = Path(*rel_path.parts[1:])
        return workspace_dir / rel_path
    return _target_microservices_root() / rel_path


def _initialize_git_repository(repo_dir: Path) -> None:
    _run_git(["init", "-b", "main"], cwd=repo_dir)
    _run_git(["add", "-A"], cwd=repo_dir)
    _run_git(
        [
            "-c",
            "user.name=SREGym",
            "-c",
            "user.email=sregym@example.com",
            "commit",
            "-m",
            _INITIAL_COMMIT_MESSAGE,
        ],
        cwd=repo_dir,
    )


def _run_git(args: list[str], *, cwd: Path) -> None:
    subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env=dict(os.environ),
    )
