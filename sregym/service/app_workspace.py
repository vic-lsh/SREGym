"""Application workspaces for agents."""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
from pathlib import Path

from sregym.paths import TARGET_MICROSERVICES

APP_SOURCE_SUBDIRS = {
    "hotel_reservation": "hotelReservation",
    "social_network": "socialNetwork",
    "train_ticket": "train-ticket",
}

_INITIAL_COMMIT_MESSAGE = "Initial application workspace snapshot"
_WORKSPACE_ENV_VAR = "SREGYM_APP_SOURCE_DIR"
_AGENT_WORKDIR_ENV_VAR = "SREGYM_AGENT_WORKDIR"
_WORKSPACE_SEED_ENV_VAR = "SREGYM_APP_WORKSPACE_SEED_DIR"


def _target_microservices_root() -> Path:
    return TARGET_MICROSERVICES


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

    return _prepare_workspace_copy(
        workspace_dir=workspace_dir,
        app_filter=app_filter,
        seed_from=seed_from,
    )


def prepare_ephemeral_application_workspace(
    *,
    exp_env_dir: str | Path,
    app_filter: str,
    seed_from: str | Path | None = None,
) -> Path:
    source_subdir = resolve_app_source_subdir(app_filter)
    workspace_dir = Path(exp_env_dir) / source_subdir
    return _prepare_workspace_copy(
        workspace_dir=workspace_dir,
        app_filter=app_filter,
        seed_from=seed_from,
    )


@contextlib.contextmanager
def ephemeral_application_workspace(
    *,
    exp_env_dir: str | Path,
    app_filter: str,
    seed_from: str | Path | None = None,
):
    workspace_dir = prepare_ephemeral_application_workspace(
        exp_env_dir=exp_env_dir,
        app_filter=app_filter,
        seed_from=seed_from,
    )
    try:
        yield workspace_dir
    finally:
        shutil.rmtree(workspace_dir, ignore_errors=True)


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
    if workspace_dir is not None and rel_path.parts and rel_path.parts[0] == workspace_dir.name:
        return workspace_dir / Path(*rel_path.parts[1:])
    return _target_microservices_root() / rel_path


def _prepare_workspace_copy(
    *,
    workspace_dir: Path,
    app_filter: str,
    seed_from: str | Path | None = None,
) -> Path:
    if workspace_dir.exists():
        shutil.rmtree(workspace_dir)

    if seed_from is not None:
        seed_dir = Path(seed_from)
        if not seed_dir.is_dir():
            raise FileNotFoundError(f"Seeded application workspace does not exist: {seed_dir}")
        shutil.copytree(seed_dir, workspace_dir)
        return workspace_dir

    source_dir = _target_microservices_root() / resolve_app_source_subdir(app_filter)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Application source directory does not exist: {source_dir}")

    shutil.copytree(
        source_dir,
        workspace_dir,
        ignore=shutil.ignore_patterns(".git", ".gitmodules"),
    )
    _initialize_git_repository(workspace_dir)
    return workspace_dir


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
