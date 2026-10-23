"""Synthetic source histories verify source-only, exactly-once developer commits."""

import json
import subprocess
from pathlib import Path

import pytest

from sregym.service.developer_source import apply_stage_change


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def seed(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Synthetic Developer")
    git(root, "config", "user.email", "test@example.invalid")
    (root / ".sdo").mkdir()
    (root / ".sdo/goal.md").write_text("synthetic goal\n")
    (root / "app.txt").write_text("before\n")
    git(root, "add", ".")
    git(root, "commit", "-m", "seed")
    return root


def test_change_is_ordinary_commit_preserves_memory_and_is_idempotent(tmp_path):
    root = seed(tmp_path)
    parent = git(root, "rev-parse", "HEAD")
    (root / "app.txt").write_text("after\n")
    patch = tmp_path / "plant.patch"
    patch.write_text(git(root, "diff") + "\n")
    git(root, "checkout", "--", "app.txt")
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"synthetic_stage": {"patch": str(patch), "message": "feat: developer update"}}))
    receipt = apply_stage_change(root, ["synthetic_stage"], plan)
    assert receipt["parent"] == parent
    assert git(root, "rev-parse", "HEAD^") == parent
    assert git(root, "diff", "HEAD^", "HEAD", "--", ".sdo") == ""
    assert (root / "app.txt").read_text() == "after\n"
    assert apply_stage_change(root, ["synthetic_stage"], plan)["commit"] == receipt["commit"]


def test_rejects_memory_patch(tmp_path):
    root = seed(tmp_path)
    (root / ".sdo/goal.md").write_text("tampered\n")
    patch = tmp_path / "memory.patch"
    patch.write_text(git(root, "diff") + "\n")
    git(root, "checkout", "--", ".sdo/goal.md")
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"stage": {"patch": str(patch), "message": "bad update"}}))
    with pytest.raises(ValueError, match="memory"):
        apply_stage_change(root, ["stage"], plan)
    assert git(root, "status", "--porcelain") == ""


def test_rejects_multi_problem_stage_and_dirty_source(tmp_path):
    root = seed(tmp_path)
    plan = tmp_path / "plan.json"
    plan.write_text("{}")
    with pytest.raises(ValueError, match="one problem"):
        apply_stage_change(root, ["a", "b"], plan)
    (root / "app.txt").write_text("dirty")
    with pytest.raises(ValueError, match="clean"):
        apply_stage_change(root, ["a"], plan)
