"""Hermetic source setup: previous-stage history and memory survive seed copying."""

import json
from pathlib import Path

from test_developer_source import git, seed

from sregym.service.app_workspace import prepare_application_workspace
from sregym.service.developer_source import apply_stage_change


def test_two_stage_seed_chaining_preserves_developer_history(tmp_path: Path):
    original = seed(tmp_path)
    patch = tmp_path / "change.patch"
    (original / "app.txt").write_text("after\n")
    patch.write_text(git(original, "diff") + "\n")
    git(original, "checkout", "--", "app.txt")
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"first": {"patch": str(patch), "message": "feat: synthetic developer change"}}))
    first = prepare_application_workspace(
        experiment_dir=tmp_path / "stage1", app_filter="hotel_reservation", resume=False, seed_from=original
    )
    receipt = apply_stage_change(first, ["first"], plan)
    second = prepare_application_workspace(
        experiment_dir=tmp_path / "stage2", app_filter="hotel_reservation", resume=False, seed_from=first
    )
    assert git(second, "rev-parse", "HEAD") == receipt["commit"]
    assert git(second, "rev-parse", "HEAD^") == git(original, "rev-parse", "HEAD")
    assert (second / ".sdo/goal.md").read_bytes() == (original / ".sdo/goal.md").read_bytes()
    assert (second / "app.txt").read_text() == "after\n"
