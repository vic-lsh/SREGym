"""Apply trusted source-only developer patches before a cold seeded deployment.

This is benchmark setup, outside the operational broker. No running controller
is updated. The inherited .sdo tree is preserved byte-for-byte.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def apply_stage_change(root: Path, problem_ids: list[str], plan_path: Path) -> dict[str, Any]:
    if len(problem_ids) != 1:
        raise ValueError("developer source setup requires exactly one problem per stage")
    if _git(root, "status", "--porcelain"):
        raise ValueError("developer source setup requires a clean application workspace")
    spec = json.loads(plan_path.read_text())[problem_ids[0]]
    patch = Path(spec["patch"])
    if not patch.is_absolute():
        patch = plan_path.parent / patch
    digest = hashlib.sha256(patch.read_bytes()).hexdigest()
    marker = f"SREGym-Developer-Patch: {digest}"
    existing = _git(root, "log", "-1", "--format=%H", "--fixed-strings", f"--grep={marker}")
    if existing:
        return {"commit": existing, "parent": _git(root, "rev-parse", f"{existing}^"), "patch_sha256": digest}
    paths = subprocess.check_output(["git", "apply", "--numstat", "-z", str(patch)], cwd=root)
    for entry in paths.split(b"\0"):
        if entry and entry.split(b"\t", 2)[-1].split(b"/")[0] == b".sdo":
            raise ValueError("developer patches must not change operational memory (.sdo)")
    parent = _git(root, "rev-parse", "HEAD")
    memory = _git(root, "rev-parse", "HEAD:.sdo")
    _git(root, "apply", "--check", str(patch))
    _git(root, "apply", "--index", str(patch))
    _git(
        root,
        "-c",
        "user.name=Application Developer",
        "-c",
        "user.email=developer@example.invalid",
        "commit",
        "-m",
        f"{spec['message']}\n\n{marker}",
    )
    commit = _git(root, "rev-parse", "HEAD")
    if _git(root, "rev-parse", "HEAD:.sdo") != memory:
        raise RuntimeError("developer commit changed operational memory")
    return {"parent": parent, "commit": commit, "patch_sha256": digest, "memory_tree": memory}
