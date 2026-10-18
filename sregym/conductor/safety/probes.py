"""Read-only, fail-open cluster reads for safety invariants.

Every helper returns ``None`` (never raises, never returns partial junk) when a
read cannot be completed cleanly, so callers fail OPEN — the core soundness rule
. ``KubeCtl.exec_command`` returns stderr *text* (not JSON) on
failure, so a non-JSON leading character maps cleanly to ``None``.
"""

from __future__ import annotations

import fnmatch
import json
import shlex
from typing import Any


def run_json(kubectl, args: str) -> Any | None:
    """Run ``kubectl <args>`` (expected to include ``-o json``) and parse stdout.

    Returns ``None`` on any error or non-JSON output.
    """
    try:
        out = kubectl.exec_command(f"kubectl {args}")
    except Exception:
        return None
    if not out:
        return None
    stripped = out.lstrip()
    if not stripped or stripped[0] not in "{[":
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


def get_items(
    kubectl,
    resource: str,
    namespace: str | None = None,
    all_namespaces: bool = False,
) -> list[dict] | None:
    """List ``resource`` as JSON and return ``.items``; ``None`` on failure.

    Note: ``None`` (read failed — fail open) is distinct from ``[]`` (read
    succeeded, nothing there).
    """
    if all_namespaces:
        scope = "--all-namespaces"
    elif namespace:
        scope = f"-n {shlex.quote(namespace)}"
    else:
        scope = ""
    data = run_json(kubectl, f"get {resource} {scope} -o json")
    if data is None:
        return None
    items = data.get("items")
    return items if isinstance(items, list) else []


def get_one(kubectl, resource: str, name: str, namespace: str | None = None) -> dict | None:
    """Get a single named ``resource`` as a JSON dict; ``None`` on failure/absence."""
    scope = f"-n {shlex.quote(namespace)}" if namespace else ""
    return run_json(kubectl, f"get {resource} {shlex.quote(name)} {scope} -o json")


def name_matches(name: str, globs: list[str]) -> bool:
    """True if ``name`` matches any shell-style glob in ``globs``."""
    return any(fnmatch.fnmatch(name, g) for g in globs)


def selector_targets_labels(selector: dict | None, label_sets: list[dict]) -> bool:
    """True if a non-empty Service ``selector`` matches (is a subset of) any of
    the given pod ``label_sets``."""
    if not selector:
        return False
    return any(all(labels.get(k) == v for k, v in selector.items()) for labels in label_sets)
