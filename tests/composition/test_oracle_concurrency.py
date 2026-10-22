# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The oracle attribute the composite schedules by (the shared settle wait: tests/oracles/test_rollouts.py)."""

import ast
import re
from pathlib import Path

import pytest

_ORACLES = Path(__file__).resolve().parents[2] / "sregym" / "conductor" / "oracles"

# evaluate() patterns that change the namespace or touch the shared workload stream.
_UNSAFE = re.compile(
    r"create_namespaced_pod|delete_namespaced_pod|kubectl (delete|apply|create|patch|scale)\b"
    r"|start_workload|\bwrk\b"
)


def _oracle_classes(path):
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.ClassDef) and node.name.endswith("Oracle"):
            yield node


def _declares_unsafe(cls: ast.ClassDef) -> bool:
    for stmt in cls.body:
        if isinstance(stmt, ast.Assign) and any(getattr(t, "id", None) == "concurrent_safe" for t in stmt.targets):
            return isinstance(stmt.value, ast.Constant) and stmt.value.value is False
    return False


_FLAGGED = sorted(p for p in _ORACLES.rglob("*.py") if _UNSAFE.search(p.read_text()))


def test_the_scan_finds_the_known_mutating_oracles():
    names = {p.name for p in _FLAGGED}
    assert {"workload.py", "data_plane_progress.py", "cumulative_admission_webhook_timeout_mitigation.py"} <= names


@pytest.mark.parametrize("path", _FLAGGED, ids=lambda p: p.name)
def test_oracles_that_mutate_the_namespace_or_share_a_workload_are_not_concurrent_safe(path):
    """Static: these modules import the Kubernetes client, so they cannot be imported here."""
    classes = list(_oracle_classes(path))

    assert classes, f"{path.name} matches a mutating pattern but defines no *Oracle class"
    for cls in classes:
        assert _declares_unsafe(cls), f"{path.name}:{cls.lineno} {cls.name} must set concurrent_safe = False"


def test_no_oracle_keeps_its_own_rollout_settle_copy():
    # One implementation (rollouts.py) is what lets the composite skip it for every sub-oracle.
    copies = [
        p.name
        for p in _ORACLES.rglob("*.py")
        if p.name != "calico_route_reflector_mitigation.py"  # waits on two namespaces, 180 s; not in the catalog
        and re.search(r"def _wait_for_rollout", p.read_text())
    ]
    assert copies == []
