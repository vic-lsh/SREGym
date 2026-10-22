"""Pytest bootstrap for the sregym test suite.

The sregym package is laid out so that top-level helper packages such as
``llm_backend`` and ``clients`` sit alongside the ``sregym`` package itself.
When pytest is invoked it does not automatically add the project root to
``sys.path``, so importing modules whose dependency chain pulls in those
helpers (e.g. ``sregym.conductor.oracles.diagnosis_oracle`` triggers the
conductor ``__init__`` which transitively imports ``llm_backend``) fails
during collection.

Add the bench/sregym directory to ``sys.path`` once, at collection time, so
those imports resolve regardless of how pytest was launched.
"""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))


@pytest.fixture
def offline_problem_dependencies(monkeypatch):
    """Resolve real problem variants without initializing cluster or model clients."""
    from sregym.conductor.problems import registry
    from sregym.generators.fault import inject_app

    monkeypatch.setattr(registry, "KubeCtl", lambda: SimpleNamespace(is_emulated_cluster=lambda: False))
    monkeypatch.setattr(inject_app, "KubeCtl", lambda: SimpleNamespace())
    for name in (
        "target_port",
        "incorrect_image",
        "readiness_probe_misconfiguration",
        "wrong_service_selector",
        "network_policy_block",
    ):
        module = import_module(f"sregym.conductor.problems.{name}")
        monkeypatch.setattr(
            module, "KubeCtl", lambda: SimpleNamespace(list_deployments=lambda _namespace: SimpleNamespace(items=[]))
        )
        monkeypatch.setattr(
            module,
            "create_app",
            lambda name: SimpleNamespace(namespace=name.replace("_", "-"), create_workload=lambda: None),
        )
        monkeypatch.setattr(module, "LLMAsAJudgeOracle", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr(
        import_module("sregym.conductor.problems.network_policy_block").client,
        "NetworkingV1Api",
        lambda: SimpleNamespace(),
    )
