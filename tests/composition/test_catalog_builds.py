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
"""Every catalog draw for the default apps builds its problem, so no planned slot fails at build time.

The cluster is replaced by an unreachable kubeconfig, an empty deployment list, no-op kubectl commands
and unchecked app paths (the application submodule need not be checked out); spawning any process fails.
Everything else in the constructors runs.
"""

import random
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from kubernetes.config import kube_config

from sregym.conductor.problems.composition import catalog
from sregym.conductor.problems.composition.schedule import DEFAULT_APPS
from sregym.service.apps.base import Application
from sregym.service.kubectl import KubeCtl

SEEDS = range(8)

_KUBECONFIG = """\
apiVersion: v1
kind: Config
clusters: [{name: offline, cluster: {server: "https://127.0.0.1:1"}}]
users: [{name: offline, user: {token: offline}}]
contexts: [{name: offline, context: {cluster: offline, user: offline}}]
current-context: offline
"""


@pytest.fixture
def offline_cluster(monkeypatch, tmp_path):
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(_KUBECONFIG)
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    monkeypatch.setattr(kube_config, "KUBE_CONFIG_DEFAULT_LOCATION", str(kubeconfig))
    monkeypatch.setattr(Application, "_validated_path", lambda self, path, label: Path(path).expanduser().resolve())
    monkeypatch.setattr(KubeCtl, "list_deployments", lambda self, namespace: SimpleNamespace(items=[]))
    monkeypatch.setattr(KubeCtl, "exec_command", lambda self, *args, **kwargs: "")

    def no_processes(*args, **kwargs):
        raise AssertionError(f"building a problem spawned a process: {args or kwargs}")

    for name in ("run", "Popen", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, no_processes)


@pytest.mark.parametrize("app", DEFAULT_APPS)
def test_every_draw_of_every_entry_builds(offline_cluster, app):
    failures = []
    for entry in catalog.entries_for(app):
        for seed in SEEDS:
            draw = entry.sample(app, random.Random(seed))
            try:
                entry.build(draw)
            except Exception as exc:  # collect every failing draw, not just the first
                failures.append(f"{entry.key} {draw.param_dict()}: {type(exc).__name__}: {exc}")
    assert not failures, "\n".join(failures)
