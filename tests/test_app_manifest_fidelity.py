"""Benchmark deployment must preserve application-authored workload documents."""

import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

from sregym.service.helm import Helm
from sregym.service.kubectl import KubeCtl
from sregym.service import source_deploy
from types import SimpleNamespace


@pytest.fixture
def app_documents():
    # Deliberately unbounded Jaeger and an unrelated workload: admission detects
    # growth; deployment must never repair either behind accepted source's back.
    return b"""# Application-owned settings, including formatting.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger
spec:
  selector:
    matchLabels: {app: jaeger}
  template:
    metadata:
      labels: {app: jaeger}
    spec:
      containers:
        - name: jaeger
          image: jaegertracing/all-in-one:1.57
          args: ["--memory.max-traces=0"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
spec:
  selector:
    matchLabels: {app: app}
  template:
    metadata:
      labels: {app: app}
    spec:
      containers:
        - name: app
          image: example/app:accepted
          resources:
            limits: {memory: 12Gi}
"""


def test_kubectl_applies_byte_identical_source_documents(tmp_path, monkeypatch, app_documents):
    source = tmp_path / "kubernetes"
    source.mkdir()
    (source / "app.yaml").write_bytes(app_documents)
    applied = []

    def capture_apply(command):
        args = shlex.split(command)
        assert args[:3] == ["kubectl", "apply", "-Rf"]
        applied.extend(path.read_bytes() for path in sorted(Path(args[3]).rglob("*.yaml")))

    kubectl = object.__new__(KubeCtl)  # No cluster client is needed for this boundary.
    monkeypatch.setattr(kubectl, "exec_command", capture_apply)
    kubectl.apply_configs("hotel-reservation", str(source))
    assert applied == [app_documents]
    assert (source / "app.yaml").read_bytes() == app_documents


def test_source_deploy_preserves_app_documents_in_render_input(tmp_path, monkeypatch, app_documents):
    source = tmp_path / "hotel"
    manifests = source / "kubernetes"
    manifests.mkdir(parents=True)
    (manifests / "app.yaml").write_bytes(app_documents)
    accepted = tmp_path / "accepted.yaml"
    accepted.write_bytes(app_documents)
    monkeypatch.setenv("SREGYM_ACCEPTED_RENDER", str(accepted))
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "fixture")
    monkeypatch.setattr(
        source_deploy._SUPPORTED_ADAPTERS["Hotel Reservation"], "ensure_images_loaded", lambda **kw: None
    )
    with source_deploy.plan_for_app(SimpleNamespace(name="Hotel Reservation")) as plan:
        assert plan.manifest_path.read_bytes() == app_documents
    assert (manifests / "app.yaml").read_bytes() == app_documents


@pytest.mark.parametrize("operation", ["install", "upgrade"])
def test_helm_rendering_preserves_app_documents(tmp_path, monkeypatch, app_documents, operation):
    chart = tmp_path / "chart"
    (chart / "templates").mkdir(parents=True)
    (chart / "Chart.yaml").write_text("apiVersion: v2\nname: fixture\nversion: 0.1.0\n")
    (chart / "values.yaml").write_text("{}\n")
    template = chart / "templates/app.yaml"
    template.write_bytes(app_documents)
    baseline = subprocess.run(
        ["helm", "template", "fixture", str(chart), "-n", "hotel-reservation"], capture_output=True, check=True
    ).stdout
    popen = subprocess.Popen
    rendered = []

    def render_offline(command, **kwargs):
        # Exercise the real Helm renderer with the deployment method's actual
        # arguments (including any injected post-renderer), without a cluster.
        args = shlex.split(command) if isinstance(command, str) else list(command)
        assert args[:2] == ["helm", operation]
        args[1] = "template"
        args = [arg for arg in args if arg not in {"--create-namespace", "--server-side=true", "--force-conflicts"}]
        kwargs.pop("shell", None)
        process = popen(args, **kwargs)
        communicate = process.communicate

        def capture(*args, **kwargs):
            output, error = communicate(*args, **kwargs)
            rendered.append(output)
            return output, error

        process.communicate = capture
        return process

    monkeypatch.setattr(subprocess, "Popen", render_offline)
    getattr(Helm, operation)(
        release_name="fixture",
        chart_path=str(chart),
        namespace="hotel-reservation",
        remote_chart=True,
        values_file=str(chart / "values.yaml"),
    )
    assert rendered == [baseline]
    assert template.read_bytes() == app_documents
