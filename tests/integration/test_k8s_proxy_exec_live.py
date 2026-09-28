"""``kubectl exec`` through the agent API proxy against a real cluster.

Opt-in: set ``SREGYM_PROXY_E2E_KUBECONFIG`` to an admin kubeconfig for a
disposable cluster (for example a kind cluster). The test creates and deletes
its own namespace. It starts the proxy the way ``Conductor`` does and checks
both exec modes: disabled answers a clean Forbidden, enabled runs the command
over websocket and SPDY but only inside the allowed application namespace.
"""

from __future__ import annotations

import os
import socket
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from sregym.service.k8s_proxy import KubernetesAPIProxy

KUBECONFIG = os.environ.get("SREGYM_PROXY_E2E_KUBECONFIG", "")
NAMESPACE = "sregym-proxy-e2e"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not KUBECONFIG, reason="set SREGYM_PROXY_E2E_KUBECONFIG to a disposable cluster"),
]


def _kubectl(kubeconfig: str, *args: str, env: dict[str, str] | None = None, stdin: str | None = None):
    return subprocess.run(
        ["kubectl", "--kubeconfig", kubeconfig, *args],
        capture_output=True,
        text=True,
        input=stdin,
        timeout=120,
        env={**os.environ, **(env or {})},
        check=False,
    )


@pytest.fixture(scope="module")
def app_pods() -> Iterator[None]:
    _kubectl(KUBECONFIG, "create", "namespace", NAMESPACE)
    _kubectl(KUBECONFIG, "-n", NAMESPACE, "run", "web", "--image=busybox:1.36", "--", "sleep", "1d")
    _kubectl(
        KUBECONFIG,
        "-n",
        NAMESPACE,
        "run",
        "loadgen",
        "--labels=app=load-generator",
        "--image=busybox:1.36",
        "--",
        "sleep",
        "1d",
    )
    ready = _kubectl(KUBECONFIG, "-n", NAMESPACE, "wait", "--for=condition=Ready", "pod", "--all", "--timeout=180s")
    assert ready.returncode == 0, ready.stderr
    yield
    _kubectl(KUBECONFIG, "delete", "namespace", NAMESPACE, "--wait=false")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def agent_kubeconfig(monkeypatch, tmp_path: Path, request) -> Iterator[str]:
    monkeypatch.setenv("SREGYM_BASE_KUBECONFIG", KUBECONFIG)
    proxy = KubernetesAPIProxy(
        hidden_namespaces={"chaos-mesh", "khaos"}, listen_port=_free_port(), allow_exec=request.param
    )
    proxy.exec_namespaces = {NAMESPACE}  # what Conductor._scope_agent_exec sets for the problem
    proxy.start()
    try:
        yield proxy.generate_agent_kubeconfig(str(tmp_path / "agent.kubeconfig"))
    finally:
        proxy.stop()


@pytest.mark.parametrize("agent_kubeconfig", [False], indirect=True)
def test_exec_disabled_is_a_clean_forbidden(app_pods, agent_kubeconfig) -> None:
    result = _kubectl(agent_kubeconfig, "-n", NAMESPACE, "exec", "web", "--", "echo", "ok")
    assert result.returncode != 0
    assert "Forbidden" in result.stderr and "disabled" in result.stderr
    assert "1006" not in result.stderr


@pytest.mark.parametrize("agent_kubeconfig", [True], indirect=True)
@pytest.mark.parametrize("websockets", ["true", "false"], ids=["websocket", "spdy"])
def test_exec_enabled_runs_in_the_app_namespace(app_pods, agent_kubeconfig, websockets) -> None:
    env = {"KUBECTL_REMOTE_COMMAND_WEBSOCKETS": websockets}
    echo = _kubectl(agent_kubeconfig, "-n", NAMESPACE, "exec", "web", "--", "echo", "ok", env=env)
    assert (echo.returncode, echo.stdout) == (0, "ok\n"), echo.stderr
    piped = _kubectl(agent_kubeconfig, "-n", NAMESPACE, "exec", "-i", "web", "--", "cat", env=env, stdin="stdin-ok")
    assert (piped.returncode, piped.stdout) == (0, "stdin-ok"), piped.stderr


@pytest.mark.parametrize("agent_kubeconfig", [True], indirect=True)
def test_exec_enabled_keeps_the_agent_scope(app_pods, agent_kubeconfig) -> None:
    hidden = _kubectl(agent_kubeconfig, "-n", NAMESPACE, "exec", "loadgen", "--", "echo", "ok")
    assert hidden.returncode != 0 and "Forbidden" in hidden.stderr
    outside = _kubectl(agent_kubeconfig, "-n", "kube-system", "exec", "deploy/coredns", "--", "true")
    assert outside.returncode != 0 and "only in the application namespace" in outside.stderr
