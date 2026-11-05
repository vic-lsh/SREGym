"""Helper (client) pods never run in the app namespace.

SDO's ``drift.unexplained`` detector flags any live object in the app namespace that the accepted
source lacks. A helper pod there, alive for seconds, opened spurious incidents in the DA2 detection
run (and a responder would be tempted to delete it). The helpers reach the app by cluster-DNS name,
so they run in ``CLIENT_NAMESPACE`` instead, and the app namespace stays exactly as the fault left it.
"""

from __future__ import annotations

import json

from sregym.conductor.problems.composition import app_bindings
from sregym.generators.fault.components import http, memcached
from sregym.generators.fault.components.kube import CLIENT_NAMESPACE, run_client_pod
from sregym.conductor.problems.composition.grading import Request

APP_NS = "social-network"


class RecordingKube:
    def __init__(self, log: str):
        self.log = log
        self.calls: list[list[str]] = []
        self.objects: list[dict] = []

    def run(self, argv, stdin=None, *, timeout_s=None):
        self.calls.append(argv)
        if argv[:3] == ["apply", "-f", "-"]:
            self.objects.append(json.loads(stdin))
        if argv[:2] == ["get", "pod"]:
            return b"Succeeded"
        if argv[0] == "logs":
            return self.log.encode()
        return b""


def _assert_app_namespace_untouched(kube: RecordingKube) -> None:
    for obj in kube.objects:
        assert obj["kind"] == "Namespace" or obj["metadata"]["namespace"] == CLIENT_NAMESPACE
    for argv in kube.calls:
        assert not any(a == APP_NS for a in argv), argv  # no `-n social-network` on any pod operation
    pods = [o for o in kube.objects if o["kind"] == "Pod"]
    assert pods and CLIENT_NAMESPACE != APP_NS
    assert pods[0]["metadata"]["labels"]["sregym.io/app-namespace"] == APP_NS


def test_client_pod_runs_outside_app_namespace_and_is_deleted():
    kube = RecordingKube("hello")
    assert run_client_pod(kube, APP_NS, "echo hello") == "hello"
    _assert_app_namespace_untouched(kube)
    assert kube.calls[-1][:2] == ["delete", "pod"]
    assert kube.calls[-1][kube.calls[-1].index("-n") + 1] == CLIENT_NAMESPACE


def test_http_fetch_uses_no_pod_in_app_namespace():
    kube = RecordingKube("@@REQ 0 OK \\n@@END".replace("\\n", "\n"))
    http.fetch(kube, app_bindings.SOCIAL_NETWORK, APP_NS, (Request("/x"),))
    _assert_app_namespace_untouched(kube)
    script = kube.objects[-1]["spec"]["containers"][0]["command"][2]
    assert f"{APP_NS}.svc.cluster.local" in script  # still reaches the app by FQDN


def test_memcached_driver_uses_no_pod_in_app_namespace():
    kube = RecordingKube("@@END")
    inst = type("I", (), {"workload": "user-memcached", "port": 11211})()
    try:
        memcached._run_chunk(kube, APP_NS, inst, [b"stats\r\n"])
    except Exception:
        pass  # the canned reply is incomplete; only where the pod ran matters
    _assert_app_namespace_untouched(kube)
    script = kube.objects[-1]["spec"]["containers"][0]["command"][2]
    assert f"user-memcached.{APP_NS}.svc.cluster.local" in script
