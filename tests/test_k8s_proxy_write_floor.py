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
"""The proxy's write floor: agent writes only in the problem's namespaces (plus admission
webhook configurations), and never a delete of a Namespace, Node, CRD or PersistentVolume.

The rules are checked on `write_refusal` directly, then once end to end through a running
proxy in front of a fake TLS API server, with the namespaces set after start as the
conductor sets them.
"""

import base64
import http.client
import json
import shutil
import socket
import ssl
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import yaml

from sregym.service.k8s_proxy import KubernetesAPIProxy, write_refusal

NS = "hotel-reservation-w1"
WRITES = ("POST", "PUT", "PATCH", "DELETE")
READS = ("GET", "HEAD", "OPTIONS")

IN_NAMESPACE = [
    f"/api/v1/namespaces/{NS}/configmaps",
    f"/api/v1/namespaces/{NS}/services/frontend",
    f"/api/v1/namespaces/{NS}/pods/geo-1/exec?command=ls",
    f"/apis/apps/v1/namespaces/{NS}/deployments/geo/scale",
    f"/apis/networking.k8s.io/v1/namespaces/{NS}/networkpolicies/deny-all",
]
OUTSIDE = [
    "/api/v1/namespaces/kube-system/configmaps/coredns",
    "/api/v1/namespaces/hotel-reservation-w2/services/frontend",
    f"/apis/apps/v1/namespaces/{NS}-x/deployments/geo",
    "/apis/rbac.authorization.k8s.io/v1/clusterroles/view",
    "/apis/scheduling.k8s.io/v1/priorityclasses/high",
    "/api/v1/nodes/worker-1",
    "/api/v1/persistentvolumes/pv-1",
    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/podchaos.chaos-mesh.org",
    "/api/v1/namespaces",
    f"/api/v1/namespaces/{NS}/finalize",
    # Paths the API server would normalize to another object.
    f"/api/v1/namespaces/{NS}/../kube-system/configmaps",
    f"/api/v1/namespaces/{NS}%2F..%2Fkube-system/configmaps",
    f"/api/v1//namespaces/{NS}/configmaps",
    "/version",
]
CLUSTER_OBJECTS = [
    f"/api/v1/namespaces/{NS}",
    "/api/v1/namespaces/kube-system",
    "/api/v1/nodes/worker-1",
    "/api/v1/nodes",
    "/api/v1/persistentvolumes/pv-1",
    "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/podchaos.chaos-mesh.org",
]
WEBHOOK_CONFIGS = [
    "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/hotel-guard",
    "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/resource-limits",
]


@pytest.mark.parametrize("method", WRITES)
@pytest.mark.parametrize("path", IN_NAMESPACE)
def test_a_write_in_the_namespace_passes(method, path):
    assert write_refusal(method, path, {NS}) is None


@pytest.mark.parametrize("method", WRITES)
@pytest.mark.parametrize("path", OUTSIDE)
def test_a_write_outside_the_namespace_is_refused(method, path):
    assert write_refusal(method, path, {NS})


@pytest.mark.parametrize("path", CLUSTER_OBJECTS)
def test_a_cluster_object_delete_is_refused(path):
    assert "deleting" in write_refusal("DELETE", path, {NS})


@pytest.mark.parametrize("namespaces", [{NS}, set()])
@pytest.mark.parametrize("method", WRITES)
@pytest.mark.parametrize("path", WEBHOOK_CONFIGS)
def test_a_webhook_config_write_passes(path, method, namespaces):
    assert write_refusal(method, path, namespaces) is None


def test_the_problem_namespace_object_may_be_relabelled_but_no_other():
    # psa_restricted_blocks_recreation's fix removes a label from the Namespace object.
    for method in ("PUT", "PATCH"):
        assert write_refusal(method, f"/api/v1/namespaces/{NS}", {NS}) is None
        assert write_refusal(method, "/api/v1/namespaces/kube-system", {NS})
    assert write_refusal("POST", f"/api/v1/namespaces/{NS}", {NS})


@pytest.mark.parametrize("path", IN_NAMESPACE)
def test_with_no_namespace_set_writes_are_refused(path):
    assert write_refusal("POST", path, set()) == "no problem namespace is set"


@pytest.mark.parametrize("namespaces", [{NS}, set()])
@pytest.mark.parametrize("method", READS)
@pytest.mark.parametrize("path", IN_NAMESPACE + OUTSIDE + CLUSTER_OBJECTS + WEBHOOK_CONFIGS)
def test_reads_always_pass(path, method, namespaces):
    assert write_refusal(method, path, namespaces) is None


def test_a_multi_namespace_app_may_write_in_each_of_its_namespaces():
    # Composite apps span several namespaces; the conductor passes all of them.
    namespaces = {NS, "social-network"}
    for ns in namespaces:
        assert write_refusal("PATCH", f"/apis/apps/v1/namespaces/{ns}/deployments/geo", namespaces) is None
    refusal = write_refusal("PATCH", "/apis/apps/v1/namespaces/default/deployments/geo", namespaces)
    assert refusal == f"writes are allowed only in namespace(s) {NS}, social-network"


# ------------------------------------------------------------------ end to end


@pytest.fixture(scope="module")
def tls(tmp_path_factory):
    """A self-signed certificate for 127.0.0.1 and its key, as PEM files."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl not found")
    d = tmp_path_factory.mktemp("tls")
    cert, key = d / "cert.pem", d / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
        timeout=60,
    )
    return cert, key


@pytest.fixture
def upstream(tls):
    """A fake API server over TLS: answers every request with a JSON object and records it."""
    seen: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _answer(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            seen.append((self.command, self.path))
            body = json.dumps({"kind": "ConfigMap", "metadata": {"name": "x"}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _answer

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(*tls)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server.server_address[1], seen
    server.shutdown()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def make_proxy(tmp_path, monkeypatch, tls, upstream):
    """Starts a proxy in front of `upstream`; stops it after the test."""
    cert, key = tls
    pem = [base64.b64encode(p.read_bytes()).decode() for p in (cert, key)]
    cfg = tmp_path / "kubeconfig"
    cfg.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "fake",
                "clusters": [
                    {
                        "name": "fake",
                        "cluster": {"server": f"https://127.0.0.1:{upstream[0]}", "certificate-authority-data": pem[0]},
                    }
                ],
                "users": [{"name": "fake", "user": {"client-certificate-data": pem[0], "client-key-data": pem[1]}}],
                "contexts": [{"name": "fake", "context": {"cluster": "fake", "user": "fake"}}],
            }
        )
    )
    monkeypatch.setenv("KUBECONFIG", str(cfg))
    started = []

    def make(**kwargs) -> KubernetesAPIProxy:
        proxy = KubernetesAPIProxy(hidden_namespaces={"chaos-mesh"}, listen_port=_free_port(), **kwargs)
        proxy.start()
        started.append(proxy)
        return proxy

    yield make
    for proxy in started:
        proxy.stop()


def _call(proxy: KubernetesAPIProxy, method: str, path: str) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", proxy.listen_port, timeout=30)
    body = b"{}" if method in ("POST", "PUT", "PATCH") else None
    conn.request(method, path, body=body, headers={"Content-Type": "application/json"})
    response = conn.getresponse()
    out = response.status, response.read()
    conn.close()
    return out


def test_the_running_proxy_applies_the_floor_to_the_namespace_set_after_start(make_proxy, upstream):
    _, seen = upstream
    proxy = make_proxy(write_floor=True)
    proxy.write_namespaces = {NS}  # as Conductor.start_problem does, on the running proxy

    status, body = _call(proxy, "POST", "/api/v1/namespaces/kube-system/configmaps")
    assert status == 403
    assert json.loads(body)["reason"] == "Forbidden"
    status, _ = _call(proxy, "DELETE", "/api/v1/persistentvolumes/pv-1")
    assert status == 403
    assert seen == []  # neither reached the API server

    assert _call(proxy, "DELETE", WEBHOOK_CONFIGS[0])[0] == 200
    assert _call(proxy, "PATCH", f"/apis/apps/v1/namespaces/{NS}/deployments/geo")[0] == 200
    assert _call(proxy, "GET", "/api/v1/namespaces/kube-system/configmaps/coredns")[0] == 200
    assert _call(proxy, "GET", "/api/v1/namespaces/chaos-mesh/pods")[0] == 403  # hidden, as before
    assert seen == [
        ("DELETE", WEBHOOK_CONFIGS[0]),
        ("PATCH", f"/apis/apps/v1/namespaces/{NS}/deployments/geo"),
        ("GET", "/api/v1/namespaces/kube-system/configmaps/coredns"),
    ]


def test_without_the_floor_the_proxy_forwards_writes_as_before(make_proxy, upstream):
    _, seen = upstream
    proxy = make_proxy()
    proxy.write_namespaces = {NS}

    assert _call(proxy, "POST", "/api/v1/namespaces/kube-system/configmaps")[0] == 200
    assert _call(proxy, "DELETE", "/api/v1/persistentvolumes/pv-1")[0] == 200
    assert seen == [("POST", "/api/v1/namespaces/kube-system/configmaps"), ("DELETE", "/api/v1/persistentvolumes/pv-1")]
