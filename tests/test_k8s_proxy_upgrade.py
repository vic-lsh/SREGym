"""The agent API proxy against an upgrade-capable fake Kubernetes API server.

The fake upstream speaks TLS like a real API server, answers pod GETs, completes
exec upgrades with ``101 Switching Protocols`` and then echoes raw bytes, and
holds watch and follow-mode log streams open, so these tests pin down what the
proxy forwards, refuses and streams without a cluster.
"""

from __future__ import annotations

import datetime
import ipaddress
import json
import socket
import ssl
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from sregym.service import k8s_proxy

PODS = {
    ("app", "web"): {"app": "web"},
    ("app", "loadgen"): {"app": "load-generator"},
    ("other", "db"): {"app": "db"},
}


def _self_signed() -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fake-apiserver")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    key_pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return cert_pem, key_pem


@dataclass
class FakeAPIServer:
    """A TLS server that answers just enough of the Kubernetes API."""

    cert_pem: str
    key_pem: str
    port: int = 0
    #: Raw header blocks of every upgrade request received.
    upgrade_requests: list[str] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)

    def start(self, tmp_path) -> None:
        cert_file, key_file = tmp_path / "srv.crt", tmp_path / "srv.key"
        cert_file.write_text(self.cert_pem)
        key_file.write_text(self.key_pem)
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(str(cert_file), str(key_file))
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        self._listener.close()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                raw, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(raw,), daemon=True).start()

    def _serve(self, raw: socket.socket) -> None:
        try:
            conn = self._context.wrap_socket(raw, server_side=True)
        except (OSError, ssl.SSLError):
            return
        try:
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                head += chunk
            header_block, _, rest = head.partition(b"\r\n\r\n")
            text = header_block.decode("latin-1")
            request_line = text.split("\r\n", 1)[0]
            _, path, _ = request_line.split(" ")
            if "upgrade:" in text.lower():
                self._upgrade(conn, text, rest)
            elif path.startswith("/api/v1/namespaces/") and "/pods/" in path and path.count("/") == 6:
                self._pod(conn, path)
            elif "watch=true" in path:
                self._watch(conn)
            elif "follow=true" in path:
                self._follow(conn)
            else:
                self._respond(conn, 404, b'{"kind":"Status","code":404}')
        except (OSError, ssl.SSLError):
            pass
        finally:
            conn.close()

    @staticmethod
    def _respond(conn, status: int, body: bytes, content_type: str = "application/json") -> None:
        conn.sendall(
            f"HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n".encode()
            + body
        )

    def _pod(self, conn, path: str) -> None:
        parts = path.split("?")[0].split("/")
        labels = PODS.get((parts[4], parts[6]))
        if labels is None:
            self._respond(conn, 404, b'{"kind":"Status","code":404}')
            return
        pod = {"kind": "Pod", "metadata": {"name": parts[6], "namespace": parts[4], "labels": labels}}
        self._respond(conn, 200, json.dumps(pod).encode())

    def _upgrade(self, conn, text: str, rest: bytes) -> None:
        self.upgrade_requests.append(text)
        conn.sendall(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Protocol: v5.channel.k8s.io\r\n\r\n"
        )
        if rest:
            conn.sendall(rest)
        while data := conn.recv(4096):  # echo raw frames back
            conn.sendall(data)

    def _stream_headers(self, conn, content_type: str) -> None:
        conn.sendall(f"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\n\r\n".encode())

    @staticmethod
    def _chunk(conn, data: bytes) -> None:
        conn.sendall(f"{len(data):x}\r\n".encode() + data + b"\r\n")

    def _watch(self, conn) -> None:
        self._stream_headers(conn, "application/json")
        for name, labels in (("loadgen", {"app": "load-generator"}), ("web", {"app": "web"})):
            event = {"type": "ADDED", "object": {"kind": "Pod", "metadata": {"name": name, "labels": labels}}}
            self._chunk(conn, json.dumps(event).encode() + b"\n")
        table = {
            "type": "MODIFIED",
            "object": {
                "kind": "Table",
                "rows": [
                    {"object": {"metadata": {"name": "web2", "labels": {"app": "web"}}}},
                    {"object": {"metadata": {"name": "lg2", "labels": {"app": "load-generator"}}}},
                ],
            },
        }
        self._chunk(conn, json.dumps(table).encode() + b"\n")
        self._stop.wait(30)  # an open watch never ends by itself

    def _follow(self, conn) -> None:
        self._stream_headers(conn, "text/plain")
        self._chunk(conn, b"tick 0\n")
        self._stop.wait(30)


@pytest.fixture
def upstream(tmp_path) -> Iterator[FakeAPIServer]:
    cert_pem, key_pem = _self_signed()
    server = FakeAPIServer(cert_pem, key_pem)
    server.start(tmp_path)
    yield server
    server.stop()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def make_proxy(monkeypatch, upstream) -> Iterator:
    started: list[k8s_proxy.KubernetesAPIProxy] = []

    def build(allow_exec: bool | None, exec_namespaces: set[str] = frozenset({"app"})):
        monkeypatch.setattr(k8s_proxy.os.path, "exists", lambda path: False)
        monkeypatch.setattr(k8s_proxy.config, "load_kube_config", lambda config_file: None)
        monkeypatch.setattr(
            k8s_proxy.KubernetesAPIProxy,
            "_load_cluster_config",
            lambda self, kubeconfig_path: ("127.0.0.1", upstream.port, upstream.cert_pem, None, None),
        )
        proxy = k8s_proxy.KubernetesAPIProxy(
            hidden_namespaces={"chaos-mesh", "khaos"}, listen_port=_free_port(), allow_exec=allow_exec
        )
        proxy.exec_namespaces = set(exec_namespaces)
        proxy.start()
        started.append(proxy)
        return proxy

    yield build
    for proxy in started:
        proxy.stop()


def _send(port: int, request: bytes) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(request)
    return sock


def _read_head(sock: socket.socket) -> tuple[int, str, bytes]:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    text = head.decode("latin-1")
    return int(text.split(" ")[1]), text, rest


def _read_body(sock: socket.socket, rest: bytes) -> bytes:
    body = rest
    while chunk := sock.recv(4096):
        body += chunk
    return body


def _exec_request(namespace: str, pod: str, subresource: str = "exec") -> bytes:
    return (
        f"GET /api/v1/namespaces/{namespace}/pods/{pod}/{subresource}?command=echo&command=ok&stdout=true HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: v5.channel.k8s.io\r\nSec-WebSocket-Protocol: v4.channel.k8s.io\r\n\r\n"
    ).encode()


def test_allow_exec_defaults_off_and_reads_the_env_switch() -> None:
    assert k8s_proxy.allow_exec_from_env({}) is False
    assert k8s_proxy.allow_exec_from_env({k8s_proxy.ALLOW_EXEC_ENV: "true"}) is True
    assert k8s_proxy.allow_exec_from_env({k8s_proxy.ALLOW_EXEC_ENV: "0"}) is False
    with pytest.raises(ValueError, match=k8s_proxy.ALLOW_EXEC_ENV):
        k8s_proxy.allow_exec_from_env({k8s_proxy.ALLOW_EXEC_ENV: "maybe"})


def test_exec_disabled_answers_a_kubernetes_forbidden_status(make_proxy, upstream) -> None:
    proxy = make_proxy(allow_exec=False)
    for subresource in ("exec", "attach", "portforward"):
        sock = _send(proxy.listen_port, _exec_request("app", "web", subresource))
        status, head, rest = _read_head(sock)
        body = json.loads(_read_body(sock, rest))
        sock.close()
        assert status == 403
        assert "application/json" in head
        assert body["kind"] == "Status" and body["reason"] == "Forbidden" and body["code"] == 403
        assert "disabled" in body["message"]
    assert upstream.upgrade_requests == []


def test_exec_enabled_tunnels_the_upgrade_and_raw_bytes(make_proxy, upstream) -> None:
    proxy = make_proxy(allow_exec=True)
    sock = _send(proxy.listen_port, _exec_request("app", "web"))
    status, head, rest = _read_head(sock)
    assert status == 101
    assert "Sec-WebSocket-Protocol: v5.channel.k8s.io" in head
    sock.sendall(b"\x82\x05hello")
    echoed = rest
    while len(echoed) < 7:
        echoed += sock.recv(4096)
    sock.close()
    assert echoed == b"\x82\x05hello"
    forwarded = upstream.upgrade_requests[0]
    assert "Upgrade: websocket" in forwarded and "Connection: Upgrade" in forwarded
    # Repeated protocol-negotiation headers survive (SPDY sends several X-Stream-Protocol-Version).
    assert forwarded.count("Sec-WebSocket-Protocol:") == 2
    assert f"Host: 127.0.0.1:{upstream.port}" in forwarded


@pytest.mark.parametrize(
    ("namespace", "pod", "reason"),
    [
        ("other", "db", "application namespace"),  # outside the problem's app namespace
        ("app", "loadgen", "not allowed"),  # hidden benchmark load generator
    ],
)
def test_exec_enabled_still_refuses_pods_outside_the_agent_scope(make_proxy, upstream, namespace, pod, reason) -> None:
    proxy = make_proxy(allow_exec=True)
    sock = _send(proxy.listen_port, _exec_request(namespace, pod))
    status, _, rest = _read_head(sock)
    body = json.loads(_read_body(sock, rest))
    sock.close()
    assert status == 403 and reason in body["message"]
    assert upstream.upgrade_requests == []


def test_hidden_namespace_exec_is_refused_before_any_upgrade(make_proxy, upstream) -> None:
    proxy = make_proxy(allow_exec=True, exec_namespaces={"app", "khaos"})
    sock = _send(proxy.listen_port, _exec_request("khaos", "injector"))
    status, _, _ = _read_head(sock)
    sock.close()
    assert status == 403
    assert upstream.upgrade_requests == []


def test_upgrade_on_other_paths_is_refused(make_proxy, upstream) -> None:
    proxy = make_proxy(allow_exec=True)
    request = (
        b"GET /api/v1/namespaces/app/services/web/proxy/ HTTP/1.1\r\nHost: x\r\n"
        b"Connection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
    )
    sock = _send(proxy.listen_port, request)
    status, _, _ = _read_head(sock)
    sock.close()
    assert status == 403
    assert upstream.upgrade_requests == []


def test_follow_logs_stream_before_the_upstream_finishes(make_proxy) -> None:
    proxy = make_proxy(allow_exec=False)
    sock = _send(proxy.listen_port, b"GET /api/v1/namespaces/app/pods/web/log?follow=true HTTP/1.1\r\nHost: x\r\n\r\n")
    status, _, rest = _read_head(sock)
    started = time.monotonic()
    while b"tick 0" not in rest:
        rest += sock.recv(4096)
    sock.close()
    assert status == 200
    assert time.monotonic() - started < 5  # arrived while the upstream stream is still open


def test_watch_streams_events_and_drops_hidden_ones(make_proxy) -> None:
    proxy = make_proxy(allow_exec=False)
    sock = _send(proxy.listen_port, b"GET /api/v1/namespaces/app/pods?watch=true HTTP/1.1\r\nHost: x\r\n\r\n")
    status, _, data = _read_head(sock)
    while data.count(b"\n") < 2:
        data += sock.recv(4096)
    sock.close()
    assert status == 200
    events = [json.loads(line) for line in data.splitlines() if line.strip()]
    assert [event["object"]["metadata"]["name"] for event in events[:1]] == ["web"]
    table_rows = events[1]["object"]["rows"]
    assert [row["object"]["metadata"]["name"] for row in table_rows] == ["web2"]
