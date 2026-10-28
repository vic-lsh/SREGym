"""Rotated webhook Secrets must change the pod template and live TLS readiness."""

import ast
import base64
import datetime
import inspect
import textwrap
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from sregym.conductor.problems.admission_webhook_tls_mismatch import AdmissionWebhookTLSMismatch
from sregym.conductor.problems.cumulative_admission_webhook_timeout_hotel_reservation import (
    CumulativeAdmissionWebhookTimeoutHotelReservation,
)
from sregym.conductor.problems.mutating_webhook_resource_limits import MutatingWebhookResourceLimits


@pytest.fixture
def certificates():
    """Two independent synthetic certs with identical names and lifetimes."""
    now = datetime.datetime.now(datetime.UTC)
    result = []
    for _ in range(2):
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic-webhook")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .sign(key, hashes.SHA256())
        )
        result.append(
            (
                cert.public_bytes(serialization.Encoding.PEM).decode(),
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ).decode(),
            )
        )
    return result


class BackendAPI:
    """Kubernetes replacement boundary; pod generations follow template identity."""

    def __init__(self):
        self.secret = None
        self.deployment = None
        self.templates = []

    def create_namespaced_secret(self, namespace, body):
        if self.secret is not None:
            raise ApiException(status=409)
        self.secret = body

    def read_namespaced_secret(self, name, namespace):
        return self.secret

    def replace_namespaced_secret(self, name, namespace, body):
        self.secret = body

    def create_namespaced_deployment(self, namespace, body):
        if self.deployment is not None:
            raise ApiException(status=409)
        self._store(body)

    def read_namespaced_deployment(self, name, namespace):
        return self.deployment

    def replace_namespaced_deployment(self, name, namespace, body):
        self._store(body)

    def _store(self, body):
        self.deployment = body
        self.templates.append(client.ApiClient().sanitize_for_serialization(body)["spec"]["template"])


def backend_templates(problem_class, certificates):
    problem = problem_class.__new__(problem_class)
    templates = []
    if problem_class is CumulativeAdmissionWebhookTimeoutHotelReservation:
        api = BackendAPI()
        problem.core_v1 = api
        problem.apps_v1 = api
        for cert, key in certificates:
            problem._ensure_tls_secret(cert, key)
            problem._ensure_backend_deployment(problem.WEBHOOK_BACKEND_NAMES[0])
        return api.templates

    material = iter(certificates)

    def generate():
        cert, key = next(material)
        return {
            "tls_crt_b64": base64.b64encode(cert.encode()).decode(),
            "tls_key_b64": base64.b64encode(key.encode()).decode(),
            "wrong_ca_bundle": "synthetic-wrong-ca",
            "ca_bundle": "synthetic-ca",
        }

    def run(args, **kwargs):
        if args[1:] == ["apply", "-f", "-"]:
            deployment = next(doc for doc in yaml.safe_load_all(kwargs["input"]) if doc["kind"] == "Deployment")
            templates.append(deepcopy(deployment["spec"]["template"]))
        return SimpleNamespace(returncode=0)

    problem._generate_tls_material = generate
    problem._run = run
    for _ in certificates:
        problem._ensure_webhook_backend()
    return templates


WEBHOOKS = [
    AdmissionWebhookTLSMismatch,
    MutatingWebhookResourceLimits,
    CumulativeAdmissionWebhookTimeoutHotelReservation,
]


@pytest.mark.parametrize("problem_class", WEBHOOKS)
def test_every_rotating_webhook_rolls_only_when_the_certificate_changes(problem_class, certificates):
    first, second = certificates
    templates = backend_templates(problem_class, [first, first, second])

    assert templates[0] == templates[1], "unchanged TLS material must be idempotent"
    assert templates[1] != templates[2], "rotating TLS material must roll the backend"
    for template in templates:
        probe = template["spec"]["containers"][0].get("readinessProbe", {})
        assert probe.get("exec", {}).get("command"), "readiness must probe the live TLS listener"


@pytest.mark.parametrize("problem_class", WEBHOOKS)
def test_rendered_probe_checks_the_live_certificate_not_the_secret_file(problem_class, certificates, tmp_path):
    import contextlib
    import socketserver
    import ssl
    import subprocess
    import sys
    import threading

    first, second = certificates
    templates = backend_templates(problem_class, [first, second])
    cert_file, key_file = tmp_path / "tls.crt", tmp_path / "tls.key"

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            pass

    def probe(template, port):
        command = template["spec"]["containers"][0]["readinessProbe"]["exec"]["command"]
        # Only the interpreter and ephemeral test port change; execute the
        # exact probe produced by the fault against a real TLS listener.
        return subprocess.run(
            [sys.executable, *command[1:3], str(port), *command[4:]],
            capture_output=True,
            text=True,
            timeout=10,
        )

    @contextlib.contextmanager
    def backend(material):
        cert_file.write_text(material[0])
        key_file.write_text(material[1])
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)
        with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
            thread.start()
            try:
                yield server.server_address[1]
            finally:
                server.shutdown()
                thread.join(timeout=5)

    with backend(first) as port:
        assert probe(templates[0], port).returncode == 0
        # Projecting a new Secret does not reload a startup SSLContext.
        cert_file.write_text(second[0])
        key_file.write_text(second[1])
        mismatch = probe(templates[1], port)
        assert mismatch.returncode != 0
        assert "certificate differs" in mismatch.stderr
    with backend(second) as port:
        assert probe(templates[1], port).returncode == 0
        assert probe(templates[0], port).returncode != 0
    # No listener must never count as ready either.
    assert probe(templates[1], port).returncode != 0


@pytest.mark.parametrize(
    "status",
    [
        {"observed_generation": 1, "updated_replicas": 1, "replicas": 1},
        {"observed_generation": 2, "updated_replicas": 0, "replicas": 1},
        {"observed_generation": 2, "updated_replicas": 1, "replicas": 2},
    ],
)
def test_cumulative_readiness_cannot_accept_the_previous_pod_generation(status, monkeypatch):
    import sregym.conductor.problems.cumulative_admission_webhook_timeout_hotel_reservation as module

    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(generation=2),
        status=client.V1DeploymentStatus(ready_replicas=1, available_replicas=1, **status),
        spec=client.V1DeploymentSpec(
            selector=client.V1LabelSelector(), template=client.V1PodTemplateSpec(), replicas=1
        ),
    )
    problem = CumulativeAdmissionWebhookTimeoutHotelReservation.__new__(
        CumulativeAdmissionWebhookTimeoutHotelReservation
    )
    problem.apps_v1 = SimpleNamespace(read_namespaced_deployment=lambda **kwargs: deployment)
    ticks = iter([0, 0, 2])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="did not become Available"):
        problem._wait_for_deployment_ready("synthetic-backend", "synthetic-policy", timeout_s=1)


def _startup_tls_server_modules(directory: Path) -> set[str]:
    def loads_certificate(tree):
        return any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "load_cert_chain"
            for node in ast.walk(tree)
        )

    found = set()
    for path in directory.glob("*.py"):
        tree = ast.parse(path.read_text())
        if loads_certificate(tree):
            found.add(path.stem)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                try:
                    embedded = ast.parse(textwrap.dedent(node.value))
                except (SyntaxError, ValueError):
                    continue
                if loads_certificate(embedded):
                    found.add(path.stem)
    return found


def test_startup_tls_server_inventory_has_rotation_coverage():
    """A new startup-loaded TLS fault must join the real manifest/probe sweep."""
    directory = Path(inspect.getfile(AdmissionWebhookTLSMismatch)).parent
    assert _startup_tls_server_modules(directory) == {problem.__module__.rsplit(".", 1)[1] for problem in WEBHOOKS}


def test_tls_server_inventory_detects_new_direct_and_embedded_loaders(tmp_path):
    script = 'ctx.load_cert_chain("synthetic.crt", "synthetic.key")'
    (tmp_path / "direct.py").write_text(script)
    (tmp_path / "embedded.py").write_text(f"server_code = {script!r}")
    (tmp_path / "description_only.py").write_text('description = "load_cert_chain"')
    assert _startup_tls_server_modules(tmp_path) == {"direct", "embedded"}


def test_cumulative_recovery_waits_for_tls_ready_backends(certificates):
    problem = CumulativeAdmissionWebhookTimeoutHotelReservation.__new__(
        CumulativeAdmissionWebhookTimeoutHotelReservation
    )
    api = BackendAPI()
    problem.core_v1 = api
    problem.apps_v1 = api
    problem.namespace = "synthetic-app"
    problem._ensure_tls_secret(*certificates[0])
    problem._ensure_backend_deployment(problem.WEBHOOK_BACKEND_NAMES[0])
    api.deployment.metadata.generation = 2
    api.deployment.status = client.V1DeploymentStatus(
        observed_generation=2,
        replicas=1,
        updated_replicas=1,
        ready_replicas=1,
        available_replicas=1,
    )
    reads = []
    read = api.read_namespaced_deployment

    def read_backend(name, namespace):
        reads.append((name, namespace))
        return read(name, namespace)

    api.read_namespaced_deployment = read_backend
    problem._remove_decoy_webhooks = lambda: None
    problem._ensure_apiserver_allow_network_policy = lambda: None
    problem._wait_for_recovery = lambda: None
    problem.recover_fault.__wrapped__(problem)
    assert reads == [(name, problem.POLICY_NAMESPACE) for name in problem.WEBHOOK_BACKEND_NAMES]


@pytest.mark.parametrize("problem_class", [AdmissionWebhookTLSMismatch, MutatingWebhookResourceLimits])
def test_recovery_does_not_silently_keep_a_stale_backend(problem_class, monkeypatch):
    import subprocess
    from importlib import import_module

    module = import_module(problem_class.__module__)
    problem = problem_class.__new__(problem_class)
    problem.namespace = "synthetic-app"
    problem.faulty_service = "synthetic-service"
    if problem_class is AdmissionWebhookTLSMismatch:
        problem._delete_webhook = lambda: None
    else:
        problem._remove_decoy_webhooks = lambda: None
        problem.admission_api = SimpleNamespace(delete_mutating_webhook_configuration=lambda **_: None)

    def run(args, **kwargs):
        if args[1:3] == ["delete", "namespace"]:
            result = subprocess.CompletedProcess(args, 1)
            if kwargs.get("check"):
                result.check_returncode()
            return result
        return subprocess.CompletedProcess(args, 0)

    monkeypatch.setattr(module.subprocess, "run", run)
    # The decorator logs recovery failures; compositions use the wrapped method
    # so a failed cleanup propagates rather than admitting an invalid next run.
    with pytest.raises(subprocess.CalledProcessError):
        problem.recover_fault.__wrapped__(problem)
