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
"""Injectors fail loudly instead of silently injecting nothing.

An injector that skips a target it cannot fault leaves the problem "injected" with no fault,
so the run grades an agent against a healthy cluster. These tests drive the injectors against
`FakeKubectl`, an in-memory cluster that understands the small kubectl grammar they use.
"""

import copy
import json
import shlex
from types import SimpleNamespace

import pytest
import yaml

import sregym.paths
from sregym.generators.fault import inject_app
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector

NAMESPACE = "hotel-reservation"

_KINDS = {
    "deployment": "Deployment",
    "deployments": "Deployment",
    "service": "Service",
    "svc": "Service",
    "configmap": "ConfigMap",
    "cm": "ConfigMap",
}


class FakeKubectl:
    """In-memory stand-in for KubeCtl. Unknown commands raise instead of being ignored."""

    def __init__(self, exec_output=""):
        self.objects = {}  # (namespace, kind, name) -> manifest dict
        self.commands = []
        self.exec_output = exec_output

    def add(self, manifest):
        meta = manifest["metadata"]
        self.objects[(meta["namespace"], manifest["kind"], meta["name"])] = copy.deepcopy(manifest)

    def get(self, namespace, kind, name):
        return self.objects.get((namespace, kind, name))

    def wait_for_stable(self, namespace, **_):
        self.commands.append(f"<wait_for_stable {namespace}>")

    def wait_for_ready(self, namespace, **_):
        self.commands.append(f"<wait_for_ready {namespace}>")

    def exec_command(self, command, input_data=None):
        self.commands.append(command)
        tokens = shlex.split(command)
        assert tokens[0] == "kubectl", command
        flags, args = {}, []
        it = iter(tokens[1:])
        for tok in it:
            if tok in ("-n", "-f", "-l", "-o", "--namespace"):
                flags[tok] = next(it)
            elif tok.startswith("--"):
                flags[tok.split("=", 1)[0]] = tok.split("=", 1)[1] if "=" in tok else True
            elif tok == "--":
                break
            else:
                args.append(tok)
        namespace = flags.get("-n", "default")
        verb = args[0]
        if verb == "get":
            obj = self.get(namespace, _KINDS[args[1]], args[2])
            if obj is None:
                return f'Error from server (NotFound): {args[1]} "{args[2]}" not found'
            return yaml.dump(obj)
        if verb == "delete":
            self.objects.pop((namespace, _KINDS[args[1]], args[2]), None)
            return ""
        if verb == "apply":
            with open(flags["-f"]) as f:
                manifest = yaml.safe_load(f)
            manifest["metadata"].setdefault("namespace", namespace)
            self.add(manifest)
            return "applied"
        if verb == "rollout":
            if args[1] == "status":
                assert "--timeout" in flags, f"unbounded rollout wait: {command}"
            return ""
        if verb == "exec":
            return self.exec_output
        raise AssertionError(f"FakeKubectl does not model: {command}")


def _deployment(name, containers=None, volumes=None):
    spec = {"containers": containers or [{"name": name, "image": f"img/{name}"}]}
    if volumes:
        spec["volumes"] = volumes
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {"metadata": {"labels": {"app": name}}, "spec": spec},
        },
    }


def _service(name):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {"selector": {"app": name}, "ports": [{"port": 8080}]},
    }


@pytest.fixture(autouse=True)
def _scratch(tmp_path, monkeypatch):
    """Keep fault backups out of the shared /tmp."""
    monkeypatch.setattr(sregym.paths, "FAULT_SCRATCH_ROOT", tmp_path)
    monkeypatch.delenv("SREGYM_KIND_CLUSTER_NAME", raising=False)


@pytest.fixture
def cluster():
    kube = FakeKubectl()
    kube.add(_deployment("geo"))
    kube.add(_service("geo"))
    return kube


def _injector(kube, namespace=NAMESPACE):
    injector = VirtualizationFaultInjector.__new__(VirtualizationFaultInjector)
    injector.testbed = namespace
    injector.namespace = namespace
    injector.kubectl = kube
    return injector


@pytest.mark.parametrize(
    "inject",
    [
        lambda inj: inj.inject_pvc_claim_mismatch(["geo"]),
        lambda inj: inj.inject_wrong_bin_usage(["geo"]),
        lambda inj: inj.inject_env_variable_shadowing(["geo"]),
        lambda inj: inj.inject_missing_service(["no-such-service"]),
        lambda inj: inj.inject_liveness_probe_misconfiguration(["no-such-deployment"]),
    ],
    ids=[
        "pvc_without_volume",
        "wrong_bin_without_profile",
        "env_shadowing_unsupported",
        "missing_service",
        "missing_deployment",
    ],
)
def test_injector_that_would_inject_nothing_raises_and_changes_nothing(cluster, inject):
    before = copy.deepcopy(cluster.objects)

    with pytest.raises(ValueError):
        inject(_injector(cluster))

    assert cluster.objects == before


def test_pvc_claim_mismatch_checks_every_target_before_changing_any(cluster):
    volumes = [{"name": "data", "persistentVolumeClaim": {"claimName": "geo-pvc"}}]
    cluster.add(_deployment("mongodb-geo", volumes=volumes))
    before = copy.deepcopy(cluster.objects)

    with pytest.raises(ValueError, match="geo"):
        _injector(cluster).inject_pvc_claim_mismatch(["mongodb-geo", "geo"])

    assert cluster.objects == before
    assert not any(c.startswith(("kubectl delete", "kubectl apply")) for c in cluster.commands)


def test_pvc_claim_mismatch_breaks_the_claim(cluster):
    volumes = [{"name": "data", "persistentVolumeClaim": {"claimName": "geo-pvc"}}]
    cluster.add(_deployment("mongodb-geo", volumes=volumes))

    _injector(cluster).inject_pvc_claim_mismatch(["mongodb-geo"])

    live = cluster.get(NAMESPACE, "Deployment", "mongodb-geo")
    assert live["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] == "geo-pvc-broken"


def test_configmap_drift_on_unsupported_service_raises():
    kube = FakeKubectl(exec_output=json.dumps({"consulAddress": "consul:8500"}))

    with pytest.raises(ValueError, match="ConfigMap drift"):
        _injector(kube).inject_configmap_drift(["rate"])

    assert not any("create configmap" in c for c in kube.commands)


@pytest.mark.parametrize(
    "output",
    ['Error from server (NotFound): deployments.apps "geo" not found', "error: the server doesn't have a resource", ""],
)
def test_kubectl_error_text_is_not_mistaken_for_an_object(output):
    kube = SimpleNamespace(exec_command=lambda command: output)

    with pytest.raises(ValueError, match="Cannot read deployment 'geo'"):
        _injector(kube)._get_deployment_yaml("geo")


def test_rollout_waits_are_bounded(cluster):
    cluster.add(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "mongo-geo-script", "namespace": NAMESPACE},
            "data": {"k": "v"},
        }
    )
    injector = _injector(cluster)
    backup = sregym.paths.fault_scratch_path("geo_modified.yaml")
    with open(backup, "w") as f:
        yaml.dump(cluster.get(NAMESPACE, "ConfigMap", "mongo-geo-script"), f)

    injector.recover_missing_configmap(["geo"])  # FakeKubectl asserts every rollout status has --timeout

    assert any("rollout status" in c for c in cluster.commands)


def test_storage_baseline_is_a_fault_scratch_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "kind-a")

    path = _injector(FakeKubectl())._storage_baseline_path("jaeger")

    assert path == tmp_path / "sregym-kind-a" / f"deployment-state-{NAMESPACE}-jaeger.yaml"


def test_misconfig_app_without_target_container_raises(monkeypatch):
    deployment = SimpleNamespace(
        spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[SimpleNamespace(name="other")])))
    )
    kube = SimpleNamespace(get_deployment=lambda name, ns: deployment, update_deployment=None)
    monkeypatch.setattr(inject_app, "KubeCtl", lambda: kube)

    with pytest.raises(ValueError, match="hotel-reserv-geo"):
        ApplicationFaultInjector(NAMESPACE).inject_misconfig_app(["geo"])
