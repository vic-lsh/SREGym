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
from sregym.service.kubectl import KubeCtl

NAMESPACE = "hotel-reservation"

_KINDS = {
    "deployment": "Deployment",
    "deployments": "Deployment",
    "service": "Service",
    "svc": "Service",
    "configmap": "ConfigMap",
    "cm": "ConfigMap",
}


def _selector_matches(selector: str, labels: dict) -> bool:
    return all(labels.get(k) == v for k, v in (term.split("=", 1) for term in selector.split(",")))


def _pod(name, labels, waiting_reason=None, namespace=NAMESPACE):
    ready = waiting_reason is None
    waiting = SimpleNamespace(reason=waiting_reason) if waiting_reason else None
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels=labels, namespace=namespace),
        status=SimpleNamespace(
            phase="Running" if ready else "Pending",
            container_statuses=[SimpleNamespace(ready=ready, state=SimpleNamespace(waiting=waiting))],
            conditions=[],
        ),
    )


class FakeKubectl:
    """In-memory stand-in for KubeCtl. Unknown commands raise instead of being ignored.

    Its namespace-wide `wait_for_ready` / `wait_for_stable` fail whenever any pod in the
    namespace is unhealthy (what the real gates do after their timeout), so a test fails if an
    injector still gates on namespace-wide health.
    """

    is_ready = KubeCtl.is_ready  # the real, pure pod-settled predicate

    def __init__(self, exec_output=""):
        self.objects = {}  # (namespace, kind, name) -> manifest dict
        self.pods = []
        self.commands = []
        self.namespace_waits = []
        self.exec_output = exec_output
        self.core_v1_api = SimpleNamespace(list_namespaced_pod=self._list_pods)

    def add(self, manifest):
        meta = manifest["metadata"]
        self.objects[(meta["namespace"], manifest["kind"], meta["name"])] = copy.deepcopy(manifest)

    def get(self, namespace, kind, name):
        return self.objects.get((namespace, kind, name))

    def _list_pods(self, namespace, label_selector=None):
        items = [
            p
            for p in self.pods
            if p.metadata.namespace == namespace
            and (label_selector is None or _selector_matches(label_selector, p.metadata.labels))
        ]
        return SimpleNamespace(items=items)

    def get_deployment(self, name, namespace):
        manifest = self.get(namespace, "Deployment", name)
        if manifest is None:
            raise LookupError(f"deployment {name} not found")
        labels = manifest["spec"]["selector"]["matchLabels"]
        return SimpleNamespace(spec=SimpleNamespace(selector=SimpleNamespace(match_labels=labels)))

    def wait_for_ready(self, namespace, service_names=None, **_):
        self.namespace_waits.append(("ready", namespace, service_names))
        if service_names is None:
            pods = self._list_pods(namespace).items
            if not all(p.status.phase == "Running" and p.status.container_statuses[0].ready for p in pods):
                raise Exception(f"Timeout: not all pods in namespace '{namespace}' are Ready")

    def wait_for_stable(self, namespace, **_):
        self.namespace_waits.append(("stable", namespace, None))
        if not all(self.is_ready(p) for p in self._list_pods(namespace).items):
            raise Exception(f"Timeout: namespace '{namespace}' did not stabilize")

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
        if verb == "delete" and args[1] == "pods":
            self.pods = [
                p
                for p in self.pods
                if not (p.metadata.namespace == namespace and _selector_matches(flags["-l"], p.metadata.labels))
            ]
            return ""
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
    spec = {"containers": containers or [{"name": name, "image": f"img/{name}", "ports": [{"containerPort": 8080}]}]}
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
    injector._save_backup("missing_configmap", "geo", cluster.get(NAMESPACE, "ConfigMap", "mongo-geo-script"))

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


@pytest.fixture
def broken_cluster(cluster):
    """`cluster` plus a `broken` workload an earlier fault left in ImagePullBackOff."""
    cluster.add(_deployment("broken"))
    cluster.add(_service("broken"))
    cluster.pods = [
        _pod("geo-1", {"app": "geo"}),
        _pod("broken-1", {"app": "broken"}, waiting_reason="ImagePullBackOff"),
    ]
    return cluster


@pytest.mark.parametrize(
    "inject",
    [
        lambda inj: inj.inject_liveness_probe_too_aggressive(["geo"]),
        lambda inj: inj.inject_sidecar_port_conflict(["geo"]),
        lambda inj: inj.inject_missing_service(["geo"]),
    ],
    ids=["liveness_probe_too_aggressive", "sidecar_port_conflict", "missing_service"],
)
def test_injector_succeeds_in_namespace_an_earlier_fault_broke(broken_cluster, inject):
    inject(_injector(broken_cluster))

    assert broken_cluster.namespace_waits == []


def test_pvc_claim_mismatch_succeeds_in_broken_namespace(broken_cluster):
    volumes = [{"name": "data", "persistentVolumeClaim": {"claimName": "geo-pvc"}}]
    broken_cluster.add(_deployment("mongodb-geo", volumes=volumes))

    _injector(broken_cluster).inject_pvc_claim_mismatch(["mongodb-geo"])

    live = broken_cluster.get(NAMESPACE, "Deployment", "mongodb-geo")
    assert live["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] == "geo-pvc-broken"
    assert broken_cluster.namespace_waits == []


def test_missing_service_restarts_only_its_backend_pods(broken_cluster):
    injector = _injector(broken_cluster)

    injector.inject_missing_service(["geo"])

    assert broken_cluster.get(NAMESPACE, "Service", "geo") is None
    assert not any("--all" in c for c in broken_cluster.commands)
    assert [p.metadata.name for p in broken_cluster.pods] == ["broken-1"]

    injector.recover_missing_service(["geo"])

    assert broken_cluster.get(NAMESPACE, "Service", "geo")["spec"]["selector"] == {"app": "geo"}
    assert not any("--all" in c for c in broken_cluster.commands)


def test_two_faults_on_one_deployment_recover_to_the_original(broken_cluster):
    injector = _injector(broken_cluster)
    original = copy.deepcopy(broken_cluster.get(NAMESPACE, "Deployment", "geo"))

    injector.inject_readiness_probe_misconfiguration(["geo"])
    injector.inject_liveness_probe_misconfiguration(["geo"])
    injector.recover_liveness_probe_misconfiguration(["geo"])
    injector.recover_readiness_probe_misconfiguration(["geo"])

    assert broken_cluster.get(NAMESPACE, "Deployment", "geo") == original


def test_recovery_continues_when_namespace_stays_broken(broken_cluster):
    injector = _injector(broken_cluster)
    injector.inject_liveness_probe_too_aggressive(["geo"])

    injector.recover_liveness_probe_too_aggressive(["geo"])  # `broken` stays broken; must not raise

    assert ("ready", NAMESPACE, None) in broken_cluster.namespace_waits
    assert (
        "livenessProbe"
        not in broken_cluster.get(NAMESPACE, "Deployment", "geo")["spec"]["template"]["spec"]["containers"][0]
    )


def test_backups_are_per_fault_scratch_files(tmp_path, monkeypatch):
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "kind-a")

    path = _injector(FakeKubectl())._backup_path("sidecar_port_conflict", "geo")

    assert path == str(tmp_path / "sregym-kind-a" / f"{NAMESPACE}_geo_sidecar_port_conflict_backup.yaml")


def _astronomy_frontend_proxy():
    manifest = _deployment(
        "frontend-proxy",
        containers=[
            {
                "name": "frontend-proxy",
                "image": "img/frontend-proxy",
                "env": [{"name": "ENVOY_PORT", "value": "8080"}, {"name": "FRONTEND_HOST", "value": "frontend"}],
            }
        ],
    )
    manifest["metadata"]["namespace"] = "astronomy-shop"
    return manifest


def test_env_variable_shadowing_overwrites_frontend_host_and_recovers_the_backup():
    kube = FakeKubectl()
    original = _astronomy_frontend_proxy()
    kube.add(original)
    injector = _injector(kube, namespace="astronomy-shop")

    injector.inject_env_variable_shadowing(["frontend-proxy"])

    env = kube.get("astronomy-shop", "Deployment", "frontend-proxy")["spec"]["template"]["spec"]["containers"][0]["env"]
    assert env == [{"name": "ENVOY_PORT", "value": "8080"}, {"name": "FRONTEND_HOST", "value": "localhost"}]
    with open(injector._backup_path("env_variable_shadowing", "frontend-proxy")) as f:
        assert yaml.safe_load(f) == original

    injector.recover_env_variable_shadowing(["frontend-proxy"])

    assert kube.get("astronomy-shop", "Deployment", "frontend-proxy") == original
    assert "kubectl rollout status deployment/frontend-proxy -n astronomy-shop --timeout=120s" in kube.commands


def test_empty_env_variable_blanks_only_the_named_variable(monkeypatch):
    env = [SimpleNamespace(name="GEO_HOST", value="geo:8083"), SimpleNamespace(name="OTHER", value="x")]
    deployment = SimpleNamespace(
        spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[SimpleNamespace(env=env)])))
    )
    updates = []
    kube = SimpleNamespace(
        get_deployment=lambda name, ns: deployment,
        update_deployment=lambda name, ns, dep: updates.append((name, ns, dep)),
    )
    monkeypatch.setattr(inject_app, "KubeCtl", lambda: kube)

    ApplicationFaultInjector(NAMESPACE).inject_empty_env_variable("frontend", "GEO_HOST")

    assert [(e.name, e.value) for e in env] == [("GEO_HOST", ""), ("OTHER", "x")]
    assert updates == [("frontend", NAMESPACE, deployment)]

    with pytest.raises(ValueError, match="MISSING"):
        ApplicationFaultInjector(NAMESPACE).inject_empty_env_variable("frontend", "MISSING")
