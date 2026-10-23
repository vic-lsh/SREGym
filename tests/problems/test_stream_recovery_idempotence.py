"""Synthetic API semantics for recoveries exercised by the mixed stream."""

import json
import shlex
from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

import sregym.generators.fault.inject_app as app_module
from sregym.conductor.problems.ad_service_high_cpu import AdServiceHighCpu
from sregym.conductor.problems.lifecycle import run_recovery
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector


@pytest.mark.parametrize("fault", ["resource_request", "sidecar_port_conflict", "assign_to_non_existent_node"])
def test_recovery_accepts_already_removed_deployment(fault, monkeypatch, tmp_path):
    commands = []

    def command(text):
        commands.append(text)
        if "delete deployment" in text and "--ignore-not-found" not in text:
            raise ApiException(status=404)
        return ""

    injector = object.__new__(VirtualizationFaultInjector)
    injector.namespace = "synthetic"
    injector.kubectl = SimpleNamespace(exec_command=command)
    monkeypatch.setattr(injector, "_get_deployment_yaml", lambda _name: {"spec": {"template": {"spec": {}}}})
    monkeypatch.setattr(injector, "_write_yaml_to_file", lambda *_: str(tmp_path / "synthetic.yaml"))
    monkeypatch.setattr(injector, "_backup_path", lambda *_: str(tmp_path / "synthetic.yaml"))
    monkeypatch.setattr(injector, "_wait_for_namespace_ready_best_effort", lambda: None)

    for _ in range(2):
        injector._recover(fault, ["synthetic"])
    assert any("kubectl apply" in text for text in commands)


def test_dns_recovery_accepts_already_restored_policy_and_missing_dns_config(monkeypatch):
    pod_spec = {"dnsPolicy": "ClusterFirst"}

    def command(text):
        if "kubectl patch" in text:
            argv = shlex.split(text)
            patch = json.loads(argv[argv.index("-p") + 1])
            if isinstance(patch, list):
                for operation in patch:
                    key = operation["path"].rsplit("/", 1)[1]
                    if operation["op"] == "remove" and key not in pod_spec:
                        raise ApiException(status=422)
            else:
                for key, value in patch["spec"]["template"]["spec"].items():
                    if value is None:
                        pod_spec.pop(key, None)
                    else:
                        pod_spec[key] = value
            pod_spec.setdefault("dnsPolicy", "ClusterFirst")
        return ""

    injector = object.__new__(VirtualizationFaultInjector)
    injector.namespace = "synthetic"
    injector.kubectl = SimpleNamespace(exec_command=command)
    monkeypatch.setattr(injector, "_wait_for_dns_policy_propagation", lambda *_args, **_kwargs: None)

    for _ in range(2):
        injector.recover_wrong_dns_policy(["synthetic"])
    assert pod_spec == {"dnsPolicy": "ClusterFirst"}


def test_ad_cpu_recovery_accepts_already_removed_limit():
    patches = []

    def command(text):
        argv = shlex.split(text)
        patch = json.loads(argv[argv.index("-p") + 1])
        if isinstance(patch, list):
            raise ApiException(status=422, reason="synthetic CPU limit already absent")
        patches.append(patch)
        return ""

    problem = object.__new__(AdServiceHighCpu)
    problem.namespace = "synthetic"
    problem.faulty_service = "ad"
    problem.feature_flag = "synthetic-high-cpu"
    problem.injector = SimpleNamespace(recover_fault=lambda _: None)
    problem.kubectl = SimpleNamespace(exec_command=command)

    for _ in range(2):
        assert run_recovery(problem.recover_fault, 2)[0] == "recovered"
    assert patches[-1]["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] is None


@pytest.mark.parametrize("baseline", [None, "2"])
def test_ad_cpu_recovery_preserves_the_preinjection_limit(baseline):
    resources = SimpleNamespace(limits={} if baseline is None else {"cpu": baseline})
    container = SimpleNamespace(name="ad", resources=resources)
    deployment = SimpleNamespace(
        spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(containers=[container])))
    )

    def command(text):
        argv = shlex.split(text)
        patch = json.loads(argv[argv.index("-p") + 1])
        if isinstance(patch, list):
            resources.limits.pop("cpu", None)
        else:
            value = patch["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"]
            if value is None:
                resources.limits.pop("cpu", None)
            else:
                resources.limits["cpu"] = value
        return ""

    problem = object.__new__(AdServiceHighCpu)
    problem.namespace = "synthetic"
    problem.faulty_service = "ad"
    problem.feature_flag = "synthetic-high-cpu"
    problem.cpu_limit = "100m"
    problem.injector = SimpleNamespace(inject_fault=lambda _: None, recover_fault=lambda _: None)
    problem.kubectl = SimpleNamespace(get_deployment=lambda *_: deployment, exec_command=command)

    assert run_recovery(problem.inject_fault, 2)[0] == "recovered"
    assert resources.limits["cpu"] == "100m"
    resources.limits = {} if baseline is None else {"cpu": baseline}  # responder already repaired it
    for _ in range(2):
        assert run_recovery(problem.recover_fault, 2)[0] == "recovered"
        assert resources.limits.get("cpu") == baseline


@pytest.mark.parametrize("status", [404, 403, 429, 500])
def test_valkey_job_cleanup_only_tolerates_not_found(monkeypatch, status):
    def delete(**_kwargs):
        raise ApiException(status=status)

    monkeypatch.setattr(app_module.client, "BatchV1Api", lambda: SimpleNamespace(delete_namespaced_job=delete))
    injector = object.__new__(ApplicationFaultInjector)
    injector.namespace = "synthetic"

    if status == 404:
        injector.recover_valkey_memory_disruption()
    else:
        with pytest.raises(ApiException):
            injector.recover_valkey_memory_disruption()
