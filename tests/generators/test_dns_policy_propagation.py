from types import SimpleNamespace

import pytest

import sregym.generators.fault.inject_virtual as inject_virtual_module
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector

CLUSTER_FIRST = "search astronomy-shop.svc.cluster.local svc.cluster.local cluster.local\nnameserver 10.96.0.10\n"
NODE_DEFAULT = "nameserver 192.168.1.1\n"
EXTERNAL = "nameserver 8.8.8.8\n"


def _dns_injector(resolv_conf):
    deployment = SimpleNamespace(spec=SimpleNamespace(selector=SimpleNamespace(match_labels={"app": "frontend"})))
    # Under ClusterFirst/Default the pod spec carries no dnsConfig; only resolv.conf shows the policy.
    pod = SimpleNamespace(metadata=SimpleNamespace(name="frontend-abc123"), spec=SimpleNamespace(dns_config=None))
    commands = []

    def exec_command(command):
        commands.append(command)
        if isinstance(resolv_conf, Exception):
            raise resolv_conf
        return resolv_conf

    kubectl = SimpleNamespace(
        apps_v1_api=SimpleNamespace(read_namespaced_deployment=lambda name, namespace: deployment),
        core_v1_api=SimpleNamespace(list_namespaced_pod=lambda namespace, label_selector: SimpleNamespace(items=[pod])),
        exec_command=exec_command,
    )
    injector = object.__new__(VirtualizationFaultInjector)
    injector.namespace = "astronomy-shop"
    injector.kubectl = kubectl
    return injector, commands


@pytest.fixture
def sleeps(monkeypatch):
    slept = []
    monkeypatch.setattr(inject_virtual_module.time, "sleep", slept.append)
    return slept


@pytest.mark.parametrize(
    ("resolv_conf", "external_ns", "expect_external"),
    [
        (EXTERNAL, "8.8.8.8", True),  # external_resolver injected
        (CLUSTER_FIRST, "8.8.8.8", False),  # external_resolver recovered
        (NODE_DEFAULT, "cluster.local", False),  # default_policy injected
        (CLUSTER_FIRST, "cluster.local", True),  # default_policy recovered
    ],
)
def test_propagation_returns_at_once_when_resolv_conf_shows_the_expected_state(
    sleeps, resolv_conf, external_ns, expect_external
):
    injector, commands = _dns_injector(resolv_conf)

    injector._wait_for_dns_policy_propagation("frontend", external_ns=external_ns, expect_external=expect_external)

    assert sleeps == []
    assert commands == ["kubectl exec frontend-abc123 -n astronomy-shop -- cat /etc/resolv.conf"]


@pytest.mark.parametrize("resolv_conf", [NODE_DEFAULT, RuntimeError("container not running")])
def test_propagation_keeps_waiting_until_max_wait_otherwise(sleeps, resolv_conf):
    injector, _ = _dns_injector(resolv_conf)

    injector._wait_for_dns_policy_propagation(
        "frontend", external_ns="cluster.local", expect_external=True, sleep=2, max_wait=6
    )

    assert sleeps == [2, 2, 2]
