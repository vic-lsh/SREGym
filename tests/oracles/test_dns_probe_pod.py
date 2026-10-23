from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.dns_resolution_mitigation import DNSResolutionMitigationOracle


def test_probe_copies_each_pods_dns_environment_and_cleans_up():
    created, deleted = [], []
    spec = SimpleNamespace(
        dns_policy="None", dns_config={"nameservers": ["8.8.8.8"]}, node_name="worker", host_network=False
    )
    source = SimpleNamespace(spec=spec)
    api = SimpleNamespace(
        create_namespaced_pod=lambda **kw: created.append(kw["body"]),
        read_namespaced_pod=lambda **kw: SimpleNamespace(status=SimpleNamespace(phase="Succeeded")),
        delete_namespaced_pod=lambda **kw: deleted.append(kw["name"]),
    )
    oracle = DNSResolutionMitigationOracle(
        SimpleNamespace(namespace="astronomy-shop", kubectl=SimpleNamespace(core_v1_api=api))
    )
    assert oracle._probe(source, ["checkout", "cart"])
    pod = created[0]
    assert pod.spec.dns_policy == "None"
    assert pod.spec.dns_config == spec.dns_config
    assert pod.spec.node_name == "worker"
    assert pod.metadata.labels == {"sregym-oracle": "dns"}
    assert "nslookup" in pod.spec.containers[0].command[-1]
    assert deleted == [pod.metadata.name]


def test_failed_probe_image_is_infrastructure_error():
    def create(**_):
        raise RuntimeError("image unavailable")

    api = SimpleNamespace(create_namespaced_pod=create, delete_namespaced_pod=lambda **_: None)
    oracle = DNSResolutionMitigationOracle(
        SimpleNamespace(namespace="astronomy-shop", kubectl=SimpleNamespace(core_v1_api=api))
    )
    source = SimpleNamespace(
        spec=SimpleNamespace(dns_policy="ClusterFirst", dns_config=None, node_name="worker", host_network=False)
    )
    with pytest.raises(RuntimeError, match="image unavailable"):
        oracle._probe(source, ["checkout"])
