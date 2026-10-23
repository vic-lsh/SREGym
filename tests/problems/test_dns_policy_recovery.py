"""Synthetic recovery after an independently successful responder deletion."""

from types import SimpleNamespace

import pytest
from kubernetes.client.rest import ApiException

from sregym.conductor.problems.colocated_composite import ColocatedComposite
from sregym.conductor.problems.service_dns_resolution_failure import ServiceDNSResolutionFailure


def recovery(status):
    calls = []

    def delete(**kwargs):
        calls.append(kwargs)
        if status is not None:
            raise ApiException(status=status, reason="synthetic API response")

    problem = object.__new__(ServiceDNSResolutionFailure)
    problem.namespace = "synthetic"
    problem.faulty_service = "checkout"
    problem.dns_failure_layer = "network_policy_block_dns"
    problem._dns_policy_name = "deny-dns-egress-checkout"
    problem._networking_v1 = SimpleNamespace(delete_namespaced_network_policy=delete)
    problem.fault_injected = True
    composite = object.__new__(ColocatedComposite)
    composite.problems = [problem]
    composite._injected = [0]
    composite._recovered = set()
    composite._recovery_timeouts = {}
    composite.recovery = {}
    composite.recover_timeout_s = 2
    composite.fault_injected = True
    return composite.recovery_result(), calls


@pytest.mark.parametrize("status", [None, 404])
def test_dns_policy_cleanup_accepts_responder_removed_policy(status):
    result, calls = recovery(status)

    assert result["success"] is True
    assert result["recovered"] == [0]
    assert result["pending"] == []
    assert calls == [{"name": "deny-dns-egress-checkout", "namespace": "synthetic"}]


@pytest.mark.parametrize("status", [403, 429, 500])
def test_dns_policy_cleanup_keeps_real_api_failures_pending(status):
    result, _ = recovery(status)

    assert result["success"] is False
    assert result["recovered"] == []
    assert result["pending"] == [0]
    assert result["errors"][0]["status"] == "error"
