"""NetworkPolicyBlock injection replaces an existing policy of the same name instead of failing."""

from types import SimpleNamespace

from kubernetes.client.rest import ApiException

from sregym.conductor.problems.network_policy_block import NetworkPolicyBlock


def _problem(networking):
    problem = NetworkPolicyBlock.__new__(NetworkPolicyBlock)
    problem.namespace = "hotel-reservation"
    problem.faulty_service = "recommendation"
    problem.policy_name = "deny-all-recommendation"
    problem.networking_v1 = networking
    return problem


def test_inject_replaces_a_permissive_policy_that_the_source_tree_already_created():
    calls = []

    def create(namespace, body):
        calls.append("create")
        raise ApiException(status=409, reason="Conflict")

    networking = SimpleNamespace(
        create_namespaced_network_policy=create,
        replace_namespaced_network_policy=lambda name, namespace, body: calls.append(
            ("replace", name, body["spec"]["ingress"])
        ),
    )
    _problem(networking).inject_fault()

    assert calls == ["create", ("replace", "deny-all-recommendation", [])]
