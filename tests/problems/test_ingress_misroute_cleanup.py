"""IngressMisroute's recovery removes the app-namespace objects its injection created."""

import copy
from types import SimpleNamespace

import pytest
from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.problems.ingress_misroute import IngressMisroute

NS = "hotel-reservation"


class FakeCore:
    def __init__(self, services):
        self.services = dict(services)

    def create_namespaced_service(self, namespace, body):
        assert namespace == NS
        if body.metadata.name in self.services:
            raise ApiException(status=409, reason="AlreadyExists")
        self.services[body.metadata.name] = body

    def delete_namespaced_service(self, name, namespace):
        assert namespace == NS
        if self.services.pop(name, None) is None:
            raise ApiException(status=404, reason="NotFound")


class FakeNetworking:
    def __init__(self, ingresses):
        self.ingresses = dict(ingresses)

    def read_namespaced_ingress(self, name, namespace):
        if name not in self.ingresses:
            raise ApiException(status=404, reason="NotFound")
        return copy.deepcopy(self.ingresses[name])

    def create_namespaced_ingress(self, namespace, body):
        meta = body["metadata"]
        rules = [
            client.V1IngressRule(
                http=client.V1HTTPIngressRuleValue(
                    paths=[
                        client.V1HTTPIngressPath(
                            path=p["path"],
                            path_type=p["pathType"],
                            backend=client.V1IngressBackend(
                                service=client.V1IngressServiceBackend(
                                    name=p["backend"]["service"]["name"], port=client.V1ServiceBackendPort(number=80)
                                )
                            ),
                        )
                        for p in rule["http"]["paths"]
                    ]
                )
            )
            for rule in body["spec"]["rules"]
        ]
        self.ingresses[meta["name"]] = client.V1Ingress(
            metadata=client.V1ObjectMeta(name=meta["name"], namespace=namespace),
            spec=client.V1IngressSpec(rules=rules),
        )

    def replace_namespaced_ingress(self, name, namespace, body):
        assert name in self.ingresses
        self.ingresses[name] = copy.deepcopy(body)

    def delete_namespaced_ingress(self, name, namespace):
        if self.ingresses.pop(name, None) is None:
            raise ApiException(status=404, reason="NotFound")


def problem(services=(), ingresses=None):
    p = object.__new__(IngressMisroute)
    p.namespace = NS
    p.path = "/api"
    p.correct_service = "frontend-service"
    p.wrong_service = "recommendation-service"
    p.ingress_name = "hotel-reservation-ingress"
    p.core_v1 = FakeCore({name: object() for name in services})
    p.networking_v1 = FakeNetworking(ingresses or {})
    p.ingress_controller = SimpleNamespace(deploy=lambda: None)
    p._created_services = []
    p._created_ingress = False
    return p


def backend(p):
    return p.networking_v1.ingresses[p.ingress_name].spec.rules[0].http.paths[0].backend.service.name


def test_inject_then_recover_leaves_no_created_objects_behind():
    p = problem(services=["frontend", "recommendation"])
    before = (set(p.core_v1.services), set(p.networking_v1.ingresses))

    p.inject_fault()
    assert backend(p) == "recommendation-service"
    assert {"frontend-service", "recommendation-service"} <= set(p.core_v1.services)
    p.recover_fault.__wrapped__(p)

    assert (set(p.core_v1.services), set(p.networking_v1.ingresses)) == before


def test_recover_restores_but_keeps_objects_that_existed_before_injection():
    seeded = problem()
    seeded.inject_fault()  # build a realistic Ingress and proxy Services to pre-seed with
    p = problem(
        services=["frontend", "recommendation", "frontend-service", "recommendation-service"],
        ingresses=seeded.networking_v1.ingresses,
    )
    p.recover_fault.__wrapped__(p)  # put the seed back on the correct backend
    before = set(p.core_v1.services)

    p.inject_fault()
    p.recover_fault.__wrapped__(p)

    assert set(p.core_v1.services) == before
    assert backend(p) == "frontend-service"


@pytest.mark.parametrize("gone", ["Service", "Ingress"])
def test_recover_tolerates_objects_the_responder_already_removed(gone):
    p = problem(services=["frontend", "recommendation"])
    p.inject_fault()
    if gone == "Service":
        p.core_v1.services.pop("recommendation-service")
    else:
        p.networking_v1.ingresses.clear()

    p.recover_fault.__wrapped__(p)

    assert set(p.core_v1.services) == {"frontend", "recommendation"}
    assert p.networking_v1.ingresses == {}
