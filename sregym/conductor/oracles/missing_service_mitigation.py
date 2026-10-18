"""Mitigation grading for a deleted Kubernetes Service."""

from sregym.conductor.oracles.base import Oracle


class MissingServiceMitigationOracle(Oracle):
    importance = 1.0

    def evaluate(self) -> dict:
        kubectl = self.problem.kubectl
        namespace = self.problem.namespace
        service = self.problem.faulty_service
        names = {item.metadata.name for item in kubectl.list_services(namespace).items}
        if service not in names:
            return {"success": False}
        try:
            endpoints = kubectl.core_v1_api.read_namespaced_endpoints(service, namespace)
        except Exception:
            return {"success": False}
        return {"success": any(subset.addresses for subset in (endpoints.subsets or []))}
