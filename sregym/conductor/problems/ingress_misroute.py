from kubernetes import client

from sregym.conductor.oracles.ingress_misroute_oracle import IngressMisrouteMitigationOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.observer.ingress_nginx import IngressNginx
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class IngressMisroute(Problem):
    def __init__(self, path="/api", correct_service="frontend-service", wrong_service="recommendation-service"):
        super().__init__(app=HotelReservation())
        self.kubectl = KubeCtl()
        self.path = path
        self.correct_service = correct_service
        self.wrong_service = wrong_service
        self.ingress_name = "hotel-reservation-ingress"
        self.root_cause = self.build_structured_root_cause(
            component=self.ingress_name,
            namespace=self.namespace,
            description=(
                f"Ingress `{self.ingress_name}` has a misconfigured backend rule for path `{self.path}`, routing "
                f"requests to `{self.wrong_service}` instead of `{self.correct_service}`. Traffic reaches a valid "
                "service, but the response semantics are incorrect for the requested endpoint, causing functional "
                "errors rather than hard connectivity failures. Users observe wrong API behavior, inconsistent results, "
                "or failures on routes that previously worked."
            ),
        )
        self.networking_v1 = client.NetworkingV1Api()
        self.core_v1 = client.CoreV1Api()
        # The controller's namespace and cluster-scoped objects are removed by the warm-lane
        # worker's created-object cleanup (``sregym/fastloop/created_objects.py``).
        self.ingress_controller = IngressNginx()
        # App-namespace objects this injection created; recovery deletes exactly these.
        self._created_services: list[str] = []
        self._created_ingress = False
        self.faulty_service = [correct_service, wrong_service]
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = IngressMisrouteMitigationOracle(problem=self)

    def _ensure_proxy_services(self):
        """Create proxy services that map the ingress backend names to the real app services."""
        service_map = {
            self.correct_service: ("frontend", 5000),
            self.wrong_service: ("recommendation", 8085),
        }
        for svc_name, (app_name, target_port) in service_map.items():
            body = client.V1Service(
                metadata=client.V1ObjectMeta(name=svc_name, namespace=self.namespace),
                spec=client.V1ServiceSpec(
                    ports=[client.V1ServicePort(port=80, target_port=target_port)],
                    selector={"io.kompose.service": app_name},
                ),
            )
            try:
                self.core_v1.create_namespaced_service(namespace=self.namespace, body=body)
            except client.exceptions.ApiException as e:
                if e.status != 409:  # already exists
                    raise
            else:
                self._created_services.append(svc_name)

    @mark_fault_injected
    def inject_fault(self):
        """Misroute /api to wrong backend"""
        self.ingress_controller.deploy()
        self._ensure_proxy_services()

        try:
            ingress = self.networking_v1.read_namespaced_ingress(name=self.ingress_name, namespace=self.namespace)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                ingress_manifest = {
                    "apiVersion": "networking.k8s.io/v1",
                    "kind": "Ingress",
                    "metadata": {
                        "name": self.ingress_name,
                        "namespace": self.namespace,
                        "annotations": {
                            "nginx.ingress.kubernetes.io/rewrite-target": "/$2",
                        },
                    },
                    "spec": {
                        "ingressClassName": "nginx",
                        "rules": [
                            {
                                "http": {
                                    "paths": [
                                        {
                                            "path": self.path + "(/|$)(.*)",
                                            "pathType": "ImplementationSpecific",
                                            "backend": {
                                                "service": {"name": self.correct_service, "port": {"number": 80}}
                                            },
                                        }
                                    ]
                                }
                            }
                        ],
                    },
                }
                self.networking_v1.create_namespaced_ingress(namespace=self.namespace, body=ingress_manifest)
                self._created_ingress = True
                ingress = self.networking_v1.read_namespaced_ingress(name=self.ingress_name, namespace=self.namespace)
            else:
                raise

        # Modify the rule for /api to wrong_service
        for rule in ingress.spec.rules:
            for path in rule.http.paths:
                if path.path.startswith(self.path):
                    path.backend.service.name = self.wrong_service
        self.networking_v1.replace_namespaced_ingress(name=self.ingress_name, namespace=self.namespace, body=ingress)

    @mark_fault_injected
    def recover_fault(self):
        """Revert misroute to correct backend, then delete the objects the injection created."""
        if self._created_ingress:
            self._delete_if_present(self.networking_v1.delete_namespaced_ingress, self.ingress_name)
            self._created_ingress = False
        else:
            ingress = self.networking_v1.read_namespaced_ingress(name=self.ingress_name, namespace=self.namespace)
            for rule in ingress.spec.rules:
                for path in rule.http.paths:
                    if path.path.startswith(self.path):
                        path.backend.service.name = self.correct_service
            self.networking_v1.replace_namespaced_ingress(
                name=self.ingress_name, namespace=self.namespace, body=ingress
            )
        while self._created_services:
            self._delete_if_present(self.core_v1.delete_namespaced_service, self._created_services[-1])
            self._created_services.pop()

    def _delete_if_present(self, delete, name: str) -> None:
        try:
            delete(name=name, namespace=self.namespace)
        except client.exceptions.ApiException as e:
            if e.status != 404:  # the responder may already have removed it
                raise
