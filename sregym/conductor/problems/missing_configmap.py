from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.apps.astronomy_shop import AstronomyShop
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.apps.social_network import SocialNetwork
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class MissingConfigMap(Problem):
    """Delete the ConfigMap one or more workloads mount, then restart them so new pods cannot start.

    ``faulty_service`` names one Deployment or a list of Deployments. The injector resolves each
    Deployment's required ConfigMap (``VirtualizationFaultInjector.REQUIRED_CONFIGMAPS``).
    """

    def __init__(self, app_name: str = "social_network", faulty_service: str | list[str] = "media-mongodb"):
        services = [faulty_service] if isinstance(faulty_service, str) else list(faulty_service)
        if not services or not all(isinstance(service, str) and service for service in services):
            raise ValueError("faulty_service must name at least one deployment")
        if len(set(services)) != len(services):
            raise ValueError(f"faulty_service lists a deployment more than once: {services}")
        self.faulty_services = services
        self.faulty_service = ", ".join(services)
        self.app_name = app_name

        if self.app_name == "social_network":
            app = SocialNetwork()
        elif self.app_name == "hotel_reservation":
            app = HotelReservation()
        elif self.app_name == "astronomy_shop":
            app = AstronomyShop()
        else:
            raise ValueError(f"Unsupported app name: {app_name}")

        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=self._describe(services),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    @staticmethod
    def _describe(services: list[str]) -> str:
        if len(services) == 1:
            subject = f"A required ConfigMap for deployment `{services[0]}` has been deleted"
            component = "this component"
        else:
            names = ", ".join(f"`{service}`" for service in services[:-1]) + f" and `{services[-1]}`"
            subject = f"The required ConfigMap of each of the deployments {names} has been deleted"
            component = "these components"
        return (
            f"{subject}, so pods lose required "
            "runtime configuration during startup and reload. Affected pods fail to initialize correctly or run "
            "with invalid defaults, leading to NotReady/CrashLoop behavior and unstable service operation. "
            f"Users observe request failures and degraded functionality for features backed by {component}."
        )

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._inject(fault_type="missing_configmap", microservices=self.faulty_services)

        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._recover(fault_type="missing_configmap", microservices=self.faulty_services)
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")
