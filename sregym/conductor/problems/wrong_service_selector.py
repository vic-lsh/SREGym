from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.service_endpoint_mitigation import ServiceEndpointMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class WrongServiceSelector(Problem):
    def __init__(
        self,
        app_name="social_network",
        faulty_service="user-service",
        replica_count=1,
        selector_mutation="add_extra_label",
        wrong_target_deployment=None,
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.replica_count = replica_count
        self.selector_mutation = selector_mutation
        self.wrong_target_deployment = wrong_target_deployment

        app = create_app(app_name)
        super().__init__(app=app)
        self.kubectl = KubeCtl()

        if self.selector_mutation == "swap_to_other_service":
            description = (
                f"The service `{self.faulty_service}` routes traffic to pods belonging to a different service "
                "because the Service selector matches the wrong deployment. "
                "Users observe unexpected responses or errors as requests reach pods "
                "that are not intended to handle this service's traffic."
            )
        else:
            description = (
                f"The service `{self.faulty_service}` has no matching endpoints because the Service selector "
                "includes a non-existent label. The service has zero or insufficient endpoints "
                "despite healthy-looking deployments, causing routing failures at the service layer. Users observe "
                "connection resets/timeouts and partial outages when requests are sent through this service."
            )

        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=description,
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = ServiceEndpointMitigationOracle(problem=self)

    def _scale_if_needed(self):
        if self.replica_count > 1:
            cmd = f"kubectl scale deployment/{self.faulty_service} --replicas={self.replica_count} -n {self.namespace}"
            self.kubectl.exec_command(cmd)
            self.kubectl.wait_for_ready(self.namespace)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self._scale_if_needed()
        service = self.kubectl.get_service_json(self.faulty_service, self.namespace)
        self.expected_service_port = service["spec"]["ports"][0]["port"]
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        if self.selector_mutation == "swap_to_other_service":
            if not self.wrong_target_deployment:
                raise ValueError("wrong_target_deployment is required for swap_to_other_service")
            injector._inject(
                fault_type="service_wrong_pod_selection",
                microservices=[self.faulty_service, self.wrong_target_deployment],
            )
        else:
            injector._inject(
                fault_type="wrong_service_selector",
                microservices=[self.faulty_service],
            )
        print(
            f"Service: {self.faulty_service} | Replicas: {self.replica_count} | "
            f"Mutation: {self.selector_mutation} | Namespace: {self.namespace}\n"
        )

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        if self.selector_mutation == "swap_to_other_service":
            injector._recover(
                fault_type="service_wrong_pod_selection",
                microservices=[self.faulty_service, self.wrong_target_deployment],
            )
        else:
            injector._recover(
                fault_type="wrong_service_selector",
                microservices=[self.faulty_service],
            )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
