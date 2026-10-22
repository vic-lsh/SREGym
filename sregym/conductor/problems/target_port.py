"""Kubernetes Service target port misconfiguration."""

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.target_port_mitigation import TargetPortMisconfigMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class K8STargetPortMisconfig(Problem):
    def __init__(
        self,
        faulty_service="user-service",
        bad_port: int = 9999,
        correct_port: int = 9090,
        app_name: str = "social_network",
    ):
        self.app_name = app_name
        super().__init__(app=create_app(app_name))
        self.faulty_service = faulty_service
        self.bad_port = bad_port
        self.correct_port = correct_port
        self.original_port = None
        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=f"service/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                f"The Service targetPort is misconfigured to {self.bad_port} (away from its correct port), so cluster requests reach no listening "
                "process inside backend pods and callers observe connection failures and upstream timeouts."
            ),
        )

        # === Attach evaluation oracles ===
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = TargetPortMisconfigMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        service = self.kubectl.get_service_json(self.faulty_service, self.namespace)
        self.original_port = service["spec"]["ports"][0]["targetPort"]
        injector.inject_misconfig_k8s([self.faulty_service], to_port=self.bad_port, from_port=self.original_port)
        print(f"[FAULT INJECTED] {self.faulty_service} misconfigured")

    @mark_fault_injected
    def recover_fault(self):
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector.recover_misconfig_k8s([self.faulty_service], from_port=self.bad_port, to_port=self.original_port)
        print(f"[FAULT RECOVERED] {self.faulty_service}")
