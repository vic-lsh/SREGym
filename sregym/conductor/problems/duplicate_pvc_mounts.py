from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class DuplicatePVCMounts(Problem):
    def __init__(self, app_name: str = "hotel_reservation", faulty_service: str = "mongodb-rate"):
        self.app_name = app_name
        self.faulty_service = faulty_service

        app = create_app(app_name)
        super().__init__(app=app)
        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "The deployment is scaled to multiple replicas while all replicas reference one ReadWriteOnce PVC, "
                "which triggers multi-attach mount conflicts and leaves replacement pods in Pending or ContainerCreating "
                "instead of reaching Ready."
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = MitigationOracle(problem=self)

        self.app.create_workload()

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._inject(
            fault_type="duplicate_pvc_mounts",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._recover(
            fault_type="duplicate_pvc_mounts",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
