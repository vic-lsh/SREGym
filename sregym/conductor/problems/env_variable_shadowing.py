from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.workload import WorkloadOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class EnvVariableShadowing(Problem):
    def __init__(self, app_name: str = "astronomy_shop", faulty_service: str = "frontend-proxy"):
        self.app_name = app_name
        self.faulty_service = faulty_service

        app = create_app(app_name)
        super().__init__(app=app)
        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "Duplicate environment variables shadow expected configuration values (for example FRONTEND_HOST), "
                "so the service resolves dependency endpoints incorrectly and user-facing request flows fail or route "
                "to the wrong backend."
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = WorkloadOracle(problem=self, wrk_manager=self.app.wrk)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector.inject_env_variable_shadowing(microservices=[self.faulty_service])
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector.recover_env_variable_shadowing(microservices=[self.faulty_service])
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")
