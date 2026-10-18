from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class LivenessProbeMisconfiguration(Problem):
    def __init__(
        self, app_name="social_network", faulty_service="user-service", probe_error_mode="wrong_path", replica_count=1
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.probe_error_mode = probe_error_mode
        self.replica_count = replica_count

        from sregym.conductor.problems.readiness_probe_misconfiguration import PROBE_ERROR_MODES

        mode = PROBE_ERROR_MODES[probe_error_mode]
        self.probe_config = mode["config"]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.injector = VirtualizationFaultInjector(namespace=self.app.namespace)
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"The deployment `{self.faulty_service}` has a misconfigured liveness probe that targets "
                f"{mode['description']}, causing Kubernetes to repeatedly kill and restart pods. "
                "Pods enter recurrent restart loops with unstable availability and shortened uptime between crashes. "
                "Users experience intermittent request failures and latency spikes as endpoints churn during restarts."
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    def _scale_if_needed(self):
        if self.replica_count > 1:
            cmd = f"kubectl scale deployment/{self.faulty_service} --replicas={self.replica_count} -n {self.namespace}"
            self.kubectl.exec_command(cmd)
            self.kubectl.wait_for_ready(self.namespace)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self._scale_if_needed()
        self.injector.inject_liveness_probe_misconfiguration(
            [self.faulty_service],
            probe_config=self.probe_config,
        )
        import time

        time.sleep(6)
        print(f"Service: {self.faulty_service} | Mode: {self.probe_error_mode} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self.injector._recover(
            fault_type="liveness_probe_misconfiguration",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
