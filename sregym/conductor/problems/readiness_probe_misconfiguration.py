from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

PROBE_ERROR_MODES = {
    "wrong_path": {
        "config": {
            "httpGet": {"path": "/healthz", "port": 8080},
            "initialDelaySeconds": 10,
            "periodSeconds": 10,
            "failureThreshold": 1,
        },
        "description": "a non-existent health endpoint (`/healthz` on port `8080`)",
    },
    "wrong_port": {
        "config": {
            "httpGet": {"path": "/", "port": 9999},
            "initialDelaySeconds": 10,
            "periodSeconds": 10,
            "failureThreshold": 1,
        },
        "description": "an unreachable port (`9999`)",
    },
    "wrong_protocol": {
        "config": {
            "tcpSocket": {"port": 9999},
            "initialDelaySeconds": 10,
            "periodSeconds": 10,
            "failureThreshold": 1,
        },
        "description": "a TCP socket check on an unreachable port (`9999`)",
    },
    "exec_false": {
        "config": {
            "exec": {"command": ["/bin/false"]},
            "initialDelaySeconds": 10,
            "periodSeconds": 10,
            "failureThreshold": 1,
        },
        "description": "an exec command (`/bin/false`) that always exits with a non-zero status",
    },
}


class ReadinessProbeMisconfiguration(Problem):
    def __init__(
        self, app_name="social_network", faulty_service="user-service", probe_error_mode="wrong_path", replica_count=1
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.probe_error_mode = probe_error_mode
        self.replica_count = replica_count

        mode = PROBE_ERROR_MODES[probe_error_mode]
        self.probe_config = mode["config"]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"The deployment `{self.faulty_service}` has a misconfigured readiness probe that targets "
                f"{mode['description']}, so pods fail readiness checks and remain NotReady. "
                "Kubernetes excludes these pods from service endpoints even though containers may still be running. "
                "Users see connection failures, partial outages, and persistent request timeouts to this service."
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    def _scale_if_needed(self):
        if self.replica_count > 1:
            cmd = f"kubectl scale deployment/{self.faulty_service} --replicas={self.replica_count} -n {self.namespace}"
            self.kubectl.exec_command(cmd)
            # Scoped to the scaled workload: other faults may already have broken the namespace.
            self.kubectl.wait_for_ready(self.namespace, service_names=self.faulty_service)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self._scale_if_needed()
        self.injector = VirtualizationFaultInjector(namespace=self.namespace)
        self.injector.inject_readiness_probe_misconfiguration(
            [self.faulty_service],
            probe_config=self.probe_config,
        )
        import time

        time.sleep(6)
        print(f"Service: {self.faulty_service} | Mode: {self.probe_error_mode} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = getattr(self, "injector", None) or VirtualizationFaultInjector(namespace=self.namespace)
        injector._recover(
            fault_type="readiness_probe_misconfiguration",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
