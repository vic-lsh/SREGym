from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.sustained_readiness import SustainedReadinessOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_STARTUP_PROBE_GUARDS = {
    "none": {
        "description": "",
    },
    "short_startup": {
        "startup_probe": {
            "periodSeconds": 10,
            "failureThreshold": 3,
        },
        "description": (
            " The deployment also has a startup probe that initially masks "
            "the liveness probe, delaying the onset of crash loops by "
            "approximately 30 seconds."
        ),
    },
}


_AGGRESSION_PROFILES = {
    "aggressive": {
        "probe_params": {
            "initialDelaySeconds": 0,
            "periodSeconds": 1,
            "failureThreshold": 1,
            "timeoutSeconds": 1,
        },
        "description": (
            "kills pods immediately — the liveness probe "
            "(`initialDelaySeconds=0`, `periodSeconds=1`, `failureThreshold=1`) with "
            "`terminationGracePeriodSeconds=0` means brief startup or transient probe failures trigger immediate "
            "pod termination. Pods are recycled too quickly to stabilize, producing repeated restart cycles and "
            "readiness instability. Users observe intermittent service drops and elevated error rates during normal traffic."
        ),
    },
    "slow_churn": {
        "probe_params": {
            "initialDelaySeconds": 5,
            "periodSeconds": 10,
            "failureThreshold": 2,
            "timeoutSeconds": 1,
        },
        "description": (
            "causes intermittent restarts under normal load variance — the liveness probe "
            "(`initialDelaySeconds=5`, `periodSeconds=10`, `failureThreshold=2`) with "
            "`terminationGracePeriodSeconds=0` allows pods to start but kills them after only two "
            "consecutive failures. Under normal load variance, transient slowdowns exceed the tight "
            "timeout and accumulate failures, causing sporadic pod restarts that degrade availability."
        ),
    },
}


class LivenessProbeTooAggressive(Problem):
    def __init__(
        self,
        app_name: str = "social_network",
        faulty_service: str = "frontend",
        aggression_profile: str = "aggressive",
        startup_probe_guard: str = "none",
    ):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.aggression_profile = aggression_profile
        self.startup_probe_guard = startup_probe_guard

        profile = _AGGRESSION_PROFILES[aggression_profile]
        self.probe_params = profile["probe_params"]
        self.startup_guard = _STARTUP_PROBE_GUARDS[startup_probe_guard]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        self.injector = VirtualizationFaultInjector(namespace=self.app.namespace)
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"The deployment `{self.faulty_service}` uses an overly aggressive liveness probe that "
                + profile["description"]
                + self.startup_guard["description"]
            ),
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = SustainedReadinessOracle(problem=self, sustained_period=30)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self.injector.inject_liveness_probe_too_aggressive([self.faulty_service], probe_params=self.probe_params)
        if self.startup_probe_guard == "short_startup":
            self._inject_startup_probe()
        print(f"Service: {self.faulty_service} | Namespace: {self.app.namespace}\n")

    def _inject_startup_probe(self):
        """Add a startup probe that masks the liveness probe during initial boot."""
        import json
        import subprocess

        ns = self.app.namespace
        svc = self.faulty_service
        guard_cfg = self.startup_guard["startup_probe"]

        # Get actual container name from the deployment spec.
        # The deployment name != container name in many apps, and strategic
        # merge patches use 'name' as the merge key on the containers list.
        # KubeCtl.get_deployment returns a V1Deployment model object (not a
        # dict), so use attribute access — matching every other caller in the
        # repo (incorrect_image, missing_env_variable, inject_app, ...).
        deploy = self.kubectl.get_deployment(svc, ns)
        container_spec = deploy.spec.template.spec.containers[0]
        container_name = container_spec.name

        # Read the container port, defaulting to 8080 if none is defined.
        # V1Container.ports is None when no ports are declared.
        ports = container_spec.ports
        container_port = ports[0].container_port if ports and getattr(ports[0], "container_port", None) else 8080

        # Use a tcpSocket probe for universal compatibility (HTTP, gRPC, etc.)
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": container_name,
                                "startupProbe": {
                                    "tcpSocket": {
                                        "port": container_port,
                                    },
                                    "periodSeconds": guard_cfg["periodSeconds"],
                                    "failureThreshold": guard_cfg["failureThreshold"],
                                },
                            }
                        ],
                    },
                },
            },
        }
        patch_cmd = [
            "kubectl",
            "patch",
            "deploy",
            svc,
            "-n",
            ns,
            "--type",
            "strategic",
            "-p",
            json.dumps(patch),
        ]
        subprocess.run(patch_cmd, capture_output=True, text=True, check=True)
        print(
            f"Injected startup probe on {svc} (container={container_name}, "
            f"port={container_port}, "
            f"period={guard_cfg['periodSeconds']}s, "
            f"failureThreshold={guard_cfg['failureThreshold']})"
        )

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self.injector.recover_liveness_probe_too_aggressive([self.faulty_service])
        print(f"Service: {self.faulty_service} | Namespace: {self.app.namespace}\n")
