from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.rolling_update_misconfiguration_mitigation import RollingUpdateMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_STRATEGY_PROFILES = {
    "drain_all": {
        "maxUnavailable": "100%",
        "maxSurge": "0%",
        "desc_prefix": "maxUnavailable=100% and maxSurge=0",
        # drain_all removes all old replicas before new ones are Ready -> outage.
        "impact": (
            "so an update drains all old replicas before new ones become Ready and the "
            "deployment remains stuck with prolonged service unavailability"
        ),
    },
    "stuck_canary": {
        # Integers (not "0"/"1" strings): a K8s IntOrString rollingUpdate field
        # only accepts a bare number as an integer; a string must be a percentage
        # (e.g. "1%"). Passing "0"/"1" as strings fails apiserver validation.
        "maxUnavailable": 0,
        "maxSurge": 1,
        "desc_prefix": "maxUnavailable=0 and maxSurge=1",
        # stuck_canary keeps all old replicas serving (maxUnavailable=0); only the
        # surged canary is stuck, so there is NO outage — just a stalled rollout.
        "impact": (
            "but because maxUnavailable=0 forbids removing any old replica, the old pods keep "
            "serving while the surged canary pod never becomes Ready; the service stays available "
            "yet the rollout stalls and the new version never finishes rolling out"
        ),
    },
}


class RollingUpdateMisconfigured(Problem):
    # Descriptions state ONLY the init-hang mechanism; the service-impact clause is
    # supplied per strategy_profile via {impact} (drain_all -> outage; stuck_canary
    # -> stalled rollout, no outage).
    INIT_HANG_MODES = {
        "sleep_infinity": {
            "command": "sleep infinity",
            "description": (
                "The rollout strategy is set to {strategy_desc} while an init container hangs "
                "indefinitely (sleep infinity), {impact}."
            ),
        },
        "crash_loop": {
            "command": "exit 1",
            "description": (
                "The rollout strategy is set to {strategy_desc} while an init container "
                "immediately crashes (exit 1), causing CrashLoopBackOff, {impact}."
            ),
        },
        "slow_timeout": {
            "command": "sleep 300",
            "description": (
                "The rollout strategy is set to {strategy_desc} while an init container "
                "hangs for 5 minutes (sleep 300) before succeeding, {impact}."
            ),
        },
    }

    def __init__(
        self,
        app_name: str = "social_network",
        faulty_service: str = "frontend",
        init_hang_mode: str = "sleep_infinity",
        strategy_profile: str = "drain_all",
    ):
        if init_hang_mode not in self.INIT_HANG_MODES:
            raise ValueError(
                f"Invalid init_hang_mode '{init_hang_mode}'. Must be one of: {', '.join(self.INIT_HANG_MODES)}"
            )
        if strategy_profile not in _STRATEGY_PROFILES:
            raise ValueError(
                f"Invalid strategy_profile '{strategy_profile}'. Must be one of: {', '.join(_STRATEGY_PROFILES)}"
            )

        self.faulty_service = faulty_service
        self.app_name = app_name
        self.init_hang_mode = init_hang_mode
        self.strategy_profile = strategy_profile
        self._strategy_config = _STRATEGY_PROFILES[strategy_profile]

        app = create_app(app_name)
        super().__init__(app=app)

        self.kubectl = KubeCtl()
        mode_info = self.INIT_HANG_MODES[self.init_hang_mode]
        description = mode_info["description"].format(
            strategy_desc=self._strategy_config["desc_prefix"],
            impact=self._strategy_config["impact"],
        )
        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=description,
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = RollingUpdateMitigationOracle(problem=self, deployment_name=self.faulty_service)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        init_command = self.INIT_HANG_MODES[self.init_hang_mode]["command"]
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._inject(
            fault_type="rolling_update_misconfigured",
            microservices=[self.faulty_service],
            init_command=init_command,
            max_unavailable=self._strategy_config["maxUnavailable"],
            max_surge=self._strategy_config["maxSurge"],
        )

        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = VirtualizationFaultInjector(namespace=self.namespace)
        injector._recover(fault_type="rolling_update_misconfigured", microservices=[self.faulty_service])
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")
