from sregym.conductor.oracles.alert_oracle import AlertOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.service.apps.astronomy_shop import AstronomyShop
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

# NOTE: the "allkeys_lru" eviction policy was removed as a variant value. It was
# both malformed (the underscore form is rejected by `CONFIG SET maxmemory-policy`,
# which needs the hyphenated "allkeys-lru") and degenerate: valkey-cart never has
# `maxmemory` configured, so no eviction can occur and the flood simply OOMKills the
# pod — behaviorally identical to "noeviction". Re-adding it as a distinct problem
# would require bounding `maxmemory` below the cgroup limit AND a data-integrity
# oracle (AlertOracle cannot observe a truly silent eviction).
_MEMORY_EVICTION_POLICIES = {
    "noeviction": {
        "description": (
            "A workload is flooding valkey-cart with very large payloads, exhausting memory. "
            "Under the noeviction policy, Valkey rejects new write commands with OOM errors "
            "once memory is full, causing explicit failures in services that depend on the cache."
        ),
    },
}


class ValkeyMemoryDisruption(Problem):
    def __init__(self, memory_eviction_policy: str = "noeviction"):
        super().__init__(app=AstronomyShop())
        self.faulty_service = "valkey-cart"
        self.kubectl = KubeCtl()
        self.memory_eviction_policy = memory_eviction_policy

        policy_config = _MEMORY_EVICTION_POLICIES[memory_eviction_policy]
        self.root_cause = self.build_structured_root_cause(
            component=f"service/{self.faulty_service}",
            namespace=self.namespace,
            description=policy_config["description"],
        )

        # === Attach evaluation oracles ===
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = AlertOracle(problem=self)

        self.app.create_workload()

    @mark_fault_injected
    def inject_fault(self):
        # Set the maxmemory-policy on the valkey-cart instance before injecting
        # the memory flood. Skip for 'noeviction' since it is the Valkey default.
        if self.memory_eviction_policy != "noeviction":
            kubectl_cmd = (
                f"kubectl -n {self.namespace} exec deploy/valkey-cart -- "
                f"valkey-cli CONFIG SET maxmemory-policy {self.memory_eviction_policy}"
            )
            self.kubectl.exec_command(kubectl_cmd)

        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._inject(fault_type="valkey_memory_disruption")
        print("[FAULT INJECTED] valkey memory disruption")

    @mark_fault_injected
    def recover_fault(self):
        # Restore the default noeviction policy if it was changed during injection.
        if self.memory_eviction_policy != "noeviction":
            kubectl_cmd = (
                f"kubectl -n {self.namespace} exec deploy/valkey-cart -- "
                f"valkey-cli CONFIG SET maxmemory-policy noeviction"
            )
            self.kubectl.exec_command(kubectl_cmd)

        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._recover(fault_type="valkey_memory_disruption")
        print("[FAULT RECOVERED] valkey memory disruption")
