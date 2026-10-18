from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.service.apps.social_network import SocialNetwork
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

# NOTE: the "no_execute" (NoExecute) value was removed. This problem taints EVERY
# node (_pick_all_nodes), and a NoExecute taint evicts all non-tolerating pods on
# those nodes cluster-wide — every other workload, other namespaces, system add-ons,
# and co-tenant benchmark workers on the shared node pool — instead of localizing to
# user-service. That uncontrolled blast radius breaks worker isolation and contradicts
# the structured root_cause (component=user-service). NoSchedule only blocks *new*
# scheduling (the original, contained behavior) and is safe to apply cluster-wide.
_TAINT_EFFECTS = {
    "no_schedule": {
        "effect": "NoSchedule",
        "description": (
            "Pods remain in Pending with scheduler taint/toleration mismatch events instead of becoming Ready. "
            "Users observe sustained request failures or degraded responses because replacement capacity never starts."
        ),
    },
}


class TaintNoToleration(Problem):
    def __init__(self, taint_effect="no_schedule"):
        self.kubectl = KubeCtl()
        super().__init__(app=SocialNetwork())

        # ── pick all nodes so the control-plane cannot be used as fallback ──
        self.faulty_nodes = self._pick_all_nodes()
        self.faulty_service = "user-service"

        taint_config = _TAINT_EFFECTS[taint_effect]
        self.taint_effect_str = taint_config["effect"]

        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"Cluster nodes are tainted with `sre-fault=blocked:{self.taint_effect_str}`, but deployment `{self.faulty_service}` "
                f"only has a non-matching toleration (`dummy-key`), so new pods cannot be scheduled onto any valid node. "
                f"{taint_config['description']}"
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        # TODO: support more precise diagnosis oracle: Nodes or DeploymentConfiguration

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

        self.injector = VirtualizationFaultInjector(namespace=self.namespace)

    def _pick_all_nodes(self) -> list[str]:
        """Return the names of all nodes in the cluster."""
        nodes = self.kubectl.core_v1_api.list_node().items
        return [n.metadata.name for n in nodes]

    @mark_fault_injected
    def inject_fault(self):
        print(
            f"Injecting Fault to Service {self.faulty_service} on Nodes {self.faulty_nodes} with effect {self.taint_effect_str}"
        )
        for node in self.faulty_nodes:
            self.kubectl.exec_command(
                f"kubectl taint node {node} sre-fault=blocked:{self.taint_effect_str} --overwrite"
            )

        patch = f"""[{{"op": "add", "path": "/spec/template/spec/tolerations",
                     "value": [{{"key": "dummy-key", "operator": "Exists", "effect": "{self.taint_effect_str}"}}]}}]"""
        self.kubectl.exec_command(
            f"kubectl patch deployment {self.faulty_service} -n {self.namespace} --type='json' -p='{patch}'"
        )
        # For NoSchedule, explicitly delete pods to trigger rescheduling.
        # For NoExecute, the taint itself evicts existing pods.
        if self.taint_effect_str == "NoSchedule":
            self.kubectl.exec_command(f"kubectl delete pod -l app={self.faulty_service} -n {self.namespace}")

    @mark_fault_injected
    def recover_fault(self):
        print("Fault Recovery")
        # Step 1: Remove taints from all nodes first
        for node in self.faulty_nodes:
            self.kubectl.exec_command(f"kubectl taint node {node} sre-fault=blocked:{self.taint_effect_str}-")
            print(f"Removed taint from node {node}")

        # Step 2: Delete any Pending pods cluster-wide so system components
        # (e.g. OpenEBS) that couldn't schedule during the fault can recover
        self.kubectl.exec_command(
            "kubectl delete pods --field-selector=status.phase=Pending --all-namespaces --force --grace-period=0 --timeout=30s 2>/dev/null || true"
        )

        # Step 3: Restart the faulty service and wait for app namespace stability
        for svc in [self.faulty_service]:
            self.kubectl.exec_command(f"kubectl rollout restart deployment {svc} -n {self.namespace}")
        self.kubectl.wait_for_stable(self.namespace)
