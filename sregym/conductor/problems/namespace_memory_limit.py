from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.namespace_memory_limit_mitigation import NamespaceMemoryLimitMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class NamespaceMemoryLimit(Problem):
    def __init__(self):
        super().__init__(app=HotelReservation())
        self.kubectl = KubeCtl()
        self.faulty_service = "search"
        self.root_cause = self.build_structured_root_cause(
            component=self.faulty_service,
            namespace=self.namespace,
            description=(
                f"Namespace-level ResourceQuota memory limit (`1Gi`) is set too low for deployment `{self.faulty_service}`, "
                "so new pods cannot be admitted or scheduled when memory demand rises. Existing pods may also be evicted "
                "as aggregate namespace usage breaches quota constraints. Users observe unstable availability, failed scaling, "
                "and intermittent request failures during routine workload pressure."
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = NamespaceMemoryLimitMitigationOracle(problem=self)

        self.app.create_workload()

    # The injection and recovery are robust-skills' VirtualizationFaultInjector
    # inject/recover_namespace_memory_limit. They live here because this tree's copies of
    # those methods sit under inject_virtual.py's `if __name__ == "__main__":` block, so the
    # injector class does not have them.
    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        # Create the memory ResourceQuota first so replacement pods are rejected.
        self.kubectl.apply_resource(
            {
                "apiVersion": "v1",
                "kind": "ResourceQuota",
                "metadata": {"name": "memory-limit-quota", "namespace": self.namespace},
                "spec": {"hard": {"memory": "1Gi"}},
            }
        )
        # Then delete the current ReplicaSet (listed first) to force pod recreation.
        replica_sets = self.kubectl.get_matching_replicasets(self.namespace, self.faulty_service)
        if not replica_sets:
            raise RuntimeError(f"No ReplicaSet found for deployment {self.faulty_service} in {self.namespace}")
        self.kubectl.delete_replicaset(name=replica_sets[0].metadata.name, namespace=self.namespace)
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        # Remove every memory-based quota, then scale the deployment back to one replica.
        for quota in self.kubectl.get_resource_quotas(self.namespace):
            if "memory" in quota.spec.hard:
                self.kubectl.delete_resource_quota(name=quota.metadata.name, namespace=self.namespace)
        self.kubectl.scale_deployment(name=self.faulty_service, namespace=self.namespace, replicas=1)
