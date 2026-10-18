"""Redeployment of the HotelReservation application but do not handle PV."""

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mitigation import MitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_virtual import VirtualizationFaultInjector
from sregym.paths import TARGET_MICROSERVICES
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_ALL_MONGODB_SERVICES = [
    "mongodb-geo",
    "mongodb-profile",
    "mongodb-rate",
    "mongodb-recommendation",
    "mongodb-reservation",
    "mongodb-user",
]

_PVC_FAILURE_SCOPES = {
    "single": {
        "services": [_ALL_MONGODB_SERVICES[0]],
        "description": (
            "A MongoDB deployment (mongodb-geo) references a non-existent PVC claim name (`claimName-broken`), so Kubernetes "
            "cannot bind the required PersistentVolumeClaim during pod scheduling. The affected stateful pod remains Pending "
            "with a volume claim binding error instead of initializing its database. Users observe failures in "
            "data-backed features because a dependent service cannot reach its healthy MongoDB backend."
        ),
    },
    "all": {
        "services": _ALL_MONGODB_SERVICES,
        "description": (
            "Multiple MongoDB deployments reference non-existent PVC claim names (`claimName-broken`), so Kubernetes "
            "cannot bind required PersistentVolumeClaims during pod scheduling. Affected stateful pods remain Pending "
            "with volume claim binding errors instead of initializing databases. Users observe cascading failures in "
            "data-backed features because dependent services cannot reach healthy MongoDB backends."
        ),
    },
}


class PVCClaimMismatch(Problem):
    def __init__(self, pvc_failure_scope: str = "all"):
        super().__init__(app=HotelReservation())
        self.kubectl = KubeCtl()
        self.app.payload_script = (
            TARGET_MICROSERVICES
            / "DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        )
        scope_config = _PVC_FAILURE_SCOPES[pvc_failure_scope]
        self.faulty_service = scope_config["services"]
        self.injector = VirtualizationFaultInjector(namespace=self.namespace)
        self.root_cause = self.build_structured_root_cause(
            component="mongodb",
            namespace=self.namespace,
            description=scope_config["description"],
        )
        # === Attach evaluation oracles ===
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self.injector.inject_pvc_claim_mismatch(microservices=self.faulty_service)

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self.injector.recover_pvc_claim_mismatch(microservices=self.faulty_service)
