"""MongoDB storage user unregistered problem in the HotelReservation application."""

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.mongo_auth_mitigation import MongoAuthMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.paths import TARGET_MICROSERVICES
from sregym.service.apps.hotel_reservation import FailureScripts, HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class MongoDBUserUnregistered(Problem):
    """Remove admin user from MongoDB — the agent must diagnose auth failure without seeing the scripts."""

    def __init__(self, app_name: str = "hotel_reservation", faulty_service: str = "mongodb-geo"):
        app = create_app(app_name)
        if isinstance(app, HotelReservation):
            # The agent must diagnose from symptoms: don't deploy the fault scripts.
            app.failure_scripts = FailureScripts.ABSENT
        super().__init__(app=app)
        self.app_name = app_name
        self.kubectl = KubeCtl()
        self.faulty_service = faulty_service
        # NOTE: change the faulty service to mongodb-rate to create another scenario
        # self.faulty_service = "mongodb-rate"
        self.root_cause = self.build_structured_root_cause(
            component=f"service/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "The expected database user credentials are missing from MongoDB, so the dependent microservice cannot "
                "authenticate and all storage-backed operations fail with authorization errors."
            ),
        )
        self.app.payload_script = (
            TARGET_MICROSERVICES / "hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua"
        )
        # === Attach evaluation oracles ===
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        # Data-plane oracle: the auth fault leaves pods Ready, so the readiness-based
        # MitigationOracle cannot see it; probe the actual mongo auth.
        self.mitigation_oracle = MongoAuthMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._inject(
            fault_type="storage_user_unregistered",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        injector = ApplicationFaultInjector(namespace=self.namespace)
        injector._recover(
            fault_type="storage_user_unregistered",
            microservices=[self.faulty_service],
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")
