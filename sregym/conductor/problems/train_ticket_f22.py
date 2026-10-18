import logging

from sregym.conductor.oracles.alert_oracle import AlertOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_tt import TrainTicketFaultInjector
from sregym.service.apps.train_ticket import TrainTicket
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

logger = logging.getLogger(__name__)

_DECOY_FLAG_COUNTS = {
    "none": 0,
    "low": 3,
    "high": 10,
}


class TrainTicketF22(Problem):
    def __init__(self, decoy_flag_count="high"):
        self.app_name = "train-ticket"
        self.faulty_service = "ts-contacts-service"
        self.fault_name = "fault-22-sql-column-name-mismatch-error"
        self._decoy_flag_count = decoy_flag_count
        self._decoy_count = _DECOY_FLAG_COUNTS[decoy_flag_count]
        super().__init__(app=TrainTicket())

        decoy_context = {
            "none": "",
            "low": " A small number of unrelated feature flags are also enabled, adding mild noise to the investigation.",
            "high": " Many unrelated feature flags are also enabled, adding significant noise to the investigation.",
        }[decoy_flag_count]

        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "SQL statements in the contacts service reference an incorrect column name, so database queries "
                "fail at execution time and related API operations return errors."
                f"{decoy_context}"
            ),
        )
        self.kubectl = KubeCtl()
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = AlertOracle(problem=self)

        self.app.create_workload()

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self.injector = TrainTicketFaultInjector(namespace=self.namespace)
        self.injector._inject(
            fault_type="tt-feat-22",
            decoy_count=self._decoy_count,
        )
        print(
            f"Injected fault-22-sql-column-name-mismatch-error (decoys={self._decoy_flag_count}) | Namespace: {self.namespace}\n"
        )

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self.injector = TrainTicketFaultInjector(namespace=self.namespace)
        self.injector._recover(
            fault_type="tt-feat-22",
        )
        print(f"Recovered from fault-22-sql-column-name-mismatch-error | Namespace: {self.namespace}\n")
