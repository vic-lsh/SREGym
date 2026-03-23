from sregym.conductor.oracles.incorrect_port import IncorrectPortAssignmentMitigationOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.service.apps.astronomy_shop import AstronomyShop
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.apps.social_network import SocialNetwork
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class IncorrectPortAssignment(Problem):
    def __init__(self, app_name: str = "astronomy_shop", faulty_service: str = "checkout",
                 env_var: str = "PRODUCT_CATALOG_ADDR", incorrect_port: str = "8082",
                 correct_port: str = "8080"):
        self.app_name = app_name
        self.faulty_service = faulty_service
        self.env_var = env_var
        self.incorrect_port = incorrect_port
        self.correct_port = correct_port

        if app_name == "social_network":
            self.app = SocialNetwork()
        elif app_name == "hotel_reservation":
            self.app = HotelReservation()
        elif app_name == "astronomy_shop":
            self.app = AstronomyShop()
        else:
            raise ValueError(f"Unsupported app name: {app_name}")

        self.namespace = self.app.namespace
        super().__init__(app=self.app, namespace=self.namespace)
        self.kubectl = KubeCtl()
        self.injector = ApplicationFaultInjector(namespace=self.namespace)
        self.root_cause = f"The deployment `{self.faulty_service}` has the environment variable `{self.env_var}` configured with an incorrect port `{self.incorrect_port}` instead of `{self.correct_port}`."
        # === Attach evaluation oracles ===
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = IncorrectPortAssignmentMitigationOracle(problem=self)

        self.app.create_workload()

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self.injector.inject_incorrect_port_assignment(
            deployment_name=self.faulty_service,
            component_label=self.faulty_service,
            env_var=self.env_var,
            incorrect_port=self.incorrect_port,
        )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        self.injector.recover_incorrect_port_assignment(
            deployment_name=self.faulty_service, env_var=self.env_var, correct_port=self.correct_port
        )
