from kubernetes import client as k8s_client

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.missing_env_variable_mitigation import MissingEnvVariableMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_ENV_CORRUPTION_MODES = {
    "remove": {
        "description": "is removed from the deployment",
    },
    "empty_value": {
        "description": "is set to an empty string in the deployment, so the application resolves it as a blank hostname",
    },
}


class MissingEnvVariable(Problem):
    def __init__(
        self,
        app_name: str = "astronomy_shop",
        faulty_service: str = "frontend",
        env_var: str = "CART_ADDR",
        env_var_value: str = "cart:8080",
        env_corruption_mode: str = "remove",
    ):
        self.faulty_service = faulty_service
        self.app_name = app_name
        self.env_var = env_var
        self.env_var_value = env_var_value
        self.env_corruption_mode = env_corruption_mode

        mode_config = _ENV_CORRUPTION_MODES[env_corruption_mode]

        app = create_app(app_name)
        super().__init__(app=app)
        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                f"The required environment variable {self.env_var} {mode_config['description']}, so the application "
                f"cannot resolve the {self.env_var} dependency endpoint and service flows fail. "
                "Symptoms include runtime configuration errors and partial outages for actions depending on this address."
            ),
        )

        self.kubectl = KubeCtl()
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = MissingEnvVariableMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        injector = ApplicationFaultInjector(namespace=self.namespace)
        if self.env_corruption_mode == "empty_value":
            injector.inject_empty_env_variable(
                deployment_name=self.faulty_service,
                env_var=self.env_var,
            )
        else:
            injector.inject_missing_env_variable(
                deployment_name=self.faulty_service,
                env_var=self.env_var,
            )

        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        if self.env_corruption_mode == "empty_value":
            # Cannot use the injector's recover_missing_env_variable here
            # because it checks if the env var exists and returns early.
            # In "empty_value" mode the var exists (with value ""), so the
            # injector would skip the restore.  Patch the value directly.
            self._recover_empty_env_variable()
        else:
            injector = ApplicationFaultInjector(namespace=self.namespace)
            injector.recover_missing_env_variable(
                deployment_name=self.faulty_service,
                env_var=self.env_var,
                env_value=self.env_var_value,
            )
        print(f"Service: {self.faulty_service} | Namespace: {self.namespace}")

    def _recover_empty_env_variable(self):
        """Restore an env var that was set to empty back to its correct value."""
        deployment = self.kubectl.get_deployment(self.faulty_service, self.namespace)
        container = deployment.spec.template.spec.containers[0]
        container_name = container.name
        current_env = container.env

        updated_env = []
        for e in current_env:
            if e.name == self.env_var:
                updated_env.append(k8s_client.V1EnvVar(name=self.env_var, value=self.env_var_value))
            else:
                updated_env.append(e)

        patch_body = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": container_name,
                                "env": [{"name": var.name, "value": var.value} for var in updated_env],
                            }
                        ]
                    }
                }
            }
        }

        self.kubectl.patch_deployment(self.faulty_service, self.namespace, patch_body)
        print(
            f"Restored environment variable '{self.env_var}' from empty to '{self.env_var_value}' "
            f"in deployment '{self.faulty_service}'."
        )
