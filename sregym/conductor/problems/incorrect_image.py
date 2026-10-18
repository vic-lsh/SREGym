from sregym.conductor.oracles.incorrect_image_mitigation import IncorrectImageMitigationOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.inject_app import ApplicationFaultInjector
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

_BAD_IMAGE_PROFILES = {
    "nonexistent_tag": {
        "bad_image": "app-image:latest",
        "description": "a non-existent image tag",
        "symptoms": (
            "so pods fail with image pull errors and the service becomes unavailable. "
            "Symptoms typically include ImagePullBackOff events and upstream request "
            "calls timing out or failing."
        ),
    },
    "nonexistent_registry": {
        "bad_image": "registry.invalid/app-image:v1",
        "description": "an image from an unreachable registry",
        "symptoms": (
            "so pods fail with image pull errors and the service becomes unavailable. "
            "Symptoms typically include ImagePullBackOff events and upstream request "
            "calls timing out or failing."
        ),
    },
    "wrong_base_image": {
        "bad_image": "busybox:latest",
        "description": "a valid but incorrect base image that lacks the application binary",
        "symptoms": (
            "so pods start but immediately crash because the container lacks the "
            "application binary. Symptoms typically include CrashLoopBackOff status "
            "and container logs showing missing executable or entrypoint errors, "
            "while upstream requests fail or time out."
        ),
    },
}


class IncorrectImage(Problem):
    def __init__(
        self,
        app_name: str = "astronomy_shop",
        faulty_service: str = "product-catalog",
        bad_image_kind: str = "nonexistent_tag",
    ):
        self.app_name = app_name
        app = create_app(app_name)
        super().__init__(app=app)
        self.kubectl = KubeCtl()
        self.faulty_service = [faulty_service] if isinstance(faulty_service, str) else faulty_service
        target_svc = self.faulty_service[0] if isinstance(self.faulty_service, list) else self.faulty_service
        self.injector = ApplicationFaultInjector(namespace=self.namespace)

        profile = _BAD_IMAGE_PROFILES[bad_image_kind]
        self.bad_image = profile["bad_image"]

        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{target_svc}",
            namespace=self.namespace,
            description=(
                f"The {target_svc} deployment is configured to pull "
                f"{profile['description']} ({self.bad_image}), "
                f"{profile['symptoms']}"
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)

        self.app.create_workload()
        self.mitigation_oracle = IncorrectImageMitigationOracle(
            problem=self, actual_images={target_svc: self.bad_image}
        )

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        self.original_images = {}
        for service in self.faulty_service:
            try:
                deployment = self.kubectl.get_deployment(service, self.namespace)
                if deployment and deployment.spec and deployment.spec.template.spec.containers:
                    self.original_images[service] = deployment.spec.template.spec.containers[0].image
            except Exception as e:
                print(f"[Warning] Failed to fetch original image for {service}: {e}")

            self.injector.inject_incorrect_image(
                deployment_name=service, namespace=self.namespace, bad_image=self.bad_image
            )
            print(f"Service: {service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        for service in self.faulty_service:
            correct_image = getattr(self, "original_images", {}).get(service)
            if not correct_image:
                print(f"[Warning] Original image for {service} unknown, skipping hardcoded fallback")
                continue
            self.injector.recover_incorrect_image(
                deployment_name=service,
                namespace=self.namespace,
                correct_image=correct_image,
            )
