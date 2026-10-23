"""Cold source-revision pilot problems; hidden expectations stay harness-side."""

from __future__ import annotations

import copy
import time
from typing import Any

from kubernetes.client import ApiClient

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.target_port import K8STargetPortMisconfig
from sregym.conductor.problems.variant_utils import create_app
from sregym.service.kubectl import KubeCtl
from sregym.service.named_init_image import init_image_patch, init_image_recovered
from sregym.utils.decorators import mark_fault_injected


def deployment_json(kubectl: KubeCtl, name: str, namespace: str) -> dict[str, Any]:
    return ApiClient().sanitize_for_serialization(kubectl.get_deployment(name, namespace))


class NamedInitMitigationOracle(Oracle):
    importance = 1.0

    def evaluate(self) -> dict[str, bool]:
        problem = self.problem
        self.settle_rollouts(only=problem.faulty_service)
        current = deployment_json(problem.kubectl, problem.faulty_service, problem.namespace)
        spec = current["spec"]["template"]["spec"]
        success = (
            init_image_recovered(current, problem.init_name, problem.original_image)
            and spec.get("initContainers") == problem.original_spec.get("initContainers")
            and spec.get("containers") == problem.original_spec.get("containers")
        )
        return {"success": success}


class NamedInitImage(Problem):
    def __init__(
        self, app_name: str = "hotel_reservation", faulty_service: str = "search", init_name: str = "wait-for-mongodb"
    ) -> None:
        super().__init__(app=create_app(app_name))
        self.faulty_service = faulty_service
        self.init_name = init_name
        self.kubectl = KubeCtl()
        self.bad_image = "registry.invalid/sp1-wait-for-mongodb:missing"
        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{faulty_service}",
            namespace=self.namespace,
            description=f"Init container {init_name} has an unavailable image; initialization and application startup fail.",
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.app.create_workload()
        self.mitigation_oracle = NamedInitMitigationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self) -> None:
        time.sleep(120)
        current = deployment_json(self.kubectl, self.faulty_service, self.namespace)
        patch = init_image_patch(current, self.init_name, self.bad_image)
        self.original_spec = copy.deepcopy(current["spec"]["template"]["spec"])
        self.original_image = next(
            c["image"] for c in self.original_spec["initContainers"] if c["name"] == self.init_name
        )
        self.kubectl.patch_deployment(self.faulty_service, self.namespace, patch)

    @mark_fault_injected
    def recover_fault(self) -> None:
        current = deployment_json(self.kubectl, self.faulty_service, self.namespace)
        self.kubectl.patch_deployment(
            self.faulty_service, self.namespace, init_image_patch(current, self.init_name, self.original_image)
        )


class HealthyObservationOracle(Oracle):
    importance = 1.0

    def evaluate(self) -> dict[str, bool]:
        deployments = self.problem.kubectl.apps_v1_api.list_namespaced_deployment(self.problem.namespace).items
        names = {d.metadata.name for d in deployments}
        return {
            "success": bool(
                "otel-collector" in names
                and "jaeger" not in names
                and deployments
                and all((d.status.available_replicas or 0) >= (d.spec.replicas or 1) for d in deployments)
            )
        }


class TracingHealthyObservation(Problem):
    """A healthy, no-fault negative. Alarm attribution is read from SDO evidence."""

    def __init__(self) -> None:
        super().__init__(app=create_app("hotel_reservation"))
        self.kubectl = KubeCtl()
        self.root_cause = "No fault was injected; the accepted source replaces Jaeger with an OpenTelemetry collector."
        self.app.create_workload()
        self.diagnosis_oracle = HealthyObservationOracle(problem=self)
        self.mitigation_oracle = HealthyObservationOracle(problem=self)

    @mark_fault_injected
    def inject_fault(self) -> None:
        # Benchmark task start only: this method performs no cluster mutation.
        time.sleep(120)

    @mark_fault_injected
    def recover_fault(self) -> None:
        pass


class ObservedTargetPortMisconfig(K8STargetPortMisconfig):
    """Allow a healthy source window before the covered targetPort fault."""

    @mark_fault_injected
    def inject_fault(self) -> None:
        time.sleep(120)
        return super().inject_fault()


def sidecar_target_port() -> ObservedTargetPortMisconfig:
    return ObservedTargetPortMisconfig(app_name="hotel_reservation", faulty_service="reservation", correct_port=8083)


def named_target_port() -> ObservedTargetPortMisconfig:
    return ObservedTargetPortMisconfig(app_name="hotel_reservation", faulty_service="geo", bad_port="missing-app")
