"""Same-application composition of independent single-fault problems.

``MultipleIndependentFailures`` wraps sub-apps in ``CompositeApp`` (name ``CompositeApp``), which the
source-deploy gate rejects, and every sub-problem builds its own application object. ``ComposedFailures``
instead shares one application, so ``problem.app.name`` stays ``Hotel Reservation``.

* Mitigation: ``CompoundedOracle`` over the sub-problems' own mitigation oracles, so ``success`` is the
  AND of all of them and ``oracles`` lists each fault's result.
* Diagnosis: one judge call per fault against that fault's root cause, reported under ``per_fault``;
  ``success`` requires every fault, ``accuracy`` is the fraction of faults found.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.compound import CompoundedOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.utils.decorators import mark_fault_injected

MIN_FAULTS = 2
MAX_FAULTS = 3
SUPPORTED_APP_NAME = "Hotel Reservation"

JudgeFactory = Callable[[Problem, str], Oracle]


def _llm_judge(problem: Problem, expected: str) -> Oracle:
    return LLMAsAJudgeOracle(problem=problem, expected=expected)


def target_deployments(problem: Any) -> list[str]:
    """Deployments a sub-problem mutates (``faulty_services`` or ``faulty_service``)."""
    services = getattr(problem, "faulty_services", None)
    if services:
        return list(services)
    service = getattr(problem, "faulty_service", None)
    if not isinstance(service, str) or not service:
        raise ValueError(f"{type(problem).__name__} does not name a faulty deployment")
    return [service]


def _label(problem: Any) -> str:
    return ", ".join(target_deployments(problem))


class PerFaultDiagnosisOracle(Oracle):
    """Grade one diagnosis independently against each component fault."""

    importance = 1.0

    def __init__(self, problem: Problem, labels: Sequence[str], oracles: Sequence[Oracle]):
        super().__init__(problem)
        if len(labels) != len(oracles) or not oracles:
            raise ValueError("labels and oracles must be non-empty and the same length")
        self.labels = list(labels)
        self.oracles = list(oracles)

    def evaluate(self, solution: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        per_fault: list[dict[str, Any]] = []
        for label, oracle in zip(self.labels, self.oracles, strict=True):
            try:
                result = dict(oracle.evaluate(solution))
            except Exception as exc:  # a judge failure scores that fault as missed
                result = {"success": False, "error": f"{type(exc).__name__}: {exc}"}
            result["component"] = label
            result["success"] = result.get("success") is True
            per_fault.append(result)
        found = sum(1 for item in per_fault if item["success"])
        return {
            "success": found == len(per_fault),
            "accuracy": round(100.0 * found / len(per_fault), 2),
            "faults_found": found,
            "faults_total": len(per_fault),
            "per_fault": per_fault,
        }


class ComposedFailures(Problem):
    def __init__(
        self,
        problems: Sequence[Problem],
        judge_factory: JudgeFactory = _llm_judge,
        settle_seconds: float = 1.0,
    ):
        problems = list(problems)
        if not MIN_FAULTS <= len(problems) <= MAX_FAULTS:
            raise ValueError(f"ComposedFailures takes {MIN_FAULTS} to {MAX_FAULTS} faults, got {len(problems)}")
        for sub in problems:
            if sub.app.name != SUPPORTED_APP_NAME:
                raise ValueError(f"ComposedFailures supports only {SUPPORTED_APP_NAME!r}, got {sub.app.name!r}")
        seen: dict[str, str] = {}
        for sub in problems:
            for deployment in target_deployments(sub):
                if deployment in seen:
                    raise ValueError(
                        f"deployment {deployment!r} is targeted by both {seen[deployment]} and "
                        f"{type(sub).__name__}; a composite needs one fault per Deployment"
                    )
                seen[deployment] = type(sub).__name__

        self.problems = problems
        self.settle_seconds = settle_seconds
        shared_app = problems[0].app
        for sub in problems[1:]:
            sub.app = shared_app
        super().__init__(app=shared_app)

        sections = [
            f"Fault {index} ({type(sub).__name__}, component {_label(sub)}):\n{(sub.root_cause or '').strip()}"
            for index, sub in enumerate(problems, start=1)
        ]
        self.root_cause = (
            f"This scenario contains {len(problems)} simultaneous, independent faults on different "
            "components.\n\n" + "\n\n".join(sections)
        )
        self.faults_str = " | ".join(type(sub).__name__ for sub in problems)

        judges = []
        for index, sub in enumerate(problems):
            others = [
                (other.root_cause or "").strip() for other_index, other in enumerate(problems) if other_index != index
            ]
            expected = (
                f"{(sub.root_cause or '').strip()}\n\n"
                "Context: this incident has other concurrent, independent faults, listed below. A diagnosis that "
                "also names them is correct; do not penalize it for mentioning them. Grade only whether the fault "
                "above is correctly identified.\n" + "\n".join(f"- {text}" for text in others)
            )
            judges.append(judge_factory(self, expected))
        self.diagnosis_oracle = PerFaultDiagnosisOracle(self, [_label(sub) for sub in problems], judges)

        mitigation_oracles = [sub.mitigation_oracle for sub in problems if sub.mitigation_oracle is not None]
        if len(mitigation_oracles) != len(problems):
            raise ValueError("every composed fault needs a mitigation oracle")
        self.mitigation_oracle = CompoundedOracle(self, *mitigation_oracles)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection ==")
        for sub in self.problems:
            print(f"Injecting Fault: {type(sub).__name__} | Namespace: {sub.namespace}")
            sub.inject_fault()
            time.sleep(self.settle_seconds)
        print(f"Injected composite: [{self.faults_str}]\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery ==")
        for sub in reversed(self.problems):
            print(f"Recovering Fault: {type(sub).__name__} | Namespace: {sub.namespace}")
            sub.recover_fault()
            time.sleep(self.settle_seconds)
        print(f"Recovered composite: [{self.faults_str}]\n")


def _readiness(service: str) -> Callable[[], Problem]:
    def build() -> Problem:
        from sregym.conductor.problems.readiness_probe_misconfiguration import ReadinessProbeMisconfiguration

        return ReadinessProbeMisconfiguration(app_name="hotel_reservation", faulty_service=service)

    return build


def _network_policy(service: str) -> Callable[[], Problem]:
    def build() -> Problem:
        from sregym.conductor.problems.network_policy_block import NetworkPolicyBlock

        return NetworkPolicyBlock(faulty_service=service)

    return build


def _configmap(service: str) -> Callable[[], Problem]:
    def build() -> Problem:
        from sregym.conductor.problems.missing_configmap import MissingConfigMap

        return MissingConfigMap(app_name="hotel_reservation", faulty_service=service)

    return build


def _selector(service: str) -> Callable[[], Problem]:
    def build() -> Problem:
        from sregym.conductor.problems.wrong_service_selector import WrongServiceSelector

        return WrongServiceSelector(app_name="hotel_reservation", faulty_service=service)

    return build


# Fixed composites. Each pairs faults on different Deployments with no call-graph dependency between the
# targets. Ids carry no ``__v_`` so they are listed like any concrete problem.
COMPOSITE_SPECS: dict[str, tuple[Callable[[], Problem], ...]] = {
    "composite_readiness_geo__network_policy_recommendation": (_readiness("geo"), _network_policy("recommendation")),
    "composite_configmap_mongodb_rate__wrong_selector_frontend": (_configmap("mongodb-rate"), _selector("frontend")),
    "composite_network_policy_recommendation__configmap_mongodb_geo": (
        _network_policy("recommendation"),
        _configmap("mongodb-geo"),
    ),
}


def composite_factories() -> dict[str, Callable[[], Problem]]:
    return {
        problem_id: (lambda builders=builders: ComposedFailures([build() for build in builders]))
        for problem_id, builders in COMPOSITE_SPECS.items()
    }
