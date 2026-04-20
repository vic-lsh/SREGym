"""Multiple faults injected into the same app/namespace simultaneously."""

import time

from sregym.conductor.oracles.compound import CompoundedOracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.utils.decorators import mark_fault_injected


class SameAppMultiFault(Problem):
    def __init__(self, problems: list[Problem]):
        if not problems:
            raise ValueError("problems list must not be empty")
        namespaces = {p.namespace for p in problems}
        if len(namespaces) > 1:
            raise ValueError(f"All problems must share the same namespace, got: {namespaces}")

        super().__init__(problems[0].app, problems[0].namespace)
        self.problems = problems
        self.root_cause = " + ".join(p.root_cause for p in problems if p.root_cause)

        # Use LLM-as-judge for diagnosis so that a free-text answer covering all
        # root causes grades successfully, regardless of exact wording or submission count.
        if self.root_cause:
            self.diagnosis_oracle = LLMAsAJudgeOracle(self, expected=self.root_cause)

        mitigation_oracles = [p.mitigation_oracle for p in problems if p.mitigation_oracle]
        if mitigation_oracles:
            self.mitigation_oracle = CompoundedOracle(self, *mitigation_oracles)

    @mark_fault_injected
    def inject_fault(self):
        for p in self.problems:
            print(f"Injecting fault: {p.__class__.__name__} | Namespace: {p.namespace}")
            p.inject_fault()
            p.verify_fault_applied()
            time.sleep(1)

    @mark_fault_injected
    def recover_fault(self):
        for p in reversed(self.problems):
            print(f"Recovering fault: {p.__class__.__name__} | Namespace: {p.namespace}")
            p.recover_fault()
            time.sleep(1)
