# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Multi-fault context in the diagnosis judge prompt, over a fake backend (no model calls)."""

from __future__ import annotations

import json
from types import SimpleNamespace

from sregym.conductor.oracles.llm_as_a_judge.judge import DiagnosisJudge
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle

OWN = "[fault_spec] component=rate; namespace=hotel-reservation || rate liveness probe hits a wrong path"
OTHERS = (
    "[fault_spec] component=reservation; namespace=hotel-reservation || reservation runs a wrong image",
    "[fault_spec] component=profile; namespace=hotel-reservation || profile Service selector is wrong",
)
SUBMISSION = "rate liveness probe path is wrong; reservation image is wrong; profile selector is wrong"
SECTION = "## Other Injected Faults (not under evaluation)"


class FakeBackend:
    """Records every prompt and answers Yes to all checklist questions it was asked."""

    def __init__(self):
        self.prompts: list[tuple[str, str]] = []

    def inference(self, messages):
        system, user = (m.content for m in messages)
        self.prompts.append((system, user))
        ids = [line.split('"id": "')[1].split('"')[0] for line in user.splitlines() if '"id": "' in line]
        answers = [{"id": qid, "answer": "Yes", "evidence": "e", "confidence": "High"} for qid in ids]
        return SimpleNamespace(content=json.dumps(answers))


def test_without_other_faults_the_prompt_has_no_multi_fault_context():
    judge = DiagnosisJudge(backend=FakeBackend())

    _, user = judge.prompt(SUBMISSION, OWN)

    assert SECTION not in user and "independent injected faults" not in user
    assert user == judge.prompt(SUBMISSION, OWN, other_faults=())[1]


def test_other_faults_are_listed_after_the_ground_truth_and_exempted_from_scope_precision():
    judge = DiagnosisJudge(backend=FakeBackend())

    system, user = judge.prompt(SUBMISSION, OWN, other_faults=OTHERS)
    single_system, single_user = judge.prompt(SUBMISSION, OWN)

    ground_truth = user.split("## Ground-Truth Root Cause\n")[1].split("\n\n")[0]
    context = user.split(SECTION)[1].split("## Agent Diagnosis")[0]
    assert ground_truth == OWN
    assert "3 independent injected faults" in context
    assert "D3-Q1" in context and "must NOT count against" in context
    assert "still count as over-attribution" in context
    for idx, root_cause in enumerate(OTHERS, start=1):
        assert f"- Other fault {idx}: {root_cause}" in context
    # Everything else is the single-fault prompt, byte for byte.
    assert system == single_system
    assert user.replace(SECTION + context, "") == single_user


def test_the_oracle_sends_its_other_faults_to_the_backend_in_every_voting_round():
    backend = FakeBackend()
    oracle = LLMAsAJudgeOracle(problem=None, expected=OWN, other_faults=OTHERS, backend=backend, num_rounds=3)

    result = oracle.evaluate(SUBMISSION)

    assert result["success"] is True
    assert len(backend.prompts) == 3
    for _, user in backend.prompts:
        assert all(root_cause in user for root_cause in OTHERS)


def test_the_oracle_without_other_faults_sends_the_single_fault_prompt():
    backend = FakeBackend()

    LLMAsAJudgeOracle(problem=None, expected=OWN, backend=backend, num_rounds=1).evaluate(SUBMISSION)

    [(_, user)] = backend.prompts
    assert user == DiagnosisJudge().prompt(SUBMISSION, OWN)[1]
