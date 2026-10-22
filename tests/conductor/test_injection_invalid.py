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
"""A fault the problem reports as not in place (``Problem.injection_failure``) runs no agent and
is graded by nothing: the row is ``infra_error`` = ``injection_invalid`` with the problem's
``Injection`` fields, and the problem is torn down at once.

As in test_finish_after_agent_stopped.py, a fake problem walks the conductor's own stage calls
(``_build_stage_sequence``, ``_begin_agent_stages``, ``inject_deferred_fault``); no cluster is
contacted.
"""

import pytest

from sregym.conductor.conductor import Conductor, ConductorConfig
from sregym.conductor.constants import AWAITING_FAULT_INJECTION, StartProblemResult

pytestmark = pytest.mark.usefixtures("offline_kubeconfig")

FAILURE = {"composite_valid": False, "fault_1_inject_error": "rollout status timed out"}


class _Oracle:
    def __init__(self):
        self.calls = 0

    def evaluate(self, *args):
        self.calls += 1
        return {"success": True}


class _App:
    namespace = "test-app"

    def __init__(self):
        self.cleaned = 0

    def cleanup(self):
        self.cleaned += 1


class _Problem:
    def __init__(self, failure):
        self.failure = failure
        self.diagnosis_oracle = _Oracle()
        self.mitigation_oracle = _Oracle()
        self.app = _App()
        self.injected = 0
        self.recovered = 0

    def inject_fault(self):
        self.injected += 1

    def recover_fault(self):
        self.recovered += 1

    def injection_failure(self):
        return self.failure


def _conductor(failure, *, defer=False):
    conductor = Conductor(ConductorConfig(defer_fault_injection=defer))
    problem = _Problem(failure)
    conductor.problem = problem
    conductor.tasklist = ["diagnosis", "mitigation"]
    conductor._build_stage_sequence()
    return conductor, problem


def _graded(results):
    return {k: v for k, v in results.items() if k != "fault_injected_at"}


def test_an_invalid_injection_opens_no_stage_and_tears_down_once():
    conductor, problem = _conductor(FAILURE)

    conductor._begin_agent_stages()

    assert conductor.injection_invalid
    assert _graded(conductor.results) == {
        "infra_error": StartProblemResult.INJECTION_INVALID.value,
        "Injection": FAILURE,
    }
    assert conductor.submission_stage == "done" and not conductor.waiting_for_agent
    assert (problem.injected, problem.recovered, problem.app.cleaned) == (1, 1, 1)
    assert problem.diagnosis_oracle.calls == problem.mitigation_oracle.calls == 0


def test_a_valid_injection_opens_the_first_stage_and_records_no_injection_fields():
    conductor, problem = _conductor(None)

    conductor._begin_agent_stages()

    assert not conductor.injection_invalid
    assert _graded(conductor.results) == {}
    assert conductor.submission_stage == "diagnosis" and conductor.waiting_for_agent
    assert (problem.injected, problem.recovered) == (1, 0)


def test_a_deferred_injection_found_invalid_ends_the_problem_instead_of_opening_a_stage():
    conductor, problem = _conductor(FAILURE, defer=True)
    conductor._begin_agent_stages()
    assert conductor.submission_stage == AWAITING_FAULT_INJECTION

    conductor.inject_deferred_fault()

    assert conductor.results["infra_error"] == StartProblemResult.INJECTION_INVALID.value
    assert conductor.results["Injection"] == FAILURE
    assert conductor.submission_stage == "done" and not conductor.waiting_for_agent
    assert (problem.injected, problem.recovered) == (1, 1)


def test_the_flag_is_cleared_for_the_next_problem():
    conductor, _ = _conductor(FAILURE)
    conductor._begin_agent_stages()

    conductor.problem = _Problem(None)
    conductor._build_stage_sequence()

    assert not conductor.injection_invalid
