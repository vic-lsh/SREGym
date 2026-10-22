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
"""The Claude Code task wording: silent on the fault count.

The prompts must not say how many faults there are, in either direction: nothing
that implies exactly one, nor more than one (``clients/harness/task_text.py``).
"""

import re

import pytest

from clients.claudecode import driver
from clients.harness import task_text as t

APP_INFO = {"app_name": "Hotel Reservation", "namespace": "hotel-reservation", "descriptions": "D"}

#: The Claude Code prompt, pinned.
PLAIN_PROMPT = """You are an SRE agent tasked with diagnosing and fixing a Kubernetes application.

Application: Hotel Reservation
Namespace: hotel-reservation

D

CRITICAL: You are running in an AUTOMATED environment. Work autonomously and make all decisions yourself. DO NOT ask for user confirmation or approval. Proceed with the best solution based on your analysis.

GOAL: Diagnose and mitigate until the application is healthy.

WORKFLOW: You will perform TWO tasks in sequence:

TASK 1: DIAGNOSIS
- Investigate the application to detect any anomalies
- Analyze metrics, logs, and traces
- When ready, submit a natural language description of what you found
- Your diagnosis is evaluated on whether you correctly identify what is faulty and why

TASK 2: MITIGATION
- Mitigate until the application is healthy
- When you are done, submit to trigger validation
- Your mitigation is evaluated on whether the application is healthy after your changes
- Your changes are also evaluated on whether they fix what is broken rather than masking symptoms

HOW TO SUBMIT:

The submission endpoint is: http://localhost:8000/submit

For DIAGNOSIS stage:
- Submit with a natural language description of what you found
- Example: POST http://localhost:8000/submit with JSON: {"solution": "<a natural language description of what you found>"}

For MITIGATION stage:
- Submit with an EMPTY STRING after you have applied your changes
- POST http://localhost:8000/submit with JSON: {"solution": ""}

Important:
- You have access to kubectl commands to inspect and modify resources in namespace(s): hotel-reservation
- You can query metrics and traces through the available observability tools
- The conductor API is available at http://localhost:8000
"""


@pytest.fixture
def prompt(monkeypatch) -> str:
    monkeypatch.setenv("API_HOSTNAME", "localhost")
    monkeypatch.setenv("API_PORT", "8000")
    monkeypatch.delenv("SREGYM_SUMMARY_FILE", raising=False)  # no operational memory appended
    return driver.build_instruction(APP_INFO)


#: Phrases that state or imply a fault count: exactly one, or more than one.
COUNT_PHRASES = (
    "root causes",
    "faults",
    "issues",
    "one or more",
    "multiple fault",
    "several",
    "single",
    "independent fault",
    "the issue",
    "this issue",
    "the problem",
    "the fix is",
    "applied the fix",
    "fix to resolve",
    "a fix",
    "each fix",
    "every issue",
    "problems you found",
    "an incident",
    "root cause of the",
    "is crashing due to",
)


def test_the_prompt_is_pinned(prompt: str) -> None:
    assert prompt == PLAIN_PROMPT


def test_the_prompt_states_the_role_goal_and_both_tasks_in_the_shared_words(prompt: str) -> None:
    assert prompt.startswith(t.ROLE + "\n")
    assert t.GOAL in prompt
    for text in (t.INVESTIGATE, t.DIAGNOSIS_SUBMIT, t.DIAGNOSIS_EVALUATED):
        assert f"- {text}\n" in prompt
    for text in (t.MITIGATE, t.MITIGATION_SUBMIT, t.MITIGATION_EVALUATED, t.MITIGATION_ROOT_CAUSE):
        assert f"- {text}\n" in prompt


#: Every task-text string, as the prompts embed it.
TASK_TEXT = {name: value for name, value in vars(t).items() if name.isupper() and isinstance(value, str)}


@pytest.mark.parametrize("name", sorted(TASK_TEXT))
def test_no_task_text_implies_a_fault_count(name: str) -> None:
    """Stricter than the prompt check: the task text says no "each" at all."""
    lowered = TASK_TEXT[name].lower()
    assert [phrase for phrase in COUNT_PHRASES if phrase in lowered] == []
    assert re.search(r"\beach\b", lowered) is None


def test_the_prompt_does_not_imply_a_fault_count(prompt: str) -> None:
    lowered = prompt.lower()
    assert [phrase for phrase in COUNT_PHRASES if phrase in lowered] == []
