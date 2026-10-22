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
"""Task wording for agent prompts that must not reveal the fault count.

The prompts must not tell the agent how many faults there are, in either direction:
no phrasing that implies exactly one ("the issue", "the fix", a single-cause example)
nor more than one ("issues", "root causes", "each"), since a generated sequence mixes
single- and multi-fault problems. The Claude Code prompt states the task with these
strings; tests/clients/test_task_text.py checks it contains them and none of the
count-implying phrases.
"""

ROLE = "You are an SRE agent tasked with diagnosing and fixing a Kubernetes application."
GOAL = "GOAL: Diagnose and mitigate until the application is healthy."

#: What the diagnosis submission is.
INVESTIGATE = "Investigate the application to detect any anomalies"
DIAGNOSIS_SUBMISSION = "a natural language description of what you found"
DIAGNOSIS_SUBMIT = f"When ready, submit {DIAGNOSIS_SUBMISSION}"
DIAGNOSIS_EVALUATED = "Your diagnosis is evaluated on whether you correctly identify what is faulty and why"
DIAGNOSIS_EXAMPLE = f"<{DIAGNOSIS_SUBMISSION}>"

MITIGATE = "Mitigate until the application is healthy"
MITIGATION_SUBMIT = "When you are done, submit to trigger validation"
MITIGATION_EVALUATED = "Your mitigation is evaluated on whether the application is healthy after your changes"
MITIGATION_ROOT_CAUSE = (
    "Your changes are also evaluated on whether they fix what is broken rather than masking symptoms"
)
