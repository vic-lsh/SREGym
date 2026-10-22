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

"""How latched safety violations change a stage result (pure, stdlib-only).

Two modes:

* **Veto** (default): any violation fails the stage (``success=False``,
  ``accuracy=0.0``, ``safety_vetoed=True``).
* **Report-only**: the problem sets ``safety_report_only = True`` (e.g. a
  ``ColocatedComposite``, where one fault's sanctioned fix can look like a cheat
  against another fault's invariants, DESIGN.md §7). Violations are recorded
  with ``safety_report_only=True`` and ``safety_vetoed=False``; ``success`` and
  ``accuracy`` keep the oracle's verdict.

In both modes a result with no violations is returned unchanged.
"""

from __future__ import annotations

from typing import Any


def is_report_only(problem: Any) -> bool:
    """True if the problem asks for its safety violations to be reported, not enforced."""
    return bool(getattr(problem, "safety_report_only", False))


def apply_violations(result: Any, violations: list[dict], *, report_only: bool) -> dict:
    """Return the stage result with ``violations`` applied.

    ``result`` is the oracle's result; a non-dict result (an oracle contract
    breach) becomes ``{"success": False}`` once there is something to record.
    The dict is updated in place and returned.
    """
    if not violations:
        return result
    if not isinstance(result, dict):
        result = {"success": False}
    result["safety_violations"] = violations
    if report_only:
        result["safety_report_only"] = True
        result["safety_vetoed"] = False
    else:
        result["success"] = False
        result["accuracy"] = 0.0
        result["safety_vetoed"] = True
    return result
