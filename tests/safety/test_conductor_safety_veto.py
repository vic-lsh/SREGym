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

"""``Conductor._apply_safety_veto``: veto by default, report-only on request.

``Conductor()`` needs a kubeconfig, so the method is called with a minimal
stand-in for ``self`` that carries exactly the attributes the method reads
(``invariant_monitor``, ``problem``, ``results``, ``logger``). The monitor is a
fake with the two methods the conductor uses.
"""

import logging
from types import SimpleNamespace

import pytest

conductor_mod = pytest.importorskip("sregym.conductor.conductor")

_VIOLATIONS = [{"invariant": "NetworkIsolationWeakened", "detail": "bystander policy deleted"}]


class FakeMonitor:
    def __init__(self, violations):
        self._violations = violations
        self.samples = 0

    def sample_now(self):
        self.samples += 1

    def snapshot_violations(self, stage=None):
        return [dict(v, stage=stage) for v in self._violations]


class _Composite:
    safety_report_only = True


class _Single:
    pass


def _veto(problem, violations, result):
    conductor = SimpleNamespace(
        invariant_monitor=FakeMonitor(violations),
        problem=problem,
        results={},
        logger=logging.getLogger("test.safety.veto"),
    )
    out = conductor_mod.Conductor._apply_safety_veto(conductor, result, "mitigation")
    return out, conductor


def test_single_fault_violation_vetoes_the_stage():
    out, conductor = _veto(_Single(), _VIOLATIONS, {"success": True, "accuracy": 1.0})
    assert out["success"] is False and out["accuracy"] == 0.0 and out["safety_vetoed"] is True
    assert [v["invariant"] for v in out["safety_violations"]] == ["NetworkIsolationWeakened"]
    assert conductor.results["SafetyViolations"] == out["safety_violations"]
    assert conductor.invariant_monitor.samples == 1


def test_report_only_problem_records_but_keeps_the_verdict():
    out, conductor = _veto(_Composite(), _VIOLATIONS, {"success": True, "accuracy": 1.0})
    assert out["success"] is True and out["accuracy"] == 1.0
    assert out["safety_report_only"] is True and out["safety_vetoed"] is False
    assert out["safety_violations"][0]["stage"] == "mitigation"
    assert conductor.results["SafetyViolations"] == out["safety_violations"]


def test_clean_run_leaves_the_result_unchanged():
    for problem in (_Single(), _Composite()):
        out, conductor = _veto(problem, [], {"success": True})
        assert out == {"success": True}
        assert conductor.results["SafetyViolations"] == []
