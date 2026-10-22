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
"""How a mitigation oracle's ``infra_error`` (it could not judge, ``Oracle.evaluate``) reaches
the row: ``CompoundedOracle`` passes it up only when the unjudged sub-oracles alone decide the
outcome, and the conductor records it as the row's ``infra_error`` unless a safety veto
decided the row."""

import logging
import time

import pytest

from sregym.conductor.conductor import Conductor
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.compound import CompoundedOracle

UNJUDGED = {"success": False, "infra_error": "metrics_server_unavailable"}


class ResultOracle(Oracle):
    def __init__(self, problem, result):
        super().__init__(problem)
        self.result = result

    def evaluate(self):
        return dict(self.result)


class RaisingOracle(Oracle):
    def evaluate(self):
        raise RuntimeError("boom")


def _compound(*subs):
    return CompoundedOracle(None, *subs).evaluate()


def test_an_unjudged_sub_oracle_that_alone_decides_the_outcome_makes_the_result_an_infra_error():
    r = _compound(ResultOracle(None, {"success": True}), ResultOracle(None, UNJUDGED))

    assert r["success"] is False
    assert r["infra_error"] == "metrics_server_unavailable"


def test_several_unjudged_sub_oracles_name_each_distinct_reason_once():
    other = {"success": False, "infra_error": "another_addon_down"}
    r = _compound(ResultOracle(None, UNJUDGED), ResultOracle(None, other), ResultOracle(None, UNJUDGED))

    assert r["infra_error"] == "another_addon_down,metrics_server_unavailable"


@pytest.mark.parametrize(
    "judged",
    [ResultOracle(None, {"success": False}), RaisingOracle(None)],
    ids=["another-sub-oracle-failed", "another-sub-oracle-raised"],
)
def test_an_unjudged_sub_oracle_in_a_result_that_failed_anyway_is_a_grade(judged):
    r = _compound(ResultOracle(None, UNJUDGED), judged)

    assert r["success"] is False and "infra_error" not in r
    assert r["oracles"][0]["infra_error"] == "metrics_server_unavailable"  # still visible per sub-oracle


def test_a_compound_whose_sub_oracles_all_judged_carries_no_infra_error():
    r = _compound(ResultOracle(None, {"success": True}), ResultOracle(None, {"success": False}))

    assert "infra_error" not in r


class FakeMonitor:
    def __init__(self, violations):
        self.violations = violations

    def sample_now(self):
        pass

    def snapshot_violations(self, stage):
        return list(self.violations)


def _evaluate_mitigation(oracle, violations=()):
    conductor = Conductor.__new__(Conductor)  # only the state _evaluate_mitigation reads
    conductor.logger = logging.getLogger("test.mitigation_infra_error")
    conductor.problem = type("Problem", (), {"mitigation_oracle": oracle})()
    conductor.results = {}
    conductor.execution_start_time = time.time()
    conductor.invariant_monitor = FakeMonitor(violations)
    conductor._evaluate_mitigation(solution=None)
    return conductor.results


@pytest.mark.parametrize("infra_error", [None, "metrics_server_unavailable"])
def test_a_mitigation_oracle_that_could_not_judge_makes_the_row_an_infra_error(infra_error):
    result = {"success": False} | ({"infra_error": infra_error} if infra_error else {})

    results = _evaluate_mitigation(ResultOracle(None, result))

    assert results["Mitigation"]["success"] is False
    assert results.get("infra_error") == infra_error


def test_a_safety_veto_is_a_verdict_so_the_row_is_not_an_infra_error():
    results = _evaluate_mitigation(ResultOracle(None, UNJUDGED), violations=[{"invariant": "no-namespace-delete"}])

    assert results["Mitigation"]["safety_vetoed"] is True
    assert "infra_error" not in results


def test_a_compound_infra_error_reaches_the_row():
    compound = CompoundedOracle(None, ResultOracle(None, {"success": True}), ResultOracle(None, UNJUDGED))

    assert _evaluate_mitigation(compound)["infra_error"] == "metrics_server_unavailable"
