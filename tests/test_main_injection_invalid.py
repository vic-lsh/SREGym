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
"""The driver loop records a problem whose fault could not be injected as one row (its
``infra_error`` and ``Injection.*`` fields), runs no agent and tries no further attempt."""

import csv

import main
from sregym.conductor.constants import StartProblemResult

FAILURE = {"composite_valid": False, "fault_0_live": False}


class _Registry:
    def get_problem_ids(self, all=False):
        return ["p1", "p2"]


class _Conductor:
    """What driver_loop reads from a conductor on the external-harness path."""

    def __init__(self):
        self.problems = _Registry()
        self.problem_id = None
        self.results = {}
        self.started = []

    async def start_problem(self):
        self.started.append(self.problem_id)
        self.results = {"infra_error": StartProblemResult.INJECTION_INVALID.value, "Injection": dict(FAILURE)}
        return StartProblemResult.INJECTION_INVALID


def test_an_invalid_injection_is_one_row_and_no_more_attempts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the driver writes its running CSV to the working directory
    monkeypatch.setenv("SREGYM_RESULTS_DIR", str(tmp_path / "results"))
    conductor = _Conductor()

    [by_agent] = main.driver_loop(conductor, use_external_harness=True, n_attempts=3)

    assert conductor.started == ["p1", "p2"]  # one attempt each, then the next problem
    rows = by_agent[None]
    assert [(r["problem_id"], r["attempt"]) for r in rows] == [("p1", 1), ("p2", 1)]
    assert rows[0]["infra_error"] == "injection_invalid"
    assert rows[0]["Injection.composite_valid"] is False and rows[0]["Injection.fault_0_live"] is False
    [running] = tmp_path.glob("_running_p2_*_results.csv")
    with running.open() as f:
        assert [r["Injection.composite_valid"] for r in csv.DictReader(f)] == ["False", "False"]
