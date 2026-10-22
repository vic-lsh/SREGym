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
"""Conductor.finish_after_agent_stopped: teardown never overlaps a running evaluation, and with
``grade_unsubmitted_stages`` an agent that timed out or exited is still graded.

The stage machine has no public seam short of deploying an app (``start_problem``), so
these tests attach a fake problem and walk the stages with the conductor's own
``_build_stage_sequence`` / ``_advance_to_next_stage`` / ``_submit_evaluate_and_advance``,
the calls ``start_problem`` makes; the race tests use the real ``submit``. No cluster
is contacted, and the only kubeconfig loaded is the fake one written here.
"""

import asyncio
import base64
import threading

import pytest
import yaml
from kubernetes import client

from sregym.conductor.conductor import Conductor, ConductorConfig, SubmissionWhileEvaluating

_PEM = base64.b64encode(b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n").decode()


@pytest.fixture(autouse=True)
def offline_kubeconfig(tmp_path, monkeypatch):
    """Conductor construction reads a kubeconfig but never contacts the cluster."""
    path = tmp_path / ".kube" / "config"
    path.parent.mkdir()
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "current-context": "fake",
                "clusters": [
                    {"name": "fake", "cluster": {"server": "https://127.0.0.1:1", "certificate-authority-data": _PEM}}
                ],
                "users": [{"name": "fake", "user": {"client-certificate-data": _PEM, "client-key-data": _PEM}}],
                "contexts": [{"name": "fake", "context": {"cluster": "fake", "user": "fake"}}],
            }
        )
    )
    # Pointed at, not unset: a developer's KUBECONFIG must never be the one loaded.
    monkeypatch.setenv("KUBECONFIG", str(path))
    monkeypatch.setenv("HOME", str(tmp_path))


#: Bound on waits for the other thread; a passing run never comes near it.
GUARD_S = 30


class _Oracle:
    def __init__(self, result: dict, events: list[str], name: str, gate: threading.Event | None = None):
        self.result = result
        self.calls: list[tuple] = []
        self.events = events
        self.name = name
        self.gate = gate  # when set, evaluate blocks until the gate opens
        self.started = threading.Event()

    def evaluate(self, *args):
        self.calls.append(args)
        self.events.append(f"{self.name} start")
        self.started.set()
        if self.gate is not None:
            assert self.gate.wait(GUARD_S), "the test never opened the gate"
        self.events.append(f"{self.name} end")
        return dict(self.result)


class _App:
    namespace = "test-app"

    def __init__(self):
        self.cleaned = 0

    def cleanup(self):
        self.cleaned += 1


class _Problem:
    def __init__(self, mitigated: bool, diagnosis_gate: threading.Event | None = None):
        self.events: list[str] = []
        self.diagnosis_oracle = _Oracle({"success": True}, self.events, "diagnosis", diagnosis_gate)
        self.mitigation_oracle = _Oracle({"success": mitigated}, self.events, "mitigation")
        self.app = _App()
        self.recovered = 0

    def inject_fault(self):
        pass

    def recover_fault(self):
        self.events.append("teardown")
        self.recovered += 1


def _conductor_in_diagnosis(
    mitigated: bool = True, diagnosis_gate: threading.Event | None = None, grade: bool = True
) -> tuple[Conductor, _Problem]:
    conductor = Conductor(ConductorConfig(grade_unsubmitted_stages=grade))
    problem = _Problem(mitigated, diagnosis_gate)
    conductor.problem = problem
    conductor.tasklist = ["diagnosis", "mitigation"]
    conductor._build_stage_sequence()
    conductor._advance_to_next_stage(start_index=0)
    assert conductor.submission_stage == "diagnosis" and conductor.waiting_for_agent
    return conductor, problem


def _submit(conductor: Conductor, solution: str) -> None:
    """What ``submit`` does, run inline instead of on its executor thread."""
    conductor.waiting_for_agent = False
    conductor._evaluating = True
    conductor._submit_evaluate_and_advance(solution, conductor.stage_sequence[conductor.current_stage_index])


@pytest.mark.parametrize("mitigated", [True, False])
def test_nothing_submitted_fails_diagnosis_without_a_judge_and_grades_the_cluster(mitigated: bool) -> None:
    conductor, problem = _conductor_in_diagnosis(mitigated)
    conductor.finish_after_agent_stopped("agent_timeout")
    assert problem.diagnosis_oracle.calls == []  # no judge call for an empty diagnosis
    assert conductor.results["Diagnosis"]["success"] is False
    assert conductor.results["Diagnosis"]["submission"] is None
    assert len(problem.mitigation_oracle.calls) == 1
    assert conductor.results["Mitigation"]["success"] is mitigated
    assert conductor.results["unsubmitted_reason"] == "agent_timeout"
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)  # torn down once


def test_a_submitted_diagnosis_keeps_its_grade_and_the_mitigation_is_graded() -> None:
    conductor, problem = _conductor_in_diagnosis()
    _submit(conductor, "geo is down")
    assert conductor.submission_stage == "mitigation"
    conductor.finish_after_agent_stopped("agent_exit")
    assert problem.diagnosis_oracle.calls == [("geo is down",)]
    assert conductor.results["Diagnosis"]["success"] is True
    assert len(problem.mitigation_oracle.calls) == 1
    assert conductor.results["unsubmitted_reason"] == "agent_exit"
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_a_finished_problem_is_left_alone() -> None:
    conductor, problem = _conductor_in_diagnosis()
    _submit(conductor, "geo is down")
    _submit(conductor, "")
    assert conductor.submission_stage == "done"
    conductor.finish_after_agent_stopped("agent_exit")
    assert len(problem.mitigation_oracle.calls) == 1
    assert "unsubmitted_reason" not in conductor.results
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_the_fake_kubeconfig_is_the_one_loaded() -> None:
    conductor = Conductor()
    assert conductor.kubectl.core_v1_api.api_client.configuration.host == "https://127.0.0.1:1"
    assert client.Configuration.get_default_copy().host == "https://127.0.0.1:1"


def test_a_running_evaluation_is_awaited_and_kept_before_teardown() -> None:
    """The agent submitted its diagnosis, then was killed while the judge ran."""
    gate = threading.Event()
    conductor, problem = _conductor_in_diagnosis(diagnosis_gate=gate)
    asyncio.run(conductor.submit("geo is down"))
    assert problem.diagnosis_oracle.started.wait(GUARD_S)
    grader = threading.Thread(target=conductor.finish_after_agent_stopped, args=("agent_timeout",))
    grader.start()
    grader.join(timeout=0.5)
    # Holds for a correct conductor however long the join waited (it must be blocked).
    assert grader.is_alive() and problem.recovered == 0
    gate.set()
    grader.join(GUARD_S)
    assert not grader.is_alive()
    assert problem.events == ["diagnosis start", "diagnosis end", "mitigation start", "mitigation end", "teardown"]
    assert problem.diagnosis_oracle.calls == [("geo is down",)]  # judged once, its verdict kept
    assert conductor.results["Diagnosis"]["success"] is True
    assert len(problem.mitigation_oracle.calls) == 1
    assert conductor.results["unsubmitted_reason"] == "agent_timeout"
    assert "infra_error" not in conductor.results
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_a_claimed_stage_cannot_be_submitted_again() -> None:
    """Once the grader claims the pending stage, the /submit handler's retry finds nothing to take."""
    gate = threading.Event()
    conductor, problem = _conductor_in_diagnosis(diagnosis_gate=gate)
    asyncio.run(conductor.submit("geo is down"))
    assert problem.diagnosis_oracle.started.wait(GUARD_S)
    with pytest.raises(SubmissionWhileEvaluating):  # held by the API, never recorded
        asyncio.run(conductor.submit("a second diagnosis"))
    gate.set()
    conductor.finish_after_agent_stopped("agent_exit")
    assert problem.diagnosis_oracle.calls == [("geo is down",)]
    assert len(problem.mitigation_oracle.calls) == 1
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_an_evaluation_outliving_the_wait_is_recorded_and_does_not_advance() -> None:
    gate = threading.Event()
    conductor, problem = _conductor_in_diagnosis(diagnosis_gate=gate)
    asyncio.run(conductor.submit("geo is down"))
    assert problem.diagnosis_oracle.started.wait(GUARD_S)
    conductor.finish_after_agent_stopped("agent_timeout", evaluation_wait_s=0.01)
    assert conductor.results["infra_error"] == "evaluation_wait_timeout"
    assert conductor.results["evaluation_wait_timeout_s"] == 0.01
    assert conductor.submission_stage == "done" and problem.recovered == 1
    gate.set()
    # The executor future is internal; awaiting it is the only way to know the late evaluation ended.
    conductor._submit_future.result(GUARD_S)
    assert problem.mitigation_oracle.calls == []  # the late diagnosis did not advance into mitigation
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_by_default_a_pending_stage_stays_ungraded_and_the_problem_is_torn_down_once() -> None:
    conductor, problem = _conductor_in_diagnosis(grade=False)
    conductor.finish_after_agent_stopped("agent_timeout")
    assert problem.diagnosis_oracle.calls == [] and problem.mitigation_oracle.calls == []
    assert "Diagnosis" not in conductor.results and "Mitigation" not in conductor.results
    assert "unsubmitted_reason" not in conductor.results
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)


def test_by_default_a_running_evaluation_is_still_awaited_before_teardown() -> None:
    gate = threading.Event()
    conductor, problem = _conductor_in_diagnosis(diagnosis_gate=gate, grade=False)
    asyncio.run(conductor.submit("geo is down"))
    assert problem.diagnosis_oracle.started.wait(GUARD_S)
    stopper = threading.Thread(target=conductor.finish_after_agent_stopped, args=("agent_timeout",))
    stopper.start()
    stopper.join(timeout=0.5)
    assert stopper.is_alive() and problem.recovered == 0
    gate.set()
    stopper.join(GUARD_S)
    assert not stopper.is_alive()
    # The diagnosis verdict is kept; the mitigation the agent never submitted is not graded.
    assert problem.events == ["diagnosis start", "diagnosis end", "teardown"]
    assert conductor.results["Diagnosis"]["success"] is True
    assert "Mitigation" not in conductor.results
    assert conductor.submission_stage == "done"
    assert (problem.recovered, problem.app.cleaned) == (1, 1)
