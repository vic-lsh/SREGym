"""tools/fault_validation/validate.py trial logic, on a fake problem and cluster."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from sregym.conductor.oracles.rollouts import Settle
from sregym.conductor.problems.composition import grading

_PATH = Path(__file__).resolve().parents[2] / "tools" / "fault_validation" / "validate.py"
_spec = importlib.util.spec_from_file_location("validate", _PATH)
validate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(validate)

_ENTRY = grading.EntryPoint("web", 80, ("/",))


class FakeCluster:
    def __init__(self, state):
        self.state = state

    def run_pod(self, manifest, timeout_s):
        line = "PROBE_OK /" if not self.state["broken"] else "PROBE_FAIL /"
        return "Succeeded", "\n".join([line] * 6 + ["PROBE_DONE"])

    def list_pods(self, namespace):
        return [{"metadata": {"name": "a"}, "status": {"phase": "Running", "containerStatuses": []}}], ""

    def wait_for_rollouts(self, namespace, timeout_s):
        return Settle(True, "", 0.0)


class FakeProblem:
    def __init__(self, state):
        self.state, self.namespace = state, "ns"
        self.binding = SimpleNamespace(entry=_ENTRY)
        self.mitigation_oracle = SimpleNamespace(evaluate=lambda: {"success": not state["broken"]})
        self.recovered = 0

    def inject_fault(self):
        self.state["broken"] = True

    def recover_fault(self):
        self.recovered += 1
        self.state["broken"] = False


def _trial(fix: bool, expect_pass: bool):
    state = {"broken": False}
    problem = FakeProblem(state)
    mitigation = (lambda: state.update(broken=False)) if fix else (lambda: None)
    row = validate.run_trial(problem, FakeCluster(state), "t", mitigation, expect_pass, persist_s=0)
    return row, problem


def test_decoy_trial_passes_when_the_verdict_fails():
    row, problem = _trial(fix=False, expect_pass=False)
    assert row["pass"] and row["symptom"]["visible"] and not row["verdict"]["success"]
    assert problem.recovered == 1  # every trial ends with the reference recovery


def test_reference_trial_passes_when_the_verdict_passes():
    assert _trial(fix=True, expect_pass=True)[0]["pass"]


def test_a_decoy_that_fixes_the_fault_fails_its_trial():
    assert not _trial(fix=True, expect_pass=False)[0]["pass"]


def test_a_crashing_mitigation_is_a_recorded_failed_trial():
    state = {"broken": False}

    def boom():
        raise RuntimeError("kubectl exploded")

    row = validate.run_trial(FakeProblem(state), FakeCluster(state), "t", boom, True, persist_s=0)
    assert not row["pass"] and "kubectl exploded" in row["error"]
