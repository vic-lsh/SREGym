"""tools/fault_validation/validate.py trial logic, on a fake problem and cluster."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sregym.conductor.oracles.rollouts import Settle
from sregym.conductor.problems.component_fault import Expect, Repair
from sregym.conductor.problems.composition import grading

_PATH = Path(__file__).resolve().parents[2] / "tools" / "fault_validation" / "validate.py"
_spec = importlib.util.spec_from_file_location("validate", _PATH)
validate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(validate)

_ENTRY = grading.EntryPoint("web", 80, ("/",))


class FakeCluster:
    def __init__(self, state, probe_phase="Succeeded"):
        self.state, self.probe_phase = state, probe_phase

    def run_pod(self, manifest, timeout_s):
        line = "PROBE_OK /" if not self.state["broken"] else "PROBE_FAIL /"
        return self.probe_phase, "\n".join([line] * 6 + ["PROBE_DONE"])

    def list_pods(self, namespace):
        return [{"metadata": {"name": "a"}, "status": {"phase": "Running", "containerStatuses": []}}], ""

    def wait_for_rollouts(self, namespace, timeout_s):
        return Settle(True, "", 0.0)


class FakeProblem:
    """``state``: broken (fault live), lost (data gone), error (oracle raises -> inconclusive)."""

    def __init__(self, state):
        self.state, self.namespace = state, "ns"
        self.binding = SimpleNamespace(entry=_ENTRY)
        self.mitigation_oracle = SimpleNamespace(evaluate=self._evaluate)
        self.recovered = 0
        self.poisoned = {}

    def _evaluate(self):
        if self.state.get("error"):
            return {"success": False, "inconclusive": True, "reason": "inconclusive", "error": "exec flake"}
        failed = (["fault"] if self.state["broken"] else []) + (["witness"] if self.state.get("lost") else [])
        reason = "fault_not_cleared" if "fault" in failed else "data_lost" if failed else "passes"
        return {"success": not failed, "failed": failed, "reason": reason}

    def inject_fault(self):
        self.state["broken"] = True
        self.poisoned = {"k": b"\xff"}

    def reference_mitigation(self):
        self.state["broken"] = False

    def recover_fault(self):
        self.recovered += 1
        self.state["broken"] = False

    def apply(self, repair):
        repair.run()

    def injected_state(self):
        return {"poisoned": {"k": "x"}}


def _trial(run, expect: Expect, *, name="t", state=None, phase="Succeeded", state_dir=None):
    state = state if state is not None else {"broken": False}
    problem = FakeProblem(state)
    h = validate.Harness(FakeCluster(state, phase), persist_s=0, probe_rounds=1, state_dir=state_dir, reset_cmd=None)
    row = h.run_trial(problem, name, Repair(lambda: run(state), expect), key="k", app="a", seed=1)
    return row, problem


def _noop(state):
    pass


def _fix(state):
    state["broken"] = False


def _wipe(state):
    state.update(broken=False, lost=True)


def test_a_decoy_that_leaves_the_fault_is_valid():
    row, problem = _trial(_noop, Expect.FAULT_NOT_CLEARED)
    assert row["status"] == "VALID" and row["pass"] and row["observed"] == "fault_not_cleared"
    assert problem.recovered == 1  # every trial ends with the reference recovery


def test_the_reference_is_valid_when_it_passes():
    assert _trial(_fix, Expect.PASSES, name="reference")[0]["status"] == "VALID"


def test_a_decoy_that_fails_for_the_wrong_reason_is_a_failed_trial():
    """Declared data_lost (a restart of the only copy), but the verdict says the fault is still there."""
    row, _ = _trial(_noop, Expect.DATA_LOST)
    assert row["status"] == "FAIL" and "expected data_lost" in row["why"]


def test_a_data_destroying_decoy_is_valid_only_through_data_lost():
    assert _trial(_wipe, Expect.DATA_LOST)[0]["status"] == "VALID"
    assert _trial(_wipe, Expect.FAULT_NOT_CLEARED)[0]["status"] == "FAIL"


def test_a_decoy_that_fixes_the_fault_fails_its_trial():
    assert _trial(_fix, Expect.FAULT_NOT_CLEARED)[0]["status"] == "FAIL"


def test_an_oracle_error_is_invalid_never_a_correctly_failing_decoy():
    def flake(state):
        state["error"] = True

    row, _ = _trial(flake, Expect.FAULT_NOT_CLEARED)
    assert row["status"] == "INVALID" and not row["pass"]


def test_a_probe_pod_that_did_not_run_is_invalid():
    assert _trial(_noop, Expect.FAULT_NOT_CLEARED, phase="Error: Pending")[0]["status"] == "INVALID"


def test_a_crashing_repair_is_an_invalid_recorded_trial():
    def boom(state):
        raise RuntimeError("kubectl exploded")

    row, _ = _trial(boom, Expect.PASSES)
    assert row["status"] == "INVALID" and "kubectl exploded" in row["why"]


def test_injected_state_is_on_disk_while_live_and_gone_after_recovery(tmp_path):
    seen = {}

    def look(state):
        seen.update(json.loads((tmp_path / "k__s1.json").read_text()))

    row, _ = _trial(look, Expect.FAULT_NOT_CLEARED, state_dir=tmp_path)
    assert seen["state"] == {"poisoned": {"k": "x"}} and seen["key"] == "k" and seen["seed"] == 1
    assert not list(tmp_path.iterdir())


def test_destructive_repairs_run_last_and_are_followed_by_a_reset(monkeypatch):
    state = {"broken": False}
    problem = FakeProblem(state)
    order = []
    problem.repairs = lambda: {
        "a_restart": Repair(lambda: order.append("a_restart") or _wipe(state), Expect.DATA_LOST, destroys_data=True),
        "z_restore": Repair(lambda: order.append("z_restore"), Expect.FAULT_NOT_CLEARED),
    }
    problem.reference_mitigation = lambda: order.append("reference") or _fix(state)
    monkeypatch.setattr(validate, "_build", lambda *a: problem)
    monkeypatch.setattr(validate.catalog, "entries_for", lambda app: [_FakeEntry()])
    h = validate.Harness(FakeCluster(state), persist_s=0, probe_rounds=1, state_dir=None, reset_cmd="true")
    rows = list(h.validate_seed("k", "a", 1, "ns"))
    assert order == ["reference", "z_restore", "a_restart"]
    trials = [r["trial"] for r in rows]
    assert trials[trials.index("decoy:a_restart") + 1] == "reset"
    assert rows[-1]["trial"] == "seed_summary" and "wall_s" in rows[-1]


class _FakeEntry:
    key = "k"

    def sample(self, app, rng):
        return SimpleNamespace(param_dict=lambda: {})


@pytest.mark.parametrize(
    "argv,flags,want",
    [
        (["--key", "k", "--lanes", "a", "b", "--seeds", "1", "2"], ("--lanes", "--seeds"), ["--key", "k"]),
        (["--seeds", "1", "--lanes", "a", "--out", "o"], ("--lanes", "--seeds"), ["--out", "o"]),
    ],
)
def test_lane_sharding_strips_the_parent_flags(argv, flags, want):
    assert validate._strip(argv, flags) == want


def test_status_reports_rows_and_liveness(tmp_path, capsys):
    out = tmp_path / "rows.jsonl"
    out.write_text(json.dumps({"seed": 1, "trial": "reference", "status": "VALID", "pass": True}) + "\n")
    (tmp_path / "rows.jsonl.pid").write_text("999999999\n")  # no such process
    assert validate.status(str(out)) == 0
    assert "running=False rows=1" in capsys.readouterr().out
    import os

    (tmp_path / "rows.jsonl.pid").write_text(f"{os.getpid()}\n")
    assert validate.status(str(out)) == 3


def test_detach_needs_an_out_file():
    assert validate.detach(["--key", "k"], None) == 2


def test_a_second_harness_on_the_same_lane_is_refused(tmp_path):
    first = validate.lane_lock(tmp_path, "/kc/a", "social-network")
    with pytest.raises(validate.LaneBusy, match="another validate.py"):
        validate.lane_lock(tmp_path, "/kc/a", "social-network")
    other = validate.lane_lock(tmp_path, "/kc/b", "social-network")  # another cluster is fine
    first.close()
    validate.lane_lock(tmp_path, "/kc/a", "social-network").close()  # released with its holder
    other.close()


def test_a_second_harness_process_is_refused(tmp_path, monkeypatch):
    """The lock is per process: a child validate.py on the same lane exits 4 with a lane_busy row."""
    import subprocess
    import sys

    monkeypatch.setattr(validate, "LOCK_DIR", tmp_path)
    held = validate.lane_lock(tmp_path, "/kc/a", "social-network")
    code = (
        "import importlib.util,sys,pathlib;"
        f"s=importlib.util.spec_from_file_location('v', {str(_PATH)!r});v=importlib.util.module_from_spec(s);"
        "s.loader.exec_module(v);"
        f"v.LOCK_DIR=pathlib.Path({str(tmp_path)!r});"
        "sys.exit(v.main(['--key','k','--namespace','social-network']))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env={**__import__("os").environ, "KUBECONFIG": "/kc/a"},
    )
    held.close()
    assert out.returncode == 4 and "lane_busy" in out.stdout


class _Clock:
    def __init__(self):
        self.t = 0.0

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += s


def _settling_harness(unhealthy_polls, deadline=100.0):
    """A cluster whose probe fails for the first ``unhealthy_polls`` probes (an app right after a reseed)."""
    clock = _Clock()
    state = {"broken": False}
    cluster = FakeCluster(state)
    calls = {"n": 0}
    real = cluster.run_pod

    def run_pod(manifest, timeout_s):
        calls["n"] += 1
        state["broken"] = calls["n"] <= unhealthy_polls
        return real(manifest, timeout_s)

    cluster.run_pod = run_pod
    h = validate.Harness(
        cluster, persist_s=0, probe_rounds=1, state_dir=None, reset_cmd="true",
        ready_deadline_s=deadline, ready_poll_s=10.0, clock=clock.now, sleep=clock.sleep,
    )
    return h, clock, calls


def test_baseline_waits_for_a_passing_probe_instead_of_going_invalid():
    h, clock, calls = _settling_harness(unhealthy_polls=3)
    got = h.wait_ready("ns", _ENTRY)
    assert got["ready"] and got["attempts"] == 4 and got["waited_s"] == 30.0 and calls["n"] == 4


def test_the_settle_wait_is_bounded_by_its_deadline():
    h, clock, calls = _settling_harness(unhealthy_polls=10**6, deadline=45.0)
    got = h.wait_ready("ns", _ENTRY)
    assert not got["ready"] and clock.t <= 45.0 and got["attempts"] <= 5


def test_a_healthy_app_is_not_delayed():
    h, clock, _ = _settling_harness(unhealthy_polls=0)
    assert h.wait_ready("ns", _ENTRY)["attempts"] == 1 and clock.t == 0.0


def test_reset_row_is_valid_only_once_the_app_is_ready_again():
    h, _, _ = _settling_harness(unhealthy_polls=2)
    row = h.reset("ns", _ENTRY)
    assert row["status"] == "VALID" and row["settle"]["attempts"] == 3
    h, _, _ = _settling_harness(unhealthy_polls=10**6, deadline=30.0)
    assert h.reset("ns", _ENTRY)["status"] == "INVALID"


def test_a_wave_symptom_is_seen_when_any_sample_shows_it():
    state = {"broken": True}
    h = validate.Harness(
        FakeCluster(state), persist_s=0, probe_rounds=1, state_dir=None, reset_cmd=None, symptom_samples=3,
        symptom_gap_s=1.0, sleep=lambda s: None,
    )
    problem = FakeProblem(state)
    seen = iter([False, False, True])
    h._symptom_once = lambda p: {"visible": next(seen), "traffic": {}, "fault": {}}
    got = h.symptom(problem)
    assert got["visible"] and got["samples"] == 3


def test_a_symptom_that_never_shows_stays_invisible_after_all_samples():
    h = validate.Harness(
        FakeCluster({"broken": False}), persist_s=0, probe_rounds=1, state_dir=None, reset_cmd=None,
        symptom_samples=2, symptom_gap_s=1.0, sleep=lambda s: None,
    )
    h._symptom_once = lambda p: {"visible": False, "traffic": {}, "fault": {}}
    got = h.symptom(FakeProblem({"broken": False}))
    assert not got["visible"] and got["samples"] == 2
