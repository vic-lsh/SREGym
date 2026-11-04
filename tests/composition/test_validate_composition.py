"""tools/fault_validation/validate_composition.py: pure judging and the walk, on a fake composite."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

from sregym.conductor.oracles.rollouts import Settle
from sregym.conductor.problems.composition.spec import FaultDraw, Footprint

_DIR = Path(__file__).resolve().parents[2] / "tools" / "fault_validation"
sys.path.insert(0, str(_DIR))
_spec = importlib.util.spec_from_file_location("validate_composition", _DIR / "validate_composition.py")
vc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(vc)
sys.path.remove(str(_DIR))


def result(verdicts, success=None, collateral=True, traffic=True):
    out = {f"fault_{i}_verdict": v for i, v in enumerate(verdicts)}
    out.update(
        success=all(verdicts) and collateral and traffic if success is None else success,
        collateral_ok=collateral,
        traffic_ok=traffic,
        oracles=[{"name": f"fault_{i}", "success": v} for i, v in enumerate(verdicts)],
    )
    return out


def test_all_broken_requires_every_verdict_and_the_composite_to_fail():
    assert vc.judge_all_broken(result([False, False]), 2)["status"] == vc.VALID
    bad = vc.judge_all_broken(result([False, True]), 2)
    assert bad["status"] == vc.FAIL and "faults [1]" in bad["why"]
    assert vc.judge_all_broken(result([False, False], success=True), 2)["status"] == vc.FAIL


def test_repair_step_composite_must_fail_until_the_last_repair():
    assert vc.judge_repair(result([True, False]), 2, [0], last=False)["status"] == vc.VALID
    early = vc.judge_repair(result([True, False], success=True), 2, [0], last=False)
    assert early["status"] == vc.FAIL and "still live" in early["why"]
    leak = vc.judge_repair(result([True, True], success=False), 2, [0], last=False)
    assert leak["status"] == vc.FAIL and "unrepaired" in leak["why"]
    assert vc.judge_repair(result([True, True]), 2, [0, 1], last=True)["status"] == vc.VALID


def test_repaired_fault_with_failing_verdict_is_a_warning_not_a_failure():
    out = vc.judge_repair(result([False, False]), 2, [0], last=False)
    assert out["status"] == vc.VALID and out["warn_repaired_verdict_fails"] == [0]


def test_all_fixed_names_what_failed():
    assert vc.judge_all_fixed(result([True, True]), 2)["status"] == vc.VALID
    assert "faults [1]" in vc.judge_all_fixed(result([True, False]), 2)["why"]
    assert "collateral" in vc.judge_all_fixed(result([True, True], collateral=False), 2)["why"]
    assert "traffic" in vc.judge_all_fixed(result([True, True], traffic=False), 2)["why"]


def test_inconclusive_collects_errored_steps_and_missing_probe():
    r = result([False])
    r["oracles"][0].update(inconclusive=True, error="snapshot missing")
    r["traffic_detail"] = "probe pod not run: Error"
    found = vc.inconclusive(r)
    assert len(found) == 2 and "snapshot missing" in found[0]


# --- the walk -------------------------------------------------------------------------------------


class Part:
    def __init__(self, live: set, i: int, component: bool):
        self.live, self.i, self.component = live, i, component
        self.repaired = 0
        if component:
            self.reference_mitigation = self._fix
            self.injected_state = lambda: {"i": i}
            self.load_injected_state = lambda s: None

    def _fix(self):
        self.live.discard(self.i)


class FakeComposite:
    def __init__(self, kinds=(True, False), valid=True, early_pass=False):
        self.live: set[int] = set()
        self.kinds, self.early_pass = kinds, early_pass
        self.problems = [Part(self.live, i, c) for i, c in enumerate(kinds)]
        self.draws = [FaultDraw(f"k{i}", "social_network", (), (Footprint("Deployment", f"w{i}"),)) for i in range(len(kinds))]
        self.validity = {"composite_valid": valid}
        self.injection_order = list(range(len(kinds)))
        self.recovered = False
        self.mitigation_oracle = SimpleNamespace(evaluate=self.evaluate)

    def inject_fault(self):
        self.live.update(range(len(self.kinds)))

    def recover_component(self, i):
        self.live.discard(i)
        return {"errors": []}

    def recovery_result(self):
        self.recovered = True
        self.live.clear()
        return {"errors": []}

    def evaluate(self):
        verdicts = [i not in self.live for i in range(len(self.kinds))]
        ok = all(verdicts) or (self.early_pass and len(self.live) <= len(self.kinds) - 1)
        return {
            **{f"fault_{i}_verdict": v for i, v in enumerate(verdicts)},
            "success": ok,
            "collateral_ok": True,
            "traffic_ok": True,
            "oracles": [{} for _ in verdicts],
        }


class FakeCluster:
    def run_pod(self, manifest, timeout_s):
        return "Succeeded", "\n".join(["PROBE_OK /"] * 6 + ["PROBE_DONE"])

    def list_pods(self, namespace):
        return [{"metadata": {"name": "a"}, "status": {"phase": "Running", "containerStatuses": []}}], ""

    def wait_for_rollouts(self, namespace, timeout_s):
        return Settle(True, "", 0.0)


def collect(composite, tmp_path):
    rows = []

    def emit(row):
        row = {**row, "pass": row.get("status") == vc.VALID}
        rows.append(row)
        return row

    h = vc.CompositionHarness(FakeCluster(), persist_s=0, probe_rounds=1, state_dir=tmp_path, reset_cmd=None)
    list(h._walk(composite, "mf__social_network__n2__s1__v_", len(composite.problems), "declared", emit))
    return rows


def test_walk_valid_composite(tmp_path):
    c = FakeComposite()
    rows = collect(c, tmp_path)
    assert [r["trial"] for r in rows] == [
        "inject",
        "all_broken",
        "repair:0:k0",
        "repair:1:k1",
        "all_fixed",
        "recovery",
    ]
    assert all(r["status"] == vc.VALID for r in rows), rows
    assert c.recovered and not list(tmp_path.glob("*.json"))


def test_walk_flags_a_repair_that_is_not_required(tmp_path):
    rows = collect(FakeComposite(early_pass=True), tmp_path)
    repair0 = next(r for r in rows if r["trial"] == "repair:0:k0")
    assert repair0["status"] == vc.FAIL and "still live" in repair0["why"]


def test_walk_invalid_when_the_composite_did_not_inject(tmp_path):
    rows = collect(FakeComposite(valid=False), tmp_path)
    assert rows[0]["trial"] == "inject" and rows[0]["status"] == vc.INVALID
    assert rows[-1]["trial"] == "recovery"


def test_state_file_survives_a_failed_recovery(tmp_path):
    c = FakeComposite()
    c.recovery_result = lambda: {"errors": ["boom"]}
    rows = collect(c, tmp_path)
    assert rows[-1]["status"] == vc.INVALID
    assert list(tmp_path.glob("*.json"))
