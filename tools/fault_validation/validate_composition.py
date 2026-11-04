"""Validate a composed multi-fault plan live: every draw injects, grades and needs its own repair.

    uv run python tools/fault_validation/validate_composition.py \
        --plan-id mf__social_network__n2__s7__reliable__g2__v_ [--plan-id ...] \
        [--repair-order declared|reverse] [--persist-s 15] [--probe-rounds 3] \
        [--reset-cmd 'lane/reset.sh --reseed'] [--out rows.jsonl] [--detach] [--status OUT]

Runs against an already-deployed app namespace (like ``validate.py``, whose probes, lane lock,
``--detach``/``--status`` and row statuses it reuses). For one plan id it resolves the composite
through ``composition.resolve`` (the path an experiment uses: all snapshots are prepared before
anything is injected) and walks:

  baseline        namespace healthy (collateral walk + traffic probe), or the plan is INVALID
  inject          composite_valid, every fault live (validity fields in the row)
  all_broken      composite oracle FAILS, every per-fault verdict fails, faults_fixed == 0; checked
                  again after --persist-s
  repair:<i>      one fault's reference repair at a time (components keep their snapshot until the
                  end: ``reference_mitigation``; other faults ``recover_component``): the composite
                  must still FAIL until the last one, and no unrepaired fault's verdict may pass.
                  ``warn`` lists a repaired fault whose own verdict still fails (its data reads go
                  through a path another live fault breaks: attribution coupling, not a failure)
  all_fixed       composite oracle PASSES: every per-fault verdict, collateral, traffic
  recovered       final recovery of whatever is pending, then a healthy namespace

Row ``status``: VALID, FAIL (a check was refuted: a composite that passes with a fault live, a
verdict that passes while its fault is live, a final verdict that fails after every reference),
INVALID (could not judge: injection or oracle errored, composite_valid false, a probe pod did not
run). Injected state of every fault is written to ``--state-dir`` while the composite is live; a
later run recovers leftovers first. Exit 0 only if every row is VALID.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import validate as single  # noqa: E402  (sibling tool: harness, lane lock, detach/status)

VALID, FAIL, INVALID = single.VALID, single.FAIL, single.INVALID
DEFAULT_STATE_DIR = Path(os.path.expanduser("~/.cache/sregym/composition-validation"))


# --- pure judging (unit tested: tests/composition/test_validate_composition.py) -------------------


def fault_verdicts(result: dict[str, Any], n: int) -> list[bool]:
    return [bool(result.get(f"fault_{i}_verdict")) for i in range(n)]


def judge_all_broken(result: dict[str, Any], n: int) -> dict[str, Any]:
    """The composite must fail, every fault's verdict must fail, none counted fixed."""
    verdicts = fault_verdicts(result, n)
    wrong = [i for i, ok in enumerate(verdicts) if ok]
    if wrong:
        return {"status": FAIL, "why": f"per-fault verdict passes while its fault is live: faults {wrong}"}
    if result.get("success"):
        return {"status": FAIL, "why": "composite passes with every fault live"}
    return {"status": VALID}


def judge_repair(result: dict[str, Any], n: int, repaired: list[int], last: bool) -> dict[str, Any]:
    """After repairing the faults in ``repaired`` (in order): the composite fails until the last one,
    an unrepaired fault's verdict never passes, and a repaired fault whose verdict fails is a warning."""
    verdicts = fault_verdicts(result, n)
    live = [i for i in range(n) if i not in repaired]
    bad = [i for i in live if verdicts[i]]
    warn = [i for i in repaired if not verdicts[i]]
    out: dict[str, Any] = {"warn_repaired_verdict_fails": warn} if warn else {}
    if bad:
        return {"status": FAIL, "why": f"verdict passes while its fault is unrepaired: faults {bad}", **out}
    if result.get("success") and not last:
        return {"status": FAIL, "why": f"composite passes with faults {live} still live", **out}
    return {"status": VALID, **out}


def judge_all_fixed(result: dict[str, Any], n: int) -> dict[str, Any]:
    verdicts = fault_verdicts(result, n)
    missing = [i for i, ok in enumerate(verdicts) if not ok]
    if missing:
        return {"status": FAIL, "why": f"verdict fails after every reference repair: faults {missing}"}
    if not result.get("collateral_ok"):
        return {"status": FAIL, "why": f"collateral fails: {result.get('collateral_reason')}"}
    if not result.get("traffic_ok"):
        return {"status": FAIL, "why": f"traffic fails: {result.get('traffic_detail')}"}
    if not result.get("success"):
        return {"status": FAIL, "why": "composite fails after every reference repair"}
    return {"status": VALID}


def inconclusive(result: dict[str, Any]) -> list[str]:
    """Per-fault oracle steps that errored (they say nothing about the claim) and probe failures to run."""
    found = [
        f"{o.get('name')}: {o.get('error')}"
        for o in result.get("oracles", [])
        if o.get("inconclusive") or o.get("infra_error")
    ]
    if str(result.get("traffic_detail", "")).startswith("probe pod not run"):
        found.append(str(result["traffic_detail"]))
    return found


def summarize(result: dict[str, Any], n: int) -> dict[str, Any]:
    """The columns worth keeping per evaluation."""
    return {
        "success": bool(result.get("success")),
        "faults_fixed": result.get("faults_fixed"),
        "verdicts": fault_verdicts(result, n),
        "reasons": [o.get("reason") for o in result.get("oracles", [])],
        "failed": [o.get("failed") for o in result.get("oracles", [])],
        "collateral_ok": result.get("collateral_ok"),
        "collateral_reason": result.get("collateral_reason"),
        "traffic_ok": result.get("traffic_ok"),
        "traffic": result.get("traffic_detail"),
        "grading_s": result.get("composite_grading_s"),
    }


# --- the harness ----------------------------------------------------------------------------------


def is_component(problem) -> bool:
    return hasattr(problem, "reference_mitigation") and hasattr(problem, "injected_state")


class CompositionHarness:
    def __init__(self, cluster, *, persist_s: float, probe_rounds: int, state_dir: Path | None, reset_cmd: str | None):
        self.base = single.Harness(
            cluster, persist_s=persist_s, probe_rounds=probe_rounds, state_dir=None, reset_cmd=reset_cmd
        )
        self.cluster = cluster
        self.persist_s = persist_s
        self.state_dir = state_dir

    # state on disk: one file per plan while its faults are live
    def _path(self, plan_id: str) -> Path | None:
        if self.state_dir is None:
            return None
        return self.state_dir / f"{hashlib.sha256(plan_id.encode()).hexdigest()[:16]}.json"

    def save_state(self, plan_id: str, composite) -> None:
        path = self._path(plan_id)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        states = [p.injected_state() if is_component(p) else {} for p in composite.problems]
        path.write_text(json.dumps({"plan_id": plan_id, "states": states}))

    def clear_state(self, plan_id: str) -> None:
        path = self._path(plan_id)
        if path is not None:
            path.unlink(missing_ok=True)

    def recover_leftovers(self) -> Iterator[dict[str, Any]]:
        """Recover composites a killed run left live: every fault's recovery with its saved state."""
        if self.state_dir is None or not self.state_dir.is_dir():
            return
        from sregym.conductor.problems.composition import resolve

        for path in sorted(self.state_dir.glob("*.json")):
            doc = json.loads(path.read_text())
            row: dict[str, Any] = {"trial": "recover_leftover", "plan_id": doc["plan_id"], "file": str(path)}
            errors = []
            try:
                composite = resolve(doc["plan_id"])
                for problem, state in zip(composite.problems, doc["states"], strict=True):
                    try:
                        if is_component(problem):
                            problem.load_injected_state(state or {})
                        single._recover(problem)
                    except Exception as e:
                        errors.append(f"{type(problem).__name__}: {type(e).__name__}: {e}"[:300])
                if not errors:
                    path.unlink()
            except Exception as e:
                errors.append(f"{type(e).__name__}: {e}"[:300])
            row.update(status=INVALID if errors else VALID, **({"error": errors} if errors else {}))
            row["pass"] = row["status"] == VALID
            yield row

    def evaluate(self, composite) -> dict[str, Any]:
        return composite.mitigation_oracle.evaluate()

    def repair(self, composite, i: int) -> None:
        problem = composite.problems[i]
        if is_component(problem):
            problem.reference_mitigation()  # keeps the snapshot and witnesses for the verdict
        else:
            outcome = composite.recover_component(i)
            if outcome.get("errors"):
                raise RuntimeError(f"recover_component({i}) failed: {outcome['errors']}")

    def validate_plan(self, plan_id: str, namespace: str, repair_order: str = "declared") -> Iterator[dict[str, Any]]:
        """Yield each row as it finishes; the last is the plan summary."""
        from sregym.conductor.problems.composition import resolve
        from sregym.conductor.problems.composition.app_bindings import BINDINGS
        from sregym.conductor.problems.composition.generator import parse_plan_id

        t0 = time.monotonic()
        composite = resolve(plan_id)
        n = len(composite.problems)
        ref = parse_plan_id(plan_id)
        kinds = [d.key for d in composite.draws]
        base = {
            "plan_id": plan_id,
            "app": ref.app,
            "kinds": kinds,
            "targets": [d.param_dict().get("target") for d in composite.draws],
        }
        rows: list[dict[str, Any]] = []

        def emit(row):
            row = {**base, **row}
            row["pass"] = row.get("status") == VALID
            rows.append(row)
            return row

        entry = BINDINGS[ref.app].entry
        baseline = self.base.health(namespace, entry)
        ok = baseline["collateral_ok"] and baseline["traffic_ok"]
        yield emit({"trial": "baseline", **baseline, "status": VALID if ok else INVALID})
        if ok:
            yield from self._walk(composite, plan_id, n, repair_order, emit)
            self.cluster.wait_for_rollouts(namespace, single.SETTLE_S)
            final = self.base.health(namespace, entry)
            ok = final["collateral_ok"] and final["traffic_ok"]
            yield emit({"trial": "recovered", **final, "status": VALID if ok else INVALID})
        counts = {s: sum(r.get("status") == s for r in rows) for s in (VALID, FAIL, INVALID)}
        good = counts[FAIL] == counts[INVALID] == 0
        yield {
            **base,
            "trial": "plan_summary",
            **counts,
            "pass": good,
            "status": VALID if good else (FAIL if counts[FAIL] else INVALID),
            "wall_s": round(time.monotonic() - t0, 1),
        }

    def _walk(self, composite, plan_id: str, n: int, repair_order: str, emit) -> Iterator[dict[str, Any]]:
        order = list(range(n)) if repair_order == "declared" else list(reversed(range(n)))
        t0 = time.monotonic()
        try:
            yield from self._steps(composite, plan_id, n, order, emit, t0)
        except Exception as e:  # an unexpected harness error: the plan says nothing about the claim
            yield emit({"trial": "harness_error", "status": INVALID, "why": f"{type(e).__name__}: {e}"[:600]})
        t1 = time.monotonic()
        try:
            outcome = composite.recovery_result()
            if outcome.get("errors"):
                raise RuntimeError(f"recovery errors: {outcome['errors']}")
            self.clear_state(plan_id)
            yield emit({"trial": "recovery", "status": VALID, "duration_s": round(time.monotonic() - t1, 1)})
        except Exception as e:  # state file stays: the next run recovers the leftovers
            yield emit({"trial": "recovery", "status": INVALID, "why": f"{type(e).__name__}: {e}"[:500]})

    def _steps(self, composite, plan_id: str, n: int, order: list[int], emit, t0: float) -> Iterator[dict[str, Any]]:
        self.save_state(plan_id, composite)  # before: a crash mid-injection leaves something to recover
        composite.inject_fault()
        self.save_state(plan_id, composite)
        validity = dict(composite.validity)
        if validity.get("composite_valid") is not True:
            yield emit(
                {
                    "trial": "inject",
                    "status": INVALID,
                    "why": "composite_valid is not true (see validity)",
                    "validity": validity,
                    "duration_s": round(time.monotonic() - t0, 1),
                }
            )
            return
        yield emit({"trial": "inject", "status": VALID, "validity": validity, "order": composite.injection_order})

        def judged(result: dict[str, Any], judge) -> dict[str, Any]:
            if problems := inconclusive(result):
                return {"status": INVALID, "why": "an oracle step errored: " + "; ".join(problems)[:300]}
            return judge(result)

        result = self.evaluate(composite)
        row = {"trial": "all_broken", "result": summarize(result, n), **judged(result, lambda r: judge_all_broken(r, n))}
        time.sleep(self.persist_s)
        again = self.evaluate(composite)
        row["persisted"] = summarize(again, n)
        if row["status"] == VALID:
            row.update(judged(again, lambda r: judge_all_broken(r, n)))
            if row["status"] != VALID:
                row["why"] = "after the persistence window: " + row["why"]
        yield emit(row)

        repaired: list[int] = []
        for step, i in enumerate(order):
            last = step == n - 1
            row = {"trial": f"repair:{i}:{composite.draws[i].key}", "repaired": [*repaired, i]}
            t1 = time.monotonic()
            try:
                self.repair(composite, i)
                repaired.append(i)
                self.save_state(plan_id, composite)
                result = self.evaluate(composite)
                row["result"] = summarize(result, n)
                row.update(judged(result, lambda r: judge_repair(r, n, repaired, last)))
            except Exception as e:
                row.update(status=INVALID, why=f"repair raised {type(e).__name__}: {e}"[:600])
                row["duration_s"] = round(time.monotonic() - t1, 1)
                yield emit(row)
                return
            row["duration_s"] = round(time.monotonic() - t1, 1)
            yield emit(row)
        yield emit(
            {"trial": "all_fixed", "result": summarize(result, n), **judged(result, lambda r: judge_all_fixed(r, n))}
        )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan-id", action="append", required=False, default=[])
    ap.add_argument("--repair-order", choices=("declared", "reverse"), default="declared")
    ap.add_argument("--namespace", default=None, help="default: the app binding's namespace")
    ap.add_argument("--persist-s", type=float, default=15.0)
    ap.add_argument("--probe-rounds", type=int, default=3)
    ap.add_argument("--reset-cmd", default=None, help="run after each plan (e.g. a reseeding reset)")
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="'' disables persisting injected state")
    ap.add_argument("--out", default=None, help="also append JSON lines here")
    ap.add_argument("--detach", action="store_true", help="new session; needs --out; <out>.pid, <out>.log")
    ap.add_argument("--status", metavar="OUT", default=None)
    if "--status" in argv:
        return single.status(argv[argv.index("--status") + 1])
    args = ap.parse_args(argv)
    if not args.plan_id:
        ap.error("--plan-id is required")
    if args.detach:
        return single.detach(argv, args.out, script=__file__)

    from sregym.conductor.problems.colocated_composite import KubeClusterIO
    from sregym.conductor.problems.composition.app_bindings import BINDINGS
    from sregym.conductor.problems.composition.generator import parse_plan_id

    refs = [parse_plan_id(p) for p in args.plan_id]
    if None in refs:
        ap.error("malformed --plan-id")
    namespace = args.namespace or BINDINGS[refs[0].app].namespace
    try:
        lock = single.lane_lock(single.LOCK_DIR, os.environ.get("KUBECONFIG", ""), namespace)  # noqa: F841 (held)
    except single.LaneBusy as e:
        print(json.dumps({"trial": "lane_busy", "status": INVALID, "pass": False, "why": str(e)}), flush=True)
        return 4
    harness = CompositionHarness(
        KubeClusterIO(),
        persist_s=args.persist_s,
        probe_rounds=args.probe_rounds,
        state_dir=Path(args.state_dir) if args.state_dir else None,
        reset_cmd=args.reset_cmd,
    )
    all_ok = True

    def emit(row):
        nonlocal all_ok
        line = json.dumps({**row, "kubeconfig": os.environ.get("KUBECONFIG", "")}, default=str)
        print(line, flush=True)
        if args.out:
            with open(args.out, "a") as f:
                f.write(line + "\n")
        all_ok = all_ok and row["pass"]

    for row in harness.recover_leftovers():
        emit(row)
    for plan_id in args.plan_id:
        for row in harness.validate_plan(plan_id, namespace, args.repair_order):
            emit(row)
        if args.reset_cmd:
            emit(harness.base.reset())
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
