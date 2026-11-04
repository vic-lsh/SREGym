"""Validate a component-fault catalog key live: symptom, persistence, every repair's declared outcome.

    uv run python tools/fault_validation/validate.py --key redis_replica_misconfig \
        --app social_network --seeds 1 2 3 [--persist-s 15] [--probe-rounds 3] [--only NAME ...] \
        [--reset-cmd 'lane/reset.sh --reseed'] [--lanes KUBECONFIG ...] [--out trials.jsonl]

Runs against an already-deployed app namespace (KUBECONFIG selects the cluster; restore_source
needs SREGYM_ACCEPTED_RENDER). For each seed it samples the key's draw, then runs one trial per
repair (the reference and every ``Repair`` of ``problem.repairs()``), each on a fresh injection:

  inject -> symptom visible? -> wait --persist-s -> still visible? -> apply the repair -> settle ->
  verdict (per-fault oracle AND namespace collateral walk AND entry-point traffic probe) -> recover

Each trial row has ``status``:
  VALID    the repair produced its declared outcome (``expect``: passes / fault_not_cleared /
           data_lost) and the symptom was visible before and after the persistence window;
  FAIL     it did not (the claim is refuted: wrong reason, a decoy that passes, no symptom);
  INVALID  the trial could not be judged: an oracle step errored (inconclusive), a probe pod did
           not run, injection or the repair raised (a declared no-op that rolled a workload).
A decoy never counts as "correctly failing" unless its verdict reason matches. Repairs that
destroy data run after the others; ``--reset-cmd`` runs after each of them (and the next trial
waits for it). ``--detach`` runs the same arguments in a new session (pid in <out>.pid, output in
<out>.log) so a caller's timeout cannot kill it; ``--status <out>`` prints its rows and whether
it is still running. Injected state is persisted under ``--state-dir`` while a fault is live; a later run
recovers any leftover fault before its first trial. ``--lanes`` shards the seeds over several
kubeconfigs (one subprocess per lane). Exit status 0 only if every row is VALID.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import secrets
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sregym.conductor.problems.composition import catalog, grading

SETTLE_S = 300.0
READY_DEADLINE_S = 900.0  # bound on waiting for a healthy app after a reseed/reset (polled, never a fixed sleep)
READY_POLL_S = 10.0
VALID, FAIL, INVALID = "VALID", "FAIL", "INVALID"
DEFAULT_STATE_DIR = Path(os.path.expanduser("~/.cache/sregym/fault-validation"))


class Harness:
    def __init__(
        self,
        cluster,
        *,
        persist_s: float,
        probe_rounds: int,
        state_dir: Path | None,
        reset_cmd: str | None,
        ready_deadline_s: float = READY_DEADLINE_S,
        ready_poll_s: float = READY_POLL_S,
        symptom_samples: int = 1,
        symptom_gap_s: float = 10.0,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.cluster = cluster
        self.ready_deadline_s, self.ready_poll_s = ready_deadline_s, ready_poll_s
        self.symptom_samples, self.symptom_gap_s = symptom_samples, symptom_gap_s
        self._clock, self._sleep = clock, sleep
        self.persist_s = persist_s
        self.probe_rounds = probe_rounds
        self.state_dir = state_dir
        self.reset_cmd = reset_cmd

    # --- probes ------------------------------------------------------------------------------------

    def probe(self, namespace: str, entry: grading.EntryPoint) -> dict[str, Any]:
        r = self.probe_rounds
        manifest = grading.probe_pod_manifest(f"validate-probe-{secrets.token_hex(4)}", namespace, entry, r)
        phase, logs = self.cluster.run_pod(manifest, timeout_s=grading.probe_deadline_s(entry, r) + 30)
        if phase.startswith("Error"):
            return {"ok": False, "error": f"probe pod not run: {phase}"}
        result = grading.parse_probe_log(logs)
        return {"ok": result.ok, "detail": result.detail(), "failed": list(result.failed_paths)}

    def health(self, namespace: str, entry: grading.EntryPoint) -> dict[str, Any]:
        pods, error = self.cluster.list_pods(namespace)
        collateral_ok, reason = (False, error) if pods is None else grading.collateral_verdict(pods)
        probe = self.probe(namespace, entry)
        return {"collateral_ok": collateral_ok, "collateral": reason, "traffic_ok": probe["ok"], "traffic": probe}

    def wait_ready(self, namespace: str, entry: grading.EntryPoint) -> dict[str, Any]:
        """Poll until rollouts are done AND a baseline probe passes (collateral + entry-point traffic),
        bounded by ``ready_deadline_s``. Right after a reseed/reset the app is briefly unhealthy (restarted
        services, stale pooled connections); judging a baseline then loses the whole seed to INVALID.
        Returns the last health reading plus ``attempts`` and ``waited_s``."""
        start = self._clock()
        attempts = 0
        while True:
            attempts += 1
            remaining = self.ready_deadline_s - (self._clock() - start)
            settled = self.cluster.wait_for_rollouts(namespace, max(1.0, min(SETTLE_S, remaining))).settled
            health = self.health(namespace, entry)
            ok = settled and health["collateral_ok"] and health["traffic_ok"]
            waited = self._clock() - start
            if ok or waited + self.ready_poll_s > self.ready_deadline_s:
                return {**health, "ready": ok, "settled": settled, "attempts": attempts, "waited_s": round(waited, 1)}
            self._sleep(self.ready_poll_s)

    def symptom(self, problem) -> dict[str, Any]:
        """Visible in any of ``symptom_samples`` readings (default 1) spaced ``symptom_gap_s`` apart. A fault whose
        victim is repeatedly OOM-killed and restarted (connection_exhaustion) shows its symptom in waves, so one
        reading can land in a healthy gap; the row records ``samples`` taken."""
        for taken in range(1, max(1, self.symptom_samples) + 1):
            out = self._symptom_once(problem)
            if out["visible"] or taken >= self.symptom_samples:
                return {**out, "samples": taken}
            self._sleep(self.symptom_gap_s)
        raise AssertionError("unreachable")

    def _symptom_once(self, problem) -> dict[str, Any]:
        """Visible at the entry points: the traffic probe fails, seeded reads changed or fail, or a
        write through the entry point does not land."""
        fault = problem.mitigation_oracle.evaluate()
        probe = self.probe(problem.namespace, problem.binding.entry)
        failed = set(fault.get("failed") or ())
        visible = not probe["ok"] or bool(failed & {"data", "write"})
        return {"visible": visible, "traffic": probe, "fault": fault}

    def verdict(self, problem) -> dict[str, Any]:
        settle = self.cluster.wait_for_rollouts(problem.namespace, SETTLE_S)
        fault = problem.mitigation_oracle.evaluate()
        health = self.health(problem.namespace, problem.binding.entry)
        success = grading.mitigation_success(
            [bool(fault.get("success"))], health["collateral_ok"], health["traffic_ok"]
        )
        return {"success": success, "settled": settle.settled, "fault": fault, **health}

    # --- state on disk -----------------------------------------------------------------------------

    def _state_path(self, key: str, seed: int) -> Path | None:
        return None if self.state_dir is None else self.state_dir / f"{key}__s{seed}.json"

    def save_state(self, key: str, app: str, seed: int, params: dict, problem) -> None:
        path = self._state_path(key, seed)
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        doc = {"key": key, "app": app, "seed": seed, "params": params, "namespace": problem.namespace}
        doc["state"] = problem.injected_state() if hasattr(problem, "injected_state") else {}
        path.write_text(json.dumps(doc))

    def clear_state(self, key: str, seed: int) -> None:
        path = self._state_path(key, seed)
        if path is not None:
            path.unlink(missing_ok=True)

    def recover_leftovers(self) -> Iterator[dict[str, Any]]:
        """Recover faults a killed run left live (one state file each)."""
        if self.state_dir is None or not self.state_dir.is_dir():
            return
        for path in sorted(self.state_dir.glob("*.json")):
            doc = json.loads(path.read_text())
            row: dict[str, Any] = {
                "trial": "recover_leftover",
                "file": str(path),
                "key": doc["key"],
                "seed": doc["seed"],
            }
            try:
                problem = _build(doc["key"], doc["app"], doc["params"], doc["namespace"])
                problem.load_injected_state(doc.get("state") or {})
                _recover(problem)
                path.unlink()
                row["status"] = VALID
            except Exception as e:
                row.update(status=INVALID, error=f"{type(e).__name__}: {e}"[:500])
            row["pass"] = row["status"] == VALID
            yield row

    # --- trials ------------------------------------------------------------------------------------

    def run_trial(self, problem, name: str, repair, *, key: str = "", app: str = "", seed: int = 0, params=None):
        expect = repair.expect.value
        row: dict[str, Any] = {"trial": name, "expect": expect, "host_load_1m_start": os.getloadavg()[0]}
        t0 = time.monotonic()
        try:
            self.save_state(key, app, seed, params or {}, problem)
            problem.inject_fault()
            self.save_state(key, app, seed, params or {}, problem)
            row["symptom"] = self.symptom(problem)
            time.sleep(self.persist_s)
            row["persisted"] = self.symptom(problem)
            if name == "reference":
                problem.reference_mitigation()
            else:
                problem.apply(repair)
            row["verdict"] = self.verdict(problem)
            row.update(judge(row, expect))
        except Exception as e:  # injection or repair raised: the trial says nothing about the claim
            row.update(status=INVALID, why=f"{type(e).__name__}: {e}"[:800])
        finally:
            try:
                _recover(problem)
                self.clear_state(key, seed)
            except Exception as e:
                row["recover_error"] = f"{type(e).__name__}: {e}"[:400]
                row["status"] = INVALID
        row["pass"] = row["status"] == VALID
        row["host_load_1m_end"] = os.getloadavg()[0]
        row["duration_s"] = round(time.monotonic() - t0, 1)
        return row

    def reset(self, namespace: str | None = None, entry: grading.EntryPoint | None = None) -> dict[str, Any]:
        """Run the reset command, then (given the app's namespace and entry point) wait until the app is
        healthy again. The row is VALID iff the app is ready within the deadline."""
        t0 = time.monotonic()
        out = subprocess.run(self.reset_cmd, shell=True, capture_output=True, text=True, check=False)
        ok = out.returncode == 0
        row: dict[str, Any] = {"trial": "reset", "cmd": self.reset_cmd, "returncode": out.returncode}
        if namespace is not None and entry is not None:
            settle = self.wait_ready(namespace, entry)
            ok = settle["ready"]
            row["settle"] = {k: settle[k] for k in ("ready", "attempts", "waited_s")}
        return {
            **row,
            "status": VALID if ok else INVALID,
            "pass": ok,
            "tail": (out.stdout + out.stderr)[-400:],
            "duration_s": round(time.monotonic() - t0, 1),
        }

    def validate_seed(
        self, key: str, app: str, seed: int, namespace: str, only=None, overrides: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield each row as soon as it finishes; the last row is the seed summary."""
        t0 = time.monotonic()
        entry = {e.key: e for e in catalog.entries_for(app)}[key]
        draw = entry.sample(app, random.Random(seed))
        params = {**draw.param_dict(), **(overrides or {})}  # --set pins a target or severity over the seed's draw
        problem = _build(key, app, params, namespace)
        base = {"key": key, "app": app, "seed": seed, "params": params}
        rows: list[dict[str, Any]] = []

        def emit(row):
            row = {**base, **row}
            rows.append(row)
            return row

        baseline = self.wait_ready(namespace, problem.binding.entry)
        ok = baseline["ready"]
        yield emit({"trial": "baseline", **baseline, "status": VALID if ok else INVALID, "pass": ok})
        if ok:
            from sregym.conductor.problems.component_fault import Expect, Repair

            reference = Repair(problem.reference_mitigation, Expect.PASSES)
            plan = [("reference", reference), *sorted(problem.repairs().items())]
            plan = [(n, r) for n, r in plan if only is None or n in only]
            plan.sort(key=lambda nr: nr[1].destroys_data)  # destructive repairs last
            for name, repair in plan:
                label = (
                    name
                    if name == "reference"
                    else f"{'acceptable' if repair.expect.value == 'passes' else 'decoy'}:{name}"
                )
                yield emit(self.run_trial(problem, label, repair, key=key, app=app, seed=seed, params=params))
                if repair.destroys_data:
                    if self.reset_cmd:
                        yield emit(self.reset(namespace, problem.binding.entry))
                    else:
                        yield emit({"trial": "reset_needed", "status": VALID, "pass": True, "after": name})
            self.cluster.wait_for_rollouts(namespace, SETTLE_S)
            final = self.wait_ready(namespace, problem.binding.entry)
            ok = final["ready"]
            yield emit({"trial": "recovered", **final, "status": VALID if ok else INVALID, "pass": ok})
        counts = {s: sum(r.get("status") == s for r in rows) for s in (VALID, FAIL, INVALID)}
        yield {
            **base,
            "trial": "seed_summary",
            **counts,
            "pass": counts[FAIL] == counts[INVALID] == 0,
            "status": VALID if counts[FAIL] == counts[INVALID] == 0 else FAIL,
            "wall_s": round(time.monotonic() - t0, 1),
        }


def judge(row: dict[str, Any], expect: str) -> dict[str, Any]:
    """Status of a finished trial (pure): see the module docstring."""
    checks = [row["symptom"], row["persisted"]]
    oracles = [c["fault"] for c in checks] + [row["verdict"]["fault"]]
    probes = [c["traffic"] for c in checks] + [row["verdict"]["traffic"]]
    if any(o.get("inconclusive") for o in oracles):
        return {"status": INVALID, "why": "an oracle step errored: " + str([o.get("error") for o in oracles])[:300]}
    if any(p.get("error") for p in probes):
        return {"status": INVALID, "why": "a probe pod did not run"}
    if not (row["symptom"]["visible"] and row["persisted"]["visible"]):
        return {"status": FAIL, "why": "no symptom at the entry points (or it did not persist)"}
    fault = row["verdict"]["fault"]
    observed = fault.get("reason")
    if observed != expect:
        return {
            "status": FAIL,
            "observed": observed,
            "why": f"expected {expect}, verdict says {observed}: {fault.get('failed')}",
        }
    if expect == "passes" and not row["verdict"]["success"]:
        return {"status": FAIL, "observed": observed, "why": "per-fault verdict passes but collateral/traffic fails"}
    return {"status": VALID, "observed": observed}


def _recover(problem) -> None:
    """``recover_fault`` with its errors raised (the decorator only logs them)."""
    undecorated = getattr(type(problem).recover_fault, "__wrapped__", None)
    (undecorated or type(problem).recover_fault)(problem)


class LaneBusy(RuntimeError):
    pass


def lane_lock(state_dir: Path, kubeconfig: str, namespace: str):
    """An exclusive, non-blocking flock per (kubeconfig, namespace): two harnesses on one lane
    would inject into each other's trials. Returns the open file (keep it open); raises LaneBusy."""
    import fcntl
    import hashlib

    state_dir.mkdir(parents=True, exist_ok=True)
    real = os.path.realpath(kubeconfig) if kubeconfig else ""
    digest = hashlib.sha256(f"{real}|{namespace}".encode()).hexdigest()[:16]
    path = state_dir / f"lane-{digest}.lock"
    f = open(path, "a+")  # noqa: SIM115 (held for the process lifetime)
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.seek(0)
        holder = f.read().strip()
        f.close()
        raise LaneBusy(
            f"another validate.py holds {path} ({holder}): kubeconfig={real} namespace={namespace}"
        ) from None
    f.seek(0)
    f.truncate()
    f.write(f"pid={os.getpid()} kubeconfig={real} namespace={namespace}\n")
    f.flush()
    return f


LOCK_DIR = Path(os.path.expanduser("~/.cache/sregym/fault-validation-locks"))


def _build(key: str, app: str, params: dict, namespace: str):
    entry = {e.key: e for e in catalog.entries_for(app)}[key]
    problem = entry.build(_Draw(app, params))
    problem.namespace = namespace
    return problem


class _Draw:
    def __init__(self, app, params):
        self.app, self._params = app, params

    def param_dict(self):
        return dict(self._params)


def _run_lanes(argv: list[str], lanes: list[str], seeds: list[int]) -> int:
    """One subprocess per kubeconfig, seeds dealt round-robin; rows stream through."""
    procs = []
    for i, kubeconfig in enumerate(lanes):
        mine = seeds[i :: len(lanes)]
        if not mine:
            continue
        child = [a for a in _strip(argv, ("--lanes", "--seeds"))] + ["--seeds", *map(str, mine)]
        env = {**os.environ, "KUBECONFIG": kubeconfig}
        procs.append(subprocess.Popen([sys.executable, __file__, *child], env=env))
    return max((p.wait() for p in procs), default=0)


def _strip(argv: list[str], flags: tuple[str, ...]) -> list[str]:
    out, skip = [], False
    for a in argv:
        if a in flags:
            skip = True
            continue
        if skip and a.startswith("--"):
            skip = False
        if not skip:
            out.append(a)
    return out


def detach(argv: list[str], out: str | None, script: str | None = None) -> int:
    """Re-run these arguments without --detach in a new session; rows go to ``out``.
    ``script`` defaults to this file (a sibling tool passes its own)."""
    if not out:
        print("--detach needs --out", file=sys.stderr)
        return 2
    child = [a for a in argv if a != "--detach"]
    with open(out + ".log", "ab") as log:
        proc = subprocess.Popen(
            [sys.executable, os.path.abspath(script or __file__), *child],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    Path(out + ".pid").write_text(f"{proc.pid}\n")
    print(json.dumps({"detached": proc.pid, "out": out, "log": out + ".log", "pid_file": out + ".pid"}))
    return 0


def status(out: str) -> int:
    """One line per row of ``out`` plus whether the detached run is still alive. Exit 0 when it
    finished and every row is VALID, 1 when a row is not, 3 while it is still running."""
    alive = False
    pid_file = Path(out + ".pid")
    if pid_file.is_file():
        try:
            os.kill(int(pid_file.read_text().strip()), 0)
            alive = True
        except (OSError, ValueError):
            alive = False
    rows = (
        [json.loads(line) for line in Path(out).read_text().splitlines() if line.strip()] if Path(out).is_file() else []
    )
    for r in rows:
        why = (r.get("why") or "")[:160]
        dur = r.get("duration_s", r.get("wall_s", ""))
        print(f"s{r.get('seed', '-')} {r['trial']:<45} {r.get('status', '-'):<8} {r.get('observed') or ''} {dur} {why}")
    print(f"running={alive} rows={len(rows)}")
    if alive:
        return 3
    return 0 if rows and all(r.get("pass") for r in rows) else 1


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key", required=True)
    ap.add_argument("--app", default="social_network")
    ap.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    ap.add_argument("--namespace", default=None, help="default: the app binding's namespace")
    ap.add_argument("--persist-s", type=float, default=15.0)
    ap.add_argument("--probe-rounds", type=int, default=3, help="traffic probe rounds per check (grading uses 6)")
    ap.add_argument("--only", nargs="*", default=None, help="repair names to run (reference, restore_source, ...)")
    ap.add_argument("--reset-cmd", default=None, help="shell command run after each data-destroying repair")
    ap.add_argument(
        "--ready-deadline-s",
        type=float,
        default=READY_DEADLINE_S,
        help="max wait for rollouts + a passing baseline probe before a seed's baseline and after a reset",
    )
    ap.add_argument(
        "--symptom-samples",
        type=int,
        default=1,
        help="readings per symptom check (visible if any shows it); for faults whose symptom comes in waves",
    )
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="'' disables persisting injected state")
    ap.add_argument("--lanes", nargs="*", default=None, help="kubeconfigs: shard seeds over them in parallel")
    ap.add_argument(
        "--set", nargs="*", default=[], metavar="K=JSON", help='pin draw parameters, e.g. target="user-service"'
    )
    ap.add_argument("--out", default=None, help="also append JSON lines here")
    ap.add_argument(
        "--detach",
        action="store_true",
        help="run in a new session (survives the caller being killed); needs --out; writes <out>.pid "
        "and <out>.log, prints the pid and returns",
    )
    ap.add_argument("--status", metavar="OUT", default=None, help="summarize <OUT> rows and whether its run is alive")
    if "--status" in argv:
        return status(argv[argv.index("--status") + 1])
    args = ap.parse_args(argv)
    if args.detach:
        return detach(argv, args.out)
    if args.lanes:
        return _run_lanes(argv, args.lanes, args.seeds)

    from sregym.conductor.problems.colocated_composite import KubeClusterIO
    from sregym.conductor.problems.composition.app_bindings import BINDINGS

    namespace = args.namespace or BINDINGS[args.app].namespace
    overrides = {k: json.loads(v) for k, _, v in (kv.partition("=") for kv in args.set)}
    try:  # one lock directory for every state dir: two runs differing only in --state-dir still collide
        lock = lane_lock(LOCK_DIR, os.environ.get("KUBECONFIG", ""), namespace)  # noqa: F841 (held)
    except LaneBusy as e:
        print(json.dumps({"trial": "lane_busy", "status": INVALID, "pass": False, "why": str(e)}), flush=True)
        return 4
    harness = Harness(
        KubeClusterIO(),
        persist_s=args.persist_s,
        probe_rounds=args.probe_rounds,
        state_dir=Path(args.state_dir) if args.state_dir else None,
        reset_cmd=args.reset_cmd,
        ready_deadline_s=args.ready_deadline_s,
        symptom_samples=args.symptom_samples,
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
    for seed in args.seeds:
        for row in harness.validate_seed(
            args.key, args.app, seed, namespace, set(args.only) if args.only else None, overrides
        ):
            emit(row)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
