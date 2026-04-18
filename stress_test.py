"""Stress-test driver for SREGym fault-injection reliability.

Mimics the `scripts/run_sregym.sh` N-worker setup (one kind cluster per worker,
per-worker env vars, per-worker log file), but replaces the agent-grading loop
with a deploy → inject → verify → (optional loadgen probe) → recover cycle.
Used to exercise the Conductor + problem registry + fault-injection pipeline
at scale without paying for agent invocations.

See `scripts/run_sregym_stress.sh` for the entry point.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import signal
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger("all.stress")


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class Verdict(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    YELLOW = "yellow"
    SKIP = "skip"


@dataclass
class StageOutcome:
    stage: str
    ok: bool
    duration_s: float
    error: Optional[str] = None


@dataclass
class ProbeResult:
    rounds: int = 0
    ok_rounds: int = 0
    success_rate: float = 0.0
    error: Optional[str] = None
    unsupported: bool = False


@dataclass
class PodState:
    phase: str
    ready: bool
    restarts: int


@dataclass
class PodSnapshot:
    pods: dict[tuple[str, str], PodState] = field(default_factory=dict)


@dataclass
class PodDiff:
    added: list[tuple[str, str]] = field(default_factory=list)
    removed: list[tuple[str, str]] = field(default_factory=list)
    changed: list[tuple[str, str, PodState, PodState]] = field(default_factory=list)


@dataclass
class ProblemResult:
    problem_id: str
    worker_id: int
    verdict: Verdict = Verdict.PASS
    stages: list[StageOutcome] = field(default_factory=list)
    pre_probe: Optional[ProbeResult] = None
    post_probe: Optional[ProbeResult] = None
    pod_diff: Optional[PodDiff] = None
    error_stage: Optional[str] = None
    yellow_reasons: list[str] = field(default_factory=list)
    skip_reason: Optional[str] = None
    elapsed_s: float = 0.0


# ---------------------------------------------------------------------------
# Pure logic
# ---------------------------------------------------------------------------


INFRA_NAMESPACES_TO_IGNORE = frozenset({
    "kube-system",
    "kube-public",
    "kube-node-lease",
    "local-path-storage",
    "openebs",
    "prometheus",
    "chaos-mesh",
    "khaos",
    "metrics-server",
    "default",
})

# Loadgen thresholds. A baseline below UNHEALTHY_THRESHOLD means the cluster
# never reached a healthy state — we can't tell if the fault worked.
_BASELINE_HEALTHY_MIN = 0.95
_POST_UNAFFECTED_MIN = 0.95
_BASELINE_UNHEALTHY_MAX = 0.50


def compute_pod_diff(
    pre: PodSnapshot,
    post: PodSnapshot,
    ignore_namespaces: frozenset[str] = INFRA_NAMESPACES_TO_IGNORE,
) -> PodDiff:
    """Diff two pod snapshots, ignoring namespaces listed in ignore_namespaces."""
    def keep(key: tuple[str, str]) -> bool:
        return key[0] not in ignore_namespaces

    pre_keys = {k for k in pre.pods if keep(k)}
    post_keys = {k for k in post.pods if keep(k)}

    added = sorted(post_keys - pre_keys)
    removed = sorted(pre_keys - post_keys)
    changed: list[tuple[str, str, PodState, PodState]] = []
    for key in sorted(pre_keys & post_keys):
        before = pre.pods[key]
        after = post.pods[key]
        if before != after:
            changed.append((key[0], key[1], before, after))
    return PodDiff(added=added, removed=removed, changed=changed)


def compute_verdict(result: ProblemResult) -> Verdict:
    """Derive a verdict from recorded stage outcomes and probe data.

    PASS: all stages green, probes show expected fault impact (or probes unsupported).
    FAIL: any stage raised.
    YELLOW: probes suggest the fault is inert or the baseline was unhealthy.
    SKIP: problem skipped (e.g. khaos required on emulated cluster).
    """
    if result.skip_reason:
        result.verdict = Verdict.SKIP
        return Verdict.SKIP
    if result.error_stage:
        result.verdict = Verdict.FAIL
        return Verdict.FAIL

    yellow: list[str] = []
    pre = result.pre_probe
    post = result.post_probe
    if (
        pre is not None
        and post is not None
        and not pre.unsupported
        and not post.unsupported
        and pre.error is None
        and post.error is None
    ):
        if pre.success_rate <= _BASELINE_UNHEALTHY_MAX:
            yellow.append(
                f"Baseline loadgen unhealthy pre-injection (rate={pre.success_rate:.2f}); "
                "deploy may be flaky — fault-visibility signal unreliable."
            )
        elif pre.success_rate >= _BASELINE_HEALTHY_MIN and post.success_rate >= _POST_UNAFFECTED_MIN:
            yellow.append(
                f"Loadgen unaffected by fault "
                f"(pre={pre.success_rate:.2f}, post={post.success_rate:.2f}); "
                "fault may be silent, inert, or on a non-loadgen path."
            )

    result.yellow_reasons = yellow
    result.verdict = Verdict.YELLOW if yellow else Verdict.PASS
    return result.verdict


# ---------------------------------------------------------------------------
# Pod snapshot + loadgen probe
# ---------------------------------------------------------------------------


def snapshot_pods(kubectl: Any) -> PodSnapshot:
    """Query the live cluster and return a PodSnapshot.

    kubectl is the sregym KubeCtl wrapper (has .exec_command).
    """
    raw = kubectl.exec_command("kubectl get pods -A -o json")
    data = json.loads(raw)
    pods: dict[tuple[str, str], PodState] = {}
    for item in data.get("items", []):
        meta = item.get("metadata", {})
        status = item.get("status", {})
        ns = meta.get("namespace", "")
        name = meta.get("name", "")
        phase = status.get("phase", "Unknown")
        container_statuses = status.get("containerStatuses", []) or []
        ready = bool(container_statuses) and all(cs.get("ready", False) for cs in container_statuses)
        restarts = sum(int(cs.get("restartCount", 0)) for cs in container_statuses)
        pods[(ns, name)] = PodState(phase=phase, ready=ready, restarts=restarts)
    return PodSnapshot(pods=pods)


def sample_loadgen(
    app: Any,
    duration_s: float,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> ProbeResult:
    """Sleep `duration_s`, then read the app's wrk2 loadgen log and aggregate.

    Apps without a `.wrk` attribute (e.g. astronomy_shop's OTEL loadgen) return
    an `unsupported=True` result. A retrievelog() that raises is recorded as
    an error rather than an exception.
    """
    wrk = getattr(app, "wrk", None)
    if wrk is None:
        return ProbeResult(unsupported=True)

    start_time = time.time()
    if duration_s > 0:
        sleep_fn(duration_s)

    try:
        entries = wrk.retrievelog(start_time=start_time)
    except Exception as exc:
        return ProbeResult(error=str(exc))

    total = len(entries)
    ok = sum(1 for e in entries if getattr(e, "ok", False))
    rate = (ok / total) if total > 0 else 0.0
    return ProbeResult(rounds=total, ok_rounds=ok, success_rate=rate)


# ---------------------------------------------------------------------------
# Per-problem runner
# ---------------------------------------------------------------------------


def _record_stage(
    result: ProblemResult,
    stage: str,
    fn: Callable[[], Any],
) -> bool:
    """Run fn() and record a StageOutcome. Returns True on success."""
    started = time.monotonic()
    try:
        fn()
    except Exception as exc:
        elapsed = time.monotonic() - started
        err = f"{type(exc).__name__}: {exc}"
        result.stages.append(StageOutcome(stage=stage, ok=False, duration_s=elapsed, error=err))
        if result.error_stage is None:
            result.error_stage = stage
        logger.exception(f"[{result.problem_id}] stage {stage!r} raised")
        return False
    elapsed = time.monotonic() - started
    result.stages.append(StageOutcome(stage=stage, ok=True, duration_s=elapsed))
    return True


def run_stress_problem(
    conductor: Any,
    problem_id: str,
    *,
    worker_id: int = 0,
    probe_enabled: bool = True,
    probe_duration_s: float = 30.0,
    snapshot_fn: Callable[[Any], PodSnapshot] = snapshot_pods,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> ProblemResult:
    """Run a single problem end-to-end on `conductor`, without agent involvement.

    Stages (recorded in result.stages):
      deploy   – Conductor.deploy_app()
      inject   – problem.inject_fault()
      verify   – problem.verify_fault_applied()
      recover  – problem.recover_fault()
      undeploy – Conductor.undeploy_app()

    Independent probes (not stages):
      pre_probe  – loadgen sampled for probe_duration_s after deploy
      post_probe – loadgen sampled for probe_duration_s after verify
      pod_diff   – pod state delta between pre-inject and post-verify snapshots
    """
    result = ProblemResult(problem_id=problem_id, worker_id=worker_id)
    started = time.monotonic()

    try:
        problem = conductor.problems.get_problem_instance(problem_id)
    except Exception as exc:
        result.error_stage = "lookup"
        result.stages.append(StageOutcome(stage="lookup", ok=False, duration_s=0.0,
                                          error=f"{type(exc).__name__}: {exc}"))
        result.elapsed_s = time.monotonic() - started
        compute_verdict(result)
        return result

    conductor.problem_id = problem_id
    conductor.problem = problem
    conductor.app = problem.app

    # Match start_problem()'s skip rule.
    try:
        if problem.requires_khaos() and conductor.kubectl.is_emulated_cluster():
            result.skip_reason = (
                f"problem requires Khaos but cluster is emulated (kind/minikube/k3d) — skipped"
            )
            result.elapsed_s = time.monotonic() - started
            compute_verdict(result)
            return result
    except Exception as exc:
        logger.warning(f"[{problem_id}] requires_khaos/is_emulated_cluster probe failed: {exc}")

    # Best-effort pre-clean.
    try:
        conductor.fix_kubernetes()
    except Exception as exc:
        logger.warning(f"[{problem_id}] fix_kubernetes failed: {exc}")
    try:
        conductor.undeploy_app()
    except Exception as exc:
        logger.warning(f"[{problem_id}] pre-clean undeploy_app failed: {exc}")

    # --- DEPLOY ---------------------------------------------------------
    deploy_ok = _record_stage(result, "deploy", conductor.deploy_app)
    if not deploy_ok:
        _safe_teardown(conductor, problem, result)
        result.elapsed_s = time.monotonic() - started
        compute_verdict(result)
        return result

    # --- PRE PROBE ------------------------------------------------------
    if probe_enabled:
        try:
            result.pre_probe = sample_loadgen(problem.app, probe_duration_s, sleep_fn=sleep_fn)
        except Exception as exc:
            result.pre_probe = ProbeResult(error=f"{type(exc).__name__}: {exc}")

    # --- POD SNAPSHOT BEFORE -------------------------------------------
    try:
        pre_snap = snapshot_fn(conductor.kubectl)
    except Exception as exc:
        pre_snap = None
        logger.warning(f"[{problem_id}] pre-injection snapshot failed: {exc}")

    # --- INJECT ---------------------------------------------------------
    inject_ok = _record_stage(result, "inject", problem.inject_fault)
    if not inject_ok:
        _safe_teardown(conductor, problem, result)
        result.elapsed_s = time.monotonic() - started
        compute_verdict(result)
        return result

    # --- VERIFY ---------------------------------------------------------
    verify_ok = _record_stage(result, "verify", problem.verify_fault_applied)
    if not verify_ok:
        _safe_teardown(conductor, problem, result)
        result.elapsed_s = time.monotonic() - started
        compute_verdict(result)
        return result

    # --- POD SNAPSHOT AFTER --------------------------------------------
    if pre_snap is not None:
        try:
            post_snap = snapshot_fn(conductor.kubectl)
            result.pod_diff = compute_pod_diff(pre_snap, post_snap)
        except Exception as exc:
            logger.warning(f"[{problem_id}] post-injection snapshot failed: {exc}")

    # --- POST PROBE -----------------------------------------------------
    if probe_enabled:
        try:
            result.post_probe = sample_loadgen(problem.app, probe_duration_s, sleep_fn=sleep_fn)
        except Exception as exc:
            result.post_probe = ProbeResult(error=f"{type(exc).__name__}: {exc}")

    # --- RECOVER --------------------------------------------------------
    _record_stage(result, "recover", problem.recover_fault)

    # --- UNDEPLOY (best-effort) -----------------------------------------
    try:
        conductor.undeploy_app()
        result.stages.append(StageOutcome(stage="undeploy", ok=True, duration_s=0.0))
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        result.stages.append(StageOutcome(stage="undeploy", ok=False, duration_s=0.0, error=err))
        if result.error_stage is None:
            result.error_stage = "undeploy"
    try:
        if getattr(conductor, "_baseline_captured", False):
            conductor.cluster_state.reconcile_to_baseline()
    except Exception as exc:
        logger.warning(f"[{problem_id}] reconcile_to_baseline failed: {exc}")

    result.elapsed_s = time.monotonic() - started
    compute_verdict(result)
    return result


def _safe_teardown(conductor: Any, problem: Any, result: ProblemResult) -> None:
    """Best-effort recovery + undeploy after a stage failure."""
    try:
        problem.recover_fault()
        result.stages.append(StageOutcome(stage="recover", ok=True, duration_s=0.0))
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        result.stages.append(StageOutcome(stage="recover", ok=False, duration_s=0.0, error=err))
    try:
        conductor.undeploy_app()
        result.stages.append(StageOutcome(stage="undeploy", ok=True, duration_s=0.0))
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        result.stages.append(StageOutcome(stage="undeploy", ok=False, duration_s=0.0, error=err))


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------


def _asdict(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _asdict(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, tuple):
        return [_asdict(v) for v in obj]
    if isinstance(obj, list):
        return [_asdict(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _asdict(v) for k, v in obj.items()}
    return obj


def write_report(results: list[ProblemResult], path: Path | str) -> None:
    """Write an aggregated JSON report covering all problem results."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    counts = {v.value: 0 for v in Verdict}
    for r in results:
        counts[Verdict(r.verdict).value] += 1

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "total": len(results),
        "counts": counts,
        "problems": [_asdict(r) for r in results],
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)


def summarize_report(results: list[ProblemResult]) -> str:
    counts = {v.value: 0 for v in Verdict}
    failed_by_stage: dict[str, int] = {}
    for r in results:
        counts[Verdict(r.verdict).value] += 1
        if r.error_stage:
            failed_by_stage[r.error_stage] = failed_by_stage.get(r.error_stage, 0) + 1
    lines = [
        f"total={len(results)} "
        f"pass={counts['pass']} fail={counts['fail']} "
        f"yellow={counts['yellow']} skip={counts['skip']}"
    ]
    if failed_by_stage:
        by_stage = ", ".join(f"{k}={v}" for k, v in sorted(failed_by_stage.items()))
        lines.append(f"failed stages: {by_stage}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI + worker infrastructure
#
# The CLI glue below is imported lazily so that unit tests can load this
# module without pulling in sregym's Conductor, kind, or MCP stack.
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SREGym fault-injection stress test.")
    p.add_argument("--parallel", type=int, default=1, help="Number of worker processes (each owns a kind cluster).")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--problems", type=str, default=None,
                   help="Comma-separated problem IDs to run.")
    g.add_argument("--all", action="store_true", help="Run every registered problem ID.")
    g.add_argument("--tasklist", type=str, default=None,
                   help="Path to a tasklist.yml from which to read problem IDs.")
    p.add_argument("--filter", type=str, default=None,
                   help="Only run problem IDs whose name contains this substring.")
    p.add_argument("--max-per-worker", type=int, default=0,
                   help="Cap problems run per worker (0 = unlimited).")
    p.add_argument("--timeout-sec", type=float, default=0.0,
                   help="Per-problem timeout in seconds (0 = no timeout).")
    p.add_argument("--stop-on-first-failure", action="store_true",
                   help="Abort the run after the first FAIL verdict.")
    p.add_argument("--no-loadgen-probe", action="store_true",
                   help="Disable the pre/post wrk2 success-rate probe.")
    p.add_argument("--probe-duration-sec", type=float, default=30.0,
                   help="Per-probe loadgen sampling duration.")
    p.add_argument("--out-dir", type=str, default=None,
                   help="Output directory (default: bench/sregym/logs/stress/<timestamp>).")
    return p.parse_args(argv)


def _default_out_dir() -> Path:
    bench = Path(__file__).resolve().parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return bench / "logs" / "stress" / ts


def _resolve_user_path(p: str) -> Path:
    """Resolve a user-supplied path arg relative to the shell-invocation cwd.

    run_sregym_stress.sh cd's into bench/sregym before invoking us, so an
    argument like ``--tasklist some/rel/path`` must resolve against the
    original cwd (recorded in SREGYM_STRESS_INVOCATION_CWD), not bench/sregym.
    """
    pp = Path(p).expanduser()
    if pp.is_absolute():
        return pp.resolve()
    base = os.environ.get("SREGYM_STRESS_INVOCATION_CWD")
    if base:
        pp = Path(base) / pp
    return pp.resolve()


def _load_problem_ids(args: argparse.Namespace) -> list[str]:
    """Resolve the --problems / --all / --tasklist selection to a list of IDs."""
    from sregym.conductor.problems.registry import ProblemRegistry  # noqa: E402

    registry = ProblemRegistry()
    if args.problems:
        ids = [p.strip() for p in args.problems.split(",") if p.strip()]
    elif args.tasklist:
        tl_path = _resolve_user_path(args.tasklist)
        if not tl_path.exists():
            raise FileNotFoundError(f"--tasklist path does not exist: {tl_path}")
        ids = registry.get_problem_ids(tasklist_path=str(tl_path))
    elif args.all:
        ids = registry.get_problem_ids(all=True)
    else:
        ids = registry.get_problem_ids()
    if args.filter:
        needle = args.filter
        ids = [pid for pid in ids if needle in pid]
    return ids


def _worker_entry(args: argparse.Namespace, worker_id: int, queue: Any, out_dir: Path) -> None:
    """Worker process entry point. Creates its own kind cluster, runs problems."""
    # Heavy imports deferred so unit tests don't pay for them.
    from sregym import worker_infra
    from logger import init_logger  # type: ignore[import-not-found]

    os.environ["SREGYM_WORKER_ID"] = str(worker_id)
    os.environ["API_PORT"] = str(8000 + worker_id)
    os.environ["MCP_SERVER_PORT"] = str(9000 + worker_id)
    _bench = os.path.dirname(os.path.abspath(__file__))
    os.environ["SREGYM_EXP_ENV"] = os.path.join(_bench, "exp_env", f"exp_env_{worker_id}")

    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.environ["SREGYM_LOG_FILE"] = str(out_dir / f"sregym_{session_ts}_w{worker_id}.log")

    worker_log_path = out_dir / f"worker_{worker_id}.log"
    with open(worker_log_path, "w") as f:
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(f.fileno(), 1)
        os.dup2(f.fileno(), 2)

        init_logger()

        def _ignore(sig, _frame):
            logger.info(f"Worker {worker_id} received signal {sig} — ignoring")

        signal.signal(signal.SIGTERM, _ignore)
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, _ignore)

        cluster_name = ""
        results: list[ProblemResult] = []
        try:
            cluster_name, _ = worker_infra.create_worker_cluster(worker_id, str(out_dir))
            from sregym.conductor.conductor import Conductor  # noqa: E402
            conductor = Conductor()

            done = 0
            while True:
                if args.max_per_worker and done >= args.max_per_worker:
                    break
                try:
                    pid = queue.get(timeout=5.0)
                except Exception:
                    continue
                if pid is None:
                    break

                print(f"[w{worker_id}] START {pid}", flush=True)
                try:
                    r = run_stress_problem(
                        conductor,
                        pid,
                        worker_id=worker_id,
                        probe_enabled=not args.no_loadgen_probe,
                        probe_duration_s=args.probe_duration_sec,
                    )
                except Exception as exc:
                    r = ProblemResult(
                        problem_id=pid,
                        worker_id=worker_id,
                        verdict=Verdict.FAIL,
                        error_stage="driver",
                    )
                    r.stages.append(StageOutcome(
                        stage="driver",
                        ok=False,
                        duration_s=0.0,
                        error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                    ))
                results.append(r)
                done += 1
                print(f"[w{worker_id}] END   {pid} -> {r.verdict.value} "
                      f"elapsed={r.elapsed_s:.1f}s", flush=True)
                if args.stop_on_first_failure and r.verdict == Verdict.FAIL:
                    break
        finally:
            try:
                worker_infra.delete_worker_cluster(cluster_name)
            except Exception as exc:
                logger.warning(f"worker {worker_id} cluster teardown failed: {exc}")
            write_report(results, out_dir / f"worker_{worker_id}_report.json")


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    out_dir = _resolve_user_path(args.out_dir) if args.out_dir else _default_out_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    problem_ids = _load_problem_ids(args)
    if not problem_ids:
        print("No problems to run.", file=sys.stderr)
        return 1

    import multiprocessing

    # Write the selection for reproducibility.
    (out_dir / "problems.json").write_text(json.dumps(problem_ids, indent=2))
    print(f"Stress run: {len(problem_ids)} problems, {args.parallel} workers, out={out_dir}")

    queue: Any = multiprocessing.Queue()
    for pid in problem_ids:
        queue.put(pid)
    for _ in range(args.parallel):
        queue.put(None)  # one sentinel per worker

    procs: list[multiprocessing.Process] = []
    for i in range(args.parallel):
        p = multiprocessing.Process(target=_worker_entry, args=(args, i, queue, out_dir))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    all_results: list[ProblemResult] = []
    for i in range(args.parallel):
        part = out_dir / f"worker_{i}_report.json"
        if not part.exists():
            continue
        data = json.loads(part.read_text())
        for item in data.get("problems", []):
            # Rehydrate minimally for summary; downstream consumers read JSON.
            all_results.append(ProblemResult(
                problem_id=item["problem_id"],
                worker_id=item.get("worker_id", i),
                verdict=Verdict(item.get("verdict", "pass")),
                error_stage=item.get("error_stage"),
            ))

    write_report(all_results, out_dir / "report.json")
    print(summarize_report(all_results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
