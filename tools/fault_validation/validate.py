"""Validate a component-fault catalog key live: symptom, persistence, decoys fail, reference passes.

    uv run python tools/fault_validation/validate.py --key redis_replica_misconfig \
        --app social_network --seeds 1 2 3 [--namespace social-network] [--persist-s 60] [--out trials.jsonl]

Runs against an already-deployed app namespace (KUBECONFIG selects the cluster; restore_source
decoys need SREGYM_ACCEPTED_RENDER). For each seed it samples the key's draw, then runs one trial
per decoy and one for the reference, each on a freshly injected fault:

  inject -> symptom visible? -> wait --persist-s -> still visible? -> apply mitigation -> settle ->
  verdict (per-fault oracle AND namespace collateral walk AND entry-point traffic probe, the
  composite rule) -> recover

A decoy trial passes when the verdict FAILS; the reference trial, and one trial per
acceptable alternative repair for this target, pass when it PASSES. A final
``recovered`` trial checks the namespace is healthy again. One JSON line per trial on stdout
(and --out). Exit status 0 only if every trial passed.
"""

from __future__ import annotations

import argparse
import json
import random
import secrets
import sys
import time
from collections.abc import Callable, Iterator
from typing import Any

from sregym.conductor.problems.composition import catalog, grading

SETTLE_S = 300.0


def _probe(cluster, namespace: str, entry: grading.EntryPoint) -> dict[str, Any]:
    manifest = grading.probe_pod_manifest(f"validate-probe-{secrets.token_hex(4)}", namespace, entry)
    phase, logs = cluster.run_pod(manifest, timeout_s=grading.probe_deadline_s(entry) + 30)
    if phase.startswith("Error"):
        return {"ok": False, "detail": f"probe pod not run: {phase}"}
    result = grading.parse_probe_log(logs)
    return {"ok": result.ok, "detail": result.detail(), "failed": list(result.failed_paths)}


def _health(cluster, namespace: str, entry: grading.EntryPoint) -> dict[str, Any]:
    pods, error = cluster.list_pods(namespace)
    collateral_ok, reason = (False, error) if pods is None else grading.collateral_verdict(pods)
    probe = _probe(cluster, namespace, entry)
    return {"collateral_ok": collateral_ok, "collateral": reason, "traffic_ok": probe["ok"], "traffic": probe}


def _verdict(problem, cluster) -> dict[str, Any]:
    settle = cluster.wait_for_rollouts(problem.namespace, SETTLE_S)
    fault = problem.mitigation_oracle.evaluate()
    health = _health(cluster, problem.namespace, problem.binding.entry)
    success = grading.mitigation_success([bool(fault.get("success"))], health["collateral_ok"], health["traffic_ok"])
    return {"success": success, "settled": settle.settled, "fault": fault, **health}


def _symptom(problem, cluster) -> dict[str, Any]:
    """Visible at the entry points: the traffic probe fails, or the seeded reads changed or fail."""
    fault = problem.mitigation_oracle.evaluate()
    probe = _probe(cluster, problem.namespace, problem.binding.entry)
    visible = not probe["ok"] or fault.get("integrity_ok") is False
    return {"visible": visible, "traffic": probe, "fault": fault}


def run_trial(problem, cluster, name: str, mitigation: Callable[[], None], expect_pass: bool, persist_s: float):
    row: dict[str, Any] = {"trial": name, "expect": "PASS" if expect_pass else "FAIL"}
    t0 = time.monotonic()
    try:
        problem.inject_fault()
        row["symptom"] = _symptom(problem, cluster)
        time.sleep(persist_s)
        row["persisted"] = _symptom(problem, cluster)
        mitigation()
        row["verdict"] = _verdict(problem, cluster)
        got = row["verdict"]["success"]
        row["pass"] = bool(row["symptom"]["visible"] and row["persisted"]["visible"] and got == expect_pass)
    except Exception as e:  # a crashed trial is a failed trial, recorded
        row["error"] = f"{type(e).__name__}: {e}"[:800]
        row["pass"] = False
    finally:
        try:
            problem.recover_fault()
        except Exception as e:
            row["recover_error"] = f"{type(e).__name__}: {e}"[:400]
    row["duration_s"] = round(time.monotonic() - t0, 1)
    return row


def validate_seed(key: str, app: str, seed: int, namespace: str, persist_s: float, cluster) -> Iterator[dict[str, Any]]:
    """Yield each trial row as soon as it finishes."""
    entry = {e.key: e for e in catalog.entries_for(app)}[key]
    draw = entry.sample(app, random.Random(seed))
    problem = entry.build(draw)
    problem.namespace = namespace
    base = {"key": key, "app": app, "seed": seed, "params": draw.param_dict()}
    baseline = {**base, "trial": "baseline", **_health(cluster, namespace, problem.binding.entry)}
    baseline["pass"] = baseline["collateral_ok"] and baseline["traffic_ok"]
    yield baseline
    if not baseline["pass"]:
        return
    for name, decoy in sorted(problem.decoy_mitigations().items()):
        yield {**base, **run_trial(problem, cluster, f"decoy:{name}", decoy, False, persist_s)}
    yield {**base, **run_trial(problem, cluster, "reference", problem.reference_mitigation, True, persist_s)}
    for name, fix in sorted(problem.acceptable_mitigations().items()):
        yield {**base, **run_trial(problem, cluster, f"acceptable:{name}", fix, True, persist_s)}
    cluster.wait_for_rollouts(namespace, SETTLE_S)
    final = {**base, "trial": "recovered", **_health(cluster, namespace, problem.binding.entry)}
    final["pass"] = final["collateral_ok"] and final["traffic_ok"]
    yield final


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key", required=True)
    ap.add_argument("--app", default="social_network")
    ap.add_argument("--seeds", type=int, nargs="+", default=[1, 2, 3])
    ap.add_argument("--namespace", default=None, help="default: the app binding's namespace")
    ap.add_argument("--persist-s", type=float, default=60.0)
    ap.add_argument("--out", default=None, help="also append JSON lines here")
    args = ap.parse_args(argv)

    from sregym.conductor.problems.colocated_composite import KubeClusterIO
    from sregym.conductor.problems.composition.app_bindings import BINDINGS

    namespace = args.namespace or BINDINGS[args.app].namespace
    cluster = KubeClusterIO()
    all_ok = True
    for seed in args.seeds:
        for row in validate_seed(args.key, args.app, seed, namespace, args.persist_s, cluster):
            line = json.dumps(row, default=str)
            print(line, flush=True)
            if args.out:
                with open(args.out, "a") as f:
                    f.write(line + "\n")
            all_ok = all_ok and row["pass"]
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
