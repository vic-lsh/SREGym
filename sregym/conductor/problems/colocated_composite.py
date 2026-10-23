"""Co-located composite fault problem.

Composes several single-fault ``Problem``s that all target the **same
application and namespace**, per robust-skills' ``third_party/sregym/docs/composite-problem-design.md``
(the § references below); ported from robust-skills, where it ran the vanilla Claude Code
multi-fault stream. Generated ``mf__`` plans build it (``composition.build_composite``).

Unlike ``MultipleIndependentFailures`` (which wraps N apps in a ``CompositeApp``
and thereby hits the multi-namespace defects catalogued in the design doc §3),
this class keeps every fault in one app / one namespace / one deploy, so none of
those defects apply.

Validity (``composite_valid``): faults are injected stable-sorted by tier (pod
restarters first, admission blockers last); an injection error is recorded, not
raised. Around every injection the namespace's specs are snapshotted
(``composition/liveness.py``). After the last injection each fault must still be
live: one of its spec changes still holds or, for a runtime-only fault with no
spec change, its own mitigation oracle still fails.

Grading is **per-fault** (design doc §5):

* Mitigation: each sub-problem's own mitigation oracle gives an attributable
  verdict, plus **one** namespace-wide collateral check and **one** end-to-end
  traffic probe (``success = all(per-fault verdicts) AND collateral_ok AND
  traffic_ok``, ``composition/grading.py``).
* Diagnosis: **one judge call per sub-fault** against the same submitted text
  (§5.5), run concurrently so N=8 fits the evaluation time cap.

Both oracles emit flat per-fault keys (``fault_0_verdict``, ``faults_fixed``,
``faults_diagnosed``, ``fault_0_live`` …) which the CSV writer flattens into
columns with no harness change (§5.6).
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import json
import secrets
import threading
import time
from typing import Any, Protocol

from sregym.conductor.constants import MAX_DIAGNOSIS_CANDIDATES
from sregym.conductor.oracles import rollouts
from sregym.conductor.oracles.base import Oracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composition import grading, liveness, recovery
from sregym.conductor.problems.composition.spec import FaultDraw, Footprint, Tier
from sregym.utils.decorators import mark_fault_injected


class ClusterIO(Protocol):
    """Everything the composite reads from or runs on the cluster (its I/O seam).

    ``kubectl`` is the object handed to every sub-problem as ``problem.kubectl``.
    Every method reports failures in its return value; none raises.
    """

    kubectl: Any

    def read_specs(self, namespace: str) -> tuple[dict[str, list[dict]], dict[str, str]]:
        """``(kind -> [object JSON], kind -> error)`` for every kind in ``liveness.ALL_KINDS``,
        one list call per kind. A kind whose read failed is absent from the first map."""
        ...

    def list_pods(self, namespace: str) -> tuple[list[dict] | None, str]:
        """``(pod JSON list, "")``, or ``(None, error)``."""
        ...

    def run_pod(self, manifest: dict, timeout_s: float) -> tuple[str, str]:
        """Create the pod, wait until it finishes or ``timeout_s`` passes, delete it on every
        exit path, and return ``(phase, logs)``. ``phase`` starts with ``"Error"`` if it was
        not created."""
        ...

    def wait_for_rollouts(self, namespace: str, timeout_s: float) -> rollouts.Settle:
        """Wait until nothing in ``namespace`` is still rolling out (``oracles/rollouts.py``:
        Deployments, StatefulSets, Terminating pods) or ``timeout_s`` passes. Returns at once
        when already settled."""
        ...


class KubeClusterIO:
    """``ClusterIO`` over the harness's real (un-proxied) kubeconfig."""

    # kind -> (API group, list method, namespaced)
    _LISTERS = {
        "Deployment": ("apps", "list_namespaced_deployment", True),
        "StatefulSet": ("apps", "list_namespaced_stateful_set", True),
        "DaemonSet": ("apps", "list_namespaced_daemon_set", True),
        "Service": ("core", "list_namespaced_service", True),
        "ConfigMap": ("core", "list_namespaced_config_map", True),
        "Secret": ("core", "list_namespaced_secret", True),
        "NetworkPolicy": ("networking", "list_namespaced_network_policy", True),
        "PersistentVolumeClaim": ("core", "list_namespaced_persistent_volume_claim", True),
        "ServiceAccount": ("core", "list_namespaced_service_account", True),
        "Role": ("rbac", "list_namespaced_role", True),
        "RoleBinding": ("rbac", "list_namespaced_role_binding", True),
        "ResourceQuota": ("core", "list_namespaced_resource_quota", True),
        "LimitRange": ("core", "list_namespaced_limit_range", True),
        "ValidatingWebhookConfiguration": ("admission", "list_validating_webhook_configuration", False),
        "MutatingWebhookConfiguration": ("admission", "list_mutating_webhook_configuration", False),
    }

    def __init__(self, kubectl=None, apis: dict[str, Any] | None = None):
        """``apis`` maps "core", "apps", "networking", "rbac", "admission" to Kubernetes API
        clients; it defaults to the real clients (tests pass fakes)."""
        if kubectl is None:
            from sregym.service.kubectl import KubeCtl

            # KubeCtl loads the harness kubeconfig; the agent's proxy kubeconfig is a separate file.
            kubectl = KubeCtl()
        if apis is None:
            from kubernetes import client

            apis = {
                "core": client.CoreV1Api(),
                "apps": client.AppsV1Api(),
                "networking": client.NetworkingV1Api(),
                "rbac": client.RbacAuthorizationV1Api(),
                "admission": client.AdmissionregistrationV1Api(),
            }
        self.kubectl = kubectl
        self._apis = apis

    @staticmethod
    def _items(response) -> list[dict]:
        return json.loads(response.data).get("items") or []

    def read_specs(self, namespace):
        raw, errors = {}, {}
        for kind in liveness.ALL_KINDS:
            try:
                if kind == liveness.NAMESPACE_KIND:
                    resp = self._apis["core"].read_namespace(namespace, _preload_content=False)
                    raw[kind] = [json.loads(resp.data)]
                    continue
                group, method, namespaced = self._LISTERS[kind]
                lister = getattr(self._apis[group], method)
                resp = lister(namespace, _preload_content=False) if namespaced else lister(_preload_content=False)
                raw[kind] = self._items(resp)
            except Exception as e:  # an unreadable kind is excluded from diffs, never guessed
                errors[kind] = f"{type(e).__name__}: {str(e)[:200]}"
        return raw, errors

    def list_pods(self, namespace):
        try:
            resp = self._apis["core"].list_namespaced_pod(namespace, _preload_content=False)
            return self._items(resp), ""
        except Exception as e:
            return None, f"list_pods failed: {type(e).__name__}: {e}"

    def wait_for_rollouts(self, namespace, timeout_s):
        read = rollouts.namespace_reader(self._apis["core"], self._apis["apps"], namespace)
        return rollouts.wait_for_rollouts(read, timeout_s)

    def run_pod(self, manifest, timeout_s):
        core = self._apis["core"]
        name, namespace = manifest["metadata"]["name"], manifest["metadata"]["namespace"]
        try:
            try:
                core.create_namespaced_pod(namespace, manifest)
            except Exception as e:
                # The delete below still runs: a create that timed out may have succeeded.
                return f"Error: {type(e).__name__}: {str(e)[:300]}", ""
            phase = "Pending"
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                phase = core.read_namespaced_pod(name, namespace).status.phase
                if phase in ("Succeeded", "Failed"):
                    break
                time.sleep(2)
            try:
                logs = core.read_namespaced_pod_log(name, namespace)
            except Exception:
                logs = ""
            return phase, logs
        except Exception as e:
            return f"Error: {type(e).__name__}: {str(e)[:300]}", ""
        finally:
            with contextlib.suppress(Exception):
                core.delete_namespaced_pod(name, namespace, grace_period_seconds=0)


def _footprint_set(fp) -> tuple[Footprint, ...]:
    """Parse one sub-problem's footprint declaration.

    Accepts a legacy ``(kind, name, field_path)`` tuple (``"<object>"`` = whole object)
    or a non-empty sequence of ``Footprint``s; anything else raises ValueError.
    """
    if isinstance(fp, Footprint):
        return (fp,)
    if isinstance(fp, (tuple, list)) and fp and all(isinstance(f, Footprint) for f in fp):
        return tuple(fp)
    if isinstance(fp, (tuple, list)) and len(fp) == 3 and all(isinstance(x, str) for x in fp):
        kind, name, field = fp
        return (Footprint(kind, name, None if field in ("", "<object>") else field),)
    raise ValueError(f"unrecognized footprint {fp!r}: expected (kind, name, field) or tuple[Footprint, ...]")


def _parse_config_text(text: str):
    """ConfigMap data parser for liveness: YAML, a superset of the JSON documents seen in data."""
    import yaml

    return yaml.safe_load(text)


def _run_with_limit(fn, limit_s: float) -> str:
    """Run ``fn`` in a daemon thread; return "" on success, else its error or a timeout note."""
    outcome: dict[str, str] = {}

    def target():
        try:
            fn()
            outcome["error"] = ""
        except BaseException as e:  # noqa: BLE001 - recovery must never propagate, not even SystemExit
            outcome["error"] = f"{type(e).__name__}: {e}"

    t = threading.Thread(target=target, daemon=True, name="composite-recover")
    t.start()
    t.join(limit_s)
    if t.is_alive():
        return f"timeout after {limit_s:g}s"
    return outcome.get("error", "")


class _TimedOut:
    """Outcome of a call that had not returned when the deadline passed."""

    def __init__(self, started: bool):
        self.started = started


def _run_bounded(calls, max_workers: int, deadline: float) -> list:
    """Run ``calls`` on at most ``max_workers`` daemon threads, in list order, until ``deadline``
    (``time.monotonic()``). Returns one outcome per call, in input order: its return value, the
    ``BaseException`` it raised, or ``_TimedOut``. No call is started after the deadline. A call
    still running at the deadline keeps running in its daemon thread; its result is discarded.
    With ``max_workers=1`` the calls run strictly one after another.
    """
    n = len(calls)
    outcomes: list = [None] * n
    finished = [False] * n
    started = [False] * n
    cond = threading.Condition()
    next_index = 0

    def worker():
        nonlocal next_index
        while True:
            with cond:
                if next_index >= n or time.monotonic() >= deadline:
                    return
                i = next_index
                next_index += 1
                started[i] = True
            try:
                outcome = calls[i]()
            except BaseException as e:  # noqa: BLE001 - a crash is this call's outcome, never the batch's
                outcome = e
            with cond:
                outcomes[i] = outcome
                finished[i] = True
                cond.notify_all()

    for k in range(max(1, min(max_workers, n))):
        threading.Thread(target=worker, daemon=True, name=f"composite-oracle-{k}").start()
    with cond:
        while not all(finished):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            cond.wait(remaining)
        return [outcomes[i] if finished[i] else _TimedOut(started[i]) for i in range(n)]


class ColocatedComposite(Problem):
    """N faults injected into one application namespace, graded per-fault.

    Args:
        problems: the sub-problems to compose. All must resolve to the same namespace.
        footprints: optional, one entry per sub-problem: a legacy
            ``(kind, name, field_path)`` tuple or a ``tuple[Footprint, ...]``. Two
            sub-problems with overlapping footprints are rejected (§8 gate 2). When
            omitted the gate is skipped with a warning.
        tiers: optional ``Tier`` per sub-problem (default ``ORDINARY``). Injection is
            stable-sorted by tier; equal tiers keep the declared order.
        draws: optional ``FaultDraw`` per sub-problem: the fault's identity for reporting
            (``FaultDraw.to_json()``) and the default for ``footprints`` and ``tiers``. When
            both a draw and an explicit footprint or tier are given they must agree.
        cluster: the ``ClusterIO`` seam; defaults to ``KubeClusterIO()``.

    Result keys are indexed by declared position ``i``, not by injection order.
    """

    # Per-fault recovery limit. A recovery that overruns keeps running in its daemon
    # thread; the conductor deletes the namespace afterwards anyway.
    recover_timeout_s = 240.0
    # Settle time before the final liveness snapshot, so controllers finish reacting.
    liveness_settle_s = 5.0
    # Pause between injections (kept from the v1 composite).
    inject_pause_s = 1.0

    def __init__(
        self,
        problems,
        footprints=None,
        tiers=None,
        draws=None,
        *,
        identity: dict[str, Any] | None = None,
        cluster: ClusterIO | None = None,
    ):
        if len(problems) < 1:
            raise ValueError("ColocatedComposite requires at least one sub-problem")
        # N == 1 is a degenerate composite kept only as the Experiment-B focus-drift
        # baseline, so it runs through the same per-fault grading instrument as N > 1.
        self.problems = list(problems)
        n = len(self.problems)
        for label, seq in (("footprints", footprints), ("tiers", tiers), ("draws", draws)):
            if seq is not None and len(seq) != n:
                raise ValueError(f"{label} must have exactly one entry per sub-problem ({len(seq)} != {n})")
        if draws is not None:
            for i, draw in enumerate(draws):
                if not isinstance(draw, FaultDraw):
                    raise TypeError(f"draws[{i}] must be a FaultDraw, got {type(draw).__name__}")
                json.dumps(draw.to_json())  # non-JSON params fail at build time, not in grading
            footprints = footprints if footprints is not None else [d.footprints for d in draws]
            tiers = tiers if tiers is not None else [d.tier for d in draws]

        # Caller-supplied identity (e.g. the plan's base N and take-K): static, JSON-safe report keys.
        self.identity: dict[str, Any] = dict(identity or {})
        json.dumps(self.identity)

        # §7 rule 1: one canonical app instance shared by all sub-problems.
        super().__init__(app=self.problems[0].app)

        self.cluster: ClusterIO = cluster if cluster is not None else KubeClusterIO()
        # §7 rule 3: MitigationOracle and friends read problem.kubectl.
        self.kubectl = self.cluster.kubectl

        # §3 / §8 gate 1: same app, same namespace.
        for p in self.problems:
            if getattr(p, "namespace", None) != self.namespace:
                raise ValueError(
                    f"ColocatedComposite requires all sub-problems in namespace "
                    f"'{self.namespace}', got '{getattr(p, 'namespace', None)}' "
                    f"for {type(p).__name__}"
                )

        # Footprints name the namespace as declared now; the conductor may add a session suffix later.
        self._declared_namespace = self.namespace

        # §7 rules 2 & 4: rebind every sub-problem to the canonical app and shared kubectl.
        for p in self.problems:
            p.app = self.app
            p.namespace = self.namespace
            p.kubectl = self.kubectl
        # Faults share the namespace: each sub-oracle judges only its own fault's evidence.
        for oracle in self.sub_oracles():
            oracle.scope_to_fault()

        # §8 gate 2: footprints pairwise disjoint (only when declared).
        if footprints is None:
            self.footprints = None
            print("[ColocatedComposite] WARNING: no footprints declared; skipping the disjointness gate (§8 gate 2).")
        else:
            self.footprints = [_footprint_set(fp) for fp in footprints]
            self._assert_disjoint_footprints()

        self.tiers = [Tier.ORDINARY if tiers is None else Tier(tiers[i]) for i in range(n)]
        self.draws: list[FaultDraw | None] = list(draws) if draws is not None else [None] * n
        for i, draw in enumerate(self.draws):
            if draw is None:
                continue
            if tuple(self.footprints[i]) != draw.footprints or self.tiers[i] != draw.tier:
                raise ValueError(f"fault {i} ({draw.key}): explicit footprints/tier disagree with its FaultDraw")
        self.injection_order = sorted(range(n), key=lambda i: self.tiers[i])  # stable

        # Safety violations are recorded, never flip success (Conductor._apply_safety_veto).
        self.safety_report_only = True

        self._requires_khaos = any(p.requires_khaos() for p in self.problems)
        self.root_cause = self._build_root_cause()
        self.validity: dict[str, Any] = {}
        self.recovery: dict[str, str] = {}

        self.mitigation_oracle = CompositeMitigationOracle(self)
        self.diagnosis_oracle = CompositeDiagnosisOracle(self)

    def requires_khaos(self) -> bool:
        return self._requires_khaos

    def _assert_disjoint_footprints(self):
        for i in range(len(self.problems)):
            for j in range(i + 1, len(self.problems)):
                for a in self.footprints[i]:
                    for b in self.footprints[j]:
                        if a.overlaps(b):
                            raise ValueError(
                                f"Footprint collision between fault {i} ({type(self.problems[i]).__name__}) "
                                f"and fault {j} ({type(self.problems[j]).__name__}): {a} overlaps {b}"
                            )

    def _build_root_cause(self):
        sections = []
        for idx, p in enumerate(self.problems, start=1):
            rc = (getattr(p, "root_cause", None) or "").strip()
            if rc:
                sections.append(f"Fault {idx} ({type(p).__name__}):\n{rc}")
        header = (
            "This scenario contains multiple independent faults in the same "
            "application namespace. Each fault and its symptoms are listed below."
        )
        return header + "\n\n" + "\n\n".join(sections) if sections else header

    # --- reporting ------------------------------------------------------------------

    def fault_identity(self, i: int) -> dict[str, Any]:
        """Key, params and tier of fault ``i``: from ``FaultDraw.to_json()`` when drawn, else
        the class name, no params and the declared tier."""
        draw = self.draws[i]
        if draw is not None:
            return draw.to_json()
        return {"key": type(self.problems[i]).__name__, "params": {}, "tier": self.tiers[i].name}

    def report_fields(self) -> dict[str, Any]:
        """Identity and validity keys merged into every stage result."""
        fields: dict[str, Any] = {
            "composite_n": len(self.problems),
            "composite_injection_order": json.dumps(self.injection_order),
        }
        for i in range(len(self.problems)):
            identity = self.fault_identity(i)
            fields[f"fault_{i}_key"] = identity["key"]
            fields[f"fault_{i}_params"] = json.dumps(identity["params"], sort_keys=True)
            fields[f"fault_{i}_tier"] = identity["tier"]
        fields.update(self.identity)
        fields.update(self.validity)
        fields.setdefault("composite_valid", False)  # not injected yet: not valid
        return fields

    # --- workload handles (audit: a sub-oracle holds a never-deployed app's wrk) ------

    def sub_oracles(self) -> list[Oracle]:
        """Every sub-problem's mitigation oracle and the oracles nested in it, each once."""
        seen: set[int] = set()
        found: list[Oracle] = []

        def visit(obj):
            if isinstance(obj, (list, tuple, set)):
                for o in obj:
                    visit(o)
                return
            if isinstance(obj, dict):
                for o in obj.values():
                    visit(o)
                return
            if not isinstance(obj, Oracle) or id(obj) in seen:
                return
            seen.add(id(obj))
            found.append(obj)
            for attr, value in vars(obj).items():
                if attr != "problem":
                    visit(value)

        for p in self.problems:
            visit(getattr(p, "mitigation_oracle", None))
        return found

    def rebind_workload_handles(self) -> int:
        """Point every sub-oracle's ``wrk`` at the canonical app's workload manager.

        Sub-problems build ``WorkloadOracle(wrk_manager=self.app.wrk)`` against their own
        app instance, which the conductor never deploys or starts. Covers each sub-problem's
        mitigation oracle and the oracles nested in it. Returns the number of rebinds.
        """
        canonical = getattr(self.app, "wrk", None)
        if canonical is None:
            return 0
        count = 0
        for oracle in self.sub_oracles():
            if hasattr(oracle, "wrk") and oracle.wrk is not canonical:
                oracle.wrk = canonical
                count += 1
        return count

    @property
    def evaluation_timeout_s(self) -> float:
        """Bound on either stage's evaluation, derived from the faults drawn
        (``CompositeMitigationOracle.time_bound_s``) and, read before any submission, the judges'
        deadline for the most candidates a submission may hold; never below the single-problem default."""
        return max(
            Problem.evaluation_timeout_s,
            self.mitigation_oracle.time_bound_s(),
            self.diagnosis_oracle.deadline_s() + 30.0,
        )

    # --- injection and liveness -----------------------------------------------------

    def _snapshot(self) -> tuple[liveness.Snapshot | None, str]:
        try:
            raw, errors = self.cluster.read_specs(self.namespace)
            snap = liveness.build_snapshot(raw, _parse_config_text)
            return snap, "; ".join(f"{k}: {v}" for k, v in sorted(errors.items()))
        except Exception as e:
            return None, f"{type(e).__name__}: {e}"

    @staticmethod
    def _oracle_success(problem) -> tuple[bool | None, str]:
        oracle = getattr(problem, "mitigation_oracle", None)
        if oracle is None:
            return None, "no spec change and no mitigation_oracle"
        try:
            return bool((oracle.evaluate() or {}).get("success", False)), ""
        except Exception as e:
            return None, f"no spec change and the mitigation oracle raised {type(e).__name__}: {e}"

    @mark_fault_injected
    def inject_fault(self):
        """Inject every fault in tier order, then judge liveness; retry the failed ones once.

        A fault whose injection raised or that is not live after the first pass is injected
        once more (in the same order), and every fault's liveness is judged again.
        ``composite_injection_retried`` lists the faults retried. Never raises: a run still
        invalid after the retry is reported by ``injection_failure``.
        """
        n = len(self.problems)
        print(f"== Composite Fault Injection ({n} faults, order {self.injection_order}) ==")
        self.rebind_workload_handles()
        changes: dict[int, tuple[liveness.Change, ...] | None] = {}
        inject_errors: dict[int, str] = {}
        snapshot_errors: list[str] = []
        validity = self._inject_and_judge(self.injection_order, changes, inject_errors, snapshot_errors)
        retry = [i for i in self.injection_order if i in inject_errors or not validity[f"fault_{i}_live"]]
        if retry:
            print(f"  retrying injection of faults {retry}")
            validity = self._inject_and_judge(retry, changes, inject_errors, snapshot_errors)
            validity["composite_injection_retried"] = json.dumps(retry)
        self.validity = validity
        print(f"  composite_valid={validity['composite_valid']}")

    def _inject_and_judge(
        self,
        order: list[int],
        changes: dict[int, tuple[liveness.Change, ...] | None],
        inject_errors: dict[int, str],
        snapshot_errors: list[str],
    ) -> dict[str, Any]:
        """Inject the faults in *order*, updating *changes*, *inject_errors* and *snapshot_errors*
        in place, then judge every fault's liveness; returns the validity fields.

        A re-injected fault keeps its earlier spec changes when re-injecting changed nothing
        (they already held); its earlier error is replaced by the new outcome.
        """
        validity: dict[str, Any] = {}
        before, err = self._snapshot()
        if err:
            snapshot_errors.append(err)
        for i in order:
            p = self.problems[i]
            print(f"  injecting fault_{i} {type(p).__name__} (tier {self.tiers[i].name}) | ns={p.namespace}")
            inject_errors.pop(i, None)
            try:
                p.inject_fault()
            except Exception as e:  # recorded: the run becomes invalid, not crashed
                inject_errors[i] = f"{type(e).__name__}: {e}"[:500]
                print(f"  ❌ fault_{i} injection failed: {inject_errors[i]}")
            time.sleep(self.inject_pause_s)
            after, err = self._snapshot()
            if err:
                snapshot_errors.append(err)
            diff = liveness.diff(before, after) if before is not None and after is not None else None
            if diff or i not in changes:
                changes[i] = diff
            before = after

        time.sleep(self.liveness_settle_s)
        final, err = self._snapshot()
        if err:
            snapshot_errors.append(err)
        if snapshot_errors:
            validity["composite_snapshot_errors"] = "; ".join(sorted(set(snapshot_errors)))[:500]

        all_live = True
        for i, p in enumerate(self.problems):
            fault_changes = changes.get(i)
            verdict = liveness.spec_verdict(fault_changes, final) if final is not None else None
            if verdict is None:
                success, oracle_error = self._oracle_success(p)
                verdict = liveness.oracle_verdict(success, oracle_error)
            validity[f"fault_{i}_live"] = verdict.live
            validity[f"fault_{i}_live_basis"] = verdict.basis
            validity[f"fault_{i}_live_detail"] = verdict.detail[:300]
            validity[f"fault_{i}_spec_changes"] = len(fault_changes) if fault_changes is not None else -1
            if i in inject_errors:
                validity[f"fault_{i}_inject_error"] = inject_errors[i]
            all_live = all_live and verdict.live
            print(f"  fault_{i} live={verdict.live} ({verdict.basis}: {verdict.detail})")

        validity["composite_valid"] = all_live and not inject_errors
        return validity

    def injection_failure(self) -> dict[str, Any] | None:
        """None once every fault was injected and is live; else the identity and validity fields."""
        if self.validity.get("composite_valid") is True:
            return None
        return self.report_fields()

    def recovery_decision(self, i: int) -> recovery.Decision:
        """Whether fault ``i`` needs its own ``recover_fault`` (``composition/recovery.py``)."""
        return recovery.decide(
            self.draws[i],
            {self._declared_namespace, self.namespace},
            host_level=self.problems[i].requires_khaos(),
        )

    @mark_fault_injected
    def recover_fault(self):
        """Best-effort recovery in reverse injection order, each under a time limit; never raises.

        The conductor then deletes the app namespace and reconciles cluster state to baseline,
        so a fault whose writes are all namespace-scoped is skipped. Per fault, ``self.recovery``
        records ``fault_i_recovery`` (recovered | skipped), ``fault_i_recovery_reason`` and, when
        recovered, ``fault_i_recover_error`` ("" on success).
        """
        print(f"== Composite Fault Recovery ({len(self.problems)} faults) ==")
        report: dict[str, str] = {}
        for i in reversed(self.injection_order):
            p = self.problems[i]
            decision = self.recovery_decision(i)
            report[f"fault_{i}_recovery_reason"] = decision.reason
            if not decision.recover:
                report[f"fault_{i}_recovery"] = "skipped"
                print(f"  skipping fault_{i} {type(p).__name__}: {decision.reason}")
                continue
            print(f"  recovering fault_{i} {type(p).__name__} | ns={p.namespace} ({decision.reason})")
            error = _run_with_limit(p.recover_fault, self.recover_timeout_s)
            report[f"fault_{i}_recovery"] = "recovered"
            report[f"fault_{i}_recover_error"] = error
            if error:
                print(f"  ⚠️ fault_{i} recovery failed: {error}")
            time.sleep(self.inject_pause_s)
        self.recovery = report


class CompositeMitigationOracle(Oracle):
    """Per-fault verdicts (§5.1/§5.4), one collateral check (§5.3), one traffic probe.

    ``success = all(per-fault verdicts) AND collateral_ok AND traffic_ok``.

    Grading runs in four steps, so N faults cost about one oracle's time, not N:

    1. One settle wait for the namespace (``oracles/rollouts.py``: rollouts finished, no pod
       still Terminating); every sub-oracle then skips its own (``Oracle.namespace_settled``).
       In an unfixed composite the namespace never settles, so N per-oracle waits on that
       same condition would cost N x the bound.
    2. The ``concurrent_safe`` sub-oracles concurrently, at most ``max_workers`` at a time.
    3. The others one at a time (they create or delete pods, or share a workload stream).
    4. The collateral check, then the traffic probe (after it, so the probe pod is never
       in the collateral walk, and after every sub-oracle, so no pod-health walk sees it).

    Steps 2 and 3 share one deadline (``deadline_s``). A sub-oracle that has not returned by
    then is a per-fault failure with ``fault_i_timed_out``; as is one that raised. A
    sub-oracle result carrying ``infra_error`` (it could not judge its fault, e.g. the
    metrics API was down) is recorded as ``fault_i_infra_error``, and as the composite's
    ``infra_error`` only when that fault alone could still flip the outcome (every other
    fault fixed, collateral and traffic passing).
    """

    importance = 1.0
    max_workers = 8
    # Bounds the composite's own settle wait; the same window each sub-oracle gave itself.
    settle_timeout_s = rollouts.ROLLOUT_SETTLE_S
    # Collateral read plus the probe pod's create, delete and log calls.
    api_slack_s = 30.0

    def __init__(self, problem):
        super().__init__(problem)
        self.sub_problems = problem.problems

    def _batches(self) -> tuple[list[int], list[int]]:
        """Declared indices of the faults whose oracle may run concurrently, and of the rest."""
        concurrent, serial = [], []
        for i, p in enumerate(self.sub_problems):
            sub = getattr(p, "mitigation_oracle", None)
            (concurrent if sub is None or sub.concurrent_safe else serial).append(i)
        return concurrent, serial

    def _budget(self, i: int) -> float:
        sub = getattr(self.sub_problems[i], "mitigation_oracle", None)
        return sub.evaluation_budget_s if sub is not None else 0.0

    def deadline_s(self) -> float:
        """Shared deadline of steps 2 and 3: the slowest concurrent budget once per wave of
        ``max_workers``, plus every serial oracle's budget."""
        concurrent, serial = self._batches()
        waves = -(-len(concurrent) // self.max_workers)
        slowest = max((self._budget(i) for i in concurrent), default=0.0)
        return waves * slowest + sum(self._budget(i) for i in serial)

    def time_bound_s(self) -> float:
        """Upper bound on ``evaluate()``: settle wait, sub-oracle deadline, traffic probe, API slack."""
        entry = grading.ENTRY_POINTS.get(getattr(self.problem.app, "name", None))
        probe_s = grading.probe_deadline_s(entry) + 30 if entry is not None else 0.0
        return self.settle_timeout_s + self.deadline_s() + probe_s + self.api_slack_s

    def _evaluate_one(self, i: int) -> dict:
        sub = getattr(self.sub_problems[i], "mitigation_oracle", None)
        if sub is None:
            return {"success": False, "error": "no mitigation_oracle"}
        return sub.evaluate() or {}

    def _settle_once(self) -> rollouts.Settle:
        settle = self.problem.cluster.wait_for_rollouts(self.problem.namespace, self.settle_timeout_s)
        for oracle in self.problem.sub_oracles():
            oracle.namespace_settled = True
        return settle

    def evaluate(self) -> dict:
        print("== Composite Mitigation Evaluation ==")
        self.problem.rebind_workload_handles()
        results: dict[str, Any] = {"oracles": []}

        start = time.monotonic()
        settle = self._settle_once()
        results["composite_settled"] = settle.settled
        results["composite_settle_s"] = round(settle.waited_s, 1)
        if not settle.settled:
            results["composite_settle_reason"] = settle.reason
        print(
            f"  settle: {'✅ namespace settled' if settle.settled else '⚠️ ' + settle.reason} ({settle.waited_s:.0f}s)"
        )

        concurrent, serial = self._batches()
        deadline_s = self.deadline_s()
        deadline = time.monotonic() + deadline_s
        outcomes: dict[int, Any] = {}
        calls = [lambda i=i: self._evaluate_one(i) for i in concurrent]
        outcomes.update(zip(concurrent, _run_bounded(calls, self.max_workers, deadline), strict=True))
        calls = [lambda i=i: self._evaluate_one(i) for i in serial]
        outcomes.update(zip(serial, _run_bounded(calls, 1, deadline), strict=True))

        verdicts, infra_errors = [], []
        for i, p in enumerate(self.sub_problems):
            name = type(p).__name__
            outcome = outcomes[i]
            timed_out = isinstance(outcome, _TimedOut)
            if timed_out:
                what = "did not finish" if outcome.started else "was not started"
                detail = {"success": False, "error": f"sub-oracle {what} within the {deadline_s:g}s grading deadline"}
            elif isinstance(outcome, BaseException):  # a sub-oracle crash is a per-fault failure
                detail = {"success": False, "error": f"{type(outcome).__name__}: {outcome}"}
            else:
                detail = dict(outcome)
            ok = bool(detail.get("success", False))
            verdicts.append(ok)
            results[f"fault_{i}_class"] = name
            results[f"fault_{i}_verdict"] = ok
            results[f"fault_{i}_timed_out"] = timed_out
            if detail.get("infra_error"):
                infra_errors.append(detail["infra_error"])
                results[f"fault_{i}_infra_error"] = detail["infra_error"]
            detail["name"] = f"fault_{i}:{name}"
            results["oracles"].append(detail)
            print(f"  fault_{i} ({name}): {'✅ fixed' if ok else '❌ not fixed'}{' (timed out)' if timed_out else ''}")

        results["faults_total"] = len(verdicts)
        results["faults_fixed"] = sum(verdicts)

        collateral_ok, reason = self._collateral()
        results["collateral_ok"] = collateral_ok
        if not collateral_ok:
            results["collateral_reason"] = reason
        print(f"  collateral: {'✅ namespace healthy' if collateral_ok else '❌ ' + reason}")

        # After the collateral walk, so the probe pod never shows up in it.
        traffic_ok, traffic_detail = self._traffic()
        results["traffic_ok"] = traffic_ok
        results["traffic_detail"] = traffic_detail
        print(f"  traffic: {'✅' if traffic_ok else '❌'} {traffic_detail}")

        success = grading.mitigation_success(verdicts, collateral_ok, traffic_ok)
        # A fault its oracle could not judge makes the row ungradeable only if judging it could
        # still flip the outcome: every judged fault fixed, collateral and traffic passing.
        # Otherwise the row has already failed and the infra error stays per fault.
        judged = [ok for ok, d in zip(verdicts, results["oracles"], strict=True) if not d.get("infra_error")]
        if infra_errors and all(judged) and collateral_ok and traffic_ok:
            results["infra_error"] = ",".join(sorted(set(infra_errors)))
        results["success"] = success
        results["accuracy"] = 100.0 if success else 0.0
        results["composite_grading_s"] = round(time.monotonic() - start, 1)
        results.update(self.problem.report_fields())
        return results

    def _collateral(self) -> tuple[bool, str]:
        pods, error = self.problem.cluster.list_pods(self.problem.namespace)
        if pods is None:
            return False, error
        return grading.collateral_verdict(pods)

    def _traffic(self) -> tuple[bool, str]:
        app_name = getattr(self.problem.app, "name", None)
        entry = grading.ENTRY_POINTS.get(app_name)
        if entry is None:
            return False, f"no entry points known for app {app_name!r}"
        namespace = self.problem.namespace
        manifest = grading.probe_pod_manifest(f"composite-traffic-probe-{secrets.token_hex(4)}", namespace, entry)
        phase, logs = self.problem.cluster.run_pod(manifest, timeout_s=grading.probe_deadline_s(entry) + 30)
        if phase.startswith("Error"):
            return False, f"probe pod not run: {phase}"
        result = grading.parse_probe_log(logs)
        return result.ok, f"{result.detail()} (pod phase {phase})"


def _root_cause(p) -> str:
    return (getattr(p, "root_cause", "") or "").strip()


class CompositeDiagnosisOracle(Oracle):
    """One judge call per sub-fault against the same submitted text (§5.5), concurrently.

    Judges are built at evaluation time, so each reads its sub-problem's ``root_cause``
    after the conductor rewrote the namespace in it. ``judge_factory(p, other_faults)`` builds
    the judge for sub-problem ``p``; ``other_faults`` holds the other sub-problems' root causes
    in declared order, so a judge does not count the submission's (correct) account of the other
    injected faults as over-attribution.

    A judge that has not returned by the deadline could not judge its fault: it is recorded as
    ``fault_i_infra_error``, and as the oracle's ``infra_error`` only when that fault alone could
    still flip the outcome (every other fault diagnosed), as ``CompositeMitigationOracle`` does.
    """

    # One judge's deadline for one candidate. Judges run concurrently, so N does not change it;
    # a judge tries a submission's candidates one after another (LLMAsAJudgeOracle.evaluate).
    candidate_deadline_s = 270.0

    def deadline_s(self, candidates: int = MAX_DIAGNOSIS_CANDIDATES) -> float:
        """Deadline for judging a submission of *candidates* candidates (default: the most the API accepts)."""
        return self.candidate_deadline_s * min(max(1, candidates), MAX_DIAGNOSIS_CANDIDATES)

    def __init__(self, problem, judge_factory=None):
        super().__init__(problem)
        self.sub_problems = problem.problems
        self._judge_factory = judge_factory or self._llm_judge

    @staticmethod
    def _llm_judge(p, other_faults: tuple[str, ...]):
        from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle

        return LLMAsAJudgeOracle(problem=p, expected=_root_cause(p), other_faults=other_faults)

    def _other_faults(self, i: int) -> tuple[str, ...]:
        others = (_root_cause(p) for j, p in enumerate(self.sub_problems) if j != i)
        return tuple(rc for rc in others if rc)

    def _judge_one(self, i: int, solution) -> dict:
        p = self.sub_problems[i]
        return self._judge_factory(p, self._other_faults(i)).evaluate(solution) or {}

    def evaluate(self, solution) -> dict:
        print("== Composite Diagnosis Evaluation ==")
        results: dict[str, Any] = {"oracles": []}
        total = len(self.sub_problems)
        diagnosed = 0
        infra_errors, judged = [], []
        deadline_s = self.deadline_s(len(solution) if isinstance(solution, list) else 1)
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, total), thread_name_prefix="composite-judge")
        try:
            futures = [pool.submit(self._judge_one, i, solution) for i in range(total)]
            deadline = time.monotonic() + deadline_s
            for i, (p, fut) in enumerate(zip(self.sub_problems, futures, strict=True)):
                name = type(p).__name__
                try:
                    detail = fut.result(timeout=max(0.0, deadline - time.monotonic()))
                    ok = bool(detail.get("success", False))
                except concurrent.futures.TimeoutError:
                    error = f"judge timed out after {deadline_s:g}s"
                    ok, detail = False, {"success": False, "error": error, "infra_error": "judge_timeout"}
                except Exception as e:
                    ok, detail = False, {"success": False, "error": f"{type(e).__name__}: {e}"}
                diagnosed += int(ok)
                results[f"fault_{i}_class"] = name
                results[f"fault_{i}_diagnosed"] = ok
                if detail.get("infra_error"):
                    infra_errors.append(detail["infra_error"])
                    results[f"fault_{i}_infra_error"] = detail["infra_error"]
                else:
                    judged.append(ok)
                detail = dict(detail)
                detail["name"] = f"fault_{i}:{name}"
                results["oracles"].append(detail)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        results["faults_total"] = total
        results["faults_diagnosed"] = diagnosed
        # An unjudged fault makes the row ungradeable only if judging it could still flip the
        # outcome (every judged fault diagnosed); otherwise the infra error stays per fault.
        if infra_errors and all(judged):
            results["infra_error"] = ",".join(sorted(set(infra_errors)))
        results["success"] = diagnosed == total
        results["accuracy"] = 100.0 * diagnosed / max(1, total)
        results.update(self.problem.report_fields())
        return results
