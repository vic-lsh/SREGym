"""The component-fault contract: one runtime-state fault on one bound component instance.

A subclass names its target workload (a binding row: ``self.target``), injects through the
running system (a driver in ``sregym/generators/fault/components``) and states, beside the
injection:

* ``fault_cleared()``: is this fault's own state gone?
* ``reference_mitigation()``: the scripted correct repair;
* ``extra_repairs()``: kind-specific alternative repairs, each a ``Repair`` that declares the
  outcome it must produce (``Expect``). The base adds the standard ones from the target row
  (``standard_repairs``): restore from source (a no-op, verified), rollout restart and pod
  restart of the target (any row type: stores and services) (data lost when the row's ``restart_loses_data``, acceptable when
  ``restart_clears`` and nothing is lost, else fault not cleared), and restarting each consumer.

The per-fault verdict (``ComponentFaultOracle``) fails a component for each check that does not
hold: ``fault`` (own state still there), ``write`` (a write through the entry point does not land,
for faults that block writes), ``data`` (the binding's seeded-data snapshot changed), ``witness``
(a store's sampled keys/documents or the marker written while the fault was live are gone). Its
``reason`` (``Expect``) is FAULT_NOT_CLEARED when ``fault`` or ``write`` failed, else DATA_LOST
when ``data`` or ``witness`` failed, else PASSES. An infrastructure error (``ComponentError``)
makes the verdict ``inconclusive``: never a pass, and the validation harness counts it INVALID,
never as a decoy that correctly failed.

Privileged check channel: a fault may restrict the identity the app uses (a Redis ACL on ``default``,
a revoked Mongo role), so every verdict, witness and integrity read of a driver goes through an
identity the fault does not touch (Redis: a read-only ``sregym_check`` user with a process-private
password, removed in ``recover_fault``; Mongo: the binding's admin credentials, ``inst.auth``).

Snapshot lifecycle: ``prepare_injection`` takes the seeded-data snapshot and the store witnesses
while the app is healthy (a composite calls it on every fault before injecting any). They are
kept until ``recover_fault``; ``inject_fault`` never re-takes them, so a retried injection cannot
snapshot another fault's damage.
"""

from __future__ import annotations

import base64
import enum
import os
import secrets
import time
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composition.app_bindings import (
    BINDINGS,
    AppBinding,
    MongoInstance,
    RedisInstance,
    WriteCheck,
)
from sregym.conductor.problems.composition.grading import Request
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.components import http
from sregym.generators.fault.components.kube import (
    ComponentError,
    Kube,
    delete_pods,
    restore_source,
    rollout_restart,
    workload_generations,
)
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class Expect(enum.StrEnum):
    """The outcome a repair must produce, and the classified reason of a verdict."""

    PASSES = "passes"
    FAULT_NOT_CLEARED = "fault_not_cleared"
    DATA_LOST = "data_lost"
    INCONCLUSIVE = "inconclusive"  # a verdict only: an oracle step errored


@dataclass(frozen=True)
class Repair:
    """One alternative repair. ``destroys_data``: it wipes a store's only copy, so the harness runs
    it after the other trials of a seed and resets the app after it. ``no_op``: it must change no
    workload (Deployment/StatefulSet generations); ``ComponentFaultProblem.apply`` verifies that."""

    run: Callable[[], None]
    expect: Expect
    destroys_data: bool = False
    no_op: bool = False

    def __post_init__(self) -> None:
        if self.expect is Expect.INCONCLUSIVE:
            raise ValueError("a repair cannot expect an inconclusive verdict")


Mitigation = Repair  # kept for subclasses that annotate with it

WRITE_CHECK_ATTEMPTS = 4  # social-network writes are one-way: give one a few seconds to land
WRITE_CHECK_WAIT_S = 3.0
NOT_CLEARED_COMPONENTS = frozenset({"fault", "write"})
LOST_COMPONENTS = frozenset({"data", "witness"})


def classify(failed: list[str] | tuple[str, ...]) -> Expect:
    """The reason of a verdict from its failed components (see the module docstring)."""
    if NOT_CLEARED_COMPONENTS & set(failed):
        return Expect.FAULT_NOT_CLEARED
    if LOST_COMPONENTS & set(failed):
        return Expect.DATA_LOST
    if failed:
        raise ValueError(f"unknown failed components {failed}")
    return Expect.PASSES


def _encode(v: Any) -> Any:
    if isinstance(v, bytes):
        return {"__b64__": base64.b64encode(v).decode()}
    if isinstance(v, dict):
        return {k: _encode(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_encode(x) for x in v]
    return v


def _decode(v: Any) -> Any:
    if isinstance(v, dict):
        if set(v) == {"__b64__"}:
            return base64.b64decode(v["__b64__"])
        return {k: _decode(x) for k, x in v.items()}
    if isinstance(v, list):
        return tuple(_decode(x) for x in v)
    return v


class ComponentFaultProblem(Problem):
    # The fault's state lives in the target's process memory (or its data), so restarting the
    # target clears it. False for faults outside the process (a node, a peer, a volume).
    restart_clears = True
    # The fault stops writes, so the verdict also needs a write that lands (``write_check``) and
    # no witness marker is written while it is live.
    blocks_writes = False
    # The fault restricts the identity the app (or a default client) uses: an ACL, a revoked role, a
    # changed credential. Its verdict, witness and integrity reads must then run on the driver's
    # privileged check channel (see the module docstring); a test enforces that for every such kind.
    restricts_identity = False
    # Attributes ``recover_fault`` needs besides the constructor params (which keys were
    # poisoned); persisted by the harness so a killed run can still recover the fault.
    STATE_ATTRS: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        app_name: str,
        target: str,
        description: str,
        namespace: str | None = None,
        kube: Kube | None = None,
        component: str | None = None,
    ):
        super().__init__(app=create_app(app_name), namespace=namespace)
        self.app_name = app_name
        self.binding: AppBinding = BINDINGS[app_name]
        self.target = self.binding.row(target)
        self.kube = kube or Kube()
        self.kubectl = KubeCtl()
        self.snapshot: http.Snapshot | None = None
        self.witnesses: list[tuple[Any, str, Any]] = []  # (row, collection or "", record)
        self.marker: tuple[WriteCheck, str] | None = None
        self.marker_detail = ""
        self._prepared = False
        self.root_cause = self.build_structured_root_cause(
            component=component or f"{self.target.kind.lower()}/{target}",
            namespace=self.namespace,
            description=description,
        )
        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = ComponentFaultOracle(problem=self)
        self.app.create_workload()

    # --- what a subclass provides ------------------------------------------------------------------

    @abstractmethod
    def inject(self) -> None:
        """Put the fault in place through the running system. Raises on failure."""

    @abstractmethod
    def fault_cleared(self) -> tuple[bool, str]:
        """``(cleared, detail)`` for this fault's own state."""

    @abstractmethod
    def reference_mitigation(self) -> None:
        """The scripted correct repair."""

    def extra_repairs(self) -> dict[str, Repair]:
        """Kind-specific alternative repairs (flush a cache, ...). Default: none."""
        return {}

    def prepare_target(self) -> None:
        """Before the snapshot, while healthy: make sure the target holds data (seed it)."""

    def write_check(self) -> WriteCheck | None:
        """The entry-point write that lands in the target (the target row's, by default)."""
        return getattr(self.target, "write_check", None)

    def data_stores(self) -> tuple[Any, ...]:
        """Store rows this fault or its repairs can touch whose restart loses data: each gets a
        witness. Default: the target, if it is such a store."""
        return (self.target,) if getattr(self.target, "restart_loses_data", False) else ()

    # --- repairs ---------------------------------------------------------------------------------

    def standard_repairs(self) -> dict[str, Repair]:
        row = self.target
        out = {"restore_source": Repair(self.restore_source, Expect.FAULT_NOT_CLEARED, no_op=True)}
        if hasattr(row, "restart_loses_data"):  # every binding row type
            loses = row.restart_loses_data
            if not self.restart_clears:
                expect = Expect.FAULT_NOT_CLEARED
            else:
                expect = Expect.DATA_LOST if loses else Expect.PASSES
            out["rollout_restart"] = Repair(self._restart, expect, destroys_data=loses)
            out["restart_stateful_pod"] = Repair(self._delete_pods, expect, destroys_data=loses)
        for svc in getattr(row, "consumers", ()):
            out[f"rollout_restart:{svc}"] = Repair(
                lambda svc=svc: rollout_restart(self.kube, self.namespace, "Deployment", svc), Expect.FAULT_NOT_CLEARED
            )
        return out

    def repairs(self) -> dict[str, Repair]:
        return {**self.standard_repairs(), **self.extra_repairs()}

    def decoy_mitigations(self) -> dict[str, Repair]:
        """Wrong repairs: the verdict must FAIL after each (for its declared reason)."""
        return {k: r for k, r in self.repairs().items() if r.expect is not Expect.PASSES}

    def acceptable_mitigations(self) -> dict[str, Repair]:
        """Repairs other than the reference that are correct for this target."""
        return {k: r for k, r in self.repairs().items() if r.expect is Expect.PASSES}

    def apply(self, repair: Repair) -> None:
        """Run a repair; a declared no-op that changed a workload raises (the trial is invalid)."""
        before = workload_generations(self.kube, self.namespace) if repair.no_op else None
        repair.run()
        if before is not None:
            after = workload_generations(self.kube, self.namespace)
            changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
            if changed:
                raise ComponentError(
                    f"{self.namespace}: a repair declared a no-op changed workloads {changed[:8]} "
                    "(live objects differ from the accepted render?)"
                )

    def _restart(self) -> None:
        rollout_restart(self.kube, self.namespace, self.target.kind, self.target.workload)

    def _delete_pods(self) -> None:
        delete_pods(self.kube, self.namespace, self.target.kind, self.target.workload)

    def restore_source(self) -> None:
        render = os.getenv("SREGYM_ACCEPTED_RENDER")
        restore_source(self.kube, self.namespace, Path(render) if render else None)

    # --- lifecycle -------------------------------------------------------------------------------

    def prepare_injection(self) -> None:
        """Take the seeded-data snapshot and store witnesses now, while the app is healthy. A
        composite calls this on every fault before injecting any. Kept until ``recover_fault``."""
        self.prepare_target()
        self.snapshot = (
            http.capture(self.kube, self.binding, self.namespace) if self.binding.integrity is not None else None
        )
        self.witnesses = [w for row in self.data_stores() for w in _take_witness(self.kube, self.namespace, row)]
        self._prepared = True

    @mark_fault_injected
    def inject_fault(self):
        print(f"== Fault Injection: {type(self).__name__} | ns={self.namespace} ==")
        if not self._prepared:
            self.prepare_injection()
        self.inject()
        check = self.write_check()
        if not self.blocks_writes and check is not None and self.data_stores():
            # The witness write: made while the fault is live, it must survive the repair. Kept only
            # if it landed (a fault may still block this particular write).
            marker = self._write_marker(check)
            landed, self.marker_detail = self._marker_visible(check, marker)
            self.marker = (check, marker) if landed else None

    @mark_fault_injected
    def recover_fault(self):
        print(f"== Fault Recovery: {type(self).__name__} | ns={self.namespace} ==")
        rows = [self.target, *(w[0] for w in self.witnesses)]
        try:
            self.reference_mitigation()
        finally:  # even if the repair raised: the next injection must take a fresh snapshot
            self.snapshot, self.witnesses, self.marker, self._prepared = None, [], None, False
            self._close_check_channels(rows)

    def _close_check_channels(self, rows: list[Any]) -> None:
        """Remove the privileged verdict-read identity the drivers opened on each store row."""
        from sregym.generators.fault.components import redis

        for row in {r.workload: r for r in rows if isinstance(r, RedisInstance)}.values():
            redis.remove_check_user(self.kube, self.namespace, row)

    def injected_state(self) -> dict[str, Any]:
        """JSON-serializable state ``recover_fault`` needs (``STATE_ATTRS``)."""
        return {a: _encode(getattr(self, a)) for a in self.STATE_ATTRS}

    def load_injected_state(self, state: dict[str, Any]) -> None:
        for a in self.STATE_ATTRS:
            if a in state:
                setattr(self, a, _decode(state[a]))

    # --- verdict ---------------------------------------------------------------------------------

    def _write_marker(self, check: WriteCheck) -> str:
        marker = f"m{secrets.token_hex(5)}"
        fields = tuple((k, marker if k == check.marker_field else v) for k, v in check.fields)
        http.fetch(self.kube, self.binding, self.namespace, (Request(check.path, fields),))
        return marker

    def _marker_visible(self, check: WriteCheck, marker: str) -> tuple[bool, str]:
        for attempt in range(WRITE_CHECK_ATTEMPTS):
            time.sleep(WRITE_CHECK_WAIT_S)
            (read,) = http.fetch(self.kube, self.binding, self.namespace, (Request(check.read),))
            if read.ok and marker in read.body:
                return True, f"marker visible at {check.read} after {attempt + 1} reads"
        return False, f"marker written through {check.path} not visible at {check.read}"

    def verdict(self) -> dict[str, Any]:
        """The per-fault verdict; raises ComponentError when a check could not run."""
        failed: list[str] = []
        cleared, detail = self.fault_cleared()
        result: dict[str, Any] = {"fault_cleared": cleared, "fault_detail": detail}
        if not cleared:
            failed.append("fault")
        check = self.write_check()
        if self.blocks_writes and check is not None:
            written, result["write_detail"] = self._marker_visible(check, self._write_marker(check))
            if not written:
                failed.append("write")
        if self.binding.integrity is None:
            result["integrity_detail"] = "no integrity check in the binding"
        elif self.snapshot is None:
            raise ComponentError(f"{type(self).__name__}: no seeded-data snapshot (fault not injected here)")
        else:
            intact, result["integrity_detail"] = http.compare(self.kube, self.binding, self.namespace, self.snapshot)
            result["integrity_ok"] = intact
            if not intact:
                failed.append("data")
        details = []
        witness_ok = True
        for row, coll, record in self.witnesses:
            ok, d = _check_witness(self.kube, self.namespace, row, coll, record)
            witness_ok, details = witness_ok and ok, [*details, d]
        if self.marker is not None:
            ok, d = self._marker_visible(*self.marker)
            witness_ok, details = witness_ok and ok, [*details, d]
        if details:
            result.update(witness_ok=witness_ok, witness_detail="; ".join(details))
        if not witness_ok:
            failed.append("witness")
        result.update(success=not failed, failed=failed, reason=classify(failed).value)
        return result


def _take_witness(kube: Kube, namespace: str, row: Any) -> list[tuple[Any, str, Any]]:
    """A store's witness records: sampled keys (Redis) or sampled document ids per collection
    (Mongo). Raises if the store is empty: a fault there could not be told from a wipe."""
    from sregym.generators.fault.components import mongo, redis

    if isinstance(row, RedisInstance):
        sample = redis.key_sample(kube, namespace, row)
        if not sample.keys:
            raise ComponentError(f"{row.workload} in {namespace}: no keys to witness (empty store)")
        return [(row, "", sample)]
    if isinstance(row, MongoInstance):
        out = []
        for coll in row.collections:
            record = mongo.data_record(kube, namespace, row, coll.name)
            if record.count:
                out.append((row, coll.name, record))
        if not out:
            raise ComponentError(f"{row.workload} in {namespace}: no documents to witness (empty store)")
        return out
    raise ValueError(f"no witness for {type(row).__name__} {row.workload}")


def _check_witness(kube: Kube, namespace: str, row: Any, coll: str, record: Any) -> tuple[bool, str]:
    from sregym.generators.fault.components import mongo, redis

    if isinstance(row, RedisInstance):
        return redis.keys_intact(kube, namespace, row, record)
    return mongo.data_intact(kube, namespace, row, coll, record)


VERDICT_ATTEMPTS = 2


class ComponentFaultOracle(Oracle):
    """``ComponentFaultProblem.verdict`` as a mitigation oracle. A ``ComponentError`` is retried
    once; if it persists the result is ``inconclusive`` (success False, reason inconclusive)."""

    # It runs client pods in the namespace (seeded-data reads, memcached reads), which other
    # oracles' pod walks would see.
    concurrent_safe = False
    evaluation_budget_s = 180.0

    def evaluate(self) -> dict:  # type: ignore[override]
        error = ""
        for attempt in range(VERDICT_ATTEMPTS):
            try:
                result = self.problem.verdict()
                print(f"  {type(self.problem).__name__}: {result}")
                return result
            except ComponentError as e:
                error = str(e)[:500]
                if attempt + 1 < VERDICT_ATTEMPTS:
                    time.sleep(5)
        return {"success": False, "inconclusive": True, "reason": Expect.INCONCLUSIVE.value, "error": error}
