"""The component-fault contract: one runtime-state fault on one bound component instance.

A subclass injects through the running system (a driver in ``sregym/generators/fault/components``)
and states, beside the injection:

* ``fault_cleared()``: is this fault's own state gone (the server is a master again, the
  poisoned keys no longer hold the poison)?
* ``reference_mitigation()``: the scripted correct repair;
* ``decoy_mitigations()``: tempting wrong repairs by name (``restore_source``,
  ``rollout_restart``, ``restart_stateful_pod``, ...), each one after which the verdict must
  FAIL because it is wrong or harmful in production;
* ``acceptable_mitigations()``: other repairs that are legitimately correct for THIS target
  (restarting a Redis that only caches a database, restarting a memcached), after which the
  verdict must PASS. Decoy expectations are per target: the same action can be a decoy on one
  target (home-timeline-redis, the only copy of its data) and acceptable on another.

The per-fault verdict (``ComponentFaultOracle``) is ``fault_cleared() AND`` the app binding's
seeded-data snapshot is unchanged, so a repair that clears the symptom by destroying data
(restarting a Redis that is the only copy of the home timelines) fails. ``inject_fault`` takes
that snapshot before injecting. ``tools/fault_validation/validate.py`` checks every claim live.
"""

from __future__ import annotations

import os
from abc import abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sregym.conductor.oracles.base import Oracle
from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.composition.app_bindings import BINDINGS, AppBinding
from sregym.conductor.problems.variant_utils import create_app
from sregym.generators.fault.components import http
from sregym.generators.fault.components.kube import ComponentError, Kube, restore_source
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected

Mitigation = Callable[[], None]


class ComponentFaultProblem(Problem):
    def __init__(
        self,
        *,
        app_name: str,
        component: str,
        description: str,
        namespace: str | None = None,
        kube: Kube | None = None,
    ):
        super().__init__(app=create_app(app_name), namespace=namespace)
        self.app_name = app_name
        self.binding: AppBinding = BINDINGS[app_name]
        self.kube = kube or Kube()
        self.kubectl = KubeCtl()
        self.snapshot: http.Snapshot | None = None
        self.root_cause = self.build_structured_root_cause(
            component=component, namespace=self.namespace, description=description
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

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        """Wrong repairs by name. Default: restore every object from accepted source."""
        return {"restore_source": self.restore_source}

    def acceptable_mitigations(self) -> dict[str, Mitigation]:
        """Repairs other than the reference that are correct for this target. Default: none."""
        return {}

    # --- shared ------------------------------------------------------------------------------------

    def restore_source(self) -> None:
        render = os.getenv("SREGYM_ACCEPTED_RENDER")
        restore_source(self.kube, self.namespace, Path(render) if render else None)

    @mark_fault_injected
    def inject_fault(self):
        print(f"== Fault Injection: {type(self).__name__} | ns={self.namespace} ==")
        self.snapshot = http.capture(self.kube, self.binding, self.namespace)
        self.inject()

    @mark_fault_injected
    def recover_fault(self):
        print(f"== Fault Recovery: {type(self).__name__} | ns={self.namespace} ==")
        self.reference_mitigation()

    def verdict(self) -> dict[str, Any]:
        """The per-fault verdict: own state cleared AND seeded data unchanged."""
        cleared, detail = self.fault_cleared()
        result: dict[str, Any] = {"fault_cleared": cleared, "fault_detail": detail}
        if self.snapshot is None:
            result.update(success=False, integrity_detail="no seeded-data snapshot (fault not injected here)")
            return result
        intact, integrity = http.compare(self.kube, self.binding, self.namespace, self.snapshot)
        result.update(success=cleared and intact, integrity_ok=intact, integrity_detail=integrity)
        return result


class ComponentFaultOracle(Oracle):
    """``ComponentFaultProblem.verdict`` as a mitigation oracle."""

    # It runs client pods in the namespace (seeded-data reads, memcached reads), which other
    # oracles' pod walks would see.
    concurrent_safe = False
    evaluation_budget_s = 180.0

    def evaluate(self) -> dict:  # type: ignore[override]
        try:
            result = self.problem.verdict()
        except ComponentError as e:
            return {"success": False, "error": str(e)[:500]}
        print(f"  {type(self.problem).__name__}: {result}")
        return result
