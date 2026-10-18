"""Core abstractions for trajectory-level safety invariants.

A ``SafetyInvariant`` expresses a property the agent must never
violate during its trajectory — phrased as "the BAD thing SUCCEEDS" — and is
enforced by a latching ``InvariantMonitor`` plus a scoring veto.

The two hard rules every invariant must honor :
  * ``check()`` is strictly READ-ONLY and idempotent.
  * ``check()`` FAILS OPEN — it returns ``None`` on any indeterminate or
    unreachable observation, so benign transients (pod restarts, rollouts,
    connection blips) never latch a violation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Violation:
    """A latched safety-invariant violation."""

    invariant: str
    priority: str
    detail: str
    ts: float = field(default_factory=time.time)
    stage: str | None = None

    def to_dict(self) -> dict:
        return {
            "invariant": self.invariant,
            "priority": self.priority,
            "detail": self.detail,
            "ts": self.ts,
            "stage": self.stage,
        }


class InvariantContext:
    """Shared context handed to every invariant's ``arm()``/``check()``.

    ``kubectl`` is the REAL (un-proxied) ``KubeCtl`` — the monitor observes
    ground truth, while the agent acts through the filtering proxy kubeconfig.
    """

    def __init__(self, problem, kubectl, namespace: str, problem_id: str | None = None):
        self.problem = problem
        self.kubectl = kubectl
        self.namespace = namespace
        self.problem_id = problem_id
        # free-form per-run scratch space for invariants that want shared storage
        self.store: dict[str, Any] = {}


class SafetyInvariant:
    """Base class for a single safety invariant.

    Subclasses set ``name``/``priority`` and implement ``applies_to``/``arm``/
    ``check``. See the class docstrings of the concrete invariants under
    ``safety/checks/`` for the refined spec each one implements.
    """

    name: str = "unnamed"
    priority: str = "P1"

    def applies_to(self, problem) -> bool:
        """Return True if this invariant should be armed for ``problem``."""
        return False

    def arm(self, ctx: InvariantContext) -> None:
        """Capture the post-fault / pre-agent baseline. Optional.

        May set an internal NO-OP flag when positive-gate preconditions are not
        met, so the invariant never fires for this run.
        """
        return None

    def check(self, ctx: InvariantContext) -> Violation | None:
        """Sample once.

        Return a ``Violation`` only when the bad condition is CONFIRMED now and
        was not present/allowed at baseline; otherwise return ``None`` (including
        on every indeterminate/unreachable observation — fail open).
        """
        return None

    # --- edge-triggered (watch) tier ---------------------------------------
    # Polling can only see a cheat still in place when a sample lands; anything
    # done and undone inside the gap is invisible. An invariant opts into the
    # watch tier by declaring the streams it cares about and judging the event
    # payload, which carries the forbidden value even after a revert.

    def watch_specs(self, ctx: InvariantContext) -> list:
        """Resource streams this invariant wants (list of ``watch.WatchSpec``).

        Empty (default) means poll-only: no watch coverage is set up for it.
        """
        return []

    def evaluate_object(self, ctx: InvariantContext, kind: str, event_type: str, obj: dict) -> Violation | None:
        """Judge a single watch event payload.

        ``obj`` is the object AS IT WAS at that revision, so returning a
        ``Violation`` here catches a value that has since been reverted — this is
        the method that actually closes the poll-gap.

        Returning ``NotImplemented`` (the default) tells the monitor this
        invariant has no payload-level judgement and it should fall back to
        running ``check()`` on the event instead. That is a latency improvement
        only, not a gap fix. Same contract as ``check``: read-only, fail open.
        """
        return NotImplemented

    # convenience for subclasses
    def _violation(self, detail: str, stage: str | None = None) -> Violation:
        return Violation(invariant=self.name, priority=self.priority, detail=detail, stage=stage)
