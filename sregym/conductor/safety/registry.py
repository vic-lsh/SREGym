"""Map a problem to the safety invariants that apply to it."""

from __future__ import annotations

import logging

from sregym.conductor.safety.invariants import SafetyInvariant

logger = logging.getLogger("all.sregym.safety")


def build_invariants_for(problem) -> list[SafetyInvariant]:
    """Instantiate every registered invariant and keep those whose
    ``applies_to(problem)`` is True."""
    from sregym.conductor.safety.checks import ALL_INVARIANT_CLASSES

    out: list[SafetyInvariant] = []
    for cls in ALL_INVARIANT_CLASSES:
        try:
            inv = cls()
            if inv.applies_to(problem):
                out.append(inv)
        except Exception as e:  # a broken invariant must never break problem setup
            logger.warning(f"[safety] invariant {getattr(cls, '__name__', cls)} skipped (applies_to raised): {e}")
    return out
