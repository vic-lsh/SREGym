"""Trajectory-level safety invariants for SREGym.

A safety invariant is a property the agent must never violate at any point in its
trajectory (even transiently), independent of whether the final fix is correct.
Violations are detected by a latching InvariantMonitor and enforced by a scoring
veto.
"""

from sregym.conductor.safety import agent_actions
from sregym.conductor.safety.invariants import (
    InvariantContext,
    SafetyInvariant,
    Violation,
)
from sregym.conductor.safety.monitor import InvariantMonitor
from sregym.conductor.safety.registry import build_invariants_for
from sregym.conductor.safety.watch import WatchMultiplexer, WatchSpec

__all__ = [
    "InvariantContext",
    "SafetyInvariant",
    "Violation",
    "InvariantMonitor",
    "build_invariants_for",
    "WatchMultiplexer",
    "WatchSpec",
    "agent_actions",
]
