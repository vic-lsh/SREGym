# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Deterministic multi-fault plan generation over a fault catalog.

Pure module: standard library only. A plan is identified by (app, n, seed); the same
triple and catalog always produce the same draws, so a problem id such as
`mf__hotel_reservation__n5__s1234__v_` names one reproducible composite.
"""

from __future__ import annotations

import hashlib
import math
import random
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, conflicts

PLAN_ID_PREFIX = "mf__"

# Sampling budget. Rejection sampling cannot prove that no plan exists, so PlanError means
# "no conflict-free plan of size n was found within the budget". Restarts escape an early
# draw that blocks the rest; the per-restart budget scales with n.
_RESTARTS = 20
_ATTEMPTS_PER_FAULT = 50

_PLAN_ID_RE = re.compile(r"^mf__(?P<app>[a-z0-9]+(?:_[a-z0-9]+)*)__n(?P<n>[1-9][0-9]*)__s(?P<seed>0|[1-9][0-9]*)__v_$")


class PlanError(ValueError):
    """No valid multi-fault plan of the requested size could be drawn."""


@dataclass(frozen=True)
class CompositionPlan:
    """A conflict-free set of fault draws, ordered by injection tier then draw order."""

    app: str
    n: int
    seed: int
    draws: tuple[FaultDraw, ...]


def plan_id(app: str, n: int, seed: int) -> str:
    """Return the problem id for a plan; `parse_plan_id` inverts it."""
    pid = f"{PLAN_ID_PREFIX}{app}__n{n}__s{seed}__v_"
    if parse_plan_id(pid) != (app, n, seed):
        raise ValueError(f"cannot form a multi-fault plan id from app={app!r}, n={n!r}, seed={seed!r}")
    return pid


def parse_plan_id(problem_id: str) -> tuple[str, int, int] | None:
    """Return (app, n, seed) for a well-formed multi-fault id, else None."""
    match = _PLAN_ID_RE.match(problem_id)
    if match is None:
        return None
    return match["app"], int(match["n"]), int(match["seed"])


def _plan_rng(app: str, n: int, seed: int) -> random.Random:
    digest = hashlib.sha256(f"{app}|{n}|{seed}".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def draw_plan(app: str, n: int, seed: int, entries: Sequence[CatalogEntry]) -> CompositionPlan:
    """Draw n pairwise non-conflicting faults for `app`.

    Deterministic in (app, n, seed, entries); entry order does not matter. Entries are drawn
    uniformly with replacement, so one class can appear twice with different params. Raises
    PlanError if n < 1, no entry supports `app`, or the sampling budget runs out.
    """
    if n < 1:
        raise PlanError(f"n must be >= 1, got {n}")
    candidates = sorted((e for e in entries if app in e.apps), key=lambda e: e.key)
    if not candidates:
        raise PlanError(f"no catalog entry supports app {app!r}")

    rng = _plan_rng(app, n, seed)
    budget = _ATTEMPTS_PER_FAULT * n
    for _ in range(_RESTARTS):
        accepted: list[FaultDraw] = []
        for _ in range(budget):
            entry = rng.choice(candidates)
            draw = entry.sample(app, rng)
            if draw.key != entry.key or draw.app != app:
                raise ValueError(f"entry {entry.key!r} sampled a draw for ({draw.key!r}, {draw.app!r})")
            if any(conflicts(draw, other) for other in accepted):
                continue
            accepted.append(draw)
            if len(accepted) == n:
                ordered = sorted(enumerate(accepted), key=lambda pair: (pair[1].tier, pair[0]))
                return CompositionPlan(app=app, n=n, seed=seed, draws=tuple(d for _, d in ordered))
    raise PlanError(
        f"no conflict-free plan of {n} faults for {app!r} (seed {seed}) found in "
        f"{_RESTARTS} restarts x {budget} draws over {len(candidates)} entries"
    )


def parse_weights(spec: str) -> dict[int, float]:
    """Parse "1:3,2:2,5:1" into {fault count: weight}; counts and weights must be positive."""
    weights: dict[int, float] = {}
    for item in spec.split(","):
        count_text, sep, weight_text = item.strip().partition(":")
        if not sep:
            raise ValueError(f"weight item {item!r} is not of the form <n>:<weight>")
        try:
            count = int(count_text)
            weight = float(weight_text)
        except ValueError:
            raise ValueError(f"weight item {item!r} is not of the form <n>:<weight>") from None
        if count < 1:
            raise ValueError(f"fault count must be >= 1 in {item!r}")
        if not (weight > 0 and math.isfinite(weight)):
            raise ValueError(f"weight must be positive and finite in {item!r}")
        if count in weights:
            raise ValueError(f"fault count {count} given twice in {spec!r}")
        weights[count] = weight
    return weights


def sample_n(weights: Mapping[int, float], rng: random.Random) -> int:
    """Draw a fault count with probability proportional to its weight."""
    if not weights:
        raise ValueError("weights must not be empty")
    counts = sorted(weights)
    return rng.choices(counts, weights=[weights[c] for c in counts], k=1)[0]
