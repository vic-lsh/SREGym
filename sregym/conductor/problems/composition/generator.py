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

An optional take-K suffix, `mf__<app>__n<N>__s<seed>__k<K>__v_`, keeps K of that N-fault plan's
draws: the first K of a fixed shuffle seeded from (app, N, seed). The K-subsets of one base plan
are nested (K=1 within K=2 within ... within K=N), so faults can be matched across problems.

An optional `__reliable` suffix (after any take-K), `mf__<app>__n<N>__s<seed>[__k<K>]__reliable__v_`,
draws from the catalog without the entries `reliability.UNRELIABLE` flags for the app. Ids
without it draw from the whole catalog, as before.
"""

from __future__ import annotations

import hashlib
import math
import random
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from sregym.conductor.problems.composition.reliability import reliable_entries
from sregym.conductor.problems.composition.spec import CatalogEntry, FaultDraw, conflicts

PLAN_ID_PREFIX = "mf__"

# Sampling budget. Rejection sampling cannot prove that no plan exists, so PlanError means
# "no conflict-free plan of size n was found within the budget". Restarts escape an early
# draw that blocks the rest; the per-restart budget scales with n.
_RESTARTS = 20
_ATTEMPTS_PER_FAULT = 50

_PLAN_ID_RE = re.compile(
    r"^mf__(?P<app>[a-z0-9]+(?:_[a-z0-9]+)*)__n(?P<n>[1-9][0-9]*)__s(?P<seed>0|[1-9][0-9]*)"
    r"(?:__k(?P<take>[1-9][0-9]*))?(?P<reliable>__reliable)?__v_$"
)


class PlanError(ValueError):
    """No valid multi-fault plan of the requested size could be drawn."""


@dataclass(frozen=True)
class PlanRef:
    """What a multi-fault id names: the N-fault plan for (app, n, seed), or its take-K subset.

    `reliable` draws from the catalog without the entries flagged unreliable for the app.
    """

    app: str
    n: int
    seed: int
    take: int | None = None
    reliable: bool = False


@dataclass(frozen=True)
class CompositionPlan:
    """A conflict-free set of fault draws, ordered by injection tier then base-plan order.

    `n` is the base plan's fault count. `take` is None for a whole plan, else K, and then
    `draws` holds K of the base plan's draws. `base_index[i]` is the position of `draws[i]` in
    the base plan's draws (0..n-1); for a whole plan it is 0..n-1 in order.
    """

    app: str
    n: int
    seed: int
    draws: tuple[FaultDraw, ...]
    take: int | None = None
    base_index: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if not self.base_index:
            object.__setattr__(self, "base_index", tuple(range(len(self.draws))))
        if len(self.base_index) != len(self.draws):
            raise ValueError(f"base_index has {len(self.base_index)} entries for {len(self.draws)} draws")


def plan_id(app: str, n: int, seed: int, take: int | None = None, reliable: bool = False) -> str:
    """Return the problem id for a plan (or its take-K subset); `parse_plan_id` inverts it."""
    suffix = ("" if take is None else f"__k{take}") + ("__reliable" if reliable else "")
    pid = f"{PLAN_ID_PREFIX}{app}__n{n}__s{seed}{suffix}__v_"
    if parse_plan_id(pid) != PlanRef(app, n, seed, take, reliable):
        raise ValueError(
            f"cannot form a multi-fault plan id from app={app!r}, n={n!r}, seed={seed!r}, take={take!r}, "
            f"reliable={reliable!r}"
        )
    return pid


def parse_plan_id(problem_id: str) -> PlanRef | None:
    """Return the PlanRef for a well-formed multi-fault id, else None (also when K > N)."""
    match = _PLAN_ID_RE.match(problem_id)
    if match is None:
        return None
    n = int(match["n"])
    take = None if match["take"] is None else int(match["take"])
    if take is not None and take > n:
        return None
    return PlanRef(match["app"], n, int(match["seed"]), take, match["reliable"] is not None)


def _seeded_rng(text: str) -> random.Random:
    # sha256, never hash(): Python's str hash is salted per process.
    digest = hashlib.sha256(text.encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def _plan_rng(app: str, n: int, seed: int) -> random.Random:
    return _seeded_rng(f"{app}|{n}|{seed}")


def take_plan(plan: CompositionPlan, k: int) -> CompositionPlan:
    """Keep K of a whole plan's draws: the first K of a shuffle seeded from (app, n, seed).

    Nested: take_plan(plan, j) draws are a subset of take_plan(plan, k) draws for j <= k, and
    K = n keeps every draw. Kept draws stay in base-plan order, which is tier order. Raises
    PlanError unless 1 <= k <= plan.n, or if `plan` is itself a subset.
    """
    if plan.take is not None:
        raise PlanError(f"plan is already a take-{plan.take} subset")
    if not 1 <= k <= plan.n:
        raise PlanError(f"take K must be in 1..{plan.n}, got {k}")
    order = list(range(plan.n))
    _seeded_rng(f"{plan.app}|{plan.n}|{plan.seed}|nest").shuffle(order)
    kept = tuple(sorted(order[:k]))
    return CompositionPlan(
        app=plan.app,
        n=plan.n,
        seed=plan.seed,
        draws=tuple(plan.draws[i] for i in kept),
        take=k,
        base_index=kept,
    )


def plan_for(ref: PlanRef, entries: Sequence[CatalogEntry]) -> CompositionPlan:
    """Draw the plan a PlanRef names: `draw_plan` (over the reliable entries when `ref.reliable`),
    then `take_plan` when it has a take-K."""
    if ref.reliable:
        entries = reliable_entries(ref.app, entries)
    plan = draw_plan(ref.app, ref.n, ref.seed, entries)
    return plan if ref.take is None else take_plan(plan, ref.take)


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
