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
"""Plan a reproducible sequence of problem ids mixing single faults and random compositions.

Pure and stdlib-only at module level (see the composition contract). The CLI (`main.py`) builds the
single-fault pool from the registry, calls `single_fault_stream` and `plan_sequence`, and runs the
returned ids exactly like a `--problem a,b,c` list.

Guarantees:
- `plan_sequence` is a function of its arguments: same inputs, same list, in any process
  (no reliance on `hash()`; slot seeds come from SHA-256).
- Each slot draws its fault count N from the weights. N == 1 takes the next id from the
  single-fault stream, in stream order. N >= 2 emits `plan_id(app, N, slot_seed(seed, slot))`
  with `app` drawn from `apps`.
- Invalid input (no apps, unknown strategy, exhausted single-fault stream, ...) raises ValueError
  naming the offender.
"""

from __future__ import annotations

import hashlib
import itertools
import random
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from sregym.conductor.problems.composition.generator import parse_plan_id, plan_id, sample_n

STRATEGIES = ("list", "shuffle", "round_robin", "grouped", "adaptive")
DEFAULT_APPS = ("hotel_reservation", "social_network", "astronomy_shop", "train_ticket")


def slot_seed(seed: int, slot: int) -> int:
    """Stable 32-bit seed for one slot, derived from the run seed and the slot index."""
    digest = hashlib.sha256(f"{seed}:{slot}".encode()).digest()
    return int.from_bytes(digest[:4], "big")


def infer_app(problem_id: str, apps: Iterable[str] = DEFAULT_APPS) -> str | None:
    """Return the app a problem id targets when the id alone determines it, else None.

    Composition plan ids carry their app. Generated variant ids (`<base>__v_<values>`) carry it
    when exactly one known app name appears as a `_`-delimited run in the value suffix (variant
    specs put `app_name` among the values). Hand-registered ids do not encode an app: None.
    """
    ref = parse_plan_id(problem_id)
    if ref is not None:
        return ref.app
    _, sep, suffix = problem_id.partition("__v_")
    if not sep:
        return None
    padded = f"_{suffix}_"
    matches = [app for app in apps if f"_{app}_" in padded]
    return matches[0] if len(matches) == 1 else None


def filter_pool_by_apps(pool: Sequence[str], apps: Iterable[str]) -> list[str]:
    """Keep ids whose app is in `apps`, plus every id whose app `infer_app` cannot determine.

    Undeterminable ids are kept, not dropped: the registry does not record an app per problem,
    and dropping them would silently shrink the pool to variants only.
    """
    wanted = frozenset(apps)
    known = sorted(wanted | frozenset(DEFAULT_APPS))
    kept = []
    for pid in pool:
        app = infer_app(pid, known)
        if app is None or app in wanted:
            kept.append(pid)
    return kept


def single_fault_stream(pool: Sequence[str], strategy: str, seed: int) -> Iterator[str]:
    """Endless stream of single-fault ids from `pool` under `strategy`.

    - list: pool order, repeated.
    - shuffle: `generate_variant_stream` (every id once per epoch, reshuffled per epoch).
    - round_robin: `generate_variant_stream_by_class` (one id per problem class per round).
    - grouped: `generate_variant_stream_grouped` (drain one class before the next).
    - adaptive: `AdaptiveScheduler` with every dispatch counted as unsolved, because a
      precomputed plan has no outcomes to feed back. Each class therefore gets its full budget
      (the largest class size) in sorted class order; a new pass reshuffles with the next seed.
    """
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown schedule {strategy!r}; expected one of {STRATEGIES}")
    if not pool:
        raise ValueError("single-fault pool is empty")
    ids = list(pool)
    if strategy == "list":
        return itertools.cycle(ids)
    # Lazy: variant_generator lives in the (heavy) problems package; keep this module stdlib-only.
    from sregym.conductor.problems import variant_generator as vg

    if strategy == "adaptive":
        return _adaptive_stream(ids, seed, vg.AdaptiveScheduler)
    window = {
        "shuffle": vg.generate_variant_stream,
        "round_robin": vg.generate_variant_stream_by_class,
        "grouped": vg.generate_variant_stream_grouped,
    }[strategy]
    return _windowed(window, ids, seed)


def _windowed(window: Callable[..., list[str]], ids: list[str], seed: int) -> Iterator[str]:
    for offset in itertools.count(0, len(ids)):
        yield from window(ids, count=len(ids), offset=offset, seed=seed)


def _adaptive_stream(ids: list[str], seed: int, scheduler_cls: type) -> Iterator[str]:
    budget = max(Counter(pid.split("__v_")[0] for pid in ids).values())
    for pass_index in itertools.count():
        scheduler = scheduler_cls(ids, consec_solves_to_stop=1, max_per_class=budget, seed=seed + pass_index)
        while (step := scheduler.next_problem()) is not None:
            _, pid = step
            scheduler.record_completion(pid, solved=False)
            yield pid


def plan_sequence(
    num_problems: int,
    weights: Mapping[int, float],
    apps: Sequence[str],
    seed: int,
    single_fault_stream: Iterator[str],
    reliable: bool = False,
) -> list[str]:
    """Return `num_problems` problem ids; see the module docstring for the guarantees.

    `single_fault_stream` is only advanced for N == 1 slots. `reliable` gives every N >= 2 id the
    `__reliable` suffix (draws without the entries flagged unreliable); the slot sequence, apps and
    seeds are unchanged by it, and single-fault ids are not affected. Raises ValueError if
    `num_problems` < 0, `apps` is empty, or the stream runs out.
    """
    if num_problems < 0:
        raise ValueError(f"num_problems must be >= 0, got {num_problems}")
    app_choices = sorted(set(apps))
    if not app_choices:
        raise ValueError("apps must not be empty")
    rng = random.Random(seed)
    sequence: list[str] = []
    for slot in range(num_problems):
        n = sample_n(weights, rng)
        if n == 1:
            pid = next(single_fault_stream, None)
            if pid is None:
                raise ValueError(f"single-fault stream exhausted at slot {slot}")
            sequence.append(pid)
        else:
            sequence.append(plan_id(rng.choice(app_choices), n, slot_seed(seed, slot), reliable=reliable))
    return sequence
