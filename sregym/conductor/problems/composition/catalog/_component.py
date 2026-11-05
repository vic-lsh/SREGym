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
"""One-call catalog entries for component faults (docs/incident-faults.md, "Add a fault kind").

Standard library plus the pure binding table. ``component_entry`` turns a declarative target
sampler into a CatalogEntry:

* ``choose(binding, rng) -> Target``: the sampled params (``target`` plus mode/severity/scope)
  and ``touches``, other workloads the fault or its repairs write (a cache it flushes);
* footprints: the whole target workload and every touched one (field None: the state lives in
  the pod), so the generator never composes two faults on the same component;
* ``reads``: the stores the binding's integrity check reads through and whose restart loses data
  (``IntegrityCheck.depends_on``); the generator never pairs this fault with one that may restart
  such a store as part of a correct repair (``restarts``, or any draw without data checks);
* the problem class is imported lazily from ``problem`` ("module:Class") and built as
  ``Class(app_name=app, **params)``.
"""

from __future__ import annotations

import importlib
import random
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from sregym.conductor.problems.composition.app_bindings import BINDINGS, AppBinding
from sregym.conductor.problems.composition.catalog._kit import Pick, deployment, entry
from sregym.conductor.problems.composition.spec import CatalogEntry

ENTRIES: tuple[CatalogEntry, ...] = ()

INCIDENT_GENERATION = 2  # spec.LATEST_GENERATION at the time the component families landed


@dataclass(frozen=True)
class Target:
    params: dict[str, Any]  # must include "target": the workload of a binding row
    touches: tuple[str, ...] = field(default=())


Choose = Callable[[AppBinding, random.Random], Target]


def component_entry(
    key: str,
    apps: Iterable[str],
    problem: str,
    choose: Choose,
    *,
    restarts: bool = False,
    generation: int = INCIDENT_GENERATION,
    revisions: Mapping[int, Choose] | None = None,
) -> CatalogEntry:
    """``needs_client_pods`` is always set: every component oracle reads through client pods.
    ``restarts``: the reference or an acceptable repair restarts the target (a frozen process)."""
    module_name, _, class_name = problem.partition(":")

    def picker(choose: Choose) -> Callable[[str, random.Random], Pick]:
        return lambda app, rng: pick(choose, app, rng)

    def pick(choose: Choose, app: str, rng: random.Random) -> Pick:
        binding = BINDINGS[app]
        t = choose(binding, rng)
        names = [t.params["target"], *(w for w in t.touches if w != t.params["target"])]
        reads = binding.integrity.depends_on if binding.integrity is not None else ()
        return Pick(dict(t.params), tuple(deployment(n) for n in names), reads=frozenset(reads), restarts=restarts, needs_client_pods=True)

    def build(app: str, params: dict[str, Any]):
        cls = getattr(importlib.import_module(module_name), class_name)
        return cls(app_name=app, **params)

    return entry(
        key,
        apps,
        picker(choose),
        build,
        generation=generation,
        revisions={g: picker(c) for g, c in (revisions or {}).items()},
    )


def pick_one(rows: tuple, what: str, app: str, rng: random.Random):
    """``rng.choice`` with an error naming the binding when nothing is verified."""
    if not rows:
        raise ValueError(f"no verified {what} in the {app} binding")
    return rng.choice(rows)
