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
"""Random multi-fault composition: build one co-located composite from a plan id.

Public surface: `resolve(problem_id)` and `build_composite(plan)`. Module level imports only
the standard library; problem classes and the composite load lazily inside the functions.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sregym.conductor.problems.composition.generator import CompositionPlan
    from sregym.conductor.problems.composition.spec import CatalogEntry


def build_composite(
    plan: CompositionPlan,
    *,
    entries: Sequence[CatalogEntry] | None = None,
    cluster: Any = None,
) -> Any:
    """Construct every drawn problem and wrap them in one ColocatedComposite.

    The composite reports the plan's identity: `composite_base_n` (the base plan's N),
    `composite_take_k` (K, or N for a whole plan) and `fault_<i>_base_index` (fault i's position
    in the base plan), so faults can be matched across take-K subsets of one plan.
    `entries` defaults to the discovered catalog and `cluster` (the composite's ClusterIO) to the
    real cluster; tests pass a fake catalog and cluster. Raises KeyError naming a draw whose key
    is not in `entries`.

    The shared app is deployed as robust-skills deploys it, whichever fault the plan draws first:
    a Hotel Reservation app gets its failure-admin ConfigMaps filled but mounted nowhere
    (`FailureScripts.UNMOUNTED`).
    """
    from sregym.conductor.problems.colocated_composite import ColocatedComposite
    from sregym.conductor.problems.composition import catalog
    from sregym.service.apps.hotel_reservation import FailureScripts, HotelReservation

    by_key = {entry.key: entry for entry in (catalog.entries() if entries is None else entries)}
    missing = sorted({draw.key for draw in plan.draws} - by_key.keys())
    if missing:
        raise KeyError(f"plan draws keys not in the catalog: {missing}")
    composite = ColocatedComposite(
        problems=[by_key[draw.key].build(draw) for draw in plan.draws],
        footprints=[draw.footprints for draw in plan.draws],
        tiers=[draw.tier for draw in plan.draws],
        draws=list(plan.draws),
        identity=_plan_identity(plan),
        cluster=cluster,
    )
    if isinstance(composite.app, HotelReservation):
        composite.app.failure_scripts = FailureScripts.UNMOUNTED
    return composite


def _plan_identity(plan: CompositionPlan) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "composite_base_n": plan.n,
        "composite_take_k": plan.n if plan.take is None else plan.take,
    }
    for i, base in enumerate(plan.base_index):
        identity[f"fault_{i}_base_index"] = base
    return identity


def resolve(problem_id: str) -> Any:
    """Return the composite for a `mf__` id, or None for any other id.

    Raises ValueError for a malformed `mf__` id and PlanError if no plan can be drawn.
    """
    from sregym.conductor.problems.composition import catalog
    from sregym.conductor.problems.composition.generator import PLAN_ID_PREFIX, parse_plan_id, plan_for

    if not problem_id.startswith(PLAN_ID_PREFIX):
        return None
    ref = parse_plan_id(problem_id)
    if ref is None:
        raise ValueError(
            f"malformed multi-fault problem id {problem_id!r}; expected "
            f"{PLAN_ID_PREFIX}<app>__n<N>__s<seed>[__k<K>][__reliable]__v_ with 1 <= K <= N"
        )
    return build_composite(plan_for(ref, catalog.entries_for(ref.app)))
