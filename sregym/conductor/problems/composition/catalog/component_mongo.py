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
"""Catalog family: MongoDB runtime faults (write lock, write rejection, renamed collection, poison document).

Targets come from the app binding's Mongo rows: a (instance, collection) pair is sampled only when
the collection lists the fault kind in ``MongoCollection.faults`` (verified live). MongoDB holds the
only copy of its data, so every draw declares the whole workload (field None).
"""

from __future__ import annotations

import random
from typing import Any

from sregym.conductor.problems.composition.app_bindings import BINDINGS, mongo_targets
from sregym.conductor.problems.composition.catalog._kit import Pick, deployment, entry
from sregym.conductor.problems.composition.spec import CatalogEntry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "mongo_write_lock": ("social_network",),
    "mongo_write_rejection": ("social_network",),
    "mongo_collection_renamed": ("social_network",),
    "mongo_poison_document": ("social_network",),
}

_WRITE_MODES = ("validator", "unique_index")  # mirrors mongo_component_faults.WRITE_MODES
_POISON_SHAPES = ("wrong_type", "field_renamed")  # mirrors components.mongo.POISON_SHAPES
_POISON_SCOPES = ("one_doc", "all_seeded")  # mirrors mongo_component_faults.POISON_SCOPES


def _pick_target(app: str, kind: str, rng: random.Random):
    pairs = mongo_targets(BINDINGS[app].mongo, kind)
    if not pairs:
        raise ValueError(f"{kind}: no verified mongo target in the {app} binding")
    return rng.choice(pairs)


def _choose_write_lock(app: str, rng: random.Random) -> Pick:
    inst, _ = _pick_target(app, "mongo_write_lock", rng)
    return Pick({"target": inst.workload}, (deployment(inst.workload),))


def _build_write_lock(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.mongo_component_faults import MongoWriteLock

    return MongoWriteLock(app_name=app, **params)


def _choose_write_rejection(app: str, rng: random.Random) -> Pick:
    inst, coll = _pick_target(app, "mongo_write_rejection", rng)
    modes = [m for m in _WRITE_MODES if m == "validator" or (coll.unique_key and coll.stamp_field)]
    return Pick(
        {"target": inst.workload, "collection": coll.name, "mode": rng.choice(modes)}, (deployment(inst.workload),)
    )


def _build_write_rejection(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.mongo_component_faults import MongoWriteRejection

    return MongoWriteRejection(app_name=app, **params)


def _choose_renamed(app: str, rng: random.Random) -> Pick:
    inst, coll = _pick_target(app, "mongo_collection_renamed", rng)
    return Pick({"target": inst.workload, "collection": coll.name}, (deployment(inst.workload),))


def _build_renamed(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.mongo_component_faults import MongoCollectionRenamed

    return MongoCollectionRenamed(app_name=app, **params)


def _choose_poison(app: str, rng: random.Random) -> Pick:
    inst, coll = _pick_target(app, "mongo_poison_document", rng)
    return Pick(
        {
            "target": inst.workload,
            "collection": coll.name,
            "shape": rng.choice(_POISON_SHAPES),
            "scope": rng.choice(_POISON_SCOPES),
        },
        (deployment(inst.workload),),
    )


def _build_poison(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.mongo_component_faults import MongoPoisonDocument

    return MongoPoisonDocument(app_name=app, **params)


ENTRIES: tuple[CatalogEntry, ...] = (
    entry("mongo_write_lock", ENABLED_APPS["mongo_write_lock"], _choose_write_lock, _build_write_lock, generation=2),
    entry(
        "mongo_write_rejection",
        ENABLED_APPS["mongo_write_rejection"],
        _choose_write_rejection,
        _build_write_rejection,
        generation=2,
    ),
    entry(
        "mongo_collection_renamed",
        ENABLED_APPS["mongo_collection_renamed"],
        _choose_renamed,
        _build_renamed,
        generation=2,
    ),
    entry("mongo_poison_document", ENABLED_APPS["mongo_poison_document"], _choose_poison, _build_poison, generation=2),
)
