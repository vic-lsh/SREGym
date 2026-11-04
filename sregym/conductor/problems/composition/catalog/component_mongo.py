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
the collection lists the fault kind in ``MongoCollection.faults`` (verified live). A fault that
flushes the collection's cache (rename, poison) also touches that cache workload.
"""

from __future__ import annotations

import random

from sregym.conductor.problems.composition.app_bindings import AppBinding, mongo_targets
from sregym.conductor.problems.composition.catalog._component import Target, component_entry, pick_one
from sregym.conductor.problems.composition.spec import CatalogEntry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "mongo_write_lock": ("social_network",),
    "mongo_write_rejection": ("social_network",),
    "mongo_collection_renamed": ("social_network",),
    "mongo_poison_document": ("social_network",),
}

# Mirrors (compared by tests/composition/test_catalog_mirrors.py).
WRITE_MODES = ("validator", "unique_index")  # mongo_component_faults.WRITE_MODES
POISON_SHAPES = ("wrong_type", "field_renamed")  # components.mongo.POISON_SHAPES
POISON_SCOPES = ("one_doc", "all_seeded")  # mongo_component_faults.POISON_SCOPES

_PROBLEMS = "sregym.conductor.problems.mongo_component_faults"


def _pick(b: AppBinding, kind: str, rng: random.Random):
    return pick_one(mongo_targets(b.mongo, kind), f"mongo target for {kind}", b.app, rng)


def _caches(coll) -> tuple[str, ...]:
    return (coll.cache,) if coll.cache else ()


def _write_lock(b: AppBinding, rng: random.Random) -> Target:
    inst, _ = _pick(b, "mongo_write_lock", rng)
    return Target({"target": inst.workload})


def _write_rejection(b: AppBinding, rng: random.Random) -> Target:
    inst, coll = _pick(b, "mongo_write_rejection", rng)
    modes = [m for m in WRITE_MODES if m == "validator" or (coll.unique_key and coll.stamp_field)]
    return Target({"target": inst.workload, "collection": coll.name, "mode": rng.choice(modes)})


def _renamed(b: AppBinding, rng: random.Random) -> Target:
    inst, coll = _pick(b, "mongo_collection_renamed", rng)
    return Target({"target": inst.workload, "collection": coll.name}, touches=_caches(coll))


def _poison(b: AppBinding, rng: random.Random) -> Target:
    inst, coll = _pick(b, "mongo_poison_document", rng)
    params = {
        "target": inst.workload,
        "collection": coll.name,
        "shape": rng.choice(POISON_SHAPES),
        "scope": rng.choice(POISON_SCOPES),
    }
    return Target(params, touches=_caches(coll))


ENTRIES: tuple[CatalogEntry, ...] = (
    component_entry("mongo_write_lock", ENABLED_APPS["mongo_write_lock"], f"{_PROBLEMS}:MongoWriteLock", _write_lock),
    component_entry(
        "mongo_write_rejection",
        ENABLED_APPS["mongo_write_rejection"],
        f"{_PROBLEMS}:MongoWriteRejection",
        _write_rejection,
    ),
    component_entry(
        "mongo_collection_renamed",
        ENABLED_APPS["mongo_collection_renamed"],
        f"{_PROBLEMS}:MongoCollectionRenamed",
        _renamed,
    ),
    component_entry(
        "mongo_poison_document", ENABLED_APPS["mongo_poison_document"], f"{_PROBLEMS}:MongoPoisonDocument", _poison
    ),
)
