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
"""Catalog family: runtime-state faults on Redis and memcached rows of the app binding.

Each entry is one ``component_entry`` call: a target sampler over the binding (verified rows
only) and the problem class path. Apps are opt-in per key (``ENABLED_APPS``): enable a key for
another app once its binding rows are verified and the key validates live there
(``tools/fault_validation/validate.py``).
"""

from __future__ import annotations

import random

from sregym.conductor.problems.composition.app_bindings import AppBinding, instances
from sregym.conductor.problems.composition.catalog._component import Target, component_entry, pick_one
from sregym.conductor.problems.composition.spec import CatalogEntry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "redis_replica_misconfig": ("social_network",),
    "memcached_poisoned_entries": ("social_network",),
}

# Masters that can never serve a sync (a real full sync would flush the replica's data): connection
# refused, a blackholed address (connect timeout), a name that does not resolve.
PHANTOM_MASTERS = ("127.0.0.1:1", "10.255.255.1:6379", "redis-primary.invalid:6379")
# Mirrors of the problem modules' constants (the pure catalog cannot import them); compared by
# tests/composition/test_catalog_mirrors.py.
CORRUPTIONS = ("garbage_bytes", "wrong_type", "stale_value")  # memcached.CORRUPTION_MODES
KEY_SCOPES = ("one_key", "all_hot_keys")  # memcached_poisoned_entries.KEY_SCOPES


def _redis_replica(b: AppBinding, rng: random.Random) -> Target:
    inst = pick_one(instances(b.redis), "redis row", b.app, rng)
    return Target({"target": inst.workload, "master": rng.choice(PHANTOM_MASTERS)})


def _memcached_poison(b: AppBinding, rng: random.Random) -> Target:
    inst = pick_one([m for m in instances(b.memcached) if m.has_hot_keys], "memcached row", b.app, rng)
    return Target(
        {
            "target": inst.workload,
            "corruption": rng.choice(CORRUPTIONS),
            "key_scope": rng.choice(KEY_SCOPES),
            "poison_seed": rng.randrange(2**31),
        }
    )


ENTRIES: tuple[CatalogEntry, ...] = (
    component_entry(
        "redis_replica_misconfig",
        ENABLED_APPS["redis_replica_misconfig"],
        "sregym.conductor.problems.redis_replica_misconfig:RedisReplicaMisconfig",
        _redis_replica,
    ),
    component_entry(
        "memcached_poisoned_entries",
        ENABLED_APPS["memcached_poisoned_entries"],
        "sregym.conductor.problems.memcached_poisoned_entries:MemcachedPoisonedEntries",
        _memcached_poison,
    ),
)
