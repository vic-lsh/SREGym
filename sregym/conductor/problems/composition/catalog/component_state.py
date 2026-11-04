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
"""Catalog family: runtime-state faults on bound components (Redis, memcached, ...).

Each entry is defined once per component kind and samples its target from the app binding
(``composition/app_bindings.py``). Apps are opt-in per key (``ENABLED_APPS``): enable a key for
another app once its binding rows are verified and the key validates live there
(``tools/fault_validation/validate.py``). The state lives in the component's pod, so every draw
declares the whole workload (field None).
"""

from __future__ import annotations

import random
from typing import Any

from sregym.conductor.problems.composition.app_bindings import BINDINGS, instances
from sregym.conductor.problems.composition.catalog._kit import Pick, deployment, entry
from sregym.conductor.problems.composition.spec import CatalogEntry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "redis_replica_misconfig": ("social_network",),
    "memcached_poisoned_entries": ("social_network",),
}

# Masters that can never serve a sync (a real full sync would flush the replica's data): connection
# refused, a blackholed address (connect timeout), a name that does not resolve.
_PHANTOM_MASTERS = ("127.0.0.1:1", "10.255.255.1:6379", "redis-primary.invalid:6379")


def _choose_redis_replica(app: str, rng: random.Random) -> Pick:
    inst = rng.choice(instances(BINDINGS[app].redis))
    return Pick({"target": inst.workload, "master": rng.choice(_PHANTOM_MASTERS)}, (deployment(inst.workload),))


def _build_redis_replica(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.redis_replica_misconfig import RedisReplicaMisconfig

    return RedisReplicaMisconfig(app_name=app, **params)


_CORRUPTIONS = ("garbage_bytes", "wrong_type", "stale_value")  # mirrors memcached.CORRUPTION_MODES


def _choose_memcached_poison(app: str, rng: random.Random) -> Pick:
    inst = rng.choice([m for m in instances(BINDINGS[app].memcached) if m.has_hot_keys])
    return Pick(
        {
            "target": inst.workload,
            "corruption": rng.choice(_CORRUPTIONS),
            "key_scope": rng.choice(("one_key", "all_hot_keys")),
            "poison_seed": rng.randrange(2**31),
        },
        (deployment(inst.workload),),
    )


def _build_memcached_poison(app: str, params: dict[str, Any]):
    from sregym.conductor.problems.memcached_poisoned_entries import MemcachedPoisonedEntries

    return MemcachedPoisonedEntries(app_name=app, **params)


ENTRIES: tuple[CatalogEntry, ...] = (
    entry(
        "redis_replica_misconfig",
        ENABLED_APPS["redis_replica_misconfig"],
        _choose_redis_replica,
        _build_redis_replica,
    ),
    entry(
        "memcached_poisoned_entries",
        ENABLED_APPS["memcached_poisoned_entries"],
        _choose_memcached_poison,
        _build_memcached_poison,
    ),
)
