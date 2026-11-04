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
"""Catalog family: Redis runtime faults (ACL revoke, maxmemory OOM, paused writes, poisoned timeline
entry, renamed timeline key, social-graph stale cache).

A target is sampled from the binding's Redis rows that list the kind in ``RedisInstance.faults``
(verified live), with its mode/severity/key from the same row. The state lives in the server (or
its data), so every draw declares the whole workload (field None).
"""

from __future__ import annotations

import random
from typing import Any

from sregym.conductor.problems.composition.app_bindings import BINDINGS, instances
from sregym.conductor.problems.composition.catalog._kit import Pick, deployment, entry
from sregym.conductor.problems.composition.spec import CatalogEntry

ENABLED_APPS: dict[str, tuple[str, ...]] = {
    "redis_acl_revoked": ("social_network",),
    "redis_out_of_memory": ("social_network",),
    "redis_writes_paused": ("social_network",),
    "redis_poisoned_timeline_entry": ("social_network",),
    "redis_timeline_key_renamed": ("social_network",),
    "social_graph_stale_cache": ("social_network",),
}

_ACL_MODES = ("write", "read")  # mirrors components.redis.ACL_MODES
_OOM_FRACTIONS = (0.1, 0.5)  # of used memory; above 0.5 writes only fail intermittently (live)
_PAUSE_MS = (3_600_000, 7_200_000)  # outlasts any episode
_POISON_KINDS = ("non_numeric", "dangling_id")  # mirrors components.redis.POISON_KINDS
_GHOSTS = tuple(str(9202 + i) for i in range(8))  # user ids no seeded or probe user has


def _row(app: str, kind: str, rng: random.Random):
    rows = [r for r in instances(BINDINGS[app].redis) if kind in r.faults]
    if not rows:
        raise ValueError(f"{kind}: no verified redis target in the {app} binding")
    return rng.choice(rows)


def _choose_acl(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "redis_acl_revoked", rng)
    return Pick({"target": inst.workload, "mode": rng.choice(_ACL_MODES)}, (deployment(inst.workload),))


def _choose_oom(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "redis_out_of_memory", rng)
    return Pick({"target": inst.workload, "fraction": rng.choice(_OOM_FRACTIONS)}, (deployment(inst.workload),))


def _choose_paused(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "redis_writes_paused", rng)
    return Pick({"target": inst.workload, "duration_ms": rng.choice(_PAUSE_MS)}, (deployment(inst.workload),))


def _choose_poison(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "redis_poisoned_timeline_entry", rng)
    poison = rng.choice(_POISON_KINDS)
    token = f"{rng.randrange(10**9, 10**10)}" if poison == "dangling_id" else f"{rng.randrange(16**8):08x}"
    return Pick(
        {"target": inst.workload, "key": rng.choice(inst.poison_keys), "poison": poison, "token": token},
        (deployment(inst.workload),),
    )


def _choose_renamed(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "redis_timeline_key_renamed", rng)
    return Pick(
        {"target": inst.workload, "key": rng.choice(inst.rename_keys), "token": f"{rng.randrange(16**6):06x}"},
        (deployment(inst.workload),),
    )


def _choose_stale(app: str, rng: random.Random) -> Pick:
    inst = _row(app, "social_graph_stale_cache", rng)
    key, _ = rng.choice(inst.stale_sets)
    return Pick({"target": inst.workload, "key": key, "ghost": rng.choice(_GHOSTS)}, (deployment(inst.workload),))


def _builder(cls_name: str):
    def build(app: str, params: dict[str, Any]):
        from sregym.conductor.problems import redis_incident_faults

        return getattr(redis_incident_faults, cls_name)(app_name=app, **params)

    return build


_FAULTS = (
    ("redis_acl_revoked", _choose_acl, "RedisAclRevoked"),
    ("redis_out_of_memory", _choose_oom, "RedisOutOfMemory"),
    ("redis_writes_paused", _choose_paused, "RedisWritesPaused"),
    ("redis_poisoned_timeline_entry", _choose_poison, "RedisPoisonedTimelineEntry"),
    ("redis_timeline_key_renamed", _choose_renamed, "RedisTimelineKeyRenamed"),
    ("social_graph_stale_cache", _choose_stale, "SocialGraphStaleCache"),
)

ENTRIES: tuple[CatalogEntry, ...] = tuple(
    entry(key, ENABLED_APPS[key], choose, _builder(cls), generation=2) for key, choose, cls in _FAULTS
)
