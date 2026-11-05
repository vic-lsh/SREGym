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

import os
import random

from sregym.conductor.problems.composition.app_bindings import AppBinding, instances
from sregym.conductor.problems.composition.catalog._component import Target, component_entry, pick_one
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

# Generation 3: the single-record kinds (poisoned entry, renamed key, stale cache) draw a wide or partial scope
# over the row's per-user keyspace. The original single-record draw (one fixed key, SREGym's own probe user) stays
# in generations 1-2 and is drawn in generation 3 only on an explicit opt-in: this environment variable set to 1.
SINGLE_SCOPE_ENV = "SREGYM_ALLOW_SINGLE_SCOPE"
SCOPED = ("partial", "wide")  # problems.scope.SCOPED
SCOPES = ("single", "partial", "wide")  # problems.scope.SCOPES
PARTIAL_FRACTIONS = (0.25, 0.5)  # problems.scope.PARTIAL_FRACTIONS


def single_scope_allowed() -> bool:
    return os.environ.get(SINGLE_SCOPE_ENV) == "1"


def _row(binding: AppBinding, kind: str, rng: random.Random):
    rows = tuple(r for r in instances(binding.redis) if kind in r.faults)
    return pick_one(rows, f"redis target for {kind}", binding.app, rng)


def _choose_acl(binding: AppBinding, rng: random.Random) -> Target:
    return Target({"target": _row(binding, "redis_acl_revoked", rng).workload, "mode": rng.choice(_ACL_MODES)})


def _choose_oom(binding: AppBinding, rng: random.Random) -> Target:
    return Target(
        {"target": _row(binding, "redis_out_of_memory", rng).workload, "fraction": rng.choice(_OOM_FRACTIONS)}
    )


def _choose_paused(binding: AppBinding, rng: random.Random) -> Target:
    return Target({"target": _row(binding, "redis_writes_paused", rng).workload, "duration_ms": rng.choice(_PAUSE_MS)})


def _choose_poison(binding: AppBinding, rng: random.Random) -> Target:
    inst = _row(binding, "redis_poisoned_timeline_entry", rng)
    poison = rng.choice(_POISON_KINDS)
    token = f"{rng.randrange(10**9, 10**10)}" if poison == "dangling_id" else f"{rng.randrange(16**8):08x}"
    return Target({"target": inst.workload, "key": rng.choice(inst.poison_keys), "poison": poison, "token": token})


def _choose_renamed(binding: AppBinding, rng: random.Random) -> Target:
    inst = _row(binding, "redis_timeline_key_renamed", rng)
    return Target(
        {"target": inst.workload, "key": rng.choice(inst.rename_keys), "token": f"{rng.randrange(16**6):06x}"}
    )


def _choose_stale(binding: AppBinding, rng: random.Random) -> Target:
    inst = _row(binding, "social_graph_stale_cache", rng)
    key, _ = rng.choice(inst.stale_sets)
    return Target({"target": inst.workload, "key": key, "ghost": rng.choice(_GHOSTS)})


def _scoped_row(binding: AppBinding, kind: str, rng: random.Random):
    rows = tuple(r for r in instances(binding.redis) if kind in r.scoped_faults)
    return pick_one(rows, f"scoped redis target for {kind}", binding.app, rng)


def _scope_params(rng: random.Random) -> dict:
    scope = rng.choice(SCOPES if single_scope_allowed() else SCOPED)
    params: dict = {"scope": scope, "pick_seed": rng.randrange(10**6)}
    if scope == "partial":
        params["fraction"] = rng.choice(PARTIAL_FRACTIONS)
    return params


def _choose_poison_g3(binding: AppBinding, rng: random.Random) -> Target:
    poison = rng.choice(_POISON_KINDS)
    token = f"{rng.randrange(10**9, 10**10)}" if poison == "dangling_id" else f"{rng.randrange(16**8):08x}"
    # the scope is drawn first so the single draw (a fixed key on a verified row) is the original one
    scoped = _scope_params(rng)
    if scoped["scope"] == "single":
        inst = _row(binding, "redis_poisoned_timeline_entry", rng)
        return Target(
            {"target": inst.workload, "key": rng.choice(inst.poison_keys), "poison": poison, "token": token, **scoped}
        )
    inst = _scoped_row(binding, "redis_poisoned_timeline_entry", rng)
    return Target({"target": inst.workload, "poison": poison, "token": token, **scoped})


def _choose_renamed_g3(binding: AppBinding, rng: random.Random) -> Target:
    token = f"{rng.randrange(16**6):06x}"
    scoped = _scope_params(rng)
    if scoped["scope"] == "single":
        inst = _row(binding, "redis_timeline_key_renamed", rng)
        return Target({"target": inst.workload, "key": rng.choice(inst.rename_keys), "token": token, **scoped})
    inst = _scoped_row(binding, "redis_timeline_key_renamed", rng)
    return Target({"target": inst.workload, "token": token, **scoped})


def _choose_stale_g3(binding: AppBinding, rng: random.Random) -> Target:
    ghost = rng.choice(_GHOSTS)
    scoped = _scope_params(rng)
    if scoped["scope"] == "single":
        inst = _row(binding, "social_graph_stale_cache", rng)
        key, _ = rng.choice(inst.stale_sets)
        return Target({"target": inst.workload, "key": key, "ghost": ghost, **scoped})
    inst = _scoped_row(binding, "social_graph_stale_cache", rng)
    return Target({"target": inst.workload, "ghost": ghost, **scoped})


_MODULE = "sregym.conductor.problems.redis_incident_faults"
ENTRIES: tuple[CatalogEntry, ...] = tuple(
    component_entry(
        key, ENABLED_APPS[key], f"{_MODULE}:{cls}", choose, revisions={3: g3} if g3 is not None else None
    )
    for key, cls, choose, g3 in (
        ("redis_acl_revoked", "RedisAclRevoked", _choose_acl, None),
        ("redis_out_of_memory", "RedisOutOfMemory", _choose_oom, None),
        ("redis_writes_paused", "RedisWritesPaused", _choose_paused, None),
        ("redis_poisoned_timeline_entry", "RedisPoisonedTimelineEntry", _choose_poison, _choose_poison_g3),
        ("redis_timeline_key_renamed", "RedisTimelineKeyRenamed", _choose_renamed, _choose_renamed_g3),
        ("social_graph_stale_cache", "SocialGraphStaleCache", _choose_stale, _choose_stale_g3),
    )
)
