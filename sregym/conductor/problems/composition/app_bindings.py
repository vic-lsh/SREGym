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
"""App bindings: each app's component instances, as data, for app-neutral component faults.

Pure module: standard library plus the pure ``grading`` table. Three layers make a component
fault (docs/incident-faults.md):

* component drivers (``sregym/generators/fault/components/<kind>.py``): app-neutral inject,
  reference-fix, decoy and check primitives that take one instance below;
* app bindings (this module): which instances of each kind an app has, how to reach them, and
  the app's seeded-data integrity check;
* catalog entries (``catalog/component_state.py`` and later families): sample a target from the
  binding's instances, so enabling a fault kind for another app is a binding row plus a
  reliability decision, not new code.

``verified`` on a binding row records whether the row was checked against a live deployment.
Only verified rows are sampled by catalog entries (``instances``).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from sregym.conductor.problems.composition.grading import ENTRY_POINTS, SOCIAL_PROBE_USERNAME, EntryPoint, Request


@dataclass(frozen=True)
class RedisInstance:
    """A Redis-protocol server (Redis or Valkey) run by one workload.

    ``holds_primary_data``: the server is the source of truth for its data (nothing behind it),
    so restarting it loses user data and a restart is a decoy; False for a rebuildable cache in
    front of a database, where a restart is an acceptable repair.
    """

    workload: str
    holds_primary_data: bool
    port: int = 6379
    cli: str = "redis-cli"  # "valkey-cli" for Valkey images
    container: str | None = None  # None: the pod's first container
    kind: str = "Deployment"
    verified: bool = True


@dataclass(frozen=True)
class MemcachedInstance:
    """A memcached server and the hot keys its consumers read.

    Hot keys are ``keys`` (literal) plus, when ``key_field`` is set, every value of that JSON
    field in the app's integrity snapshot (post ids in a timeline). ``warm`` requests make the
    consumer cache those keys before a fault reads them. ``consumers`` are the workloads that
    read the cache (rollout-restarting one is a decoy).
    """

    workload: str
    consumers: tuple[str, ...]
    keys: tuple[str, ...] = ()
    key_field: str | None = None
    warm: tuple[Request, ...] = ()
    port: int = 11211
    kind: str = "Deployment"
    verified: bool = True

    @property
    def has_hot_keys(self) -> bool:
        return bool(self.keys) or self.key_field is not None


@dataclass(frozen=True)
class MongoInstance:
    workload: str
    database: str
    collections: tuple[str, ...] = ()
    port: int = 27017
    kind: str = "Deployment"
    verified: bool = True


@dataclass(frozen=True)
class ServiceInstance:
    """An application process reachable on a port (thrift, gRPC, HTTP). A ``proxy`` is the
    user-facing reverse proxy (nginx-thrift, envoy frontend-proxy)."""

    workload: str
    port: int
    protocol: str  # "thrift" | "grpc" | "http"
    proxy: bool = False
    kind: str = "Deployment"
    container: str | None = None  # None: the pod's first container
    verified: bool = True


@dataclass(frozen=True)
class WorkloadTarget:
    """Any bound workload seen as a process: the common shape pod-level drivers (freeze a process,
    shape its network, hold connections open) need, derived from the typed rows."""

    workload: str
    kind: str
    container: str | None
    port: int
    protocol: str
    holds_primary_data: bool  # True: a restart loses the only copy of user data (restart is a decoy)
    proxy: bool = False


@dataclass(frozen=True)
class IntegrityCheck:
    """Seeded-data snapshot-and-compare, app-neutral.

    Before injection the fault requests every ``reads`` path at the entry point and keeps the
    normalized bodies; after mitigation the same reads must return the same data. If any read
    comes back empty (``""``, ``[]``, ``{}``), ``seed`` runs once first, so the snapshot holds
    real data. ``volatile_keys`` are JSON object keys dropped before comparing.
    """

    reads: tuple[str, ...]
    seed: tuple[Request, ...] = ()
    volatile_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class AppBinding:
    app: str  # catalog app key (hotel_reservation, social_network, ...)
    namespace: str
    entry: EntryPoint
    redis: tuple[RedisInstance, ...] = ()
    memcached: tuple[MemcachedInstance, ...] = ()
    mongo: tuple[MongoInstance, ...] = ()
    services: tuple[ServiceInstance, ...] = ()
    integrity: IntegrityCheck | None = None
    notes: tuple[str, ...] = field(default=())


def workload_targets(binding: AppBinding, *, verified_only: bool = True) -> tuple[WorkloadTarget, ...]:
    """Every bound workload as a ``WorkloadTarget``. Mongo instances always hold primary data; memcached
    and services never do; a Redis says so itself."""
    out = [
        *(
            WorkloadTarget(r.workload, r.kind, r.container, r.port, "redis", r.holds_primary_data)
            for r in instances(binding.redis, verified_only=verified_only)
        ),
        *(
            WorkloadTarget(m.workload, m.kind, None, m.port, "memcached", False)
            for m in instances(binding.memcached, verified_only=verified_only)
        ),
        *(
            WorkloadTarget(m.workload, m.kind, None, m.port, "mongo", True)
            for m in instances(binding.mongo, verified_only=verified_only)
        ),
        *(
            WorkloadTarget(s.workload, s.kind, s.container, s.port, s.protocol, False, s.proxy)
            for s in instances(binding.services, verified_only=verified_only)
        ),
    ]
    return tuple(out)


def instances(rows: tuple, *, verified_only: bool = True) -> tuple:
    """The rows a catalog entry may sample: verified ones unless asked otherwise."""
    return tuple(r for r in rows if r.verified or not verified_only)


# --- social_network ---------------------------------------------------------------------------------
# Seeded-data users: ids outside the wrk2 workload range (1..962) and the traffic probe's (9200..),
# so only faults and mitigations change what they read. 9101 follows 9100, so 9100's posts are in
# 9101's home timeline, which lives only in home-timeline-redis.

_SOCIAL_AUTHOR, _SOCIAL_FOLLOWER = 9100, 9101


def _register(uid: int) -> Request:
    name = f"sregym_seed_{uid}"
    return Request(
        "/wrk2-api/user/register",
        (
            ("first_name", "seed"),
            ("last_name", str(uid)),
            ("username", name),
            ("password", name),
            ("user_id", str(uid)),
        ),
    )


def _compose(uid: int, text: str) -> Request:
    return Request(
        "/wrk2-api/post/compose",
        (("user_id", str(uid)), ("username", f"sregym_seed_{uid}"), ("post_type", "0"), ("text", text)),
    )


_SOCIAL_SEED = (
    _register(_SOCIAL_AUTHOR),
    _register(_SOCIAL_FOLLOWER),
    Request("/wrk2-api/user/follow", (("user_id", str(_SOCIAL_FOLLOWER)), ("followee_id", str(_SOCIAL_AUTHOR)))),
    *(_compose(_SOCIAL_AUTHOR, f"sregym seeded post {i}") for i in range(3)),
)

_SOCIAL_PROBE_LOGIN = Request(
    "/api/user/login", (("username", SOCIAL_PROBE_USERNAME), ("password", SOCIAL_PROBE_USERNAME))
)

SOCIAL_NETWORK = AppBinding(
    app="social_network",
    namespace="social-network",
    entry=ENTRY_POINTS["Social Network"],
    redis=(
        RedisInstance("home-timeline-redis", holds_primary_data=True),
        RedisInstance("user-timeline-redis", holds_primary_data=False),  # cache over user-timeline-mongodb
        RedisInstance("social-graph-redis", holds_primary_data=False),  # cache over social-graph-mongodb
    ),
    memcached=(
        # user-service caches "<username>:login" (password hash, salt, id) on login.
        MemcachedInstance(
            "user-memcached",
            consumers=("user-service",),
            keys=(f"{SOCIAL_PROBE_USERNAME}:login",),  # the traffic probe logs in as this user
            warm=(*ENTRY_POINTS["Social Network"].setup, _SOCIAL_PROBE_LOGIN),
        ),
        # post-storage-service caches each post's JSON under its decimal post id on read.
        MemcachedInstance("post-storage-memcached", consumers=("post-storage-service",), key_field="post_id"),
        # Only GetExtendedUrls and media reads would use these; no entry point reaches them.
        MemcachedInstance("url-shorten-memcached", consumers=("url-shorten-service",), verified=False),
        MemcachedInstance("media-memcached", consumers=("media-service",), verified=False),
    ),
    mongo=tuple(
        MongoInstance(f"{name}-mongodb", database=name, verified=False)
        for name in ("media", "post-storage", "social-graph", "url-shorten", "user", "user-timeline")
    ),
    services=(
        ServiceInstance("nginx-thrift", 8080, "http", proxy=True),
        *(
            ServiceInstance(name, 9090, "thrift")
            for name in (
                "compose-post-service",
                "home-timeline-service",
                "media-service",
                "post-storage-service",
                "social-graph-service",
                "text-service",
                "unique-id-service",
                "url-shorten-service",
                "user-mention-service",
                "user-service",
                "user-timeline-service",
            )
        ),
    ),
    integrity=IntegrityCheck(
        reads=(
            f"/wrk2-api/user-timeline/read?user_id={_SOCIAL_AUTHOR}&start=0&stop=20",
            f"/wrk2-api/home-timeline/read?user_id={_SOCIAL_FOLLOWER}&start=0&stop=20",
        ),
        seed=_SOCIAL_SEED,
    ),
)

# --- hotel_reservation (from its manifests; not validated live) -------------------------------------

HOTEL_RESERVATION = AppBinding(
    app="hotel_reservation",
    namespace="hotel-reservation",
    entry=ENTRY_POINTS["Hotel Reservation"],
    memcached=tuple(
        MemcachedInstance(f"memcached-{name}", consumers=(name,), verified=False)
        for name in ("profile", "rate", "reservation")
    ),
    mongo=tuple(
        MongoInstance(f"mongodb-{name}", database=f"{name}-db", verified=False)
        for name in ("geo", "profile", "rate", "recommendation", "reservation", "user")
    ),
    services=(
        ServiceInstance("frontend", 5000, "http", proxy=True, verified=False),
        *(
            ServiceInstance(name, port, "grpc", verified=False)
            for name, port in (
                ("geo", 8083),
                ("profile", 8081),
                ("rate", 8084),
                ("recommendation", 8085),
                ("reservation", 8087),
                ("search", 8082),
                ("user", 8086),
            )
        ),
    ),
)

# --- astronomy_shop (from its manifests; not validated live) ----------------------------------------

ASTRONOMY_SHOP = AppBinding(
    app="astronomy_shop",
    namespace="astronomy-shop",
    entry=ENTRY_POINTS["OpenTelemetry Demo Astronomy Shop"],
    redis=(RedisInstance("valkey-cart", holds_primary_data=True, cli="valkey-cli", verified=False),),
    services=(ServiceInstance("frontend-proxy", 8080, "http", proxy=True, verified=False),),
)

BINDINGS: Mapping[str, AppBinding] = {b.app: b for b in (SOCIAL_NETWORK, HOTEL_RESERVATION, ASTRONOMY_SHOP)}
