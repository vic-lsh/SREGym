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

from sregym.conductor.problems.composition.grading import (
    ENTRY_POINTS,
    SOCIAL_PROBE_USERNAME,
    EntryPoint,
    Request,
)


@dataclass(frozen=True)
class WriteCheck:
    """A write whose effect the entry points show: POST ``path`` with ``fields`` (the ``marker_field``
    value is replaced by a fresh marker each time), then ``read`` must return a body containing the
    marker. Needed where the app swallows a failed database write (social-network's compose is
    one-way and answers 200 whatever the database did), so an HTTP status probe cannot see it."""

    path: str
    fields: tuple[tuple[str, str], ...]
    marker_field: str
    read: str


@dataclass(frozen=True)
class RedisInstance:
    """A Redis-protocol server (Redis or Valkey) run by one workload.

    ``holds_primary_data``: the server is the source of truth for its data (nothing behind it),
    so restarting it loses user data and a restart is a decoy; False for a rebuildable cache in
    front of a database, where a restart is an acceptable repair.
    ``consumers``: services that talk to it (restarting one is a decoy).
    ``write_check``: an entry-point write that lands in this server and the read that shows it;
    the store witness writes a marker through it while a fault is live (where writes work).
    """

    workload: str
    holds_primary_data: bool
    port: int = 6379
    cli: str = "redis-cli"  # "valkey-cli" for Valkey images
    container: str | None = None  # None: the pod's first container
    kind: str = "Deployment"
    verified: bool = True
    consumers: tuple[str, ...] = ()
    write_check: WriteCheck | None = None
    # Incident fault kinds verified live on this server (catalog keys; only those sample it) and the
    # keys they act on: sorted-set keys a poisoned member may go into, keys a rename hides, and
    # (key, member) pairs whose member a stale-cache fault drops.
    faults: tuple[str, ...] = ()
    poison_keys: tuple[str, ...] = ()
    rename_keys: tuple[str, ...] = ()
    stale_sets: tuple[tuple[str, str], ...] = ()

    @property
    def restart_loses_data(self) -> bool:
        return self.holds_primary_data


@dataclass(frozen=True)
class MemcachedInstance:
    """A memcached server and the hot keys its consumers read.

    Hot keys are ``keys`` (literal) plus, when ``key_field`` is set, every value of that JSON
    field in the app's integrity snapshot (post ids in a timeline), formatted into ``key_template``
    when the cache key is derived from the value. ``warm`` requests make the
    consumer cache those keys before a fault reads them. ``consumers`` are the workloads that
    read the cache (rollout-restarting one is a decoy).
    """

    workload: str
    consumers: tuple[str, ...]
    keys: tuple[str, ...] = ()
    key_field: str | None = None
    key_template: str = "{}"  # cache key of a ``key_field`` value ("{}_cap": hotel-reservation capacities)
    warm: tuple[Request, ...] = ()
    port: int = 11211
    kind: str = "Deployment"
    verified: bool = True

    @property
    def has_hot_keys(self) -> bool:
        return bool(self.keys) or self.key_field is not None

    @property
    def restart_loses_data(self) -> bool:
        return False  # a cache: consumers refill it


@dataclass(frozen=True)
class MongoCollection:
    """One collection of a Mongo instance and what the Mongo faults need to know about it.

    ``cache``: workload (a memcached or Redis row of the binding) that fronts reads of this
    collection; faults whose symptom is hidden by a warm cache flush it after injecting.
    ``key_field``: a field of the collection's documents that also appears in the app's integrity
    snapshot (post ids), so a document fault can pick documents the entry points read.
    ``poison_fields``: (string field, object field) a malformed-document fault may break.
    ``unique_key``/``stamp_field``: an indexable field every document carries and a numeric
    timestamp field; together they let the unique-index write rejection reject every NEW document
    (a partial unique index over documents newer than now) while existing ones are unaffected.
    ``write_check``: a write plus the read that must show it, used by faults that block writes.
    ``faults``: the Mongo fault kinds verified live on this collection (only those sample it).
    ``merge``: how the reference repair of a renamed collection folds the documents written during
    the fault into the old ones: ``append_only`` (one document per event: insert the new ids) or
    ``union_arrays`` (one aggregate per key: also add the new array elements to the old document);
    ``merge_key`` is the field documents are matched by (the one services look them up by: ``_id`` is
    an ObjectId generated on insert, so documents of a recreated collection never share it).
    """

    name: str
    cache: str | None = None
    key_field: str | None = None
    poison_fields: tuple[str, ...] = ()
    unique_key: str | None = None
    stamp_field: str | None = None
    write_check: WriteCheck | None = None
    faults: tuple[str, ...] = ()
    merge: str = "append_only"
    merge_key: str = "_id"

    def __post_init__(self) -> None:
        if self.merge not in ("append_only", "union_arrays"):
            raise ValueError(f"collection {self.name}: unknown merge strategy {self.merge!r}")


# Compose as the traffic probe's user (9200) and read it back from that user's timeline. Compose is
# one-way, so a database that refuses the write still answers 200; only the missing post shows it.
# Probe user 9200 composes; 9201 follows it (grading.ENTRY_POINTS setup). Reads go deep enough
# that the traffic probe's own composes during one trial do not push a marker out of the window.
_SOCIAL_COMPOSE_CHECK = WriteCheck(
    "/wrk2-api/post/compose",
    (("user_id", "9200"), ("username", "sregym_probe_9200"), ("post_type", "0"), ("text", "")),
    "text",
    "/wrk2-api/user-timeline/read?user_id=9200&start=0&stop=100",
)
_SOCIAL_HOME_CHECK = WriteCheck(
    "/wrk2-api/post/compose",
    (("user_id", "9200"), ("username", "sregym_probe_9200"), ("post_type", "0"), ("text", "")),
    "text",
    "/wrk2-api/home-timeline/read?user_id=9201&start=0&stop=100",
)

REDIS_FAULTS = (
    "redis_acl_revoked",
    "redis_out_of_memory",
    "redis_writes_paused",
    "redis_poisoned_timeline_entry",
    "redis_timeline_key_renamed",
    "social_graph_stale_cache",
)


@dataclass(frozen=True)
class MongoInstance:
    """A mongod run by one workload. By default it holds the only copy of its data (nothing is
    behind it, and the pod has no volume), so restarting it is a decoy. ``persistent``: its data
    directory is a volume (hotel-reservation's PVCs), so a restart keeps the data and keeps
    on-disk fault state (validators, renamed collections, poisoned documents) but still clears
    process state (an fsync lock). ``consumers``: services whose restart is a decoy too."""

    workload: str
    database: str
    collections: tuple[MongoCollection, ...] = ()
    consumers: tuple[str, ...] = ()
    port: int = 27017
    kind: str = "Deployment"
    shell: str = "mongo"  # the legacy shell of mongo:4.4; "mongosh" on newer images
    container: str | None = None
    verified: bool = True
    # (username, password) for an instance started with authentication (hotel's mongodb-* run
    # createUser scripts); None: unauthenticated, as in social-network.
    auth: tuple[str, str] | None = None
    persistent: bool = False

    @property
    def restart_loses_data(self) -> bool:
        return not self.persistent  # the only copy unless the data directory is a volume


def mongo_targets(rows: tuple[MongoInstance, ...], fault: str) -> tuple[tuple[MongoInstance, MongoCollection], ...]:
    """(instance, collection) pairs of verified rows whose collection lists ``fault`` as verified."""
    return tuple((r, c) for r in instances(rows) for c in r.collections if fault in c.faults)


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
    consumers: tuple[str, ...] = ()

    @property
    def restart_loses_data(self) -> bool:
        return False  # stateless


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
    ``unordered_keys``: JSON keys whose list values the app returns in a varying order (hotel's
    ``features``); they are compared as sets. ``depends_on``: workloads whose stored data the reads return and whose restart loses it; the
    generator never composes a fault on one of them with a fault that relies on this check.
    """

    reads: tuple[str, ...]
    seed: tuple[Request, ...] = ()
    volatile_keys: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    unordered_keys: tuple[str, ...] = ()


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

    def rows(self) -> tuple:
        return (*self.redis, *self.memcached, *self.mongo, *self.services)

    def row(self, workload: str, kind: type | None = None):
        """The binding row run by ``workload`` (of row type ``kind`` if given); ValueError if none."""
        found = [r for r in self.rows() if r.workload == workload and (kind is None or isinstance(r, kind))]
        if not found:
            what = kind.__name__ if kind else "row"
            raise ValueError(f"{self.app} binding has no {what} for workload {workload!r}")
        return found[0]


StoreRow = RedisInstance | MemcachedInstance | MongoInstance
BindingRow = StoreRow | ServiceInstance


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
        RedisInstance(
            "home-timeline-redis",
            holds_primary_data=True,
            consumers=("home-timeline-service",),
            write_check=_SOCIAL_HOME_CHECK,
            # Keys are user ids. 9101 is the seeded follower whose home timeline the integrity check reads.
            faults=REDIS_FAULTS[:5],
            poison_keys=(str(_SOCIAL_FOLLOWER),),
            rename_keys=(str(_SOCIAL_FOLLOWER),),
        ),
        # caches over user-timeline-mongodb and social-graph-mongodb (a key lost from either refills from Mongo)
        RedisInstance(
            "user-timeline-redis",
            holds_primary_data=False,
            consumers=("user-timeline-service",),
            write_check=_SOCIAL_COMPOSE_CHECK,
            faults=REDIS_FAULTS[:4],
            poison_keys=(str(_SOCIAL_AUTHOR),),
        ),
        RedisInstance(
            "social-graph-redis",
            holds_primary_data=False,
            consumers=("social-graph-service",),
            write_check=_SOCIAL_HOME_CHECK,  # compose fans out to the author's followers: reads the follower's home timeline
            faults=(REDIS_FAULTS[0], REDIS_FAULTS[1], REDIS_FAULTS[3], REDIS_FAULTS[5]),
            poison_keys=("9200:followers",),  # the traffic probe's author
            stale_sets=(("9200:followers", "9201"),),
        ),
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
    mongo=(
        # Faults are listed per collection once verified live (docs/incident-faults.md, Mongo).
        MongoInstance(
            "user-timeline-mongodb",
            "user-timeline",
            (
                MongoCollection(
                    "user-timeline",
                    cache="user-timeline-redis",
                    write_check=_SOCIAL_COMPOSE_CHECK,
                    faults=("mongo_write_lock", "mongo_write_rejection", "mongo_collection_renamed"),
                    merge="union_arrays",  # one document per user holding that user's posts
                    merge_key="user_id",
                ),
            ),
            consumers=("user-timeline-service",),
        ),
        MongoInstance(
            "post-storage-mongodb",
            "post",
            (
                MongoCollection(
                    "post",
                    cache="post-storage-memcached",
                    key_field="post_id",
                    poison_fields=("text", "creator"),
                    merge_key="post_id",
                    unique_key="post_type",
                    stamp_field="timestamp",
                    write_check=_SOCIAL_COMPOSE_CHECK,
                    faults=(
                        "mongo_write_lock",
                        "mongo_write_rejection",
                        "mongo_collection_renamed",
                        "mongo_poison_document",
                    ),
                ),
            ),
            consumers=("post-storage-service",),
        ),
        MongoInstance(
            "social-graph-mongodb",
            "social-graph",
            (MongoCollection("social-graph", cache="social-graph-redis", faults=("mongo_write_lock",)),),
            consumers=("social-graph-service",),
        ),
        MongoInstance(
            "user-mongodb",
            "user",
            # No fault: nothing the traffic probe or the write checks do writes user-mongodb (register is
            # setup-only and its outcome is not graded; login reads through user-memcached), so a lock,
            # validator or rename here has no user-visible symptom.
            (MongoCollection("user", cache="user-memcached", faults=()),),
            ("user-service",),
        ),
        # No service reads or writes these: not valid targets.
        MongoInstance("media-mongodb", "media", verified=False),
        MongoInstance("url-shorten-mongodb", "url-shorten", verified=False),
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
        # user timelines: user-timeline-redis is a cache over user-timeline-mongodb; posts:
        # post-storage-memcached over post-storage-mongodb; home timelines: only home-timeline-redis.
        depends_on=("home-timeline-redis", "user-timeline-mongodb", "post-storage-mongodb"),
    ),
)

# --- hotel_reservation ------------------------------------------------------------------------------
# Every mongod keeps its data on a PVC (a restart keeps it), and the services seed their database at
# startup. geo and rate run with --auth (admin/admin). The frontend takes GET query parameters.

_HOTEL_SEARCH = "/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"
_HOTEL_LOGIN = "/user?username=Cornell_5&password=5555555555"

HOTEL_RESERVATION = AppBinding(
    app="hotel_reservation",
    namespace="hotel-reservation",
    entry=ENTRY_POINTS["Hotel Reservation"],
    memcached=(
        # profile and rate cache each hotel under its id; the hotels in the snapshot's reads.
        MemcachedInstance("memcached-profile", consumers=("profile",), key_field="id"),
        MemcachedInstance("memcached-rate", consumers=("rate",), key_field="id"),
        # reservation caches "<hotel>_cap" and "<hotel>_<in>_<out>" counts.
        MemcachedInstance(
            "memcached-reserve",
            consumers=("reservation",),
            key_field="id",
            key_template="{}_cap",
        ),
    ),
    mongo=(
        MongoInstance(
            "mongodb-geo",
            "geo-db",
            (MongoCollection("geo", faults=("mongo_collection_renamed",)),),
            consumers=("geo",),
            auth=("admin", "admin"),
            persistent=True,
        ),
        MongoInstance(
            "mongodb-profile",
            "profile-db",
            (
                MongoCollection(
                    "hotels",
                    cache="memcached-profile",
                    key_field="id",
                    poison_fields=("name", "address"),
                    faults=("mongo_collection_renamed", "mongo_poison_document"),
                ),
            ),
            consumers=("profile",),
            persistent=True,
        ),
        MongoInstance(
            "mongodb-rate",
            "rate-db",
            (MongoCollection("inventory", cache="memcached-rate", faults=("mongo_collection_renamed",)),),
            consumers=("rate",),
            auth=("admin", "admin"),
            persistent=True,
        ),
        MongoInstance(
            "mongodb-recommendation",
            "recommendation-db",
            (MongoCollection("recommendation"),),
            consumers=("recommendation",),
            persistent=True,
        ),
        MongoInstance(
            "mongodb-reservation",
            "reservation-db",
            (
                MongoCollection(
                    "reservation",
                    cache="memcached-reserve",
                    faults=("mongo_write_lock", "mongo_write_rejection", "mongo_collection_renamed"),
                ),
            ),
            consumers=("reservation",),
            persistent=True,
        ),
        MongoInstance(
            "mongodb-user",
            "user-db",
            (MongoCollection("user", faults=("mongo_collection_renamed",)),),
            consumers=("user",),
            persistent=True,
        ),
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
    # No seed: the services create their data at startup. recommendations?require=rate is left out
    # (ties in the rate sort make its order vary between identical requests). No depends_on: every
    # mongod keeps its data on a volume, so a restart loses nothing the reads return.
    integrity=IntegrityCheck(
        reads=(
            _HOTEL_SEARCH,
            "/recommendations?require=dis&lat=37.7749&lon=-122.4194",
            "/recommendations?require=price&lat=37.7749&lon=-122.4194",
            _HOTEL_LOGIN,
        ),
        unordered_keys=("features",),
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
