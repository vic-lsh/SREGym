"""MongoDB faults on the running server: write lock, write rejection, renamed collection, poison document.

Every manifest stays correct. MongoDB holds the only copy of its data and its pod has no volume, so
restarting it (rollout restart, deleting the pod) clears each fault by destroying the data: the
per-fault verdict also requires that sampled ``_id``s of the target collection recorded before the
fault are still present, which is how those decoys fail. Restoring source changes no object (no-op),
and restarting the consuming service leaves the database state alone.

* ``MongoWriteLock``: ``fsyncLock`` blocks writes (reads work). Reference: ``fsyncUnlock``.
* ``MongoWriteRejection``: a validator no document satisfies, or a partial unique index every new
  document collides with. Reference: clear the validator / drop the index and its sentinel.
* ``MongoCollectionRenamed``: the collection is renamed away; readers see nothing and writers
  recreate an empty one. Reference: merge the newer documents back and rename with ``dropTarget``.
* ``MongoPoisonDocument``: documents the entry points read are made malformed for their reader.
  Reference: repair them in place (lossless: the original value stays in the document).
"""

from __future__ import annotations

from typing import Any

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Expect, Repair
from sregym.conductor.problems.composition.app_bindings import (
    BINDINGS,
    MemcachedInstance,
    MongoCollection,
    MongoInstance,
    RedisInstance,
)
from sregym.generators.fault.components import http, memcached, mongo, redis
from sregym.generators.fault.components.kube import ComponentError

WRITE_MODES = ("validator", "unique_index")
POISON_SCOPES = ("one_doc", "all_seeded")


def resolve(app_name: str, target: str, collection: str | None) -> tuple[MongoInstance, MongoCollection]:
    """The binding row and collection for a target; ``collection`` None means its first collection."""
    matches = [m for m in BINDINGS[app_name].mongo if m.workload == target]
    if not matches:
        raise ValueError(f"{app_name} binding has no mongo instance {target!r}")
    inst = matches[0]
    if collection is None:
        if not inst.collections:
            raise ValueError(f"{target} lists no collections")
        return inst, inst.collections[0]
    found = [c for c in inst.collections if c.name == collection]
    if not found:
        raise ValueError(f"{target} binding has no collection {collection!r}")
    return inst, found[0]


class MongoFaultProblem(ComponentFaultProblem):
    """Shared plumbing: target resolution, seeding, cache flush. The data-loss witness, the write
    round trip and the standard repairs come from the base class (the target is a store whose
    restart loses data)."""

    fault_kind = ""

    def __init__(self, app_name: str, target: str, collection: str | None, description: str, namespace=None, kube=None):
        self.instance, self.coll = resolve(app_name, target, collection)
        super().__init__(app_name=app_name, target=target, description=description, namespace=namespace, kube=kube)

    def _ns_args(self) -> tuple[Any, str, MongoInstance, str]:
        return self.kube, self.namespace, self.instance, self.coll.name

    def prepare_target(self) -> None:
        """Seed again if a previous wrong repair (a restarted mongod) emptied the collection; runs
        before the snapshot, while the app is healthy."""
        if mongo.collection_count(*self._ns_args()) == 0 and self.binding.integrity is not None:
            http.fetch(self.kube, self.binding, self.namespace, self.binding.integrity.seed)

    def write_check(self):
        return self.coll.write_check

    def flush_cache(self) -> None:
        """Empty the cache in front of the collection (a restart of a cache is harmless)."""
        name = self.coll.cache
        if name is None:
            return
        row = self.binding.row(name)
        if isinstance(row, MemcachedInstance):
            memcached.flush_all(self.kube, self.namespace, row)
        elif isinstance(row, RedisInstance):
            redis.cli(self.kube, self.namespace, row, "FLUSHALL")
        else:
            raise ComponentError(f"{self.namespace}: cache {name!r} of {self.coll.name} is not a cache row")


class MongoWriteLock(MongoFaultProblem):
    fault_kind = "mongo_write_lock"
    blocks_writes = True

    def __init__(self, app_name: str, target: str, namespace=None, kube=None):
        super().__init__(
            app_name,
            target,
            None,
            f"MongoDB {target} was fsync-locked at runtime (fsyncLock). Reads still work, but every write to it "
            "blocks and then times out, so requests that write through it fail.",
            namespace,
            kube,
        )

    def inject(self) -> None:
        mongo.fsync_lock(self.kube, self.namespace, self.instance)
        if not mongo.lock_state(self.kube, self.namespace, self.instance)["locked"]:
            raise ComponentError(f"{self.instance.workload} in {self.namespace}: fsyncLock did not take")

    def fault_cleared(self) -> tuple[bool, str]:
        locked = mongo.lock_state(self.kube, self.namespace, self.instance)["locked"]
        return not locked, f"{self.instance.workload} fsyncLock={'held' if locked else 'released'}"

    def reference_mitigation(self) -> None:
        mongo.fsync_unlock(self.kube, self.namespace, self.instance)


class MongoWriteRejection(MongoFaultProblem):
    fault_kind = "mongo_write_rejection"
    blocks_writes = True

    def __init__(self, app_name: str, target: str, collection: str, mode: str, namespace=None, kube=None):
        if mode not in WRITE_MODES:
            raise ValueError(f"unknown mode {mode!r}")
        self.mode = mode
        what = (
            "a schema validator that no document satisfies"
            if mode == "validator"
            else "a unique index that every new document collides with"
        )
        super().__init__(
            app_name,
            target,
            collection,
            f"MongoDB collection {collection} in {target} gained {what} at runtime. Reads work, but every "
            "write is rejected by the database, so requests that write through it fail.",
            namespace,
            kube,
        )
        if mode == "unique_index" and not (self.coll.unique_key and self.coll.stamp_field):
            raise ValueError(f"{target}.{collection} has no unique_key/stamp_field for mode unique_index")

    def inject(self) -> None:
        if self.mode == "validator":
            mongo.set_validator(*self._ns_args())
        else:
            mongo.add_blocking_unique_index(*self._ns_args(), self.coll.unique_key, self.coll.stamp_field)
        cleared, detail = self.fault_cleared()
        if cleared:
            raise ComponentError(f"{self.instance.workload} in {self.namespace}: {self.mode} did not take ({detail})")

    def fault_cleared(self) -> tuple[bool, str]:
        if self.mode == "validator":
            st = mongo.validator_state(*self._ns_args())
            return not st["validator"], f"{self.coll.name} validator={'set' if st['validator'] else 'cleared'}"
        st = mongo.blocking_index_state(*self._ns_args())
        return not (
            st["index"] or st["sentinel"]
        ), f"{self.coll.name} blocking index={st['index']} sentinel={st['sentinel']}"

    def reference_mitigation(self) -> None:
        if self.mode == "validator":
            mongo.clear_validator(*self._ns_args())
        else:
            mongo.drop_blocking_unique_index(*self._ns_args())


class MongoCollectionRenamed(MongoFaultProblem):
    fault_kind = "mongo_collection_renamed"

    def __init__(self, app_name: str, target: str, collection: str, namespace=None, kube=None):
        super().__init__(
            app_name,
            target,
            collection,
            f"The MongoDB collection {collection} in {target} was renamed to {collection}{mongo.RENAME_SUFFIX} at "
            "runtime. Readers find an empty collection, writers silently create a new empty one, and the old "
            "data is no longer served.",
            namespace,
            kube,
        )

    def inject(self) -> None:
        mongo.rename_away(*self._ns_args())
        self.flush_cache()  # a warm cache would hide the missing data
        if not mongo.collection_state(*self._ns_args())["old_exists"]:
            raise ComponentError(f"{self.instance.workload} in {self.namespace}: rename did not take")

    def fault_cleared(self) -> tuple[bool, str]:
        st = mongo.collection_state(*self._ns_args())
        return not st["old_exists"], (
            f"{self.coll.name}: {st['count']} documents, {self.coll.name}{mongo.RENAME_SUFFIX} "
            f"{'holds ' + str(st['old_count']) if st['old_exists'] else 'gone'}"
        )

    def reference_mitigation(self) -> None:
        mongo.merge_back(*self._ns_args())
        self.flush_cache()  # readers may have cached the empty view


class MongoPoisonDocument(MongoFaultProblem):
    fault_kind = "mongo_poison_document"
    STATE_ATTRS = ("poisoned",)

    def __init__(self, app_name: str, target: str, collection: str, shape: str, scope: str, namespace=None, kube=None):
        if shape not in mongo.POISON_SHAPES:
            raise ValueError(f"unknown shape {shape!r}")
        if scope not in POISON_SCOPES:
            raise ValueError(f"unknown scope {scope!r}")
        self.shape, self.scope = shape, scope
        self.poisoned: tuple[str, ...] = ()
        super().__init__(
            app_name,
            target,
            collection,
            f"Documents in the MongoDB collection {collection} of {target} were made malformed "
            f"({'a string field holds an object' if shape == 'wrong_type' else 'a required field was renamed'}). "
            "The service that reads them cannot decode them, so every request that reads those documents fails.",
            namespace,
            kube,
        )
        if not (self.coll.key_field and len(self.coll.poison_fields) == 2):
            raise ValueError(f"{target}.{collection} has no key_field/poison_fields in its binding")

    def _doc_args(self) -> tuple[Any, ...]:
        return (*self._ns_args(), self.coll.key_field, self.poisoned, self.shape, *self.coll.poison_fields)

    def inject(self) -> None:
        assert self.snapshot is not None and self.coll.key_field
        keys = self.snapshot.json_field_values(self.coll.key_field)
        if not keys:
            raise ComponentError(f"no {self.coll.key_field} values in the seeded-data snapshot")
        self.poisoned = tuple(keys[:1] if self.scope == "one_doc" else keys)
        self.poisoned = tuple(
            mongo.poison_documents(
                *self._ns_args(), self.coll.key_field, self.poisoned, self.shape, *self.coll.poison_fields
            )
        )
        self.flush_cache()  # a cached copy of the post would hide the malformed document

    def fault_cleared(self) -> tuple[bool, str]:
        if not self.poisoned:
            return False, "nothing poisoned (fault not injected here)"
        st = mongo.poison_state(*self._doc_args())
        return not st["poisoned"], (
            f"{len(st['poisoned'])}/{len(self.poisoned)} poisoned documents still malformed in {self.coll.name}"
        )

    def reference_mitigation(self) -> None:
        if self.poisoned:
            mongo.unpoison_documents(*self._doc_args())

    def extra_repairs(self) -> dict[str, Repair]:
        if not self.coll.cache:
            return {}
        # flushing the cache re-reads the same malformed documents
        return {"flush_cache": Repair(self.flush_cache, Expect.FAULT_NOT_CLEARED)}
