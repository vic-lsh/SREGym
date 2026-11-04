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

import secrets
import time
from typing import Any

from sregym.conductor.problems.component_fault import ComponentFaultProblem, Mitigation
from sregym.conductor.problems.composition.app_bindings import BINDINGS, MongoCollection, MongoInstance
from sregym.conductor.problems.composition.grading import Request
from sregym.generators.fault.components import http, memcached, mongo, redis
from sregym.generators.fault.components.kube import ComponentError, delete_pods, rollout_restart

WRITE_MODES = ("validator", "unique_index")
POISON_SCOPES = ("one_doc", "all_seeded")
WRITE_CHECK_ATTEMPTS = 4  # the write is one-way: give it a few seconds to land before reading it back
WRITE_CHECK_WAIT_S = 3.0


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
    """Shared plumbing: target resolution, seeded data, cache flush, data-loss check, decoys."""

    fault_kind = ""
    blocks_writes = False  # the fault stops writes, so the verdict also needs a write that lands

    def __init__(self, app_name: str, target: str, collection: str | None, description: str, namespace=None, kube=None):
        self.instance, self.coll = resolve(app_name, target, collection)
        self.record: mongo.DataRecord | None = None
        super().__init__(
            app_name=app_name,
            component=f"deployment/{target}",
            description=description,
            namespace=namespace,
            kube=kube,
        )

    # --- helpers -----------------------------------------------------------------------------------

    def _ns_args(self) -> tuple[Any, str, MongoInstance, str]:
        return self.kube, self.namespace, self.instance, self.coll.name

    def prepare(self) -> None:
        """Make sure the target collection holds data, and record it for the data-loss check.

        A previous wrong repair (a restarted mongod) may have emptied it; seed again, then retake the
        seeded-data snapshot so it matches what the entry points return now.
        """
        if mongo.collection_count(*self._ns_args()) == 0:
            http.fetch(self.kube, self.binding, self.namespace, self.binding.integrity.seed)  # type: ignore[union-attr]
            self.snapshot = http.capture(self.kube, self.binding, self.namespace)
        record = mongo.data_record(*self._ns_args())
        if record.count == 0:
            raise ComponentError(
                f"{self.instance.workload} in {self.namespace}: {self.coll.name} is empty after seeding; "
                "a fault there cannot be told from a data-destroying repair"
            )
        self.record = record

    def flush_cache(self) -> None:
        """Empty the cache in front of the collection (a restart of a cache is harmless)."""
        name = self.coll.cache
        if name is None:
            return
        for m in self.binding.memcached:
            if m.workload == name:
                return memcached.flush_all(self.kube, self.namespace, m)
        for r in self.binding.redis:
            if r.workload == name:
                redis.cli(self.kube, self.namespace, r, "FLUSHALL")
                return
        raise ComponentError(f"{self.namespace}: cache {name!r} of {self.coll.name} is not in the binding")

    def write_roundtrip(self) -> tuple[bool, str]:
        """Write through the entry point and read it back (``MongoCollection.write_check``)."""
        check = self.coll.write_check
        if check is None:
            return True, "no write check for this collection"
        marker = f"sregym-rt-{secrets.token_hex(4)}"
        fields = tuple((k, marker if k == check.marker_field else v) for k, v in check.fields)
        http.fetch(self.kube, self.binding, self.namespace, (Request(check.path, fields),))
        for attempt in range(WRITE_CHECK_ATTEMPTS):
            time.sleep(WRITE_CHECK_WAIT_S)
            (read,) = http.fetch(self.kube, self.binding, self.namespace, (Request(check.read),))
            if read.ok and marker in read.body:
                return True, f"marker post visible after {attempt + 1} reads"
        return False, f"a post written through {check.path} did not appear at {check.read}"

    def verdict(self) -> dict[str, Any]:
        result = super().verdict()
        if self.blocks_writes and self.coll.write_check is not None:
            written, detail = self.write_roundtrip()
            result.update(
                success=bool(result["success"]) and written,
                integrity_ok=bool(result.get("integrity_ok")) and written,
                write_detail=detail,
            )
        if self.record is None:
            result.update(success=False, data_detail="no data record (fault not injected here)")
            return result
        intact, detail = mongo.data_intact(*self._ns_args(), self.record)
        result.update(success=bool(result["success"]) and intact, data_intact=intact, data_detail=detail)
        return result

    def _restart(self) -> None:
        rollout_restart(self.kube, self.namespace, self.instance.kind, self.instance.workload)

    def _delete_pod(self) -> None:
        delete_pods(self.kube, self.namespace, self.instance.kind, self.instance.workload)

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        decoys["rollout_restart"] = self._restart
        decoys["restart_stateful_pod"] = self._delete_pod
        for svc in self.instance.consumers:
            decoys[f"rollout_restart:{svc}"] = lambda svc=svc: rollout_restart(
                self.kube, self.namespace, "Deployment", svc
            )
        return decoys


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
        self.prepare()
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
        self.prepare()
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
        self.prepare()
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
        self.prepare()
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

    def decoy_mitigations(self) -> dict[str, Mitigation]:
        decoys = super().decoy_mitigations()
        if self.coll.cache:  # flushing the cache re-reads the same malformed documents
            decoys["flush_cache"] = self.flush_cache
        return decoys
