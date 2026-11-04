"""MongoDB component driver: run the server's shell inside its pod.

App-neutral: every primitive takes ``(kube, namespace, inst: MongoInstance, ...)`` and raises
``ComponentError`` naming the namespace, workload and command. Each script prints one JSON line
that the Python side parses, so a primitive never trusts shell exit status alone. Primitives
come in inject/repair/status triples:

* write lock: ``fsync_lock`` / ``fsync_unlock`` / ``lock_state`` (writes hang, reads work);
* write rejection: ``set_validator``/``clear_validator``/``validator_state`` (a ``$jsonSchema``
  that no document satisfies) and ``add_blocking_unique_index``/``drop_blocking_unique_index``/
  ``blocking_index_state`` (a partial unique index that rejects every new document);
* renamed collection: ``rename_away`` / ``merge_back`` / ``collection_state`` (renaming back
  fails while the original name exists again, so the repair merges new documents then
  renames with ``dropTarget``);
* malformed document: ``poison_documents`` / ``unpoison_documents`` / ``poison_state``;
* data checks: ``data_record`` / ``data_intact`` (the sampled ``_id``s are still present and the
  collection did not shrink), which is how a repair that restarts the only copy is caught.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from sregym.conductor.problems.composition.app_bindings import MongoInstance
from sregym.generators.fault.components.kube import ComponentError, Kube, exec_in, pod_for

# Production-like names: an operator (or an agent) inspecting the collection sees an ordinary schema
# rule and index, nothing that names a harness. The sentinel field marks the one document the
# unique-index fault inserts, so data checks can leave it out.
VALIDATOR_FIELD = "schema_version"  # a field no document has, so a $jsonSchema requiring it rejects all
BLOCKING_INDEX = "idx_post_type_ts_uniq"
SENTINEL_FIELD = "migration_placeholder"
RENAME_SUFFIX = "_old"
POISON_SHAPES = ("wrong_type", "field_renamed")
POISON_RENAME_SUFFIX = "_legacy"
SAMPLE_IDS = 20
CONNECT_ATTEMPTS = 8  # a restarted mongod accepts connections some seconds after its pod is Ready
CONNECT_BACKOFF_S = 5.0

_PRELUDE = (
    "function chk(r){ if(!r || r.ok !== 1){ throw new Error('command failed: ' + tojson(r)); } return r; }\n"
    "function out(o){ print('@@JSON ' + JSON.stringify(o)); }\n"
)


def _js(value: Any) -> str:
    return json.dumps(value)


def run_js(kube: Kube, namespace: str, inst: MongoInstance, script: str) -> dict[str, Any]:
    """Run ``script`` in the instance's shell and return the object it passed to ``out()``."""
    argv = [inst.shell, "--quiet", "--port", str(inst.port)]
    if inst.auth is not None:
        argv += ["-u", inst.auth[0], "-p", inst.auth[1], "--authenticationDatabase", "admin"]
    argv += ["--eval", _PRELUDE + script]
    for attempt in range(CONNECT_ATTEMPTS):
        try:
            pod = pod_for(kube, namespace, inst.kind, inst.workload)
            text = exec_in(kube, namespace, pod, argv, container=inst.container)
            break
        except ComponentError as e:
            if attempt == CONNECT_ATTEMPTS - 1:
                raise ComponentError(f"{inst.workload} in {namespace}: {inst.shell} script failed: {e}") from e
            time.sleep(CONNECT_BACKOFF_S)
    for line in reversed(text.splitlines()):
        if line.startswith("@@JSON "):
            return json.loads(line[len("@@JSON ") :])
    raise ComponentError(f"{inst.workload} in {namespace}: no result from {inst.shell}: {text.strip()[:400]!r}")


def _coll(inst: MongoInstance, collection: str, var: str = "c") -> str:
    return f"var {var} = db.getSiblingDB({_js(inst.database)}).getCollection({_js(collection)});\n"


# --- write lock --------------------------------------------------------------------------------


def lock_state(kube: Kube, namespace: str, inst: MongoInstance) -> dict[str, Any]:
    """``{"locked": bool}`` from ``currentOp().fsyncLock``."""
    r = run_js(
        kube, namespace, inst, 'var o = db.getSiblingDB("admin").currentOp(); out({locked: o.fsyncLock === true});'
    )
    return {"locked": bool(r["locked"])}


def fsync_lock(kube: Kube, namespace: str, inst: MongoInstance) -> None:
    """``fsyncLock``: flush and block every write, reads keep working. Already locked: no-op."""
    run_js(
        kube,
        namespace,
        inst,
        'var a = db.getSiblingDB("admin"); if(a.currentOp().fsyncLock !== true){ chk(a.fsyncLock()); }'
        " out({locked: a.currentOp().fsyncLock === true});",
    )


def fsync_unlock(kube: Kube, namespace: str, inst: MongoInstance) -> None:
    """``fsyncUnlock`` until the lock count reaches zero. Not locked: no-op."""
    r = run_js(
        kube,
        namespace,
        inst,
        'var a = db.getSiblingDB("admin"); var n = 0;'
        " while(a.currentOp().fsyncLock === true && n < 10){ chk(a.fsyncUnlock()); n++; }"
        " out({locked: a.currentOp().fsyncLock === true, unlocks: n});",
    )
    if r["locked"]:
        raise ComponentError(f"{inst.workload} in {namespace}: still fsync-locked after {r['unlocks']} unlocks")


# --- write rejection: validator ------------------------------------------------------------------


def set_validator(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> None:
    """``collMod`` a strict ``$jsonSchema`` requiring a field no document has: every insert and
    every update that touches a document fails with ``Document failed validation``."""
    schema = {"$jsonSchema": {"bsonType": "object", "required": [VALIDATOR_FIELD]}}
    run_js(
        kube,
        namespace,
        inst,
        f"var d = db.getSiblingDB({_js(inst.database)});"
        f" chk(d.runCommand({{collMod: {_js(collection)}, validator: {_js(schema)},"
        ' validationLevel: "strict", validationAction: "error"}));'
        " out({ok: true});",
    )


def clear_validator(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> None:
    run_js(
        kube,
        namespace,
        inst,
        f"var d = db.getSiblingDB({_js(inst.database)});"
        f' chk(d.runCommand({{collMod: {_js(collection)}, validator: {{}}, validationLevel: "off"}}));'
        " out({ok: true});",
    )


def validator_state(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> dict[str, Any]:
    """``{"exists", "validator": bool, "level"}``; ``validator`` is True when a validator is set."""
    return run_js(
        kube,
        namespace,
        inst,
        f"var i = db.getSiblingDB({_js(inst.database)}).getCollectionInfos({{name: {_js(collection)}}});"
        " var o = i.length ? i[0].options : {};"
        " var v = o.validator || {}; var lvl = o.validationLevel || 'strict';"
        " out({exists: i.length > 0, validator: Object.keys(v).length > 0 && lvl !== 'off', level: lvl});",
    )


# --- write rejection: unique index -----------------------------------------------------------------


def add_blocking_unique_index(
    kube: Kube, namespace: str, inst: MongoInstance, collection: str, key: str, stamp: str
) -> dict[str, Any]:
    """Make every NEW document fail with a duplicate key error without touching existing ones.

    A unique index on ``key`` restricted (``partialFilterExpression``) to documents whose ``stamp``
    is above the collection's newest; a sentinel document in that range takes the one free slot, so
    any later insert collides with it. Existing documents are outside the filter, so the index
    builds although ``key`` repeats among them. Returns ``{"sentinel_id": ..., "threshold": ...}``.
    """
    return run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection)
        + f"var top = c.find({{{_js(stamp)}: {{$exists: true}}}}).sort({{{_js(stamp)}: -1}}).limit(1).toArray();"
        f" var t = top.length ? top[0][{_js(stamp)}] : NumberLong(0); var tn = Number(t);"
        f" chk(c.runCommand({{createIndexes: {_js(collection)}, indexes: [{{key: {{{_js(key)}: 1}},"
        f" name: {_js(BLOCKING_INDEX)}, unique: true,"
        f" partialFilterExpression: {{{_js(stamp)}: {{$gt: t}}}}}}]}}));"
        f" var doc = {{{_js(SENTINEL_FIELD)}: true, {_js(key)}: 0, {_js(stamp)}: NumberLong(tn + 1)}};"
        " var ins = c.insertOne(doc); chk({ok: ins.acknowledged ? 1 : 0});"
        " out({sentinel_id: ins.insertedId.str, threshold: tn});",
    )


def drop_blocking_unique_index(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> None:
    """Drop the blocking index and the sentinel. Neither present: no-op."""
    run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection) + f"if(c.getIndexes().some(function(i){{return i.name === {_js(BLOCKING_INDEX)};}}))"
        f" {{ chk(c.dropIndex({_js(BLOCKING_INDEX)})); }}"
        f" c.deleteMany({{{_js(SENTINEL_FIELD)}: true}}); out({{ok: true}});",
    )


def blocking_index_state(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> dict[str, Any]:
    return run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection)
        + f"out({{index: c.getIndexes().some(function(i){{return i.name === {_js(BLOCKING_INDEX)};}}),"
        f" sentinel: c.countDocuments({{{_js(SENTINEL_FIELD)}: true}})}});",
    )


# --- renamed collection ------------------------------------------------------------------------------


def collection_state(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> dict[str, Any]:
    """``{"count", "exists", "old_exists", "old_count"}`` for the collection and its ``_old`` rename."""
    old = collection + RENAME_SUFFIX
    return run_js(
        kube,
        namespace,
        inst,
        f"var d = db.getSiblingDB({_js(inst.database)});"
        f" var names = d.getCollectionNames();"
        f" function n(x){{ return names.indexOf(x) >= 0 ? d.getCollection(x).countDocuments({{}}) : 0; }}"
        f" out({{exists: names.indexOf({_js(collection)}) >= 0, count: n({_js(collection)}),"
        f" old_exists: names.indexOf({_js(old)}) >= 0, old_count: n({_js(old)})}});",
    )


def rename_away(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> None:
    """``renameCollection`` to ``<collection>_old`` (a migration gone wrong). Writers silently
    recreate an empty ``<collection>``; readers see no data."""
    run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection) + f"chk(c.renameCollection({_js(collection + RENAME_SUFFIX)})); out({{ok: true}});",
    )


MERGE_STRATEGIES = ("append_only", "union_arrays")  # correct: keep every document from both sides
LOSSY_MERGES = ("overwrite", "drop_target")  # wrong: lose writes made during the fault


def merge_back(
    kube: Kube,
    namespace: str,
    inst: MongoInstance,
    collection: str,
    strategy: str = "append_only",
    key: str = "_id",
) -> dict[str, Any]:
    """Repair a rename: fold the documents written since into ``<collection>_old``, then rename it
    over ``<collection>`` with ``dropTarget`` (a plain rename back fails with ``target namespace
    exists`` once writers recreated the name). No ``_old`` collection: no-op. Documents are matched
    by ``key`` (the field services look documents up by, e.g. ``user_id``; ``_id`` is an ObjectId
    generated per insert, so the recreated collection's documents never share it).

    ``append_only`` (one document per event, e.g. posts): insert the newer documents whose ``_id`` is
    absent from ``_old``. ``union_arrays`` (one aggregate document per key, e.g. a user's timeline):
    as append_only, and where both sides hold the ``_id``, add the newer array elements to the old
    array and let newer scalars win. The lossy strategies exist only as wrong repairs:
    ``overwrite`` replaces the old document by the newer one (the aggregate's history is lost),
    ``drop_target`` renames back without merging (every write made during the fault is lost).
    Writers still active during the merge can lose a write made between the copy and the rename.
    """
    if strategy not in (*MERGE_STRATEGIES, *LOSSY_MERGES):
        raise ValueError(f"unknown merge strategy {strategy!r}")
    old = collection + RENAME_SUFFIX
    fold = {
        "append_only": "if(!e){ chk({ok: o.insertOne(doc).acknowledged ? 1 : 0}); }",
        "union_arrays": (
            "if(!e){ chk({ok: o.insertOne(doc).acknowledged ? 1 : 0}); } else {"
            " var add = {}, set = {}; Object.keys(doc).forEach(function(f){"
            "  if(f === '_id') return;"
            "  if(Array.isArray(doc[f])){ add[f] = {$each: doc[f]}; } else { set[f] = doc[f]; } });"
            " var up = {}; if(Object.keys(add).length){ up.$addToSet = add; }"
            " if(Object.keys(set).length){ up.$set = set; }"
            " if(Object.keys(up).length){ chk({ok: o.updateOne({_id: e._id}, up).acknowledged ? 1 : 0}); } }"
        ),
        "overwrite": (
            "var r = Object.assign({}, doc); delete r._id;"
            " chk({ok: o.replaceOne({_id: e ? e._id : doc._id}, r, {upsert: true}).acknowledged ? 1 : 0});"
        ),
        "drop_target": "",
    }[strategy]
    return run_js(
        kube,
        namespace,
        inst,
        f"var d = db.getSiblingDB({_js(inst.database)}); var names = d.getCollectionNames();"
        f" var merged = 0; var renamed = false;"
        f" if(names.indexOf({_js(old)}) >= 0){{"
        f"  var o = d.getCollection({_js(old)});"
        f"  if(names.indexOf({_js(collection)}) >= 0 && {_js(bool(fold))}){{"
        f"   d.getCollection({_js(collection)}).find().forEach(function(doc){{"
        f"    var q = {{}}; q[{_js(key)}] = doc[{_js(key)}]; var e = o.findOne(q); {fold} merged++; }});"
        f"  }}"
        f"  chk(o.renameCollection({_js(collection)}, true)); renamed = true; }}"
        f" out({{merged: merged, renamed: renamed}});",
    )


# --- malformed documents -----------------------------------------------------------------------------


def _key_filter(key_field: str, key_expr: str) -> str:
    """Shell expression matching ``key_field`` against the JS string ``key_expr`` whichever type the
    service stored it as (string, int64 or number)."""
    return (
        f"{{$or: [{{{_js(key_field)}: {key_expr}}}, {{{_js(key_field)}: NumberLong({key_expr})}},"
        f" {{{_js(key_field)}: parseInt({key_expr})}}]}}"
    )


def poison_documents(
    kube: Kube,
    namespace: str,
    inst: MongoInstance,
    collection: str,
    key_field: str,
    keys: tuple[str, ...],
    shape: str,
    string_field: str,
    object_field: str,
) -> list[str]:
    """Make the documents whose ``key_field`` is in ``keys`` malformed for their reader.

    ``wrong_type``: ``string_field`` becomes ``{v: <original>}`` (a string read throws);
    ``field_renamed``: ``object_field`` is renamed to ``<object_field>_legacy`` (a required
    member is missing). Both keep the original value in the document, so the repair is lossless.
    Returns the keys actually changed; raises if none matched.
    """
    if shape not in POISON_SHAPES:
        raise ValueError(f"unknown poison shape {shape!r}")
    if shape == "wrong_type":
        update = f"[{{$set: {{{_js(string_field)}: {{v: '${string_field}'}}}}}}]"
        guard = f"{_js(string_field)}: {{$type: 'string'}}"
    else:
        update = f"{{$rename: {{{_js(object_field)}: {_js(object_field + POISON_RENAME_SUFFIX)}}}}}"
        guard = f"{_js(object_field)}: {{$exists: true}}"
    r = run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection) + f"var done = []; {_js(list(keys))}.forEach(function(k){{"
        f" var res = c.updateOne({{$and: [{_key_filter(key_field, 'k')}, {{{guard}}}]}}, {update});"
        " if(res.modifiedCount === 1){ done.push(k); } }); out({done: done});",
    )
    if not r["done"]:
        raise ComponentError(
            f"{inst.workload} in {namespace}: no {collection} document with {key_field} in {list(keys)} was poisoned"
        )
    return list(r["done"])


def unpoison_documents(
    kube: Kube,
    namespace: str,
    inst: MongoInstance,
    collection: str,
    key_field: str,
    keys: tuple[str, ...],
    shape: str,
    string_field: str,
    object_field: str,
) -> None:
    """Undo ``poison_documents`` from the data in the document. Healthy documents: no-op."""
    if shape == "wrong_type":
        update = f"[{{$set: {{{_js(string_field)}: '${string_field}.v'}}}}]"
        guard = f"{_js(string_field + '.v')}: {{$type: 'string'}}"
    else:
        update = f"{{$rename: {{{_js(object_field + POISON_RENAME_SUFFIX)}: {_js(object_field)}}}}}"
        guard = f"{_js(object_field + POISON_RENAME_SUFFIX)}: {{$exists: true}}"
    run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection) + f"{_js(list(keys))}.forEach(function(k){{"
        f" c.updateOne({{$and: [{_key_filter(key_field, 'k')}, {{{guard}}}]}}, {update}); }}); out({{ok: true}});",
    )


def poison_state(
    kube: Kube,
    namespace: str,
    inst: MongoInstance,
    collection: str,
    key_field: str,
    keys: tuple[str, ...],
    shape: str,
    string_field: str,
    object_field: str,
) -> dict[str, Any]:
    """``{"poisoned": [keys still malformed], "missing": [keys with no document]}``."""
    if shape == "wrong_type":
        bad = f"{{{_js(string_field + '.v')}: {{$exists: true}}}}"
    else:
        bad = f"{{{_js(object_field + POISON_RENAME_SUFFIX)}: {{$exists: true}}}}"
    return run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection) + f"var p = [], m = []; {_js(list(keys))}.forEach(function(k){{"
        f" var f = {_key_filter(key_field, 'k')};"
        " if(c.countDocuments(f) === 0){ m.push(k); }"
        f" else if(c.countDocuments({{$and: [f, {bad}]}}) > 0){{ p.push(k); }} }}); out({{poisoned: p, missing: m}});",
    )


# --- data checks -------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DataRecord:
    """What the collection held before the fault: its size and a sample of ``_id`` (as shell literals)."""

    count: int
    ids: tuple[str, ...]


def collection_count(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> int:
    return int(run_js(kube, namespace, inst, _coll(inst, collection) + "out({n: c.countDocuments({})});")["n"])


def data_record(kube: Kube, namespace: str, inst: MongoInstance, collection: str) -> DataRecord:
    r = run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection)
        + f"var real = {{{_js(SENTINEL_FIELD)}: {{$exists: false}}}};"
        + f" var ids = c.find(real, {{_id: 1}}).sort({{_id: 1}}).limit({SAMPLE_IDS}).toArray().map(function(x){{return tojson(x._id);}});"
        " out({count: c.countDocuments(real), ids: ids});",
    )
    return DataRecord(int(r["count"]), tuple(r["ids"]))


def data_intact(
    kube: Kube, namespace: str, inst: MongoInstance, collection: str, record: DataRecord
) -> tuple[bool, str]:
    """Every recorded ``_id`` is still present and the collection did not shrink."""
    r = run_js(
        kube,
        namespace,
        inst,
        _coll(inst, collection)
        + f"var real = {{{_js(SENTINEL_FIELD)}: {{$exists: false}}}};"
        + f" var ids = {_js(list(record.ids))}.map(function(s){{ return eval('(' + s + ')'); }});"
        " out({count: c.countDocuments(real), present: ids.length ? c.countDocuments({_id: {$in: ids}}) : 0});",
    )
    ok = r["present"] == len(record.ids) and r["count"] >= record.count
    return ok, (
        f"{inst.workload}.{collection}: {r['present']}/{len(record.ids)} sampled documents present, "
        f"{r['count']} documents (was {record.count})"
    )
