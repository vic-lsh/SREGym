# Incident faults: component faults on the running system

Faults in this family break an app through its running components (a Redis role, cache
entries, a database record, a process, a connection pool) while every Kubernetes manifest stays
correct. Each one has a scripted reference repair and tempting wrong repairs ("decoys") that the
per-fault verdict must reject. They compose with the rest of the multi-fault catalog
(`sregym/conductor/problems/composition/`).

## Three layers

| Layer | Where | App-specific? |
| --- | --- | --- |
| Component driver | `sregym/generators/fault/components/<kind>.py` | No: takes a binding row |
| App binding | `sregym/conductor/problems/composition/app_bindings.py` | Yes, data only |
| Catalog entry + problem class | `composition/catalog/component_state.py`, `sregym/conductor/problems/<fault>.py` | No: samples from the binding |

App-neutral: drivers (`kube`, `http`, `redis`, `memcached`), the snapshot-and-compare integrity
check, the write probe, the contract base class, the harness. Per app: binding rows (instances,
entry-point writes in `grading.ENTRY_POINTS`, integrity reads and seed requests), and any fault
that depends on business logic (a specific record shape, a specific token format).

### Drivers

- `kube.py`: the one Kubernetes seam (`Kube.run(argv)`; tests pass a fake). Pod lookup by the
  workload's selector, `exec_in`, `run_client_pod` (a busybox probe-labelled pod in the
  namespace), and the generic repairs used as references or decoys: `rollout_restart`,
  `delete_pods` (restart the stateful pod), `restore_source` (re-apply `SREGYM_ACCEPTED_RENDER`).
- `http.py`: fetch entry-point requests from inside the namespace; `capture`/`compare` implement
  the binding's `IntegrityCheck`.
- `redis.py`: Redis or Valkey via the CLI in the server's pod (`RedisInstance.cli`). `cli` raises
  on any error reply (`READONLY`, `NOPERM`, `OOM`, `BUSY`, `LOADING`, ...: non-tty redis-cli
  prints them raw and exits 0).
- `memcached.py`: text protocol over `nc` from a client pod; `corrupt()` makes poisoned values.

Add a driver module per new component kind (mongo, process, network, load, neighbor, proxy). A
driver function takes `(kube, namespace, instance_row, ...)`, raises `ComponentError` with the
namespace, workload and command, and never names an app.

### Bindings

`AppBinding` lists an app's `redis`, `memcached`, `mongo` and `services` rows (a `ServiceInstance`
with `proxy=True` is the user-facing proxy: nginx-thrift, envoy frontend-proxy), its `entry`
(from `grading.ENTRY_POINTS`) and its `integrity` check. Every row has `verified`: only verified
rows are sampled (`instances(rows)`). Mark a row verified only after checking it against a live
deployment. Add fields to a row type when a new driver needs them (a Mongo collection, a TCP
port), not new row types per app.

`social_network` is filled and verified for Redis and the two memcached instances that entry
points reach; `hotel_reservation` and `astronomy_shop` rows come from their manifests and are
unverified.

### Seeded-data integrity (snapshot and compare)

`IntegrityCheck(reads, seed, volatile_keys, depends_on)` (optional per binding: an app without
one skips the data comparison, its stores still get witnesses): before injection
`prepare_injection` requests every read at the entry point and keeps the normalized bodies (JSON
with `volatile_keys` dropped); if any read is empty or failing it runs `seed` and reads again. The
per-fault verdict requires the same reads to return the same data afterwards. `depends_on` lists
the stores those reads return data from whose restart loses it (social network:
home-timeline-redis, user-timeline-mongodb, post-storage-mongodb); the generator uses it (below).

Social network seeds users 9100 and 9101 (9101 follows 9100) and three posts by 9100; it reads
9100's user timeline and 9101's home timeline. These ids are outside the wrk2 workload's range
(1..962) and the probe's (9200, 9201), so only faults and repairs change them.

### Store witnesses

Every store row whose restart loses data (`restart_loses_data`: Mongo always, Redis when
`holds_primary_data`, memcached never) that a fault targets (`data_stores()`, default: the target)
gets a witness in the base class, taken in `prepare_injection`: sampled key names (Redis, `SCAN`)
or sampled `_id`s per collection (Mongo). The verdict requires them all present afterwards. When
the row has a `write_check` and the fault does not block writes, a marker is also written through
the entry point while the fault is live (kept only if it landed) and must still be readable after
the repair, so a repair that drops writes accepted during the fault fails too. A store with
nothing to witness makes `prepare_injection` raise (a fault there could not be told from a wipe).

### Write probe

`grading.EntryPoint` has optional `writes` (form POSTs requested every round, like `paths`) and
`setup` (run once per probe, outcome ignored). Social network writes: compose-post, follow and
login as probe user 9200 (follower 9201, so composes also write a home timeline). A write with
`marker_field`/`readback` counts OK only if a fresh marker appended to that field is then read
back (compose answers 200 when the database dropped the write); social compose reads 9200's user
timeline. Other apps keep their GET-only probe.

## The contract

Subclass `ComponentFaultProblem` (`sregym/conductor/problems/component_fault.py`) and pass
`target=` (a workload with a row in the app binding; `self.target` is that row,
`AppBinding.row()` finds it):

- `inject()`: put the fault in place through the running system (exec, client pod, node
  tooling). Never leave a Kubernetes object whose deletion is the fix. Raise if it did not take.
- `fault_cleared() -> (bool, detail)`: is this fault's own state gone?
- `reference_mitigation()`: the scripted correct repair (also `recover_fault`).
- Class attributes: `restart_clears` (default True: the state lives in the target's process or
  data, so restarting the target clears it), `blocks_writes` (the verdict also writes through the
  row's `write_check` and needs it to land), `STATE_ATTRS` (attributes `recover_fault` needs
  beyond the constructor params, persisted by the harness).
- Hooks: `extra_repairs()` (kind-specific alternatives), `prepare_target()` (seed the target while
  healthy), `write_check()`, `data_stores()`.

Repairs are `Repair(run, expect, destroys_data=False, no_op=False)` with `expect` one of
`Expect.PASSES`, `FAULT_NOT_CLEARED`, `DATA_LOST`. `repairs()` = `standard_repairs()` +
`extra_repairs()`; the standard ones come from the target row, not per class:

| Repair | Expect |
| --- | --- |
| `restore_source` (re-apply `SREGYM_ACCEPTED_RENDER`) | FAULT_NOT_CLEARED, `no_op=True` (verified: Deployment/StatefulSet generations unchanged, else the trial is INVALID) |
| `rollout_restart`, `restart_stateful_pod` of a store target | DATA_LOST (`destroys_data`) if `restart_loses_data`; PASSES if not; FAULT_NOT_CLEARED if not `restart_clears` |
| `rollout_restart:<consumer>` for each row consumer | FAULT_NOT_CLEARED |

`decoy_mitigations()` / `acceptable_mitigations()` are views (expect != / == PASSES).
`apply(repair)` runs one and checks a declared no-op.

The verdict (`verdict()`, wrapped by `ComponentFaultOracle`) lists `failed` components: `fault`
(own state still there), `write` (a write-blocking fault's write did not land), `data` (seeded
snapshot changed), `witness` (sampled keys/ids or the live-written marker gone), and a `reason`:
FAULT_NOT_CLEARED if `fault` or `write` failed, else DATA_LOST if `data` or `witness` failed,
else PASSES. A `ComponentError` in any check is retried once; then the oracle answers
`{"success": False, "inconclusive": True, "reason": "inconclusive"}`. Composite liveness treats
inconclusive as unproven, and the harness counts it INVALID, never as a decoy that failed.

Lifecycle: `prepare_injection` takes the snapshot and witnesses while healthy (a composite calls it
on every fault before injecting any and is invalid, with nothing injected, if one fails);
`inject_fault` takes them only if not prepared and never re-takes them (a composite retry reuses
them); `recover_fault` runs the reference and releases them.

## Add a fault kind

1. Driver function(s) in `sregym/generators/fault/components/<kind>.py` (inject, fix, status) if
   the component kind needs new primitives.
2. A problem class (subclass `ComponentFaultProblem`): `inject`, `fault_cleared`,
   `reference_mitigation`, plus `extra_repairs` only for repairs the row cannot derive.
3. One `component_entry(key, apps, "module:Class", choose)` call in a catalog family module
   (`catalog/component_*.py`). `choose(binding, rng) -> Target(params, touches)` samples the
   target from verified binding rows (`instances`, `pick_one`) plus mode/severity/scope; `touches`
   lists other workloads it or its repairs write (a cache it flushes). Footprints (whole
   workloads), read dependencies, lazy build and generation 2 come from the helper; pass
   `restarts=True` if the reference or an acceptable repair restarts the target.
4. New keys go into `reliability.UNRELIABLE` with `_PENDING` until validated live on 3 seeds.
   Catalog constants mirrored from problem modules are listed in
   `tests/composition/test_catalog_mirrors.py`.
5. Unit tests with the fake `Kube` (`tests/composition/test_component_faults.py`): add the kind to
   `CASES` there (or the Mongo equivalent) and `test_every_repair_produces_its_declared_outcome`
   checks every repair's outcome; the fakes must model a restart wiping a store's data.

## Plan ids and catalog generations

Ids without a generation segment draw from generation 1 only, so adding a family never changes
what an existing id draws (pinned: `fixtures/vanilla_s42_n100_plan.json`,
`fixtures/social_network_g1_plans.json`, `fixtures/mixed10_s42_plans.json`,
`fixtures/catalog_generation1_keys.json`). Component kinds are generation 2: opt in with
`mf__<app>__n<N>__s<seed>[__k<K>][__reliable]__g2__v_` (or `--catalog-generation 2` for a planned
sequence). Generation 2 is open while no experiment has used `__g2` ids; once one has, freeze it
(`spec.FROZEN_GENERATION = 2`, pin its key set) and add `LATEST_GENERATION = 3` for new families.

## Enable an existing kind on another app

1. Fill (or verify) that app's binding rows for the kind (and its `integrity` check if it has
   one), and set `verified=True` after checking them on a live deployment.
2. Add the app to the key's `ENABLED_APPS` (at the open generation: a frozen generation's key set
   per app is pinned).
3. Validate live; then decide its reliability flag.

## Validate live

Against a deployed namespace (KUBECONFIG selects the cluster; `restore_source` needs
`SREGYM_ACCEPTED_RENDER`):

```bash
uv run python tools/fault_validation/validate.py --key memcached_poisoned_entries \
    --app social_network --seeds 1 2 3 --persist-s 15 --probe-rounds 3 \
    --reset-cmd '<lane>/reset.sh --reseed' --out /tmp/trials.jsonl [--lanes kc1 kc2]
```

Per seed: a `baseline` health row, one row per repair (the `reference`, every `decoy:<name>` and
`acceptable:<name>`), each on a fresh injection with the symptom checked before and after the
persistence window, a final `recovered` row and a `seed_summary` row with wall time. A row is
VALID when the verdict reason equals the repair's declared outcome, FAIL when it does not (or no
symptom), INVALID when an oracle or probe step errored or the repair raised. Data-destroying
repairs run last, each followed by `--reset-cmd`. Injected state is written to `--state-dir`
while a fault is live and a later run recovers leftovers first. `--lanes` shards seeds over
kubeconfigs in parallel. `--only` limits the repairs. Exit 0 only if every row is VALID.

## MongoDB faults

Driver `components/mongo.py` runs the server's shell in its pod (`mongo --eval`, one JSON line
back) and raises `ComponentError` with the namespace and workload. Primitives come as
inject, repair and status:

| Fault kind | Inject | Reference repair | Status read |
| --- | --- | --- | --- |
| `mongo_write_lock` (target) | `fsync_lock` | `fsync_unlock` (until count 0) | `lock_state` |
| `mongo_write_rejection` (target, collection, mode) | `set_validator` (a `$jsonSchema` no document meets) or `add_blocking_unique_index` | `clear_validator` / `drop_blocking_unique_index` | `validator_state` / `blocking_index_state` |
| `mongo_collection_renamed` (target, collection) | `rename_away` (to `<name>_old`), then flush the collection's cache | `merge_back`: copy newer documents into `_old`, rename over the name with `dropTarget` (a plain rename back fails once writers recreated the name) | `collection_state` |
| `mongo_poison_document` (target, collection, shape, scope) | `poison_documents` (`wrong_type`: a string field becomes `{v: <original>}`; `field_renamed`: a required object field gets a `_legacy` suffix), then flush the cache | `unpoison_documents` (lossless: the original is in the document) | `poison_state` |

The unique-index mode builds a partial unique index over documents newer than the collection's
newest, plus a sentinel document in that range, so existing documents are unaffected and every new
one collides. The mongod holds the only copy of its data and its pod has no volume: restarting it
(rollout restart or deleting the pod) clears each fault by destroying the data. The per-fault
verdict therefore also runs `data_intact`: `_id`s sampled from the target collection before the
fault must still be present and the collection must not have shrunk (the base-class store
witness). `prepare_target` reseeds an empty collection before the snapshot. Repairs: the standard
ones (restarts are DATA_LOST) plus, for poison, `flush_cache` (FAULT_NOT_CLEARED). Object names are
production-like (`schema_version` validator field, index `idx_post_type_ts_uniq`, placeholder
field `migration_placeholder`, which data checks exclude). `MongoInstance.auth` carries
credentials for instances started with authentication.

Binding fields (`MongoInstance`, `MongoCollection` in `app_bindings.py`): per instance `database`,
`collections`, `consumers`, `shell`, `container`; per collection `cache` (the memcached/Redis row
that fronts it, flushed after faults a warm cache would hide), `key_field` (a document field that
also appears in the integrity snapshot, used to pick documents the entry points read),
`poison_fields` (string field, object field), `unique_key`/`stamp_field` (enable the unique-index
mode) and `faults`, the Mongo fault kinds verified live on that collection; only those sample it
(`mongo_targets`). `write_check` (a `WriteCheck`: POST, then a read that must show the marker) lets
write-blocking faults be seen where the app swallows a failed database write: social-network's
compose is one-way and answers 200 whatever mongod did, so a rejected post is invisible to the
HTTP probe; the verdict of `mongo_write_lock`/`mongo_write_rejection` writes a marker post as the
probe user and reads it back. Live findings (social-network): write lock is visible on
user-timeline, post-storage (compose) and social-graph (follow); write rejection on user-timeline
and post (not social-graph: the follow path hides the rejected write; the `user` collection is
only written at registration, which no probe does); rename is visible on user-timeline and post
with the cache flushed (not `user` or social-graph). Instances unreachable from any entry point (`media-mongodb`) or empty after the
seed (`url-shorten-mongodb`) are `verified=False`.

## Redis faults

Six kinds on a bound Redis row (`RedisInstance.faults` lists the kinds verified live; `poison_keys`,
`rename_keys` and `stale_sets` name the keys they act on). All state is runtime-only, so restarting
the server clears it: acceptable for `user-timeline-redis` and `social-graph-redis` (they refill from
Mongo), data loss on `home-timeline-redis` (the only copy of home timelines). Driver primitives are the
"Incident-fault primitives" block of `components/redis.py`; problem classes are in
`problems/redis_incident_faults.py`.

| Key | Injection (reference) | Entry-point symptom (live, redis:6.2.4) |
| --- | --- | --- |
| `redis_acl_revoked` | `ACL SETUSER default -@write` or `-@read` (`+@all`) | write: compose 500 `NOPERM ... 'zadd'` (follow 500 on social-graph); read: timeline read 500 `NOPERM ... 'zrevrange'` (compose 500 on social-graph) |
| `redis_out_of_memory` | `CONFIG SET maxmemory` at 10% or 50% of used memory (`maxmemory` back) | compose 500 `OOM command not allowed...` (follow on social-graph); reads fine. Limits near 100% of use are intermittent, so never sampled |
| `redis_writes_paused` | `CLIENT PAUSE 3600000 WRITE` (`CLIENT UNPAUSE`; never ALL, which also blocks UNPAUSE) | compose blocks 10 s then 500 `TTransportException`; reads fine |
| `redis_poisoned_timeline_entry` | `ZADD <key> 1e18 corrupt-<tok>` or a dangling numeric id (`ZREM` that member) | that key's read 500 `stoul` / `Return set incomplete`; `DEL` of the key repairs a cache but destroys a home timeline |
| `redis_timeline_key_renamed` | `RENAME <uid> <uid>:bak-<tok>` on home-timeline-redis only (merge back with ZUNIONSTORE, since compose re-creates the key) | home read `{}` with HTTP 200: only a content check sees it. On the two caches a rename heals itself |
| `social_graph_stale_cache` | drop a follower from `<uid>:followers` and add a ghost id (`DEL` the key) | compose 200 but the post skips the follower; the service trusts a non-empty key |

The reference of the rename kind is idempotent (recovery runs it after any repair). Error replies come
back without a `(error)` prefix and exit 0 through `kubectl exec`, so `redis.cli` checks the reply prefix.

## Lane notes (read before validating)

- nginx-thrift: the stock `worker_processes auto` forks one worker per host CPU (64 on a large
  host) into a 128Mi limit; workers grow as they load the Lua thrift clients and the pod is
  OOMKilled under light traffic (seen on the first probe of a fresh lane: 7/36 ok, exit 137).
  Patch `worker_processes` to a small number (2 or 4) in the nginx-thrift ConfigMap of the
  rendered manifest the lane deploys AND in the file `SREGYM_ACCEPTED_RENDER` points at, so the
  `restore_source` decoy does not bring the crash back. The DeathStarBench source still says
  `auto`.
- The Conductor's continuous `wrk2-job` adds load (and nginx pressure); SDO's clean start deletes
  it. Delete it for validation unless the fault needs background traffic.
- The first request after a pod restart can fail once on a stale pooled connection. The traffic
  probe tolerates it (90% threshold) and integrity reads retry a failed read (never changed data).
- A fresh app fails its first requests while connection pools warm up; seeding retries.
- The harness writes the injected state to `--state-dir` while a fault is live; a killed run's
  fault is recovered by the next run before its first trial (`recover_leftover` row).
- A data-destroying repair (restart of post-storage-mongodb) leaves caches pointing at lost data
  (user-timeline-redis lists post ids that are gone), so later snapshots fail: the harness runs
  those repairs last and calls `--reset-cmd` (a reseeding reset) after each.

## Pod-level drivers (node access and injector pods)

Pod-level faults take a `WorkloadTarget` (`app_bindings.workload_targets(binding)`: workload, kind,
container, port, protocol, `holds_primary_data`), derived from the binding rows, so no driver names an
app. A `ServiceInstance` row carries its TCP `port` and optional `container`. `pod_fault.PodLevelFault`
fixes the per-target restart rule: where `holds_primary_data` is true (home-timeline-redis, Mongo) a
rollout restart or pod deletion is a decoy (the seeded-data check fails it); otherwise both are
acceptable repairs, because the fault lives in the process or pod sandbox a restart replaces.

- `node.py`: the node seam. `Nodes(kube, shell)` finds a pod's node and a container's host pid from the pod
  spec (`crictl inspect`). `shell` is a `NodeShell` (`DockerNode`: `docker exec <node>` on kind); tests pass a
  fake. Needs privileged node access, so it is for kind-like clusters.
- `process.py` (`process_frozen`): SIGSTOP the container's init process from the node (a pid-1 process
  ignores signals from inside its namespace). The pod stays Running and Ready and its port still accepts
  connections. Reference: `kill -CONT 1` through `kubectl exec`. Cleared means the current main process is not
  in state T. Target: any thrift service, Redis or Mongo.
- `network.py` (`pod_network_impairment`): `tc qdisc replace ... netem delay|loss` in the pod netns via
  `nsenter -t <pid> -n`. Severities are calibrated so the entry-point probe (5 s wget timeout) fails:
  delay 3000 or 6000 ms; loss 90 or 95 percent (60 percent on home-timeline-redis passed the probe, 35/36, so
  it is not offered). Reference: delete the qdisc. The qdisc dies with the pod, so a restart is also a repair on
  stateless targets.
- `foreign.py`: pods the injector runs in the `sregym-injector` namespace, never the app's: a client or
  tenant, not part of the app. Stopping the pod is the repair. Everything labelled with the app namespace
  and fault kind.
- `load.py` (`connection_exhaustion`, `traffic_surge`): the hog runs the TARGET's own image (it ships bash, so
  `/dev/tcp` holds thousands of sockets in one process) and re-opens its connections whenever the server drops
  them, so restarting the victim does not end it. A thread-per-connection server runs out of its 128Mi at about
  4-6k connections (OOM kills and restarts). The surge is busybox wget loops against the entry point from a 2 CPU
  client. Reference for both: stop the client. For the surge that is the stated choice: scaling out is a drift
  from accepted source that restore_source would revert and needs a capacity judgement, while removing the
  source is exactly what a rate limit at the edge achieves.
- `neighbor.py` (`NoisyNeighbor`, not in the catalog): a CPU burner pinned to the victim's node. On the
  3-CPU kind worker neither six busy loops with a 6 CPU request nor sixteen with a 30 CPU request produced a
  failing probe (it only slowed from 8 s to 14 s). Enabling it needs a latency budget in the verdict.

Live validation costs about 4 minutes per trial (three probes at 5 s per failing request), so a stateful
target with three decoys takes 15 minutes per seed.
