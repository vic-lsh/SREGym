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

Check channel: a fault that restricts an identity (`restricts_identity`, e.g. `redis_acl_revoked`) must not be judged through it. Every Redis verdict, witness and integrity read (`key_sample`, `keys_intact`, `key_exists`, `has_member`, `zset_members`, `zcard`) runs as a read-only `sregym_check` ACL user (random password kept only in the harness process, created lazily by `default`, `ACL DELUSER` in `recover_fault`); Mongo reads always use the binding's admin credentials. Tests enforce this for every such kind.

The verdict (`verdict()`, wrapped by `ComponentFaultOracle`) lists `failed` components: `fault`
(own state still there), `write` (a write-blocking fault's write did not land), `data` (seeded
snapshot changed), `witness` (sampled keys/ids or the live-written marker gone), and a `reason`:
FAULT_NOT_CLEARED if `fault` failed, else DATA_LOST if `data` or `witness` failed (a failing `write` is
then a symptom of the loss), else FAULT_NOT_CLEARED if only `write` failed, else PASSES. A `ComponentError` in any check is retried once; then the oracle answers
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
sequence). Generation 2 is frozen (`spec.FROZEN_GENERATION = 2`, key sets pinned in `fixtures/catalog_generation2_keys.json`):
the detection experiments used `__g2` ids. Generation 3 (`__g3`, `--catalog-generation 3`) is open. A generation may also
change how an existing kind draws: `CatalogEntry.revisions` maps a generation to a replacement sampler
(`entry.at_generation(g)`; `plan_for` applies it), so g1/g2 ids keep their draws (`fixtures/g2_scoped_kinds_plans.json`
pins every g2 plan that draws a revised kind). Generation 3 uses this for the scope of the single-record data faults
(next section above); `reliability.UNRELIABLE_FROM` flags a kind only for ids of a generation and later, so a g3
revision does not make the kind unreliable for `__reliable__g2` ids. `validate.py --generation 3` samples a seed with the g3 draw.

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

## Validate a composition live

`tools/fault_validation/validate_composition.py` checks that the faults of one plan id inject
together, grade correctly and each need their own repair (offline plan statistics:
`tools/fault_validation/compose_stats.py`):

```bash
uv run python tools/fault_validation/validate_composition.py \
    --plan-id mf__social_network__n2__s50__reliable__g2__v_ [--plan-id ...] \
    [--repair-order declared|reverse] --reset-cmd '<lane>/reset.sh' --out rows.jsonl --detach
```

It resolves the composite exactly as an experiment does (every snapshot prepared before anything is
injected), then rows: `baseline`, `inject` (composite_valid, every fault live), `all_broken` (the
composite and every per-fault verdict fail, again after `--persist-s`), `repair:<i>:<kind>` (one
reference repair at a time: the composite must still fail until the last, and no unrepaired fault's
verdict may pass), `all_fixed` (composite, every verdict, collateral and traffic pass), `recovery`,
`recovered`, `plan_summary`. Statuses are VALID/FAIL/INVALID as in the single-fault harness.
Component faults repair through `reference_mitigation()` (their snapshot survives for the verdict),
other faults through `recover_component`. Injected state of every fault is persisted per plan and a
later run recovers leftovers first.

Known coupling (warning `warn_repaired_verdict_fails`, not a failure): every component fault's
`data` check re-reads the same entry-point reads, which traverse several stores. A repaired fault
can therefore still report `data_lost` while another live fault breaks those reads; the composite is
unaffected (it needs every fault fixed), only `faults_fixed` undercounts until the last repair.

## MongoDB faults

Driver `components/mongo.py` runs the server's shell in its pod (`mongo --eval`, one JSON line
back) and raises `ComponentError` with the namespace and workload. Primitives come as
inject, repair and status:

| Fault kind | Inject | Reference repair | Status read |
| --- | --- | --- | --- |
| `mongo_write_lock` (target) | `fsync_lock` | `fsync_unlock` (until count 0) | `lock_state` |
| `mongo_write_rejection` (target, collection, mode) | `set_validator` (a `$jsonSchema` no document meets) or `add_blocking_unique_index` | `clear_validator` / `drop_blocking_unique_index` | `validator_state` / `blocking_index_state` |
| `mongo_collection_renamed` (target, collection) | `rename_away` (to `<name>_old`), then flush the collection's cache | `merge_back(strategy)`: fold the newer documents into `_old` (`append_only`: insert new `_id`s; `union_arrays`: also add new array elements to an aggregate document, as user-timeline's one-document-per-user), then rename over the name with `dropTarget` (a plain rename back fails once writers recreated the name). Decoys `rename_back_drop_target` (every write made during the fault is lost) and, for aggregate collections, `merge_overwrite` (the old history is lost) both verdict `data_lost`: a marker is composed before the fault and another during it, and both must be readable after | `collection_state` |
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
mode), `merge` (`append_only` or `union_arrays`, the rename repair's strategy) and `faults`, the Mongo fault kinds verified live on that collection; only those sample it
(`mongo_targets`). `write_check` (a `WriteCheck`: POST, then a read that must show the marker) lets
write-blocking faults be seen where the app swallows a failed database write: social-network's
compose is one-way and answers 200 whatever mongod did, so a rejected post is invisible to the
HTTP probe; the verdict of `mongo_write_lock`/`mongo_write_rejection` writes a marker post as the
probe user and reads it back. Live findings (social-network): write lock is visible on
user-timeline, post-storage (compose) and social-graph (follow); write rejection on user-timeline
and post (not social-graph: the follow path hides the rejected write; the `user` collection is
only written at registration: `register` is setup-only in the traffic probe and its outcome is not graded, login reads through user-memcached, so `user-mongodb` has no user-visible symptom and is in no fault's target set; it can return when a probe registers a fresh user and grades the status); rename is visible on user-timeline and post
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

## Scope of single-record data faults (generation 3)

Five kinds corrupted ONE record, chosen among SREGym's own probe users (DX: `redis_poisoned_timeline_entry` on
`9200:followers`, `redis_timeline_key_renamed` on `9101`, `social_graph_stale_cache` on `9200:followers`), so SDO's probes
over probe-owned identities could not see them, and no real incident looks like that. Generation 3 gives them a
`scope` (`problems/scope.py`, pure and seeded):

| scope | meaning | drawn in g3 |
| --- | --- | --- |
| `wide` | every eligible record (a whole keyspace: a bad deploy, a botched key migration, broken cache invalidation) | yes |
| `partial` | a seeded random fraction (25% or 50%, at least 2 records) | yes |
| `single` | the original fixed record (`key`, the probe user) | only on an explicit opt-in: `SREGYM_ALLOW_SINGLE_SCOPE=1` (`component_redis.single_scope_allowed`) |

Records are chosen from what the app really holds, at `prepare_injection` while healthy, never from SREGym's own users
(`uid_below=9000` excludes 9100/9101/9200/9201/9210): Redis rows list a `KeySpace` (`RedisInstance.keyspace`: SCAN glob,
regex, entry-point read) and the kinds verified with scope (`scoped_faults`); Mongo collections and memcached rows name a
`reader_field` (the user a document or cached value belongs to) and the per-user `read`. A cold cache (a restarted
`social-graph-redis`, an empty post cache) is warmed by composing or reading as seeded users, never by shrinking the scope; fewer
than `min_keys` eligible records is a `ComponentError`.

| Kind | wide | partial | Reference | Scoped decoys (declared) |
| --- | --- | --- | --- | --- |
| `redis_poisoned_timeline_entry` (home-timeline-redis, social-graph-redis) | the member in every user's sorted set | in a fraction | `ZREM` of that member from every damaged key (one `EVAL` batch) | `remove_member_first_key` (not cleared), `delete_keys` (data_lost on home-timeline-redis, passes on the cache), restarts as before |
| `redis_timeline_key_renamed` (home-timeline-redis) | every timeline key renamed `<uid>:bak-<tok>` | a fraction | merge every backup back (`ZUNIONSTORE` max, then `DEL`) | `merge_first_key` (not cleared), `delete_backup_keys` (data_lost), `rename_back` (passes) |
| `social_graph_stale_cache` (social-graph-redis) | one follower dropped (and a ghost added) in every author's cached set | in a fraction | `DEL` of every damaged set (refills from Mongo) | `readd_first_author` (not cleared), `readd_follower` (passes) |
| `mongo_poison_document` (post) | every seeded post malformed | a fraction | `unpoison_documents` for all | `unpoison_first_document` (not cleared), `flush_cache` (not cleared) |
| `memcached_poisoned_entries` (post-storage-memcached) | every cached seeded post poisoned | a fraction | delete the poisoned keys | `delete_first_key` (not cleared), `flush_all` (passes) |

The other Mongo and Redis kinds act on a whole server or collection (write lock, write rejection, renamed collection, ACL,
maxmemory, pause, replica) and need no scope. Hotel's `mongo_poison_document` and `memcached_poisoned_entries` have no per-user
read, so their g3 draw keeps the g2 scopes without the single ones (`all_seeded`, `broad`/`all_hot_keys`).

The verdict checks the whole incident, not a sample: `fault_cleared` counts the damaged records (one `EVAL` batch through the
read-only check user, which may run scripts but not write), so fixing one record is `fault_not_cleared`. Visibility comes from
`extra_reads()`: a spread sample of up to 3 damaged users' reads, snapshotted while healthy and compared after the repair
(component `data`), and, for follower sets, `write_checks()`: compose as two damaged authors and read a follower's home timeline
(component `write`). Both are the fault's own records, so a partial scope is visible even when it misses the fixed integrity users.

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

## Catalog status (generation 2, live validation of 2026-11-04)

A kind is reliable (in `__reliable__g2` plans) only with 3 seeds where every trial is VALID with its declared
reason. Flags and one-line reasons live in `composition/reliability.py`; rows in `validation/<family>/`
(outside the repository). "VALID n/m" counts trials; seeds are fault draws (distinct targets or variants).
Every kind: reference repair passes; decoys `restore_source` and `rollout_restart:<consumer>` end
`fault_not_cleared`; where a store restart loses data, `restart_stateful_pod` and `rollout_restart` end `data_lost`.

| Kind | App | Family | Parameters | Reference fix | Extra decoys / acceptable | Validation | Pilot |
|---|---|---|---|---|---|---|---|
| redis_replica_misconfig | social | Redis | target Redis, bogus master | `REPLICAOF NO ONE` | restart (data_lost on home-timeline-redis) | reliable (F1 seed 2) | P4 L1: SDO did not detect (replica serves reads) |
| memcached_poisoned_entries | social, hotel | cache | target, corruption, key scope (g2: probe, broad; g3: wide, partial) | delete the poisoned keys | flush_all, restart (acceptable: a cache) | g2 social reliable (seed 5, broad 3/12/15 VALID); hotel flagged: seeds 1,3 data_lost; g3 scoped draw (social) flagged: only `prepare_target` ran live | P4 L2: not detected |
| redis_acl_revoked | social | Redis | target, read or write | `ACL SETUSER default +@all` | restart (acceptable on caches; data_lost on home-timeline-redis) | reliable (home-timeline-redis seeds 1-3 reference/restore/consumer rollout VALID; VAL2 restart decoys seeds 1 write, 4 read, 8 VALID data_lost; user-timeline seed 7 all VALID) | P4 L4 not measured |
| redis_out_of_memory | social | Redis | target | restore `maxmemory` | restart (as above) | reliable (seeds 9, VAL 1, VAL2 2 all VALID 8/8 incl. data_lost restarts) | no |
| redis_writes_paused | social | Redis | target | `CLIENT UNPAUSE` | restart (as above) | flagged: 1 seed (5) VALID 6/6; VAL2 seed 1 VALID through the restart decoy, interrupted at the time box | no |
| redis_poisoned_timeline_entry | social | Redis | target, shape, scope (g3: wide, partial; g2: key) | `ZREM` the poisoned member | delete_key, restart (acceptable on caches) | g2 (single key) reliable (seeds 7, 1, VAL2 2 all VALID; seed 5 FAIL was on the pre-fix harness, not rerun); g3 flagged: wide s2 and partial s5 on home-timeline-redis VALID 7/7 each, 2 of 3 seeds, social-graph-redis variants not run | no |
| redis_timeline_key_renamed | social | Redis | target, scope (g3: wide, partial; g2: key) | merge back with `ZUNIONSTORE` | delete_backup_key, restart (data_lost) | g2 (single key) reliable (seed 1 8/8, VAL2 seeds 2, 3 VALID 8/8 each incl. data_lost restarts); g3 flagged: partial 0.25 s1 and wide s4 VALID 8/8 each, 2 of 3 seeds | no |
| social_graph_stale_cache | social | Redis | ghost ids, scope (g3: wide, partial; g2: key) | restore the follower set (`readd_follower`) | restart (acceptable) | g2 (single key) reliable (VAL: seeds 1-3 VALID 8/8 each, after the stale-follower and write-retry fixes); g3 flagged: wide s4 and partial 0.5 s2 VALID 7/7 each, 2 of 3 seeds | no |
| mongo_write_rejection | social, hotel | Mongo | collection, mode (validator, ...) | clear validator / drop blocking index | restart (data_lost) | social reliable (3 seeds VALID); hotel flagged: decoys INVALID | P5 (write path) blocked by deployer, see P5.md |
| mongo_collection_renamed | social, hotel | Mongo | collection | `merge_back` then rename with `dropTarget` | restart (data_lost) | social reliable; hotel seeds 1,3 VALID, seed 2 FAIL | P5 queued |
| mongo_write_lock | social, hotel | Mongo | target | `fsyncUnlock` | restart (data_lost) | hotel reliable (3 seeds, 6/6 each); social flagged: VAL2 seeds 1, 2 all VALID 6/6; 5 and 7 INVALID earlier; seed 3 not run | no |
| mongo_poison_document | social, hotel | Mongo | collection, shape, scope (g3 social: wide, partial) | `unpoison_documents` | flush_cache (not cleared), restart (data_lost) | flagged: social VAL2 seed 1 reference/flush/restore VALID, interrupted before restarts (earlier references INVALID); hotel decoys FAIL (no symptom) ; g3 scoped draw (social) not run live | no |
| process_frozen | social | process | target service/store | `kill -CONT 1` | restart (acceptable on stateless) | reliable (VAL: seeds 1 post-storage-mongodb 9/9, 2 user-timeline-redis 7/7, 3 compose-post-service 6/6; also a frozen Redis now verdicts without exec into it) | P4 L3/R3/S3: learned stall playbook, reused deterministically |
| pod_network_impairment | social | process | target, delay 3000/6000 ms or loss 90/95% | delete qdisc | restart (acceptable) | reliable (VAL: seeds 1 post-storage-mongodb 9/9, 2 user-timeline-redis 7/7, 3 compose-post-service 6/6) | P5 B2: folded into the stall/restart playbook |
| connection_exhaustion | social | load | compose-post-service, ~12000 sockets | stop the hog | restart is a decoy | reliable (VAL: seeds 1-3 VALID 6/6 with `--symptom-samples 3`: the OOM/restart waves can leave one probe reading in a healthy gap) | no |
| traffic_surge | social | load | entry-point load | stop the client | none | dropped | no |

VAL round (2026-11-04, two lanes, host load 50-190 so trials took 5-10 min): promoted `process_frozen`,
`pod_network_impairment`, `social_graph_stale_cache`, `connection_exhaustion`. Not run before the time box, still
flagged with their earlier evidence: `redis_writes_paused`, `redis_poisoned_timeline_entry`,
`redis_timeline_key_renamed`, `redis_acl_revoked` (restart decoys on home-timeline-redis), social `mongo_poison_document`
and hotel `mongo_collection_renamed`; `redis_out_of_memory` and social `mongo_write_lock` are partial (above).
VAL2 round (2 lanes, 3 h): promoted `redis_out_of_memory`, `redis_poisoned_timeline_entry`,
`redis_timeline_key_renamed`, `redis_acl_revoked` (restart decoys). Still flagged: social `mongo_write_lock` (2 of 3
seeds), `redis_writes_paused` (1 of 3 plus one partial), social `mongo_poison_document` (partial seed 1), hotel
`mongo_collection_renamed` and the direct `process_frozen --set target=home-timeline-redis` run (not reached).
FS round (generation 3 scope, 2 lanes, 2026-11-05, `validate.py --generation 3`): every row VALID with its declared reason,
2 seeds per kind (3 needed to promote, not reached, so no g3 promotion and the kinds stay flagged from generation 3 in
`reliability.UNRELIABLE_FROM`; g2 flags are untouched).

| Kind (home of the damage) | Scope, seed | Trials (all VALID) |
|---|---|---|
| `redis_poisoned_timeline_entry` (home-timeline-redis) | wide s2 (non_numeric), partial 0.5 s5 (dangling_id) | reference, `remove_member_first_key`, restore_source, rollout_restart of the consumer, `delete_keys` (data_lost), restart_stateful_pod and rollout_restart (data_lost): 7/7 each |
| `redis_timeline_key_renamed` (home-timeline-redis) | partial 0.25 s1, wide s4 | reference, `merge_first_key`, `rename_back` (passes), restore_source, consumer rollout, `delete_backup_keys` (data_lost), both restarts (data_lost): 8/8 each |
| `social_graph_stale_cache` (social-graph-redis) | wide s4, partial 0.5 s2 | reference, `readd_follower`, restarts (acceptable on a cache), restore_source, consumer rollout, `readd_first_author`: 7/7 each |

Not run live: the social-graph-redis follower-set variant of the poisoned entry (compose-and-read check; `s12` wide and `s1` partial
were queued), scoped `mongo_poison_document` and `memcached_poisoned_entries` (unit-tested; memcached `prepare_target` ran live and chose
380 seeded entries after warming), and the third seed of every kind. Trials take 3 to 6 minutes on the Redis timeline kinds and 10 on
the stale cache (two compose-and-read checks that fail slowly while the fault is live), so a 3-hour two-lane box covers about 6 seed runs.
The stale-cache `readd_first_author` decoy first ran INVALID on both seeds (it captured its keys before `prepare_injection` chose them; fixed
and rerun alone: VALID).

Harness and fault fixes behind the promotions:

- The harness waits (polled, bounded by `--ready-deadline-s`, default 900 s) for rollouts and a passing baseline probe
  before each seed's baseline, after every reset and before the final recovered check, instead of judging a freshly
  reseeded lane.
- A fault's own checks must not depend on the component it disables (`disables_target`): a frozen Redis cannot run
  `redis-cli`, so witness and seeded-read checks are skipped while the fault is live (it is `fault_not_cleared`
  anyway) and run after the thaw, where they decide `data_lost`.
- `social_graph_stale_cache` owns its dropped follower (user 9210): the traffic probe's own setup `follow` re-added the
  old follower (9201) between the harness's two symptom readings, so every earlier trial said "no symptom that
  persisted". A blocked-write marker is also retried with up to 3 fresh writes (a write right after a Redis restart can
  fail once on a stale pooled connection).
- `connection_exhaustion`'s symptom comes in waves (the hog OOM-kills and restarts compose-post-service), so a single
  reading can land in a healthy gap; the harness option `--symptom-samples N` (visible if any of N readings shows
  it) was needed, and the rows record `samples`.

Dropped and not offered:

- `traffic_surge` (in the catalog, always flagged): the surge OOM-kills user-timeline-service and wedges its
  Mongo (100m CPU), so the reference fails at verdict time (14/18, 29-31/36); needs a redesign that also
  restarts crashed pods.
- `noisy_neighbor` (class and driver kept, not in the catalog): a CPU burner on the 3-CPU kind worker only
  slowed the probe (8 s to 14 s), never failed it; it would need a latency budget in the verdict.
- nginx slowloris: not feasible; would need more than 64 workers x 1024 held connections on a 3 CPU node,
  and nginx's 60 s header/body timeouts free them (S1-FEASIBILITY.md).
- Clock skew / JWT: not feasible; pods share the node clock, no libfaketime in any image, and the wrk2 routes
  do not verify JWTs; the secret is manifest drift (S1-FEASIBILITY.md).

Integrity reads poll up to 3 times, 5 s apart, before reporting `data_lost` (a restarted store or service
answers empty until its pool reconnects). A Redis row with `restricts_identity` is read through a privileged
check user, so ACL faults do not blind the verdict.
