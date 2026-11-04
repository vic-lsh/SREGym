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
- `redis.py`: Redis or Valkey via the CLI in the server's pod (`RedisInstance.cli`).
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

`IntegrityCheck(reads, seed, volatile_keys)`: before injection, `inject_fault` requests every
read at the entry point and keeps the normalized bodies (JSON with `volatile_keys` dropped); if
any read is empty or failing it runs `seed` once and reads again. The per-fault verdict requires
the same reads to return the same data afterwards. A repair that clears a symptom by destroying
data (restarting the only copy of the home timelines) therefore fails.

Social network seeds users 9100 and 9101 (9101 follows 9100) and three posts by 9100; it reads
9100's user timeline and 9101's home timeline. These ids are outside the wrk2 workload's range
(1..962) and the probe's (9200, 9201), so only faults and repairs change them.

### Write probe

`grading.EntryPoint` has optional `writes` (form POSTs requested every round, like `paths`) and
`setup` (run once per probe, outcome ignored). Social network writes: compose-post, follow and
login as probe user 9200 (follower 9201, so composes also write a home timeline). Other apps keep
their GET-only probe.

## The contract

Subclass `ComponentFaultProblem` (`sregym/conductor/problems/component_fault.py`):

- `inject()`: put the fault in place through the running system (exec, client pod, node
  tooling). Never leave a Kubernetes object whose deletion is the fix. Raise if it did not take.
- `fault_cleared() -> (bool, detail)`: is this fault's own state gone?
- `reference_mitigation()`: the scripted correct repair (also `recover_fault`).
- `decoy_mitigations() -> dict[name, callable]`: wrong repairs. The base gives
  `restore_source`; add `rollout_restart`, `rollout_restart:<consumer>`, `restart_stateful_pod`
  where they are wrong or harmful in production. Do not list a repair as a decoy where it is in
  fact acceptable (restarting a Redis that only caches a database).
- `acceptable_mitigations() -> dict[name, callable]`: other repairs that are correct for this
  target; the verdict must PASS after them. Decoy expectations are per target, from binding data:
  rollout-restarting `home-timeline-redis` (`holds_primary_data=True`, the only copy of home
  timelines) is a decoy, while restarting `user-timeline-redis` or `social-graph-redis` (caches
  that refill from Mongo) is acceptable. Restarting or flushing a memcached is always acceptable;
  only restarting its consumer and restoring source are decoys.

The per-fault verdict (`ComponentFaultOracle`) is `fault_cleared() AND` the seeded data is
unchanged. The composite then ANDs it with the namespace collateral walk and the traffic probe.

## Add a fault kind

1. If the component kind is new, add a driver module with the inject, fix and check primitives.
2. Add a problem class subclassing `ComponentFaultProblem` that resolves its target row from
   `BINDINGS[app_name]` and calls the driver.
3. Add a catalog entry in `catalog/component_state.py` (or a new family module): `choose` samples
   the target from `instances(binding.<kind>)` plus mode/severity/scope from the rng; the
   footprint is the whole workload (`deployment(name)`, field None) because the state lives in
   the pod. Add the key to `ENABLED_APPS` with only the apps you validated.
4. New keys enter `reliability.UNRELIABLE` for their app until validated live; removing a flag
   changes `__reliable` plan ids and the pinned `tests/composition/fixtures/vanilla_s42_n100_plan.json`.
5. Unit tests with the fake `Kube` in `tests/composition/test_component_faults.py`.
6. Validate live (below) on at least 3 seeds.

## Enable an existing kind on another app

1. Fill (or verify) that app's binding rows for the kind and its `integrity` check, and set
   `verified=True` after checking them on a live deployment.
2. Add the app to the key's `ENABLED_APPS`.
3. Validate live; then decide its reliability flag.

## Validate live

Against a deployed namespace (KUBECONFIG selects the cluster; `restore_source` needs
`SREGYM_ACCEPTED_RENDER`):

```bash
uv run python tools/fault_validation/validate.py --key memcached_poisoned_entries \
    --app social_network --seeds 1 2 3 --persist-s 60 --out /tmp/trials.jsonl
```

Per seed: a `baseline` health row, one `decoy:<name>` row per decoy (passes when the verdict
FAILS), a `reference` row and one `acceptable:<name>` row per acceptable repair (pass when it PASSES), each on a fresh injection with the symptom
checked before and after the persistence window, and a final `recovered` health row. Exit 0 only
if every row passed.

### Lane notes (read before validating)

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
- The harness holds the injected state in its process (which keys it poisoned). If it dies
  mid-trial the fault stays live (a poisoned login key outlived a killed run on the F0 lane):
  clean up by hand with the kind's broad repair (`flush_all` the memcached, `REPLICAOF NO ONE`)
  before the next run; its baseline row fails until you do.
