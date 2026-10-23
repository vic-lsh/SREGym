# S3A — SREGym sdo3 interface and implementation

## Interface for S3B (version 2)

These are benchmark control-plane fields only. Never copy hidden identities, injection evidence or oracle results into production agent requests, prompts or memory.

### Injection (worker and deferred HTTP)

Worker request stays `{"op":"inject","problem_id":"...","compose":false}`. Its result retains `started_at`, `finished_at`, `fault` (first global index), and `faults` (all global indices contributed by this problem, in declared order), and adds:

```json
{"protocol_version":2,"injection":{"valid":true,"infra_error":null,"details":{}},"components":[{"fault":0,"incident":0,"problem_id":"mf__...","component_index":0,"injection_order":1,"identity":{"class":"WrongBinUsage"}},{"fault":1,"incident":0,"problem_id":"mf__...","component_index":1,"injection_order":0,"identity":{"class":"PSARestrictedBlocksRecreation"}}]}
```

`component_index` is the declared index within the harness composite (null for a plain problem). `injection_order` is the zero-based global injection position, independent of declared index. `identity` includes `class` plus the generated draw's JSON fields when available, or `problem_id` for a catalog component. Decoys are omitted from addressable components but still recovered. Multiple problem IDs are injected separately with `compose:true`; flatten each reply's `faults`/`components`, never assume one ID contributes one fault.

An invalid injection returns `injection:{"valid":false,"infra_error":"injection_invalid","details":{...}}`, with the same indices retained for cleanup. Injection exceptions also become invalid results. The JSON-lines envelope remains `ok:true` because the request completed; clients MUST check `injection.valid` before starting incident handling. Oracle and further compose requests are rejected while an invalid injection awaits recovery. Cleanup remains available; a recovery failure prevents further injection. A non-composed inject starts a fresh composition only when cleanup has not failed and no invalid injection remains pending.

`POST /inject_fault` retains `status` and `stage` and adds the identical `protocol_version` and `injection` object. Valid means the fault was installed and stage advancement succeeded; terminal `done` alone is never proof of validity. Invalid results require no incident response, reflection, receipt wait or agent-failure credit. Details are benchmark evidence, not agent-visible state.

### Component oracle

`{"op":"oracle","fault":1}` checks only that fault. The reply retains `kind`, `success`, `details`, and adds `protocol_version:2` and `components:[<the addressed component>]`. Omitting `fault` uses official aggregate problem oracles (including collateral/traffic policy), with all `components`. No per-fault timing is implied by an aggregate grade.

### Full recovery for warm lanes

`{"op":"recover","mode":"full"}` restores every injected component, decoys included, in reverse actual injection order. `full` is the worker default; the worker rejects other modes. `{"op":"recover","mode":"full","fault":1}` restores only that addressed fault. Replies retain `seconds` and `recovered` (global indices successfully restored in recovery order), and add:

```json
{"protocol_version":2,"success":false,"status":"partial","mode":"full","recovered":[1],"pending":[0],"errors":[{"incident":0,"component_index":0,"status":"error","error":"RuntimeError: recovery failed"}],"components":[]}
```

`status` is `recovered`, `partial`, `error`, or `timeout`; `success` is true only after all requested cleanup succeeds. `pending` includes every still-unrecovered global fault. `components` identifies requested addressable faults in recovery order. Errors for decoys have a component index but no global fault. Failed faults remain pending and retryable. A timeout leaves an untrusted warm lane: injection stays blocked for this worker's lifetime because the timed-out recoverer may still be running. Other cleanup failures block injection until a successful retry.

Harness `ColocatedComposite` adds `fault_indices`, `component_oracle(index)`, `recover_component(index)`, `pending_components`, and `recover_fault(mode="full")`. Its existing no-argument recovery retains the conductor's namespace-teardown optimization; only explicit `full` guarantees every injected fault is restored. `CompositeFaultProblem` accepts the same explicit `full` mode while preserving its existing complete recovery. Both expose stable component descriptions via `fault_components()` and structured cleanup via `recovery_result()`. Full recovery reports actual errors/timeouts instead of calling them recovered.

## Progress

Interface published before implementation. Based on canonical `origin/sdo3` at `0657113de26471ca7108b7c258edc3665bd55a51`. No clusters, model calls or credentials are used.
