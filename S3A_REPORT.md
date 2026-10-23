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

Completed implementation and offline validation. Interface was published before implementation. Based on canonical `origin/sdo3` at `0657113de26471ca7108b7c258edc3665bd55a51`. No clusters, model calls or credentials are used.

## Implemented and pushed

All pushes target **only** `origin/sdo3` (`vic-lsh/SREGym`), without force. Fetched and rebased against the canonical remote before the implementation push.

- Interface: `fc7394e6a271b3cc88fd2ec383d14c6bbbb236fb`.
- Pilot port: `76a6fa1bbf0e8505e0b48bef13115136be836a74` (includes `d373fe36`, the port of sdo2 `2c737e6`, then `76a6fa1b`, the port of `c46996b`). The sole import conflict retains both `ContainerRunner` and `apply_stage_change`.
- Implementation: **`79871c365a2324493c37376103b369b1880acbd7`**. This is the S3B integration target; the final report-only commit will not change the interface or implementation.

The worker and conductor now consult `injection_failure()` and convert injector exceptions into terminal invalid-injection results while retaining cleanup ownership. Invalid injection cannot be graded or joined by another injection before cleanup. The conductor's existing terminal infrastructure row, no-agent/no-grading behavior and cleanup are preserved. `/status` and `/get_app` do not acquire hidden injection identity.

Both composite classes expose declared fault indices, component identity and injection position, individual oracles and full recovery. Generated identities carry their `FaultDraw` JSON; catalog identities name their registered component. The worker keeps global fault indices separate from component indices and actual injection positions, so a plain problem plus a generated composite exposes four indices for the three-fault test fixture, or three indices for a two-fault composite. Addressing one component never substitutes aggregate grading.

Full recovery restores namespace-local faults too: PSA labels, quota and database runtime state all reach their fault-specific recovery callbacks. It unwinds actual injection order, including retry order, and includes catalog decoys. Legacy decorators that swallow recovery exceptions are bypassed under the recovery deadline. Errors remain pending and can be retried; timeouts remain quarantined because their daemon may still mutate the lane after the deadline. A successful cleanup of some other component cannot clear an earlier failure. Catalog rollback also includes an injector that raises after a partial write; its automatic rollback is bounded and an already-completed rollback can be acknowledged without false cleanup credit.

The conductor retains its default namespace-teardown optimization. Warm consumers must use the worker's `full` default or call the composite with `mode="full"`. Full-mode `recovered` contains successful work only. A single-component recovery may succeed while other, unrequested faults remain pending; check `success` for that request and `pending` for the whole composition. A decoy failure can make `success:false` even with no addressable faults pending; inspect `errors` and retry aggregate recovery.

The two checked-kubectl tests stub `KubeCtl.__init__` instead of loading configuration. Stream-oracle tests now use `tests/fixtures/sregym/fastloop`; standalone harness checkouts set `SREGYM_SDO_ROOT` to the SDO checkout. Application sources were initialized at the existing pin `d1a7e02eb873520d2df3eea9f81f54eda5eb149d`; no submodule pin was changed.

## Validation and evidence

Risk classes: failure handling and platform wiring. The requested handoff gate is level 1 permanent contracts followed by level 5 offline integration/unit coverage. No cluster or live validation was authorized or performed. Real API-server permissions, actual workload restoration and live platform cleanup remain unverified; S3B owns its separately authorized scripted lane run.

All commands source `/mnt/data/shli/cache/env.sh`. No credential contents were inspected. Judge/provider key variables were removed from the full-suite process and the live judge test stayed skipped.

- Failing first: `S3A_contract_red.log` records **9 failed / 4 passed** for SG2 lifecycle boundaries; `S3A_additional_red.log` records **3 failed / 13 passed** for retained cleanup failure, missing component oracle and deferred injection exceptions; `S3A_catalog_red.log` records **2 failed** for partial catalog rollback and nonraising invalid components.
- Initial pilot/image-preparation/invalid-resume selection: **34 passed**, `S3A_port_tests.log`.
- Final combined lifecycle, composite, fixtures, pilot, image-preparation and invalid-resume selection: **229 passed** in 7.03 s, `S3A_focused_final.log`.
- First full offline suite, before the final two catalog fixes: **3,131 passed / 13 skipped**, 74.25 s, `S3A_full_suite.log`.
- Final full offline suite on implementation commit `79871c36`: **3,133 passed / 13 skipped**, 74.59 s, `S3A_full_suite_final.log`. Skips: one unavailable nested train-ticket manifest, one explicitly disabled live judge, ten safety rules without pod watchers, and one absent gitignored trajectory fixture. The stream contract is executed, not skipped. One pre-existing warning concerns the unregistered pytest `slow` marker.
- All 12 implementation/test files pass `uv run ruff check` and `uv run ruff format --check`: `S3A_changed_ruff.log`, `S3A_changed_format.log`. `git diff --check` passes.
- Full-tree Ruff finds two **pre-existing** `SIM115` violations in `tests/test_k8s_proxy_config.py:53-54`; full-tree format checking finds the **pre-existing** `sregym/paths.py` formatting difference with installed Ruff 0.15.10. Both reproduce from `origin/sdo3` versions without this implementation. Logs: `S3A_ruff.log`, `S3A_format.log`. These unrelated files were left intact. SREGym has no `scripts/format_code.sh` or `scripts/check_errors.sh`; its native Ruff configuration was used, and the separate SDO checkout was not reformatted.

Reproduce the full offline suite from `/mnt/data/shli/sregym-sdo3` (the same selection as SG2; cluster integration and legacy external-agent/file-editing suites are excluded):

```bash
source /mnt/data/shli/cache/env.sh
SREGYM_SDO_ROOT=/mnt/data/shli/sdo-exp/mixed10/scratch/sg2-sdo \
  env -u JUDGE_MODEL_ID -u JUDGE_API_KEY -u AGENT_API_KEY \
      -u OPENAI_API_KEY -u ANTHROPIC_API_KEY \
  uv run python -m pytest -q -rs tests/test_*.py tests/clients tests/composition \
    tests/conductor tests/generators tests/llm_as_a_judge tests/oracles \
    tests/problems tests/safety tests/service tests/traces --tb=short
```

Evidence logs live alongside the task at `/mnt/data/shli/sdo-exp/mixed10/`. The implementation and this report are committed in the harness checkout. The SDO checkout and its pre-existing modifications were left intact; no SDO gitlink or production code changed. No PR was created because the task explicitly requested direct fast-forward pushes to `sdo3`.

## S3B handoff

Use `79871c365a2324493c37376103b369b1880acbd7` or the later report-only branch head. Gate both worker and HTTP injection on `injection.valid`, flatten `faults`/`components` for every submitted problem ID, select full recovery, and require `recovery.success` before advancing or baseline reuse. A failed cleanup is an infrastructure failure; a timeout requires discarding the worker/lane rather than retrying into the next incident. The worker keeps serving cleanup and health requests so the benchmark can retain evidence and terminate cleanly.

The requested S3A items (pilot ports and SG2 fixes 3, 4, 5 and the upstream part of 10) are complete. The broader capability, grading, pipeline and result-analysis changes remain with the SDO adapter owner. This handoff does not authorize a live mixed10 run.
