# Seed-compatible warm-lane recovery

Ordinary `Problem.recover_fault()` is mitigation recovery. It may retain a changed external state,
such as a rotated database credential. It is not a promise that the immutable application seed can
be safely restored afterward.

The fastloop worker's `recover` operation with `mode: "full"` advertises
`recovery_contract: "seed-compatible/v1"` and `protocol_version: 2`. Full recovery calls an optional
public `recover_to_baseline()` hook instead of ordinary recovery. Both composite implementations
apply this rule to their injected components in reverse order, including decoys. Only successful
components are marked recovered; errors remain retryable, and timed-out components quarantine the
worker. Repeating full recovery after success returns success without rerunning completed resets.

The caller performs this ordered operation before reusing a slot:

1. Call `recover` with `mode: "full"`; require the seed-compatible contract and successful recovery.
2. Restore all captured application seed manifests. Stop on any restore failure.
3. Call `refresh`. This invokes optional `refresh_after_recovery()` hooks only after all injected
   components have recovered. Require a successful response with the same contract and version.
4. Verify application health and current workload rollouts before starting the next slot.

A failed manifest restore or refresh may retry the whole sequence. Refresh runs again on every
request, because manifests may have been reapplied between requests. Refresh errors block new
injection until a successful retry; a refresh timeout remains quarantined even after its daemon exits.
The caller must not treat a reset response alone as permission to start the next slot.

The Astronomy Shop credential fault reuses its existing baseline reset to restore the backend and
bootstrap configuration before seed restoration. Post-seed refresh terminates existing database
sessions and rolls out all database clients. Ordinary mitigation recovery retains its existing
rotated-credential semantics. Credential operations capture command I/O, suppress command logging,
and return sanitized failures. Recovery replies contain no credential values or Secret payloads.
