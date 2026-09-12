# #2444 Bounded Writer Migration Handoff

Workspace: `/tmp/agent-g-2444-implementation`. No commit created.
Gate read: https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272.
Read the three-contract audit and original consolidated census; did not adopt
the superseded timeout proposal.

## Exact Production Scope

All paths below are relative to the shared workspace.

| File | Migrated operation and preserved behavior |
| --- | --- |
| `internal/store/internal/adminpersistence/destructive_reset_cleanup.go` | `ApplyDestructiveResetCleanup` uses `RunTransactionWithOptionsOutcome`; nil write options, explicit `ReadOnly: true` for dry-run. Entire cleanup/count result built with callback SQL context. Retained startup transaction helpers unchanged. |
| `internal/store/internal/schemastore/postgres_bootstrap.go` | Bootstrap advisory transaction lock, compatibility inspection, fresh DDL, origin, and generated state plans execute in `RunTransactionWithOptionsOutcome` with original default options. Schema admission follows acknowledged COMMIT, not callback success or absence of cleanup errors. |
| `internal/store/internal/backend/runlifecycle/active_run_quiescence.go` | Quiescence, preview, and reset receipt compose in `RunTransactionOutcome`; original author-activity-before-run-lock order retained. Helper receives SQL context. Preview and empty/no-reset selections roll back to a local SQL savepoint before outer settlement, preserving initialization rollback without ignored/unowned cleanup. Reset receipt still commits when no runs remain. |
| `internal/store/internal/routingrules/owner.go` | Entity flow-instance lookup plus active upsert, deactivation, and insert-inactive fallback share one `RunTransaction`. RowsAffected errors now propagate instead of being ignored. Standalone route loading unchanged. |
| `internal/store/internal/ingresspersistence/owner.go` | Ensure insert/readback and transition-event update use `RunTransactionOutcome`. Existing transition owner also gates its state/changed result on acknowledged COMMIT. SQLite implementation unchanged. |
| `internal/store/internal/mailboxpersistence/postgres.go` | Insert, notification update, and expiry CTE UPDATE RETURNING use the transaction owner. Expiry scan and rows close finish inside the callback; scan/close errors are joined. Insert identity and expiry rows survive acknowledged-COMMIT-plus-cleanup error, but are withheld when COMMIT is not acknowledged. Standalone list/count/get reads unchanged. |

All result-bearing changes retain result plus postcommit error independently.
No automatic callback retry, provider detachment, new framework, global BeginTx
shim, or new durable model. Existing durable candidate/effect/receipt SQL and
retained startup helpers were not replaced. No separate candidate sink owner
was added in this slice.

Source check: no `backend.BeginTx` or direct `backend.Exec*` remains in these six
files. Remaining direct pooled queries are standalone SELECT projections.

## Dry-Run Classification

Reset cleanup remains a strict read-only transaction, now fully owned through
result and cleanup settlement. It retains default isolation rather than gaining
the generic repeatable-read runner's options.

Active quiescence dry-run is NOT a pure ephemeral-read exception:
`privateauthoractivity.Begin` inserts the singleton if absent and locks it before
run `FOR UPDATE` locks. It therefore retains writable default options and owned
SQL drain. A local savepoint reverses even that initialization on successful
preview/no-op paths. Failure instead goes through the backend's full rollback.
The same helper and lock order serve real writes, with no provider work inside.

## Added Tests

- `internal/store/internal/runtimepersistence/bounded_writer_cancellation_test.go`: `TestBoundedPostgresWritersCancelBeforeCommit` has 11 branches times 3 cuts (33 cases): ingress ensure/event, routing active/deactivate/insert-inactive, mailbox insert/notify/expire, quiescence, quiescence preview, reset cleanup. Cuts are stopped admission, notice-triggered cancellation during real SQL, and independent server exception alongside cancellation. Checks no uncommitted result, unchanged durable state, same healthy PID, and healthy successor. `TestPostgresQuiescencePreviewAndEmptySelectionRollBackInitialization` verifies fresh singleton initialization remains absent after both preview and empty real selection.
- `internal/store/internal/schemastore/postgres_bootstrap_exit_test.go`: `TestPostgresBootstrapOwnsSQLAndCommitOutcome` exercises success, stopped admission, cancellation during SQL, independent failure alongside cancellation, and injected loss of a real successful COMMIT acknowledgement. Checks owned SQL contexts, no callback replay, durable schema, and schema-admission outcome. Lost acknowledgement is driver-wrapper fault injection, not a network/process-death proof.

## Exact Passing Commands

Run from `/tmp/agent-g-2444-implementation` with the existing PostgreSQL test DSN.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestBoundedPostgresWritersCancelBeforeCommit$' -count=3 -timeout=3m -v
```

PASS, 80.184s; 99 subcase executions, no skips.

```sh
go test -race ./internal/store/internal/schemastore -run '^TestPostgresBootstrapOwnsSQLAndCommitOutcome$' -count=3 -timeout=3m -v
```

PASS, 8.830s; 15 subcase executions, no skips.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^(TestBoundedPostgresWritersCancelBeforeCommit|TestPostgresQuiescencePreviewAndEmptySelectionRollBackInitialization|TestRuntimeIngressStatePersistsTypedTransitions|TestPostgresStore_Mailbox_CRUD_Expire_Notify|TestManagerStore_.*RoutingRules.*|TestPostgresStore_ApplyDestructiveResetCleanup_.*|TestActiveRunDeliveryQuiescenceReadbackParity|TestActiveRunQuiescenceCancelsExactGenericAndWorkflowTimerFamiliesOnBothStores|TestResetQuiescenceReceiptCommitsWithRunCancellationBothStores|TestPostgresSchemaBootstrap.*|TestSchemaBootstrapRollsBackFailedFreshCreation)$' -count=1 -timeout=5m -v
```

PASS, 79.225s; 32 top-level tests, no skips. Includes both-store delivery
quiescence/readback, timer-family cancellation, reset receipt atomicity, and
fresh-schema rollback. This run compiled immediately before the final mailbox
scan/close error-join edit; the subsequent three-repeat writer matrix covers
that final edit.

```sh
go test -race ./internal/store/internal/adminpersistence ./internal/store/internal/schemastore ./internal/store/internal/backend/runlifecycle ./internal/store/internal/routingrules ./internal/store/internal/ingresspersistence ./internal/store/internal/mailboxpersistence -count=1 -timeout=3m
```

PASS; schemastore 9.958s, other five packages compile with no package-local tests.
`git diff --check` passes for all eight owned production/test files.

Earlier runtime test attempts hit temporary concurrent runfork options/type
and eventpersistence handoff-signature compile mismatches. These were not edited
by this slice; the successful runs above followed their owner fixes.

## Limits

Full `go run ./cmd/swarm-test`, composed serve/crash/transport proofs, and
per-owner injected postcommit cleanup-failure matrices were not run by this
slice. Outcome propagation uses Kepler's acknowledged outcome API; no generic
failure-class closure is claimed from these targeted passes. No shared server
was stopped, no live provider called, and no settled delivery replayed.

## Independent Read-Only Outcome Review

Production edits were frozen for this review. Reviewed the current PostgreSQL
and SQLite transaction owners, candidate handoff outcome helpers, and
result-bearing completion, event, lifecycle, pipeline, and LLM consumers.

### Actionable Finding

P1: Runtime rotation consumers discard an acknowledged successor lease on a
postcommit error. `internal/runtime/llm/session_rotation.go:62` and `:104` return
nil whenever `Registry.Rotate` returns an error. The migrated PostgreSQL owner
(`internal/store/internal/backend/llmpersistence/postgres_sessions.go:344`)
explicitly returns the committed lease alongside a handoff error. Both helpers
therefore leave the runtime pointing at the terminated predecessor and hide
the successor from their callers. `prepareManagedSessionForTurn` then releases
the predecessor (`session_rotation.go:37`), while PostgreSQL Release requires
the exact active session ID (`postgres_sessions.go:194`). The successor stays
leased until expiry. Retain and adopt the acknowledged successor even when
returning the independent error, and release that successor on the error path.
Add consumer-level turn-limit, parse-failure, and prepare/release tests.

Reproduced without changing shared source using an external Go test overlay:

```sh
go test -race -overlay=/tmp/agent-g-2444-review-overlay.json ./internal/runtime/llm -run '^TestReviewCommittedRotationResultSurvivesError$' -count=1 -timeout=2m -v
```

FAIL, 0.019s: turn and parse both return nil after a committed rotation; prepare
leaves the successor leased to worker-1. The probe uses an in-memory registry
wrapped to return lease-plus-error after successful rotation and to enforce
PostgreSQL's exact-session release predicate. This is a consumer contract
probe, not a real PostgreSQL transport failure. Probe source is
`/tmp/agent-g-2444-review-rotation_test.go`.

### Passing Review Checks

```sh
go test -race ./internal/store/internal/backend/postgres ./internal/store/internal/backend/sqlite ./internal/store/internal/runhandoff -count=1 -timeout=3m
```

PASS: PostgreSQL 34.508s, SQLite 17.087s, runhandoff 1.009s. Existing tests
exercise callback panic cleanup, PostgreSQL panic plus rollback failure,
SQLite busy-at-COMMIT recovery, and handoff result/error retention.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^(TestCompletionTransactionAcknowledgementBoundaryBothStores|TestCompletionCommittedEvidenceAfterHandoffFailureProbe|TestDecision.*Handoff.*|TestSQLiteWorkflowEngineMutationPreBusyAttemptUsesCallerContext)$' -count=1 -timeout=3m -v
```

PASS, 44.969s, no skips. Covers both-store completion acknowledgement cuts,
committed completion evidence after handoff failure, human/proposed decision
handoff outcomes, and SQLite first-attempt caller context.

No additional concrete defect found in the reviewed transaction/handoff owner
changes. SQLite admission, caller cancellation before commit, first-real-busy
budget start, backoff, and terminal cleanup/uncertain-commit replay exclusion
remain unchanged in the diff. The acknowledged-success short circuit prevents
late cancellation or a cleanup error from erasing or replaying a commit.
These passing tests do not cover the failing runtime rotation consumer path,
nor establish an exhaustive pooled postcommit cleanup-fault matrix. No full
repository suite or new real PostgreSQL transport-failure test was run during
this read-only review. No commit created.

## Final Session Consumer Receipt: C1/C2 Fixed

The earlier rotation review finding and consumer census C1/C2 are fixed in the
runtime session caller slice. Production edits are frozen. C3 authority repair
cleanup is outside this slice and remains parent-owned. No commit created.

Exact production scope is eight files under `internal/runtime/llm`:
`session_rotation.go`, `live_session.go`, `conversation.go`, `api_runtime.go`,
`cli_runtime.go`, `openai_compatible_runtime.go`, `openai_responses_runtime.go`,
and `mock_runtime.go`. Faraday's PostgreSQL/SQLite session result producers were
not edited here; this consumes their nonnil acknowledged lease plus error.

C1: All five provider continuation owners release their exact current lease
with `context.WithoutCancel(ctx)` and join cleanup errors into named returns
without clearing a successful response. Conversation initial/follow-on and
fork-chat boundaries preserve response-plus-error, stop before tool dispatch,
and do not turn a cleanup failure into a new provider admission. The existing
completion owner still controls recovery/refusal, without a new abstraction.

C2: Turn-limit and parse-failure rotation helpers adopt and return the exact
acknowledged successor even when handoff fails. Four provider parse-failure
callers update the cleanup lease independently of error and retain the rotation
error alongside the provider error. Preparation releases the successor, not
its terminated predecessor. Preparation and all five startup/continuation
callers also release a nonnil acquisition result on error; the transient live
session adapter no longer suppresses that lease. No operation retry was added.

### Exact Consumer Tests

- `TestSessionRotationRetainsAcknowledgedLeaseAndErrors`: six cases, turn/parse/preparation with committed and uncommitted results. Checks exact successor adoption, unchanged state without acknowledgement, one rotation, joined cleanup error, and cleanup after caller cancellation.
- `TestSessionPreparationAndProvidersReleaseAcknowledgedAcquireOnError`: six cases, preparation and all five provider continuation owners. Checks one acquire/one release/no rotation, both independent errors, and availability to another lease holder.
- `TestSessionStartReleasesCommittedAcquireOnErrorOrCancellation`: ten cases, all five startup owners with acquire-handoff failure or cancellation after acquisition. Checks required detached release, visible cleanup error, and no startup publication.
- `TestSessionCleanupErrorRetainsResponseAndDoesNotReplayProvider`: two cases, cleanup failure with healthy/canceled caller. Real local HTTP adapter plus conversation returns `done` and the cleanup error with an admitted completion handle. Existing completion recovery consumes that same evidence on another invocation: exactly one HTTP request, one settlement, and no additional lease acquisition. Exact lease is independently reacquired by worker-2 after cleanup.
- `TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome`: eight cases, turn/parse/preparation/acquisition-in-preparation, each with healthy or failing handoff. Real PostgreSQL COMMIT precedes the failing candidate sink. The sink independently observes active successor and terminated predecessor; tests assert exact returned/runtime identity, one rotation, durable row counts, release error retention, and exact successor lease availability to worker-2.

Unit/HTTP tests live in `internal/runtime/llm/session_rotation_outcome_test.go`
and `session_cleanup_response_test.go`; the real PostgreSQL consumer matrix is
`internal/store/internal/runtimepersistence/session_rotation_consumer_outcome_test.go`.

```sh
go test -race ./internal/runtime/llm -run '^(TestSessionRotationRetainsAcknowledgedLeaseAndErrors|TestSessionPreparationAndProvidersReleaseAcknowledgedAcquireOnError|TestSessionStartReleasesCommittedAcquireOnErrorOrCancellation|TestSessionCleanupErrorRetainsResponseAndDoesNotReplayProvider)$' -count=3 -timeout=2m -v
```

PASS, 1.244s; 24 leaves per repetition, 72 executions, no skips.

```sh
go test -race ./internal/runtime/llm -count=1 -timeout=5m
```

PASS, 19.594s on final production and test sources. An earlier full-package run
and first focused no-replay run failed because the new test omitted the required
tool executor; the test fixture was corrected before these final passes.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome$' -count=1 -timeout=3m -v
```

PASS, 13.809s; eight leaves, no skips. `git diff --check` passes for the LLM
production changes and the new PostgreSQL consumer test.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome$' -count=3 -timeout=3m -v
```

Final repeated PASS, 37.105s; 24 real PostgreSQL consumer leaves, no skips.

### Limits

No live provider API/CLI calls, production credentials, provider replay, server
kill, or real transport-loss injection. HTTP proof uses a loopback test server;
its session registry and completion persistence/recovery are fault-injected
test harnesses, not a process-crash or durable-recovery proof. The separate
PostgreSQL test uses the real session/transaction/candidate owners and database,
but injects only postcommit sink failure. It does not prove lost COMMIT response
or arbitrary SQL stalls. SQLite producer and real both-store owner proofs remain
Faraday's scope; shared runtime caller tests do not alter SQLite busy/cancel
policy. Parent owns the full `swarm-test` integration receipt.
