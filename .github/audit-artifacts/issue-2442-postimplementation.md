# Post-Implementation Proof Audit: #2442

Current native-operation, retained-proof and final qualification authority:
[cycle 4](issue-2441-cycle4-postimplementation.md). The checkpoint below is
historical, including its since-removed callback-wide cancellation-worker paths.

Agent-g, PR #2441. Code commit 3da58f72a; integrated code/test head e1b6ecd52
over origin/master b38b84045. Qualification has exposed native query-cancellation
and rollback-disposition failures; no merge closure. The exact counterexample
and requested provenance-boundary ruling are in
issue-2442-query-cancellation-boundary.md and #2442 issuecomment-5611438051.
Binding approval: #2442 issuecomment-5611206618 and #2321
issuecomment-5611206250. The pre-code mapping/spec is eb8113c2d and
issue-2442-implementation-mapping.md. No additional gate was requested.

## Concept, Boundary And Owners

Chosen class: PostgreSQL transaction outcome and transaction-owned resource
release on every exit. The ordinary-only proposal was symptom-shaped and was
corrected before implementation: retained transactions have the same exit
obligation but a genuinely different connection/possession lifetime. Parent
#2250 and transaction-envelope architecture #2412 remain open. #2439 remains
SQLite-only. This repair aims to eliminate the approved two-owner class entirely,
not merge the owners or claim all PostgreSQL cancellation failures are explained.

The authoritative contract is platform-spec.yaml:
engine.runtime_core_persistence_store_contracts, including the ordinary and
retained PostgreSQL exit paragraphs added in eb8113c2d. No SQLSTATE/text blanket
filter, coordinator suppression, callback replay, compatibility, new connection
registry, or transaction framework is introduced.

| Canonical owner / touched consumer family | Systematic disposition and exact execution |
| --- | --- |
| postgres.Backend.runTransaction; RunTransaction and RunReadTransaction | Corrected real semantic owner of one sql.Conn/sql.Tx pair. Deferred rollback/disposal precedes conn.Close on all callback exits, including panic. Read uses repeatable-read/read-only; write retains its existing transaction mode. |
| Activity journal, agent directive, decisions, delivery/dead letter, effects, entity runtime, events, LLM persistence, generic schedules, managed capability, reply context, pipeline/fan-out, run lifecycle | Already consume the ordinary backend runner. Repository census of backend.RunTransaction/RunReadTransaction and generated persistence registry confirms no consumer change or second terminator in this boundary. The full selected-store suite exercises these production consumers; no credit from shared naming alone. |
| RunAuthorityTransaction; AdvisoryLockLease.RunTransaction | Corrected retained transaction-operation owner. Callback panic rolls back, ends activeTx and releases operationMu, while a healthy session and its advisory possession survive. Unsafe commit/rollback fences possession before endTx can unlock it. |
| startupownership/owner.go | Already uses lease.RunTransaction for admission, source-set and heartbeat/ownership operations. Public retained-session capability proof exercises the actual owner, not an alternate connection. |
| startupownership/authority_maintenance.go and reset_operations.go | Already use that same retained transaction entry. Existing exact-session reset and release proofs remain required and were run. |
| pipelinepersistence/selected_fork_commit.go | Already uses state.postgresLease.RunTransaction. Selected-fork lifecycle, log and activation proof remains in the integrated selected-store suite; no fork identity/validation change in this repair. |
| runWithIndependentCallerCancellation | Existing cancellation-worker owner, now joined in a defer even on panic; it retains run error, caller cancellation and cancel-operation failure rather than replacing the first with the second. |
| SessionAuthority.endTx / rollbackSessionTransaction / forceDiscardConnectionLocked | Existing exact private operation/session owner. Healthy rollback preserves possession; failed/externally settled transaction disposes the unsafe session. Already-closed sql.Conn bookkeeping is not another failure; independent errors remain. |
| SQLite and SQLite-only fork planner/channel readers | Different backend contract and #2439 owner, unchanged. SQLite proof remains distinct, not inferred from PostgreSQL results. |

All production RunAuthorityTransaction callers are through AdvisoryLockLease;
the latter's production consumers are the startupownership files and selected
fork commit listed above. RunSessionTransaction had zero production/test callers
outside its own declaration/body and is deleted. Old non-authoritative paths
removed: callback-dependent rollback, panic-skipped endTx, cancellation replacing
callback/commit error, and duplicate closed-connection cleanup evidence. No
ordinary connection is promoted to retained authority and no healthy retained
session is returned after every transaction.

## Manifestation Proof

Tests use real host PostgreSQL with pool size one. A transparent driver wrapper
forwards the real context query/exec, transaction, reset and validity methods;
only named transaction barriers and injected commit/rollback failures differ.
Deadline and rollback barriers control ordering; no query success is fabricated.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Ordinary read/write callback panic strands transaction/connection | reproduced and fixed | TestPostgresTransactionPanicAndFailureExits, read/write panic and panic+rollback-failure. Original panic survives, next pool1 operation completes; write readback excludes leaked/partial commits. |
| Owning cancel/deadline after successful callback loses identity or manufactures TxDone | reproduced and fixed | TestPostgresTransactionCancellationRollbackOrders, read/write x cancel/deadline x physical rollback pending/completed x nil/independent callback. Exact ctx.Err on successful callback; both causes on independent failure; next pool1 operation. |
| Callback TxDone or genuine commit failure gets hidden by cancellation | reproduced and fixed | TestPostgresTransactionPanicAndFailureExits callback_tx_done and commit_canceled. Callback identity remains; real committed readback is one, actual injected commit error retained, no replay. |
| Ordinary rollback failure or panic cleanup leaves live transaction | execution-proven through the same corrected path | Same matrix rollback_failure and panic_rollback_failure. Real connection disposed, next operation/readback proceeds; cleanup error retained or logged while original panic propagates. |
| Admission fails or nil callback | execution-proven through the same corrected path | TestPostgresTransactionAdmissionExits, ordinary/retained x nil/canceled/begin-failure/closed; no callback admission, error identity, no activeTx, next pool1 operation. |
| Retained callback panic leaks activeTx/operationMu | reproduced and fixed | TestRetainedPostgresPanicPreservesPossessionAndNextOperation and TestRetainedPostgresTransactionExitMatrix/panic. Exact original panic, activeTx cleared, subsequent transaction and real pg_locks possession. |
| Retained cancellation hides callback or cancellation-worker failure | reproduced and fixed | TestRetainedPostgresTransactionExitMatrix cancel/deadline/callback_canceled/cancel_failure, joined typed causes, rollback readback and healthy advisory possession. No fabricated native/server query cancellation is classified. |
| Cancellation worker outlives panic | reproduced and fixed | TestRetainedPostgresCancellationWorkerJoinedDuringPanic holds the cancellation worker, proves panic cannot escape before join, then checks original panic. |
| Unsafe retained rollback/commit or auto-rollback remains reusable | execution-proven through the same corrected path | TestRetainedPostgresTransactionExitMatrix rollback_failure/commit_failure/commit_canceled/auto_rollback: fenced session refuses next callback, next pool1 read works, expected committed-or-rolled-back state and no replay. |
| Healthy retained success/rollback loses possession | execution-proven through the same corrected path | Retained matrix and TestRetainedPostgresPanicPreservesPossessionAndNextOperation query actual pg_locks from the next transaction on the same session. |
| Real retained public capability/reset/claim consumers | execution-proven through the same corrected path | TestPostgresProcessCapabilityUsesRetainedSessionWithPoolSizeOne, TestPostgresAdvisoryProofCallerCancellationPreservesExactSession, TestPostgresDestructiveResetLockReleasesExactSession, TestPostgresPipelineClaimUsesExactSessionAcrossCommitAndRollback, TestPostgresTerminalAdvisoryReleaseFailureDiscardsExactSession, TestPostgresAmbiguousAdvisoryAcquireDiscardsBorrowedSessionAfterTransaction. Count3 PASS 3.523s. |
| Historic API/runtime 57014 and cancellation observations | split / escalated as separate class | LSF-033..038 stay individually identified in #2353 and the cycle-3 combined audit. Each exact test was executed; passing samples do not attribute all server cancellation to this owner or authorize filtering. |

Focused complete backend: go test -race ./internal/store/internal/backend/postgres
-count=3 -timeout=3m, PASS 39.565s at code head e1b6ecd52. Admission-only race
count3 PASS 9.045s. Earlier fixture attempts that used multiple statements through
the default prepared adapter or waited for physical close rather than rollback
completion failed; the final wrapper/barriers correct those test assumptions.
Those attempts are not positive proof receipts.

The one full-profile swarm-test completed exit1. PostgreSQL backend13.794s and
selected runtimepersistence507.803s passed, but the real API cleanup paths still
returned raw native cancellation/rollback errors. The server-notice probe fails
read/write3/3. See issue-2442-query-cancellation-boundary.md. Complete #2442
class elimination is explicitly NOT claimed from the passing helper tests.

## Closure, Tracking And Residuals

Achieved: bounded owner implementation and deterministic terminal-path proof;
integrated qualification is failing and complete closure is withheld. The chosen class is
intended to close in this PR; unrelated query execution cancellation is not
reclassified by SQLSTATE or message. Own Commit's exact ErrTxDone is normalized
only with actual owning cancellation; arbitrary callback/commit/rollback errors
are retained. Cleanup failure during panic is logged without replacing the panic.

Watchlist decision: retain lead's pushed 68dd4a1 mapping to
atomic_runtime_state_mutation and shutdown_and_runtime_lifecycle. No new issue or
POTENTIAL_ISSUES entry. #2442 is the concrete tracker, #2250/#2412 the broader
architecture feedback. Long-run direction is clearer transaction envelopes and
startup phases, not a merged ordinary/retained owner. Remaining parent tail is
not estimable from this repair; confidence high for the named terminal branches,
unknown for historically unclassified PostgreSQL/J5 failures. A larger envelope
refactor is multi-PR work with uncertain ROI until its separate census; no such
refactor is necessary to address the independently reproduced defects here.
