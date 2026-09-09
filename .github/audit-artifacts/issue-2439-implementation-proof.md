# #2439 Implementation Proof Accounting

Agent: agent-g. Same combined #2321/#2432 PR, separate SQLite repair commit.
Gate: https://github.com/division-sh/swarm/issues/2439#issuecomment-5602701049.
This supplements the reviewer-completed pre-implementation audit on #2439; it
does not ask for a new gate. Final integrated suite and PR review are pending.

Update: the pre-master-integration combined full-profile suite exited 0 at the
eb4c8c613 code checkpoint. Current-master integration is 34d86df18; its final
suite was cancelled while queued after an independent #2432 reset-retention
contradiction was reproduced. See issue-2432-reset-integration-escalation.md.
This does not retract the focused SQLite exit proofs or assert final PR closure.

## Class And Owners

Working class: selected SQLite transaction termination and exact-connection
disposition on every exit. Immediate parent: selected-store connection/resource
lifetime and transaction-failure preservation. Broader parent #2250 remains open,
including PostgreSQL-specific raw transaction/session/panic cleanup; no universal
backend closure is claimed. The original diagnostic failure was an entry point,
not the boundary. No child tail remains inside this SQLite class if final proof
passes; the heterogeneous #2250 tail is not credibly countable here.

`sqlite.Backend.runTransactionOnce` owns the pinned `sql.Conn`, `sql.Tx`, callback,
commit, rollback and final release/disposal. `CloseConnection` is a private backend
disposition utility, not a new transaction/domain owner. `RunTransaction` retains
the existing admission token, caller deadline and five-second busy policy;
`RunReadTransaction` retains read-only snapshot/no mutation admission/no retry.
Actual SQLite busy/locked COMMIT errors permit existing replay only after disposal.
Uncertain COMMIT or failed cleanup never permits replay, including busy-looking
error text. Errors remain attributable; the original callback panic propagates.

All named runtime write families listed in the approved census continue through
this owner: lifecycle/diagnostics, event/delivery/replay/receipts, pipeline/timers,
entity/effects/decision/activity, run lifecycle, directives/sessions, mailbox,
idempotency and channel operations; SQLite conversation/selected fork use their
existing mutation wrapper. No diagnostic-specific retry or public SQL port added.

Six former read terminators now consume `RunReadTransaction`:

| Consumer | State after repair |
| --- | --- |
| AgentSQLite.ListAgentDeliveryLifecycleFacts | Migrated, same transaction/as-of projection. |
| AgentSQLite.readOperatorAgentSummarySnapshot | Migrated; public ListOperatorAgents delegates. |
| AgentSQLite.LoadOperatorAgentDiagnosis | Migrated; summary and queue share one snapshot. |
| AgentSQLite.ListPendingAgentDeliveryFacts | Migrated; exact normalized identity set retained. |
| AgentSQLite.ListPendingAgentDeliveryDetails | Migrated; pagination/as-of retained. |
| standingServiceAdapter.ListStandingServiceStatuses | Migrated; PostgreSQL still uses its existing repeatable-read runner. |

`schemastore.SQLite.BootstrapSchema` remains the distinct raw BEGIN IMMEDIATE
schema-admission owner. It retains the connection through rollback/disposal and
WAL setup, reports genuine cleanup failures, and never treats failed COMMIT as
success. No schema migration or old-store path is introduced.

Deleted: SQLite.Backend.BeginTx, its dead conversation-fork interface requirement,
six caller-owned commit/rollback pairs, ignored bootstrap rollback error. Test-only
hostile ambient/lock fixtures use ConstructionHandle explicitly; they are not
runtime consumers. PostgreSQL-only BeginTx/retained-session paths survive as the
explicit #2250 split. ConstructionHandle/Conn remain legitimate private construction
and process-possession ports. Resolved persistence inventory is regenerated with
each changed operation classified as private backend or existing domain adapter;
no new blanket allowlist or public raw authority.

## Manifestation Proof Table

| Row | Classification | Exact proof / execution |
| --- | --- | --- |
| T1 success | execution-proven through the same corrected path | TestTransactionExitFaultDisposition/success{1,3}: durable once, zero healthy closes. |
| T2 callback/statement failure | execution-proven through the same corrected path | Same test callback{1,3}, existing transaction controls; pool/independent readers agree and next read/write succeed. |
| T3 deferred COMMIT | reproduced and fixed | TestCommitFailureReturnsCleanConnectionProbe, exact preserved G reproducer and callback-error control, count=3 under race. |
| T4 later successful autocommit | reproduced and fixed | TestReviewerCommitFailureCapturesLaterAutocommitWrite count=3 under race; independent durability asserted. |
| T5 failed rollback | reproduced and fixed | TestTransactionExitFaultDisposition/{rollback_failure,panic_rollback_failure}{1,3}; actual SQLite wrapper faults Rollback, exact close, retained primary/cleanup, no retry. |
| T6 callback panic | reproduced and fixed | TestReviewerTransactionPanicCleanup/{read,write} count=3 under race; original context alive, panic identity and next progress. |
| T7 cancellation | execution-proven through the same corrected path | TestTransactionCancellationCuts read/write pool_wait, before_begin, callback_error, before_commit; existing callback cancellation races, no leaked connection/token. Newly exposed canceled-Begin callback entry fixed. |
| T8 busy recovery | reproduced and fixed | TestReviewerBusyAtCommitRecovery count=3 under race (rollback-journal control, not served WAL contention); existing first-attempt/read/admission budget tests retained. |
| T9 uncertain committed result | execution-proven through the same corrected path | TestTransactionExitFaultDisposition/committed_error{1,3}: real commit then busy-looking error, one callback, durable one, exact disposal. |
| T10 six snapshots | execution-proven through the same corrected path | TestSQLiteStandaloneSelectedReadsBypassMutationAdmission explicitly executes all six, cancellation and unchanged side effects; TestStandaloneSelectedReadAccessModeGuard forbids local Begin/Commit/Rollback; existing consistent-snapshot and positive semantic read tests retained. |
| T11 bootstrap | execution-proven through the same corrected path | TestSQLiteBootstrapOwnsEveryExit count=3 race: fresh/current, inspect error, commit error, rollback error, pre-acquire cancellation, pre-commit cancellation and committed-but-error; exact origin DDL, real DB readback. Full schema plan coverage remains TestSQLiteSchemaStoreBootstrapsPlatformAndGeneratedTables. |
| T12 pool isolation | execution-proven through the same corrected path | ExitFaultDisposition pools1/3, separate connection readback and a pinned surviving connection; other connection remains usable, exactly one disposed on uncertainty. IndependentHandleAndReopen diagnostic proof supplies retained reopen. |
| T13 diagnostics | reproduced and fixed | TestLifecycleDiagnosticAtomicProjection before_commit SQLite/PostgreSQL now PASS, plus concurrency/ack/prefix/provenance/reopen matrix. No repeated transition or duplicate log. |
| T14 combined acceptance | not yet closure-credited | Final full-profile swarm-test queued/running; #2432 and #2321 retain separate proof accounting. |

Focused evidence: `/tmp/agent-g-2439-exits-final.log` PASS 2.656s, all new owner
faults/reviewer probes count=3 race; `/tmp/agent-g-2439-bootstrap-proof.log` PASS
2.696s count=3 race; `/tmp/agent-g-2439-integration-focused.log` PASS 26.206s;
`/tmp/agent-g-2439-guards.log` store authority 6.068s and API-spec subset 0.817s.
The first bootstrap driver-panic experiment injected a panic inside database/sql's
driver callback, not a user transaction callback, and deadlocked database/sql's
internal lease. It was removed as an invalid probe for the approved callback
contract, not treated as repaired. Full-schema race repetition exceeded a short
test timeout in regex rendering; the exit fixture now uses real origin-table DDL
and leaves the existing full-schema test intact. Neither change alters an oracle.

## Integration And Tracking

Unchanged golden burst now passes (`/tmp/agent-g-2439-golden-burst.log`, 246.405s),
but this is not causal classification of the earlier PostgreSQL timeout. Original
evidence remains `/tmp/agent-g-2432-integrated-full.log`: one active candidate with
analyzed but no completed event, one due timer, zero active deliveries and zero
unsettled pipeline events. The SQLite repair does not explain that PG occurrence.
Final integrated reproduction/diagnosis remains required; do not claim retry-to-green
as elimination. Historical genuine live proof at 97a9ea04a remains separately
labelled; this repair made no provider calls or Telegram sends/replays.

Watchlist decision: reviewer refinement swarm-docs@823590f is sufficient; same
existing node and #2439 mapping, no new issue or POTENTIAL_ISSUES entry. Architecture
feedback: exact connection lifetime is now explicit in the existing owner; broader
PostgreSQL cleanup/retained-session audit stays #2250, likely several bounded
owner-specific passes, high correctness ROI but not estimated as a one-line fix.
Intended closure is failure class eliminated for #2439 only after final acceptance
and independent review. Current status: implemented/focused proof, not merge-ready.
