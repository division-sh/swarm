# Post-Implementation Proof Audit: #2439

Current status and final-head proof: [cycle 4](issue-2441-cycle4-postimplementation.md).
The checkpoint status below is historical; the SQLite exit table remains
separate from the PostgreSQL class.

Cycle-3 combined status is in `issue-2321-pr2441-review3-proof.md`.
PostgreSQL transaction exits and the remaining native query-cancellation gap
belong to separate #2442, not a retrospective expansion of SQLite #2439.
The combined PR remains not review-ready; historical positive SQLite receipts
remain valid but do not qualify the still-failing integrated head.

Current review-cycle-2 T7 correction and negative controls are in
`issue-2321-pr2441-review2-proof.md`. Only the owner's canceled Commit result is
normalized; independent callback and real commit/cleanup failures remain intact.
PostgreSQL cancellation/disposition is separately escalated, not absorbed into
this approved SQLite class. No final-head whole-suite closure is claimed.

Rebased integration receipts: `issue-2321-pr2441-rebase-accounting.md`.
No SQLite transaction-exit change was required by the rebase. The new compile
check is not full execution proof, and combined qualification remains blocked.

## PR #2441 Review Pass 1 Accounting

**Current combined result: not review-ready.** The full profile failed the API
pin-page SQLite cell with a coordinator retirement error, now LSF-030 in #2353.
A deterministic coordinator probe fails 3/3; isolated API count10 passes do not
repair it. This is outside SQLite transaction exit ownership. See the separate
`issue-2321-pr2441-review1-escalation.md`; no transaction/runtime fix was made.

No new SQLite runtime finding or repair in this pass. The bounded #2321 CI
consumer and #2432 spec corrections supersede the combined-head qualification
claim until required CI and the new full profile finish. The existing #2439 proof
table below remains historical executed evidence; it does not imply those new
checks passed. No additional live messages or transaction behavior changes.

## Previous Qualified Receipt

Agent: agent-g. Same combined #2321/#2432 PR, separate SQLite repair commit.
Gate: https://github.com/division-sh/swarm/issues/2439#issuecomment-5602701049.
This supplements the reviewer-completed pre-implementation audit on #2439; it
does not ask for a new gate. Final integrated full-profile swarm-test passed at
qualifying HEAD 1f5680f25bdb7fc988ac5e74a9c8bb27c1698336, code/test tree
be9e98fe3fb2504b68dcefa487eaec66a222a519. Subsequent edits are audit documents and
two gofmt alignment corrections in serveapp/main.go. The formatted file exactly
matches gofmt of the qualified source; SQLite/tests/spec are unchanged. Initial
CI static-format failure is retained, not represented as a pass.
Independent PR/merge review remains required.

Update: the pre-master-integration combined full-profile suite exited 0 at the
eb4c8c613 code checkpoint. The branch is now rebased onto B's merged #2440 at
origin/master 47c0e70d8ef89a2c99f0be4af632f24692121b83. B's receipt owner supersedes
the historical #2432 reset integration freeze; #2439 remains a separate commit.
Post-rebase SQLite exit/bootstrap race selections pass (16.971s/13.910s).
The second integrated swarm-test passed the SQLite/store packages but failed one
unrelated custom web-search rate-limit timing assertion. Test-only repair
ae8bbb782 checks admission rather than downstream server-arrival spacing and
adds a delayed-transport control. Historical failures remain failed receipts;
final qualification after the T7 correction below passed. Full accounting is in
the #2321 proof audit.

The lead then reproduced a genuine T7 read cancellation defect at 65b09065f:
automatic rollback before Commit lost context.Canceled/DeadlineExceeded behind
ErrTxDone. The existing early-green checkpoints did not close T7. Commit
74461cda7 repairs the read boundary with errors.Join(ctx.Err(), result), retaining
callback/cleanup causes and no new retry or transaction owner. This implements
the authorized correction in issuecomment-5607060442; spec now explicitly binds
both caller cancellation identities at both transaction boundaries.

## Class And Owners

Working class: selected SQLite transaction termination and exact-connection
disposition on every exit. Immediate parent: selected-store connection/resource
lifetime and transaction-failure preservation. Broader parent #2250 remains open,
including PostgreSQL-specific raw transaction/session/panic cleanup; no universal
backend closure is claimed. The original diagnostic failure was an entry point,
not the boundary. No known child tail remains inside this qualified SQLite class;
the heterogeneous #2250 tail is not credibly countable here.

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
| T7 cancellation | reproduced and fixed | TestTransactionCancellationCuts read/write pool_wait, before_begin, callback_error, before_commit; TestTransactionCancellationRollbackOrders read/write, caller cancel/deadline, rollback in progress/finished before callback return, nil/callback error. Original read boundary fails the deterministic finished-rollback control 3/3 with both ErrTxDone and callback-only results. Repair preserves both causes; race count=50 passes. |
| T7 pool observation | execution-proven through the same corrected path | Same real-driver barrier holds return from physical Close: runner has returned cancellation, exact driver closes=1 while database/sql InUse=1. The connection is already physically disposed, not reusable. Releasing the barrier and executing subsequent single-connection read/write proves opens=2, closes=1, InUse=0 with no sleep or retry. Existing immediate Stats assertion moves after that subsequent-operation boundary. |
| T8 busy recovery | reproduced and fixed | TestReviewerBusyAtCommitRecovery count=3 under race (rollback-journal control, not served WAL contention); existing first-attempt/read/admission budget tests retained. |
| T9 uncertain committed result | execution-proven through the same corrected path | TestTransactionExitFaultDisposition/committed_error{1,3}: real commit then busy-looking error, one callback, durable one, exact disposal. |
| T10 six snapshots | execution-proven through the same corrected path | TestSQLiteStandaloneSelectedReadsBypassMutationAdmission explicitly executes all six, cancellation and unchanged side effects; TestStandaloneSelectedReadAccessModeGuard forbids local Begin/Commit/Rollback; existing consistent-snapshot and positive semantic read tests retained. |
| T11 bootstrap | execution-proven through the same corrected path | TestSQLiteBootstrapOwnsEveryExit count=3 race: fresh/current, inspect error, commit error, rollback error, pre-acquire cancellation, pre-commit cancellation and committed-but-error; exact origin DDL, real DB readback. Full schema plan coverage remains TestSQLiteSchemaStoreBootstrapsPlatformAndGeneratedTables. |
| T12 pool isolation | execution-proven through the same corrected path | ExitFaultDisposition pools1/3, separate connection readback and a pinned surviving connection; other connection remains usable, exactly one disposed on uncertainty. IndependentHandleAndReopen diagnostic proof supplies retained reopen. |
| T13 diagnostics | reproduced and fixed | TestLifecycleDiagnosticAtomicProjection before_commit SQLite/PostgreSQL now PASS, plus concurrency/ack/prefix/provenance/reopen matrix. No repeated transition or duplicate log. |
| T14 combined acceptance | execution-proven through the same corrected path | Full-profile swarm-test PASS, exit 0 at 1f5680f25; SQLite backend 15.800s, runtimepersistence 552.917s, catalog/fork 503.274s, releasee2e 840.195s and all remaining packages passed. #2432 and #2321 retain separate proof accounting. |

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

Final T7 evidence: before-control /tmp/agent-g-cancellation-before-control.log
FAIL 3/3, SHA256 fe08f57d3efcdf7493f861976028761739cd202ca5d5eb6454f1b8acd4ddc396.
Repaired cancellation matrix race count=50 PASS 30.660s,
/tmp/agent-g-cancellation-repeat50-final.log, SHA256
43e2cb2fc0c04b91bfe4b2873613464c821ba5a7970874f2d0d58bda2cf0929a.
Complete cancellation plus physical exit/commit/reviewer controls race count=3
PASS 3.684s, SHA256 6a2dd3b05f2477f47a0d41cf8b172ae5f3db030f2ee48790f6fa6d8df7f6b13c.
The first new count=50 run FAILED a test notification race: callback-return and
runner-done were both ready and select could incorrectly report no callback
entry. Removed that competing receive, preserving the independent barrier and
all production/cancellation/retirement assertions. This failed run is not hidden
or counted green. The corrected repeated matrix above is the actual receipt.
Final unchanged executable/test full-profile swarm-test PASS at qualifying HEAD
1f5680f25 (code be9e98fe3):
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...`.
Log /tmp/agent-g-final-bounded-repairs-full.log, SHA256
1e449b463692002e4457cfb68cdee552a3a29edbad738d679fe6ec8234307db0.
The old stale queue request was cancelled before execution, never claimed as
qualification. No workload reduction, diagnostic retry or physical-owner bypass.

Unchanged golden burst now passes (`/tmp/agent-g-2439-golden-burst.log`, 246.405s),
but this is not causal classification of the earlier PostgreSQL timeout. Original
evidence remains `/tmp/agent-g-2432-integrated-full.log`: one active candidate with
analyzed but no completed event, one due timer, zero active deliveries and zero
unsettled pipeline events. The SQLite repair does not explain that PG occurrence.
The lead recorded this as LSF-028 and the later pre-ingress health-no-result as
LSF-029 in #2353, both observed/unclassified. Ruling issuecomment-5607051620
permits PR opening after bounded capture repair and current-head qualification;
it is not causal closure or merge approval. Any recurrence must be investigated.
Genuine live proof at 5426deb7b remains separately labelled in #2321; this T7
repair made no provider calls or Telegram sends/replays.

Watchlist decision: reviewer refinement swarm-docs@823590f is sufficient; same
existing node and #2439 mapping, no new issue or POTENTIAL_ISSUES entry. Architecture
feedback: exact connection lifetime is now explicit in the existing owner; broader
PostgreSQL cleanup/retained-session audit stays #2250, likely several bounded
owner-specific passes, high correctness ROI but not estimated as a one-line fix.
Achieved implementation closure claim: **failure class eliminated** for #2439,
with all T1-T14 proofs and integrated acceptance passed. Independent merge review
remains required; this does not close PostgreSQL or the broader #2250 class.
