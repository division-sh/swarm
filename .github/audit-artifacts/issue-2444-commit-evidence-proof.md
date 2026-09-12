# #2444 Acknowledged Commit Evidence (F2)

Date: 2026-09-10 UTC. Workspace: `/tmp/agent-g-2444-implementation`.
This is the focused F2 implementation receipt, not full qualification. No commit
was created by this slice. Runtime edits are frozen. Parent owns the final spec,
registry refresh, and whole-suite qualification.

## Canonical Contract

The final PostgreSQL method spelling is **RunTransactionWithOptionsOutcome**;
there is no `RunTransactionOutcomeWithOptions` alias.

```go
// postgres.Backend; fn is func(context.Context, *sql.Tx) error
RunTransaction(ctx, fn) error
RunTransactionOutcome(ctx, fn) (bool, error)
RunTransactionWithOptions(ctx, opts, fn) error
RunTransactionWithOptionsOutcome(ctx, opts, fn) (bool, error)
// opts is *sql.TxOptions

// sqlite.Backend
RunTransaction(ctx, label, fn) error
RunTransactionOutcome(ctx, label, fn) (bool, error)

// private runhandoff package
WithCandidateHandoffOutcome(ctx, func(*CandidateHandoff) (bool, error)) (bool, error)
WithCandidateHandoffOutcomeResult[T](ctx, func(*CandidateHandoff) (T, bool, error)) (T, error)
```

The boolean is true only when the owner's native `sql.Tx.Commit` returned nil.
It survives a separately returned cleanup or handoff error. False means no
acknowledged COMMIT, not proof of rollback: a server can commit while its reply
is lost. Callback success, context state, error text, and database error codes
do not establish acknowledged success. The generic handoff owner releases a
result only on true, attempts the live handoff even with an independent cleanup
error, and joins the errors without replaying the SQL callback.

PostgreSQL mutations admit a closed SQL transaction, use a cancellation-neutral
SQL context, and check the logical caller before COMMIT admission. After that
admission the actual COMMIT return governs; late caller cancellation is not
appended to an independent COMMIT failure. The distinct `RunReadTransaction`
remains caller-cancellable. SQLite Begin, callback, and COMMIT retain the original
caller/attempt context and existing busy-retry budget. Acknowledged COMMIT returns
before late cancellation or busy classification can erase success or replay it.

This restores application-visible outcome evidence; it does not promise to
recover errors stock pq/database/sql never expose. No pq patch, cancellation
provenance inference, new authority, or new retry framework is introduced here.
The obsolete error-only `WithCandidateHandoff` and generic
`WithCandidateHandoffResult`, their unused lifecycle aliases, and the dead effect
error-only handoff helper were removed after callers migrated.

## Exact Consumer Mapping

Paths below are repository-relative. Both-store rows mean PostgreSQL and SQLite
selected-store implementations, not a claim that every branch has fault injection.

| Consumer / file | Outcome handling | Evidence in this receipt |
| --- | --- | --- |
| `backend/postgres/transaction.go`: `RunTransactionOutcome`, `RunTransactionWithOptionsOutcome`; `backend/sqlite/transaction.go`: `RunTransactionOutcome` (all below `internal/store/internal/`) | Existing connection/transaction owner returns acknowledged bool independently of deferred cleanup; uncertain COMMIT cannot grant mutation replay. | PostgreSQL owner regressions, full SQLite package, real-SQL completion admission/COMMIT matrix. |
| `internal/store/internal/runhandoff/candidate_handoff.go`: `WithCandidateHandoffOutcome`, `WithCandidateHandoffOutcomeResult` | True preserves result and submits handoff despite transaction error; false withholds result and cancels reservation. Errors remain separately discoverable with `errors.Is`. | `TestCandidateHandoffOutcomeRetainsResultAndIndependentErrors`: four bool/handoff-failure combinations, callback once, exact submit/cancel counts. |
| `internal/store/internal/backend/effectpersistence/completion_settlement.go`: both `SettleCompletion` implementations | Builds `CompletionSettlementResult{Committed:true,...}` on acknowledged COMMIT even when handoff fails; preserves provider-head and continuation admission errors. | F2 probe plus 12-case acknowledgement matrix. Durable attempt, turn, spend, reservation, and candidate readback. |
| `internal/runtime/effects/effects.go`: existing `Handle.SettleCompletion`; `internal/runtime/effects/completion.go`: result contract | Existing consumer uses `result.Committed` independently of error to retain settled handle state, spend projection, and typed continuation. Provider settlement's existing detachment is not generalized to SQLite mutation policy. | F2 calls real Handle; cancellation boundary tests deliberately call the selected-store settlement owner directly. |
| `internal/store/internal/backend/eventpersistence/event_commit.go`: `CommitPublication`, `CommitAPIEventPublication`, shared `commitPublication` | Both stores use outcome-bearing private mutation/handoff helpers. Acknowledged publication/API result survives postcommit error; PostgreSQL API identity-lease release no longer zeros it. Unacknowledged mutation result is withheld. Transaction-internal selected-fork helpers remain subordinate to their caller's owner. | Ordinary publication healthy and injected lost-ack cases on both stores. API release and standalone-run candidate handoff are source-mapped, not separately fault-injected here. |
| `internal/store/internal/backend/eventpersistence/inbound_publication.go`, `sqlite_inbound_publication.go`: `CommitInboundPublication` | Explicit handoff is admitted by acknowledged bool, not absence of cleanup error; retains `CommitResult` and joins transaction/handoff errors. | Existing both-store inbound integrity, replay, and rollback regression test; no dedicated postcommit cleanup injection. |
| `internal/store/internal/backend/delivery/lifecycle.go`: `SettleSuccess`, `SettleFailure`, `postgresDeliveryMutationOutcome`, `sqliteDeliveryMutationOutcome` | Both stores retain committed `Snapshot` through handoff/cleanup errors and withhold unacknowledged snapshots. | Both-store `SettleSuccess` handoff-failure/lost-ack cases. `SettleFailure` shares the migrated outcome path but is not independently fault-injected here. |
| `internal/store/internal/backend/runlifecycle/run_lifecycle_mutation_adapter.go`: `runPostgresLifecycleOperation[T]`, `runSQLiteLifecycleOperation[T]` | Result-bearing lifecycle operations retain their typed results through acknowledged cleanup/handoff errors. | Generic handoff proof; other lifecycle mutations remain covered by existing regressions, not every typed result has a new fault cut. |
| `internal/store/internal/backend/runlifecycle/run_lifecycle_candidates.go`: `RequestCompletionCandidate`, `ExecuteCompletionCandidate` | Request disposition survives acknowledged error. Execution returns `CompletionResult.Committed=true` only after owner acknowledgement. | Both-store candidate request handoff-failure/lost-ack cases; actual Execute path consumed in F2 recovery. |
| `internal/runtime/runlifecycle/domain.go`, `executor.go`: `CompletionResult`, `Executor.runChain`, `Retire` | Committed-plus-error is not rewritten into `OutcomeRetryCurrent`; generic schedule activations and committed outcome survive. Independent error is retained on the existing executor and returned by `Retire`. Invalid committed result is reported, not replayed. | Deterministic executor red/green test plus full runtime lifecycle package race. |
| `internal/store/internal/backend/runlifecycle/run_control.go`, `run_control_sqlite.go`: `runControlTransition` serving stop/pause/continue | Both stores preserve acknowledged `State`, perform handoff despite cleanup error, and join both errors. | Existing transition, pause ownership, stop pending-work, and both timer-family cancellation regressions. No dedicated postcommit cleanup injection. |

The private author-activity and runtime mutation helpers in the corresponding
`owner.go` files carry this same bool through revision finalization; they do not
add another commit authority. Parent owns decision persistence and external
attempt callers; Faraday owns pipeline/LLM siblings. Their separate receipts and
the final registry establish those migrations, not the tests listed here.

## Red Evidence and Recovery Surface

Before the repair, the existing
`TestCompletionCommittedEvidenceAfterHandoffFailureProbe` failed on both stores:
the attempt was durably settled, turn/spend persisted, and the live candidate
submission failed, but the returned result said `Committed=false`. Initial race
run: FAIL 11.636s, tool-output receipt `9ef16e` (no log file saved).

The separate executor regression
`TestExecutorCommittedErrorPreservesContinuationWithoutReplay` initially observed
two executions, lost generic-schedule continuation, and nil retirement error.
Initial run: FAIL 0.004s, receipt `0b900b` (no log file saved).

The green F2 probe releases the failing live sink, starts the real lifecycle
executor against the same selected store, and uses the canonical candidate
reader plus actual `ExecuteCompletionCandidate`. The exact run candidate is
consumed once and durably rearmed with a higher revision. The fixture still has
a live origin lease, so **rearm**, not terminal completion, is the correct
continuation. Independent readback still has exactly one turn and spend row,
the settled attempt, and zero reservations. This is real SQL plus a real
continuation coordinator, not merely a helper/row-existence assertion. It is not
a process-kill, wire-loss, public CLI, or provider-process recovery proof.

## Exact Final Commands and Results

Commands ran from `/tmp/agent-g-2444-implementation`. Logs are local audit
receipts, not committed artifacts; PostgreSQL subtests ran and were not skipped.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^Test(ResultSiblingsPreserveAcknowledgedHandoffOutcomeBothStores|CompletionCommittedEvidenceAfterHandoffFailureProbe|CompletionTransactionAcknowledgementBoundaryBothStores)$' -count=1 -timeout=180s -v > /tmp/issue2444-f2-final-focused.log 2>&1
```

PASS **56.938s**. Top-level tests: F2 **11.91s**, acknowledgement boundary
**21.62s**, result siblings **22.36s**. F2 has healthy/handoff-failure cases on
both stores. The 12-case boundary covers entry cancellation, callback
cancellation, admitted-COMMIT cancellation, refused COMMIT, lost acknowledgement,
and healthy settlement on both stores. The 12 sibling cases cover publication
healthy/lost-ack and delivery/candidate handoff-failure/lost-ack on both stores.

Lost acknowledgement is a test driver returning an injected error **after real
native COMMIT succeeds**, not a TCP proxy. The boundary verifies durable commit
can exist while returned `Committed` remains false and no live handoff occurs.
It also verifies mutation write count/no replay. Existing-run publication does
not request a standalone completion candidate; its acknowledged case is labelled
healthy and does not claim handoff-failure evidence.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^Test(InboundEvidencePersistsTypedNoSubscriberByDesign|PostgresStore_RunControlTransitionsAndStopAbandonsPendingWork|PostgresStore_RunControlContinueRequiresOperatorPauseOwner|SQLiteRuntimeStore_RunControlStopAbandonsPendingWork|RunControlStopCancelsExactGenericAndWorkflowTimerFamiliesOnBothStores)$' -count=1 -timeout=120s -v > /tmp/issue2444-f2-inbound-runcontrol.log 2>&1
```

PASS **22.501s**. Includes both-store inbound integrity/rollback controls and
both-store exact generic/workflow timer cancellation.

```sh
go test -race ./internal/store/internal/backend/postgres -run '^Test(PostgresTransaction|RunReadTransaction|AuthorityTransaction|AuthorityOutcome)' -count=1 -timeout=120s > /tmp/issue2444-f2-postgres-owner.log 2>&1
```

PASS **16.161s**. Mutation cancellation tests now await the logical caller rather
than the detached SQL context; ephemeral reads still exercise native automatic
rollback. Commit-error expectations preserve the independent native error
instead of appending late caller cancellation. Retained authority code belongs
to the session-owner agent; this is shared-tree regression credit only.

```sh
go test -race ./internal/runtime/runlifecycle ./internal/store/internal/runhandoff ./internal/store/internal/backend/sqlite -count=1 -timeout=120s > /tmp/issue2444-f2-owner-packages.log 2>&1
```

PASS: runtime lifecycle **1.017s**, handoff **1.010s**, SQLite **17.120s**. Full
SQLite package includes its existing caller cancellation and busy-policy tests.

```sh
go test -race ./internal/runtime/runlifecycle ./internal/store/internal/runhandoff -count=1 -timeout=120s > /tmp/issue2444-f2-final-wrapper-executor.log 2>&1
```

PASS after dead-wrapper deletion: runtime lifecycle **1.018s**, handoff
**1.010s**. Includes both new deterministic tests named above.

`git diff --check` passed (receipt `d9543b`). Source census after migration found
no old `WithCandidateHandoff(...)`, `WithCandidateHandoffResult(...)`, or
`RunTransactionOutcomeWithOptions` call/definition in `internal` Go sources
(receipt `9301bd`, expected rg exit 1).

## Qualification Limits

This receipt does not claim exhaustive cleanup-failure injection for every
result-bearing sibling, invisible stock-driver error reporting, remote shutdown
latency bounds, provider replay integration, or full-suite success. Public
startup/shutdown, process death, actual TCP COMMIT-loss, retained ownership, and
other agents' writer migrations require their own receipts and the consolidated
gate. The final full-suite/registry run must use the settled shared tree.

## Qualification Follow-Up: Sentinel Identity and Standing Retirement

The unchanged public registry test exposed `errors.Join(nil, sentinel)` changing
the identity of the sole operation error. The PostgreSQL owner now joins caller
cancellation with Begin/callback failure only when `ctx.Err()` is nonnil;
otherwise it returns the original error. Cleanup errors remain joined when
present. No registry assertion was weakened. New
`TestPostgresTransactionSoleErrorIdentity` checks exact `==` identity for Begin
and callback failures in both mutation and ephemeral-read runners.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresRegistry_AcquireFailsClosedOnSuspendedResumableOwner$' -count=1 -timeout=120s -v > /tmp/issue2444-f2-sentinel-before.log 2>&1
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresRegistry_AcquireFailsClosedOnSuspendedResumableOwner$' -count=3 -timeout=120s -v > /tmp/issue2444-f2-sentinel-after.log 2>&1
go test -race ./internal/store/internal/backend/postgres -run '^Test(PostgresTransaction|RunReadTransaction|AuthorityTransaction|AuthorityOutcome)' -count=1 -timeout=120s -v > /tmp/issue2444-f2-sentinel-transactions.log 2>&1
```

Before: FAIL **5.821s**, exact suspended-session sentinel comparison. After:
PASS **8.263s**, all three repetitions. Transaction tests: PASS **22.000s**,
including the four new exact-identity cases. Production changes in this follow-up
are limited to the two error-return branches in `backend/postgres/transaction.go`.

A separate standing-retirement regression was reproduced at this checkpoint;
the approved coordinator-local repair and green receipts follow below:

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity$/postgres$' -count=10 -timeout=120s -v > /tmp/issue2444-f2-standing-retirement-probe.log 2>&1
go test -race ./internal/runtime/deliverycontinuation -run '^TestCoordinatorRetirement(ScanOutcomeMatrix|PreservesFatalDispatch)$' -count=3 -timeout=90s -v > /tmp/issue2444-f2-retirement-classification.log 2>&1
```

Standing probe: FAIL **13.348s**, two failed repetitions out of ten, after the
sentinel repair. The test's coordinator cleanup returned `context canceled`
joined with `driver: bad connection`; PostgreSQL transaction cleanup also logged
that rollback failure. Classifier controls: PASS **1.091s**, count three.

Concrete path: `deliverycontinuation.Coordinator.run` calls `scan` with its
cancellable worker context; `DeliveryPostgresOwner.ScanDeliveryContinuations`
uses caller-cancellable `Backend.RunReadTransaction`. Its transaction owner
preserves a non-ErrTxDone rollback failure. `Coordinator.finish` invokes
`ordinaryCoordinatorStop`, which recursively requires every error leaf to be
owned cancellation; a BadConn leaf therefore remains fatal and is returned by
`Retire`. This is not the sole-sentinel identity class. The run used real stock
pq without injected server/network failure, but code/text/context alone still
cannot establish cancellation provenance for an individual BadConn leaf.

No blanket filter or read-policy change was made. A repair requires coordinating
the retirement policy of this admitted background scan with its existing owner,
not silently detaching all ephemeral reads or declaring BadConn to be owned
cancellation. At this checkpoint the standing-retirement gate was not green.

## Approved Coordinator-Local Read Drain

The parent subsequently authorized draining each admitted selected-store read
at the existing delivery-continuation coordinator, without changing ordinary
ephemeral read APIs. The repair is confined to
`internal/runtime/deliverycontinuation/coordinator.go`; new deterministic proofs
are in `coordinator_read_drain_test.go` in the same package.

| Existing read consumer | Admitted unit | Stop/settlement behavior |
| --- | --- | --- |
| `Coordinator.scan` -> `ScanDeliveryContinuations` | One selected-store page, including its SQL transaction and cleanup | Check logical worker cancellation/retirement before entry, pass `context.WithoutCancel(ctx)`, preserve independent read error, then check stop before consuming the page or admitting another page. |
| `Coordinator.scan` -> `StandingRunRestartDisposition` | One exact run disposition read | Same pre/post gate; a result drained after stop cannot authorize delivery dispatch. |
| `Coordinator.reconcileHeld` -> `ObserveDeliveryContinuation` | One held-delivery observation | Same pre/post gate; do not consume the observation or begin another read after stop. |
| `Coordinator.scan` -> dispatcher | Existing dispatch operation, not detached | Recheck logical stop immediately before admission; retain the original cancellable worker context and existing dispatch error handling. |

`scanStopError` uses the existing coordinator mutex. The successful pre-entry
check is the local admission point against `Retire`; an operation admitted
before retirement may finish, and no lock is held across read I/O or dispatch.
Cancellation-neutral contexts retain owner/authority values. No new lifetime,
scope, registry, SQL interceptor, or error classifier is added.

The existing standing worker lease covers startup enumeration and all later
scans. Startup readiness cannot be returned after the startup context stops.
`Synchronize` requests execute in that worker lifetime; cancelling just the
synchronization waiter stops its wait but does not release an admitted read's
lease. Retirement may time out its wait while the worker still drains, as
before; it does not falsely publish completed cleanup. Independent read errors
return before logical stop checks and remain visible through `finish`/`Retire`.
`RunReadTransaction` is unchanged and still caller-cancellable for ordinary
callers. BadConn and network/server errors are not filtered.

### Deterministic Before/After

```sh
go test -race ./internal/runtime/deliverycontinuation -run '^TestCoordinatorDrainsAdmittedReadsBeforeRetirement$' -count=1 -timeout=90s -v > /tmp/issue2444-read-drain-before.log 2>&1
```

Before repair: FAIL **0.029s**. The 36-case matrix crosses startup/wake/
synchronization, page/standing/held read, retirement/inherited parent cancellation,
and healthy/independent read failure. It observes cancellation delivered to
admitted reads and, in some branches, additional work after cancellation.

```sh
go test -race ./internal/runtime/deliverycontinuation -count=3 -timeout=120s -v > /tmp/issue2444-read-drain-after.log 2>&1
go test -race ./internal/runtime/deliverycontinuation -run '^TestCoordinator(DrainsAdmittedReadsBeforeRetirement|SynchronizeWaiterCancellationKeepsReadLease|RetirementScanOutcomeMatrix|RetirementPreservesFatalDispatch)$' -count=3 -timeout=90s -v > /tmp/issue2444-read-drain-focused-final.log 2>&1
```

After repair: full package PASS **15.033s**, count three. Final focused run PASS
**1.069s**, count three, including the added independent synchronization-waiter
cancellation proof. Barrier assertions verify the existing worker lease remains
active and completion unpublished until read release, read contexts retain
values but have no cancellation, independent failures survive, and no further
read/page/dispatch is admitted after the stopped read drains. Existing joined
failure and fatal-dispatch controls remain green.

### Real Dual-Store Repetition

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity$' -count=10 -timeout=180s -v > /tmp/issue2444-read-drain-dualstore.log 2>&1
```

PASS **28.982s**, ten SQLite and ten PostgreSQL executions, no skips. This is the
same existing real coordinator/selected-store startup and retirement test whose
PostgreSQL branch failed twice in ten before the read-drain repair; the test was
not weakened or rewritten. The deterministic matrix supplies exact in-read
barriers; this real-store repetition supplies integration coverage rather than
a claim that every database run hit the same instruction-level interleaving.

No contract conflict was found with the approved operation-level graceful drain.
This proof does not establish a latency bound for blackholed I/O or change the
existing outer shutdown escalation contract. Production edits are frozen again;
the parent owns the settled-tree full qualification and registry refresh.
