# #2442: Query-Cancellation Provenance Counterexample

Code/test head e1b6ecd52. This is a failed qualification checkpoint, not a
closure claim or a request to ignore a failing test. Existing cycle-3 approval
remains valid for the implemented two-owner transaction exits and shutdown edge.

The one authorized full-profile run completed exit1 with these two unchanged
API tests and the separate compiled startup failure documented in
issue-2321-startup-abort-cycle.md:

- TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate:
  owning context cancellation, scan delivery obligation context cancellation,
  and driver.ErrBadConn from the transaction's deferred rollback.
- TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError:
  owning context cancellation plus scan delivery obligation *pq.Error57014.

The five previously named API signatures passed race count3 before this run.
That sample was not closure, and these new failures supersede any green
interpretation. The callback/transaction exit fixes are real, but do not close
native query cancellation attribution.

## Deterministic Source-Of-Cancellation Proof

Disposable worktree /tmp/agent-g-2442-query-probe is detached at e1b6ecd52.
query_cancellation_provenance_probe_test.go configures the REAL retained driver
connection's lib/pq notice handler. The server executes:

    DO $$ BEGIN RAISE NOTICE 'probe-active'; PERFORM pg_sleep(30); END $$

Only receipt of that notice cancels the actual owning callback/transaction
context. There is no external administrator cancel, expired statement timeout,
mocked query result, database mutation, network provider or timing guess. Both
ordinary read and write transactions return context.Canceled joined to a raw
*pq.Error57014. Their error-leaf check fails **3/3 each**, 1.618s total.
The original cancellation is now retained, but the raw native cancellation
still looks independent to the deliberately strict coordinator classifier.

Command: go test ./internal/store/internal/backend/postgres
-run '^TestQueryCancellationProvenanceProbe$' -count=3 -timeout=1m -v
Log: /tmp/agent-g-2442-query-probe.log. The initial probe had a compile-only
driver.Conn cast error; that was fixed before the six failing executions above.

## Exact Boundary And Decision Needed

Selected delivery ScanDeliveryContinuations enters Backend.RunReadTransaction,
Adapter.ScanContinuations, loadRecord/loadByEventAndRoute, QueryRowContext and
scanRecord. The leaf at adapter.go:2594 wraps the driver's Scan error. The outer
backend receives only an arbitrary callback error. lib/pq v1.11.2 watchCancel
sets its private connection error to ctx.Err and sends cancellation, but native
server query errors remain *pq.Error; Rollback can map private bad state to
driver.ErrBadConn. The public ResetSession/IsValid APIs do not expose the
original cancellation identity.

The implemented owner intentionally preserves arbitrary callback, commit and
rollback errors. Normalizing every callback57014 or rollbackErrBadConn whenever
ctx is canceled would violate the explicit gate: a callback can return an
independent error with those values, and a real connection failure can race
cancellation. The existing owner cannot establish leaf provenance from that
callback error alone. No SQLSTATE/text suppression has been added.

The smallest credible next design must identify where actual PostgreSQL query
and cleanup cancellation provenance is established, before arbitrary callbacks
erase it. Extending a canonical driver/query boundary affects an additional
consumer/producer boundary beyond the two transaction exits. I have not added a
driver decorator, alternate pool, new registry, changed callback protocol,
query-local heuristic, detached query context or retry. Please rule on the exact
allowed provenance owner/boundary before that change; no general lifecycle or
transaction framework is proposed. Keep this on existing #2442/#2321/#2353,
not another issue or retrospective #2439 widening.

The production head remains frozen. The full suite finished and its complete
evidence is retained; no rerun to green. Existing focused fixes
and their positive/negative tests remain intact. No re-review/merge request,
complete-class claim, new live message or settled-delivery replay.
