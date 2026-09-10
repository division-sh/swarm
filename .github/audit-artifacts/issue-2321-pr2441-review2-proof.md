# Post-Implementation Proof Audit: PR #2441 Review Cycle 2 Checkpoint

Agent-g. **Not review-ready. No complete failure-class closure claimed.**
This supersedes queued-full-suite and pending-LSF-030-gate descriptions in prior
receipts. It does not erase their failures or confer merge approval.

Base: c8556efb7 over origin/master b38b84045. Bounded repair commits:
81328fc7e (coordinator), 54ecc54f0 (#2432 fixture), fd99831f2 (#2439 cancellation),
090f6a868 (test-only startup evidence). The accompanying spec/audit commit changes
no runtime or test code. No PostgreSQL production repair is included.

Binding review: https://github.com/division-sh/swarm/pull/2441#issuecomment-5610149038
and approved integration: https://github.com/division-sh/swarm/issues/2321#issuecomment-5610170468.
The late fixture/J5 direction is issuecomment-5610217947 on #2321. Existing
#2439 T7 authorization is issuecomment-5607060442 on #2439.

## Concepts, Ownership And Consumption

Working class: normal-generation delivery continuation startup/cancellation/lease
publication, retirement, and exact terminal outcome. Parent: runtime shutdown and
work ownership, OPEN #2250. The original retired-error symptom was an entry point,
not the boundary. No second normal coordinator interpreter was found. The target
remains entire chosen-class elimination, but supported-store integration has
exposed a separate PostgreSQL result boundary and is not qualified yet.

| Canonical owner / consumers | Systematic consumption and old path disposition |
| --- | --- |
| Coordinator.Start / Retire / finish | Moved to one completion path for initial abort and worker exit. Cancellation is installed with started before lease acquisition. Late leases settle before done. Old recordFailure and independent done/lease publication deleted. |
| Initial, wake, timer and Synchronize scans | All use scan; worker returns its exact result to finish. Empty success after observed retirement is refused. Both blanket context-error suppressions (worker and fatal dispatch) are deleted. |
| ordinaryCoordinatorStop | Private predicate in the existing owner. Every joined branch must be typed owner retirement or actual owning cancellation; independent store/dispatch/invariant/cleanup failure is retained. No string matching or reporter suppression. |
| AcceptCommitted / Retain / Acquire / observe | Already consume the same mutex fence, now return the one retirement sentinel. No new admission after the fence. |
| Exact carrier Resolve / return / Release | Already consume the existing continuation capability. Accepted carriers may return/drain after retirement; no duplicate consumption or new authority. |
| Runtime managed startup / synchronize / failure callback | Already consumes the normal coordinator. Callback remains fatal for genuine failure, not a second classifier. |
| Runtime.stopWithOptions / occurrence join | Already consumes Retire before route retirement and final occurrence join. Repeated retirement now returns the recorded failure. A canceled waiting caller does not undo retirement. |
| API selected-store fixture / EventBus dispatch and handoff | Already consumes real coordinator, selected store and reporter. Original pin-cursor test is preserved; no teardown assertion removed. API occurrence cleanup checks actual join. |
| Selected-fork execution | Different execution authority, no normal coordinator installed. Existing real dual-store diagnostic/fork lifetime controls retained. |
| runlifecycle.Executor | Different completion-candidate reservation/chain owner. No generic merge with continuation coordination. Its lifecycle behavior is not claimed globally closed. |
| SQLite runTransactionOnce / read and write wrappers | Existing #2439 exact-connection owner. After a successful callback, check cancellation before Commit; only that Commit's exact ErrTxDone racing owning cancellation maps to cancellation. Callback and real commit/cleanup errors remain intact. No coordinator-level sql.ErrTxDone exception. |
| Diagnostic fixture first, second and reopened handles | All now install the existing typed payload admitter. Force each independent handle to project an exact pending item, retaining real transactions and replay. Production diagnostic owners unchanged. |
| Internal H child and parent harness | Existing presenter supplies bounded startup/runtime/cleanup/shutdown causes and phases; a parent-requested signal captures bounded goroutine stacks before cleanup can hide them. No production signal handler or lifetime framework. Child ordinal/PID is recorded at every readiness wait. |

Authoritative spec changes: engine.runtime_core_persistence_store_contracts,
executable_event_delivery_obligation_rows.process_local_coordinator_lifetime and
the existing SQLite read/write cancellation/transaction-exit rules. Existing
diagnostic provenance, mode, payload admission and fork contracts are unchanged.

## Coordinator Manifestation Proof

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Retirement observed before cancellation becomes visible | reproduced and fixed | TestCoordinatorRetirementBeforeCancellationIsNotFatal holds real cancellation publication while scan hits the typed fence. |
| Start acquires lease after retirement | reproduced and fixed | TestCoordinatorRetirementDuringLeaseAcquisition, success and independent admission failure; no scan, no leaked lease, no successful Start. |
| Cancellation hides independent scan failure | reproduced and fixed | TestCoordinatorRetirementScanOutcomeMatrix: initial/wake/synchronize crossed with empty/deferred/canceled/failure/joined. Exact fatal identity and callback count retained. |
| Fatal dispatcher loses unrelated joined cause | reproduced and fixed | TestCoordinatorRetirementPreservesFatalDispatch with actual dispatcher barrier and joined cancellation/independent error. |
| Lease settlement error lost on startup abort | execution-proven through the same corrected path | TestCoordinatorStartupPreservesLeaseCleanupFailure preserves both primary and actual duplicate-settlement error, ActiveCount=0. |
| Before-start retirement, repeated retire and accepted carrier drain | execution-proven through the same corrected path | TestCoordinatorRetireBeforeStartAndDrainAcceptedCarrier plus existing authority, transfer, terminal and reclaim controls. |
| Exhaustion, exact synchronized failure and existing wake/timer paths | execution-proven through the same corrected path | Complete deliverycontinuation package, race count3 PASS 14.706s. |
| PostgreSQL canceled scan returns independent-looking connection errors | split / escalated as separate class | Original real API test, race count3 FAIL in PostgreSQL: context canceled + driver.ErrBadConn + two sql.ErrConnDone. Ordinary PostgreSQL transaction termination is outside #2439's approved SQLite boundary. No suppression or PG implementation. |

The API test passed both stores count10 without race (7.231s), then FAILED in its
race count3 run (16.911s). The later failure supersedes any interpretation of the
non-race pass as supported-surface closure. Exact failure log:
/tmp/agent-g-2441-r2-api-cleanup-race.log, SHA256
eddbd3b0165348a4f37aebadd7435b34c8f0cccf94a82b3719d46a895bc50f37.

## Separate #2432 And #2439 Proof Tables

| Issue / manifestation | Status | Exact proof |
| --- | --- | --- |
| #2432 second/reopened SQLite handle lacks typed admission | reproduced and fixed | TestLifecycleDiagnosticIndependentHandlesAndReopen: concurrent projection, forced first winner, forced second winner, lost response/reopen and forced reopened winner. Count3 PASS 3.298s; complete selected identity/fork/independent-handle race count3 PASS 137.599s on both stores. |
| #2432 fork provenance/identity regressions | execution-proven through the same corrected path | TestLifecycleDiagnosticForkLifetime and TestLifecycleDiagnosticSettlementIdentityAndModeOnBothStores in the same both-store race selection. |
| #2439 cancellation plus manufactured Commit ErrTxDone | reproduced and fixed | Strengthened TestTransactionCancellationRollbackOrders fails before correction count3 (1.255s); read/write, cancellation/deadline, in-progress/finished physical rollback, nil/callback-error matrix passes race count10 (6.575s). |
| #2439 independent callback TxDone and genuine commit failure during cancellation | execution-proven through the same corrected path | TestTransactionDoesNotSuppressCallbackTxDone and TestTransactionExitFaultDisposition/cancel_committed_error for pool1/3. Callback identity, real committed readback, no replay and actual injected commit error retained. |
| #2439 physical transaction exit / disposal / panic / busy controls | execution-proven through the same corrected path | TestTransaction*, TestReviewer*, TestCommitFailure* race count3 PASS 3.720s, including the final new cancellation negatives. |

The initial coordinator-corrected API count5 failed SQLite with context canceled
joined with ErrTxDone, before fd99831f2. That is retained in
/tmp/agent-g-2441-r2-api-cleanup.log, SHA256
c1ad78b3671bfd1785630c356a8e17b3a5e38b70c5132bed7df771aa7b0f74d5.
The deterministic pre-fix cancellation log is
/tmp/agent-g-2441-r2-cancellation-before.log, SHA256
29f7b054abf1bad960d2dd8ad8081298045f12fa2766891dc6a8cddbac1236d9.

## J5 Investigation And Proof Limits

LSF-032 remains **observed/unclassified**, not fixed. The completed c8556efb7
full-profile suite failed two tests: independent SQLite diagnostic admission and
J5 readiness with one active runtime lease. Full log
/tmp/agent-g-2441-ci-repair-full.log, SHA256
7fd8cfc34725d60b63609d62f935b6373293378b2bb264789d257f08b5cfd560.

Source inspection traced prepare/release abort through stopWithOptions, manager,
timers/schedules, coordinator, outbox, EventBus reset and final occurrence join.
Self-check subscription completion is deferred before that join. No evidence
identifies the historical lease owner, failing child or startup cause, and no
claim that coordinator retirement caused J5 is made.

Full-profile-selected isolated J5 count5 PASS 93.685s (both child identities logged).
Unchanged five-journey SQLite/PostgreSQL matrix count3 PASS 152.019s with its
existing two-child concurrency, assertions and deadlines. These are reproduction
attempts and regression controls, NOT causal closure or retry-to-green.
Logs /tmp/agent-g-2441-r2-j5.log (SHA256
5c2d177cd0f5013e6d8c322f5eeb6cc88369340c2e3959e566e6e4912c9304bf) and
/tmp/agent-g-2441-r2-lifecycle-matrix.log (SHA256
34216712be78ab8dd19507a5d36debb3df0e5c1f5d00a5dcbd6a2026f2b7e898).

New TestOwnedLifecycleStartupEvidence count3 PASS 0.011s verifies cause/phase/stack
retention and bounded errors. TestCompiledProcessLifecycleStartupEvidence PASS
13.299s exercises the real H child's signal via an intentionally canceled observer
after readiness, preserves cancellation and redaction, then verifies health and
clean stop. It does NOT simulate J5's missing production cause. Before handler
installation the parent records unavailable evidence without sending a potentially
destructive unhandled signal. Capture has a separate bounded two-second budget;
the readiness deadline remains unchanged. Stacks are capped at 1MiB.

## Qualification, Parent And Tracker Decision

API-spec PASS 1.540s, production async-site/work-boundary guards PASS 1.031s;
git diff --check PASS. Focused tests ran with regular go test as requested.
No new expensive full swarm-test was launched after the PostgreSQL scope boundary
was encountered. Prior c855 CI51-success/1-skip is historical, not final-head proof.

Achieved closure: bounded implementation and focused owner proofs, **not full
failure-class elimination or merge closure**. #2321/#2432/#2439 stay open.
LSF-030/031 are repair checkpoints pending review; LSF-032 remains unclassified.
The new PG observation requires a disposition in existing #2353/#2250 and a
bounded absorb/split ruling before PG code changes. No new issue is created.
Existing watchlist shutdown/runtime-lifecycle and harness refinements
9046e80/b19111f remain applicable; the PG manifestation is escalated to the
already-tracked backend lifetime parent, not silently promoted by G.

Architecture direction remains the existing coordinator plus backend-specific
transaction/connection owners, not another coordinator, retry or framework.
Estimated remaining work: one ordinary PostgreSQL cancellation/disposition proof
and repair slice if approved (medium confidence), plus J5 causal evidence or an
explicit residual-proof disposition (unknown effort). Broader #2250 remains open;
its total tail is not estimated from these local passes. No live Claude/Telegram
calls or settled-delivery replay occurred. No full-suite pass or re-review request.
