# Post-Implementation Proof Audit: cycle 4

Agent-g; PR #2441. Qualified code/test head
a45d96acc3e0e96fef751279f0185756dbb7b500 over master b38b84045.
The final full-profile qualification passed. This audit supersedes the cycle-4
checkpoint and the cycle-3 closure status, while preserving their failed receipts.
Claimed closure: the four approved working failure classes are eliminated within
the named owner/consumer census. Independent merge review remains required.

Binding approval: PR comments 5611631358 and 5611646238. The pre-code owner,
consumer, failure and proof map is issue-2441-cycle4-implementation-mapping.md.
No new gate, issue, legacy path, retry or recovery framework was introduced.

## Concepts, closure and governing authority

The combined PR retains separate classes and proof credit:

- #2321: command-owned execution selection, structural admission and owned test
  lifetime, including the approved provider/tooling/catalog and composed startup
  corrections. Its original CLI symptom was narrower than the actual consumer
  class. The original 29-manifestation census remains in
  issue-2321-postimplementation.md; the cycle-4 additions are tabulated here.
- #2432: atomic lifecycle diagnostic projection/acknowledgement and immutable
  provenance, including both selected-fork validators and destructive cleanup.
- #2439: exact SQLite connection/transaction cleanup and cancellation ownership,
  including standalone snapshots and bootstrap.
- #2442: PostgreSQL ordinary and retained transaction outcome/resource ownership,
  extended by the explicit cycle-4 gate to native query/row/cancellation evidence.

The intended closure level is elimination of each approved working class, not
elimination of all lifecycle architecture debt or all historically observed test
failures. The commitment is supported by the final qualification and manifestation
proofs below, not inferred from sharing an owner. The broader lifecycle/startup parent remains open in
#2250; transaction-envelope architecture remains in #2412. #2319 remains F-owned
and is not an acceptance dependency of this PR under the recorded split.

Governing spec: platform-spec.yaml command execution/structural admission and
provider-state contracts cited by issue-2321-postimplementation.md;
engine.runtime_core_persistence_store_contracts.backend_neutral_runtime_mutation_write_boundary;
durable_pipeline_processing_obligation_authority startup_blockage_evidence and
startup_timer_handoff and startup_creation_handoff; existing startup and owned-work shutdown rules. Spec,
implementation and execution proof ship in the same PR.

## Owner and consumer census

| Canonical owner | Consumers and systematic disposition |
| --- | --- |
| Existing command selection/shared composition | Public mock test, live serve, verify/describe structural readers, native/exec and non-Claude controls already consume the corrected owner. Exact original census and proof remain in issue-2321-postimplementation.md; cycle 4 does not introduce another selector. |
| Runtime.Start | Standalone preparation/release owns synchronous abort on errors and panic. Nil/duplicate-start and independent preparation failures have explicit regression tests. |
| PreparedStartup / prepareServeRuntimeContextSet / RuntimeContextManager | Real semantic phase owners, not local helper aliases. Prepared calls return abort authority to composition. All candidates are observed/fenced before cleanup; registered standing children retire before parent joins, and unregistered prepared siblings are also closed. Production prepared callers are Runtime.Start and serve composition; direct readiness/workflow-timer test callers retain explicit cleanup. Normal, reset and reset-publication failure consume this same ordering. |
| RuntimeContextManager reset discard | Releases both each exact child occurrence and its parent lease through RetireAndWait. The bounded terminal Retire/Wait census found no remaining equivalent bypass. Shutdown ordering still stops delivery continuations before Manager. |
| Scheduler exact task lifetime | Runtime preparation withholds callbacks; completed recovery and continuation synchronization release them. Workflow initial/adopted timers, generic one-shot/every/cron, restored and rebound tasks all use startTask/runOnce. Their original task, lease, occurrence and due time remain authoritative. Cancellation/stop never releases withheld callbacks. |
| RecoveryManager / EventBus / selected-store pipeline claim owner | Existing eligibility and explicit-exhaustion decisions remain unchanged. Every Blocked producer is named and tested: ingress before/during dispatch, local/in-flight/external claim contention, run dispatch refusal, bounded retry. Exact phase/event/run/local claim diagnostics are observational, not readiness or replay authority. |
| Dynamic creation and readiness reconciliation | Authorized startup CurrentPending finalization consumes the existing processPrepared phase and a closed creation dispatch mode. Its normal creation commit uses existing queued-publication bookkeeping, without receiver execution or pipeline settlement; recovery acquires that exact event after topology finishes. Ordinary creation stays async. Completed plans do not re-emit. The readiness loop consumes CurrentPending only after the startup latch clears and is not a second independent premature producer. |
| Native pq operation gate and OperationScope | One exact native operation owns cancellation issuance versus ErrorResponse/terminal observation. Ordinary and retained callers attach/bind the record before Begin; Query hands it to Rows through final drain/Close, prepared execution gets its own gate, and Commit/Rollback retain settlement authority. Native/worker/physical-close failures survive database/sql error replacement. All native context query/exec entry points use the gate; original connection/statement watchers are removed. |
| postgres.Backend.runTransaction | Every existing ordinary read/write consumer listed in issue-2442-postimplementation.md continues through the private runner. Exact Raw binding survives disposal; unsafe rollback disposes before joining, safe automatic rollback is joined before pool release. No callback replay or pool reset. |
| SessionAuthority / RunAuthorityTransaction | Startup ownership, maintenance/reset and selected-fork commit retain the existing exact session and advisory lease. Transactions, standalone possession probes, Begin and terminal release consume operation-local native authority. The callback-wide pg_cancel_backend worker, cancelCurrentOperation, activeTxCancel and unused query port are removed. Healthy rollback/observation clears its binding before the next retained operation; unsafe cleanup fences the exact session. |
| Diagnostic projection/provenance and SQLite transaction owners | Unchanged semantic owners from #2432/#2439, requalified separately below. Transparent SQL test adapters now forward the native binding/reset/validity capabilities rather than changing query or payload admission behavior. |

No second production adoption, startup abort, scheduler occurrence, diagnostic
projection or transaction terminator was added. The pinned pq v1.11.2 source,
license, delta documentation and deterministic native tests live in third_party/pq.
SQL codecs/configuration/authentication and the existing database/sql pool are
retained. COPY is a distinct upstream streaming producer, absent from the
selected-store caller census; explicit selected scopes reject it before send.
Its dependency API is not an alternate selected-store transaction path.

## #2321 manifestation proof

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Lower prepared abort joins outer-owned standing children | reproduced and fixed | TestPreparedRuntimeAbortReturnsBeforeOuterStandingChildrenRetire: zero/one/three children, empty/accepted work, canceled/duplicate release; race count3. |
| Partial preparation/registration/release across context positions and reset | reproduced and fixed | TestServeStartupAbortFailureMatrixBothStores: 120/120 ordinary/reset x SQLite/PostgreSQL x first/middle/last and named failure entrances. Exact grants, descriptors, leases and visibility are asserted. |
| Reset publication discards a prepared child but retains its parent lease | reproduced and fixed | Reset-publication matrix 18/18 count3 and 6/6 race; exact reset discard uses RetireAndWait. |
| Standalone early failure/panic, duplicate and nil Start | execution-proven through the same corrected path | Named standalone/duplicate/nil/panic tests, race count3; original panic and existing running owner survive their respective cases. |
| Concurrent Start or PrepareStart loser aborts the winning owner | reproduced and fixed | TestRuntimeConcurrentStartDoesNotAbortWinningPreparation: both pre-claim barrier cases fail before, pass after reusing startupPrepareMu; targeted concurrency/startup controls race count10 PASS 1.199s. |
| Standing initial timer competes with startup recovery | reproduced and fixed | TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores: before fix both stores report phase1 claim_busy against the timer's local publication claim; after fix race count3 passes. |
| Interrupted activation creation competes with startup recovery | reproduced and fixed | TestComposedStartupCreationPublicationHandoffOnBothStores: real persisted activation with post-commit finalization omitted, then composed startup. Before fix both stores retain the exact creation publication claim at phase1 and fail phase16 (also race FAIL 12.917s). Final race matrix passes all 8 startup/ordinary-async/recovery-disabled/invalid-mode cases in 37.738s: exact event/delivery counts 1/1 before and after recovery, receipt/handoff counts 0/0 before and 1/1 after, with no creation receiver entered during readiness. Mode validation race PASS 1.026s; commit rollback controls pass on both stores. |
| Generic and workflow scheduler sibling paths | execution-proven through the same corrected path | TestSchedulerStartupRetainsExactWakeupAndSettlesWithheldWork; TestSchedulerStartupRejectsAlreadyRegisteredWork; TestSchedulerStartupReleasesCronAndEverySuccessorsThroughSameTaskPath; existing dual-store generic and workflow timer/topology controls. |
| Recovery refusal gates | execution-proven through the same corrected path | TestStartupRecoveryClassifiesBlockedBranchesOnBothStores, TestStartupRecoveryIdentifiesAsyncPublicationClaimOnBothStores and RecoveryManager tests preserve refusal and future authority after exact release. |
| Supported compiled fresh/restart and J1-J5 | execution-proven through the same corrected path | Full-profile swarm-test selection of TestCompiledProcessLifecycleStartupEvidence and TestCompiledProcessFullLifecycleJourneysSQLitePostgres passed initially in 66.989s; frozen final code head a45d96acc passed again in 58.815s. Both store postures retain distinct proof credit. |
| Historical H/J5 and PostgreSQL anomalies lacking exact evidence | split / escalated as separate class | #2353 retains individual LSF receipts. The new timer counterexample does not identify the absent historical event/claim; passing journeys do not retroactively classify those observations. |

## #2442 manifestation proof

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Real owner-canceled read/write query escapes as independent 57014 | reproduced and fixed | TestPostgresNativeQueryCancellation: notice-controlled real server execution, read/write x Exec/QueryRow/Rows/prepared Exec/prepared Rows. All ten failed before; all pass after. |
| Streaming rows and deadline ownership | execution-proven through the same corrected path | TestPostgresNativeStreamingCancellation (prepared/unprepared) and TestPostgresNativeQueryDeadline (ordinary/retained), with subsequent pool/possession progress. |
| Retained query cancellation and healthy-session preservation | execution-proven through the same corrected path | TestRetainedPostgresNativeCancellationPreservesExactSession proves the same pg_backend_pid and advisory possession after each canceled native surface. |
| Standalone monitoring leaves a canceled binding, failed-BEGIN cleanup loses Close evidence, or a canceled proof retains known-closed possession | reproduced and fixed | TestPostgresSessionScope* proves healthy monitor/unlock/competing acquisition, independent BEGIN/native Close evidence, all four closed-proof/caller-cancellation cases, exactly-once retirement, and pure owned cancellation retaining the same healthy PID; race count3 PASS 9.294s. The closed-proof matrix failed all four rows before its correction. |
| Native-invalid but open sql.Conn survives cancellation; proof disposition loses physical-close errors | reproduced and fixed | TestPostgresSessionScopeProofDisposesInvalidOpenConnection and TestPostgresSessionScopeProofDispositionPreservesCloseFailure: ProveCurrent/MonitorProveCurrent, real transport failure plus cancellation and drained server-error/missing-possession variants. All six fail before (agent tool receipt 17d86f). Corrected whole backend race PASS 27.604s; independently rerun session-scope race PASS 8.771s in /tmp/agent-g-2441-r4-final-session-scope.log. Exact disposal, original causes, next pool1 progress and retirement-after-unlock are asserted. |
| SQL-mock budget fixture cannot establish native operation authority | reproduced and fixed | TestPostgresStoreBudgetSpendPersistenceQueries now uses the real host store and verifies persisted spend fields/identity, active-only targets, flow lookup and totals. Both budget tests PASS 1.115s; no no-op binding or driver bypass is introduced. |
| Independent server/admin cancellation, timeout, server error and callback errors | execution-proven through the same corrected path | TestPostgresNativeCancellationPreservesIndependentFailures: both read/write stores of error identity; server cancellation and statement timeout remain 57014, callback 57014/BadConn and server-before-cancel errors survive. |
| Native Begin/send/Commit races, worker failure/join, row/panic/physical cleanup | execution-proven through the same corrected path | TestNativeScope* deterministic native gate suite, race count10; connection binding/reset and hidden Rows.Close evidence included. |
| Ordinary and retained panic/commit/rollback/admission exits | execution-proven through the same corrected path | Existing transaction exit matrix retained. Unsafe disposal precedes scope.Wait; real write readback, no callback replay, exact error/panic preservation and next operation are asserted. |
| Previously failing public API paths | reproduced and fixed | TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate and TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError, count3 PASS 21.221s initially; rerun after final native disposition repair count3 PASS 17.366s (/tmp/agent-g-2441-r4-final-api-leaves.log). |
| Public retained capability/claim/reset consumers | execution-proven through the same corrected path | Exact-session possession, process capability pool1, pipeline claim commit/rollback and destructive-reset lock tests, count3 PASS 2.356s. |

Native attribution is explicitly a local linearization result, not a claim that
PostgreSQL identifies the physical cause of wire-identical competing cancellations.
The server does not supply that identity. Independently observed failures and
uncertain commit/cleanup remain failures; no SQLSTATE/text/caller-context filter
is applied to arbitrary returned errors.

## #2432 and #2439 separate proof credit

| Class / manifestation | Status | Exact proof |
| --- | --- | --- |
| #2432 concurrent/reopened projections, failed acknowledgement, immutable attribution | reproduced and fixed | Existing TestLifecycleDiagnostic* matrix, including forced winners on both independent handles and reopened payload admission; full-profile selected suite PASS 31.379s. |
| #2432 fork provenance, both activation validators and destructive cleanup | reproduced and fixed | Existing fork diagnostic/cleanup proof families in issue-2432-postimplementation.md and selected lifecycle/fork diagnostic tests; final full suite passed, including runtimepersistence541.165s. |
| #2439 failed COMMIT, busy recovery, panic and snapshot cleanup | reproduced and fixed | Existing SQLite pinned transaction/read/bootstrap matrix in issue-2439-implementation-proof.md; final full suite passed, including SQLite backend15.794s, the unchanged owner and six migrated snapshot consumers. |
| #2439 canceled read ErrTxDone ordering | reproduced and fixed | Existing deterministic cancellation rollback barriers, both pending/completed physical rollback orders; no cancellation-priority suppression of independent failures. |

## Tracking, residuals and qualification

Watchlist decision: retain the approved existing atomic_runtime_state_mutation
and shutdown_and_runtime_lifecycle refinements (lead commit 5603178). No new node,
issue, potential-issues entry or broader refactor is warranted. The parent sibling
census and deletion results are in the versioned pre-code mapping; this work
absorbs the approved native-operation and composed-startup manifestations rather
than pretending the prior local endpoint fixes closed them.

Architecture feedback remains tracked by #2250/#2412. The long-run direction is
smaller explicit startup phases and narrower transaction envelopes, not another
registry or speculative hot replacement. Rough remaining tail: several distinct
startup/lifecycle and transaction-envelope slices, low-confidence multi-week
effort. This bounded patch does not recensus those entire parent issues or claim
a numeric tail of zero; eliminating their broader debt is medium/high ROI but
not a prerequisite to these exact owners' closure. Implementation broadened the
local proof obligations, not that parent estimate.

Failed evidence is retained: the original three-failure full run; the native
before-fix matrix; the intermediate scope-join-before-disposal deadlock; the
exact-context-identity integration regression; and test-budget timeouts in broad
race selections. The latter ended during fixture/schema processing without a
race warning or assertion failure, not in a reproduced runtime deadlock. The
complete non-race startup matrix and targeted race controls have separate credit.
No new live messages or replay of settled deliveries were made in cycle 4.
Previously recorded genuine live/restart proof remains explicitly historical,
as allowed by the cycle-4 ruling; mocked/compiled proof is not relabeled live.

The first cycle-4 full-profile run (runtime head 8473ee9e3) finished exit1.
Releasee2e789.580s, runtime130.487s, serveapp646.040s and both backend packages
passed. Failures were test integration: the composed timer test lacked an execution
unit in the committed catalog plan (affecting every catalog consumer), the new
pinned driver lacked exact SQL census entries, SCRAM Step calls collided with the
managed-turn lexical guard, and the budget-spend SQL mock could not provide native
operation authority. These are not ignored or counted as a green full suite.
Receipt /tmp/agent-g-2441-r4-full.log, SHA256
40ed0bdd59e017af07c848efd016b8d4cfb83713941ad44638dcf3245e73004a.
The immediately launched rerun was stopped before qualification after the full
failure list exposed the budget mock. It has no positive proof credit. Inventory
repairs have focused passing controls; the budget fixture now uses real PostgreSQL
execution, not a fabricated binding capability. Final registry/spec controls pass
(store26.936s, apispec1.505s); exact new native facts are private-backend entries,
not a widened inventory exclusion.

## Final Qualification

Frozen code/test head: a45d96acc3e0e96fef751279f0185756dbb7b500. The containing
audit commit changes documentation only. The exact command was:

```sh
SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...
```

Exit 0; all 164 test-bearing packages passed. Selected package receipts:
releasee2e800.396s; apiv1 197.608s; cliapp202.025s; runtime138.729s;
cataloge2e495.256s; conformance202.711s; serveapp667.615s; root store30.176s;
PostgreSQL backend26.422s; SQLite backend15.794s; runtimepersistence541.165s.
The earlier failed catalog consumers were actually executed in this passing run,
not credited from their previous inventory refusal. No test exclusion, runtime
readiness timeout widening, compatibility binding or cancellation filter was added.

Full receipt: /tmp/agent-g-2441-r4-qualified-full.log, SHA256
44192f255ce6041597fd8eaa9d6b3c21ceb4cb6a84556084fd67fec0433cfdf6.
The command waited for the shared runner before execution; queue time is not
runtime test time. CI for the containing commit is reported separately on the PR
thread and must pass before re-review is requested. This is not lead approval.

## Focused Receipt Hashes

| Receipt | SHA256 |
| --- | --- |
| Final API leaves, count3 | 07b72eb3e81162837a22e159ff230e845cd73a737b73770106d4e7604d9ccf13 |
| Final native session-scope race | 2470191c720f7e570a591f69925c0d1a5accaaa0e335180cd7495f394566fc5b |
| Creation startup before repair | 1b26df114f7c2708fcfa031efd2c681bbfc82bac5a824f0fccceac123e69ef07 |
| Creation full control matrix race | 9dfad6c86be5a38150e80996bce334634686348328a2e12e6f67b3c78ed96b02 |
| Final catalog/SQL/CI inventory controls | 5b66de8c847cbb40177dce219b98e428ff75cde8cba9a43e7333dc56ddd015f5 |
| Final root store/specification controls | c8bfe38ad37a9a55d44ac2e814bf90d541000013ccaacbcbdd8c99e5ac492de6 |
| Real PostgreSQL and unchanged SQLite budget proof | f6d567a463d3185705cb13d38682a06004c88c5b80f3e5e52f3fa2d475bf889f |
| Frozen-head compiled startup and J1-J5 | 09c2663e8725b593aea943a7ca1d0cb771ee4969176a389ed2abacaec3d36d0c |
