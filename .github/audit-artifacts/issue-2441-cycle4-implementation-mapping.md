# Cycle-4 authorized implementation mapping

## Cycle-5 additive boundary correction

Binding approval: PR #2441 comments 5619760258 and 5619769161; #2442's
updated body. No new class, gate or framework. The cycle-4 explicit-binding
census did not cover implicit native transaction authority on a retained raw
connection. Its closure claim is superseded pending this repair's qualification.

The native path is acquisition -> implicit Begin -> transaction statements ->
Commit/Rollback -> independent query/prepared work/next Begin/advisory cleanup.
The exact transaction, not the physical connection retaining it, owns implicit
cancellation. Failed/canceled Begin must never publish a connection binding.
Explicit selected-store binding instead owns physical cleanup through disposal
or healthy unbinding. Neither lifetime is replaced with pool reset or filtering.

| Owner / consumer | Disposition and proof |
| --- | --- |
| pq.startNative / txnScope / finishNativeTransaction | Same chosen class; retain implicit authority only in the live transaction, never operationScope. TestNativeScopeImplicitBeginLifetime covers commit, rollback, canceled admission, server refusal and interrupted transport. |
| Retained raw sql.Conn / conversation-fork advisory cleanup | Previously omitted same-concept consumer, moved to corrected native owner. TestImplicitTransactionScopeEndsAtSettlement covers both settlements, same-PID query, prepared query/exec and fresh transaction; TestImplicitTransactionScopeDoesNotBlockAdvisoryCleanup covers the exact retained connection/transaction/independent cleanup sequence. No claim that a complete public fork journey failed. |
| Successor transaction admitted before predecessor cancellation | Same owner, TestImplicitSuccessorScopeDoesNotInheritPredecessor proves execution and commit survive predecessor cancellation. |
| Explicit ordinary/SessionAuthority bindings and unsafe disposal | Already consume native owner, remain distinct longer cleanup lifetime. TestNativeScopeExplicitBindingSurvivesTransaction proves binding and physical-close error evidence after both settlements and failed/canceled Begin; existing real native/session backend race suite preserves healthy PID/possession and unsafe disposal. |
| Required CI static checks | Missing test consumer, moved to explicit root dependency invocation: go test -race github.com/lib/pq -run '^TestNativeScope' -count=1. TestCIRequiresPinnedNativeScopeProof guards unconditional non-optional execution. Root ./... remains separate proof; it does not discover the nested driver module. |

Old implicit writes into connection operationScope become invalid and are removed,
not cleared later by arbitrary reset/disposal. All other cycle-4 owner rows remain
applicable. Governing delta: storage backend_neutral_runtime_mutation_write_boundary
rules; pinned-driver SWARM_PATCH documents both lifetimes and separate execution.
Watchlist refinement is lead a5b5706, existing atomic-runtime-state and
invariant-suite nodes. #2250/#2412 retain broader debt; no new issue or potential
issue entry. Parent tail estimate unchanged. Complete chosen-class closure remains
the target; final-head integrated qualification and second high-risk review are
required before merge. No live sends or settled-delivery replay are authorized.

## Original cycle-4 mapping

Binding approval: PR #2441 comments 5611631358 and 5611646238. This additive
mapping precedes cycle-4 code changes. No new semantic gate is requested.
Current closure is incomplete; the previous full qualification failed.

## #2321 composition and recovery

| Owner / consumers | Required change and execution proof |
| --- | --- |
| Runtime.Start / PreparedStartup.Start | Standalone Start owns automatic failure cleanup; prepared release returns failure to its composing owner without joining outer-owned standing children. Migrate direct prepared callers in readiness and workflow timer tests; prove standalone cleanup, canceled/repeated release and independent failures. |
| prepareServeRuntimeContextSet / RuntimeContextManager.DeactivateAll | Withdraw/fence the entire registered set before any join; retire standing children before runtime parents; close prepared unregistered siblings for both ordinary and reset failure. Real SQLite/PostgreSQL composition tests after registration, zero/one/multiple children and first/middle/last context failure; no stale visibility. |
| reset complete-set publication | The same rollback covers failures before publication and after any execution release. Never leave prepared runtimes outside manager ownership unclosed. |
| PipelineCoordinator / EventBus sweeper / pipeline claim owner | Identify exact ingress pause, locally busy claim, run dispatch block or bounded retry at explicit-exhaustion refusal. Preserve phase/claim/event/run evidence using existing diagnostics; do not infer readiness, retry, or change eligibility. Absorb only the authorized bounded producer/handoff correction after reproduction. |
| test-only lifecycle evidence | Render bounded named string fields instead of precision-formatting a struct. Retain actual initial startup error and child stack. |

### Recovery evidence refinement

Before this focused change, the historical SQLite H receipt identifies only phase16
and Blocked. The existing five compiled passes and failed creation-only probe do
not identify its event or owner. Scope remains classification, then only a proven
ordering/eligibility repair; readiness failure is unchanged.

| Manifestation / canonical owner | Focused evidence obligation |
| --- | --- |
| Ingress pause / EventBus dispatch gate | Separate before-scan and during-dispatch refusal; no settlement or readiness. |
| Claim busy / selected-store pipeline owner | Candidate phase/event/run; local held claim/purpose/scan versus acquisition or external lock with unknown owner; exact release restores recovery. |
| Run dispatch block / EventBus run gate and standing owner | Preserve exact event/run/type and original refusal, including missing standing owner; no readiness or lost future authority. |
| Retry release / interceptor and EventBus sweep | Preserve typed reason/failure, retain only through bounded pass, release before return; readiness remains refused. |
| Async publication / EventBus publication claim | Controlled post-commit barrier proves a live publication can compete with a recovery scan on both stores; not proof that historical H took this branch. |

Producer census: creation occurrence calls DispatchPreparedPublishAsync; Manager.Run
starts readiness reconciliation, whose startup-pending latch clears at topology
completion before pipeline recovery; initial-entry reconciliation arms workflow
timers and adopted standing completion restores them before recovery. Ordinary
autonomous restoration and the outbox sweeper start after recovery. These are source
facts, not authorization to suppress work, wait for all runtime leases, or add a retry.
Diagnostic-direct lifecycle writes remain claim-free/non-executable under the
selected-store exclusion. #2442 and startup-abort edits remain separately owned.

Controlled reproduction now proves the standing-timer ordering defect on both
stores using the unchanged H standing_telegram fixture, real composed preparation,
registration and release, and a lifecycle probe holding the committed 100ms warmup
timer publication. At phase10's recovery entrance it is already publication-owned;
the phase1 ordinary scan reports claim_busy and phase16 fails. Receipt:
`/tmp/agent-g-2441-recovery-timer-before-enabled.log`. SQLite event
7b36930b-00eb-5c49-bd2f-1be009858052 holds publication claim
93459683-20fa-4f20-9448-4d5d5fa24cd5; PostgreSQL event
45469bea-9023-55be-8930-328f528c9782 holds publication claim
7893c8b7-deb2-4f39-99c6-02a79ee00028. Both use standing run
e91ce34b-95d6-5b88-b125-fde56ae0d207. The test ran in an isolated snapshot of
1fb6cf159 plus recovery changes because concurrent native-PG edits did not compile.
This is a newly proven same-class defect, not retroactive historical attribution.

Bounded correction: Runtime preparation withholds the existing Scheduler before
topology finalization, then releases its exact registered tasks after recovery and
continuation synchronization. Registration, persisted eligibility, occurrence IDs,
due times, leases and ordinary stop/join remain canonical. No blanket recovery
retry, waiting for publication quiescence, timer reset, additional registry, or
startup-abort change. Focused tests must prove late execution and canceled/stopped
withheld task settlement as well as the real composed both-store regression.

The final sibling probe also reproduces interrupted activation's `.created`
publication overlapping recovery on both stores, independently of timers.
`TestComposedStartupCreationPublicationHandoffOnBothStores` commits an exact
activation without its post-commit finalization, then executes real composed
startup and holds the creation receiver at PostCommitDispatchStarted. Both
stores report phase1 claim_busy and phase16 refusal on that exact creation ID.
Receipt: /tmp/agent-g-2441-startup-creation-handoff.log. This belongs to cycle-4
instruction C's explicitly named manager activation/event producer census, not
a new gate or a retroactive attribution of historical H.

The repair must hand that exact startup-created obligation to the existing
pipeline recovery owner after topology completion, without racing asynchronous
publication. Ordinary creation remains receiver-owned. Do not replace async
dispatch with a lower synchronous call that can reenter its own readiness
attempt, join all runtime work, add a queue/registry, or weaken Blocked admission.
The startup-only phase already requires pending-readiness recovery authorization;
recovery-disabled admission must remain unchanged.

## #2442 native cancellation boundary

Canonical connection construction and private driver integration must retain
operation-bound evidence before callbacks, rows and transaction owners lose it.
The implementation/spec must name its cancellation/completion linearization and
retain genuinely ambiguous competing outcomes. SQLSTATE/text, canceled caller
context, IsValid and ResetSession alone are not provenance.

| Consumer family | Required proof |
| --- | --- |
| Ordinary Exec, Query/QueryRow, streaming Next/Err/Close, prepared variants | Real owner cancellation/deadline plus server/admin cancellation, statement timeout, independent error-before-cancel and cancellation-before-completion controls; subsequent pool1 progress. |
| Begin/Commit/Rollback and ordinary read/write callback | Independent callback57014/BadConn and query/network/commit/rollback failures survive concurrent cancellation. No callback replay, uncertain commit suppression or blanket filtering. |
| Retained session and cancellation worker | Exact active-operation cancellation, worker failure/join, healthy advisory possession and unsafe exact physical disposal. |

No alternate pool, global error registry, second SQL interpreter, compatibility,
retry mechanism or detached cleanup is authorized. Existing #2432 diagnostic and
#2439 SQLite proof tables remain separate. #2250/#2412 retain broader debt.
Watchlist refinement 5603178 is already lead-owned and pushed; no new issue or
potential-issues entry. LSF-040..043 are tracked in #2353. Historical unclassified
failures are not retroactively explained.

Final retained-probe census adds native-invalid cancellation and post-probe
physical-close failures. The existing possession-check owner must retain its
operation scope and serialization until deciding whether to preserve or dispose
the exact session. Native validity can fence reuse, never prove cancellation
provenance. A canceled probe must not retain a poisoned connection; a drained
server failure or missing advisory possession must retain the independent close
error through disposal. ProveCurrent and MonitorProveCurrent need the same exact
negative proofs, plus healthy cancellation/next-possession controls. This is
within cycle-4 A's native/retained/physical-discard boundary.

Qualification: focused controls, failing API leaves, real composed and compiled
fresh/restart proofs, J1-J5, race/spec/registry guards, then one repaired-head
`SWARM_TEST_PROOF_PROFILE=full go run ./cmd/swarm-test -- -count=1 -timeout=30m ./...`.
No live messages or replay of settled deliveries.

## #2321 startup-abort implementation/proof handoff

The composition now owns every supplied candidate before preflight/catalog/lifecycle
preparation, including candidates that never register. Preparation and release unwind
through the same whole-set rollback on errors and panics. PrepareStart no longer
joins a parent implicitly; standalone Start owns its early failure cleanup as well.
Partial catalog registrations unwind on panic, and the preparation lifecycle lock
is released before outer abort can run. Duplicate standalone invocation preserves
already-started execution rather than acquiring its cleanup ownership.

`TestServeStartupAbortFailureMatrixBothStores` adds three-context first/middle/last
catalog, lifecycle, preparation, registration/publication, independent release,
cancellation, panic, and joined cleanup-error cases for normal/reset composition.
It checks exact registration/release position, fencing at grant retirement,
manager/parent lease settlement, grant/descriptor visibility, duplicate abort,
and subsequent selected-store possession. Post-repair qualification is recorded
below; earlier failed receipts remain preserved.

Original blocking manifestation: reset publication fails at the second candidate after
constructing the first standing child. `context_manager_reset.go:94-106` discards
that child with Retire plus Wait, not RetireAndWait, leaving its runtime parent
lease held. Outer rollback then joins forever. The lead explicitly extended write
ownership to this approved startup boundary; discard now calls RetireAndWait.
No workaround or timeout-as-success was added. Original receipt:
`/tmp/agent-g-startup-matrix.log` (controlled SIGQUIT after the diagnostic reported
one active lease); a separate initial matrix run also timed out at the same leaf.

Bounded named-caller census: RuntimeContextManager registration-failure cleanup,
newStandingOccurrencesLocked partial construction, prepared standing publication,
StandingServiceTransition.Retire, RetireStandingServiceOccurrence, and ordinary /
whole-set deactivation already finish parent ownership with RetireAndWait.
Runtime constructor-failure cleanup and stopWithOptions do likewise. Reset's
discardPrepared was the sole defective Retire/Wait pair in these startup callers.
StandingServiceTransition.Wait and WaitStandingServiceOccurrence intentionally
drain restorable fenced occurrences; serve transitions delegate to these owners.
No unrelated retirement sweep was performed. The composed matrix now asserts
zero process parent leases before duplicate-shutdown controls can mask a defect.

Focused receipts from this shared, changing worktree, not a frozen-head claim:
- Existing readiness/registered normal/reset abort controls: PASS (runtime 0.009s,
  serveapp 14.849s) before the later panic refinements.
- Standalone early/panic cleanup, including grant-evidence panic: PASS count3,
  0.010s (`/tmp/agent-g-startup-panic.log`).
- Compiled startup evidence and SQLite fresh/restart smoke: PASS 38.313s before
  the later panic refinements (`/tmp/agent-g-startup-compiled.log`).
- SQLite normal matrix reached all 30 leaves without assertion failure, then the
  PostgreSQL portion hit the concurrent driver's Exec/ExecContext recursion
  (`/tmp/agent-g-startup-normal-matrix.log`); not a complete matrix PASS.
- Later race/regression builds encountered the in-progress native driver boundary
  (`pq.NewOperationScope`, then `nativeOperation.observeResponse` undefined).
  Preserve `/tmp/agent-g-startup-*-race.log` and regression logs; rerun on the
  integrated head after the reset discard fix. No full suite or PG files changed
  by the startup-abort agent.

Post-repair focused execution (same shared worktree; no full-profile claim):

| Execution | Exact result / receipt |
| --- | --- |
| Reset publication first/middle/last, SQLite/PostgreSQL, count3 | PASS all 18 leaves; serveapp 31.523s. `/tmp/agent-g-startup-reset-publication-fixed-2.log`. |
| Reset publication first/middle/last, SQLite/PostgreSQL, race count1 | PASS all 6 leaves; serveapp 42.392s. `/tmp/agent-g-startup-reset-publication-focused-race.log`. |
| Complete startup-abort matrix, count1, plus existing registered/active-ingress and standalone readiness/panic/duplicate controls | PASS all 120 new matrix leaves (matrix 149.58s); runtime 0.015s, serveapp 169.729s. `/tmp/agent-g-startup-complete-fixed-2.log`. |
| Due timer/topology withholding, continuation-before-manager drain, lifecycle-executor retirement, grant-after-persistence settlement, atomic dynamic-topology preflight | PASS runtime 4.979s, serveapp 2.657s. `/tmp/agent-g-startup-regression-fixed.log`. |
| Compiled startup evidence and SQLite full-lifecycle fresh/restart smoke | PASS 52.161s. `/tmp/agent-g-startup-compiled-fixed.log`. |
| Nil receiver, duplicate Start, and standalone early/panic cleanup, race count3 | PASS 1.043s. `/tmp/agent-g-startup-nil-and-panic-race.log`. `stopWithOptions` guards nil before dereferencing; the new defer does not introduce a nil-receiver panic. |
| Complete startup-abort matrix plus existing controls, race count1 | Runtime PASS 1.126s; serveapp FAIL 600.135s at the command's 10-minute budget, while `postgres/reset=false/registration/context=1` was 3s into provider-manifest parsing during fixture construction. No race warning or parent-join stack; this is NOT a full race PASS. Preserve `/tmp/agent-g-startup-complete-fixed-race.log`. No startup/cleanup timeout was changed. |

The first post-fix attempts encountered the concurrent driver's removed watchCancel
reference before tests ran. Preserve `/tmp/agent-g-startup-reset-publication-fixed.log`
and `/tmp/agent-g-startup-complete-fixed.log`; the `-2` receipts above are the actual
successful executions after that integration completed. The only reset production
change is discardPrepared's terminal RetireAndWait; the matrix additionally checks
parent leases before duplicate shutdown. No commit or push by this agent.

### Exact startup test rows

Each matrix row expands to
`TestServeStartupAbortFailureMatrixBothStores/{sqlite,postgres}/reset={false,true}/<phase>/context={0,1,2}`.
All 12 leaves per row passed in the complete count1 run. The registration row's
reset=true subset also passed count3 (18 executions across both stores).
That exact six-leaf reset subset additionally passed with race instrumentation.

| Exact phase | Failure boundary / required observation | Leaves |
| --- | --- | --- |
| `catalog` | Conflicting catalog before lifecycle preparation; all candidate grants released | 12 PASS |
| `lifecycle` | Independent grant Evidence error during lifecycle preparation | 12 PASS |
| `preparation` | System node cannot report readiness; exact preceding registrations observed | 12 PASS |
| `preparation_cancel` | Caller canceled at selected context preparation; preceding registrations retired | 12 PASS |
| `registration` | Invalid pack digest at normal Register or reset complete-set publication; no discarded-child parent lease | 12 PASS |
| `release` | Independent MarkProbesSettled failure after real complete-set registration | 12 PASS |
| `release_cancel` | Selected release cancels its owning context; exact prior releases completed | 12 PASS |
| `prepare_panic` | Selected preparation progress callback panics; original panic and cleanup preserved | 12 PASS |
| `release_panic` | Selected release progress callback panics; original panic and cleanup preserved | 12 PASS |
| `release_cleanup_error` | Independent release cause plus every grant's cleanup error retained; later cleanup continues | 12 PASS |

All matrix leaves assert zero runtime and process-parent leases, retired grants,
released event descriptors, no loaded visibility, fencing before grant retirement,
and successor selected-store possession. Duplicate abort/stop cannot reopen the set.

| Exact additional test name | Execution / result |
| --- | --- |
| `TestRuntimeStartWaitsForSystemNodeSubscriptionReadiness` | complete fixed count1 PASS |
| `TestPreparedRuntimeStartupKeepsFirstCandidateUnadmittedWhileSecondBlocksOrFails` | complete fixed count1 PASS |
| `TestPreparedRuntimeStartupRejectsCancellationBeforeExecutionRelease` | complete fixed count1 PASS |
| `TestPreparedRuntimeAbortReturnsBeforeOuterStandingChildrenRetire` | complete fixed count1 PASS; zero/one/three children, empty/active |
| `TestRuntimeStartFailsClosedWhenSystemNodeSubscriptionReadinessIsCanceled` | complete fixed count1 PASS |
| `TestRuntimeStartRejectsSystemNodeWithoutSubscriptionReadiness` | complete fixed count1 PASS |
| `TestRuntimeDuplicateStartDoesNotAbortExistingStartup` | complete fixed count1 and dedicated race count3 PASS |
| `TestRuntimeStartOwnsEarlyFailureAndPanicCleanup` | complete fixed count1 and dedicated race count3 PASS; catalog error/panic, retired preparation, grant/preparation/release panic, independent release error |
| `TestRuntimeStartNilReceiverReturnsError` | dedicated race count3 PASS |
| `TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores` | complete fixed count1 PASS, all 14 leaves including active ingress |
| `TestResetStandingPreparationFailureDoesNotRetainPrecedingOccurrence` | complete fixed race count1 runtime package PASS |
| `TestRecoveredRuntimeContextsStayFencedUntilExactPublicationRelease` | complete fixed race count1 runtime package PASS |
| `TestResetRuntimeContextsRequireRetirementAndPublishWholeIdenticalSet` | complete fixed race count1 runtime package PASS |
| `TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores` | regression fixed count1 PASS, both stores |
| `TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress` | regression fixed count1 PASS |
| `TestRuntimeShutdownBoundsLifecycleExecutorRetirementByGrace` | regression fixed count1 PASS |
| `TestRuntimeShutdownRetiresGrantAfterCompletionPersistenceSettles` | regression fixed count1 PASS |
| `TestDynamicTopologyStartupPreflightPostgresScopesTwoContextsAndRefusesAtomically` | regression fixed count1 PASS |
| `TestCompiledProcessLifecycleStartupEvidence` | compiled fixed count1 PASS |
| `TestCompiledProcessFullLifecycleSQLiteSmoke` | compiled fixed count1 PASS |
