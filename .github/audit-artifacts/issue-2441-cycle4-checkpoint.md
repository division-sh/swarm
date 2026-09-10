# Cycle-4 local implementation checkpoint (not review-ready)

The cycle-4 approval remains in force. No further gate is requested. This is
partial implementation evidence, not a final Post-Implementation Proof Audit or
a failure-class closure claim. #2441 remains changes-needed.

## #2321 startup abort

Implemented the concrete circular-cleanup repair. PreparedStartup.Start consumes
execution release without joining children owned by its composing caller. Its
mutex protects only consumption, not the outer topology callback. Runtime.Start
retains synchronous failure/panic cleanup for standalone callers. Serve's existing
composition rollback closes admission on all prepared runtimes, invokes the
context manager's whole-set deactivation (withdraw, fence, retire children, join
parents), and closes prepared runtimes outside manager registration too. Normal
and reset startup share that rollback; reset no longer returns before disposing
unregistered prepared runtimes. The continuation-before-manager edge is intact.

Prepared-start caller census: production Runtime.Start and
prepareServeRuntimeContextSet; tests in runtime_startup_readiness_test.go and
workflow_timer_startup_recovery_test.go. The direct test callers already retain
explicit Shutdown/closeRuntime ownership, including failed release. No bypass or
new cleanup framework was introduced. Authoritative platform-spec.yaml and the
cycle-4 owner mapping accompany the change.

Proof executed:

| Manifestation | Execution evidence / limit |
| --- | --- |
| Lower abort waits on outer standing children | TestPreparedRuntimeAbortReturnsBeforeOuterStandingChildrenRetire: zero/one/three children, empty and accepted child work, canceled release, exact child ownership retained until outer cleanup, duplicate release refusal; race count3 PASS. |
| Real registered composition abort | TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores: ordinary and reset registered-release cases PASS SQLite/PostgreSQL; exact runtime leases settle and contexts cease being selectable. |
| Accepted standing ingress work | The same test's normal_active_release/reset_active_release cases acquire actual ingress work, require retirement cancellation, prove abort cannot return before Done, and verify every sibling is fenced before the join. Both stores PASS. |
| Partial normal registration | normal_second_preparation injects independent later-runtime admission refusal after the first candidate's preparation, requires the first real registration to exist, and checks all runtime leases and visibility after rollback. Both stores PASS. |
| Existing preparation/refusal/release controls | Existing reset second-preparation failure and successful execution cases retained. Runtime prepared-start, standalone cancellation, and continuation-before-manager shutdown race controls PASS count3. |
| Existing timer caller ownership | TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores PASS (3.843s), including publication abort and independent standing-finalization failure. |
| Actual compiled fresh startup | TestCompiledProcessLifecycleStartupEvidence PASS once (24.352s), then count5 (78.574s). These passes do not classify or fix the historical recovery refusal. |
| Evidence rendering | TestOwnedLifecycleStartupEvidence PASS. Startup failure renders named bounded string fields instead of numeric-padding struct precision. |
| Guards | Full apispec (1.495s), worklifetime (0.979s), persistence-authority registry (4.492s), and diff check PASS. |

Remaining startup proof includes the full first/middle/last multi-context failure
matrix and repaired-head J1-J5 qualification. Existing two-context proofs are not
claimed to cover those additional permutations automatically.
The final expanded composition selection (all seven cases, both stores) passed
with `-race -count=1` in 78.554s; its receipt is
/tmp/agent-g-2441-r4-composition-expanded.log.

## Recovery refusal: historical branch unclassified; timer ordering repaired

Added startup-only diagnostics at the four existing blocker producers: ingress
pause before scan/during dispatch, locally busy claim, run dispatch refusal, and
bounded retry. Busy-claim diagnostics include exact candidate event, opaque phase,
requested purpose and available competing local claim purpose/scan. Work-bearing
branches report event and run. These are observations, not eligibility decisions;
RecoverToExhaustion still fails closed. No recovery retry or ordering change was
made on an inference.

A temporary controlled creation-dispatch probe on the canonical Telegram fixture
did not reach its hypothesized creation-dispatch barrier; startup returned nil.
That probe was removed, its failed receipt retained at
/tmp/agent-g-2441-r4-creation-probe.log. It does not establish the historical cause.
The five compiled passes likewise do not establish a runtime recovery fix.

Focused RecoveryManager, standing owner, retry release, local run blockage and
corrupt-scope consumer tests PASS. The apispec/pipelinepersistence invocations in
that *filtered* command matched no tests and receive no credit; the separate full
apispec command above did execute tests.

### Subsequent bounded recovery correction

The unchanged H fixture has a standing initial-entry warmup timer due after 100ms.
`TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores`
uses real serve composition/registration/release and holds that timer at the
existing EventPersisted lifecycle probe, after its occurrence commit but before
publication settlement. With startup recovery enabled, the uncorrected code
FAILS on both stores: phase1 ordinary recovery encounters its exact local
publication claim and reports claim_busy; phase16 rejects readiness. The earlier
creation-only hypothesis did not exercise this timer edge. The exact event/run/
claim tuples and owner census are in the cycle-4 implementation mapping.

The bounded repair prepares the existing Scheduler before topology finalization,
retaining ordinary task records and leases while withholding callbacks. Runtime
releases those callbacks only after required recovery and delivery-continuation
synchronization, including when standing completion already registered an overdue
timer. Stop, cancellation and retirement join withheld work without releasing it.
Generic one-shot, every and cron wakeups, workflow timers, and re-bound projections
all enter startTask -> runOnce; recurrence remains a lifecycle-owned committed
successor registration, not an independent scheduler clock. No new producer,
pending-work registry, recovery retry, eligibility override or abort owner exists.

| Focused proof | Result and receipt |
| --- | --- |
| Real composed timer ordering before fix, SQLite/PostgreSQL | FAIL both, with exact local publication-owner evidence; `/tmp/agent-g-2441-recovery-timer-before-enabled.log` (SHA256 `8da04770350e67f82146c6951d795a88a71080cf211dde5a6b98e53d08f3a499`). |
| Same test after fix, isolated 1fb6cf159 + recovery changes | PASS both, 5.098s; `/tmp/agent-g-2441-recovery-timer-after-isolated.log`. |
| Same test against combined worktree, race count3 | PASS both stores all three repetitions, 58.342s; `/tmp/agent-g-2441-recovery-timer-combined-race-current.log`. |
| Blocked ingress-before/during-dispatch, scan-owned claim, run gate, bounded retry; async publication owner; standing owner and local-run fairness; scheduler stop/cancel/exact wakeup; RecoveryManager fail-closed controls | Isolated race count3 PASS: pipeline 2.520s, bus 92.138s; `/tmp/agent-g-2441-recovery-focused-race-isolated.log`. Each blocker preserves future recovery after exact release and cannot create a receipt/readiness. |
| Generic cron/every successor path; real generic one-shot/recurring both-store runtime; topology-withheld due schedules/timers and timer restoration without generic store | Combined tree PASS: pipeline 0.153s, runtime 9.650s; `/tmp/agent-g-2441-recovery-adjacent-combined.log`. Cron proof is scheduler projection/successor proof, not a new real-server cron occurrence test. |
| Final combined Blocked branch matrix, async publication claim, RecoveryManager and scheduler controls, race count1 | PASS: bus 20.525s, pipeline 1.228s; `/tmp/agent-g-2441-recovery-blockage-combined-race-final.log`. |
| Spec source-reference and timer-scope guards; diff check | PASS (apispec 0.110s). |

The isolated snapshot was needed while parallel native-PG edits were temporarily
uncompilable. Earlier test harness attempts left recovery disabled and correctly
failed the recovery-entrance assertion; they are not reproduction evidence.
Both harness receipts and native compilation failures remain in `/tmp` under
`agent-g-2441-recovery-*`. An intermediate combined branch-matrix rerun was blocked
by `pq.BindOperationScope` not yet being defined during another native edit; the
subsequent successful rerun is listed above. Passes qualify their built snapshot,
not later edits to the combined head.

This proves and repairs a concrete same-class standing timer ordering defect.
It does NOT identify the event or claim in historical compiled H or LSF-032, and
does not convert the five earlier compiled passes into fix evidence. There was no
new compiled H, full-profile, J1-J5, CI, live-provider call or settled-delivery replay
in this focused recovery pass. Async creation dispatch and the manager readiness
loop remain in the producer census; the H fixture's reproduced blocker was the
standing timer, not creation publication. Whole-class/PR closure remains withheld.

Recovery-owned file changes:

- `internal/runtime/pipeline/scheduler.go` and new `scheduler_startup_test.go`.
- `internal/runtime/runtime.go`: only the two scheduler prepare/release hook additions; all startup-abort changes belong to the concurrent owner.
- `internal/runtime/bus/sweeper.go` and new `startup_recovery_blockage_test.go`.
- `internal/runtime/pipeline/coordinator_recovery_test.go`; production RecoveryManager admission semantics are unchanged.
- `internal/store/internal/backend/pipelinepersistence/owner_operations.go`: owner diagnostics and candidate run projection only, no eligibility or native-PG change.
- New `internal/serveapp/startup_recovery_timer_order_test.go`.
- `platform-spec.yaml`: adjacent `startup_blockage_evidence` and `startup_timer_handoff` recovery subsections only.
- Recovery subsections of this checkpoint and `issue-2441-cycle4-implementation-mapping.md`.

## #2442: native repair outstanding

No PostgreSQL cancellation implementation is added in this checkpoint. Existing
transaction-exit fixes do not close native query cancellation. lib/pq's watcher
discards cancel-request errors and stores cancellation privately, while native
query errors can still escape as server 57014 and rollback exposes ErrBadConn.
The authorized native query/row/driver integration, exact linearization and
ambiguity contract, ordinary/retained tests and unchanged failing API leaves are
still required. No SQLSTATE/text/ctx.Err filter has been introduced.

#2432 and #2439 retain their separate previous implementation/proof records.
No new full-suite run, live-provider message, replay or review request was made.
No new issue/watchlist node is needed; approved refinement 5603178 and existing
#2353/#2250/#2412 tracking remain applicable.

## Receipts

- /tmp/agent-g-2441-r4-startup-evidence.log: 32fa4643872bf47357be28272cadbce853d0582f154359f74b64405b44dda07e
- /tmp/agent-g-2441-r4-abort-race.log: cd0c3e87484924be8910c65ae85eb96b68699292d19006a20c86f4b66903eae2
- /tmp/agent-g-2441-r4-runtime-final.log: f194dda1363956ca1644b69197530e398c756ff58f7822bd8ffa4bd55b692522
- /tmp/agent-g-2441-r4-composition-final.log: ffe3ef93c65127a0136b209664d0d674c3694a01040b6dad2315c322396bfc0a
- /tmp/agent-g-2441-r4-focused.log: 6b1d0376d604dcd6bb42d80bab63f7078a0f6e938912493f4e9b22260000551d
- /tmp/agent-g-2441-r4-creation-probe.log: f18bcbd79363d461be896e146c5664b9003734e43bf843015f73b1f58357d8d3
