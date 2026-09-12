# Issue 2444: pipeline and session writer proof

Workspace: `/tmp/agent-g-2444-implementation`. Date: 2026-09-10 UTC.
The initial backend pass was frozen, then the parent reopened the bounded runtime consumer pass. That pass is in qualification below; this header is not a production freeze declaration. No commit was made.

## Gate and scope

Read the binding comment at https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272 with `gh api repos/division-sh/swarm/issues/comments/5623494272 --jq .body`, the consolidated removal census, and `issue-2444-three-contracts-gate.md` in `/tmp/agent-g-2444-gate/.github/audit-artifacts/`. The three-contracts gate supersedes the census timeout discussion.

This work uses the existing Backend transaction owner. It adds no timeout framework, transaction shim, provider-context detachment, or mutation replay. SQL, row consumption and revision finalization use the runner callback context. Caller/workflow contexts remain outside the admitted SQL unit. Default transaction options and fence/finalization ordering are preserved. The shared Kepler API is `RunTransactionWithOptionsOutcome`; these consumers need the default-options `RunTransactionOutcome` entry point.

Owned production changes:

- `internal/store/internal/backend/llmpersistence/{postgres.go,postgres_sessions.go,sqlite_sessions.go,owner.go}`. SQLite sessions and the owner helper were explicitly authorized later.
- `internal/store/internal/backend/agentpersistence/directive_operations.go`.
- `internal/store/internal/backend/channelonboarding/owner.go` and `operatorchannel/owner.go`.
- `internal/store/internal/backend/pipelinepersistence/{flow_instance_routes.go,owner.go,workflow_timer_activation.go,workflow_timer_occurrence_commit.go,generic_schedule_occurrence_commit.go,workflow_engine_mutation_commit.go,fan_out_owner.go,workflow_decision_route_commit.go,standing_service.go}`.
- `pipelinepersistence/owner_operations.go` was NOT edited by this worker. Other shared-tree changes belong to other agents.

## Exact consumers and results

### LLM sessions

PostgreSQL: `UpsertConversation`, `UpdateLiveSessionWatchdog`, `Acquire`, `AcquireLiveSession`, `Release`, `Rotate`, `IncrementTurn`, `AdoptSessionID`, `ResetAll`. Removed the now-unused `finalizePostgresRunForkRevisionTx` helper. Revision finalization now runs inside the Backend-owned transaction, not a helper that also commits.

SQLite F2 siblings: `Acquire`/`AcquireLiveSession`, `Release`, `Rotate`, `AdoptSessionID`, `ResetAll`, through the new `runRuntimeMutationOutcome` owner helper. Existing error-only mutation entry points remain available for callers without postcommit handoff.

Both stores return the lease/conversation, rotated successor lease, or reset summary after acknowledged COMMIT, independently of subsequent cleanup/handoff errors. Unacknowledged outcomes return zero results. Release and AdoptSessionID remain error-only APIs, but still attempt the live candidate handoff after acknowledged COMMIT plus cleanup error. Handoff failures are joined, not substituted for earlier errors. PostgreSQL rotation operation-ID readback remains inside the named operation; no transaction replay was added. SQLite rotation semantics other than outcome handling are unchanged.

Fermat was notified that a nonnil Rotate successor lease plus error is committed ownership that the runtime caller must consume/release, not discard or rotate again. His runtime/llm fixes/proofs are separate from this artifact.

### Other bounded PostgreSQL writers

- Routes: `UpsertFlowInstanceRoute`, `ReplaceFlowInstanceRouteRecords`, `DeleteFlowInstanceRoute`, `RollbackFlowInstanceRoute`. The active-run fence precedes mutation on the callback SQL context.
- Directives: `RenewDirectiveExecutionLease`; the active-run terminal-expiry DELETE reached by `ReconcileDirectiveOperation` and `ReconcileDirectiveOperations`. The DELETE retains terminal/expiry/active-run guards. `Deleted` increments only after acknowledged COMMIT, including when cleanup later fails; it remains zero for the unacknowledged transaction.
- Channel onboarding: `ReserveChannelOnboarding`, `AdvanceChannelOnboarding`, `PublishConnectedChannelActivation`, `RetireConnectedChannelActivation`, `ReserveChannelTeardown`, `RetireChannelTeardownAuthority`, `CompleteChannelTeardown`, plus sibling users `ReconcileChannelOnboardingBinding` and `ResetChannelOnboardingPendingIdentity` through their existing runner.
- Operator channel: `EnsureOperatorPrincipal`, `BeginChannelBinding`, `ConfirmChannelBinding`, `ExpireChannelBinding`, `UnbindOperatorChannel`, `BindOperatorChannelFromProof`, `CompleteProofResponsibility` through the existing runner.

Channel/operator runner callbacks already accept the scoped SQL context, including callbacks that shadow the name `ctx`. These migrations do not introduce an outcome-bearing API for those domain operations. Their pre-existing `result, err` failure shape is retained; a nonzero result there must not be interpreted as independent COMMIT evidence.

### Pipeline result-bearing siblings, both stores

`CommitWorkflowTimerReconciliation`, `CommitWorkflowTimerOccurrence`, `CommitGenericScheduleOccurrence`, `CommitWorkflowEngineMutation`, `CommitProposedEffectRoute`, `CommitHumanTaskDeferredRoute`, and `CommitHumanTaskOutcomeRoute` now consume `runPrivateAuthorActivityMutationOutcome`. The order remains author activity Begin, operation, author activity Finalize, revision Finalize, Backend COMMIT. Acknowledged results survive independent cleanup, validation, and handoff errors; unacknowledged outcomes return zero. Candidate handoff is attempted on acknowledgement, even if cleanup failed. Human-task route commits have no candidate handoff.

`ClaimFanOutIntent` returns intent/claim/found plus independent error only after acknowledged COMMIT; unacknowledged outcomes return zero/false. `CommitFanOutChunk` preserves acknowledged result directly, without requiring another read. The existing exact durable readback remains only for an unacknowledged, completed callback, without replay. Postcommit cleanup, successful-turn release and handoff errors accumulate in the existing `CommittedFanOutChunk.PostCommitFailure`, with nil second-return error: the runtime uses that second error for mutation retry. The new acknowledged-cleanup control supplies a nil readback and an independent finish error to prove both properties.

Standing-service PostgreSQL and SQLite adapters use `WithCandidateHandoffOutcome` plus the outcome-aware existing owner. Deleted the dead `withRunLifecycleCandidateHandoff` helper. The adapter tracks acknowledgement, zeros uncommitted results, and retains delivery-continuation evidence on acknowledged error. Consumers: `ReconcileStandingService`, `LoadReconciledStandingService`, `ReconcileStandingServiceSet`, `SuspendStandingService`, `ResumeStandingService`, `ResetStandingService`, `PublishStandingService`, and error-only `AdmitStandingServiceRun`. Publication sequence is not exposed on unacknowledged COMMIT.

## Focused controls

- New `runtimepersistence/writer_migration_cuts_test.go`: 21 PostgreSQL cases for LLM AcquireLiveSession, directive renew, directive terminal expiry, operator principal; healthy, entry cancellation, admitted-write cancellation, COMMIT-admitted cancellation, injected COMMIT-return uncertainty; LLM also postcommit handoff failure. Real SQL/COMMIT, durable rows, write/commit counts, no replay, result evidence, handoff count and successor connection use are checked.
- Extended `pipelinepersistence/fan_out_owner_test.go`: both stores, exact committed readback, rolled back, contradictory readback, acknowledged COMMIT plus cleanup error. The latter also joins a separate finish-turn error without readback.
- New `runtimepersistence/pipeline_writer_outcome_test.go`: real workflow-engine state/delivery transaction on both stores; healthy, postcommit sink failure, stale-claim rollback. Checks exact retained delivery claim, durable transition/delivery, handoff after persistence, canonical recovery candidate, and zero result/no handoff on refusal.
- New `runtimepersistence/sqlite_session_outcome_test.go`: ten real SQLite cases covering AcquireLiveSession, Rotate, Release, AdoptSessionID and ResetAll, each healthy and with a postcommit sink failure. Checks handoff count, conversation/lease/summary, durable provider ID, released lease, and committed successor relation/ownership.

Injected COMMIT-return uncertainty is a driver-probe observation cut, not actual socket ACK loss. Sink failures are postcommit control injections, not SIGKILL tests. This artifact does not claim peer-monitor, remote-possession, process-crash, or full-suite qualification. Nor does it claim every typed sibling has an individually injected cleanup-error test.

## Commands and receipts

All commands below ran in `/tmp/agent-g-2444-implementation`. Test paths below are relative to that directory.

```sh
go test -race ./internal/store/internal/backend/pipelinepersistence ./internal/store/internal/backend/llmpersistence ./internal/store/internal/backend/agentpersistence ./internal/store/internal/backend/channelonboarding ./internal/store/internal/backend/operatorchannel -count=1 -timeout=2m
```

PASS: pipelinepersistence 6.836s; agentpersistence 1.032s. llmpersistence, channelonboarding and operatorchannel report `[no test files]`: BUILD COVERAGE ONLY, not execution proof. Pipeline package includes the eight fan-out outcome/readback controls.

```sh
go test -race ./internal/store/internal/runtimepersistence -run 'Test(PostgresDirectiveOperation|SQLiteDirectiveOperation|ResetContinuationReevaluatesDirectiveExpiryBothStores|ForkedSourceDirectiveReservationTransitionsAndRecoveryRefuse)' -count=1 -timeout=3m
```

PASS: 54.032s.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresWriterMigrationCancellationAndCommitCuts$' -count=1 -timeout=3m -v
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresWriterMigrationCancellationAndCommitCuts$' -count=3 -timeout=4m
```

PASS: 34.777s (all 21 cases executed, no skips); repeat PASS 66.317s. An earlier repeat attempt encountered transient compilation errors in parent-owned eventpersistence during shared edits; the completed repeat above supersedes that attempt.

```sh
go test -race ./internal/store/internal/runtimepersistence -run 'Test(WorkflowEngineMutation|SQLiteWorkflowEngineMutationPreBusyAttemptUsesCallerContext|FanOutChunkCommitsMixedRealEventBusPlansAtomicallyOnBothStores|FanOutSelectedStoreOwnerParity)' -count=1 -timeout=3m -v
```

PASS: 35.372s. Executed fan-out selected-store parity, mixed real event-bus chunk commits, six new engine handoff/refusal controls, exact node-delivery settlement, payload fan-out plus delivery settlement, and SQLite pre-busy caller-context control.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestPostgresGenericSchedule(EmptyTerminalPreparationTransfersExactClaim|OccurrenceUsesDatabaseClockAcrossPrepareAndCommit)$' -count=1 -timeout=2m -v
```

PASS: 7.228s, including missing/malformed terminal preparation and real database-clock occurrence commit.

```sh
go test -race ./internal/store/internal/runtimepersistence -run 'Test(PostgresStore.*FlowInstanceRoute|SQLiteRuntimeStore.*FlowInstanceRoute|ChannelOnboardingSelectedStoreContractParity|OperatorChannelSelectedStoreContractParity|PostgresRegistry_Reset|ManagerStore_.*(LiveSession|LiveConversation|LoadActiveConversationIncludesRetryLineage)|PostgresLifecycleSessionMutation|RunForkRevisionSessionProjection|ForkedSourceSessionTurnAndConversationConsumersRefuse)' -count=1 -timeout=3m
```

PASS: 37.793s. This explicitly excludes the suspended-session sentinel test described below; it is not a passing receipt for that test.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^Test(SQLiteSessionMutationRetainsAcknowledgedHandoffOutcome|StandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity|SQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState|PostgresStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState)$' -count=1 -timeout=3m -v
```

FAIL: 33.338s. All ten SQLite session controls passed (24.32s), both standing-service operator lifecycle tests passed (2.52s SQLite, 1.01s PostgreSQL), and SQLite startup recovery passed. PostgreSQL startup-recovery cleanup failed at `standing_service_store_test.go:147`: `retire startup-order coordinator: context canceled`, joined with `driver: bad connection`. This command is not counted as a pass.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity/postgres$' -count=1 -timeout=1m -v
```

Isolated rerun PASS: 6.429s. This isolated pass did not resolve the intermittent failure. Kepler subsequently fixed its cause by draining admitted coordinator reads before retirement; the unchanged standing startup/retirement parity test passed under `-race -count=10` on both stores (28.982s). See [commit-evidence proof](issue-2444-commit-evidence-proof.md), "Real Dual-Store Repetition", for exact commands and deterministic read-drain controls. The historical FAIL above remains part of this receipt.

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^Test(SQLiteSessionMutationRetainsAcknowledgedHandoffOutcome|SQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState|PostgresStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState)$' -count=1 -timeout=2m
```

PASS: 35.469s. All ten SQLite session controls and both standing-service operator lifecycle tests executed in a passing command after production edits froze.

`git diff --check` over owned backend files passed. Files were formatted with `gofmt`.

## Failed/bounded attempts and caller handoff

A previous broad route/session/channel command used `Test(ChannelOnboarding|OperatorChannel|...)` prefixes and timed out at 300.066s while progressing through the large SQLite pending-reset lifecycle matrix. No passing receipt is claimed. It was replaced by the narrower contract-parity command above.

```sh
go test -race ./internal/store/internal/runtimepersistence -run 'Test(PostgresStore.*FlowInstanceRoute|SQLiteRuntimeStore.*FlowInstanceRoute|ChannelOnboarding|OperatorChannel|PostgresRegistry|ManagerStore_.*(LiveSession|LiveConversation|LoadActiveConversationIncludesRetryLineage)|PostgresLifecycleSessionMutation|RunForkRevisionSessionProjection|ForkedSourceSessionTurnAndConversationConsumersRefuse)' -count=1 -timeout=5m
```

The first narrowed command used `PostgresRegistry_` rather than `PostgresRegistry_Reset` and failed at 41.382s in `TestPostgresRegistry_AcquireFailsClosedOnSuspendedResumableOwner`, `postgres_store_additional_test.go:501`. That test compares `err != runtimesessions.ErrSessionSuspended`; Backend returned `errors.Join(ctx.Err(), operationErr)` even with nil cancellation, wrapping the sentinel. Kepler subsequently repaired sole-error identity in Backend; the unchanged suspended-registry control passed under `-race -count=3`. Exact before/after commands are in [commit-evidence proof](issue-2444-commit-evidence-proof.md). This worker did not modify Backend or weaken the assertion; the historical failure is preserved here.

```sh
go test -race ./internal/store/internal/runtimepersistence -run 'Test(PostgresStore.*FlowInstanceRoute|SQLiteRuntimeStore.*FlowInstanceRoute|ChannelOnboardingSelectedStoreContractParity|OperatorChannelSelectedStoreContractParity|PostgresRegistry_|ManagerStore_.*(LiveSession|LiveConversation|LoadActiveConversationIncludesRetryLineage)|PostgresLifecycleSessionMutation|RunForkRevisionSessionProjection|ForkedSourceSessionTurnAndConversationConsumersRefuse)' -count=1 -timeout=3m
```

Historical runtime caller census at initial review (the parent subsequently assigned the generic/timer/gate/engine consumers to this worker, fan-out claim to Schrodinger, and runtime LLM to Fermat):

- `runtime/genericschedule/owner.go:412` discards a retained result and returns `CommitRetry` on any error.
- `runtime/pipeline/workflow_timer_owner.go:371,814` takes error/retry paths before consuming retained reconciliation/occurrence evidence.
- `runtime/pipeline/workflow_gate_decision.go:135,488,492,598` releases publication plans on error before consuming retained proposed-effect, human-task or gate-engine publications.
- `runtime/pipeline/engine_adapter.go:273,420` preserves settled delivery and terminal deactivation evidence, but paths without those markers can discard other committed evidence on error.
- `runtime/pipeline/fan_out_pump.go:29` ignores a retained acknowledged claim on error, before installing release cleanup; that claim waits for expiry. Chunk commits instead consume the established `PostCommitFailure` field, which this migration preserves.
- `runtime/pipeline/workflow_gate_terminal.go:161` and `workflow_timer_lifecycle.go:144` already inspect retained terminal deactivation authority on error.
- Runtime LLM successor ownership handling is assigned to Fermat; lease-plus-error semantics were explicitly communicated.

These are a handoff census, not claims that parent-owned fixes are absent at the time of later suite qualification. Other agents can change those files after this worker's review. The full swarm suite is the parent's qualification responsibility.

## Runtime consumer addendum

The parent subsequently assigned this worker the bounded generic-schedule, timer,
decision-route, engine, coordinator and bus consumers. Schrodinger owns
`fan_out_pump.go`, its claim-cleanup controls, and `selected_fork_commit.go`;
Fermat owns runtime LLM rotation and manager receipts. Those files were not edited
by this worker in the runtime pass. Fermat's finalized
`Snapshot.MatchesSettlementClaim` is used by both coordinator settlement branches.

Exact runtime paths and dispositions:

- `genericschedule.Lifecycle.fire` preserves acknowledged typed occurrence outcomes and independent errors; `handleWakeup` only starts mutation recovery for `CommitRetry`. Committed/stale/terminal projections still retire or update the exact wakeup. Missing result plus nil error returns retry plus an explicit error and releases the prepared plan. Invalid acknowledged results release plans without mutation replay.
- `WorkflowTimerLifecycle.reconcileInitialDeclarations` consumes explicit reconciliation acknowledgement; `fireWakeup` distinguishes zero/no-ack from invalid acknowledged results, retains occurrence errors, finalizes and dispatches acknowledged publications. `handleWakeup` projects the committed recurrence even with an error, rather than entering mutation recovery.
- `handleProposedEffectDecisionCard`, `commitHumanTaskRoute`, deferred/expired/human-task handlers, `handleStageGateDecisionCard`, and `routeWorkflowGateDecision` propagate committed route evidence independently of error. Nil publication is not acknowledgement. Invalid acknowledged publication releases preparation without replay. Exact `StatusRouted` plus activation/card/decision-event/state evidence gates repeated gate execution.
- `pipelineEngineMutationOwner.CommitEngineMutation` and `commitEntitylessEngineMutation` reject missing explicit acknowledgement even with nil error. `Executor.Execute` retains committed emit/activity intents before claim validation, reports cleanup error without classifying the acknowledged persistence as rejected, and dispatches or returns those intents. Uncommitted persistence failures expose no actionable intents. `executeNodeContractHandler` and coordinator node dispatch retain outcome/emissions through their upper returns.
- Coordinator `SettleSuccess` and `SettleFailure` consume the shared exact snapshot/claim predicate and the operation-specific status. Settlement guard completion and terminal continuation release use acknowledgement, not `err == nil`. Failed retry settlements retain continuation ownership; success/dead-letter settlements release it. Independent settlement/release errors are retained.
- Bus `runInterceptorSet`, `runNodeDeliveryRouteInterceptors`, and `runInterceptorsForDeliveryRoutes` carry successful acknowledged work and errors, but clear aggregate acknowledgement on a later unrelated interceptor/projection/admission failure. Previously admitted emissions remain available. A later explicit retry/terminal disposition is not promoted to success.
- `completeCommittedPublishDispatch` drains remaining deferred work on every exit, preserving the original failure/disposition. `publishPersistedRecipientsWithScope`, `dispatchIntent`/`dispatchAndRecord`, and `processClaimedPipelineWork` retain postcommit-only errors while acknowledging completed dispatch, but do not acknowledge independent later failures. The outer sweep records settlement/retry ownership before checking the independent error, so cleanup still releases a returned retry claim.
- `DispatchDeliveryContinuation` drains exact staged interceptor publications before returning an error. Its existing closed `DispatchResult` cannot carry successful disposition plus error: it returns `Fatal(error)` after those handoffs, not silent success or a mutation replay request. `dispatchCommittedInterceptorPublications` attempts all staged children; transferred live-delivery incompleteness is suppressed only if every error leaf is that exact condition. Joined claim-release/receiver/interceptor failures remain errors.

No speculative no-claim retry framework was added. The served engine delivery
guard and exact routed gate evidence remain the replay barriers. The upper
executor controls below are runtime fixture tests, not a claim that they execute
the SQL backend. Real store engine/delivery transaction controls and real gate
coordinator execution are separately identified.

### New executed controls

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^TestGenericScheduleSchedulerConsumesCommittedErrorOnBothStores$' -count=1 -timeout=2m -v
go test -race ./internal/store/internal/runtimepersistence -run '^TestWorkflowTimerSchedulerConsumesCommittedErrorOnBothStores$' -count=1 -timeout=2m -v
go test -race ./internal/store/internal/runtimepersistence -run '^TestWorkflowGateConsumesCommittedErrorWithoutRouteReplayOnBothStores$' -count=1 -timeout=2m -v
```

PASS: generic scheduler **11.489s**, four cases; timer scheduler **13.007s**, four
cases; gate coordinator **11.805s**, four cases. Each runs SQLite and PostgreSQL,
healthy and injected error after the actual selected-store COMMIT. Generic
asserts one mutation, finalization/dispatch, terminal claim retirement and one
event. Timer executes the real scheduler callback, asserts recurrence reload,
one mutation/finalization/dispatch, reported error and one event. Gate executes
`coordinator.Intercept`, checks acknowledged outcome plus independent error,
exact routed activation/decision identity and one event; repeating the decision
does not call the mutation owner again. These are postcommit-error injections,
not transport loss or process-death simulations.

Fixture-development failures were not counted as passes: timer setup initially
lacked execution-mode authority; subsequent assertions used nonexistent event
columns before being corrected to schema-owned `event_name`. Gate setup initially
missed receiver binding and injected at the preparatory decision mutation rather
than only the route publication. The final commands above execute all cases.

```sh
go test -race ./internal/runtime/engine -run '^TestExecutorRetainsAcknowledgedIntentsWithIndependentError$' -count=1 -timeout=2m -v
go test -race ./internal/runtime/genericschedule ./internal/runtime/pipeline -run '^Test(LifecycleMissingOrInvalidCommitResultReleasesPlan|EntitylessEngineMissingAcknowledgementDoesNotFinalize)$' -count=1 -timeout=2m -v
go test -race ./internal/runtime/pipeline -run '^TestWorkflowTimerMissingOrInvalidCommitResultDoesNotDispatch$' -count=1 -timeout=2m -v
```

PASS: engine **1.045s**, six controls (acknowledged/uncommitted/malformed-claim,
immediate/deferred dispatch). An initial failure exposed proposed actionable
intents surviving an uncommitted persistence error; production now clears those
fields. Generic **1.025s** (two controls), entityless engine **1.024s** (one
control), timer **4.524s** (four controls on real DB-backed runtime fixtures,
with malformed owner responses injected). Missing result plus nil error never
becomes acknowledged dispatch; invalid acknowledged results do not request
mutation replay and release preparation.

```sh
go test -race ./internal/runtime/engine ./internal/runtime/pipeline ./internal/runtime/genericschedule -run 'Test(ExecutorRetainsAcknowledgedIntentsWithIndependentError|Executor_ActivityIntentPersistsBeforePostCommitDispatch|EntitylessEngineMissingAcknowledgementDoesNotFinalize|WorkflowTimerMissingOrInvalidCommitResultDoesNotDispatch|WorkflowTimerLifecycleOneShotExactCompletionOnBothStores|WorkflowGate|LifecycleMissingOrInvalidCommitResultReleasesPlan|LifecycleCommittedReplayDoesNotFinalizeOrDispatchAgain)$' -count=1 -timeout=3m
```

PASS: engine **1.048s**, pipeline **4.839s**, generic **1.028s**. Every package
executed tests. The anchored `WorkflowGate` alternative is not coverage of
prefixed gate tests; the separate real gate command above provides that receipt.

### Independent bus review receipt

Cicero extended and froze `interceptor_commit_outcome_test.go` after this worker's
initial six provenance controls. Parent-relayed exact receipt:

```sh
go test -race ./internal/runtime/bus -run '^TestInterceptorCommit' -count=3 -timeout=90s -v
```

PASS **1.062s**, **39 executions**, no skips or races. Local single-run receipt
also PASS **1.036s**. Named controls:
`TestInterceptorCommitForegroundDrainsBeforeTerminalExit`,
`TestInterceptorCommitContinuationRetainsErrorAndHandoff`,
`TestInterceptorCommitIncompleteDeliveryPreservesReleaseError`,
`TestInterceptorCommitFirstDeferredFailureStillDrainsSecond`, and
`TestInterceptorCommitErrorProvenance`. These test bus boundary fixtures, not SQL
COMMIT. They prove prior commit does not mask a later uncommitted failure or
deferred-admission failure, terminal/retry exit still drains the child, and
independent claim release errors survive delivery-incomplete classification.

Fermat's independent manager/shared-predicate receipt is recorded in
[manager receipt proof](issue-2444-manager-receipt-proof.md). The historical
standing retirement and sole-sentinel failures above are resolved by Kepler's
admitted-read drain and sole-error identity repairs, with repeated real-store
receipts in [commit-evidence proof](issue-2444-commit-evidence-proof.md).
