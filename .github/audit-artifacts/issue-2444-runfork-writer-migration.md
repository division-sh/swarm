# #2444 Selected Run-Fork Writer Migration Receipt

Scope: assigned `runforkpersistence` manual PostgreSQL writers in the shared
`/tmp/agent-g-2444-implementation` worktree. No commit. Conversation-fork files,
backend runner implementation, planner, driver removal and global qualification
remain with their respective owners.

Approval: https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272.
Read the three-contract gate and the older consolidated census; the historical
mandatory transport timeout is superseded and is not implemented here.

## Closed Writer Census

All 13 enumerated manual PostgreSQL writer transactions now use the existing
Backend runner. `RunTransactionWithOptionsOutcome(ctx, opts, fn) (bool, error)`
is the actual Kepler API. No global detachment or retry owner was introduced.

| Existing owner / consumer | Isolation and retained gates | Outcome handling |
| --- | --- | --- |
| `ActivateRunFork` | READ COMMITTED; schema and handoff reservation before BEGIN; lineage/frontier locks, source/fork state, replay admission, replay/fan-out writes, source freeze, author activity and revision finalization inside transaction | Acknowledged activation/source-freeze/replay fields are populated before postcommit handoff; cleanup and handoff errors remain joined. SQLite sibling uses its existing outcome runner too. |
| `MaterializeRunFork` | READ COMMITTED; standalone plan and selection admission before BEGIN; active source, bundle/scenario/fan-out/exact-repeat validation, pins, entity state, binding, author activity and revisions inside transaction | Return new materialization only after acknowledged COMMIT, retaining any cleanup error. Exact-existing verification settles through the same runner rather than an ignored deferred rollback. |
| Selected-contract activation port | READ COMMITTED; schema and handoff reservation remain before BEGIN; transaction-local planner, binding/frontier/route recovery, replay and source-advanced checks, freeze/divergence transitions and finalization unchanged | Port returns acknowledged bool separately from error; shared SQLite/PostgreSQL result owner preserves activation even if cleanup/handoff fails. |
| Selected-contract materialization port | READ COMMITTED; source lock, same-transaction planning, blockers, workflow readiness, identity/pins/profile, binding/routes and finalization unchanged | Port outcome distinguishes committed identities from uncommitted results, preserving pre-materialization blocker diagnostics. SQLite uses its existing outcome runner. |
| Selected-contract discard port | SERIALIZABLE; paused-state, dependent-lineage and retained-completion checks remain inside transaction; delivery terminalization, tombstone/deletion and revision ordering unchanged | Existing error-only API uses the error-only options runner; no callback retry or false success on uncertainty. |
| `LoadRunForkSelectedContractSourceEvents` | READ COMMITTED; source status, active fork, event ownership/order/projection, prepared-event writes, activity/revision finalization remain in transaction | It is a writer despite its name. Prepared results are returned only after acknowledged COMMIT, including committed-plus-cleanup-error. |
| `RecordRunForkSelectedContractRouteRecovery` | Default isolation; schema/normalization before BEGIN; active source/fork checks and insert inside | Returns acknowledged record separately from cleanup error. |
| Runtime execution issue | READ COMMITTED; exact non-mutating admission, active fork, durable binding lock, fingerprints/current generation checks and insert inside | Issued identity only after acknowledged COMMIT. |
| Runtime execution claim | Default isolation; active fork, exact prepared owner/lease/generation/fingerprint CAS inside | Authority only after acknowledged COMMIT, including committed-plus-error; no authority fabricated for lost acknowledgement. |
| Runtime execution heartbeat | Default isolation; current live effect authority and exactly-one update inside | Existing error-only API; runner owns result/cleanup and precommit refusal. |
| Runtime execution quiesce | Default isolation; current authority, no-live-attempts and exactly-one update inside | Same error-only ownership. |
| Runtime execution close | Default isolation; quiesced/failed state CAS and exactly-one update inside | Same error-only ownership. |
| Runtime execution fail | Default isolation; current authority, no-live-attempts, prepared/running fence-generation CAS inside | Same error-only ownership. |

Remaining manual BEGIN census, excluding conversation files: exactly two,
`PlanRunFork` and `EnsureRunForkNoPostForkCommittedReplayScopeMarkers`.
Both are unchanged REPEATABLE READ, read-only, caller-cancellable snapshots.
The legacy activation's standalone planner and historical-replay admission
callback explicitly keep the original caller context, not the SQL-drain context.
There is no remaining production `tx.Commit()` in this assigned package slice.

Removed the now test-only `commitRunForkAuthorActivityTransaction` and exported
wrapper. Its single source-freeze test now finalizes activity/revisions and
commits directly; the test alias is removed. Removed unused
`run_fork_revision_writer.go` (two private finalizers and two exported forwarding
methods, no callers). The non-committing `finalizeRunForkAuthorActivityTransaction`
remains with four real production callers. Updated only the corresponding
selected-activation census token in `run_fork_revision_writer_census_test.go`.
Generated persistence-authority inventory refresh remains part of parent-wide
qualification, not an edited historical census.

## Proof Receipts

New tests in `run_fork_writer_settlement_test.go`:

- `TestSelectedForkWriterPortsSettlement`: 21 PostgreSQL cases across the three
  selected ports: healthy, pre-BEGIN cancellation, cancellation during DML with
  result drain, independent callback error racing cancellation, panic, missing
  COMMIT acknowledgement and cancellation after actual COMMIT. Checks exact
  driver BEGIN/COMMIT/ROLLBACK counts, unchanged isolation, detached admitted
  SQL, durable observer state and no callback replay. This exercises the real
  ports and SQL settlement, not every semantic callback or process death.
- `TestSelectedForkClaimPreservesCommittedEvidence`: five cases using the real
  claim SQL/COMMIT with a minimal relational fixture: healthy, pre-BEGIN and
  pre-COMMIT refusal, missing acknowledgement, committed-plus-cleanup-error.
  Cleanup is injected at the existing result/outcome seam after a real COMMIT;
  it is not claimed as a physical connection-close fault. Uncertainty drops
  returned authority despite durable running state; cleanup failure preserves it.
- `TestSelectedForkActivationProjectsAcknowledgedOutcome`: two isolated typed
  projection controls, committed-plus-error versus uncertain. SQL is deliberately
  not executed here; it is not a durable activation/handoff recovery proof.

Passing command, race count=3, 26.145s, no skips (28 cases per repetition):

```sh
go test -race ./internal/store/internal/backend/runforkpersistence -run '^TestSelectedFork(WriterPortsSettlement|ClaimPreservesCommittedEvidence|ActivationProjectsAcknowledgedOutcome)$' -count=3 -timeout=2m
```

Passing supported behavioral matrix, race count=1, 148.441s, no skips:

```sh
go test -race ./internal/store/internal/runtimepersistence -run '^Test(SelectedFork|SelectedContract|MaterializeRunFork|ActivateRunFork|LoadRunFork|ForkedRunSelected|ForkedSourceCannotWriteSelected|RunForkSourceFreeze|RunForkActivation|RunForkMaterializ)' -count=1 -timeout=5m
```

This covers existing SQLite/PostgreSQL issuance/claim/finalization/recovery,
discard/rollback/dependency locking, materialization/activation/refusal,
prepared events and source-freeze tests. It does not claim every test in this
pattern has a dual-store variant or that the full suite ran.

After dead-helper deletion: assigned writer tests PASS again, race count=1,
9.008s. The paired source-freeze/census rerun could not compile while another
owner was changing `eventpersistence/event_commit.go` handoff signatures (lines
239, 340, 430 in that build). Earlier broad non-race selection failed only at
the stale `active_run_quiescence.go` writer-token guard outside this assignment;
the focused behavioral run above passed. The next post-deletion source-freeze/
census run compiled and completed in 27.457s: source-freeze tests had no failures,
with the only failure still the unrelated `active_run_quiescence.go` writer-token
guard. Final isolated post-deletion source-freeze/writer rerun PASS on both
packages, race count=1: writer package 10.965s, runtime persistence 27.689s:

```sh
go test -race ./internal/store/internal/backend/runforkpersistence ./internal/store/internal/runtimepersistence -run '^Test(SelectedFork(WriterPortsSettlement|ClaimPreservesCommittedEvidence|ActivationProjectsAcknowledgedOutcome)|RunForkSourceFreeze)' -count=1 -timeout=3m
```

`git diff --check` passed.

No shared database shutdown, provider call, global detachment, planner rewrite,
conversation-fork edit, uncertain-COMMIT callback replay, or commit was performed.
These are bounded writer migration receipts, not #2444 full qualification or
failure-class closure.

## Separate F2 Runtime Fan-Out Claim Consumer Receipt

Date: 2026-09-10 UTC. This later, separately assigned consumer repair is not part
of the 13-writer runfork census above. Read
`issue-2444-pipeline-session-writers-proof.md` before the repair: the existing
store returns `ClaimFanOutIntent` intent/claim/`found=true` only after acknowledged
COMMIT, independently of a subsequent cleanup error.

Repair scope: `internal/runtime/pipeline/fan_out_pump.go` and the new
`internal/runtime/pipeline/fan_out_claim_outcome_test.go` only. An acknowledged
claim plus error now invokes `ReleaseFanOutClaim` synchronously for that exact
claim using the existing `context.WithoutCancel(ctx)` cleanup pattern, then
returns `false` with the claim and release errors joined. No evaluation,
publication preparation, chunk commit, retryable release, or blocking is entered.
Unacknowledged/no-work results remain non-owning. Normal success handling and
chunk retry logic are unchanged. Faraday's other pipeline/generic consumers and
the frozen runfork production changes were not edited.

New exact test: `TestFanOutClaimErrorSettlesAcknowledgedOwnershipWithoutRetry`.
Six subtests (Go-normalized names):

- `acknowledged`
- `acknowledged_release_error`
- `acknowledged_caller_canceled`
- `acknowledged_canceled_release_error`
- `unacknowledged`
- `no_work`

Assertions cover exactly one claim attempt; exactly one release of the returned
claim before return for acknowledged cases, zero releases otherwise; retained
claim/release errors; caller context retained at claim admission; cleanup immune
to caller cancellation while retaining context values; `reenter=false`; and zero
evaluation, chunk, retryable-release, block, publication-preparation or recorded
publication calls.

Before the production fix, the following regression command failed in 0.010s:
all four acknowledged cases failed because release was skipped or its error was
absent. The unacknowledged and no-work controls did not fail.

```sh
go test ./internal/runtime/pipeline -run '^TestFanOutClaimErrorSettlesAcknowledgedOwnershipWithoutRetry$' -count=1 -timeout=2m
```

After the fix, the following command PASSed in 4.284s (Go-reported package
runtime, excluding compilation), with race detection and `-count=3`:

```sh
go test -race ./internal/runtime/pipeline -run '^Test(FanOut|SQLiteFanOutTriggerPersistsOneIntentWithoutEagerDeliveries|SQLiteNestedFanOutCreatesIndependentDurableIntentAndExactLineage)' -count=3 -timeout=3m
```

The selection contains six top-level tests, each repeated three times; the six
new claim controls therefore execute 18 times. Exact top-level names:

- `TestFanOutClaimErrorSettlesAcknowledgedOwnershipWithoutRetry`
- `TestFanOutCommitFailureAlgebraIsClosed`
- `TestFanOutBlockedTurnCausePreservesTypedFailuresAndNamesRawFailureStage`
- `TestFanOutPrecommitRetryReleasesPlansAndAdaptivelyReleasesClaim`
- `TestSQLiteFanOutTriggerPersistsOneIntentWithoutEagerDeliveries`
- `TestSQLiteNestedFanOutCreatesIndependentDurableIntentAndExactLineage`

Proof limits: the new regression uses an injected typed owner and recording bus,
not a real PostgreSQL COMMIT or physical cleanup fault. It proves the runtime
consumer invokes and completes the exact release call before returning, not
durable release when the store itself reports a release error. Store
acknowledgement semantics and durable mutation evidence are covered separately
by the pipeline-session writer receipt. No real provider is invoked; absence of
provider work is checked through the preceding evaluation/publication boundaries.
This is not a process-crash, lost-ACK, silent-monitor, full pump-loop scheduling,
or full-suite qualification. The command above was not verbose; no per-test
runtime or independent skip inventory is claimed. `gofmt` and scoped
`git diff --check` passed. No new production abstraction, global detachment,
provider retry, or commit was introduced. This receipt append makes no further
production changes and does not rerun the tests.

## Selected-Fork Runtime Caller Census and Repair

Date: 2026-09-10 UTC. This later assignment owns only
`internal/runtime/runforkexecution/{runtime_container.go,execution.go,activation_gate.go}`
and `runtime_outcome_test.go`. The store writers remain frozen. No fork model,
planner, restart recovery, new production abstraction, or commit was introduced.

### Exact Upper Callers

- `ExecuteSelectedContractRunFork` in `execution.go` calls the materializer,
  `buildSelectedContractForkLocalRuntimeContainer`, `container.Publish`,
  `container.Quiesce`, selected activation, and `container.Close`. Build/publish
  failures settle returned running authority through `container.Fail` before
  discard. The result now retains materialization, runtime proof, published-event
  evidence and any acknowledged activation even when later cleanup fails.
- `ActivateSelectedContractRunFork` in `activation_gate.go` has three branches:
  non-selected delegation to `Store.ActivateRunFork` at line 67; replay-ready
  selected execution through the same builder/container and selected activation
  at line 308; selected state-only `Store.ActivateRunFork` at line 338. Both
  direct branches already preserve activation-plus-error without retry. The
  replay branch now adopts returned build authority before failure cleanup and
  retains its runtime proof on error.
- `selected.RunFork.Execute` and `selected.RunFork.Activate` in
  `internal/store/selected/selected.go` supply the constructed execution owner
  and directly forward runtime result-plus-error unchanged. Both backends are
  supported: `newPostgresRunFork` and `newSQLiteRunFork` construct the same runtime
  family through `NewSelectedContractExecutionOwner`; SQLite has no refusal gate.
- `apiv1.SelectedContractRunForkExecutor.ExecuteRunFork` in
  `internal/apiv1/operator_run_fork.go:105` consumes the selected runtime result;
  its error branch returns a zero API result. `serveapp` wires its function to
  `selected.RunFork.Execute` in `store_capabilities.go:86`. This outer API loss is
  reported to parent, not edited or claimed closed here. No local retry loop was
  found in this adapter.

### Sibling Classification

| Store result / immediate runtime consumer | Classification and bounded disposition |
| --- | --- |
| `IssueRunForkSelectedContractRuntimeExecution` / builder, then both upper runtime callers | Verified loss of returned issuance on error. Builder now preserves execution ID, generation and fingerprints in its returned proof. An issuance error never calls Claim or Publish. Both callers use the existing discard path; real-store controls retain the prepared execution row with a cancelled run tombstone, not executable running authority. |
| `ClaimRunForkSelectedContractRuntimeExecution` / builder, then both upper runtime callers | Verified loss of acknowledged running authority on error. Builder now returns the exact authority even on error, including later local admission/scope failures. Both callers route a structurally valid returned token only to existing guarded Fail/Close cleanup, never to Publish. Cleanup errors are joined and suppress subsequent discard. |
| `MaterializeRunForkForSelectedContractExecution` / `ExecuteSelectedContractRunFork:181` | Already returns the materialization result on error and stops before runtime issuance. Durable paused-fork identity is not a live claimed execution. No materializer retry added. This classification is source tracing, not a newly injected materialization-error test. |
| `ActivateRunForkForSelectedContractExecution` / both upper runtime callers | Verified unnecessary discard attempt after `Activated=true` plus error. Both callers now close the runtime without discarding an acknowledged activation. The standalone caller also no longer zeros its result when activation succeeds and Close reports an error. One activation call, no reactivation. |
| `ActivateRunFork` / non-selected and selected state-only activation-gate branches | No defect verified: both already return activation plus independent error. Two exact direct-branch controls confirm retained evidence and one call; production branches unchanged. |
| `LoadRunForkSelectedContractSourceEvents` / `container.Publish:278`, then both upper runtime callers | Writer returning prepared-event data, not a live runtime claim. On error Publish stops before selected-event commit; both upper callers Fail/Close and discard. Dual-store injected prepared-write-error controls verify this existing route without replaying preparation. |
| `HeartbeatRunForkSelectedContractRuntimeExecution` / Publish heartbeat worker | Error-only mutation. Existing worker reports the error and cancels runtime execution; no returned acknowledgement marker or callback replay is inferred. |
| `QuiesceRunForkSelectedContractRuntimeExecution` / `container.Quiesce`, both upper runtime callers | Error-only mutation. Existing failure settlement remains; standalone error results now retain prior runtime/publication evidence. No inference that a quiesce error means committed or uncommitted. No newly injected quiesce-error control is claimed. |
| `FailRunForkSelectedContractRuntimeExecution`, `CloseRunForkSelectedContractRuntimeExecution` / `container.Fail` and `container.Close` | Existing current-authority and terminal-state guards are retained. Fail error stops Close/discard; Close error stops discard and preserves prior result. Returned errors do not authorize a retry. Cleanup uses existing uncancelled named-operation contexts. |
| `DiscardMaterializedSelectedContractExecutionFork` / `cleanupSelectedContractExecutionFailure` | Error-only bounded cleanup. Now uses `context.WithoutCancel(ctx)` so caller cancellation does not skip cleanup, and joins the typed discard error instead of formatting it with `%v`. No discard retry. |
| `RecordRunForkSelectedContractRouteRecovery` | No production invocation from this runtime package, and repository direct-call search found definitions/forwarders rather than an upper runtime writer call. This package reads persisted recovery through `LoadRunForkSelectedContractRouteRecovery` before execution. |
| `PlanRunFork`, `EnsureRunForkNoPostForkCommittedReplayScopeMarkers` | Pure snapshot consumers remain on the original cancellable caller context; no snapshot detachment. |
| `CommitSelectedForkEvent` / `container.Publish:417` | Separate unresolved store-contract boundary: `pipelinepersistence/selected_fork_commit.go` populates its result inside an error-only transaction on both stores, so a structurally valid value plus error is not independent COMMIT evidence. Runtime currently abandons prepared publication on error. No speculative adoption/replay change made; parent/store-owner decision required. |

`Authority.Valid()` is a structural check, NOT acknowledgement proof. Cleanup
still invokes the store's exact current-authority gate (running state, owner,
generation, fingerprints, fence and current lease); no runtime side effect is
authorized from a returned token on error. Prepared issuance cannot use Fail:
the current-authority gate requires `running`, despite the later failure UPDATE
also naming `prepared`. The repair deliberately does not claim a prepared
issuance just to manufacture cleanup authority.

Frozen-store limit: PostgreSQL issue/claim return values only after the runner's
acknowledged outcome. SQLite issue/claim in
`runforkpersistence/selected_contract_runtime_execution.go` still expose
callback-populated values through an error-only runner; SQLite prepared-event
loading has the same error-only shape. This receipt does not upgrade these values
to COMMIT evidence. These producer-contract gaps and the selected-event commit
gap remain parent/store-owner scope, not claimed closed by runtime testing.

### Runtime Proof

`TestSelectedForkRuntimeConsumersSettleReturnedOutcomes` executes 13 cases for
each combination of `postgres|sqlite` and `execute|activation_gate`: 52 real-store
cases. Both complete upper runtime functions are exercised, not only the builder
or a projection helper. Exact case names:

```text
healthy
issue_refused
issue_acknowledged_error
issue_discard_error
claim_refused
claim_acknowledged_error
claim_acknowledged_cancel
claim_fail_error
claim_close_error
prepared_events_error
activation_acknowledged_error
activation_and_close_errors
close_acknowledged_error
```

Checks include exact issue/claim/fail/close/activate/discard/preparation/event
commit call counts, exact authority passed to Fail, preserved errors and result
identity, zero downstream event work after admission/preparation error, durable
execution state and run status read after return, successful cleanup after caller
cancellation, and no discard after acknowledged activation or cleanup failure.
Healthy controls execute selected event publication and activation on both
stores. Refusal cases deliberately do not invoke the selected-store mutation;
they are not simulated lost-ACK cases.

`TestSelectedForkActivationGateRetainsDirectActivationOutcome` adds two isolated
typed controls: `non_selected` and `selected_state_only`. They are projection/call
count proof, not real-store activation fault injection. Total: 54 controls.

The first complete dual-store command PASSed in 137.262s, race count=1, no skips:

```sh
go test -race ./internal/runtime/runforkexecution -run '^TestSelectedFork(RuntimeConsumersSettleReturnedOutcomes|ActivationGateRetainsDirectActivationOutcome)$' -count=1 -timeout=5m -v
```

The probes were subsequently tightened to require a nil real-store error before
injecting postcommit errors for issuance, claim, preparation, activation and
close. The final rerun of that same command PASSed in 131.024s, race count=1,
with all 52 real-store cases and both direct-branch controls executed, no skips.
This keeps real acknowledged results distinct from merely populated callback
values. Fail/discard errors are separate pre-operation refusal injections;
close errors follow a real successful Close.

Existing targeted regression command PASSed in 15.989s, race count=1:

```sh
go test -race ./internal/runtime/runforkexecution -run '^Test(ActivateSelectedContractRunFork(DelegatesNonSelectedActivation|ConsumesAdmissionBeforeStateOnlyActivation|ExecutesReplayReadyContractSwapThroughSelectedRecipients)|ExecuteSelectedContractRunFork(WritesForkLocalExecutionAndLineage|CleansUpBeforeActivationOnPublishFailure|ProviderFailurePreservesEvidenceThroughCleanup)|SelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority)$' -count=1 -timeout=3m
```

Seven top-level tests cover existing direct gates, selected publication/lineage,
replay activation, publish/provider-failure cleanup and competing live containers.
Earlier fixture-building runs failed before their target cuts: the initial
PostgreSQL replay-gate fixture used an unsupported node-delivery replay
(55.891s command); SQLite setup first lacked normal delivery authority (8.216s),
then used an inconsistent singular route projection (6.685s). These are not
passing receipts. Fixtures were corrected, not production gates weakened.

Limits: fault injection is at the existing typed store-return boundary after
real successful operations, not physical socket ACK loss or connection cleanup
failure. New runtime controls use mock execution posture and do not call a real
provider. SQLite source setup uses its canonical entity/publication writers;
PostgreSQL setup additionally retains older source receipt/dead-letter facts.
No process crash, transport monitor, full-suite qualification, or resolution of
the outside-scope producer/API gaps is claimed. Production corrections are
unchanged since the first complete dual-store pass.

Final status: runtime production changes and focused tests are frozen for parent
qualification. `gofmt` and scoped `git diff --check` pass. No store production
files or other runtime packages were changed by this assignment. No commit.
