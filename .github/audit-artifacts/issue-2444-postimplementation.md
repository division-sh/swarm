# Post-Implementation Proof Audit: #2444

Agent-g. Final qualification status: **PENDING**. This artifact is not merge
approval. The final PR comment binds the tested shipping head and actual suite
result. Component receipts below distinguish fault injection, public HTTP,
compiled lifecycle, real process death and wire-level proof.

## Binding Boundary

- Category: architecture/model debt; high-risk store/lifecycle semantic change.
- Approved pre-audit: issue comment 5623287325, published audit head
  `c226307ae73e25f20b7e2a9d7150cf7afc83436d`; copied historical gate/census documents
  accompany this artifact. The old proposed mandatory transport timeout and
  investigation freeze are superseded, not retained as product behavior.
- Independent implementation gate: https://github.com/division-sh/swarm/issues/2444#issuecomment-5623494272.
- Implementation baseline: `origin/master` at
  `0d7513f76bea234a7c153f488c7167847d1d8bd5`, after E's #2445.
  The pre-integration WIP is preserved at `48f73ad30`; its proof receipts are
  historical, not qualification of this merged-owner integration. See
  `issue-2444-merged-owner-integration.md` for the additional consumer census.
- Exact concepts: admitted SQL/result/cleanup lifetime, acknowledged COMMIT
  versus uncertainty, durable request/result association, reusable possession
  and irreversible local execution fencing, durable continuation after crash.
- Chosen class: application lifecycle/outcome policy coupled to the pq fork,
  including the enumerated pooled/retained consumers and F1/F2/local-fence defects.
  This PR aims to eliminate that chosen class entirely, not all store or
  idempotency architecture debt.
- Immediate parent: owned operation settlement and durable continuation across
  graceful stop, process death and database loss. Broader parents remain open:
  #2412 (transaction envelope), #2250 (orchestrator decomposition), #2159
  (fact propagation versus ambient cancellation). #2442 stays closed historical
  evidence. The original fork-removal question was not a complete consumer model;
  the approved census and amendments expanded it before closure.
- Intended closure: failure class eliminated, contingent on final qualification.
  No remaining same-concept writer may be excused as merely sharing a helper.

## Spec and Architecture

Authoritative `platform-spec.yaml` is updated in this PR at:

- `engine.runtime_core_persistence_store_contracts.backend_neutral_runtime_mutation_write_boundary.rules`:
  stock driver, named closed-operation drain, explicit cancellation before COMMIT,
  acknowledged outcome plus independent errors, uncertainty without replay,
  preserved SQLite caller/busy policy and ephemeral-read/pregrant exceptions.
- API idempotency contract: named `conversation.fork` creation and completion are
  atomic. Generic callbacks remain outside a transaction; method/actor/key/hash,
  TTL and intentional unkeyed distinct creation are preserved.
- `runtime_startup_authority_facts.possession_loss_contract` and startup authority
  grants: serialized proof budget controls local grants/readiness, not network
  cleanup completion or remote advisory release. Late success cannot revive
  authority; physical disposal fences before optional lineage readback.
- Shutdown/join/exit clauses: grace expiry is incomplete drain; retained work
  joins before dependency release. Cleanup failure cannot produce success exit.

No new durable table, ledger, outbox, driver patch, socket deadline wrapper,
global BeginTx detachment shim, retry system, migration or compatibility path.
Containers, live providers and original #2008 WIP are not modified by this work.

## Canonical Owners and Systematic Consumption

These are existing semantic owners, not the first local helper encountered.
The component receipts enumerate exact method/isolation/gate details. The raw
SQL and callback census was extended through result consumers, not stopped at
the transaction helper.

| Owner | Consumers and disposition | Invalid/removed interpretation |
| --- | --- | --- |
| PostgreSQL `Backend` transaction runner; SQLite `Backend` transaction runner | Moved named pooled writers: pipeline flow/directive; channel onboarding/operator channel; external authorization/response observation; LLM acquire/release/rotate/increment/adopt/watchdog; selected-fork writers including E's selected stop/recovery and preparation gates; reset/bootstrap/quiescence; routing/ingress/mailbox DML. Existing mutation owners continue delegating here. | Manual writer BEGIN/COMMIT, caller-cancellable SQL inside admitted PG mutation, ignored cleanup, callback success treated as COMMIT evidence. SQLite retains its own caller/busy policy. |
| PostgreSQL `SessionAuthority` / `AdvisoryLockLease` | Already canonical retained owner; pipeline eligibility/claimed-work reads and transaction callers now consistently consume owned drain/outcome. Generic schedule retained connection/key owner follows the same admitted-unit policy. | Custom pq native scopes, interruptible reused-session reads, local-close implies remote-release. |
| Exact startup process capability / generation grant | Background monitor, operation-error recheck, failed-release recheck, transition, retirement and terminal readback consume immediate irreversible fencing independently of SQL join. Serve watches the same capability to withdraw readiness. | Deadline checked only after SQL; grant mutex held across proof; late transition revives authority; readback before safety decision. |
| Named conversation-fork creation + existing API completion owner | API `conversation.fork` uses `CreateAPIConversationFork`; PG request lock precedes serializable BEGIN; both-store creation/completion commit together. Existing unkeyed lifecycle creation stays deliberately distinct. | Generic API callback followed by separate completion for this named operation. No list-query success inference. |
| Acknowledged transaction outcome + `CandidateHandoff` | Effect settlement, publication/inbound, delivery, run control/lifecycle, workflow/generic timer/fan-out/decision route, cards/supersession, standing reconciliation, both-store LLM mutation, selected fork all preserve result and error separately. | Error-only handoff helpers and callback-nil commit inference deleted. Uncertain commit never submits live work. |
| Runtime effect handle, lifecycle executor, LLM session owner | Existing handle keeps committed settlement; executor retains postcommit error without re-executing the candidate; session callers adopt/release the exact acknowledged successor alongside error. | Rewriting committed outcome as retry, dropping acknowledged lease, swallowing mandatory release errors. |
| Manager and node delivery settlement consumers | Both consume the existing settlement owner's exact acknowledged snapshot and shared `Snapshot.MatchesSettlementClaim`; they finish the claim and retain/release its continuation even when the owner also returns an error. | `err == nil` as settlement evidence; discarding a settled snapshot. Arbitrary readback is explicitly not acknowledgement evidence. |
| Serve composition and authority maintenance | Existing finalizer retains dependency lifetime and nonzero cleanup outcome; maintenance preserves acknowledged repair plus release error. | Printed cleanup failure with success exit; ignored possession release. |
| Delivery-continuation coordinator | Its admitted page/observation/standing-disposition SQL reads drain within the existing worker lifetime; logical retirement is checked before further work. Standalone store read APIs remain cancellable. | Retirement interrupts current scan and manufactures a cleanup failure; blanket filtering is not a repair. |
| Ephemeral projections / fresh pregrant acquisition | Different semantic concept: five operator read snapshots plus three runfork read snapshots (including E's original-source lookup) have explicit read-only options and no retained authority. Generic API arbitrary callback remains caller-scoped; fresh lock acquisition owns no grant yet. | No broad detachment exception for reusable authority or provider callbacks. These exact exceptions remain live intentionally. |

Deletion census: no production custom pq native API remains; no module Replace;
copied `third_party/pq` removed, as are its CI/native-scope test requirement and
raw-SQL/SCRAM exemptions. Historical investigation prose remains labelled history.
Selected-fork dead transaction/finalizer aliases and obsolete error-only handoff
wrappers are removed. Remaining direct BEGIN calls outside backend owners are
the eight named read-only snapshots, not writer bypasses.

## Manifestation Coverage

Each row names execution evidence, not only owner identity. Component receipts
state which branches use real SQL, seam injection or typed projection controls.
Final classification below is subject to the full qualification status above.

| Manifestation | Status | Exact proof |
| --- | --- | --- |
| Driver coupling/construction | reproduced and fixed | `TestOpenPostgresUsesStockDriver`; module v1.11.2 has no Replace; deleted source/API census |
| Graceful stop of admitted mutation and streaming result; concurrent refusal; cleanup exit | execution-proven through the same corrected path | `TestServePostgresGracefulContract`, real runFrom/listeners/lifetime with injected selected SQL leaves |
| Explicit cancellation before commit, admitted commit, uncertain commit, panic and unsafe disposal | execution-proven through the same corrected path | `TestPostgresTransaction*`, `TestRetainedPostgresTransactionExitMatrix`, SQLite transaction package and `TestCompletionTransactionAcknowledgementBoundaryBothStores` |
| Ephemeral streaming/prepared reads vs retained exact-session reuse | execution-proven through the same corrected path | `TestPostgresEphemeralStreamingCancellation`, `TestRetainedPostgresCancellationDrainsAndPreservesExactSession`, `TestPostgresQueryDeadlineAndRetainedDrain`, `TestAuthorityRead*` |
| Retained pipeline eligibility/claimed-work reads | reproduced and fixed | `TestPipelineGracefulClaimReadCancellation`, `TestPipelineGracefulParentFenceDrainsCanceledWait`, `TestPipelineGracefulWriteOutcome`, `TestPipelineGracefulCanceledAdmissionBothStores`; actual crash recovery below |
| Process dies before settlement commit | execution-proven through the same corrected path | `TestPipelineProcessSIGKILLRecovery`, actual SQLite/PostgreSQL child SIGKILL and successor EventBus recovery |
| Process dies after commit before acknowledgement/handoff | execution-proven through the same corrected path | Same test's after-commit child barrier: actual lifecycle continuation, no duplicate settlement |
| Database session/service loss and restart | execution-proven through the same corrected path | `TestServePostgresLossAndRestartFromDurableState`, private real PG termination/immediate stop, refused unavailable startup, same durable run/entity continuation |
| Public publication loses database COMMIT response | execution-proven through the same corrected path | `TestServePostgresLostCommitResponseRecoversDurablePublication`, TCP frame drop, caller error, startup delivery once and exact keyed replay |
| Remote advisory lock survives local disconnection | execution-proven through the same corrected path | `TestServePostgresRemotePossessionOutlivesLocalLoss`, real successor refusal until remote release |
| Silent monitor cannot fence until SQL returns | reproduced and fixed | `TestSessionMonitorFenceBeforeSilentProofDrain`, `TestSessionMonitorSilentSocketFencesBeforeJoinedRelease` |
| Proof budget falsely counts healthy admitted-work wait | execution-proven through the same corrected path | `TestSessionMonitorBudgetStartsAfterAdmittedTransaction` |
| Late transition/readback resurrects authority; error/release recheck waits before fencing | reproduced and fixed | `TestGenerationGrantLocalFenceDuringTransition`, `TestProcessCapabilityLateLineageOnlyEnrichesTerminal`, `TestProcessCapabilityRecheckFencesBeforeOperationJoin`, `TestTerminalFencePrecedesStalledReadbackAndJoins` |
| Silent proof composed readiness/dependency lifecycle | execution-proven through the same corrected path | `TestServePostgresSilentMonitorWithdrawsReadinessAndJoins`: real serve, readiness/grant withdrawn, proof/remote lock/dependencies still retained, late reply joins with exit 1 |
| F1 keyed fork committed before separate API completion | reproduced and fixed | `TestConversationForkCommitBeforeAPICompletionProbe`, both stores |
| F1 actual response loss and reconstructed handler replay | reproduced and fixed | `TestConversationForkLostHTTPResponseReplays`, real HTTP EOF after commit, one fork/completion and same returned ID; not SIGKILL |
| F1 concurrency/hash/actor/key/TTL/unkeyed/source/precommit controls | execution-proven through the same corrected path | `TestConversationForkAtomicCreationControls`, `TestConversationForkGracefulMutation`, existing lifecycle controls and OpenRPC mutating probes |
| F2 acknowledged settlement erased by failed live handoff | reproduced and fixed | `TestCompletionCommittedEvidenceAfterHandoffFailureProbe`, both stores, actual lifecycle executor consumes missing handoff and durably rearms exact candidate |
| F2 acknowledged cleanup vs lost ACK and no replay | reproduced and fixed | `TestCompletionTransactionAcknowledgementBoundaryBothStores`, `TestAuthorityTransactionOutcomeEvidence`, `TestCandidateHandoffOutcomeRetainsResultAndIndependentErrors` |
| Result-bearing publication/delivery/candidate siblings | execution-proven through the same corrected path | `TestResultSiblingsPreserveAcknowledgedHandoffOutcomeBothStores`; inbound/run-control parity controls named in commit-evidence receipt |
| Manager receipt drops acknowledged settlement or continuation on error | reproduced and fixed | `TestManagerReceiptOutcomeBothSelectedStores`, `TestWriteReceiptPreservesCommittedSettlementAndContinuation`, `TestProcessEventDoesNotResettleAcknowledgedReceiptError`, `TestSnapshotMatchesSettlementClaim`; exact selected-store settlement, handoff error and no-resettlement controls |
| EventBus early exits and continuation replay discard committed child handoffs or independent errors | execution-proven through the same corrected path | `TestInterceptorCommitForegroundDrainsBeforeTerminalExit`, `TestInterceptorCommitContinuationRetainsErrorAndHandoff`, `TestInterceptorCommitIncompleteDeliveryPreservesReleaseError`, `TestInterceptorCommitFirstDeferredFailureStillDrainsSecond`, `TestInterceptorCommitErrorProvenance`; 39 race case executions, fixture/injection proof, not a pre-fix execution or wire-loss claim |
| Lifecycle executor turns committed-plus-error into retry | reproduced and fixed | `TestExecutorCommittedErrorPreservesContinuationWithoutReplay` |
| Human/proposed decision completion and supersession | execution-proven through the same corrected path | `TestDecisionCompletionPreservesCommittedHandoffOutcome`, human/proposed lifecycle, winner/supersession and stage-card parity controls |
| Workflow/generic timer, fan-out, decision route and standing handoffs | execution-proven through the same corrected path | Pipeline/session writer receipt: exact migrated commit method -> acknowledged outcome -> handoff, real workflow mutation fault proof and adjacent dual-store suites |
| Pipeline/directive/channel/LLM closed writer cancellation | execution-proven through the same corrected path | `TestPostgresWriterMigrationCancellationAndCommitCuts` and exact consumer matrices in pipeline/session writer receipt |
| External authorize/direct/owned response observation | execution-proven through the same corrected path | `TestExternalEffectClosedMutationCancellation`, 12 cuts; existing dual-store lifecycle and recovery posture tests |
| Selected runfork manual writer and committed authority results | execution-proven through the same corrected path | `TestSelectedForkWriterPortsSettlement`, `TestSelectedForkClaimPreservesCommittedEvidence`, selected-fork/source-freeze/materialization/activation matrix |
| Reset/bootstrap/quiescence/routing/ingress/mailbox DML and cleanup | execution-proven through the same corrected path | `TestBoundedPostgresWritersCancelBeforeCommit`, `TestPostgresBootstrapOwnsSQLAndCommitOutcome`, quiescence/reset receipt and CRUD regression matrix |
| Runtime LLM committed lease/release error consumers | reproduced and fixed | `TestSessionRotationRetainsAcknowledgedLeaseAndErrors`, `TestSessionPreparationAndProvidersReleaseAcknowledgedAcquireOnError`, `TestSessionStartReleasesCommittedAcquireOnErrorOrCancellation`, `TestSessionCleanupErrorRetainsResponseAndDoesNotReplayProvider`, `TestPostgresRuntimeSessionRotationPreservesCommittedHandoffOutcome`; no live provider calls |
| Delivery-continuation read interrupted by graceful retirement | reproduced and fixed | `TestCoordinatorDrainsAdmittedReadsBeforeRetirement`, `TestCoordinatorSynchronizeWaiterCancellationKeepsReadLease`; `TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity` passes ten race repetitions on both stores after the reproduced PostgreSQL failure |
| Authority repair outcome/release consumer | reproduced and fixed | `TestRepairAuthorityOutcomeAndJoinedRelease`: both dialect owners, acknowledged repair plus release error, no-ACK result withholding, joined release barrier; driver/possession injection, not new wire proof |
| Broader generic transaction/API/startup architecture | split / escalated as separate class | #2412/#2250/#2159 remain open; no general API idempotency, exactly-once provider or prompt network-cleanup claim |

## Qualification and Tracking

Final whole suite must use `go run ./cmd/swarm-test -- ./... -count=1 -timeout=30m`.
An explicit `./...` is required when arguments are supplied. Private PostgreSQL
transport tests require `SWARM_INVESTIGATION_PG_BIN=/usr/lib/postgresql/16/bin`;
the final run supplies it rather than counting skipped cases as proof.

Component receipts: `issue-2444-commit-evidence-proof.md`,
`issue-2444-runfork-writer-migration.md`, `issue-2444-bounded-writers-proof.md`,
`issue-2444-decision-effect-proof.md`, `issue-2444-local-fencing-proof.log`,
`issue-2444-authority-maintenance-proof.md`, `issue-2444-manager-receipt-proof.md`,
and the consumer-census review plus
its final disposition. No exact live provider
or Telegram test is required for this store class; none was performed.

Watchlist decision: refine existing `atomic_runtime_state_mutation` and
`shutdown_and_runtime_lifecycle` nodes, retaining parent classes and explicit
network-silence/join limitations. Refinement `9020f65` is published on the docs
`agent-g/2444-investigation` branch, not claimed merged to docs master. No new
issue or POTENTIAL_ISSUES entry.

Architecture feedback: application outcomes and lifetime must remain above the
driver. This PR removes that coupling; broader transaction-envelope unification,
startup/lifecycle decomposition and ambient-context cleanup remain tracked in
#2412/#2250/#2159, not silently absorbed. Estimated parent tail: at least three
existing streams, roughly 3-6 bounded slices, low confidence; this PR does not
audit those parents to closure. Estimate is not reduced by a green child suite.
Likely effort is multiple PRs over weeks, with ROI in reducing repeated consumer
migrations and lifecycle ordering debt; no speculative framework is added now.

Closure feasibility: the chosen class is closeable in this PR by deleting the
driver fork and migrating all currently known in-class writers, readers and
outcome consumers. Fixing only the local transaction or fork-create endpoint
would have left live sibling interpreters; the census explicitly covered and
repaired them. Merge-readiness remains contingent on complete final-head proof
and independent review, not the presence of these owners or this document.
