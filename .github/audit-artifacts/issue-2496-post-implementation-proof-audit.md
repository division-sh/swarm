# Post-Implementation Proof Audit: #2496 / #2525

Qualification capture: production/test source885737eeb on master9a1f27fbd.
Default managed qualification, the remaining supplements and exact-head CI are
pending. This draft does not supersede the independent changes-needed verdict
or claim review/merge readiness. The final PR proof-audit comment records later
execution receipts and is authoritative for qualification status at review.
Earlier diagnostic REDs remain recorded in
issue-2496-wip2-repair.md; a passing correction is not a waiver of those results.

## Class, Contract And Closure

Category: architecture/semantic drift, runtime lifecycle and supported-surface
parity. Concepts: immutable construction and creating-input evidence, exact
attempt-scoped attachment, constructed-instance presence, constructor eligibility
and projection, and publication/retirement ownership. Working class:
flow_construction_and_attachment_authority_drift. The issue was already broader
than its readiness symptom; all approved keyed/keyless and header consumers are
included, not just the originally failing Testhelper. Immediate parent2411 and
architecture parent2250 remain open. This PR aims to eliminate its chosen class
entirely; final closure remains subject to the qualification receipts below and
independent review, not shared-owner or cleaner-architecture reasoning alone.

Binding references: original audit5915862768; additive audit5916581924;
replacement Gate5916752092; exact predecessor retry5915334928; WIP1 repair
5944833985; WIP2 disposition5947674701; discard consumer repair5950195540.
Authoritative platform-spec.yaml is changed in this PR at engine.flow_constructor,
engine.dynamic_instance_lifecycle.creation.process, platform_tables.tables
flow_instances/flow_instance_runtime_readiness, routing_derivation
route_plan_authority, join.stage_entry_identity, run_model.fork
selected_contract_readiness_classifier, standing admission/tree/restart and
destructive discard, and mailbox.card_superseded. Historical six-phase wording
is superseded: five forward attachment phases and a separate emission receipt.

## Canonical Owners

| Owner | Real semantic authority and touched consumers |
| --- | --- |
| O1 | Selected-store admission resolution and exact-attempt conditional phase advancement. Manager activation/Ensure/retry/startup, selected staging, agent/route/timer attachment and diagnosis consume its typed receipts. The process caches installed resources; it cannot decide persisted readiness. |
| O2 | Canonical constructor and named atomic construction commit; strict constructed-header/iff-declared-field projections. Root/keyed/recursive keyless activation, publication, standing, presence, ordinary mutation, snapshot materialization, fork activation/replay and discard consume it. Immutable construction is not an attachment attempt. |
| O3 | Creating-delivery obligation and optional creation-event append/completion receipt in existing publication commits. Bus publication, delivery claim/dependency, post-commit finalization, restart and retry consume it without replaying an already-settled handler. |
| O4 | A's prepared initial lifecycle/StageEntry/member/arm/timer contributions. O2 persists the returned Instance/Lifecycle together; attachment and retirement consume those facts without recreating them. PrepareInitialEntryLifecycle and FlowInstanceActivationPlan signatures are unchanged. |
| O5 | Existing exact Manager lifecycle, physical agent/route/timer resources, accepted-work leases, source/grant ownership and joined retirement. Failure/unknown acknowledgment retains the exact owner; only acknowledged predecessor abandonment admits a successor. No detached cleanup or second generation authority. |
| O6 | Compiled per-input constructor contract using the existing assignment/type checker. Loader/verify, describe/authoring projection, public publication admission, Manager preparation and runtime InitialFields consume the same facts. Closed2397 is substrate, not a deferred owner. |

Full supported flow: decode/verify O6 -> artifact persistence/admission -> exact
run/generation and source grant -> O2 constructor/preflight -> atomic tree,
fields, A lifecycle and creating evidence -> O1 admission responsibility retained
before Begin -> agents_registered -> route_installed -> timers_armed -> ready
-> O3 creating/optional emission settlement -> ordinary delivery -> terminal
header/readback -> fence/disposition, release dependencies, join resources and
acknowledge abandonment -> restart/reset. Earlier gates must succeed before a
delivery is reachable. Process possession, A collection semantics, activity
lineage and unsupported historical replay retain their distinct authorities.

## Systematic Consumption Census

All C01-C20 are accounted for; no live same-concept bypass is deferred. The
updated exact authority registry, historical writer census, raw-method/hostile
guards and proof-plan partition controls enforce the boundary, alongside the
execution proofs below. State-only import/native setup never counts as a public
constructor or runnable authority.

| Seam | Final disposition and execution witness |
| --- | --- |
| C01 delivery_target_ownership | Moved to O2/O6; effect/field/emit/compute/fan-out classifiers no longer confer construction. OrdinaryHandlerRequiresCanonicalConstruction and ConstructorBeforeNodeAndAgent controls. |
| C02 delivery_target_application / instance readers | Moved to strict O2 header/optional fields. ReceiverReadinessRequiresConstructedHeader and WorkflowEngineConstructedTransitionAtomic; field-only and missing required fields refuse. |
| C03 bridge/executor/types | Retired missing-state construction and EntityMaterializationAdmitted. TestExecutor_ValidateRequestRejectsHandlerConstruction, TestExecutorRejectsRetiredHandlerConstructionBeforeMutation and ordinary native negative matrix. |
| C04 node_declarative | Retired future-ID/stage/initial-state synthesis. Constructor-before-node/agent and ordinary absent-target controls; existing-target effects still execute. |
| C05 engine adapter/mutation/entityruntime/backend commit | Moved to exact update-only O2 presence; OrdinaryWorkflowMutationCannotConstructOrRepair and native transition atomicity. No companion repair branch. |
| C06 constructor commit/activation/persistence/Manager | Already canonical, strengthened atomic recursive tree and immutable receipts. RootEagerTree, descendant rollback/cancellation, acknowledged failure and public restart/reset. |
| C07 event carriers/bus receiver/contracts/pinrouting | Moved to O2/O3 admitted construction receipt and actual creating dependency; node supplier deleted. ReceiptBindsCreatingPublication and real node/agent execution controls. |
| C08 delivery claim/receiver/lifecycle adapters | Moved to exact constructed/current-attempt authority. Ordinary/SelectedGrantRetirementFencesClaim, constructed-presence and receipt/dependency isolation. |
| C09 bus dispatch/planner/ordinary source/route store/target projection | Moved to O2/O6/O1; cache absence cannot create. ReceiverConfigActivationRaceAndRollback, ConstructorPublicationContenders, public connected/provider cases. |
| C10 snapshot/replay/fan-out transport | Exact historical transport is a different fact, consuming canonical O2 header/receipt before execution. Historical snapshots/replay positive and corruption negatives;642 refusal remains. |
| C11 selected named mutation/materializer/activation/replay/discard | Moved to strict fixed-revision O2 projections; inventories headers, not fields. ForkActivationInventoriesConstructedHeaders, replay coordinate/companion matrix and real-schema destructive-discard retention/rollback matrix. |
| C12 selected agent blueprints/runtime/receiver claim | Different agent declaration/lifecycle concept; consumes O2/O1 inputs. Selected preparation, retirement, grant and changed-target public controls; no handler factory. |
| C13 assignment/stage-gate substrate | Kept existing branch/presence analysis; O6 adds constructor-local guaranteed facts. TestFlowConstructorSuppliedInitialFacts, TestFlowConstructorCandidatesDoNotShareFacts, NestedPresence and progressive-presence public restart. |
| C14 bootverify/report/coverage/flow boundary | Moved to O6; no flag/effect/usage inference or template exemption. TestFlowConstructorProjectionAdmission, TestFlowConstructorVerificationConsumesContract, TestFlowConstructorCreationEdgesConsumeConnectionPolicy and reconstructed parity. |
| C15 contracts/prompt/connect/handler decode/topology | Moved to compiled O6 contract, retired create_entity grammar/corpus. Disk/reconstructed retirement negatives, declaration/call census; A collection/join business implementation unchanged. |
| C16 operator publication/authoringview | Moved to compiled constructor/key admission and rendering. TestDescribeRendersCanonicalConstructors, exact runtime projection/key-slot and public receiver/fork paths. |
| C17 legacy create_entity registration/handler/writer/forward port | Deleted; explicit retired-name refusal remains. TestEntityTools_ConstrainedAllowedToolsDoNotPermitLegacyEntityTools and managed capability negative; canonical commit-ack proofs replace direct writer setup. |
| C18 selected models/conversation negative catalogs | Kept as a different refusal concept, no executable permission. Deferred-work admission and staged non-agent refusal controls. |
| C19 decisions/gate fence/terminal writer | Existing-instance mutation, not construction. Header gate summary/freeze and terminal supersession now consume/write canonical header; missing-target mutation refuses. |
| C20 canonical routing/native/scenario/replay fixtures | Positive execution obtains O2; state-only imports are explicit negatives. Public launch/restart and selected evidence are labeled separately from physical/native fixtures. |

Header sibling census: selected ordinary source, activity projection, effect
projection and fan-out generation; activity eligibility, A join admission,
fan-out barrier admission, decision summaries and terminal supersession writer;
generic AND selected activation inventory; historical receiver replay; standing
restart/reset; and selected discard. All executable readers use exact header
identity/progress plus required optional fields. Business field mutation, query
collections including imports, fixed-revision historical field snapshots and
whole-fork destruction serve different named invariants, not readiness inference.
MaterializingEntityTarget survives only as typed keyed constructor intent or
admitted historical carrier; no static lazy or handler construction path survives.

## Receipt Registry

- W: managed WIP2 race/count3 at02b776816, native291.210s/Manager9.608s;
  tree/ancestry/inventory/replay, lost admission/native cleanup/pre-start/budget.
- X: managed cleanup/selected delivery race/count3 on c76849f82 production,
  pipeline71.171s/native40.251s; subsequent census/schema changes are test-only.
- U: public standing/progressive/sequential count3 at02b776816,259.878s.
  Sequential uses compiled internal mock-lifecycle processes with public RPC,
  not real-provider/public-launcher qualification. Standing/progressive use
  public verify/serve, source deletion and hash-only restart.
- V: unchanged volume root, backend-separated count3; SQLite296.582s PASS,
  PostgreSQL258.000s PASS. The earlier mixed command RED600.031s is not count3 proof.
- A: final-source attachment race/count3 pending in three bounded root groups;
  original thirteen-root aggregate RED600.054s, stale ABA oracle and package
  timeout. The corrected oracle additionally requires pre-Abandon observation
  refusal. Case deadlines and matrix assertions are not reduced.
- K: constructor race/count3 pending, binary built on b75f88ac6;885737eeb changes
  only the separately selected ABA test and audit metadata, not these roots.
- S: final-source public receiver/selected and numeric restart supplement pending.
- H: final-source strict header/presence/descriptor/creating-emission/standing
  native supplement pending, race/count1; both backends are mandatory.
- D: repaired final-source default14-unit managed suite pending. Earlier broad-01
  REDs stopped qualification and do not credit later units.
- G: exact registry/hostile guards4.344s PASS; complexity policy PASS, unchanged
  thresholds; historical writer census/native cleanup3.675s PASS; synthetic
  discard schema root count3,11.383s PASS. Final static guards are rerun in D.

Test names in the table are exact roots, with package located by the checked-in
test declaration; root names are not the hypothetical pre-audit names. A/K/S/D
must finish before final qualification is claimed on the PR. Existing
earlier source receipts are not silently relabeled exact-head evidence.

## Manifestation Coverage

| ID / known manifestation | Disposition | Exact proof / invariant |
| --- | --- | --- |
| P01 phase representation/load budget | reproduced and fixed | TestFlowAttachmentConditionalPhaseProgressBothStores; TestFlowReadinessPassUsesAtMostTwoPublicLoads (W/A/D), instrumented <=2 loads, no marker inference. |
| P02 partial agent/route/timer acquisition | reproduced and fixed | TestFlowAttachmentAgentAcquisitionCutsBothStores, TestFlowAttachmentRouteAcquisitionCutsBothStores, TestFlowAttachmentTimerAcquisitionCutsBothStores (A); each before/after/cancel/panic cut. |
| P03 fail-once predecessor cleanup | reproduced and fixed | TestFlowAttachmentCleanupRetainsExactPredecessorBothStores (A), route/agent_join/timer/abandonment/response_loss, fail_once. |
| P04 persistent cleanup | reproduced and fixed | Same named A matrix, persistent leaves; no successor before exact cleanup and acknowledged abandonment. |
| P05 timers before topology | reproduced and fixed | TestActivateFlowInstanceArmsInitialTimersOnlyAfterRuntimeInstallation (D); TestFlowAttachmentTimerAcquisitionFailureCannotBecomeReadyBothStores (A). |
| P06 lost phase acknowledgment | reproduced and fixed | TestFlowAttachmentNativeCommitAcknowledgmentBothStores (A), real COMMIT/rollback/response-loss cuts. |
| P07 intact attempt replay | execution-proven through the same corrected path | TestFlowActivationAttemptAdmissionBothStores (A); TestFlowAttachmentReadyReplayDoesNotRepeatInstallation (D). |
| P08 joined failed retry | reproduced and fixed | TestFailedFlowActivationCreationPosturesRemainRetryableBothStores and TestFlowAttachmentCleanupRetainsExactPredecessorBothStores (A), new ID/planned, immutable construction unchanged. |
| P09 ABA/stale callback | reproduced and fixed | TestFlowAttachmentPlanABARetainsPredecessorUntilSettlementBothStores (A); TestDynamicFlowRuntimeReadinessRejectsABAAfterAdmissionBeforeExecution (D). |
| P10 concurrent successor | reproduced and fixed | TestFlowActivationAttemptAdmissionBothStores (A); TestDynamicFlowRuntimeReadinessCoalescesConcurrentAttemptsByRunAndInstance (D). |
| P11 lost Begin/abandonment | reproduced and fixed | TestManagerRetainsAdmissionBeforeBeginAndResolvesLostResponse; TestManagerCancelledAdmissionRetainsResolutionUntilRetirement; TestManagerNativeLostAdmissionCleanupBothStores (W); A cleanup response-loss. |
| P12 ready value while fenced | reproduced and fixed | TestReceiverOrdinaryGrantRetirementFencesClaimBothStores, TestReceiverSelectedGrantRetirementFencesClaimBothStores; lifecycle fence/admission-wins/replacement controls (D). |
| P13 cancellation/panic | reproduced and fixed | A acquisition cuts and phase failure; TestSelectedPartialActivationInventoryRetainsEveryFailedStage, TestSelectedPartialRuntimeCleanupRemainsOwnedByPreparation (D). |
| P14 terminal/retired retry | execution-proven through the same corrected path | TestFlowActivationAttemptAdmissionBothStores (A); TestDynamicFlowRuntimeReadinessTerminalRaceRetiresProcessRoute and TestDynamicFlowRuntimeReadinessTerminalBeforeCreationCommitRetiresMaterializedTopology (D). |
| P15 source coordinate replacement | reproduced and fixed | TestDynamicFlowRuntimeReadinessSameVersionSourceReplacementQueuesExactPlan (D); exact source/hash in A attempt identity. |
| P16 descriptor/diagnosis census | execution-proven through the same corrected path | TestDynamicFlowRuntimeReadinessProjectionClassifiesSourceTransitionsBothStores, TestDynamicFlowRuntimeReadinessPlanBatchIsAtomicBothStores, TestActiveFlowInstanceDescriptorAuthorityScopesCensusToExactRunBothStores (H); TestRecoverableStateSnapshotIncludesReadinessOnlyPendingWork (D). |
| P17 fieldless/header presence | reproduced and fixed | TestFieldlessFlowConstructionKeepsLifecycleWithoutStateRowBothStores (K); TestReceiverReadinessRequiresConstructedHeaderBothStores; no fabricated fields. |
| P18 eager recursive construction | reproduced and fixed | TestFlowConstructorRootEagerTreeBothStores, TestFlowConstructorCommitsKeylessDescendantsBothStores, TestFlowConstructorDescendantFaultRollsBackTreeBothStores (K); U complete standing trees before ingress. |
| P19 provider/root construction | reproduced and fixed | TestProviderSelectedRootStandingBootBothStores (D); TestReleaseReceiverInitializationBothStores (S), actual race-instrumented verify/serve/HTTP, typed connected and direct-schema cases. |
| P20 projection/rendered contract | reproduced and fixed | TestFlowConstructorActivationConsumesExactInputBothStores (K); TestFlowConstructorRuntimeProjection, TestFlowConstructorProjectionAdmission and TestDescribeRendersCanonicalConstructors (D/focused). |
| P21 creating input delivered once | reproduced and fixed | TestFlowConstructorPersistsCreatingInputWithoutAutoEmitBothStores, TestFlowConstructorReplayAndRefusalBothStores (K); TestReceiverConstructionReceiptBindsCreatingPublicationBothStores. |
| P22 keyless child has no invented input | execution-proven through the same corrected path | TestFlowConstructorRootEagerTreeBothStores/TestFlowConstructorCommitsKeylessDescendantsBothStores (K), no child creating receipt; U unchanged construction receipts. |
| P23 select/create presence race | reproduced and fixed | TestReceiverConfigActivationRaceAndRollbackBothStores; TestReceiverConfigPublicationContendersBothStores; presence refuses header/field contradiction, no cache-based creation. |
| P24 stage-entry/arm not repeated | execution-proven through the same corrected path | A phase/retry/cleanup snapshots and K immutable replay; TestA2JoinNativeCommitBoundaryOnBothStores eight physical cells, not public proof. |
| P25 public restart | reproduced and fixed | TestStandingRootTreePublicRestartAndResetBothStores and TestEntityProgressivePresencePublicServeRestartSQLitePostgres (U); TestReceiverCompositionRestartBothStores (D). |
| P26 optional emission atomicity | execution-proven through the same corrected path | TestDynamicFlowRuntimeCreationOccurrenceRollsBackAppendedEventOnBothStores, TestDynamicFlowRuntimeCreationOccurrenceLinearizesWithTerminalizationOnBothStores, TestDynamicFlowRuntimeCreationOccurrenceRejectsRetiredAttemptOnBothStores; TestReceiverConfigReadinessRestartPreservesPendingAutoEmitBothStores. |
| P27 recovery/header taxonomy | reproduced and fixed | TestFlowConstructorHistoricalFieldlessSnapshotBothStores, TestFlowConstructorHistoricalFieldedAndTerminalSnapshotBothStores; TestForkActivationInventoriesConstructedHeadersBothStores (W), strict iff-fields. |
| P28 no agents/no autoemit/pre-start | reproduced and fixed | TestStandingFlowAgentsBindAttemptBeforeManagerRun; TestCompletedStandingPreRunHandoffRetainsReconstructionAuthority and TestInterruptedStandingPreRunRequiresExplicitRecovery (W/D); A no-agent timer cuts. |
| P29 constructor before node/agent | reproduced and fixed | TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores (D); TestReceiverConstructionReceiptBindsCreatingPublicationBothStores; no node-first supplier. |
| P30 sibling/observer topology | reproduced and fixed | TestDynamicFlowRuntimeReadinessSiblingAdditionReconcilesUnchangedAgentTopology; TestDynamicFlowRuntimeReadinessSameVersionRouteRevisionReplacesExactTopology (D); U independent alpha/beta text/callback. |
| P31 selected supported execution | execution-proven through the same corrected path | TestSelectedForkPublicChangedTargetExecutionBothStores and TestSelectedForkPendingInputBothStores (S); supported receiver execution X, grants/refusals unchanged. |
| P32 automatic retry | reproduced and fixed | TestDynamicFlowRuntimeReadinessAutomaticRetryCompletesWithoutEnsureOrRestart; TestDynamicFlowRuntimeReadinessNoAutoEmitArmFailureConvergesAfterMissedSignal (D). |
| P33 source-scoped startup | reproduced and fixed | TestSourceScopedStartupDerivesEntireTransitionSetBeforeWriting, TestSourceScopedStartupExcludesTerminalDynamicFlowTopology, TestSourceScopedStartupFinalizesIncompleteDynamicFlowRuntimeReadiness (D); U exact restart. |
| P34 failure versus shutdown | reproduced and fixed | TestFlowActivationShutdownPreservesFailedAttemptDisposition (D); A fail-once/persistent/phase-panic real sink matrix, exact abandonment/joins. |
| P35 reset/pending resources | reproduced and fixed | U reset isolation; TestFlowActivationAttemptBatchRetirementBothStores (A); TestSelectedForkDestructiveDiscardRollbackPendingDeliveryBothStores (X), no orphan headers/receipts. |
| P36 partial selected ownership | reproduced and fixed | TestSelectedPartialActivationInventoryRetainsEveryFailedStage/TestSelectedPartialRuntimeCleanupRemainsOwnedByPreparation (D); TestSelectedForkRetainedDiscardPendingDeliveryTombstonesBothStores (X). |
| P37 A construction/member/arm/delivery composition | execution-proven through the same corrected path | TestPipelineCompiledJoinTransitionEvidenceOnBothStores, TestA2SameBusinessCommitTransitionArmAndPublicationOnBothStores (D); TestGoldenNumericDataScatterParkRestartBothStores (S), compiled mock process with public CLI/RPC, not paid provider. |
| P38 domain commit loss | reproduced and fixed | TestFlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores (K); TestRootConnectedTemplatePublicationAcknowledgmentLossBothStores (S); native receiver fault/contender matrix. |
| P39 sequential/static/high-cardinality disposal | reproduced and fixed | TestGoldenAgentWorkloadSequentialRunsBothStores (U), rechecks A after B; unchanged TestVolumeFanOutExactJobflow1362ImportRouteAndSettleBothStores (V), both backends count3 through settlement/readback/hash restart. |
| P40 unsupported historical non-agent/timer replay | split / escalated as separate class | Open642, existing named deferred/non-agent/timer refusal controls (D/S), unchanged snapshots/no extra execution; positive inventory is not a replay waiver. |
| P41 surviving old authority APIs | reproduced and fixed | TestDynamicFlowLegacyExecutionAuthorityAPIsAbsent, TestDynamicFlowRuntimeReadinessProductionConsumersStatic, exact registry/hostile/writer census and TestFlowConstructorProofPartitionRequiresBothStores (G/D); P01/P53 provide execution. |
| P42 receiver address/grammar calculus | split / escalated as separate class | C2438 slice1 and2413; merged projection consumed, address/handler-retirement loader negatives retained (D). Constructor verification is HERE in P45-P52, never deferred to closed2397. |
| P43 successful response reconstruction | split / escalated as separate class |1930, domain acknowledgment P38 preserved; TestAudit2277ForkCompletionLoss control distinguishes successful domain evidence from failed response reconstruction. No new response protocol. |
| P44 broader agent/process/timer/performance | split / escalated as separate class |2497/2495/2411/2394; B generation and A collection semantics retained; V unchanged timing/settlement oracles, not performance-class closure. |
| P45 supplied initial facts | reproduced and fixed | TestFlowConstructorSuppliedInitialFacts, TestFlowConstructorRuntimeProjection (focused/D); TestFlowConstructorActivationConsumesExactInputBothStores (K); public progressive-presence U. |
| P46 internal collision | reproduced and fixed | TestFlowConstructorProjectionAdmission, TestFlowConstructorVerificationConsumesContract, TestFlowConstructorReconstructedSourceParity (focused/D), pre-mutation refusal; no value-equality exception. |
| P47 type disagreement | execution-proven through the same corrected path | TestFlowConstructorProjectionAdmission/TestFlowConstructorRuntimeProjection/TestFlowConstructorReconstructedSourceParity; K exact typed input/refusal, no coercion or default repair. |
| P48 zero eligible inputs | reproduced and fixed | TestFlowConstructorEligibilityUsesAssignmentChecker/TestFlowConstructorVerificationConsumesContract; public construction rejects unprovided facts before mutation. |
| P49 exact resolved key | reproduced and fixed | TestFlowConstructorResolvedKeySatisfiesOnlyItsDeclaredSlot, TestFlowConstructorPayloadConsumesExactDeliveryProjection (focused/D); K input projection, producer bytes unchanged. |
| P50 candidate/branch contamination | execution-proven through the same corrected path | TestFlowConstructorCandidatesDoNotShareFacts/TestFlowConstructorNestedPresence, progressive-presence assignment controls (focused/D/U). |
| P51 invalid creation edge | reproduced and fixed | TestFlowConstructorCreationEdgesConsumeConnectionPolicy; native constructor replay/conflict (K), public connected admission (S). |
| P52 verify/describe/runtime parity | reproduced and fixed | TestFlowConstructorReconstructedSourceParity; TestDescribeRendersCanonicalConstructors; K strict compiled input and public verify/serve persistence U/S. |
| P53 handler effect-based creation | reproduced and fixed | TestOrdinaryHandlerRequiresCanonicalConstructionBothStores; TestOrdinaryWorkflowMutationCannotConstructOrRepairBothStores (K); TestExecutor_ValidateRequestRejectsHandlerConstruction/TestExecutorRejectsRetiredHandlerConstructionBeforeMutation (D). |
| P54 construction vs handler failure | execution-proven through the same corrected path | TestReceiverCompositionFailureSettlementBothStores (S); TestReceiverConstructionReceiptBindsCreatingPublicationBothStores and K fault rollback; unrelated obligations unchanged. |
| P55 state-only/fieldless/corrupt target | reproduced and fixed | TestFlowConstructorScenarioImportCannotAcquireExecutionBothStores (K); TestSupportedStateOnlyProducersCannotAcquireWorkflowConstructionOnBothStores; strict receiver replay/corruption W and X selected missing-fields refusal. |
| P56 selected/public constructor admission | reproduced and fixed | TestSelectedForkPublicChangedTargetExecutionBothStores (S); native root/invalid fact/K and bootverify negatives; source/grant/642 refusals preserved. |
| P57 historical carrier/dependency | reproduced and fixed | HistoricalFieldless/FieldedAndTerminal snapshots; TestOrdinaryReplayInitializedReceiversBothStores coordinate/companion matrix (W/H); selected pending input S,642 unsupported refusal retained. |
| P58 retired grammar/interpreters/corpus | reproduced and fixed | TestHandlerConstructionRetirementDiskAndReconstructedSource, engine retired request negatives, static consumer guards (D/G); ordinary absence execution P53, not grep-only closure. |
| P59 single atomic construction operation | reproduced and fixed | K recursive construction, rollback/cancellation/immutable replay/ack-loss; TestOrdinaryWorkflowMutationCannotConstructOrRepairBothStores; O2 selected named projection and X atomic discard. |
| P60 direct legacy tool/intent | reproduced and fixed | TestEntityTools_ConstrainedAllowedToolsDoNotPermitLegacyEntityTools; TestManagedCapabilitySurfaceDoesNotInjectRetiredLegacyEntityTools (D); direct writer/forward ports removed, canonical K commit-loss proof. |
| WIP2-S1 incomplete standing root tree/sibling receiver | reproduced and fixed | TestStandingPreparationVerifiesCompleteRootTreeBothStores and TestStandingTreeConstructionFailureDoesNotPublishPartialSetBothStores (W/H); TestStandingRootTreePublicRestartAndResetBothStores (U) and TestProviderSelectedRootStandingBootBothStores (D), alpha/beta text/callback, complete independent trees, exact restart/reset. |
| WIP2-S2 keyed ancestor wrongly eligible | reproduced and fixed | TestStandingRequiresConstructibleAncestry, TestStandingKeyedAncestryRefusesBeforeMutationBothStores (W/H/D), root and intermediate cases, unchanged snapshots; disk/reconstructed/boot agree. |
| WIP2-H1 fieldless activation inventory omitted | reproduced and fixed | TestForkActivationInventoriesConstructedHeadersBothStores (W/H), both generic/selected inventory counters, fieldless/fielded; later admission refusal is asserted, not called successful activation. |
| WIP2-H2 field-row replay receiver authority | reproduced and fixed | TestOrdinaryReplayInitializedReceiversBothStores (W/H), valid agent replay and missing/foreign-run/path/template/stage/JSON/required-field negatives, exact pre-write snapshots, state-only refusal;642 retained. |
| D1 retained completion discard leaves constructed headers | reproduced and fixed | TestSelectedForkReceiverSupportedDeliveryBothStores (X), held exact settlement followed by named activation refusal; TestSelectedForkRetainedDiscardPendingDeliveryTombstonesBothStores/TestSelectedForkDestructiveDiscardRollbackPendingDeliveryBothStores (X), headers/readiness/construction removed atomically or preserved on cancellation/rollback. |

## Tracking, Size And Architecture Feedback

Watchlist decision: refine the existing construction/readiness/lifecycle/header
nodes; lead WIP2 refinement1fcab1a plus issue5950195540 for cleanup consumer.
No new issue or POTENTIAL_ISSUES entry. Parent sibling probe checked process
possession, A collection/join, activity journal/lineage, public response completion,
timer causal history, addressing and state-field collections. They constrain this
child, but do not justify absorbing their distinct failure classes. Parent remains
open: roughly3-4 families/7-11 landings, low confidence; implementation widened
this child's consumers, not the parent's estimated obligation or ownership.

Achieved owner migration: complete cutover of known construction/attachment
interpreters, with no live same-concept fallback after a local endpoint fix.
Qualification remains the explicit pending barrier above; no failure-class closure
is inferred solely from that census. A new source/attempt never reuses removed
resources, and a cleanup failure never makes a successor runnable.

Architecture smell was distributed reconstruction of construction permission and
readiness; disposition promote now in2496, not a deferred child. Broader runtime
decomposition remains2250 and next agent-row work2497. Prefer typed semantic
operations and exact evidence over generic repositories/continuation frameworks.
Remaining broader work is high ROI, approximately8-15 engineer-days for the
nearest several owner families, medium-low confidence; not a promised closure
schedule or reason to preserve dual ownership.

Measured non-test Go against master9a1f27fbd, including generated/production
fixtures and ignoring rename presentation:183 files,+7116/-7612,net-496. The
user explicitly made the3000-line target soft; gross additions are not hidden.
Readiness core386 lines (<500), actual public-load budget<=2/pass tested.
Complexity policy unchanged: cognitive hotspots581->578 (>=50:192->188),
cyclomatic266->262 (>=50 remains51), maxima302/184 unchanged; no hotspot-count
increase. No vendoring, old-store migration/compatibility, --full, capacity
bypass, deadline inflation or provider credentials in CI.
