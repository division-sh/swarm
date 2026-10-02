# R5.1 Implementation Checkpoint

This is an unfinished worktree receipt, not a proof audit, closure claim or
reviewable partial cutover. Gate E's complete construction/attachment boundary
and P01-P60 obligations remain binding.

## Binding Boundary

- Original audit base: `8fac0f2e7cedeed2ec91857cbf7abbb62c705c2d`.
- Current integrated base: C #2505, `1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f`.
- Worktree: `/tmp/agent-e-2496-audit`.
- Branch: `agent-e/2496-construction-attachment`.
- Unpublished preservation checkpoint: `1a95262e1`; rebased checkpoint: `4ff8a3e4f`.
- Timer/startup and C-consumer corrections are local, unpublished work.
- Replacement Gate E: #2496 comment `5916752092`.
- Original audit: `5915862768`; G1/G2 addendum: `5916581924`.
- Frozen constructor/header contract: #1786 comment `5903652180`.
- Soft size target clarification: #2496 comment `5921288989`.

The user/lead clarified that 3000 added production lines is a soft target.
Actual additions/deletions remain reportable; no partial landing, competing
constructor, compatibility reader, compressed code or weakened proof is allowed.
Readiness core below 500 lines, at most two public loads per pass, exact physical
settlement/joins, A/B contracts, C-then-A integration and qualification still bind.

## Actual Worktree Measurement

Production Go only; tests/spec excluded. Generated production and moved lines
count as additions, with no deletion credit against additions.

| Measurement | Lines |
| --- | ---: |
| Production additions, moved/generated lines counted | 5385 |
| Production deletions | 6910 |
| Net production growth | -1525 |
| Current `manager/flow_runtime_readiness.go` | 363 |

Measured against the integrated C base with `git diff 1c4cce209 --no-renames
--numstat -- '*.go' ':(exclude)*_test.go'`. There are no untracked production files.
These are worktree counts, not final PR-head compliance evidence.

The existing exhaustive reader/checker implementations moved to the shared
pipeline consumer count in full (862 lines); this is not a second verifier.
Readiness resource, source-plan, startup and execution owners also count in full.
Their placement alone is not proof of canonicalization: the store phase/attempt
guards, resource ownership and actual consumer paths require separate evidence.

## Implemented, With Focused Evidence

- Constructor admission consumes the exact receiver input, typed resolved key
  and the delivery's typed payload projection, not caller-supplied Fields. The
  keyless constructor refuses arguments. Supplied-only initialization, internal
  collision checks and per-input assignment consume the existing checker.
- The activation commit constructs the complete header, field state iff fields
  are declared, eager keyless descendant tree and unchanged A Instance/Lifecycle
  contribution atomically. Fieldless staged/inert component controls pass.
- Receiver presence distinguishes absent construction from missing field rows;
  ordinary handler execution cannot obtain construction permission from flags,
  field references, absent state, a node supplier or a future-ID carrier.
- Current receiver initialization is flow-only and exact. Old node/dependency
  records fail closed. Node success/failure no longer creates or cancels another
  receiver's construction or agent obligation.
- Selected-store attachment uses conditional phase/attempt/plan guards and
  monotonic successor admission. The core is 363 lines; physical ownership and
  joined retirement remain explicit, not deleted to meet a size target.
- Immutable construction replay validates the transaction-bound canonical
  target, not just receipt equality and row existence. Foreign field contracts
  and foreign workflow headers refuse without repair. Legitimate current
  fields, stage, gates and bookkeeping progress is not creation identity.
- Historical/import projection uses the shared exact header projection and
  preserves fixed-revision fields, stage and branch clocks. Component readback
  and strict exact-reuse tests pass; this does not prove runnable static
  selected routing or recovery.
- The standalone MaterializeInitialEntry / CommitWorkflowInitialMaterialization
  API is retired throughout production, test consumers and generated facade
  forwarding. Both isolated implementation files, the commit port/result types,
  coordinator/store/backend methods and fake writer are deleted. The immutable
  construction receipt remains with the canonical O2 owner. The preserved
  PrepareInitialEntryLifecycle signature is unchanged.
- All 56 old calls are accounted for below. External component fixtures explicitly
  prepare, commit through the actual selected store and finalize only acknowledged
  Created results. The 48-line fixture assembler only combines the returned
  Instance/Lifecycle and exact source/route data; it cannot prepare or persist.
  Its empty route sets earn no public construction/readiness qualification.
- Unit fixture header reads now consume independently persisted header evidence,
  not a copy of entity state. Their writes use the existing exact canonical header
  columns and revision/current-state guards. Missing construction repair remains
  refused; no alternate construction receipt or readiness writer was introduced.
- Root execution now uses the same canonical Stored route identity as construction.
  Its unit regression was red first and now passes. This fixes the audited scope
  mismatch without qualifying public root startup or receiver routing.
- Source revision supersession and retirement preserve the predecessor's installed
  phase/ordinal until cleanup. Begin admits one successor at ordinal + 1 and
  planned, rather than resetting progress during retirement. The real-store
  source-revision matrix checks this settled retry contract on both stores.
- The creation-event atomicity fixture now prepares and commits through O2.
  The gate consumer gets its initial gate/card from the constructor lifecycle,
  not manually fabricated state. The timer consumer declares recurrence in
  source, rather than editing compiled semantics. Both consumer fixtures use
  the actual store commit with empty component route sets; they are not public
  route installation or served-run proof.
- Run-control's two-timer-family cleanup fixture now constructs a real fieldless
  header through O2, not an empty entity declaration or standalone constructor.
  The SQLite persistence component also consumes the actual constructor and
  admitted numeric declaration. Its explicit display-metadata persistence check
  is not public construction qualification.
- The canonical acknowledged-error matrix now counts actual lifecycle
  finalization: exactly once on a committed creation, unchanged on exact replay,
  and zero on an unacknowledged nonzero result. This replaces the standalone
  initial-materialization fault fixture; activity journal regressions remain.
- Immutable constructor replay now preserves business configuration (including
  nullable and identity-lookalike keys), exact microsecond clocks and the
  fresh-schema rejection of projection version 1. Twelve changed-construction
  variants per store refuse as conflicting duplicates without mutation. Missing
  field evidence preserves that typed disposition instead of returning a generic
  error; neither outcome can repair construction.
- Authoritative handler, receiver presence, timer initial-entry, reachability,
  public API/CLI and continuation prose no longer permits handler construction
  or state-only companion repair. Parsed-spec retirement and source-reference
  checks pass; OpenRPC is regenerated. The C16 production public-API interpreter
  still requires migration, so this is not spec/runtime closure.

Earlier pre-C focused receipts (not current-head or public qualification):

| Proof | Result |
| --- | --- |
| FlowActivationAttemptAdmission, FlowActivationAttemptBatchRetirement, DynamicFlowRuntimeReadinessRejectsABAObservation, both stores | PASS, 2.686s |
| ReceiverConstructionAndObserverIsolation, raw-history refusal and ReceiverFlowInitializationPublication, both stores | PASS, 18.452s |
| ReceiverConstructionAndObserverIsolation/node_agent/terminal_race, both stores, `-race -count=3` | PASS, 26.230s |
| Manager acknowledged finalization, two public loads, exact replay and acknowledged-error controls, `-race -count=3` | PASS, 5.983s |
| FlowConstructorReplayAndRefusal, both stores, `-race -count=3` | PASS, 68.908s; eight cells per store |
| Same replay/progress/corruption matrix after typed missing-evidence correction, both stores, `-race -count=3` | PASS, 69.296s; eight cells per store |
| FlowConstructorImmutableReplayConflict, both stores, `-race -count=3` | PASS, 103.238s; twelve changed-construction cells per store, exact replay/config/clock controls and retired projection-version refusal |
| WorkflowInitialLifecyclePreparationRejectsUnownedEmissions, after retiring duplicate standalone fixtures | PASS, 0.107s; preparation refuses before persistence |
| ReceiverConfigActivationRaceAndRollback/exact, both stores, `-race -count=3` | PASS, 17.056s; deterministic SQL writer/contender barrier |
| Creation occurrence terminalization, appended-event rollback and retired-attempt refusal, both stores, `-race -count=3` | PASS, 41.196s |
| Workflow timer/gate committed-error consumers, both stores, `-race -count=3` | PASS, 38.614s |
| Canonical constructor acknowledged-error/lifecycle finalization, run-control two-family cleanup and SQLite persistence, `-race -count=3` | PASS, 35.416s; both stores for both dual-store roots |
| FlowAttachmentPhaseFailureRetainsConstruction, both stores, all 32 cells, `-race -count=1` | PASS, 56.976s |
| Same complete attachment matrix, `(planned|agents_registered)`, both stores, `-race -count=3` | PASS, 81.768s |
| Same complete attachment matrix, `(route_installed|timers_armed)`, both stores, `-race -count=3` | PASS, 82.671s |
| Constructed transition, mixed-snapshot and state-only scenario refusal matrix, both stores, `-race -count=3` | PASS, 162.823s |
| Historical fieldless, fielded and terminal snapshots, both stores, `-race -count=3` | PASS, 98.850s |
| Scenario import cannot acquire execution, both stores, `-race -count=3` | PASS, 25.925s |
| FlowConstructorPayloadConsumesExactDeliveryProjection | PASS, 0.016s |
| RunForkHistoricalAdmissionConsumers architecture guard | PASS, 8.401s |
| FanOutReceiverInitializationDoesNotBecomeChildPublication and ReplayReceiverRequiresFixedPublicationAndInitializedState | PASS, 0.011s |
| ExecutorFanOutCapturesReceiverWithoutPublicationAuthority | PASS, 0.008s |
| Testplanning proof/partition/policy checks after backend-row registration | PASS, 10.281s |
| FlowConstructorProofPartitionRequiresBothStores, including run-control and immutable replay rows | PASS, 0.005s |
| Spec construction-before-handler, retired tool, analyzer-reference, handler hierarchy and API coverage checks | RED, 0.101s; PASS after contract correction, 0.136s |
| GeneratedOpenRPCArtifactMatchesPlatformSpec, construction and analyzer-reference checks | PASS, 0.169s |
| DynamicFlowLegacyExecutionAuthorityAPIsAbsent, strengthened standalone-constructor guard | RED, 4.916s; eight live production declarations |
| Same retired-API guard after complete removal, including test/testutil declarations and references | PASS, 1.485s; only guard-name string literals survive |
| Pure flowactivationfixture.Command assembly and exact-authority refusal controls | PASS, 0.005s, `-count=3`; no preparation or persistence |
| FlowInstanceIdentity_RootConstructorAndExecutionShareRoute | RED, 0.111s; PASS, 0.149s, `-count=3`, after canonical route consumption |
| Complete FlowInstanceIdentity unit group after correcting the template fixture | PASS, 0.246s, `-count=3` |
| Eight affected package test binaries after caller migration | PASS, managed compile-only; no execution qualification |
| PersistenceAuthorityFindingRegistry and PersistenceEffectiveMethodSetsDoNotExposeRawAuthority | PASS, 10.029s; 78 new findings classified, 146 retired findings removed |
| FlowConstructorReplayAndRefusal, FlowConstructorAcknowledgedFailureRetainsExactIdentity, FlowConstructorReadinessKeepsExactRunOwnership and DynamicFlowCreationSourceRevisionPublication, both stores | PASS, 55.304s, managed `-race -count=1` |
| Retired API guard plus Manager at-most-two-public-loads control | PASS, 11.164s, managed `-race -count=1` |
| Migrated compiled admission/timer/no-op controls, identity group, timer lifecycle/arm/mode/reconciliation controls and plan-mode refusal | PASS, 47.636s, managed `-race -count=3`; all included both-store roots executed |
| Parsed spec construction/retirement and generated OpenRPC controls after retirement | PASS, 0.201s |
| FlowConstructorProofPartitionRequiresBothStores, including exact run-readiness row | PASS, 0.006s |

The receiver matrix executes node-only, node/agent and node/two-agent shapes,
success/failure/retry-cancel/terminal-race/publication-rollback cells, exact
duplicate construction and malformed/retired-carrier refusals. It proves store
and planner consumption, not a provider or public-launcher journey.

Replay's first run was RED for foreign_fields and foreign_header (2.358s).
The correction calls the existing canonical target reader/validator. Each
negative now snapshots all selected-store tables and proves no repair or
mutation. The positive advances state through the real mutation owner, then
proves replay neither rewrites progress nor repeats lifecycle work.

The stronger missing-fields replay check was RED (0.506s): refusal was correct
but its failure was untyped. The canonical replay comparator now reports an
occupied incomplete construction as unequal, so the commit owner returns the
existing conflicting-duplicate disposition. Focused missing-fields/receipt
controls passed (0.736s), then the complete dual-store race matrix passed above.
No compatibility constructor or reader-side repair was added.

Retired fixture-to-proof mapping:

| Retired standalone fixture | Canonical replacement proof |
| --- | --- |
| WorkflowInitialMaterializationReportsExactReplayWithoutReapplyingEffectsSQLitePostgres | FlowConstructorImmutableReplayConflictBothStores for config/clock/contract/stage conflicts and projection-version refusal; FlowConstructorReplayAndRefusalBothStores for progressed-state preservation and missing field/receipt refusal; FlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores for exactly-once lifecycle finalization |
| WorkflowInitialMaterializationConcurrentExactReplayPostgres and its polling/fake-lifecycle helpers | ReceiverConfigActivationRaceAndRollbackBothStores/exact uses the actual SQL writer/blocked-contender barrier on both stores; one Created, one acknowledged replay and no replay lifecycle evidence. Exactly-once finalization remains independently covered by FlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores |
| WorkflowInitialMaterializationRejectsUnownedLifecycleEmissions | WorkflowInitialLifecyclePreparationRejectsUnownedEmissions calls the preserved preparation owner directly and verifies refusal before any persisted header |
| DynamicFlowRuntimeReadinessPersistsAndReplaysExactlyOnBothStores: immutable replay/config/clock and acknowledged lifecycle effects | FlowConstructorImmutableReplayConflictBothStores, FlowConstructorReplayAndRefusalBothStores and FlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores; actual selected-store commit, not a synthetic readiness writer |
| Same retired fixture: changed source, stale callbacks and immutable creation occurrence | DynamicFlowCreationSourceRevisionPublicationBothStores and DynamicFlowRuntimeCreationOccurrenceRejectsRetiredAttemptOnBothStores; exact attempt/plan guards and actual creation commit |
| Same retired fixture: phase/mode admission and no-auto-emit | FlowActivationAttemptAdmissionBothStores, FlowAttachmentPhaseFailureRetainsConstructionBothStores, FlowReadinessPlanRejectsMissingAndConflictingExecutionModes and FlowConstructorPersistsCreatingInputWithoutAutoEmitBothStores |
| Same retired fixture: independent, paused and terminal run ownership | FlowConstructorReadinessKeepsExactRunOwnershipBothStores; actual run lifecycle mutations, immutable predecessor readback, successor projection and terminal replay no-mutation proof |

These replace the synthetic fixture invariants with separately named execution
proofs. Agent/route installation remains a distinct public integration obligation;
neither the old synthetic fixture nor empty component route sets prove it.

## Exhaustive Old-API Caller Retirement

The original AST census contained 56 calls, not merely the eight production
declarations found by the architecture guard. Every call has a disposition.

Forty calls in these external component files now use explicit preserved lifecycle
preparation -> pure command assembly -> selected-store CommitFlowInstanceActivation
-> acknowledged Created lifecycle finalization. Compilation is proven for all
eight affected packages. Execution proof is only claimed for the named rows above;
this migration does not mark every supported-surface test green.

| Consumer file | Migrated calls |
| --- | ---: |
| runtime/artifact_action_result_delivery_test.go | 2 |
| runtime/bus/eventbus_publish_test.go | 4 |
| runtime/cataloge2e/run_scoped_flow_agent_identity_e2e_test.go | 1 |
| runtime/cataloge2e/runtime_harness_test.go | 2 |
| runtime/channel_runtime_supported_surface_test.go | 1 |
| runtime/conformance/fan_in_barrier_runtime_conformance_test.go | 1 |
| runtime/conformance/fan_in_stream_conformance_test.go | 1 |
| runtime/conformance/persisted_surfaces_test.go | 1 |
| runtime/eventbus_receiver_authority_e2e_test.go | 1 |
| runtime/node_delivery_startup_recovery_test.go | 4 |
| runtime/pipeline/delivery_target_declared_key_supported_surface_external_test.go | 1 |
| runtime/pipeline/receiver_future_appearance_external_test.go | 2 |
| runtime/pipeline/workflow_gate_recovery_external_test.go | 5 |
| runtime/pipeline/workflow_join_supported_surface_external_test.go | 2 |
| runtime/pipeline/workflow_timer_supported_surface_external_test.go | 7 |
| runtime/workflow_timer_startup_recovery_test.go | 3 |
| serveapp/main_runtime_test.go | 2 |

Paths in this table are relative to `internal/`. The remaining sixteen calls are:

- Eight explicit unit fixture seeds: six workflow_timer_owner controls, one
  compiled timer-evidence control and one compiled no-op control. They use
  preserved preparation, the private unit fixture state/lifecycle writers and
  public finalization. They are marked unit setup, not canonical-store proof.
- Three compiled initial-admission calls: replaced by direct preparation/refusal
  proof. Preparation does not persist; repeated preparation returns the same
  normalized instance and no repeated causal timer contribution.
- Five calls in the synthetic readiness/replay fixture: deleted with its fake
  source revision and creation-event helpers; invariant mapping appears above.

The source guard inspects declarations, fields, types, values and selectors in
runtime/store/testutil Go, including tests, for the retired constructor symbols.
The final source census finds no live old constructor call, declaration, wiring
or forwarder. Unrelated route test mocks are not claimed retired by this check.

The refreshed persistence census classified every one of 78 new findings:
69 private-backend, six typed-process-local, two typed-public-facade and one
private-domain-adapter. No unclassified finding or runtime raw-SQL exception was
accepted. The removed isolated writer accounts for retired census entries; the
registry and effective-method-set guards both pass.

## Timer/Startup Repair And C Integration

The earlier broader managed race supplement failed even though its canonical-store
and Manager groups passed. Those red receipts remain recorded below, not relabeled
as flakiness or baseline exoneration. The final count-one managed repetition now
passes all three roots on SQLite and PostgreSQL. The same three roots then passed
managed `-race -count=3`, with unchanged deadlines and readback assertions.

| Proof | Original red / current focused result |
| --- | --- |
| TestAuthoredWorkflowTimerExecutesCompiledConnectRouteOnBothStores | Original RED, both stores, empty trace at unchanged 5s deadline, 19.51s. Current PASS, both stores, 1.82s; real scheduler/EventBus/compiled connect, explicitly prepared component consumer. Not parent-driven public eager construction. |
| TestWorkflowTimerOneShotRestoresBeforeFireAndStaysTerminalAfterRestartOnBothStores | Original RED, both stores, 13.86s. Current PASS, both stores, 1.02s; unchanged one-shot restoration/terminality/readback assertions. |
| TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores | Original RED, both stores, persisted-identity refusal, package 9.414s. Current PASS, both stores, package 5.887s; compiled internal Runtime.Start with exact stored root identity, not public-launcher/served qualification. |

The pipeline package in the original failed supplement took 33.417s; the final
count-one run took 2.856s. No deadline, terminality/readback assertion, admission
guard or old constructor was restored. Intermediate runs exposed source/version
fixture mismatches, missing root blueprint, incomplete consumer identity, and
stale startup inventory; they are real red observations, not erased history.

Repairs consume the actual source hash/version and occurrence scope. Component
consumer construction uses its compiled initial stage, canonical lifecycle header
identity, fields iff declared and the actual named activation commit. Root execution
retains the canonical authored root plus exact run ID, not an authored-scope or
UUID-shaped substitute. Readiness recovery uses the plan's complete stored route,
not a scope inferred from path segments.

The source-backed route table now retains construction blueprints for keyed and
keyless scopes under their canonical scope keys, including root. Per-run installed
owners remain separate from declarations. The deterministic route regression was
red for the missing root blueprint, then passed (0.134s), including isolated
retirement and foreign/malformed-root rejection.

Startup finalization observes the exact retained preparation attempt, not the
inventory snapshot preceding acceptance or predecessor replacement. It still
checks source/hash/ordinal, exact process binding and current store disposition.
The planned/retired/aborted handoff regression proves one preparation load and two
finalization loads, no repeated construction and a retained exact route. The
bounded-load, revision and ABA controls passed together (0.274s). Its earlier
retired-case red exposed a unit batch-retirement adapter that failed to project
retired disposition; that fixture now mirrors the selected-store contract.

C09 selected target lookup previously read entity_state and inferred active
ownership when its header was missing. That omitted legitimate fieldless flows
and kept state-only ownership authoritative. Both scoped and unscoped selected
target readers now enumerate flow_instances, consume header lifecycle evidence,
and never synthesize a field row or orphan-header fallback. The new both-store
header lookup regression passes on both stores under `-race -count=3`: fieldless
and fielded construction, all/path/source lookups, foreign-run exclusion and
state-only negative lookup after header removal. No field rows are added by a
lookup; existing orphan field evidence remains untouched. Public root/selected
qualification remains unfinished.

C #2505 is integrated by rebase at its exact merge commit. Positive E-added
fixtures no longer use retired authored mode values or mapping-only event pins;
intentional negative fixtures remain. Constructor verification consumes actual
compiled connection edges, not pin-owned create/select policy. Disk/reconstructed
source tests cover create/select-or-create rejection and select-only allowance,
including another valid constructor input that must not conceal an invalid edge.
The complete focused constructor verification group passed (0.790s). C integration
compile checks passed; that does not qualify every migrated runtime consumer.

### Current Integrated-Head Race Receipts

One managed six-package run completed successfully with `-race -count=3 -v`.
These are package execution durations, not sums or full-suite timing:

| Selected package / actual roots | Result |
| --- | --- |
| pipeline: AuthoredWorkflowTimerExecutesCompiledConnectRouteOnBothStores and WorkflowTimerOneShotRestoresBeforeFireAndStaysTerminalAfterRestartOnBothStores | PASS, 61.575s; SQLite and PostgreSQL each executed three times. |
| runtime: RuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores | PASS, 44.880s; SQLite and PostgreSQL each executed three times. |
| manager: StartupFinalizationConsumesRetainedPreparationAttempt; FlowReadinessPassUsesAtMostTwoPublicLoads; revision/ABA-after-admission refusals; retired-API and production-consumption guards | PASS, 40.484s. |
| bus: RouteTableKeylessConstructionPublishesExactRunOwners; foreign-source replacement refusal; mismatched explicit route refusal | PASS, 3.741s. |
| bootverify: FlowConstructorCreationEdgesConsumeConnectionPolicy | PASS, 8.242s; select/create/select-or-create, disk and reconstructed source. |
| runtimepersistence: SelectedRunTargetOwnersUseConstructedHeadersBothStores; FlowConstructorAcknowledgedFailureRetainsExactIdentityBothStores; FlowConstructorCommitsKeylessDescendantsBothStores; descendant rollback/cancellation/corruption; ReceiverReadinessRequiresConstructedHeaderBothStores | PASS, 161.823s; every included SQLite/PostgreSQL leaf executed three times. |

The new header-lookup backend leaves and the Runtime.Start backend leaves are
registered in the existing proof-plan units. A second managed supplement passed
with `-race -count=1 -v` on the same integrated worktree:

| Selected package / actual roots | Result |
| --- | --- |
| store: PersistenceAuthorityFindingRegistry and PersistenceEffectiveMethodSetsDoNotExposeRawAuthority | PASS, 26.885s; all 10,959 findings classified. |
| runtimepersistence: OrdinaryHandlerRequiresCanonicalConstructionBothStores; HistoricalFieldlessSnapshot; HistoricalFieldedAndTerminalSnapshot; ScenarioImportCannotAcquireExecution | PASS, 70.322s; every included SQLite/PostgreSQL leaf executed, including strict reuse/corruption refusals. |
| apispec: PlatformSpecConstructionPrecedesOrdinaryHandlers and GeneratedOpenRPCArtifactMatchesPlatformSpec | PASS, 2.797s. |
| testplanning: FlowConstructorProofPartitionRequiresBothStores | PASS, 1.052s; includes both new backend rows and the runtime-unit assertion. |

No complete suite or exact-head CI is claimed. Remaining route derivation and
retirement consumers still require the complete owner-consumption and physical
cleanup matrix; passing these selected paths is not class-wide closure.

The SQLite persistence migration first failed because the old raw fixture
declared an unadmitted `decimal` structural type (0.493s). A managed run that
compiled that fixture also failed (20.108s); it is not erased or called flakiness.
The fixture now uses the admitted numeric declaration; the earlier pre-C
35.416s combined race-count3 receipt includes that correction. This focused
C-integration run does not repeat that SQLite persistence test. No production
type fallback was introduced.

## Remaining Owner-Complete Work

1. Wire canonical public root construction and creating-event ownership. The
   real catalog receiver test is currently RED: ordinary root delivery reaches
   a missing root header and correctly refuses instead of repairing it in the
   handler. Do not bypass this gate with private seeding or a handler fallback.
2. Finish runnable selected/static routing and recovery integration. Strict
   historical-header/import component reads now pass. Historical projection is
   not fresh business construction and cannot confer execution permission
   through ordinary state-only companion repair.
3. Replace C16's public API handler-flag search with compiled input/key constructor
   admission. This was already audited, not a newly split class. The isolated
   high-level constructor API and all 56 old callers are now retired; its absence
   is not a substitute for finishing this remaining public interpreter.
4. Integrate C then A at pinned agreed checkpoints before routing-facing shared
   changes. Preserve A's PrepareInitialEntryLifecycle signature and returned
   Instance/Lifecycle together; do not edit collection/join implementation.
   C PR #2505 is integrated as 1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f
   (implementing head ab707f89ba5e1dc23dbcdd335c820ab00ba95ebd),
   after preserving the unfinished E changes locally. A's pinned
   1d5059fb231fba92484eff0240d8fdf252792d8f is not integrated yet. Previous comments
   saying C is unmerged or unintegrated are superseded, not relabeled as proof.
5. Complete physical resource/startup/cleanup/unknown-acknowledgment evidence,
   all P01-P60 rows, public root/descendant and hash-only restart journeys,
   supported selected execution, volume/sequential controls and managed suite.

The catalog assertion migration now checks exact shared constructed ownership,
not node-first settlement. Its SQLite collector leaf failed in 0.658s at the
missing public root constructor. This is an approved unfinished obligation, not
passing qualification or a new class requiring another prose-only gate.

No full suite, served qualification, final spec validation, PR-head CI, push,
reviewable PR, merge approval or failure-class elimination is claimed. Local
preservation checkpoints do not authorize a partial cutover landing.

Watchlist: the existing complete construction/attachment mapping and canonical
Gate E receipt `04c51c95` remain correct. No new architecture issue is inferred
from implementation size. #2411/#2250 remain open; this checkpoint earns no
parent closure credit.

## Public Construction and Integrated Recovery Checkpoint

This section supersedes the earlier pending/RED public-root, C16 and hash-only
restart statements above. Earlier receipts remain historical, not silently
relabeled. This is still unfinished implementation evidence, not a proof audit
or a reviewable partial cutover.

### Integration and Ownership

- C #2505 remains integrated at `1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f`.
- A's original pin `1d5059fb2` is integrated at `32b8e5e00`; its C-rebased tail
  through `048b2819b` is integrated through local `84772e532`. Both shared
  signatures are unchanged. No E edit to A's collection/join production owner.
- A's next handoff `910c26d67` (#2496 comment `5930688604`) was fetched and read.
  Its eleven incremental commits are not yet credited as integrated or qualified.
  Preserve the M34 two-parent/shared-leaf-key `route_plan_instance_conflict`
  refusal and R7 disposition; no routing workaround is authorized.
- Ordinary root ingress now plans canonical root construction and eager keyless
  descendants before receiver classification, through the existing Manager
  constructor. Missing planner authority refuses. There is no handler repair.
- C16 public API admission uses the exact compiled constructor input/key and
  connected receiver policy, not a `create_entity` handler scan. Positive fixture
  bundles now live in the closed canonical fixture owner.
- Transaction-local nested construction results remain unacknowledged. Only
  the acknowledged outer mutation promotes the complete immutable result tree.
  Promotion consumers: ordinary API/event publication, inbound publication,
  activation, workflow engine mutation, fan-out, human-task expiry, decision-card
  mutation, workflow decision routing, workflow timer and generic schedule
  occurrence. Manager no longer copies a parent's acknowledgment into children.
- Startup completion distinguishes the exact retained pre-run successor and its
  actually observed predecessor ordinal. Interrupted preparation still requires
  explicit recovery; a stale historical ready snapshot cannot authorize it.
- Same-attempt reconciliation now consumes durable phase progress to skip
  completed installation while verifying agent/route owners. New attachment
  admission always resets physical installation to planned. The original retry
  test reproduced duplicate timer arming under race; its durable-ready callback
  now joins the exact owned pass before shutdown and asserts successor ordinal 2.
- The deleted materializer-authority test is retired, not restored under another
  name. Current construction-before-node/agent execution owns positive proof.

### Execution Receipts Actually Run

Commands use the managed runner for multi-package/served execution, and ordinary
`go test` only for small targeted checks. No `--full`, capacity override, deadline
increase, private-root seeding workaround or weaker refusal assertion.

| Receipt | Result and proof boundary |
| --- | --- |
| `/tmp/agent-e-2496-public-recovery-final.log` | Overall RED because A activation fixture omitted business Config.order_id; bus, Manager, catalog and serveapp groups PASS. |
| `TestReceiverCompositionRestartBothStores` and `TestReleaseReceiverInitializationBothStores` in that receipt | serveapp PASS 72.957s, both stores: race-instrumented actual CLI verify/serve, connected typed construction and provider ingress, exact child-claim interruption, source deletion, hash-only restart, byte-identical durable route and immutable construction projections, exact-once delivery and terminal public readback. Not real paid-provider qualification. |
| `TestReceiverConstructionBeforeNodeAndAgentExecutionBothStores` | catalog PASS 7.187s, SQLite/PostgreSQL x collector/renamed-observer. Exact typed created-at evidence is read through the workflow owner, not driver-dependent raw timestamp scans. |
| `/tmp/agent-e-2496-public-repair.log` | API PASS 13.263s and catalog PASS 30.112s under race; API is planning/persistence component evidence, not standalone readiness qualification. |
| `/tmp/agent-e-2496-integrated-recovery-race.log` | Entire managed group PASS: pipeline 70.801s, Manager 4.424s, runtimepersistence 11.350s. A initial join activation (eight cells), root/child non-loop re-entry, count-join execution/restart, startup/pre-run recovery refusal, two-load bound and nested construction/outer-acknowledgment matrix. |
| `/tmp/agent-e-2496-timer-arm-retry-race.log` and `...-joined.log` | RED preserved: 20 race repetitions reproduced shutdown-before-owned-completion; exact join then exposed duplicate arming (3 instead of 2). |
| `/tmp/agent-e-2496-readiness-phase-retry-race.log` | PASS 12.249s, three retry/startup/load-bound roots, race count=10. |
| `/tmp/agent-e-2496-ready-replay.log` | PASS 3.215s, deterministic same-attempt ready replay, race count=3; unchanged durable row, one timer arm, one route install, no creation emission. |
| `/tmp/agent-e-2496-historical-projection-repair.log` | PASS 0.365s: exact batched metadata, constructed/import distinction and complete delivery-route projection inventory. |
| `/tmp/agent-e-2496-fixed-descriptor-repair.log` | PASS 4.659s: prepared/raw current header-owned descriptor reads and fresh-fact corruption errors. This is adapter component evidence, not runnable topology. |

The public backend leaves are now required in the existing local catalog,
local serveapp, CI catalog, CI serveapp-channel and CI serveapp-runtime units.
The proof-partition guard checks those exact children. The closed fixture-owner
repository guard is queued through the capacity-managed runner; no PASS claimed
until execution completes.

### Full Managed Suite: RED, Not Complete

`go run ./cmd/swarm-test` was actually run and exited 1. Receipt:
`/tmp/agent-e-2496-managed-suite.log`. It stopped at `broad-01`, with 14 failed
packages, 165 failed root tests and 659 failed root/subtest records. The other
13 planned units did not execute. This was an integrated dirty-worktree
diagnostic run, not exact-head qualification or a flakiness determination.

Repairs since that run include reader/check census expectations, retired
handler/materializer tests, canonical fixture ownership, A initial lifecycle
fixture identity/configuration, Manager interrupted startup and exact successor
handoff, outer acknowledgment promotion, phase-idempotent timer reconciliation,
historical projection schemas and projection-only readiness fixtures. Individual
passing repairs do not turn that full receipt green.

Remaining failure families are explicit work, not silently delegated follow-ups:
old positive handler/future-first-materialization fixtures, component buses with
no root constructor planner, source-coordinate mismatch in gate/join/timer
fixtures, state-only selected-fork seeds rejected by strict historical header
admission, private route-recovery schemas/fixtures missing full headers, and
adapter source guards/oracles still describing the deleted state/companion
decision. Some newly repaired families have not yet had their complete backend
matrix rerun. Genuine regressions and stale fixtures must be separated by named
execution, never by assuming every RED is expected.

### Updated Measurement and Closure State

Against the integrated C base, inclusive of integrated A changes, tracked
production Go has 9233 additions / 8829 deletions. The two untracked production
files (root_construction.go 68 lines, canonical fixture constructor_proofs.go
89 lines) add 157 more: **9390 additions / 8829 deletions, net +561**. Tests/spec
excluded; generated/moved and fixture production Go count. This inclusive
measurement must not be compared to the earlier E-only checkpoint as if A's
contribution were absent. Core readiness is **371 lines**, below the binding 500.

Chosen-class elimination, P01-P60/C01-C20 completeness, runnable selected/static
recovery, full physical retirement/failure matrix, all-green managed suite,
exact-head CI, push and reviewable PR remain unproven. Existing Gate/watchlist
mapping stays binding; #2411/#2250 remain open. No Post-Implementation Proof Audit
or failure-class closure is claimed by this checkpoint.

## Managed Qualification and Consumer Repairs: 2026-10-01

This section supersedes the earlier pending A integration, queued guard,
public-path and measurement statements. Earlier RED receipts remain recorded.
It is an unfinished implementation checkpoint, not a Post-Implementation Proof
Audit or permission to land a partial cutover.

### Pins and Head Boundaries

- A's eleven incremental commits through `910c26d67` are integrated through
  local `3ae2bf970`, after the existing C integration. Both shared lifecycle
  signatures remain unchanged. No E production edit to A's collection/join
  owner; M34's two-portfolio/shared-leaf-key refusal and R7 disposition remain.
- A newly introduced test call to the retired `MaterializeInitialEntry` failed
  pipeline compilation on `3ae2bf970`. It is migrated at `0422d09e0` to the
  preserved lifecycle preparation, explicit command assembly, actual selected
  construction commit and acknowledged-only finalization. Compilation alone
  does not qualify that test's execution.
- Current code checkpoint is `81dab82ad`. Commits after `0422d09e0` change
  only tests/guards: production Go is byte-identical to the public qualification
  head. This does not turn an earlier receipt into exact-current-head CI.

### Public Construction/Recovery Qualification: PASS

Receipt `/tmp/agent-e-2496-public-repaired-head.log`, on `0422d09e0`, executed
18 roots in seven packages through `go run ./cmd/swarm-test -- ...`, under
`-race -count=1 -v -timeout=12m`. Capacity wait was 7m20s; no bypass or `--full`.

| Package | Actual PASS and boundary |
| --- | --- |
| serveapp, 78.008s | ReceiverCompositionRestartBothStores and ReleaseReceiverInitializationBothStores: actual race-instrumented CLI verify/serve and public RPC, both stores, exact claim interruption, source-directory deletion, hash-only restart, byte-identical route/construction projections, exact-once delivery and terminal public readback. Internal provider fixture, not paid-provider qualification. |
| apiv1, 14.661s | Three root/renamed-connected create-input rejection roots; component planning/persistence proof, not independent runnable readiness. |
| cataloge2e, 32.157s | ReceiverConstructionBeforeNodeAndAgentExecutionBothStores, collector and renamed observer; exact typed creation evidence and receiver ownership. |
| pipeline, 72.902s | A initial-join activation, fieldless paired reply, root/child non-loop re-entry and count-join real execution/restart, both stores. |
| manager, 4.940s | Source-scoped completed topology reconstruction, retained standing pre-run handoff, interrupted-pre-run explicit refusal, two-load bound, same-attempt ready replay, missed-signal timer-arm convergence. Component Manager controls, not public launcher proof. |
| bus, 1.044s | Nested construction acknowledgment promotes the exact complete tree only after enclosing commit; transaction-local evidence remains unchanged. |
| runtimepersistence, 10.953s | Recursive keyless construction commit/rollback and exact descendant readback, both stores. |

This replaces the earlier RED A fixture mismatch for this exact selector.
It does not qualify every P01-P60/C01-C20 manifestation or selected-fork journey.

### Full Managed Suite: RED on Two Committed Heads

Both runs used the default `go run ./cmd/swarm-test`, without `--full` or capacity
override. The runner stopped on `broad-01`; the other 13 of 14 units did not run.

| Head / receipt | Actual result |
| --- | --- |
| `3ae2bf970`, `/tmp/agent-e-2496-managed-suite-integrated-head.log` | Exit 1, eight failed packages, 48 failed roots, 255 root/subtest failure records, plus pipeline build failure from the retired constructor call. Unexecuted pipeline roots are not included in that count. |
| `d5a6530f9`, `/tmp/agent-e-2496-managed-suite-consumer-repairs.log` | Exit 1, five failed packages, 124 failed roots, 533 root/subtest failure records. Pipeline now builds and executes; runforkexecution panics in its source-cleanup fault probe, leaving selected proofs without terminal records. |

The root/subtest counts overlap and cannot be compared as closure metrics across
the build failure, executing pipeline and package panic. The complete named
failure inventory for the latter run is committed in
`issue-2496-managed-suite-d5a6530f9.json`. It preserves every failed root and
package, not a selected list presented as exhaustive qualification.

Remaining failures cover pipeline, runforkexecution, runforkreadiness, tools and
runforkpersistence. Strict historical fork admission rejects state-only seeds;
positive component buses lack canonical root construction; older handler/future
first-materialization tests still assert retired permissions; gate/join/timer
fixtures carry stale source/owner coordinates. These are not presumed flaky or
all presumed test-only. Each positive fixture still needs an exact constructed
owner and preserved business/history/rollback/replay oracles. Source cleanup's
panic is not qualified as repaired merely because its fixture failed an earlier
admission check. No timeout, failure classification or #642 refusal is weakened.

### Executed Repairs After That Full Run

| Exact scope / receipt | Actual result |
| --- | --- |
| `d5a6530f9` test/guard migration | Manager receiver-config matrix PASS `-race -count=3` 10.885s; three compiled preview roots PASS 0.235s; dialect/aggregate source guards including hostile receipt queries PASS `-count=3` 0.032s. Exact creating-receipt lookup remains inside the target transaction. |
| ReceiverCompositionMissingStatePolicy | PASS `-count=3` 0.123s; all handler shapes refuse absent construction and exact targets cannot downgrade. |
| Business effects, `/tmp/agent-e-2496-business-effects-repaired.log` | Managed PASS `-race -count=3`, pipeline 12.324s, both stores: initialized repeated writes, ordered append and emission, reopen/readback and failed-outbox rollback. Setup explicitly uses the compiled constructor's initial fields with private component header seeding; not public construction proof. |
| Historical header/hash recovery, `/tmp/agent-e-2496-recovery-header-repaired-final.log` | Managed PASS `-race -count=3`, runforkpersistence 7.081s, both stores: fixed-revision business config/numeric kinds, complete header metadata, exact readiness hash, missing plan, internally valid wrong-run/wrong-bundle plans with their own hashes, header mismatch and orphan refusal. Every negative checks its named reason. Reduced private SQL schemas are not public/store-construction qualification. |
| Tool routing mocks | PASS `-race -count=3` 3.238s, three roots. Explicit constructed descriptors authorize existing-only targets and complete event target readback. Mock component evidence only. |
| Selected readiness missing config | PASS `-race -count=3` 6.050s; agent/activity x two path spellings require the exact metadata owner's refusal and return no projection. No reader reconstruction from fields, path or state buckets. |
| Ownership unit matrix | PASS `-race -count=3` 4.119s, nine roots: missing root/keyed/upsert construction refuses, explicit constructor candidates remain accepted, sibling/terminal/draining/contradictory evidence and stamped wrong identity still refuse. The separate declaration-bound join unit is not in this selector and remains open. |

Intermediate repaired-fixture REDs are preserved in
`/tmp/agent-e-2496-component-consumer-repairs.log` (root flow ID versus display
name) and `/tmp/agent-e-2496-recovery-header-repaired.log` (internally inconsistent
wrong-run agent identity). Their corrections do not alter production admission.
These receipts were run on the corresponding staged test trees; the committed
test blobs match the executed trees. They are not a rerun of the complete suite.

### Current Measurement and Honest Closure

Against integrated C base `1c4cce2094bdaf48dd4d5faf6dbfe33da662bc0f`, inclusive
of A integration: production Go **9399 additions / 8831 deletions, net +568**.
All production files are tracked; generated/moved/fixture production counts,
tests/spec excluded. Readiness core remains **371 lines**. The recorded soft cap
does not waive ownership, systematic-consumption or proof obligations.

The public construction/restart selector is qualified; the full managed suite
is still RED and incomplete. Complete P01-P60/C01-C20 proof, runnable selected
recovery, outstanding physical lifecycle/unknown-acknowledgment rows,
volume/sequential qualification and exact-head CI remain open. No PR, push,
review-ready partial cutover or failure-class elimination is claimed. No new
design ruling is needed for the bounded migrations completed here. Gate/watchlist
mapping stays binding; #2411/#2250 remain open.

## Public/History Qualification Checkpoint After Constructed-History Repairs

This section supersedes the prior claim that production Go is unchanged since
the public receipt at `0422d09e0`. It is unfinished implementation evidence,
not a Post-Implementation Proof Audit or a reviewable partial cutover.

### Bounded Repairs

- `2e5bb53fc`: selected operation fixtures construct the source root through the
  actual selected-store activation commit before capturing its revision. The
  obsolete entity-ID parameter is deleted from every caller. These are explicit
  component plans with empty route/arm sets, not public readiness proof.
- `6b7c7b9cd`: agent-only receiver replay and producer-state preparation consume
  the complete reconstructed state and constructed-header evidence from one
  fixed snapshot. The previous entity-ID-only lookup falsely refused valid
  constructed history and could admit imported state. The existing canonical
  decoder now owns both consumers; the authoritative selected-contract receiver
  preparation specification is updated with this commit. This is within audited
  C11/P55/P59, not a new continuation or replay capability.
- `c619db1fa`: producer projection retains that validated history through source
  preparation's existing strict child-state validator. No validator is bypassed,
  no header is inferred, and no source-to-child business field is rewritten.
- `210dd9037`: the declaration-bound root join unit's initial-entry carrier and
  live owner use the same canonical stored route. A production owners are
  unchanged.
- `175ef6fa9`: recovery adapter components provide all required header fields,
  and their private reader consumes header identity plus the exact optional
  declared-field relationship. It no longer elects an owner from field rows.
  The actual selected-store route-owner matrix is independently executed.
- `fbec4271b`: timer/startup components use their actual admitted bundle pin and
  workflow version, with the explicit staged-header flag. The alternate-pin
  refusal/recovery path remains; completion, cancellation and deadlines are not
  weakened. This prepares component construction, not served-run qualification.
- `8dde1681e`: recurring cancellation preserves canonical root entity identity
  (`run_id`) into public planning instead of seeding a contradictory random
  root ID. Its exact route, delivery, execution, revision and timer assertions
  remain unchanged.

### Receipts Actually Executed

| Receipt / executed tree | Actual result and proof boundary |
| --- | --- |
| `/tmp/agent-e-2496-selected-operation-construction.log`, `2e5bb53fc` | Managed `-race -count=1`, aggregate RED: seven operation lifetime/replacement/process-death/source-cleanup roots PASS; input execution fails at the partial historical decoder. Source cleanup reaches all 34 both-store fault cells, including panic, fail-once and persistent failure. Later production edits mean this is historical evidence, not final-head cleanup qualification. |
| `/tmp/agent-e-2496-constructed-replay-red.log` | Red-first valid root/receiver replay fails; imported history is admitted. The subsequent corrected snapshot decoder rejects import/missing/contradictory/duplicate/absent-config evidence without repair. |
| `/tmp/agent-e-2496-constructed-history-qualification.log`, `210dd9037` | Managed `-race -count=1`, aggregate RED: actual CLI receiver initialization/hash-only restart PASS on both stores (serveapp 89.561s); producer/replay unit matrix and declaration-bound join unit PASS. Input execution still loses history in the later strict validator. |
| `/tmp/agent-e-2496-history-carriage-unit.log` | Three producer/replay roots PASS `-race -count=3`, 1.105s. The producer assertion checks retained exact source entity/stage/construction evidence, not merely projected route identity. |
| `/tmp/agent-e-2496-public-history-carriage-qualification.log`, `175ef6fa9` at child execution | Managed PASS `-race -count=1`, no `--full`: selected input matrix 18 SQLite/PostgreSQL cells, 47.040s package; serveapp 75.960s, both actual CLI initialization/restart roots. Queue began at `c619db1fa`; test-only recovery edits were committed before capacity admission, then the executed tree remained unchanged. |
| Public serveapp boundary in that receipt | Race-instrumented CLI verify/serve, typed connected receiver construction, exact delivery interruption, source-directory deletion, hash-only restart, byte-identical durable route/construction evidence, exactly-once execution and terminal public RPC readback. Internal provider fixture, not paid-provider qualification. |
| `/tmp/agent-e-2496-route-components-header-owner.log` | Four pipeline adapter roots PASS `-race -count=3`, 6.390s; complete exact header, missing/ambiguous fields, malformed config, terminal timestamp and fieldless optional-field relation. Private adapter evidence only. |
| `/tmp/agent-e-2496-route-owner-components-final.log` | Actual selected-store route owner PASS on both stores `-race -count=3`, 18.067s; historical/wrong-run, ambiguous fields and terminal refusal controls retained. Raw component setup is not public construction proof. |
| `/tmp/agent-e-2496-timer-startup-source-repair.log` | Intermediate RED: timer convergence passes; startup reaches terminal gate state but completion refuses the fixture's missing staged-header flag. This is not a completion waiver. |
| `/tmp/agent-e-2496-startup-gate-header-repair.log` | Both-store startup gate recovery and terminal completion PASS `-race -count=3`, 39.074s. |
| `/tmp/agent-e-2496-timer-startup-complete-source-qualification.log`, `fbec4271b` | Managed `-race -count=3`, aggregate RED, 102.757s: seven of eight both-store roots pass all repetitions; recurring restart/cancellation reaches public planning and correctly refuses the fixture's noncanonical root identity. |
| `/tmp/agent-e-2496-recurring-timer-canonical-root.log`, exact test blob committed at `8dde1681e` | The remaining recurring fire/restore/cancel root PASS on both stores `-race -count=3`, 22.204s, with unchanged revision, exact-delivery/lifecycle and no-post-cancellation-fire assertions. This is the separate correction receipt, not a rewritten aggregate-green claim. |

Static/policy controls on the unchanged production tree also PASS:
`FlowConstructorProofPartitionRequiresBothStores` (0.006s),
`DynamicFlowLegacyExecutionAuthorityAPIsAbsent` (1.243s),
`RunForkHistoricalAdmissionConsumers` (9.588s), generated OpenRPC (0.157s), and
`PlatformSpecConstructionPrecedesOrdinaryHandlers` (0.098s). These supplement,
but never replace, actual consumer execution proof.

### Default Managed Suite

The complete requested command, `go run ./cmd/swarm-test`, ran on frozen head
`210dd9037`, waited 6m31s for capacity, and returned exit 1 at `broad-01`.
It records **two failed packages, 99 failed roots and 416 root/subtest failure
records**. Pipeline elapsed 353.542s; runforkexecution elapsed 149.429s.
The other **13 of 14 units did not execute**. The exhaustive named inventory is
`issue-2496-managed-suite-210dd9037.json`. Counts overlap and are not closure
metrics or evidence that remaining failures are flaky or test-only.

A second default managed run completed on frozen primary head `c61ad65a7`,
receipt `/tmp/agent-e-2496-managed-suite-after-history-carriage.log`, after the
history-carriage/recovery/source-pin corrections. It returned exit 1 at
`broad-01`: **two failed packages, 89 failed roots and 382 root/subtest failure
records**, with the other **13 of 14 units unexecuted**. Pipeline elapsed
302.412s; runforkexecution elapsed 145.772s. Its exhaustive named inventory is
`issue-2496-managed-suite-c61ad65a7.json`. The later recurring-root test repair
has its separate both-store race receipt; it was not edited into that frozen
checkout and does not make the full run green. Neither full run is final PR-head
qualification or complete chosen-class proof.

### Current Boundary and Remaining Work

Against the integrated C base, inclusive of A through `910c26d67`: production
Go **9438 additions / 8850 deletions, net +588**; readiness core **371 lines**.
No untracked production files, capacity bypass, increased deadline, new replay
permission, detached cleanup or compatibility constructor is introduced.

A's later handoff through `324295525` is recorded but not integrated or credited
here. Its public map verification/ruling and M34/R7 disposition remain explicit;
the two shared lifecycle APIs and A/B ownership boundaries are unchanged.

Pending same-class consumers include older handler-first construction and
state-only scenario permissions, positive private buses without root
construction, exact gate/join/timer source/owner coordinates, and selected
execution/workspace/outcome fixtures that still supply imported rather than
constructed history. They are not automatically exonerated as fixture failures:
each needs its named execution and negative/rollback/restart oracle.
Owner-complete P01-P60/C01-C20, final physical retirement/unknown-acknowledgment,
volume/sequential proof, all-green managed qualification and exact-head CI remain
open. No PR, push, closure audit or failure-class elimination is claimed.

## Constructor Consumer Retirement And Upstream Integration Checkpoint

This is additive implementation accounting, not a proof audit or closure claim.
Earlier failing receipts remain failing; later corrections do not rewrite them.

### Consumer And Historical Projection Repairs

- `7316c2f0c` preserves declared root agent scope `.` while run ID remains the
  logical root instance/entity. Source-agent setup and selected blueprints use
  the same exact route, not a run ID masquerading as a declared scope.
- `20ffbce8a` fixes an actual selected receiver failure: fork fields had projected
  lifecycle ownership, but the constructed header retained the source run's
  stage-entry bookkeeping. Header and fields now consume the same exact
  source-to-child execution correspondence. Root/static/keyed/fieldless unit
  shapes reject foreign run/path/entity evidence and preserve causal occurrence.
  The authoritative specification is amended with the production repair.
- `5f88846c4` retires handler-first positive setup in delivery, preview,
  initial-transition, timer-child, query guard, gate-route and deferred-replay
  controls. Positive components construct before ordinary execution; state-only
  and absent targets remain refusal cases. A's collection/join production was
  not changed. Its corruption fixture now corrupts the authoritative header
  accumulator rather than an obsolete shadow field row.
- `f54f237af` deletes the unused `UpdateStateCreateCompanion` variant and its
  companion-decision bit/methods. Both-store imported-state controls additionally
  execute the remaining ordinary-update variant and prove exact missing-header
  refusal with unchanged execution tables. No replacement repair permission.
- `14dc14139` binds the remaining fixtures to persisted causing events, explicit
  execution posture/mode and actual constructed source history. Repeat initial
  lifecycle preparation preserves the exact stage-entry reference and emits no
  additional timer, gate or schedule contribution. Its final serialized-state
  comparison correction still requires a separate passing receipt.

### Receipts And Aggregate Results

| Receipt / executed tree | Actual result and boundary |
| --- | --- |
| `/tmp/agent-e-2496-constructed-outcomes.log`, `bcdeefcfd` | Managed both-store selected outcomes PASS, 66 cells, 158.188s. Actual canonical source construction precedes capture; not public launch qualification. |
| `/tmp/agent-e-2496-constructed-workspace.log`, `6873afa21` | Managed workspace source construction/cleanup PASS, eight cases per store plus two mock-Claude controls, 43.826s. No paid-provider claim. |
| `/tmp/agent-e-2496-construction-lifecycle-controls.log`, `5e962adc2` | Managed both-store lifecycle/gate/timer/recovery controls PASS, 12 roots, race count3, 81.390s. |
| `/tmp/agent-e-2496-constructed-components-repaired.log`, frozen secondary `e0c8c2698` | Managed race count1 PASS, 19 pipeline roots, 175.794s. Repairs the prior component catalog mismatch; private setup remains component evidence. |
| `/tmp/agent-e-2496-constructor-handler-consumer-proofs.log` | Aggregate RED: ten roots PASS, two roots FAIL. Pipeline 66.303s RED; runforkexecution 17.179s PASS includes the actual LoopActivity matrix. The failed roots were exact-once causing-event setup and real-CAS retry; later receipts qualify their corrections separately. |
| `/tmp/agent-e-2496-root-agent-construction-proofs.log`, frozen secondary `967ac4fae` | Aggregate RED: 11 roots PASS, three FAIL. Manager 2.298s PASS, pipeline 65.072s RED, runforkexecution 24.698s RED. Header-corruption, selected receiver lifecycle correspondence and synthetic-carry setup required the later repairs above. |
| `/tmp/agent-e-2496-final-construction-consumer-proofs.log`, frozen secondary `4e0becb61` | Aggregate RED: 17 roots PASS, six FAIL. Historical entry projection 1.039s PASS; runforkexecution 23.416s RED; pipeline 69.007s RED. Actual selected receiver execution and header-accumulator corruption rejection now PASS on both stores. Exact named inventory: `issue-2496-consumer-matrix-4e0becb61.json`. |
| `/tmp/agent-e-2496-final-consumer-repair-matrix.log`, primary `14dc14139` | Aggregate RED: seven roots PASS, one FAIL. Runtime persistence 13.487s PASS, runforkexecution 29.021s PASS; pipeline 27.157s RED only at repeated initial-entry serialized-state comparison. Exact-once, first transition, composition conflict, gate route/reply context, deferred-work refusal and state-only ordinary-update refusal PASS without weakening their execution/refusal oracles. Exact inventory: `issue-2496-consumer-matrix-14dc14139.json`. |

Small focused checks separately passed: projected header/field correspondence
race count3 (1.154s), shared state-writer decision/AST guard race count3 (1.061s),
prospective publication two-variant control race count3 (1.177s), selected
receiver SQLite (2.299s), query guard PostgreSQL (1.827s), missing-header
ordinary update SQLite (0.621s), repaired gate-route SQLite approve (0.431s),
deferred timer refusal (1.777s), and synthetic-carry refusal (1.338s).
None turns an aggregate-red receipt into an aggregate-green result.

### Default Managed Suite And Landing Order

The frozen `3e06dc2b5` default managed run returned RED at broad-01: two failed
packages, **38 failed roots / 129 root-or-subtest failure records**, and 13 later
units unexecuted. `issue-2496-managed-suite-3e06dc2b5.json` retains every root.
These counts describe that earlier tree, not the remaining current failure tail.

A new default `go run ./cmd/swarm-test` is running on frozen secondary
`6656b48de` (same production/test tree as primary `14dc14139`), receipt
`/tmp/agent-e-2496-managed-suite-final-consumer-repairs.log`. No `--full`,
capacity bypass or in-place test-tree modification. The known subsequent
initial-entry comparison repair is not incorporated into that frozen run.

Current origin/master `e110bfb36` is integrated in the separate worktree at
`92e0087d2`, followed by the exact describe fixture migration at `928ea45ff`.
The single conflict preserved upstream rendering decomposition, E's constructor
projection, and A's integrated deadline/count/fan-out join vocabulary without
restoring retired window fields. Focused constructor and describe characterization
checks PASS (0.502s); no final-head suite or public qualification credit yet.

The latest binding order is **A FIRST, then E**, recorded on #1994 at comment
5935249153 and #2496 at 5935260908. This supersedes prior E-first checkpoints.
Continue independent E qualification now; integrate merged A and execute actual
P24/P37 constructor/lifecycle/member/arm/readiness proof afterward. A's separate
handoff tests and the older pinned integration do not prove those final rows.
Gate/watchlist boundaries remain unchanged. #2411/#2250 stay open. No PR, push,
merge-ready partial cutover, CI claim or chosen-class elimination is asserted.

## Canonical Header Authority Qualification Checkpoint

The frozen default run at `6656b48de` completed RED at broad-01: 173 packages
and 4,880 root tests passed; two packages and five roots failed, with 43
root/subtest failure records. The other 13 local units did not execute. The
exhaustive failed-root inventory is `issue-2496-managed-suite-6656b48de.json`.
This does not qualify the full suite or the subsequent upstream-integrated tree.

`5a32050df` corrects repeated initial-entry comparison using canonical persisted
facts: exact stage-entry identity, no additional lifecycle contributions and
unchanged serialized state remain the assertions. `02d610157` moves bookkeeping,
gate and accumulator corruption fixtures to the exact constructed header,
checks one affected row, and supplies canonical source-to-child ownership to
historical selected-header components. Missing readiness, malformed history,
foreign identity and no-repair assertions remain intact.

The five failed roots now pass in the managed both-store matrix at
`02d610157`, `-race -count=3`: pipeline 20.382s, runforkpersistence 7.670s,
no failed or skipped records. Receipt:
`/tmp/agent-e-2496-header-authority-repair-matrix.log`; exact inventory:
`issue-2496-header-authority-matrix-02d610157.json`. The malformed-shape root
retains its existing PostgreSQL-specific shape matrix; each both-store root
executes both leaves three times. This is a correction receipt, not a green
reinterpretation of the preceding default run.

A remaining private list adapter was discovered while auditing fixture
consumption. Its inner join from field rows omitted constructed fieldless
headers and read old lifecycle shadow values. The red-first list regression
reproduces omission on SQLite. The repaired adapter mirrors production's
header-first exact optional-field join and handles PostgreSQL NULL fields
explicitly, without inventing state or compatibility behavior. This is private
component evidence, not public construction qualification.
Focused repaired SQLite and PostgreSQL leaves each pass `-race -count=3`
(4.249s and 6.611s respectively). The first PostgreSQL repair attempt exposed
the NULL-field scan defect and remained RED; only the corrected leaf is credited.

Measured against current integrated master `e110bfb36`, inclusive of pinned A
work: production Go 9,461 additions / 8,884 deletions, net +577, 219 files.
Readiness core remains 371 lines. These counts are not the final PR-head delta.
The soft production cap does not waive owner-complete cutover or proof.

A's PR #2515 is now open. A-first remains the latest binding order; E's actual
P24/P37 constructor-integrated lifecycle/member/arm/readiness proofs follow
its merge. Earlier A-only receipts are not E closure. Continue independent
public and default managed qualification; no PR, push, CI green or complete
P01-P60/C01-C20 closure is claimed here. Parent/watchlist boundaries are unchanged.

Fresh public qualification on `b6202c993` returned aggregate RED: actual
serveapp receiver construction/restart roots passed both stores (75.277s),
but runforkexecution could not compile because the newly integrated W5
module fixture still passed the deleted arbitrary source-entity argument.
That fixture now consumes the real constructed-root helper; its focused
SQLite selected-module execution passes (0.722s). No W5 production owner changed.
The simultaneous default run was cancelled through its owned runner after
capacity admission, before useful broad-unit execution; it is interrupted,
not a full-suite result. Both qualification receipts require fresh corrected-head
execution. No running qualification checkout was edited to incorporate the repair.

## Corrected Public Proof And Nested Constructor Sequence Repair

The unchanged `283468b7e` public qualification is GREEN at `-race -count=1`,
with no failed or skipped records: runforkexecution 59.603s, serveapp 85.697s.
All four selected roots ran both stores: pinned-module selected fork, strict
selected-input execution evidence, public receiver initialization and public
receiver composition/source-deletion/hash-only restart. Receipt:
`/tmp/agent-e-2496-public-w5-integrated.log`. This does not qualify all P rows
or the full managed suite.

The simultaneous frozen default run exposed a further real constructor check:
`TestFlowConstructorNestedPresence` rejected optional nested record members
before reaching presence analysis. The compiled event-schema copy changed the
canonical empty `required: []` sequence to a nil slice. Strict tool-schema
admission correctly refused that nil; relaxing admission would be wrong.
The copier now retains empty-vs-nil sequence presence and independent backing
storage. Red-first `TestEventSchemaCopyPreservesRequiredSequencePresence`
reproduced the empty-to-nil defect; its empty/required/nil cells pass race3
(1.023s), preserving rejection of nil. The constructor required/optional/guarded
presence cells pass race3 (2.959s). This is a local correction receipt while
the preceding default run remains running/RED, not aggregate qualification.
No A lifecycle signature, assignment rule or supplied-field type rule changed.

Physical cleanup is being qualified independently of phase-write failures:
the existing attachment phase matrix is not, by itself, proof of fail-once or
persistent route/agent-join/timer cleanup and acknowledged abandonment.

## Exact Physical Cleanup Proof Added

`TestFlowAttachmentCleanupRetainsExactPredecessorBothStores` covers both
backends, each route/agent-join/timer/abandonment sink, and fail-once/persistent
faults through a running Manager, real selected-store rows and actual scheduler
projections. Persistent retries must preserve the same accepted predecessor,
fenced exact agent tokens and unchanged resource acquisition count. Settlement
must admit the next ordinal, never revive that predecessor; header, immutable
creating/lifecycle receipt, mutation ledger and timer identities/clocks remain
unchanged. Shutdown compares the complete joined error evidence, not merely
one matching substring. Both backend leaves are required in the proof plan.

Focused development receipts: SQLite route fail-once passed (5.686s), SQLite
persistent agent join passed under race (21.930s), and PostgreSQL persistent
abandonment passed under race (7.607s). These are selected leaves, not a complete
matrix or P02-P04/P11/P13/P34/P36 closure claim. The complete managed matrix
and further acquisition/selected failure rows still need execution.

The frozen `283468b7e` default run completed RED at broad-01: 174 packages /
4,894 roots passed; only bootverify's nested-presence root failed (two failing
leaves plus root/package, four failure records). The remaining 13 local units
did not run. Its receipt is `/tmp/agent-e-2496-default-w5-integrated.log`.
The schema sequence fix at `26574aad1` is undergoing fresh default execution
in a frozen checkout; the earlier RED is retained, not counted as GREEN.

The separate frozen core matrix exposed old root coordinates in three positive
fixtures (`RootEagerTree`, `HistoricalFieldlessSnapshot`, and
`HistoricalFieldedAndTerminalSnapshot`). Production correctly refused those
plans before persistence because immediate keyless children belong to the
run-local root, not `.` or the root's business key. The fixtures now construct
the exact run root on keyed and keyless branches. Historical readback consumes
the existing entity/route ownership projector, including exact root-parent
replacement; it no longer expects root execution identity to survive unchanged
in another run. Corruption and no-repair assertions remain.
Focused SQLite keyed root and staged fieldless history pass (0.508s, 0.542s).
The frozen core receipt remains RED; these local leaves are not aggregate proof.

The old core command ended RED at its default 10-minute aggregate test budget:
39 root executions passed, the three obsolete-root fixture roots each failed
twice, and the requested count3 matrix did not complete (47 failure records
including the package). Exhaustive root inventory:
`issue-2496-core-matrix-283468b7e.json`; receipt:
`/tmp/agent-e-2496-constructor-attachment-core-matrix.log`.
Fresh repaired core plus cleanup proof is queued on unchanged `fc7febb09`,
race count3, with a 25-minute aggregate command budget. Production timeouts
and each deterministic per-case retirement/readiness deadline are unchanged.
The old timeout and failures remain visible; this is not a timeout-based
production repair or completed qualification.

## Cleanup Evidence Versus Retained Timer Ownership

The new full cleanup matrix on frozen `fc7febb09` exposed a test-oracle
error: route/agent/timer cleanup failure precedes exact timer retirement,
so its scheduler projection intentionally retains one owned work lease.
Global quiescence therefore reports both that lease deadline and the joined
terminal cleanup failure. Shutdown after successful reclaim correctly returns
the recorded cleanup failure without the preceding quiescence deadline.
Comparing those two composite errors directly was wrong; production cleanup
and all deadlines remain unchanged.

The corrected oracle requires structured work/terminal evidence, permits
only the exact one-lease deadline at the three pre-timer-retirement sinks,
and independently checks that the scheduler still owns that timer task.
Abandonment failure cannot retain that lease. Shutdown still compares the
complete recorded cleanup evidence exactly. The corrected SQLite route
fail-once leaf passes under race (11.578s); the full frozen matrix remains
RED and is not credited as corrected-head cleanup qualification. All other
27 constructor/attachment roots passed its first repetition; remaining
repetitions and a fresh corrected cleanup matrix must finish before credit.

## Physical Timer Acquisition Must Precede Readiness

The completed frozen `fc7febb09` matrix ran all three requested repetitions
(1,423.971s). All 27 existing constructor/attachment roots passed, 81 root
executions; the new cleanup root failed each repetition solely at its incorrect
composite shutdown oracle (36 failed leaf executions plus three roots and the
package, 40 records). Exact inventory: `issue-2496-core-matrix-fc7febb09.json`.
The corrected cleanup-only run at `bd54b10dd` was interrupted while queued,
before test execution, to replace it with the expanded current-head matrix.
It is not a pass or an execution failure.

P02's actual timer acquisition probe exposed an existing O4 bypass already
inside the approved census: attempt-owned initial reconciliation called the
generic timer helper, which logs scheduler failure and returns success while
delegating to background recovery. Readiness could therefore advance despite
no installed wakeup. Red-first probing reproduced nil success with both a
missing scheduler and, on the final regression fixture, a stopped scheduler
(SQLite 0.641s). Attempt-owned attachment now calls the existing synchronous
timer owner and propagates acquisition failure to exact attachment cleanup.
Other timer recovery consumers keep their separate existing contract; no new
framework, authority, retry registry or provider behavior is introduced.

The permanent both-store regression uses a running Manager, durable constructor
commit and actual scheduler stopped after binding. It requires failure before
timers_armed/ready, exact joined cleanup and aborted disposition, no published
route, and unchanged constructed header, creating/lifecycle receipt, mutation
ledger and timer identity/clock. Both-store race count3 passes (15.806s).
The proof partition check passes (0.007s). The authoritative table contract
now explicitly disallows background recovery as attachment phase evidence.
These are focused working-tree receipts, not full repaired-head qualification
or complete P02/P13 acquisition-cut coverage.

The cleanup matrix now includes fail-once and persistent loss of the response
after a real selected-store abandonment commit. While acknowledgment remains
unknown, the exact process owner and ordinal must survive even though durable
disposition is already aborted; resource acquisition counts cannot change.
Disabling the lost-response fault permits an idempotent acknowledgment of that
same predecessor, then exactly one fresh successor. The PostgreSQL persistent
development leaf passes under race (14.919s). This is not full P11 admission/
abandonment/rollback/unknown coverage; the complete expanded matrix is pending.

## Actual Agent Publication Acquisition Cuts

The P02/P13 probe injects error, panic and caller cancellation immediately
before and after real generation-owned route publication, using a running
Manager and both selected stores. Red-first SQLite error proof showed the
existing unlaunched compensation attempted the undeclared operation kind
`start_failed`. The store rejected it, leaving durable running state and
local failed state; a later attachment retry then attempted duplicate adoption.
The panic probe separately timed out: publication unwind bypassed unlaunched
generation completion and the retained readiness cleanup correctly waited
for that never-settled generation. RED receipts:
the captured focused `sqlite/before_publish/error` execution (constraint
failure followed by duplicate adoption), and
`/tmp/agent-e-2496-agent-publication-panic-red.log`.

The existing lifecycle owner now uses its declared same-generation
`self_release` operation, retaining `start_failed` only as the transition
trigger. The prepared-publication boundary converts a publication panic to
error so the same exact-token abort, prepared-route discard and admitted-lease
settlement execute before the retirement join. Neither operation constraints
nor duplicate-adoption guards are relaxed. The authoritative publication
compensation contract is updated with this exact disposition.

The two previously failing SQLite leaves pass under race (6.631s error,
7.045s panic). The permanent regression additionally checks the committed
self-release receipt, discarded prepared authority, joined abandonment,
one successor ordinal, and unchanged construction/header/initial receipt/
mutation ledger. Caller cancellation must not abandon accepted work.
Complete both-store acquisition-cut qualification remains pending; these
focused leaves are not full P02/P13 or repaired-head qualification.

The frozen default run at `26574aad1` ended RED: its broad unit passed all
175 packages, then the separate catalog-required unit failed compiled
describe baseline checks and three public scenario anchors. There are two
failed roots and 36 failure records. Constructor projections intentionally
changed describe output; the scenario helper still authors retired
`create_entity`. Those current consumers require migration, not a restored
handler constructor. The other local proof units did not execute and the
default suite is not green.

## Expanded Attachment Receipt And Catalog Consumer Migration

The complete expanded matrix at frozen `8cfe529ce` passes managed race count3
(646.076s): all five attachment roots execute three times on SQLite/PostgreSQL,
including phase-response cuts, physical fail-once/persistent cleanup, actual
timer acquisition rejection, conditional phase progress and plan ABA. No failed
or skipped records. Inventory: `issue-2496-attachment-matrix-8cfe529ce.json`.
This qualifies that snapshot, not later agent-publication changes or every
remaining P02/P11/P13/P34/P36 manifestation.

The compiled public gate/mailbox scenario helper now uses already-constructed
keyless roots and receiver headers, not handler `create_entity`. Its human-task
and notice variants consume the same source-only blueprint; keyed variants
receive their instance key through canonical resolution rather than a handler
key write. The unchanged public stage-gate anchor passes (42.732s); the complete
scenario/topology matrix is not yet credited.

Describe's pinned fingerprint is migrated, not normalized around the new
contract. Of 45 exact public outputs measured in the failed default run,
30 change and 15 remain byte-identical. The changed outputs render the approved
canonical constructor projection; the barrier/golden outputs also reflect
the already-pinned A join vocabulary and artifact migration. Those latter
source differences were checked explicitly against the original receipt.
The old baseline ID and every old/new row are retained in
`issue-2496-describe-baseline-migration.json`; the normalization remains only
the repository absolute path. The same current renderer/artifact files are
unchanged from the measured snapshot. The compiled template-select-or-create
text leaf passes (39.036s). Complete twice-per-row qualification remains due.

The failed default receipt is retained separately as
`issue-2496-default-26574aad1.json`: two failed roots, 36 failed records and
176 package-pass events, with later local proof units unexecuted. Updating
the expected approved projection and retiring a fixture flag does not
retroactively turn this default run green.

## Agent Acquisition And Remaining C15/C20 Retirement

The frozen `0174996a8` acquisition matrix is GREEN (70.405s): actual
before/after `AgentRoutePreparation.Publish` error, panic and caller-cancel
cuts on SQLite/PostgreSQL, race count3, all 36 leaf executions and no skips.
It qualifies the same-generation failed-publication compensation and exact
joined cleanup on that snapshot, not every outstanding P02/P13 cell.

The catalog/mailbox aggregate at frozen `ae2b8b57f` is RED. All 45 compiled
describe rows (twice each) and every compiled scenario anchor pass; six
mailbox roots pass. `TestHumanTaskRealRequesterTopologyBothStores` fails
18 keyless requester leaves across both stores; its keyed leaves pass.
Actual receipts are `/tmp/agent-e-2496-agent-publication-matrix.log` and
`/tmp/agent-e-2496-catalog-mailbox-complete-qualification.log`. The requester
continuation failure remains an implementation obligation, not flaky proof.

C15/C20 re-census exposed an unfinished approved migration: source admission
still accepted retired `create_entity`, and stage/describe projections still
used it as handler eligibility authority. Source admission now rejects every
presence (including false/null/empty) through the shared node decoder on disk
and reconstructed source. `HandlerTransitionSemantic.CreateEntity` and its
source projection are deleted; ordinary stage topology and describe fan-out
consume the same nonterminal handler eligibility. Primary-entity demand
checks classify actual entity operations, not a constructor flag. Remaining
typed invalid-marker consumers only refuse unsupported inputs; prompt-tool
creation detection remains the distinct retired-tool diagnostic concept.

Positive source-only helpers and 33 YAML corpus documents are mechanically
migrated with classified functions/structured YAML nodes. A creation-only
handler becomes `{}`, never bare/null. Intentional retired-source specimens
remain negative. The duplicate positive explicit-handler-create receiver
policy is removed; constructor-owned receiver execution is the supported
case, while lexical rejection has its own presence matrix. Counter/marker
history expects an ordinary mutation after construction, not handler-owned
creation. This is not a claim that the remaining public receiver or corpus
execution matrix has passed.

Focused node grammar/disk/reconstructed parity plus unchanged catalog parity
controls pass (2.583s), ordinary-handler fan-out projection passes (0.016s),
and lexical/typed retirement plus ordinary static-primary boot controls pass
(0.195s). Earlier RED oracle/projection receipts remain separate. Full
affected-package qualification and exact public receiver proof are still due.
Changing source corpus bytes requires explicit fingerprint reconciliation,
not additional normalization of the compiled describe proof.

## Constructed Static Agent And Routing Consumer Retirement

The requester failure traced to two live static-agent construction authorities:
flow attachment used a present root route at the run UUID, while delivery
finalization independently constructed a root declaration identity. The
canonical flow-to-agent route conversion now maps root headers to the explicit
root agent route, with exact run ownership; all four attachment installation,
verification and retirement consumers use that conversion. Static declarations
remain entityless execution identities attached to a separate canonical flow
header. Root subscription/configuration projection uses the declaration scope.

Delivery planning consumes exact already-installed descriptors or the agents
declared by the exact normalized constructor plan, including recursive children.
The speculative static-recipient factory and committed-delivery static-agent
factory are deleted. Finalization can activate an admitted lifecycle but cannot
create a missing declaration. This is within approved C09/C12/C15, not a new
compatibility path. The authoritative agent-identity contract is updated.

Actually executed development receipts (not final-head qualification):

- `/tmp/agent-e-2496-human-root-declaration-matrix.log`: managed complete human
  requester matrix on both stores PASS70.998s. A later additional exact one-agent
  count assertion still requires rerun.
- `/tmp/agent-e-2496-backlog-pause-matrix.log`: real coordinator/Manager backlog,
  pending/failed/stale-in-progress pause and carrier-election fencing on both
  stores, managed race count3 PASS58.344s. The component harness explicitly
  materializes its admitted declaration before delivery; no public-constructor
  qualification is claimed from this setup.
- `/tmp/agent-e-2496-g-cancellation-integration.log`: approved one-file G handoff
  adapted to the new attempt ordinal API; exact post-CAS cancellation barrier,
  joined revised successor and waiter ordering controls, race count100
  PASS19.149s. Source commit `9d1125f8ca9eb6f86038718fd9c480294c3d848f` is
  test-observer synchronization only, not production cancellation semantics.
- `/tmp/agent-e-2496-constructed-delivery-component-controls-v2.log`: actual
  nested PostgreSQL handler execution, strict previews using loaded canonical
  control, agent-only connect refusal, localized-event/import-boundary/outbox
  controls PASS3.373s. This is component qualification, not public launch.
- `/tmp/agent-e-2496-consumer-retirement-managed.log`: aggregate RED; contracts
  PASS152.776s, bootverify PASS24.897s, authoringview PASS4.715s and canonicalrouting
  PASS37.777s; bus RED67.953s. The local remote-tracker guard was skipped because
  it is enforced by CI. The fixture migrations below cannot retroactively
  qualify this old receipt.

C20 routing fixtures now require constructed headers before ordinary delivery;
materializers no longer stand in for missing receiver construction. In-memory
fixtures declare exact headers explicitly. Store parity fixtures use actual
initial lifecycle preparation and the selected-store activation commit. Their
empty route sets are not public readiness evidence. The keyless routing fixture
has no unused uninitializable required field; its dynamic deployment variant
retains the actual constructor key. Preview fixtures load persisted state and
control rather than weakening exact delivery-target validation. Immutable
duplicate routes, hostile owner contradictions, no-repair corruption and exact
replay assertions remain binding.

The first new dual-store parity race receipt remains RED31.142s: both positive
fixtures admitted the default fixture source while construction used the exact
selected artifact. The corruption negative passed all repetitions. Explicit
source/artifact run admission is repaired; subsequent source-control probing
also caught a run-origin mismatch in the historical run-creating output
fixture. The constructed-run output now preserves existing-run origin. Full
fresh parity and affected-package qualification are still required.

No full-suite GREEN or P01-P60/C01-C20 closure claim is made. A/#2515 is still
OPEN at `4347ae046b3c4be0726f46eebd067931bcf92d2d`; genuine P24/P37 composition
follows its merge. Remaining P02/P11/P13/P34/P36 and P39 proof obligations remain
explicit. Core readiness remains 371 lines; all new constructor proofs must
still bind the actual merged final head.

## Exact Retirement And Partial Acquisition Checkpoint

This section supersedes the preceding pending route/human qualification and
current-head statements. It remains implementation evidence, not a proof audit,
chosen-class closure claim or permission to land an incomplete cutover.

- Editable worktree: `/tmp/agent-e-2496-upstream-integration`, branch
  `agent-e/2496-upstream-integration`, integrated master `e110bfb368369d701d3e7784f418addf02d2f4ec`.
- `0874c6848` adds before/after real flow-route publication cuts to the existing
  running-Manager acquisition proof: error, panic and caller cancellation.
  The named retirement response is not accepted as cleanup evidence; the
  observer requires exact durable abandonment and the actual joined fault.
- `c39c5225c` retains the complete admitted RunScopedFlowInstance throughout
  normal attachment, selected transfer and joined route/timer cleanup. The
  root's stored scope is `.`; cleanup no longer derives a scope from its run
  UUID. Typed lost-publication retirement receives the full admitted identity.
  Foreign-run transfer and stale retirement cannot affect a successor.
- The fieldless fan-out publication assertion now expects the canonical
  constructor plan's MaterializingEntity posture, not retired Existing setup.
  Header identity, absence of entity_state and all three constructed child and
  delivery/replay assertions remain. Its independent shutdown failure was real:
  route-derived scope crossed the root timer owner's persisted identity.
- The static source guard accounts for two deleted lifecycle factories. The
  persistence census separately classifies eleven already-integrated private
  join-admission SQL/TX findings and deletes two retired CompletionEvaluator
  callback findings. No raw-authority exception or A production change is added.
- `6918d3624` adds actual partial timer acquisition: one wakeup installed before
  the second acquisition, and both wakeups installed before a final error,
  panic or caller cancellation. It uses the admitted occurrence's real Begin
  interface, not a production fault hook. Both backend children are required
  in the proof plan and partition guard. Construction/lifecycle/creating-input
  and timer evidence must remain unchanged across a fresh failed-attempt retry.
- `e4f97ecaa` deletes one stale filesystem primitive manifest row and gives the
  revised-initial-timer mode component its real owned scheduler. PrepareStartup
  retains that component's explicit fire boundary; mock-mode and physical
  registration assertions remain, with scheduler Stop/Wait cleanup.
- The new before-second/panic cell found a real deadlock: registerProjection
  held its mutex across occurrence acquisition without panic-safe unlock.
  `f3fa31d58` preserves atomic task admission/replacement but defers unlocking
  that existing section. Panic reaches the existing attachment cleanup owner,
  which can now join the already-installed wakeup. The authoritative readiness
  contract states this obligation; no detached cleanup or new framework.

### Completed Receipts

| Executed head / receipt | Actual result and boundary |
| --- | --- |
| `0874c6848`, `/tmp/agent-e-2496-route-and-agent-acquisition-matrix-pinned.log` | Managed race count3 PASS115.231s; both roots, 72 acquisition leaf executions, no failures/skips. Real running Manager and selected stores; before/after agent and flow route acquisition, error/panic/caller cancellation, exact joined compensation and immutable construction evidence. |
| `c39c5225c`, `/tmp/agent-e-2496-exact-root-retirement-matrix.log` | Managed race count3 PASS in all four packages: catalog37.596s, Manager28.847s, bus1.064s, runforkexecution1.040s. Twenty-seven root executions, no failures/skips. Actual fieldless fan-out on both stores, exact root/static/keyed transfer, batched joined retirement, publication stale/foreign-run refusal and retired-authority guards. |
| `264244cfe`, `/tmp/agent-e-2496-constructed-public-route-matrix.log` | Aggregate RED retained: bus33.545s and serveapp527.905s PASS race count3; catalog32.575s RED on all three fieldless-root repetitions. Scalar/cross-flow/no-repair bus rows and the complete human requester topology pass, including the additional exact one-agent count. Human proof is real authenticated HTTP/WebSocket with retained mock lifecycle, not public-launcher/real-provider qualification. |
| `264244cfe`, `/tmp/agent-e-2496-constructed-consumers-complete.log` | Complete affected bus/canonicalrouting packages PASS race count1, 43.384s/29.868s, 634 root passes. The remote tracker-state guard is locally skipped and remains CI-enforced; no executable routing proof was skipped. |
| `c39c5225c`, `/tmp/agent-e-2496-default-c39c5225c.log` | Default managed run RED: two failed packages/roots, four test failure records; 173 packages and 4897 roots PASS. Five existing environment/tracker skips. Broad-01 stops on the manifest and scheduler-less component; the other thirteen units did not execute. |
| `6918d3624`, `/tmp/agent-e-2496-partial-timer-acquisition-matrix.log` | RED diagnostic, not a completed matrix: before-second/error passes, before-second/panic deadlocks in retirement on the held scheduler mutex. SIGQUIT of E's exact test PID captures the contested stack and ends the run; later cells/repetitions did not execute. |
| Corrected development controls | Manifest PASS0.476s; revised-initial-timer SQLite mode PASS race count3, 3.270s; panic-after-complete-projection SQLite control PASS race count3, 15.210s; repaired before-second/panic SQLite control PASS race count3, 11.244s. These are small controls, not the complete dual-store matrix. |

The queued e4f97ecaa default run was explicitly canceled before execution to
include the real scheduler repair. No whole-suite credit is assigned to it.
Fresh timer/startup/generic-schedule race count3, default managed qualification
and unchanged P39 sequential/1362-row race count3 proof are queued/running on
frozen `f3fa31d58`; their outcome is not yet claimed.

Measured at f3fa31d58 against integrated master e110bfb36, inclusive of the
previously integrated A contribution: production Go **9693 additions / 9244
deletions, net +449**. Generated/moved and fixture production lines count; tests
and spec are excluded. All production files are tracked. Readiness core is
**371 lines**, below the binding 500-line limit; the load-bound test passed in
the earlier named receipt and is not replaced by this size calculation.

A-first integration remains binding. #2515 is still OPEN at
`593942b5bc1c8310578ceb4e83b0bdd46c7dab22`; P24/P37 must use its actual merged
constructor/member/arm contribution. P11 admission uncertainty, remaining
selected acquisition/transfer/terminal-sink cells and final-head supported
qualification remain explicit obligations. No PR, full-suite GREEN,
exact-head CI or complete P01-P60/C01-C20 closure is claimed. #2411/#2250 remain
open under the existing approved Gate/watchlist mapping.

## Merged A Integration And Current Consumer Checkpoint

This supersedes the old A-pending and measured-current-head statements above,
not their RED evidence. A/#2515 merged at
8466039d008a9daa1a948e8c00dd48d732a2650f and is integrated at74016e7cd.
PrepareInitialEntryLifecycle and its Instance/Lifecycle contribution remain
unchanged. Current implementation/proof checkpoint is6e4af4233; no PR or
chosen-class closure is claimed. Canonical watchlist receipt is swarm-docs
af82fee, produced from an isolated clean worktree; the shared dirty docs
checkout was not modified.

### Actual Receipts

| Executed tree / receipt | Result and boundary |
| --- | --- |
| Frozen a82888074, /tmp/agent-e-2496-public-volume-correct-paths.log | Managed supported-surface supplement PASS: both-store progressive-presence public serve/restart; unchanged sequential/public readback56.18s; unchanged1362 fan-out settlement/public readback/hash-only restart200.94s total. PostgreSQL81.56s, settlement77.255s, unchanged deadline. No race flag. The supplied describe root did not exist and receives no proof credit. |
| f2fe842b9 writer/terminal controls, /tmp/agent-e-2496-writer-terminal-controls.log | Both-store race count3 PASS62.719s. Actual constructor, compiled transition and claimed delivery; rollback, exact settlement and committed terminal truth preserved. |
| Frozen f2fe842b9, /tmp/agent-e-2496-default-f2fe842b9.log | Default managed RED after the complete CLI package347.459s. Only failing test root is TestCatalogRequiredVerifyAll: one retired create_entity loader fixture retained dialect_compliance as its expected category. Later planned units did not execute. The intentional nested child-failure harness output is not another failed root. |
| Category correction9a5d6878a, /tmp/agent-e-2496-catalog-retirement-proof.log | Complete managed TestCatalogRequiredVerifyAll PASS12.660s, all158 catalog cases. A previous single-subtest invocation passed the leaf but failed the parent's158-case inventory requirement and receives no complete-root credit. |
| 43b5ab564, /tmp/agent-e-2496-historical-agent-generation.log | Eight historical generation cells on both stores PASS8.897s with actual O2 source construction. Current/historical, no-role, external and missing/foreign/unknown revision refusals retain their original meaning. Not served-execution proof. |
| Constructor-backed activity fixture9ab42140a, /tmp/agent-e-2496-activity-constructor-source-control.log | Rehomed-child flow/type refusal control PASS3.727s on both stores. Root construction requires its actual complete two-entity tree; static producer construction requires one. No raw state/header fabrication. |
| Standing eligibility3836036a4, focused runtime and bootverify receipts | Disk/reconstructed keyless-root acceptance, keyed-root refusal and unassigned-initial-reader refusal PASS race count3, runtime3.649s and bootverify3.885s. The predicates consume O6; contained collection cardinality is untouched. |
| Standing public journey3836036a4, /tmp/agent-e-2496-root-provider-after-eligibility.log | Managed RED2.598s, both stores. It passes declaration validation then refuses recursive construction because the standing root is still planned at the authored static coordinate rather than the selected generation run. No webhook execution or public-journey success credit. The preceding child-only singleton RED2.197s is retained. |
| Expanded header consumption57ec76c65, /tmp/agent-e-2496-header-consumption-expanded-correct-source.log | RED22.687s, eight named rows on each store. Absence, zero and original-loop controls pass. Fieldless source fails the entity_state-only lookup; missing/corrupt/wrong-flow/wrong-type child headers are ignored. Complete source/child/application snapshots remain unchanged. All16 leaves are required in the proof plan; partition control PASS0.008s. |
| Canonical contention fixture6e4af4233 | All four uncontended ordinary/selected backend controls pass in the earlier8.141s receipt. The contested writer remains RED1.726s before the real SQL barrier: it attempts pending-to-done with an unchanged pending construction StageEntry. It needs an actually admitted compiled transition, not discarded entry evidence or a fabricated occurrence. The full contention matrix is not qualified. |

P11's actual Manager lost-Begin-response probe remains RED: a committed planned
attempt loses its retained process owner. Existing standalone store replay is
not acknowledgment-resolution proof. The exact non-executable request/readback
contract remains requested in issuecomment5941372820; no generic retry owner,
guessed ordinal or executable authority from an unacknowledged result is added.
Four selected header readers and the five parent sibling families are recorded
in issuecomment5942328365, with the standing coordinate in5942515887. No broader
parent absorption, A/B ownership change or compatibility reader is implied.

Measured production Go delta against merged8466039d, including generated,
moved and production fixture lines, at current6e4af4233:162 files,
5986 additions /7381 deletions, net-1395. The user's soft-cap clarification
does not hide the gross additions. The purchased invariant remains one immutable
constructor plus an exact selected-store attachment attempt/phase authority.
Readiness core is381 lines; <=2 public loads/pass still requires execution proof,
not a count of call sites.

Default managed qualification on frozen3836036a4 completed RED: broad-01,
catalog-required-inventory and complete158-case verification passed, then
local-catalog-smoke failed5.099s. The remaining ten units did not execute.
Subsequent changes are not assigned that frozen run's exact-head credit.
Full GREEN, hosted CI GREEN, formal
Post-Implementation Proof Audit and review-ready PR remain outstanding.

## Exact Receiver And Configured-Agent Ownership Checkpoint

The consumer repair followingac6f6939a resolves canonical root receipt scope
and carries actual configured agent entity ownership in readiness plan version5.
Static declaration agents remain entityless; the flow constructor's header
does not confer agent ownership of that entity. Routing, both selected-store
adapters, normal/selected producers and Manager verification consume the hashed
expectation. Changed/foreign entity ownership is refused without store mutation.
The authoritative spec is updated with these same invariants and retired plan
shapes are unsupported, not read by a compatibility path.

Catalog and lifecycle store fixtures now use actual O2 construction. Exact
scoped identity replaces short-name counting; original duplicate publication,
restart, source isolation, byte-identical projection and no-repair oracles stay.
A's otherwise dead standingActivatedFlow helper is removed; the eligibility
proof directly consumes the canonical constructor and does not revive boot code.
No A collection/join file or B generation API changed.

Current measured production Go versus merged8466039d, with --no-renames and all
generated/moved/production fixtures counted:162 files,6003 additions/7410
deletions, net-1407. Gross additions remain disclosed under the user's soft cap.
Readiness core381 lines. The actual <=2 public loads/pass execution guard passes.

Actual receipts and retained earlier REDs are enumerated in
issue-2496-current-qualification-update.md. Catalog/receiver non-race matrix
PASS49 records; exact ownership/presence/receipt matrix PASS85 records. Combined
race count3 remains aggregate RED because two older lifecycle fixture roots
inserted headers without entity IDs; catalog and the other six packages pass.
Corrected lifecycle race count3 PASS48.254s,15 root executions,46 pass records,
no skips, including both-store foreign-agent-entity and preparation/takeover
refusals. These are development-tree receipts, not final-head hosted CI or
full-suite qualification. Fresh default managed qualification remains required.

P11 acknowledgment resolution, C10/C11 header/parent consumer dispositions and
standing-root generation binding remain the three recorded authority requests.
The contention writer remains unfinished rather than supplied with fabricated
entry evidence or weakened frontier/replay assertions. No PR readiness,
Post-Implementation Proof Audit, parent closure or chosen-class elimination is
claimed at this checkpoint.
