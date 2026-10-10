# Entry-Golden Current Source Repair

Part of2542; related to2151. Binding ruling:
https://github.com/division-sh/swarm/pull/2604#issuecomment-6095675391.
This is a bounded permanent-proof consumer repair, not a new migration family.

The original broad-12 failure is reproduced:36 disappeared source rows, including
the deleted authored-rule diagnostic. These accumulated selector changes already
exist at5906 and are not caused by history compaction. The root's historical
entry/order/final decisions remain independent and unchanged. All566 original
rows retain their file/source/flow/function/literal identity and semantic tuple;
only36 explicit current_selector records are added. The original intent and all
one-time rewrite ledgers remain byte-identical. No blanket baseline capture or
candidate-derived expected values.

The existing rewrite-stages-2566 owner now separates immutable decision identity
from movable current coordinates. Checking reads exactly one declared live file,
function and ordinal, with no search or historical fallback. Extraction reuses
reviewed history and carries only matching unchanged decisions' explicit current
selectors; unknown, duplicate, corrupt or unmatched bindings fail. Destination
validation refuses missing and ambiguous functions/literals. The two closed
constructor cases actually execute their existing canonicalrouting constructors
and compare the materialized schema to the declared source before checking the
original semantic tuple. No general resolver, new source API or runtime layer.

All36 original/current schema bodies compare byte-identical against reachable
master055bbbaba; the named source-correspondence proof logs both SHA256 values
for every row and checks both typed semantic tuples. This includes the preserved
schema prefix of the guard-reachability composition: the entire generator body
is unchanged except native fixture propagation, so its existing parameterized
execution proofs carry rather than selecting a convenient unrelated literal.
Likewise the emission helper's event-schema parameter never changes its queued
stage declaration. No current runtime/source bytes change in this repair.

## Complete Finite Mapping

Each row below retains its independently reviewed tuple and has identical old/new
schema bytes. The three patterns are same-file function/ordinal movement (31),
shared owner movement (three, including the retired diagnostic), and actual
closed constructor materialization (two).

| Original file/function/literal | Exact current source | Entry; declaration order; finals | Proof/disposition |
| --- | --- | --- | --- |
| internal/runtime/pipeline/authored_rule_receiver_retry_test.go:TestAuthoredRuleReceiverPreparationRetryBothStores/literal-6 | internal/runtime/pipeline/authored_rule_receiver_retry_test.go:VerifyNativeAuthoredRuleReceiverPreparationRetryBothStoresForTest/literal-8 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/authored_selection_changed_state_retry_test.go:TestAuthoredSelectionRetryReloadsCurrentStateBothStores/literal-12 | internal/runtime/pipeline/authored_selection_changed_state_retry_test.go:VerifyNativeAuthoredSelectionRetryReloadsCurrentStateBothStoresForTest/literal-12 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/composition_receiver_business_test.go:TestCompositionReceiverInitializationAndRepeatedBusinessWritesBothStores/literal-4 | internal/runtime/pipeline/composition_receiver_business_test.go:VerifyCompositionReceiverInitializationAndRepeatedBusinessWritesBothStoresForTest/literal-2 | active; active,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/engine_adapter_test.go:TestPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowID/literal-2 | internal/runtime/pipeline/engine_adapter_test.go:VerifyPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowIDForTest/literal-2 | ready; ready; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/engine_adapter_test.go:TestPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowID/literal-4 | internal/runtime/pipeline/engine_adapter_test.go:VerifyPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowIDForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/engine_adapter_test.go:TestPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaults/literal-4 | internal/runtime/pipeline/engine_adapter_test.go:VerifyPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaultsForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/entity_complete_snapshot_test.go:TestEntityLastFieldClearAndColdReloadBothStores/literal-4 | internal/runtime/pipeline/entity_complete_snapshot_test.go:VerifyEntityLastFieldClearAndColdReloadBothStoresForTest/literal-2 | active; active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/entity_complete_snapshot_test.go:TestSelectedHandlerSparsePresenceWriteAndEmitBothStores/literal-4 | internal/runtime/pipeline/entity_complete_snapshot_test.go:VerifySelectedHandlerSparsePresenceWriteAndEmitBothStoresForTest/literal-2 | active; active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/entity_complete_snapshot_test.go:TestSelectedHandlerSparseEqualityMutationsBothStores/literal-12 | internal/runtime/pipeline/entity_complete_snapshot_test.go:VerifySelectedHandlerSparseEqualityMutationsBothStoresForTest/literal-10 | active; active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_committed_cleanup_test.go:TestHandlerCommittedCleanupErrorRetainsExactOutcomeBothStores/literal-2 | internal/runtime/pipeline/handler_committed_cleanup_test.go:VerifyNativeHandlerCommittedCleanupErrorRetainsExactOutcomeBothStoresForTest/literal-4 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_committed_cleanup_test.go:TestGuardRejectedSettlementSurvivesContinuationCleanupFailureBothStores/literal-2 | internal/runtime/pipeline/handler_committed_cleanup_test.go:VerifyNativeGuardRejectedSettlementSurvivesContinuationCleanupFailureBothStoresForTest/literal-4 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmit/literal-4 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:newEmitPersistenceTestCoordinator/literal-2 | internal/runtime/pipeline/constructor_handler_native_fixture_test.go:nativeEmitPersistenceBundleForTest/literal-2 | researching; researching,mvp_speccing; none | Shared native source owner, original consumers retained |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReads/literal-4 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReadsForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContext/literal-4 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyNativeExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContextForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentity/literal-4 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentityForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValue/literal-4 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyNativeExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValueForTest/literal-4 | queued; queued; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:TestExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntity/literal-2 | internal/runtime/pipeline/handler_engine_transaction_test.go:VerifyNativeExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntityForTest/literal-4 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_engine_transaction_test.go:declarativeEmitContractTestBundleWithEntry/literal-9 | internal/runtime/pipeline/workflow_handler_native_fixture_test.go:declarativeEmitContractSourceForTest/literal-2 | queued; queued; none | Shared native source owner, original consumers retained |
| internal/runtime/pipeline/handler_entity_requirement_execution_test.go:TestEntitylessNodeContractEmissionDoesNotMaterializeWorkflowStateOnSQLiteAndPostgres/literal-2 | internal/runtime/pipeline/handler_entity_requirement_execution_test.go:VerifyNativeEntitylessNodeContractEmissionDoesNotMaterializeWorkflowStateOnSQLiteAndPostgresForTest/literal-4 | active; active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/handler_entity_requirement_execution_test.go:TestEntitylessPayloadGuardDoesNotPublishOrMaterializeOnBothStores/literal-2 | internal/runtime/pipeline/handler_entity_requirement_execution_test.go:VerifyNativeEntitylessPayloadGuardDoesNotPublishOrMaterializeOnBothStoresForTest/literal-4 | active; active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/reviewer_2460_finalization_test.go:TestCommittedAttemptFailureNotificationKeepsRetryContinuationBothStores/literal-2 | internal/runtime/pipeline/reviewer_2460_finalization_test.go:VerifyNativeCommittedAttemptFailureNotificationKeepsRetryContinuationBothStoresForTest/literal-4 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/reviewer_2460_finalization_test.go:TestReview2460HandlerCompletedPanicReleasesCommittedContinuationBothStores/literal-2 | internal/runtime/pipeline/reviewer_2460_finalization_test.go:VerifyNativeReview2460HandlerCompletedPanicReleasesCommittedContinuationBothStoresForTest/literal-4 | queued; queued,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/supported_handler_effects_test.go:TestSupportedHandlerAppendEmitReadbackAndRollbackBothStores/literal-8 | internal/runtime/pipeline/supported_handler_effects_test.go:VerifySupportedHandlerAppendEmitReadbackAndRollbackBothStoresForTest/literal-8 | active; active,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/testdata/diagnostics/authored_rule_receiver_retry_test.go:TestAuthoredRuleReceiverPreparationRetryDiagnosticBothStores/literal-6 | internal/runtime/pipeline/authored_rule_receiver_retry_test.go:VerifyNativeAuthoredRuleReceiverPreparationRetryBothStoresForTest/literal-8 | queued; queued,done; done | Duplicate diagnostic retired; surviving native retry consumer |
| internal/runtime/pipeline/workflow_compiled_adapter_test.go:TestCompiledTransitionPreviewExecutionAgreementOnBothStores/literal-88 | internal/runtime/pipeline/workflow_compiled_adapter_test.go:VerifyNativeCompiledTransitionPreviewExecutionAgreementOnBothStoresForTest/literal-88 | ready; ready,drafting,review,escaped; escaped | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_compiled_lifecycle_evidence_test.go:TestAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStores/literal-37 | internal/runtime/pipeline/workflow_compiled_lifecycle_evidence_test.go:VerifyAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStoresForTest/literal-43 | queued; queued,active; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_expression_flow_identity_test.go:TestPipelineExpressionPreservesExactExecutionFlowOnBothStores/literal-2 | internal/runtime/pipeline/workflow_expression_flow_identity_test.go:VerifyPipelineExpressionPreservesExactExecutionFlowOnBothStoresForTest/literal-2 | ready; ready; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_expression_flow_identity_test.go:TestPipelineExpressionPreservesExactExecutionFlowOnBothStores/literal-8 | internal/runtime/pipeline/workflow_expression_flow_identity_test.go:VerifyPipelineExpressionPreservesExactExecutionFlowOnBothStoresForTest/literal-8 | ready; ready; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_first_event_materialization_test.go:TestNodeContractFirstEventTransitionsFromCanonicalInitialStateOnBothStores/literal-2 | internal/runtime/pipeline/workflow_first_event_materialization_test.go:VerifyNativeNodeContractFirstEventTransitionsFromCanonicalInitialStateOnBothStoresForTest/literal-2 | waiting; waiting,done; done | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_guard_cause_hostility_test.go:TestPipelineRejectsFabricatedGuardCauseOnBothStores/literal-2 | internal/runtime/pipeline/workflow_guard_cause_hostility_test.go:VerifyNativePipelineRejectsFabricatedGuardCauseOnBothStoresForTest/literal-2 | ready; ready,done,killed; done,killed | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_guard_reachability_proof_test.go:TestGuardTerminationVerifiedExecutionAndRestartBothStores/literal-39 | internal/runtime/pipeline/workflow_guard_reachability_proof_test.go:VerifyNativeGuardTerminationVerifiedExecutionAndRestartBothStoresForTest/literal-39 | ready; ready; none | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_instance_activation_test.go:TestTemplateInstanceSystemNodeDeliveryUsesExactLocalHandlerKey/literal-4 | internal/runtime/pipeline/workflow_instance_activation_test.go:VerifyNativeTemplateInstanceSystemNodeDeliveryUsesExactLocalHandlerKeyForTest/literal-6 | initializing; initializing,ready; ready | Exact native proof rename/ordinal, unchanged source |
| internal/runtime/pipeline/workflow_node_delivery_authority_test.go:TestWorkflowNodeRetryWaitSurvivesHeartbeatSettlementParity/literal-2 | internal/runtime/testfixtures/canonicalrouting/pipeline_delivery_authority.go:CopyPipelineDeliveryRetry/literal-2 | queued; queued,done; done | Executed closed constructor, identical materialized schema |
| internal/runtime/pipeline/workflow_node_delivery_authority_test.go:newDeliveryAuthorityCoordinator/literal-2 | internal/runtime/testfixtures/canonicalrouting/pipeline_delivery_authority.go:CopyPipelineDeliveryAuthority/literal-2 | queued; queued,done; done | Executed closed constructor, identical materialized schema |
| internal/runtime/pipeline/workflow_transition_hydration_proof_test.go:TestCompiledTransitionPersistedCoordinatesAndTimerCauseOnBothStores/literal-2 | internal/runtime/pipeline/workflow_transition_hydration_proof_test.go:VerifyNativeCompiledTransitionPersistedCoordinatesAndTimerCauseOnBothStoresForTest/literal-2 | ready; ready,waiting,done; done | Exact native proof rename/ordinal, unchanged source |

The diagnostic deletion is visible in master055bbbaba..d99a77e6b. Its original
schema and three no-fault/fresh/recovered retry cases are retained in the native
source. A named additional control requires the old file to remain absent and
the surviving public test to directly invoke that exact native proof using the
original pipelineDeliveryNativeFixture. The historical row is neither removed
nor redirected to a Git snapshot or an equal-looking unrelated flow.

## Consumption, Counterexamples And Qualification

The current-selector data has one owner: entries.json. Both permanent checking
and the existing prepare-entry-golden extraction consume it; no duplicate
inventory. Stripping only current_selector produces the exact predecessor corpus,
and two normal extraction executions produce the same bytes/checksum:
59042d520ebae717e9e3642dacc9005234d0b16061b85364458a96fbd5503176.
All historical intent/equivalence/rewrite records remain unchanged.

Negative controls reject missing file/function/ordinal, ambiguous declarations
and original identity, duplicate/unmatched relocations, altered historical entry,
order, final membership or identity, unsupported constructor dispatch and changed
actual stage facts. The existing sorted-but-reachable refusal is unchanged.
Per-original-file coverage remains exact; deleting both a golden and intent row
cannot satisfy the independently retained566-decision count.

The complete owner package is run under race at the final source, including all
566 typed-corpus cases, generated/materialized/catalog-retained variants, incoming
sources, finite release/scenario closure, original hostile and new selector
controls. The actual current hosted plan binds the owner to broad-12 and the
canonical source owner to broad-04; its48 packages are inspected, not silently
run as duplicate tiers. Twenty-one affected generated/golden/source/registry
roots in canonicalrouting, runforkrevision and testtiming pass, with their exact
names retained in entry-broad-generated-guards.jsonl. cmd/swarm-openrpc-gen has
no test files; no additional Golden/Generated/Corpus/Inventory roots exist in
cmd/swarm-test-timing or cmd/swarm-unused. Existing unaffected receipts carry.

Fresh persistence ratchet, immutable policy destination, helper fingerprint and
all16 mandatory native families pass with unchanged54777 findings/38919 total raw
operation sites,10698 debt/7754 raw debt sites/67 uncertainties. No new debt or
collector/baseline allowance. Native both-store receipts carry: only the proof
coordinates change, not source materialization, execution, assertion or lifetime.

The supplemental one-time -prove command remains RED on Telegram ingress source
hash drift. The current file is byte-identical to master055bbbaba and does not
match the historical rewrite output; prove.go explicitly describes that receipt
as independent of permanent goldens, not a recurring freeze on legitimate later
fixture bytes. This is not a passing proof or a reason to refresh intent/hashes.
The normal finite/idempotence/typed/permanent corpus tests pass. Its failure/log
is retained and disclosed separately from required current package qualification.

Hosted2a0543542 is FAILURE: broad-12 is the only real proof red; summary and timing
are downstream incomplete evidence after cancellation. Timing reports INCOMPLETE,
not an established elapsed-budget regression. No budget, timeout, skip, selector,
CI-Units or workload change. Hosted full remains required on the replacement
signed head; no duplicate local tier. Final complexity/unused/formatting and
SHA-bound proof receipts are reported on the PR. Six compacted historical groups
and their reachable input pins are preserved; this repair adds one signed commit.

Architecture disposition: promote only the explicit historical/current selector
separation at the existing golden owner. Existing2542 source-construction and
inventory watchlist/checklist capture it; no new issue. Frozen cohorts remain the
closure boundary, not all2151/2542 debt. Final strict/fork/integrated obligations
and earlier unwaived aggregate reds remain open and unchanged.
