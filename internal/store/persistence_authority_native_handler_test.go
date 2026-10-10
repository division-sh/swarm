package store_test

import (
	"strings"
	"testing"
)

func verifyNativeHandlerFixturesDoNotReceiveRawAuthority(t *testing.T, findings []authorityFinding) {
	for _, finding := range findings {
		if nativeHandlerAuthority(finding) {
			t.Errorf("native handler cohort regained raw authority: %s", finding.registryLine())
		}
	}
}

func nativeHandlerAuthority(finding authorityFinding) bool {
	if !finding.RawSQL {
		return false
	}
	switch finding.File {
	case "internal/runtime/pipeline/workflow_handler_native_fixture_test.go",
		"internal/runtime/pipeline/workflow_handler_native_external_test.go",
		"internal/runtime/pipeline/engine_adapter_test.go",
		"internal/runtime/pipeline/delivery_target_application_test.go",
		"internal/runtime/pipeline/workflow_transition_mutation_test.go",
		"internal/runtime/pipeline/workflow_transition_event_authority_test.go",
		"internal/runtime/pipeline/workflow_closed_owner_native_fixture_test.go",
		"internal/runtime/pipeline/workflow_closed_owner_native_external_test.go",
		"internal/runtime/pipeline/workflow_missing_header_native_external_test.go",
		"internal/runtime/pipeline/entity_complete_snapshot_test.go",
		"internal/runtime/pipeline/composition_receiver_business_test.go",
		"internal/runtime/pipeline/workflow_expression_flow_identity_test.go":
		return true
	case "internal/runtime/pipeline/handler_entity_requirement_execution_test.go":
		return finding.Enclosing == "executeExistingOwnerBehavior" ||
			finding.Enclosing == "TestExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgres" ||
			finding.Enclosing == "VerifyExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgresForTest"
	case "internal/runtime/pipeline/workflow_gate_recovery_external_test.go":
		switch finding.Enclosing {
		case "insertGateRecoveryRun", "gateRecoveryPipelineReceiptCount", "assertGateRecoveryProcessedReceipt",
			"assertGateRecoveryErrorReceipt", "assertGateRecoveryObligationStatus", "assertProposedEffectProofCounts", "loadProposedEffectProofRequest":
			return true
		}
	case "internal/runtime/pipeline/workflow_compiled_lifecycle_evidence_test.go":
		return finding.Enclosing == "TestAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStores" ||
			finding.Enclosing == "VerifyAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStoresForTest"
	case "internal/runtime/tools/entity_sparse_mutation_test.go":
		return true
	case "internal/runtime/diagnostics_test.go",
		"internal/runtime/runtime_recovery_diagnostics_test.go",
		"internal/runtime/runtime_recovery_fan_out_fixture_test.go",
		"internal/runtime/runtime_log_native_fixture_test.go",
		"internal/runtime/runtime_log_native_external_test.go":
		return true
	case "internal/runtime/manager/work_lifetime_test_helpers_test.go":
		return finding.Enclosing == "newTestAgentManagerWithOptions"
	case "internal/runtime/manager/unit_constructor_delivery_authority_test.go":
		return true
	case "internal/runtime/manager/delivery_lifecycle_test_helpers_test.go",
		"internal/runtime/manager/delivery_native_fixture_bridge_test.go",
		"internal/runtime/manager/delivery_native_owner_external_test.go",
		"internal/runtime/manager/delivery_native_selected_external_test.go",
		"internal/runtime/manager/delivery_native_consumers_external_test.go":
		return true
	case "internal/runtime/manager/delivery_claim_ack_test.go",
		"internal/runtime/manager/receipt_settlement_outcome_test.go",
		"internal/runtime/manager/receipts_test.go",
		"internal/runtime/manager/failure_test.go",
		"internal/runtime/manager/lifecycle_coordinator_test.go",
		"internal/runtime/manager/execution_projection_test.go":
		return strings.HasPrefix(finding.Enclosing, "ProveNative")
	case "internal/runtime/tools/executor_sqlite_persistence_test.go":
		return finding.Enclosing == "TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores" ||
			finding.Enclosing == "TestEntityTools_SQLiteBackendNeutralEntityPersistence" ||
			finding.Enclosing == "TestRoleScopedEntityTools_SQLiteCurrentEntityPersistence" ||
			finding.Enclosing == "newPostgresHumanTaskToolStoreForTest"
	case "internal/runtime/tools/executor_http_settlement_ack_test.go":
		return true
	case "internal/runtime/tools/entity_field_ack_response_test.go":
		return true
	case "internal/runtime/pipeline/handler_engine_transaction_test.go":
		switch finding.Enclosing {
		case "TestExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayload", "VerifyExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayloadForTest",
			"TestExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmit", "VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest",
			"TestExecuteNodeContractHandlerDefersCommittedEmissions", "VerifyExecuteNodeContractHandlerDefersCommittedEmissionsForTest",
			"TestExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEvent", "VerifyExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEventForTest",
			"TestExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrder", "VerifyExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrderForTest",
			"TestExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEvent", "VerifyExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEventForTest",
			"TestExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSource", "VerifyExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSourceForTest",
			"TestExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelope", "VerifyExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelopeForTest",
			"TestExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnly", "VerifyExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnlyForTest",
			"TestExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSites", "VerifyExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSitesForTest":
			return true
		}
	case "internal/runtime/pipeline/workflow_node_delivery_authority_test.go":
		return finding.Enclosing == "TestPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrier" ||
			finding.Enclosing == "VerifyPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrierForTest" ||
			finding.Enclosing == "TestPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthority" ||
			finding.Enclosing == "VerifyPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthorityForTest"
	case "internal/runtime/pipeline/activity_engine_test.go":
		return finding.Enclosing == "TestPipelineActivityIntentWriterDoesNotUseAmbientPostCommitAuthority" ||
			finding.Enclosing == "TestPipelineActivityIntentWriterEmitsImmediateDiagnosticWithoutPublication"
	case "internal/runtime/pipeline/delivery_continuation_signal_registration_test.go":
		return true
	case "internal/serveapp/served_delivery_quiescence_reader_test.go":
		return true
	case "internal/serveapp/main_runtime_test.go":
		return finding.Enclosing == "waitServedRunDeliveryQuiescence"
	}
	return false
}

func TestNativeHandlerGuardRejectsRawParametersCallbacksAndUnlistedSnapshotSiblings(t *testing.T) {
	for _, probe := range []struct{ file, source string }{
		{"internal/runtime/manager/delivery_lifecycle_test_helpers_test.go", `package fixture;import "database/sql";func unlistedManagerDeliverySibling(db *sql.DB){_ = db}`},
		{"internal/runtime/manager/delivery_native_fixture_bridge_test.go", `package fixture;import("context";"database/sql");func unlistedNativeManagerBridge(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/manager/delivery_native_selected_external_test.go", `package fixture;import "database/sql";func unlistedSelectedManagerSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/manager/receipts_test.go", `package fixture;import "database/sql";func ProveNativeManagerDeliverySibling(db *sql.DB){_ = db}`},
		{"internal/serveapp/main_runtime_test.go", `package fixture;import "database/sql";func waitServedRunDeliveryQuiescence(db *sql.DB){_ = db}`},
		{"internal/serveapp/served_delivery_quiescence_reader_test.go", `package fixture;import "database/sql";func unlistedDeliverySummarySibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_continuation_signal_registration_test.go", `package fixture;import "database/sql";func unlistedContinuationSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/activity_engine_test.go", `package fixture;import "database/sql";func TestPipelineActivityIntentWriterEmitsImmediateDiagnosticWithoutPublication(db *sql.DB){_ = db}`},
		{"internal/runtime/manager/work_lifetime_test_helpers_test.go", `package fixture;import "database/sql";func newTestAgentManagerWithOptions(db *sql.DB){_ = db}`},
		{"internal/runtime/manager/unit_constructor_delivery_authority_test.go", `package fixture;import "database/sql";func unlistedUnitManagerSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/diagnostics_test.go", `package fixture;import "database/sql";func unlistedLoggerSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/runtime_recovery_diagnostics_test.go", `package fixture;import("context";"database/sql");func unlistedRecoverySibling(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/runtime_log_native_fixture_test.go", `package fixture;import "database/sql";func unlistedNativeLoggerSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/runtime_log_native_external_test.go", `package fixture;import "database/sql";func unlistedNativeLoggerExternalSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/runtime_recovery_fan_out_fixture_test.go", `package fixture;import "database/sql";func unlistedFanOutCapacitySibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_entity_requirement_execution_test.go", `package fixture;import "database/sql";func executeExistingOwnerBehavior(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_gate_recovery_external_test.go", `package fixture;import "database/sql";func insertGateRecoveryRun(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_gate_recovery_external_test.go", `package fixture;import "database/sql";func assertGateRecoveryProcessedReceipt(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/entity_sparse_mutation_test.go", `package fixture;import "database/sql";func seedEntityToolSourceRun(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/entity_sparse_mutation_test.go", `package fixture;import "database/sql";func unlistedSparseToolSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_sqlite_persistence_test.go", `package fixture;import "database/sql";func TestEntityTools_ReadImportedCanonicalEntityContractOnBothStores(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_sqlite_persistence_test.go", `package fixture;import "database/sql";func TestRoleScopedEntityTools_SQLiteCurrentEntityPersistence(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_sqlite_persistence_test.go", `package fixture;import "database/sql";func newPostgresHumanTaskToolStoreForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_sqlite_persistence_test.go", `package fixture;import("context";"database/sql");func TestEntityTools_SQLiteBackendNeutralEntityPersistence(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/tools/executor_http_settlement_ack_test.go", `package fixture;import "database/sql";func requireHTTPSettlementOutcome(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/entity_field_ack_response_test.go", `package fixture;import "database/sql";func unlistedEntityAcknowledgmentSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_http_settlement_ack_test.go", `package fixture;import "database/sql";func unlistedHTTPSettlementSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/tools/executor_http_settlement_ack_test.go", `package fixture;import("context";"database/sql");func TestHTTPToolAcknowledgedSettlementCleanupPreservesResponseBothStores(query func(context.Context,*sql.Tx)error){_ = query}`},
		{"internal/runtime/pipeline/handler_entity_requirement_execution_test.go", `package fixture;import("context";"database/sql");func VerifyExistingOwnerExecutionSemanticsPersistOnSQLiteAndPostgresForTest(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/pipeline/entity_complete_snapshot_test.go", `package fixture;import "database/sql";func unlistedSnapshotSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func unlistedEngineSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func unlistedDeliveryTargetSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_transition_mutation_test.go", `package fixture;import "database/sql";func unlistedTransitionSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_transition_event_authority_test.go", `package fixture;import("context";"database/sql");func unlistedEventAuthoritySibling(read func(context.Context,*sql.Tx)error){_ = read}`},
		{"internal/runtime/pipeline/workflow_compiled_lifecycle_evidence_test.go", `package fixture;import "database/sql";func VerifyAcceptedLifecycleConsumerRejectsUnownedTransitionOnBothStoresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_node_delivery_authority_test.go", `package fixture;import("context";"database/sql");func VerifyPipelinePreclaimFailurePreservesErrorAndReturnsExactCarrierForTest(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/pipeline/workflow_node_delivery_authority_test.go", `package fixture;import "database/sql";func VerifyPipelineCoordinatorInterceptSkipsNodeWithoutPersistedDeliveryAuthorityForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayloadForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import("context";"database/sql");func VerifyExecuteNodeContractHandlerDefersCommittedEmissionsForTest(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEventForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrderForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import("context";"database/sql");func VerifyExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEventForTest(read func(context.Context,*sql.Tx)error){_ = read}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSourceForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelopeForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import("context";"database/sql");func VerifyExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnlyForTest(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/pipeline/handler_engine_transaction_test.go", `package fixture;import "database/sql";func VerifyExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSitesForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_closed_owner_native_fixture_test.go", `package fixture;import "database/sql";func unlistedClosedOwnerSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/workflow_missing_header_native_external_test.go", `package fixture;import "database/sql";func unlistedMissingHeaderSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func VerifyDeliveryTargetApplicationRejectsStateOnlyChildRelabeledAsParentOnBothStoresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func VerifyDeliveryTargetApplicationRejectsWrongRunRootTargetsBeforeMutationOnBothStoresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import("context";"database/sql");func VerifyDeliveryTargetApplicationRejectsInvalidPersistencePresenceAndLifecycleWithoutMutationForTest(seed func(context.Context,*sql.DB)){_ = seed}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyPipelineEngineMutationOwnerRoundTripsTypedCarrierForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import("context";"database/sql");func VerifyPipelineEngineStateRepoLoadStateMissingEntityDoesNotMaterializeDefaultsForTest(read func(context.Context,*sql.Tx)error){_ = read}`},
		{"internal/runtime/pipeline/workflow_expression_flow_identity_test.go", `package fixture;import "database/sql";func unlistedQuerySibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyPipelineEngineEvaluatorQueryEntitiesUsesExecutingFlowIDForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyPipelineEngineMutationOwnerRejectsForeignFlowWriteForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyPipelineEngineStateRepoLoadStateRejectsMalformedPersistedCarrierForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func VerifyDeliveryTargetApplicationRejectsMissingExactExistingTargetWithoutMutationForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import("context";"database/sql");func VerifyDeliveryTargetApplicationCarriesConstructedScenarioPreStateThroughMutationOnSQLiteAndPostgresForTest(write func(context.Context,*sql.Tx)error){_ = write}`},
		{"internal/runtime/pipeline/composition_receiver_business_test.go", `package fixture;import "database/sql";func unlistedReceiverSibling(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyPipelineEngineMutationOwnerRejectsWrongRunRootAddressBeforeMutationOnBothStoresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import("context";"database/sql");func VerifyWorkflowEngineFirstMaterializationRejectsMissingOrContradictoryEntityContractOnBothStoresForTest(read func(context.Context,*sql.Tx)error){_ = read}`},
		{"internal/runtime/pipeline/engine_adapter_test.go", `package fixture;import "database/sql";func VerifyWorkflowEngineMutationRejectsEntityContractDriftOnBothStoresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func VerifyDeliveryTargetApplicationReloadsCurrentScopedStateOnSQLiteAndPostgresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import "database/sql";func VerifyDeliveryTargetApplicationPreservesCompositionTargetOnSQLiteAndPostgresForTest(db *sql.DB){_ = db}`},
		{"internal/runtime/pipeline/delivery_target_application_test.go", `package fixture;import("context";"database/sql");func VerifyDeliveryTargetApplicationConsumesDeclarationBoundJoinTargetWithoutPayloadSelectorOnBothStoresForTest(read func(context.Context,*sql.Tx)error){_ = read}`},
	} {
		rejected := false
		for _, finding := range debtAuthorityFindingsFromSource(t, probe.file, probe.source) {
			rejected = rejected || nativeHandlerAuthority(finding)
		}
		if !rejected {
			t.Fatal("handler cohort regained raw authority")
		}
	}
}
