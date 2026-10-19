package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"
)

//go:embed testdata/fixture_setup_successors_2617.json
var fixtureSetupSuccessorBytes2617 []byte

func canonicalComparatorEqual2617(left, right string) bool {
	a, err := canonicalFunction(left)
	b, otherErr := canonicalFunction(right)
	return err == nil && otherErr == nil && a == b
}

func fixtureSetupCurrentEndpoint2617(row recipe, actual string) (recipe, error) {
	switch row.Family + "/" + row.File + "/" + row.Function {
	case "native-catalog-preconstruction/internal/runtime/cataloge2e/runtime_harness_test.go/newRuntimeHarnessWithTerminalProvider",
		"native-run-setup-family144/internal/runtime/runforkexecution/activity_fork_execution_test.go/seedSource",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/buildSelectedForkProofContainer",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/seedSelectedExecutionStateOnlySourceRun",
		"native-run-setup-family144/internal/runtime/runforkexecution/constructed_agent_input_execution_test.go/proveSelectedConstructedAgentInput",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/TestSelectedContractForkRejectsSyntheticCarryDynamicCreationBeforeMutation",
		"native-run-setup-family144/internal/runtime/runforkexecution/execution_test.go/TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority",
		"native-fanout-barrier-authority/internal/store/internal/runtimepersistence/fan_out_delivery_barrier_selected_store_test.go/TestFanOutDeliveryBarrierCompletionAndSupersessionWinnerMatrixOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinUsesSelectedStoreScheduleOwnerOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinSchedulePreservesMockExecutionModeOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinDurableIdentityIncludesStageOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinCountWaitsDespiteEmptyStateMembersOnBothStores",
		"native-pipeline-delivery-exact-join-semantic/internal/runtime/pipeline/workflow_join_lifecycle_test.go/TestWorkflowJoinFailurePersistsCanonicalDeliveryOutcomeAndRuntimeLog",
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity",
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries",
		"native-shared-coordinator-authority/internal/runtime/node_delivery_startup_recovery_test.go/TestDeliveryContinuationCoordinatorRecoversNodeDeliveriesThroughCanonicalSelectedStore",
		"native-shared-coordinator-authority/internal/runtime/template_instance_delivery_test.go/TestTemplateInstanceActivationConfigSubscriberPersistsRenderedRouteAndDeliveryRows",
		"native-bus-terminal-refusal-state/internal/runtime/bus/eventbus_publish_test.go/TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerPostgres",
		"native-bus-terminal-refusal-state/internal/runtime/bus/eventbus_publish_test.go/TestEventBusRejectsTerminalRunEventsThroughEveryPublishOwnerSQLite",
		"native-bus-selected-run-construction/internal/runtime/bus/eventbus_publish_test.go/eventBusTestRunContextForSource",
		"native-bus-refusal-duplicate-construction/internal/runtime/bus/eventbus_publish_test.go/TestSelectedForkCommitFailurePreventsDispatchAndProviderReachability",
		"native-bus-run-control-construction/internal/runtime/bus/run_control_dispatch_test.go/seedRunControlTestRun",
		"native-bus-source-hash/internal/runtime/bus/eventbus_publish_test.go/TestEventBusPublish_ClassifiesCanonicalRunSourceArtifactThroughRunLifecycleOwner",
		"native-bus-target-construction/internal/runtime/bus/target_owner_store_parity_test.go/TestCrossFlowConstructedTargetOwnershipRoundTripOnBothBackends",
		"native-bus-dispatch-seed/internal/runtime/bus/event_identity_dispatch_surface_test.go/newCompleteEventDispatchFixtureWithOrigin",
		"native-startup-actor-construction/internal/runtime/runtime_constructed_actor_restart_test.go/proveRuntimeConstructedActorCensusBothStores",
		"native-run-setup-family144/internal/store/internal/runtimepersistence/flow_instance_descriptor_authority_test.go/TestDynamicFlowRuntimeReadinessObservedStateGuardBothStores",
		"native-run-setup-family144/internal/store/internal/runtimepersistence/reset_inventory_parity_test.go/TestResetInventoryAndOperationLeaseBothStores",
		"native-pipeline-source-work-owner/internal/runtime/pipeline/author_activity_external_test_context_test.go/newScopedTestEventBus",
		"native-conversation-session-owner/internal/runtime/conformance/persisted_surfaces_test.go/TestCanonicalSessionWatchdogSurface_RoundTripsThroughConversationReader",
		"native-pipeline-delivery-shared-tail-semantic/internal/runtime/pipeline/workflow_timer_attachment_fixture_external_test.go/newTimerReplayAttachmentOwner",
		"native-shared-coordinator-authority/internal/runtime/channel_runtime_supported_surface_test.go/TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores",
		"native-shared-coordinator-authority/internal/runtime/final_flow_instance_authoring_runtime_test.go/TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance",
		"native-manager-explicit-delivery-configuration/internal/runtime/manager/work_lifetime_test_helpers_test.go/newTestAgentManagerWithOptions",
		"native-manager-delivery-caller/internal/runtime/manager/delivery_claim_ack_test.go/TestManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim",
		"native-manager-delivery-caller/internal/runtime/manager/receipt_settlement_outcome_test.go/TestWriteReceiptPreservesCommittedSettlementAndContinuation",
		"native-manager-delivery-caller/internal/runtime/manager/receipt_settlement_outcome_test.go/TestProcessEventDoesNotResettleAcknowledgedReceiptError",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestProcessEventRenewsExactClaimAroundAgentHandler",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestProcessEventHeartbeatCoversBlockedOutputAndPreventsReclaim",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestProcessEventSettlementFailureIsNotReportedAsReplayed",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestWriteReceiptUsesCanonicalHandlerRetryBase",
		"native-manager-delivery-caller/internal/runtime/manager/receipts_test.go/TestWriteReceipt_ContextCancellationReturnsFailureAndLeavesClaimForLeaseRecovery",
		"native-manager-delivery-owner/internal/runtime/manager/receipts_test.go/TestWriteReceiptLongRunningClaimUsesExactRenewalTime",
		"native-manager-delivery-caller/internal/runtime/manager/failure_test.go/TestRunningManagerInterventionSettlementFailureShutsDownAndRecoversClaim",
		"native-manager-delivery-owner/internal/runtime/manager/failure_test.go/TestProcessEventOutcomeUncertainTerminalDeliverySuppressesReplay",
		"native-manager-delivery-owner/internal/runtime/manager/execution_projection_test.go/TestRunningManagerDeliveryCarrierDispositionMatrix",
		"native-run-setup-family144/internal/runtime/budget_recovery_parity_test.go/TestCompletionBudgetRecoveryProjectionParity",
		"native-run-setup-family144/internal/runtime/manager/delivery_native_selected_external_test.go/openManagerNativeSelectedDelivery",
		"native-run-setup-family144/internal/runtime/runtime_log_native_external_test.go/nativeRuntimeLogFixture",
		"native-run-setup-family144/internal/runtime/runtime_shutdown_admission_test.go/TestRuntimeShutdownDeliveryFixtureClaimsThroughCanonicalAdapter",
		"native-run-setup-family144/internal/runtime/runtime_shutdown_admission_test.go/TestRuntimeShutdownWithOptions_PropagatesConfiguredGraceToManagerDrain",
		"native-run-setup-family144/internal/runtime/runtime_shutdown_admission_test.go/TestRuntimeShutdown_ClosesAdmissionBeforeManagerDrainAndInboundIngress",
		"native-run-setup-family144/internal/runtime/runtime_shutdown_fan_out_test.go/TestRuntimeShutdownFanOutCommittedTurnKeepsDependenciesLive",
		"native-run-setup-family144/internal/runtime/runtime_startup_readiness_test.go/newStartupReadinessTestRuntime",
		"native-run-setup-family144/internal/runtime/workflow_timer_startup_recovery_test.go/TestRuntimeStartFailsClosedWhenManagerHydrationWouldWithholdWorkflowTimersOnBothStores",
		"native-handler-source-coordinator/internal/runtime/pipeline/workflow_timer_replay_selected_store_external_test.go/TestWorkflowTimerCauseReplayReopenAndIsolationBothStores",
		"native-startup-actor-construction/internal/runtime/runtime_agent_memory_native_startup_test.go/assertNativeRuntimeStartCarriesMemoryIdentity",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_RecoveryDisabledEmitsDeniedDecisionForActiveSchedules",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_RecoveryDisabledAllowsAndLogsManagerSnapshotWork",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_RecoveryEnabledEmitsAllowedDecisionSummary",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_WorkflowOnlyRecoveryUsesFamilyAwareBootAndRestorationDetail",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_RecoveryFailureEmitsDegradedDecisionSummary",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_DynamicFlowReadinessFinalizationFailureIsBootFatal",
		"native-runtime-recovery-log/internal/runtime/runtime_recovery_diagnostics_test.go/TestRuntimeStart_RecoveryInspectionAndManagerHydrationFailureIsBootFatal",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_Log_StampsSourceArtifactFactOnRunRow",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_LogRejectsDeletedPersistedSourceArtifactFact",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_Log_PersistsCanonicalRunOwnershipFromContext",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_Log_DerivesLineageFromPersistedSubjectEvent",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_Log_DoesNotDeriveLineageFromUnpersistedSubjectEvent",
		"native-runtime-log-persistence/internal/runtime/diagnostics_test.go/TestRuntimeLogger_Log_PersistsTypedRuntimeLineage",
		"native-run-setup-family144/internal/runtime/workflow_timer_startup_recovery_test.go/TestGenericScheduleLifecyclePublishesOneShotAndRecurringThroughWorkflowRuntimeOnBothStores",
		"native-run-setup-family144/internal/runtime/workflow_timer_startup_recovery_test.go/TestRuntimeStartRestoresWorkflowTimersWithoutGenericScheduleStoreOnBothStores",
		"native-run-setup-family144/internal/runtime/workflow_timer_startup_recovery_test.go/TestRuntimeStartWithholdsDueSchedulesAndTimersUntilDynamicTopologyCompletesOnBothStores":
	default:
		return recipe{}, fmt.Errorf("unlisted fixture comparator endpoint")
	}
	if fmt.Sprintf("%x", sha256.Sum256(fixtureSetupSuccessorBytes2617)) != "03c030fb2a6a3d1b2b372ab76b184410cca88ae045040c900aec3bb9109cbb89" {
		return recipe{}, fmt.Errorf("fixture comparator evidence changed")
	}
	var pins []fixtureSetupSuccessor2617
	if err := json.Unmarshal(fixtureSetupSuccessorBytes2617, &pins); err != nil {
		return recipe{}, err
	}
	if len(pins) != 114 {
		return recipe{}, fmt.Errorf("fixture comparator receipt count changed")
	}
	var original recipe
	var current string
	exactOriginalSetup := false
	for index, pin := range pins {
		if !sameReviewedRecipeKey(row, pin.Original) {
			continue
		}
		if current == "" {
			original = pin.Original
			current = original.Successor
			if current == "" {
				current = original.After
			}
		} else {
			prior := original
			prior.Successor = current
			if !reflect.DeepEqual(prior, pin.Original) {
				return recipe{}, fmt.Errorf("fixture comparator crossed its historical receipt")
			}
		}
		reversed, err := reverseReviewedSuccessor2617(pin.Candidate, pin.Edits)
		if err != nil || !canonicalComparatorEqual2617(reversed, current) {
			return recipe{}, fmt.Errorf("fixture comparator failed exact reversal")
		}
		current = pin.Candidate
		exactOriginalSetup = exactOriginalSetup || index >= 47
	}
	history := row
	history.Successor = original.Successor
	projected, err := q10FixtureSetupCurrentSource(row)
	if err != nil {
		return recipe{}, err
	}
	endpointCurrent := canonicalComparatorEqual2617(row.Successor, current) && canonicalComparatorEqual2617(actual, projected)
	if exactOriginalSetup {
		// These setup deltas carry custody; the older comparator normalization
		// intentionally ignores some setup syntax and cannot prove this cut.
		endpoint, endpointErr := canonicalFunction(row.Successor)
		source, sourceErr := canonicalFunction(actual)
		want, wantErr := canonicalFunction(current)
		projected, projectedErr := canonicalFunction(projected)
		endpointCurrent = endpointErr == nil && sourceErr == nil && wantErr == nil && projectedErr == nil && endpoint == want && source == projected
	}
	if current == "" || !reflect.DeepEqual(history, original) || row.Successor == "" || !endpointCurrent {
		return recipe{}, fmt.Errorf("fixture comparator current source or history differs")
	}
	return original, nil
}
