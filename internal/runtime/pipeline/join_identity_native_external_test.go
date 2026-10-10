package pipeline_test

import (
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"testing"
)

func TestSiblingFlowJoinDeclarationsStayIndependentAcrossRestartOnBothStores(t *testing.T) {
	pipeline.VerifyNativeSiblingFlowJoinDeclarationsStayIndependentAcrossRestartOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestRootAndFlowWorkflowJoinArrivalCompletionCancelsExactScheduleOnBothStores(t *testing.T) {
	pipeline.VerifyNativeRootAndFlowWorkflowJoinArrivalCompletionCancelsExactScheduleOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinArmArrivalRaceIsEarlyOrAdmittedOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinPersistedArrivalClassificationOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinPersistedArrivalClassificationOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestCompiledTransitionEvidenceRoundTripOnBothStores(t *testing.T) {
	pipeline.VerifyNativeCompiledTransitionEvidenceRoundTripOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReads(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReadsForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentityForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValue(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValueForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerPublishesAfterPersistencePrerequisiteFieldSucceeds(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerPublishesAfterPersistencePrerequisiteFieldSucceedsForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerRejectsMissingWriteSourceBeforeEmit(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerRejectsMissingWriteSourceBeforeEmitForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContext(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContextForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestWorkflowGateEntryUsesOneTransactionAndRollsBackOnCardFailure(t *testing.T) {
	pipeline.VerifyNativeWorkflowGateEntryUsesOneTransactionAndRollsBackOnCardFailureForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowGateEntryCreatesMatchingActivationAndCardOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowGateEntryCreatesMatchingActivationAndCardOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowGateCommittedDecisionWinsOrdinaryAndTimerExitRacesOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowGateCommittedDecisionWinsOrdinaryAndTimerExitRacesOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowGateOrdinaryExitSupersessionCarriesCardFlowIdentityOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowGateOrdinaryExitSupersessionCarriesCardFlowIdentityOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestUpdateEntityState_LogsMutationRowForStateTransition(t *testing.T) {
	pipeline.VerifyNativeUpdateEntityState_LogsMutationRowForStateTransitionForTest(t, pipelineDeliveryNativeFixture)
}

func TestNativeMutationLoggedStateTransitionFailsClosedWithoutJournalBothStores(t *testing.T) {
	pipeline.VerifyNativeMutationLoggedPipelineWritesFailClosedWithoutEntityMutationsTableForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowTimerLifecycleReconcilesProgressedInitialDeclarationsProspectivelyOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowTimerLifecycleReconcilesProgressedInitialDeclarationsProspectivelyOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestNativeDirectLifecycleComponentRejectsMissingOrForeignClaimBothStores(t *testing.T) {
	pipeline.VerifyNativeDirectLifecycleComponentRejectsMissingOrForeignClaimForTest(t, pipelineDeliveryNativeFixture)
}

func TestPipelineRejectsFabricatedGuardCauseOnBothStores(t *testing.T) {
	pipeline.VerifyNativePipelineRejectsFabricatedGuardCauseOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestNestedFanOutDiamondJoinsRetainIndependentDeclarationHandles(t *testing.T) {
	pipeline.VerifyNativeNestedFanOutDiamondJoinsRetainIndependentDeclarationHandlesForTest(t, pipelineDeliveryNativeFixture)
}

func TestConcurrentRootAndFlowSameLeafJoinsRemainDistinctAcrossRestart(t *testing.T) {
	pipeline.VerifyNativeConcurrentRootAndFlowSameLeafJoinsRemainDistinctAcrossRestartForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinExpectedZeroStageExitCancelsPendingCompletionOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinExpectedZeroStageExitCancelsPendingCompletionOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinExpectedZeroCompletesAfterRestartOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestPipelineCompiledJoinTransitionEvidenceOnBothStores(t *testing.T) {
	pipeline.VerifyNativePipelineCompiledJoinTransitionEvidenceOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinArrivalTimeoutRaceHasOneCloseWinnerOnBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinArrivalTimeoutRaceHasOneCloseWinnerOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestActivityBoringProofHandAuthoredReadOnlyForkReexecuteUsesForkLocalIdentity(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofHandAuthoredReadOnlyForkReexecuteUsesForkLocalIdentityForTest(t, pipelineDeliveryNativeFixture)
}

func TestActivityBoringProofHandAuthoredFlowCrashAfterRequestBeforeResultCompletesOncePostgres(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofHandAuthoredFlowCrashAfterRequestBeforeResultCompletesOncePostgresForTest(t, pipelineDeliveryNativeFixture)
}

func TestActivityBoringProofHandAuthoredFlowDispatchesOutsideTransactionAndReusesRecordedResult(t *testing.T) {
	pipeline.VerifyNativeActivityBoringProofHandAuthoredFlowDispatchesOutsideTransactionAndReusesRecordedResultForTest(t, pipelineDeliveryNativeFixture)
}

func TestFanOutCoalescedBacklogAcceleratedControl(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeFanOutCoalescedBacklogAcceleratedControlForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestFanOutCompletedIntentRefillsCoalescedBacklog(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeFanOutCompletedIntentRefillsCoalescedBacklogForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestSQLiteNestedFanOutCreatesIndependentDurableIntentAndExactLineage(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeSQLiteNestedFanOutCreatesIndependentDurableIntentAndExactLineageForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestSQLiteFanOutTriggerPersistsOneIntentWithoutEagerDeliveries(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeSQLiteFanOutTriggerPersistsOneIntentWithoutEagerDeliveriesForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerLogsComputeModuleReplayEvidenceBeforeFailureReturn(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeExecuteNodeContractHandlerLogsComputeModuleReplayEvidenceBeforeFailureReturnForTest(t, backend, pipelineDeliveryNativeFixture)
		})
	}
}

func TestExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntity(t *testing.T) {
	pipeline.VerifyNativeExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntityForTest(t, pipelineDeliveryNativeFixture)
}

func TestNodeContractFirstEventTransitionsFromCanonicalInitialStateOnBothStores(t *testing.T) {
	pipeline.VerifyNativeNodeContractFirstEventTransitionsFromCanonicalInitialStateOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestA2NonLoopStageReentryOnBothStores(t *testing.T) {
	pipeline.VerifyNativeA2NonLoopStageReentryOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestReentrantJoinCompletionDoesNotCancelNextGeneration(t *testing.T) {
	pipeline.VerifyNativeReentrantJoinCompletionDoesNotCancelNextGenerationForTest(t, pipelineDeliveryNativeFixture)
}

func TestWorkflowJoinRetainedGenerationMutationRoundTripBothStores(t *testing.T) {
	pipeline.VerifyNativeWorkflowJoinRetainedGenerationMutationRoundTripBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestRootAndFlowWorkflowJoinLoopSupersessionCancelsExactGenerationOnBothStores(t *testing.T) {
	pipeline.VerifyNativeRootAndFlowWorkflowJoinLoopSupersessionCancelsExactGenerationOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestJoinCapturedLoopOutcomeAfterRestartBothStores(t *testing.T) {
	pipeline.VerifyNativeJoinCapturedLoopOutcomeAfterRestartBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestA2UntilClosesMultipleJoinsAndPreservesOrdinaryHandlerOnBothStores(t *testing.T) {
	pipeline.VerifyNativeA2UntilClosesMultipleJoinsAndPreservesOrdinaryHandlerOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestExecuteNodeHandlerPlanResult_NestedPackageRootConnectDoesNotAuthorizeRepositoryRootHandler(t *testing.T) {
	pipeline.VerifyNativeExecuteNodeHandlerPlanResult_NestedPackageRootConnectDoesNotAuthorizeRepositoryRootHandlerForTest(t, pipelineDeliveryNativeFixture)
}

func TestRootAndFlowWorkflowJoinStageExitCancelsExactScheduleOnBothStores(t *testing.T) {
	pipeline.VerifyNativeRootAndFlowWorkflowJoinStageExitCancelsExactScheduleOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestRootAndFlowWorkflowJoinImmediateCompletionFiresExactHandleAfterRestartOnBothStores(t *testing.T) {
	pipeline.VerifyNativeRootAndFlowWorkflowJoinImmediateCompletionFiresExactHandleAfterRestartOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestRootAndFlowWorkflowJoinTimeoutFiresExactHandleAfterRestartOnBothStores(t *testing.T) {
	pipeline.VerifyNativeRootAndFlowWorkflowJoinTimeoutFiresExactHandleAfterRestartOnBothStoresForTest(t, pipelineDeliveryNativeFixture)
}

func TestObsoleteJoinOccurrenceUsesAtomicNodeSettlementBothStores(t *testing.T) {
	pipeline.VerifyNativeObsoleteJoinOccurrenceUsesAtomicNodeSettlementBothStoresForTest(t, pipelineDeliveryNativeFixture)
}
