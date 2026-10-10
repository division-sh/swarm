package manager_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

func TestResetRuntimeState_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeResetRuntimeState_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestAuthBreakerShutdown_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeAuthBreakerShutdown_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestPersistedExecutableAdoptionStartsInCurrentManagerOccurrence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativePersistedExecutableAdoptionStartsInCurrentManagerOccurrence(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestExecutionProjectionRecoveryStartsPersistedRunningCell(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeExecutionProjectionRecoveryStartsPersistedRunningCell(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestExecutionProjectionSpawnDuringRunActivatesRegisteredProjection(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeExecutionProjectionSpawnDuringRunActivatesRegisteredProjection(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestAgentManagerDeferredSelfRetirementSettlesAcceptedAndReturnsBufferedDelivery(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeAgentManagerDeferredSelfRetirementSettlesAcceptedAndReturnsBufferedDelivery(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestRunningManagerDeliveryCarrierDispositionMatrix(t *testing.T) {
	t.Run("unit", func(t *testing.T) {
		manager.ProveNativeRunningManagerDeliveryCarrierDispositionMatrix(t, nil, nil)
	})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeRunningManagerDeliveryCarrierDispositionMatrix(t,
				func(t *testing.T) *manager.ManagerDeliveryNativeFixture { return openManagerNativeDelivery(t, backend) },
				func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
					return openManagerNativeSelectedDelivery(t, backend)
				},
			)
		})
	}
}

func TestProcessEventSelectedForkTerminalizesRetryableFailureBeforeRuntimeRetirement(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventSelectedForkTerminalizesRetryableFailureBeforeRuntimeRetirement(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeSelectedDelivery(t, backend)
			})
		})
	}
}

func TestProcessEvent_PropagatesInboundParentWithoutTraceSeeding(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEvent_PropagatesInboundParentWithoutTraceSeeding(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestClaimedAttemptExecutorDoesNotInheritLaneAuthorityThroughEventBusDescendant(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeClaimedAttemptExecutorDoesNotInheritLaneAuthorityThroughEventBusDescendant(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeManagerClaimPostcommitErrorConsumesOnlyAcknowledgedClaim(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestWriteReceiptPreservesCommittedSettlementAndContinuation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeWriteReceiptPreservesCommittedSettlementAndContinuation(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventDoesNotResettleAcknowledgedReceiptError(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventDoesNotResettleAcknowledgedReceiptError(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventQuiescenceReadFailureDoesNotAbandonClaim(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventQuiescenceReadFailureDoesNotAbandonClaim(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventCancellationRetainsFinalReceipt(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventCancellationRetainsFinalReceipt(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventConsumesDrainedCompletionObservationWithoutSecondReceiptOrOutput(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventConsumesDrainedCompletionObservationWithoutSecondReceiptOrOutput(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventDeterministicOutputIdentitySurvivesPartialSuccessRetry(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventDeterministicOutputIdentitySurvivesPartialSuccessRetry(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEvent_RecordsCanonicalDeliveryLifecycleTransitions(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEvent_RecordsCanonicalDeliveryLifecycleTransitions(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventRenewsExactClaimAroundAgentHandler(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventRenewsExactClaimAroundAgentHandler(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventHeartbeatCoversBlockedOutputAndPreventsReclaim(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventHeartbeatCoversBlockedOutputAndPreventsReclaim(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventSettlementFailureIsNotReportedAsReplayed(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventSettlementFailureIsNotReportedAsReplayed(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestClaimedAttemptExecutorSerializesLiveAndRecoveryForOneAgent(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeClaimedAttemptExecutorSerializesLiveAndRecoveryForOneAgent(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEvent_SkipsLateOutputAndReceiptAfterDestructiveResetQuiescence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEvent_SkipsLateOutputAndReceiptAfterDestructiveResetQuiescence(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEvent_SkipsHandlerForAlreadyQuiescedDelivery(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEvent_SkipsHandlerForAlreadyQuiescedDelivery(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeWriteReceipt_LogsRetryingAndExhaustedDeliveryLifecycleTransitions(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestWriteReceiptUsesCanonicalHandlerRetryBase(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeWriteReceiptUsesCanonicalHandlerRetryBase(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestWriteReceipt_ContextCancellationReturnsFailureAndLeavesClaimForLeaseRecovery(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeWriteReceipt_ContextCancellationReturnsFailureAndLeavesClaimForLeaseRecovery(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestWriteReceiptLongRunningClaimUsesExactRenewalTime(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeWriteReceiptLongRunningClaimUsesExactRenewalTime(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventPreservesAgentFailureEnvelopeAcrossReceiptAndReplayRecord(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventPreservesAgentFailureEnvelopeAcrossReceiptAndReplayRecord(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestRunningManagerInterventionFailureSettlesClaimBeforeShutdownAndRecovery(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeRunningManagerInterventionFailureSettlesClaimBeforeShutdownAndRecovery(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestRunningManagerInterventionSettlementFailureShutsDownAndRecoversClaim(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeRunningManagerInterventionSettlementFailureShutsDownAndRecoversClaim(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestProcessEventOutcomeUncertainTerminalDeliverySuppressesReplay(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeProcessEventOutcomeUncertainTerminalDeliverySuppressesReplay(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestSourceSetTransitionKeepsRealEventBusDeliveryPendingUntilAggregateRelease(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeSourceSetTransitionKeepsRealEventBusDeliveryPendingUntilAggregateRelease(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}

func TestSourceSetTransitionRetainsDequeuedDeliveryAndRouteAcrossManagerCancellation(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			manager.ProveNativeSourceSetTransitionRetainsDequeuedDeliveryAndRouteAcrossManagerCancellation(t, func(t *testing.T) *manager.ManagerDeliveryNativeFixture {
				return openManagerNativeDelivery(t, backend)
			})
		})
	}
}
