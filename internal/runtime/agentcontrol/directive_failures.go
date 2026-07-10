package agentcontrol

import runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"

const (
	DirectiveBoardStepFailedDetail               = "directive_board_step_failed"
	DirectiveHeartbeatShutdownUnconfirmedDetail  = "directive_heartbeat_shutdown_unconfirmed"
	DirectiveFailurePersistenceUnconfirmedDetail = "directive_failure_persistence_unconfirmed"
	DirectiveResultPersistenceUnconfirmedDetail  = "directive_result_persistence_unconfirmed"
	DirectiveExecutionLeaseExpiredDetail         = "directive_execution_lease_expired"
	DirectiveExecutionNotAdmittedDetail          = "directive_execution_not_admitted"
	directiveManagerComponent                    = "agent-manager"
	directiveOperationRecoveryComponent          = "directive-operation-recovery"
)

func DirectiveBoardStepFailure(err error) runtimefailures.Envelope {
	if existing, ok := runtimefailures.EnvelopeFromError(err); ok {
		return existing
	}
	return directiveFailure(runtimefailures.ClassInternalFailure, DirectiveBoardStepFailedDetail, directiveManagerComponent, "execute_directive")
}

func DirectiveHeartbeatShutdownUnconfirmedFailure() runtimefailures.Envelope {
	return directiveFailure(runtimefailures.ClassOutcomeUncertain, DirectiveHeartbeatShutdownUnconfirmedDetail, directiveManagerComponent, "stop_directive_heartbeat")
}

func DirectiveFailurePersistenceUnconfirmedFailure() runtimefailures.Envelope {
	return directiveFailure(runtimefailures.ClassOutcomeUncertain, DirectiveFailurePersistenceUnconfirmedDetail, directiveManagerComponent, "finalize_directive_failure")
}

func DirectiveResultPersistenceUnconfirmedFailure() runtimefailures.Envelope {
	return directiveFailure(runtimefailures.ClassOutcomeUncertain, DirectiveResultPersistenceUnconfirmedDetail, directiveManagerComponent, "record_directive_result")
}

func DirectiveExecutionLeaseExpiredFailure() runtimefailures.Envelope {
	return directiveFailure(runtimefailures.ClassOutcomeUncertain, DirectiveExecutionLeaseExpiredDetail, directiveOperationRecoveryComponent, "reconcile_execution_lease")
}

func DirectiveExecutionNotAdmittedFailure() runtimefailures.Envelope {
	return directiveFailure(runtimefailures.ClassInternalFailure, DirectiveExecutionNotAdmittedDetail, directiveOperationRecoveryComponent, "reconcile_prepared_operation")
}

func directiveFailure(class runtimefailures.Class, detail, component, operation string) runtimefailures.Envelope {
	failure := runtimefailures.FromError(runtimefailures.New(class, detail, component, operation, nil), component, operation)
	return failure.Failure
}
