package runtimepersistence

import (
	"context"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
)

func (s *PostgresStore) ActivateDeliveryAuthorityOutcome(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	return s.deliveryPostgresOwner.ActivateDeliveryAuthorityOutcome(ctx, authority)
}

func (s *SQLiteRuntimeStore) ActivateDeliveryAuthorityOutcome(ctx context.Context, authority runtimedelivery.ExecutionAuthority) (runtimedelivery.ActivationCommit, error) {
	return s.deliverySQLiteOwner.ActivateDeliveryAuthorityOutcome(ctx, authority)
}

func (s *PostgresStore) StopRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecyclePostgresOwner.StopRunControlOutcome(ctx, req)
}

func (s *SQLiteRuntimeStore) StopRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecycleSQLiteOwner.StopRunControlOutcome(ctx, req)
}

func (s *PostgresStore) PauseRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecyclePostgresOwner.PauseRunControlOutcome(ctx, req)
}

func (s *SQLiteRuntimeStore) PauseRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecycleSQLiteOwner.PauseRunControlOutcome(ctx, req)
}

func (s *PostgresStore) ContinueRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecyclePostgresOwner.ContinueRunControlOutcome(ctx, req)
}

func (s *SQLiteRuntimeStore) ContinueRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runLifecycleSQLiteOwner.ContinueRunControlOutcome(ctx, req)
}

func (s *PostgresStore) AdmitGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error) {
	return s.genericSchedulePostgresOwner.AdmitGenericScheduleOutcome(ctx, command)
}

func (s *SQLiteRuntimeStore) AdmitGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error) {
	return s.genericScheduleSQLiteOwner.AdmitGenericScheduleOutcome(ctx, command)
}

func (s *PostgresStore) CancelGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelCommit, error) {
	return s.genericSchedulePostgresOwner.CancelGenericScheduleOutcome(ctx, command)
}

func (s *SQLiteRuntimeStore) CancelGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelCommit, error) {
	return s.genericScheduleSQLiteOwner.CancelGenericScheduleOutcome(ctx, command)
}
