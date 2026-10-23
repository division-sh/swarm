package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// The prepared command targets this facade. Native activation consumes the
// admitted context without attempting a second process join.
func (s *PostgresStore) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	if !startupownership.PreparedOperationEntered(ctx, startupownership.PreparedForkActivate, s) {
		return runfork.RunForkActivation{}, startupownership.ErrProcessNotCurrent
	}
	return s.runForkPostgresOwner.ActivateRunForkForSelectedContractExecution(ctx, req)
}

func (s *SQLiteRuntimeStore) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	if !startupownership.PreparedOperationEntered(ctx, startupownership.PreparedForkActivate, s) {
		return runfork.RunForkActivation{}, startupownership.ErrProcessNotCurrent
	}
	return s.runForkSQLiteOwner.ActivateRunForkForSelectedContractExecution(ctx, req)
}
