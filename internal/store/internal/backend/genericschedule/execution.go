package genericschedule

import (
	"context"
	"database/sql"
	"errors"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

func readbackOnlyAdmission(err error) bool {
	return errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) ||
		errors.Is(err, runtimerunlifecycle.ErrRunNotActive) ||
		errors.Is(err, runtimerunlifecycle.ErrRunNotFound)
}

func ordinaryTerminalAdmission(err error) bool {
	return !errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) &&
		(errors.Is(err, runtimerunlifecycle.ErrRunNotActive) || errors.Is(err, runtimerunlifecycle.ErrRunNotFound))
}

func requireActivationExecutionTx(ctx context.Context, tx *sql.Tx, execution RunExecutionOwner, activationID string) error {
	runID, err := timerRunIDTx(ctx, tx, activationID)
	if err != nil {
		return err
	}
	return execution.RequireRunExecutionTx(ctx, tx, runID)
}

func observeActivationExecution(ctx context.Context, q sourceadmission.ExecutionQuery, execution RunExecutionOwner, activationID string) (bool, error) {
	runID, err := timerRunIDTx(ctx, q, activationID)
	if err != nil {
		return false, err
	}
	return execution.ObserveRunExecution(ctx, q, runID)
}
