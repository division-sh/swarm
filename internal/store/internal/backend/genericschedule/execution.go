package genericschedule

import (
	"context"
	"database/sql"
	"errors"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
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

type fanOutBarrierRunExecutionOwner interface {
	RequireFanOutBarrierRunExecutionTx(context.Context, *sql.Tx, string) error
}

type fanOutBarrierRunExecution struct{ RunExecutionOwner }

func (e fanOutBarrierRunExecution) RequireRunExecutionTx(ctx context.Context, tx *sql.Tx, runID string) error {
	authority, _ := runtimeeffects.AuthorityFromContext(ctx)
	if authority.Kind != runtimeeffects.AuthoritySelectedContractFork {
		return e.RunExecutionOwner.RequireRunExecutionTx(ctx, tx, runID)
	}
	barrier, ok := e.RunExecutionOwner.(fanOutBarrierRunExecutionOwner)
	if !ok {
		return errors.New("fan-out barrier schedule mutation requires its run execution owner")
	}
	return barrier.RequireFanOutBarrierRunExecutionTx(ctx, tx, runID)
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
