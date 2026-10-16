package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/testutil/runtimepipelinefixture"
)

type ForeignOutboxDispatchObservation struct {
	PostCommitActions      int
	RollbackActions        int
	ForeignTransactionOpen bool
}

// The selected-store fixture owner contains the complete foreign transaction
// cut. Only detached observations leave it; the dispatcher receives no executor.
func ObserveOutboxDispatchWithForeignTransactionForTest(ctx context.Context, selected any, dispatcher engine.PostCommitDispatcher, intents []engine.EmitIntent) (observation ForeignOutboxDispatchObservation, err error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return observation, err
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return observation, err
	}
	db.SetMaxOpenConns(1)
	defer func() { err = errors.Join(err, db.Close()) }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return observation, err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	var postCommit, rollback []runtimepipelinefixture.OwnerAction
	foreign := runtimepipelinefixture.WithSQLTx(ctx, tx)
	foreign = runtimepipelinefixture.WithPostCommitActions(foreign, &postCommit)
	foreign = runtimepipelinefixture.WithRollbackActions(foreign, &rollback)
	err = dispatcher.DispatchPostCommit(foreign, intents)
	observation.PostCommitActions, observation.RollbackActions = len(postCommit), len(rollback)
	var one int
	probeErr := tx.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	observation.ForeignTransactionOpen = probeErr == nil && one == 1
	return observation, errors.Join(err, probeErr, tx.Commit())
}
