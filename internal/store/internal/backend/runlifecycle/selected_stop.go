package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// StopSelectedRunTx consumes the selected owner's transaction. Selected binding
// authority remains that owner's responsibility; terminal work has one owner.
func (s *RunLifecyclePostgresOwner) StopSelectedRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, req runcontrol.TransitionRequest) (runcontrol.State, error) {
	var result runcontrol.State
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		state, err := lockRunControlState(ctx, tx, req.RunID)
		if err != nil {
			return err
		}
		if _, err := authoractivity.BundleScopeForSource(ctx, state.BundleHash); err != nil {
			return fmt.Errorf("selected stop source scope: %w", err)
		}
		if err := rejectPostgresStandingRunStopTx(ctx, tx, req.RunID); err != nil {
			return err
		}
		result, err = s.stopRunControlTx(ctx, tx, attempt, state, req)
		return err
	})
	return result, err
}

func (s *RunLifecycleSQLiteOwner) StopSelectedRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, req runcontrol.TransitionRequest) (runcontrol.State, error) {
	var result runcontrol.State
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		state, err := loadSQLiteRunControlState(ctx, tx, req.RunID)
		if err != nil {
			return err
		}
		if _, err := authoractivity.BundleScopeForSource(ctx, state.BundleHash); err != nil {
			return fmt.Errorf("selected stop source scope: %w", err)
		}
		if err := rejectSQLiteStandingRunStopTx(ctx, tx, req.RunID); err != nil {
			return err
		}
		result, err = s.stopRunControlTx(ctx, tx, attempt, state, req)
		return err
	})
	return result, err
}
