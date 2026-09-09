package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

// StopSelectedRunTx consumes the selected owner's transaction. Selected binding
// authority remains that owner's responsibility; terminal work has one owner.
func (s *RunLifecyclePostgresOwner) StopSelectedRunTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, req runcontrol.TransitionRequest) (runcontrol.State, error) {
	state, err := lockRunControlState(ctx, tx, req.RunID)
	if err != nil {
		return runcontrol.State{}, err
	}
	if _, err := authoractivity.BundleScopeForSource(ctx, state.BundleHash); err != nil {
		return runcontrol.State{}, fmt.Errorf("selected stop source scope: %w", err)
	}
	if err := rejectPostgresStandingRunStopTx(ctx, tx, req.RunID); err != nil {
		return runcontrol.State{}, err
	}
	return s.stopRunControlTx(ctx, tx, story, effects, state, req)
}

func (s *RunLifecycleSQLiteOwner) StopSelectedRunTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, req runcontrol.TransitionRequest) (runcontrol.State, error) {
	state, err := loadSQLiteRunControlState(ctx, tx, req.RunID)
	if err != nil {
		return runcontrol.State{}, err
	}
	if _, err := authoractivity.BundleScopeForSource(ctx, state.BundleHash); err != nil {
		return runcontrol.State{}, fmt.Errorf("selected stop source scope: %w", err)
	}
	if err := rejectSQLiteStandingRunStopTx(ctx, tx, req.RunID); err != nil {
		return runcontrol.State{}, err
	}
	return s.stopRunControlTx(ctx, tx, story, effects, state, req)
}
