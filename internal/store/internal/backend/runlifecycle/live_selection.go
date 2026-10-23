package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/lib/pq"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func (s *RunLifecyclePostgresOwner) LoadLiveRun(ctx context.Context, scope runtimerunlifecycle.LiveRunScope) (runtimerunlifecycle.LiveRun, bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunlifecycle.LiveRun{}, false, err
	}
	return loadLiveRun(ctx, s.backend, scope)
}

func (s *RunLifecycleSQLiteOwner) LoadLiveRun(ctx context.Context, scope runtimerunlifecycle.LiveRunScope) (runtimerunlifecycle.LiveRun, bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunlifecycle.LiveRun{}, false, err
	}
	return loadLiveRun(ctx, s.backend, scope)
}

func loadLiveRun(ctx context.Context, q rowQueryer, scope runtimerunlifecycle.LiveRunScope) (runtimerunlifecycle.LiveRun, bool, error) {
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.LiveRun{}, false, err
	}
	result := runtimerunlifecycle.LiveRun{Scope: scope}
	var bundleHash string
	err := q.QueryRowContext(ctx, `SELECT run_id,bundle_hash FROM runs
		WHERE flow_path=$1 AND status='live'`, scope.FlowPath).Scan(&result.RunID, &bundleHash)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunlifecycle.LiveRun{}, false, nil
	}
	if err != nil {
		return runtimerunlifecycle.LiveRun{}, false, fmt.Errorf("read live run: %w", err)
	}
	result.Source, err = runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	return result, err == nil, err
}

func liveScopeForRun(ctx context.Context, q rowQueryer, runID string) (runtimerunlifecycle.LiveRunScope, error) {
	var scope runtimerunlifecycle.LiveRunScope
	err := q.QueryRowContext(ctx, `SELECT COALESCE(flow_path,'') FROM runs WHERE run_id=$1`, runID).Scan(&scope.FlowPath)
	if errors.Is(err, sql.ErrNoRows) {
		return scope, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil || scope.Empty() {
		return scope, err
	}
	return scope, scope.Validate()
}

func requireCreateScope(ctx context.Context, tx *sql.Tx, request runtimerunlifecycle.CreateRequest) error {
	scope, err := liveScopeForRun(ctx, tx, request.RunID)
	if err != nil {
		return err
	}
	if scope != request.LiveScope {
		return errors.New("run creation conflicts with its original flow")
	}
	return nil
}

func continueTargetState(ctx context.Context, tx *sql.Tx, postgres bool, state runtimeruncontrol.State) (runtimerunlifecycle.State, error) {
	scope, err := liveScopeForRun(ctx, tx, state.RunID)
	if err != nil {
		return "", err
	}
	if !scope.Empty() {
		return runtimerunlifecycle.StateLive, nil
	}
	return runtimerunlifecycle.StateRunning, requireGenericContinueAuthority(ctx, tx, postgres, state)
}

func rejectLiveRunStop(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var status string
	var flowPath sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT status,flow_path FROM runs WHERE run_id=$1`, runID).Scan(&status, &flowPath); err != nil {
		return false, err
	}
	if status == string(runtimerunlifecycle.StateLive) {
		return true, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: runID, CurrentStatus: status, Detail: "pause the live run before stopping it"}
	}
	return flowPath.Valid, nil
}

func classifyLiveClaimFailure(ctx context.Context, q rowQueryer, runID string, err error) error {
	if !isLiveClaimConflict(err) || ctx.Err() != nil {
		return err
	}
	scope, readErr := liveScopeForRun(ctx, q, runID)
	if readErr != nil || scope.Empty() {
		return errors.Join(err, readErr)
	}
	return classifyLiveScopeClaimFailure(ctx, q, scope, runID, err)
}

func isLiveClaimConflict(err error) bool {
	var pg *pq.Error
	var sq *sqlite.Error
	return errors.As(err, &pg) && pg.Code == "23505" && pg.Constraint == "idx_runs_live_flow" ||
		errors.As(err, &sq) && sq.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE
}

func classifyLiveScopeClaimFailure(ctx context.Context, q rowQueryer, scope runtimerunlifecycle.LiveRunScope, runID string, err error) error {
	if !isLiveClaimConflict(err) || scope.Empty() || ctx.Err() != nil {
		return err
	}
	current, found, readErr := loadLiveRun(ctx, q, scope)
	if readErr != nil || !found || current.RunID == runID {
		return errors.Join(err, readErr)
	}
	return errors.Join(&runtimerunlifecycle.LiveRunConflictError{Scope: scope, LiveRunID: current.RunID}, err)
}

type liveSelectionMutation interface {
	pauseLive(context.Context, *sql.Tx, *mutationprotocol.Attempt, runtimeruncontrol.State, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
	resumeLive(context.Context, *sql.Tx, *mutationprotocol.Attempt, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
}

func (m postgresRunLifecycleMutation) pauseLive(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return m.store.pauseRunControlTx(ctx, tx, attempt, state, req)
}

func (m sqliteRunLifecycleMutation) pauseLive(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return m.store.pauseRunControlTx(ctx, tx, attempt, state, req)
}

func (m postgresRunLifecycleMutation) resumeLive(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	state, err := lockRunControlState(ctx, tx, req.RunID)
	if err != nil {
		return state, err
	}
	if state.ControlStatus != "" {
		return m.store.continueRunControlTx(ctx, tx, attempt, state, req)
	}
	// A newly materialized child is paused without an operator control row.
	_, err = m.TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: req.RunID, State: runtimerunlifecycle.StateLive})
	state.Status = string(runtimerunlifecycle.StateLive)
	return state, err
}

func (m sqliteRunLifecycleMutation) resumeLive(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	state, err := loadSQLiteRunControlState(ctx, tx, req.RunID)
	if err != nil {
		return state, err
	}
	if state.ControlStatus != "" {
		return m.store.continueRunControlTx(ctx, tx, attempt, state, req)
	}
	_, err = m.TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: req.RunID, State: runtimerunlifecycle.StateLive})
	state.Status = string(runtimerunlifecycle.StateLive)
	return state, err
}

// SelectLiveRunTx is part of activation's native attempt, never a second commit.
func (s *RunLifecyclePostgresOwner) SelectLiveRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, now time.Time) (string, error) {
	var paused string
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		paused, err = selectLiveRunTx(ctx, tx, attempt, postgresRunLifecycleMutation{store: s, tx: tx, attempt: attempt}, runID, now)
		return err
	})
	return paused, err
}

func (s *RunLifecycleSQLiteOwner) SelectLiveRunTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, now time.Time) (string, error) {
	var paused string
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		paused, err = selectLiveRunTx(ctx, tx, attempt, sqliteRunLifecycleMutation{store: s, tx: tx, attempt: attempt}, runID, now)
		return err
	})
	return paused, err
}

func selectLiveRunTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, mutation liveSelectionMutation, runID string, now time.Time) (string, error) {
	scope, err := liveScopeForRun(ctx, tx, runID)
	if err != nil {
		return "", err
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	previous, found, err := loadLiveRun(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	if found && previous.RunID == runID {
		return "", nil
	}
	if found {
		req := runtimeruncontrol.TransitionRequest{RunID: previous.RunID, Now: now, Reason: "fork_live_switch", ControlledBy: "run.fork"}
		_, err := mutation.pauseLive(ctx, tx, attempt, runtimeruncontrol.State{RunID: previous.RunID, Status: string(runtimerunlifecycle.StateLive), BundleHash: previous.Source.BundleHash()}, req)
		if err != nil {
			return "", err
		}
		scope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, previous.Source.BundleHash())
		if err != nil {
			return "", err
		}
		if err := recordRunControlTransition(ctx, attempt, previous.RunID, scope, req, "paused"); err != nil {
			return "", err
		}
	}
	req := runtimeruncontrol.TransitionRequest{RunID: runID, Now: now, Reason: "fork_live_switch", ControlledBy: "run.fork"}
	selected, err := mutation.resumeLive(ctx, tx, attempt, req)
	if err != nil {
		return "", err
	}
	activityScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, selected.BundleHash)
	if err != nil {
		return "", err
	}
	if err := recordRunControlTransition(ctx, attempt, selected.RunID, activityScope, req, "resumed"); err != nil {
		return "", err
	}
	return previous.RunID, nil
}
