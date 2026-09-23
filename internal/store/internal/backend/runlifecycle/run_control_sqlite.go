package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storestandingdisposition "github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition"
	"github.com/google/uuid"
)

func (s *RunLifecycleSQLiteOwner) StopRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "stop")
}

func (s *RunLifecycleSQLiteOwner) PauseRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "pause")
}

func (s *RunLifecycleSQLiteOwner) ContinueRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "continue")
}

func (s *RunLifecycleSQLiteOwner) RunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("run lifecycle SQLite owner is required")
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return false, nil
	}
	var blocked bool
	if err := s.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM run_control_state
			WHERE run_id = ? AND control_status IN ('paused', 'stopped')
		)
	`, runID).Scan(&blocked); err != nil {
		return false, fmt.Errorf("load sqlite run dispatch control state: %w", err)
	}
	return blocked, nil
}

func (s *RunLifecycleSQLiteOwner) runControlTransition(ctx context.Context, req runtimeruncontrol.TransitionRequest, action string) (runtimeruncontrol.StoreTransition, error) {
	if s == nil || s.backend == nil {
		return runtimeruncontrol.StoreTransition{}, fmt.Errorf("run lifecycle SQLite owner is required")
	}
	runID := nullUUIDString(req.RunID)
	if runID == "" {
		return runtimeruncontrol.StoreTransition{}, fmt.Errorf("run_id is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.Reason = strings.TrimSpace(req.Reason); req.Reason == "" {
		req.Reason = "operator_request"
	}
	if req.ControlledBy = strings.TrimSpace(req.ControlledBy); req.ControlledBy == "" {
		req.ControlledBy = "api.v1"
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite run control transition", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimeruncontrol.State, error) {
		var state runtimeruncontrol.State
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			state, err = loadSQLiteRunControlState(txctx, tx, runID)
			if err != nil {
				return runControlStageFailure(action, "lock_run", err)
			}
			occurrenceScope, err := runtimeauthoractivity.BundleScopeForSource(txctx, state.BundleHash)
			if err != nil {
				return runControlStageFailure(action, "source_scope", fmt.Errorf("sqlite run control source scope: %w", err))
			}
			switch action {
			case "pause":
				state, err = s.pauseRunControlTx(txctx, tx, attempt, state, req)
			case "continue":
				state, err = s.continueRunControlTx(txctx, tx, attempt, state, req)
			case "stop":
				if err := rejectSQLiteStandingRunStopTx(txctx, tx, runID); err != nil {
					return runtimeruncontrol.StopFailure("standing_admission", err)
				}
				state, err = s.stopRunControlTx(txctx, tx, attempt, state, req)
			default:
				err = fmt.Errorf("unsupported run control action %q", action)
			}
			if err != nil {
				return err
			}
			if action == "pause" || action == "continue" {
				transition := "paused"
				if action == "continue" {
					transition = "resumed"
				}
				transitionID := uuid.NewString()
				if err := attempt.Record(txctx, runtimeauthoractivity.Draft{
					Kind: runtimeauthoractivity.KindRunLifecycle, Transition: transition,
					SourceOwner: "runs", SourceIdentity: transitionID, DedupKey: "run-transition:" + transitionID,
					OccurredAt: req.Now.UTC(), RunID: runID, Scope: occurrenceScope,
					Projection: runtimeauthoractivity.Projection{
						SubjectType: "run", SubjectID: runID, ControlReason: req.Reason, Source: req.ControlledBy,
					},
				}); err != nil {
					return err
				}
			}
			return nil
		})
		return state, err
	})
	state, committed := result.Value()
	outcome := runtimeruncontrol.StoreTransition{State: state, Acknowledged: committed}
	if action == "stop" {
		return outcome, classifyStopTransactionOutcome(result.Phase(), committed, result.Err())
	}
	return outcome, result.Err()
}

func loadSQLiteRunControlState(ctx context.Context, tx *sql.Tx, runID string) (runtimeruncontrol.State, error) {
	var state runtimeruncontrol.State
	var controlStatus, reason, controlledBy sql.NullString
	var updatedAt any
	err := tx.QueryRowContext(ctx, `
		SELECT r.run_id, COALESCE(r.status, ''), COALESCE(r.bundle_hash, ''), COALESCE(rc.control_status, ''),
		       COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, ''), rc.updated_at
		FROM runs r
		LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = ?
	`, runID).Scan(&state.RunID, &state.Status, &state.BundleHash, &controlStatus, &reason, &controlledBy, &updatedAt)
	if err == sql.ErrNoRows {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("load sqlite run control state: %w", err)
	}
	state.ControlStatus = strings.TrimSpace(controlStatus.String)
	state.BundleHash = strings.TrimSpace(state.BundleHash)
	state.Reason = strings.TrimSpace(reason.String)
	state.ControlledBy = strings.TrimSpace(controlledBy.String)
	if at, ok, err := sqliteTimeValue(updatedAt); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("scan sqlite run control updated_at: %w", err)
	} else if ok {
		state.UpdatedAt = at
	}
	return state, nil
}

func (s *RunLifecycleSQLiteOwner) pauseRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	switch lifecycleState {
	case runtimerunlifecycle.StateRunning:
	case runtimerunlifecycle.StatePaused:
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyPaused, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := (sqliteRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: state.RunID, State: runtimerunlifecycle.StatePaused}); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("pause sqlite run lifecycle: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'paused', ?, ?, ?, ?, NULL)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'paused', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, paused_at = COALESCE(run_control_state.paused_at, excluded.paused_at),
			stopped_at = NULL
	`, state.RunID, sqliteNullableString(req.Reason), req.ControlledBy, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run pause control state: %w", err)
	}
	state.Status = string(runtimerunlifecycle.StatePaused)
	state.ControlStatus = "paused"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *RunLifecycleSQLiteOwner) continueRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if lifecycleState != runtimerunlifecycle.StatePaused {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := (sqliteRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: state.RunID, State: runtimerunlifecycle.StateRunning}); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("continue sqlite run lifecycle: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'running', ?, ?, ?, NULL, NULL)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'running', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, stopped_at = NULL
	`, state.RunID, sqliteNullableString(req.Reason), req.ControlledBy, req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run continue control state: %w", err)
	}
	state.Status = string(runtimerunlifecycle.StateRunning)
	state.ControlStatus = "running"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *RunLifecycleSQLiteOwner) stopRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, runtimeruncontrol.StopFailure("validate_run", err)
	}
	if !lifecycleState.Active() {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	}
	abandoned, cancellations, err := s.quiesceStoppedRunWorkTx(ctx, tx, attempt, state.RunID, req.Reason, req.Now.UTC())
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, _, err := s.markRunTerminalTx(ctx, tx, attempt, runtimerunlifecycle.TerminalRequest{RunID: state.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: req.Now.UTC()}); err != nil {
		return runtimeruncontrol.State{}, runtimeruncontrol.StopFailure("terminal_state", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'stopped', ?, ?, ?, NULL, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'stopped', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, stopped_at = excluded.stopped_at
	`, state.RunID, sqliteNullableString(req.Reason), req.ControlledBy, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, runtimeruncontrol.StopFailure("control_state", err)
	}
	state.Status = "cancelled"
	state.ControlStatus = "stopped"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	state.AbandonedDeliveries = abandoned
	state.TimerCancellations = cancellations
	return state, nil
}

func (s *RunLifecycleSQLiteOwner) quiesceStoppedRunWorkTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, runID, reason string, now time.Time) (int, []runtimetimercancellation.Ref, error) {
	if s.delivery == nil || s.pipeline == nil {
		return 0, nil, fmt.Errorf("run lifecycle SQLite quiescence owners are required")
	}
	deliveries, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, attempt, runID, "run_stopped")
	if err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("deliveries", err)
	}
	if _, err := s.pipeline.TerminalizeRunTx(ctx, attempt, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), now); err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("pipeline", err)
	}
	if _, err := s.TerminateActiveSessionsTx(ctx, attempt, []string{runID}, "run_stopped", now); err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("sessions", err)
	}
	cancellations, err := cancelActiveRunTimerFamiliesTx(ctx, tx, attempt, false, []string{runID}, "run_stopped", now)
	if err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("timers", err)
	}
	return len(deliveries), cancellations, nil
}

func rejectSQLiteStandingRunStopTx(ctx context.Context, tx *sql.Tx, runID string) error {
	disposition, err := storestandingdisposition.ReadByRun(ctx, tx, false, runID)
	if err != nil {
		return fmt.Errorf("inspect sqlite standing run control ownership: %w", err)
	}
	if disposition.UsesGenericRecovery() {
		return nil
	}
	return fmt.Errorf("run %s is owned by standing service %s; %s", runID, disposition.ServiceID, disposition.RunControlGuidance())
}

func sqliteNullableString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}
