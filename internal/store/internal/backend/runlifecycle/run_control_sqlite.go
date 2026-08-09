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
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func (s *RunLifecycleSQLiteOwner) StopRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.runControlTransition(ctx, req, "stop")
}

func (s *RunLifecycleSQLiteOwner) PauseRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.runControlTransition(ctx, req, "pause")
}

func (s *RunLifecycleSQLiteOwner) ContinueRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
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

func (s *RunLifecycleSQLiteOwner) runControlTransition(ctx context.Context, req runtimeruncontrol.TransitionRequest, action string) (runtimeruncontrol.State, error) {
	if s == nil || s.backend == nil {
		return runtimeruncontrol.State{}, fmt.Errorf("run lifecycle SQLite owner is required")
	}
	runID := nullUUIDString(req.RunID)
	if runID == "" {
		return runtimeruncontrol.State{}, fmt.Errorf("run_id is required")
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
	handoff, err := ReserveCandidateHandoff(ctx)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	defer handoff.Rollback()
	var state runtimeruncontrol.State
	if err := s.runPrivateAuthorActivityMutation(ctx, "sqlite run control transition", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		state, err = loadSQLiteRunControlState(txctx, tx, runID)
		if err != nil {
			return err
		}
		occurrenceScope, err := runtimeauthoractivity.BundleScopeForSource(txctx, state.BundleHash)
		if err != nil {
			return fmt.Errorf("sqlite run control source scope: %w", err)
		}
		switch action {
		case "pause":
			state, err = s.pauseRunControlTx(txctx, tx, state, req, handoff)
		case "continue":
			state, err = s.continueRunControlTx(txctx, tx, state, req, handoff)
		case "stop":
			if err := rejectSQLiteStandingRunStopTx(txctx, tx, runID); err != nil {
				return err
			}
			state, err = s.stopRunControlTx(txctx, tx, story, state, req)
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
			if err := story.Record(txctx, runtimeauthoractivity.Draft{
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
	}); err != nil {
		return runtimeruncontrol.State{}, err
	}
	return state, handoff.Commit()
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

func (s *RunLifecycleSQLiteOwner) pauseRunControlTx(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest, handoff *CandidateHandoff) (runtimeruncontrol.State, error) {
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
	if _, err := s.TransitionActiveTx(ctx, tx, nil, handoff, runtimerunlifecycle.ActiveTransitionRequest{RunID: state.RunID, State: runtimerunlifecycle.StatePaused}); err != nil {
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

func (s *RunLifecycleSQLiteOwner) continueRunControlTx(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest, handoff *CandidateHandoff) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if lifecycleState != runtimerunlifecycle.StatePaused {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := s.TransitionActiveTx(ctx, tx, nil, handoff, runtimerunlifecycle.ActiveTransitionRequest{RunID: state.RunID, State: runtimerunlifecycle.StateRunning}); err != nil {
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

func (s *RunLifecycleSQLiteOwner) stopRunControlTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if !lifecycleState.Active() {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	}
	abandoned, cancellations, err := s.quiesceStoppedRunWorkTx(ctx, tx, story, state.RunID, req.Reason, req.Now.UTC())
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, _, err := s.MarkTerminalTx(ctx, tx, story, runtimerunlifecycle.TerminalRequest{RunID: state.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: req.Now.UTC()}); err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'stopped', ?, ?, ?, NULL, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'stopped', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, stopped_at = excluded.stopped_at
	`, state.RunID, sqliteNullableString(req.Reason), req.ControlledBy, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run stop control state: %w", err)
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

func (s *RunLifecycleSQLiteOwner) quiesceStoppedRunWorkTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, runID, reason string, now time.Time) (int, []runtimetimercancellation.Ref, error) {
	if s.delivery == nil || s.pipeline == nil {
		return 0, nil, fmt.Errorf("run lifecycle SQLite quiescence owners are required")
	}
	deliveries, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, tx, story, runID, "run_stopped")
	if err != nil {
		return 0, nil, err
	}
	if _, err := s.pipeline.TerminalizeRunTx(ctx, tx, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), now); err != nil {
		return 0, nil, err
	}
	if _, err := s.TerminateActiveSessionsTx(ctx, tx, []string{runID}, "run_stopped", now); err != nil {
		return 0, nil, err
	}
	cancellations, err := cancelActiveRunTimerFamiliesTx(ctx, tx, false, []string{runID}, "run_stopped", now)
	if err != nil {
		return 0, nil, err
	}
	return len(deliveries), cancellations, nil
}

func rejectSQLiteStandingRunStopTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var serviceID string
	err := tx.QueryRowContext(ctx, `SELECT service_id FROM standing_services WHERE current_run_id = ?`, runID).Scan(&serviceID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect sqlite standing run control ownership: %w", err)
	}
	return fmt.Errorf("run %s is owned by standing service %s; use `swarm standing suspend %s` or `swarm standing reset %s`", runID, serviceID, serviceID, serviceID)
}

func sqliteNullableString(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}
