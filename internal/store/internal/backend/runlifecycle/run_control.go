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

func (s *RunLifecyclePostgresOwner) StopRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "stop")
}

func (s *RunLifecyclePostgresOwner) PauseRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "pause")
}

func (s *RunLifecyclePostgresOwner) ContinueRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	return s.runControlTransition(ctx, req, "continue")
}

func (s *RunLifecyclePostgresOwner) RunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return false, nil
	}
	var blocked bool
	err := s.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM run_control_state
			WHERE run_id = $1::uuid
			  AND control_status IN ('paused', 'stopped')
		)
	`, runID).Scan(&blocked)
	if err != nil {
		return false, fmt.Errorf("load run dispatch control state: %w", err)
	}
	return blocked, nil
}

func (s *RunLifecyclePostgresOwner) runControlTransition(ctx context.Context, req runtimeruncontrol.TransitionRequest, action string) (runtimeruncontrol.StoreTransition, error) {
	if s == nil || s.backend == nil {
		return runtimeruncontrol.StoreTransition{}, fmt.Errorf("postgres store is required")
	}
	runID := nullUUIDString(req.RunID)
	if runID == "" {
		return runtimeruncontrol.StoreTransition{}, fmt.Errorf("run_id is required")
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		req.Reason = "operator_request"
	}
	req.ControlledBy = strings.TrimSpace(req.ControlledBy)
	if req.ControlledBy == "" {
		req.ControlledBy = "api.v1"
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeruncontrol.StoreTransition{}, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimeruncontrol.State, error) {
		var state runtimeruncontrol.State
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			state, err = lockRunControlState(txctx, tx, runID)
			if err != nil {
				return runControlStageFailure(action, "lock_run", err)
			}
			occurrenceScope, err := runtimeauthoractivity.BundleScopeForSource(txctx, state.BundleHash)
			if err != nil {
				return runControlStageFailure(action, "source_scope", fmt.Errorf("run control source scope: %w", err))
			}
			switch action {
			case "stop":
				if err := rejectPostgresStandingRunStopTx(txctx, tx, runID); err != nil {
					return runtimeruncontrol.StopFailure("standing_admission", err)
				}
				state, err = s.stopRunControlTx(txctx, tx, attempt, state, req)
			case "pause":
				state, err = s.pauseRunControlTx(txctx, tx, attempt, state, req)
			case "continue":
				state, err = s.continueRunControlTx(txctx, tx, attempt, state, req)
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

func lockRunControlState(ctx context.Context, tx *sql.Tx, runID string) (runtimeruncontrol.State, error) {
	var state runtimeruncontrol.State
	var controlStatus, reason, controlledBy sql.NullString
	var updatedAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT
			r.run_id::text,
			COALESCE(r.status, ''),
			COALESCE(r.bundle_hash, ''),
			COALESCE(rc.control_status, ''),
			COALESCE(rc.reason, ''),
			COALESCE(rc.controlled_by, ''),
			rc.updated_at
		FROM runs r
		LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
		FOR UPDATE OF r
	`, runID).Scan(&state.RunID, &state.Status, &state.BundleHash, &controlStatus, &reason, &controlledBy, &updatedAt)
	if err == sql.ErrNoRows {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("lock run control state: %w", err)
	}
	state.RunID = strings.TrimSpace(state.RunID)
	state.Status = strings.TrimSpace(state.Status)
	state.BundleHash = strings.TrimSpace(state.BundleHash)
	state.ControlStatus = strings.TrimSpace(controlStatus.String)
	state.Reason = strings.TrimSpace(reason.String)
	state.ControlledBy = strings.TrimSpace(controlledBy.String)
	if updatedAt.Valid {
		state.UpdatedAt = updatedAt.Time
	}
	return state, nil
}

func (s *RunLifecyclePostgresOwner) pauseRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
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
	if _, err := (postgresRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: state.RunID,
		State: runtimerunlifecycle.StatePaused,
	}); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("pause run lifecycle: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES ($1::uuid, 'paused', NULLIF($2, ''), $3, $4, $4, NULL)
		ON CONFLICT (run_id) DO UPDATE SET
			control_status = 'paused',
			reason = NULLIF($2, ''),
			controlled_by = $3,
			updated_at = $4,
			paused_at = COALESCE(run_control_state.paused_at, $4),
			stopped_at = NULL
	`, state.RunID, req.Reason, req.ControlledBy, req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist run pause control state: %w", err)
	}
	state.Status = string(runtimerunlifecycle.StatePaused)
	state.ControlStatus = "paused"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *RunLifecyclePostgresOwner) continueRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	lifecycleState, err := runtimerunlifecycle.ParseState(state.Status)
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if lifecycleState != runtimerunlifecycle.StatePaused || state.ControlStatus != "paused" {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := (postgresRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).TransitionActive(ctx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: state.RunID,
		State: runtimerunlifecycle.StateRunning,
	}); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("continue run lifecycle: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE run_control_state
		SET control_status = 'running',
		    reason = NULLIF($2, ''),
		    controlled_by = $3,
		    updated_at = $4,
		    paused_at = NULL,
		    stopped_at = NULL
		WHERE run_id = $1::uuid
		  AND control_status = 'paused'
	`, state.RunID, req.Reason, req.ControlledBy, req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist run continue control state: %w", err)
	}
	state.Status = string(runtimerunlifecycle.StateRunning)
	state.ControlStatus = "running"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *RunLifecyclePostgresOwner) stopRunControlTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
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
	if _, _, err := (postgresRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).MarkTerminal(ctx, runtimerunlifecycle.TerminalRequest{
		RunID: state.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: req.Now.UTC(),
	}); err != nil {
		return runtimeruncontrol.State{}, runtimeruncontrol.StopFailure("terminal_state", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES ($1::uuid, 'stopped', NULLIF($2, ''), $3, $4, NULL, $4)
		ON CONFLICT (run_id) DO UPDATE SET
			control_status = 'stopped',
			reason = NULLIF($2, ''),
			controlled_by = $3,
			updated_at = $4,
			paused_at = NULL,
			stopped_at = COALESCE(run_control_state.stopped_at, $4)
	`, state.RunID, req.Reason, req.ControlledBy, req.Now.UTC()); err != nil {
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

func rejectPostgresStandingRunStopTx(ctx context.Context, tx *sql.Tx, runID string) error {
	disposition, err := storestandingdisposition.ReadByRun(ctx, tx, true, runID)
	if err != nil {
		return fmt.Errorf("inspect standing run control ownership: %w", err)
	}
	if disposition.UsesGenericRecovery() {
		return nil
	}
	return fmt.Errorf("run %s is owned by standing service %s; %s", runID, disposition.ServiceID, disposition.RunControlGuidance())
}

func (s *RunLifecyclePostgresOwner) quiesceStoppedRunWorkTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, runID, reason string, now time.Time) (int, []runtimetimercancellation.Ref, error) {
	deliveries, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, attempt, runID, "run_stopped")
	if err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("deliveries", err)
	}
	if _, err := s.pipeline.TerminalizeRunTx(ctx, attempt, runID, runtimepipelineobligation.DeadLetter("run_stopped", nil), now); err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("pipeline", err)
	}
	if _, err := terminateActiveRunSessionsTx(ctx, tx, attempt, []string{runID}, "run_stopped", now); err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("sessions", err)
	}
	cancellations, err := cancelActiveRunTimerFamiliesTx(ctx, tx, attempt, true, []string{runID}, "run_stopped", now)
	if err != nil {
		return 0, nil, runtimeruncontrol.StopFailure("timers", err)
	}
	return len(deliveries), cancellations, nil
}
