package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func (s *PostgresStore) StopRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.runControlTransition(ctx, req, "stop")
}

func (s *PostgresStore) PauseRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.runControlTransition(ctx, req, "pause")
}

func (s *PostgresStore) ContinueRunControl(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	return s.runControlTransition(ctx, req, "continue")
}

func (s *PostgresStore) RunDispatchBlocked(ctx context.Context, runID string) (bool, error) {
	if s == nil || s.DB == nil {
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
	err := s.DB.QueryRowContext(ctx, `
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

func (s *PostgresStore) runControlTransition(ctx context.Context, req runtimeruncontrol.TransitionRequest, action string) (runtimeruncontrol.State, error) {
	if s == nil || s.DB == nil {
		return runtimeruncontrol.State{}, fmt.Errorf("postgres store is required")
	}
	runID := nullUUIDString(req.RunID)
	if runID == "" {
		return runtimeruncontrol.State{}, fmt.Errorf("run_id is required")
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
		return runtimeruncontrol.State{}, err
	}
	var state runtimeruncontrol.State
	err := s.runAuthorActivityMutation(ctx, "postgres run control transition", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		state, err = lockRunControlState(txctx, tx, runID)
		if err != nil {
			return err
		}
		occurrenceScope, err := runtimeauthoractivity.BundleScopeForSource(txctx, state.BundleHash)
		if err != nil {
			return fmt.Errorf("run control source scope: %w", err)
		}
		switch action {
		case "stop":
			if err := rejectPostgresStandingRunStopTx(txctx, tx, runID); err != nil {
				return err
			}
			state, err = s.stopRunControlTx(txctx, tx, state, req)
		case "pause":
			state, err = pauseRunControlTx(txctx, tx, state, req)
		case "continue":
			state, err = continueRunControlTx(txctx, tx, state, req)
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
			return runtimeauthoractivity.Record(txctx, runtimeauthoractivity.Draft{
				Kind: runtimeauthoractivity.KindRunLifecycle, Transition: transition,
				SourceOwner: "runs", SourceIdentity: transitionID, DedupKey: "run-transition:" + transitionID,
				OccurredAt: req.Now.UTC(), RunID: runID, Scope: occurrenceScope,
				Projection: runtimeauthoractivity.Projection{
					SubjectType: "run", SubjectID: runID, ControlReason: req.Reason, Source: req.ControlledBy,
				},
			})
		}
		return nil
	})
	return state, err
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

func pauseRunControlTx(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	switch state.Status {
	case "running":
	case "paused":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyPaused, RunID: state.RunID, CurrentStatus: state.Status}
	case "completed", "failed", "cancelled", "forked":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, fmt.Errorf("unsupported run status %q", state.Status)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'paused'
		WHERE run_id = $1::uuid
		  AND status = 'running'
	`, state.RunID); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("pause run: %w", err)
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
	state.Status = "paused"
	state.ControlStatus = "paused"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func continueRunControlTx(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	if state.Status != "paused" || state.ControlStatus != "paused" {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'running',
		    ended_at = NULL
		WHERE run_id = $1::uuid
		  AND status = 'paused'
	`, state.RunID); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("continue run: %w", err)
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
	state.Status = "running"
	state.ControlStatus = "running"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *PostgresStore) stopRunControlTx(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	switch state.Status {
	case "running", "paused":
	case "completed", "failed", "cancelled", "forked":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, fmt.Errorf("unsupported run status %q", state.Status)
	}
	abandoned, err := s.quiesceStoppedRunWorkTx(ctx, tx, state.RunID, req.Reason, req.Now.UTC())
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if err := supersedeDecisionCardsForRun(ctx, tx, state.RunID, "run_stopped", req.Now.UTC(), false, true); err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, err := storerunlifecycle.MarkTerminal(ctx, tx, state.RunID, "cancelled", nil, req.Now.UTC(), runLifecycleOptions()); err != nil {
		return runtimeruncontrol.State{}, err
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
		return runtimeruncontrol.State{}, fmt.Errorf("persist run stop control state: %w", err)
	}
	state.Status = "cancelled"
	state.ControlStatus = "stopped"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	state.AbandonedDeliveries = abandoned
	return state, nil
}

func rejectPostgresStandingRunStopTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var serviceID string
	err := tx.QueryRowContext(ctx, `SELECT service_id::text FROM standing_services WHERE current_run_id = $1::uuid`, runID).Scan(&serviceID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect standing run control ownership: %w", err)
	}
	return fmt.Errorf("run %s is owned by standing service %s; use `swarm standing suspend %s` or `swarm standing reset %s`", runID, serviceID, serviceID, serviceID)
}

func (s *PostgresStore) quiesceStoppedRunWorkTx(ctx context.Context, tx *sql.Tx, runID, reason string, now time.Time) (int, error) {
	deliveries, err := lockActiveRunQuiescenceDeliveriesTx(ctx, tx, []string{runID})
	if err != nil {
		return 0, err
	}
	eventIDs := map[string]struct{}{}
	for _, delivery := range deliveries {
		if err := terminalizeActiveRunQuiescenceDeliveryTx(ctx, tx, delivery, "run_stopped", reason, now); err != nil {
			return 0, err
		}
		eventIDs[delivery.EventID] = struct{}{}
	}
	for eventID := range eventIDs {
		if err := upsertActiveRunQuiescencePipelineReceiptTx(ctx, tx, eventID, "run_stopped", reason, now); err != nil {
			return 0, err
		}
	}
	if _, err := terminateActiveRunSessionsTx(ctx, tx, []string{runID}, "run_stopped", now); err != nil {
		return 0, err
	}
	if _, err := cancelActiveRunTimersTx(ctx, tx, []string{runID}); err != nil {
		return 0, err
	}
	return len(deliveries), nil
}

func (s *PostgresStore) abandonPendingRunDeliveriesTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT d.delivery_id::text, d.event_id::text, d.subscriber_type, d.subscriber_id, COALESCE(d.retry_count, 0)
		FROM event_deliveries d
		WHERE d.run_id = $1::uuid
		  AND d.status = 'pending'
		ORDER BY d.event_id::text ASC, d.subscriber_type ASC, d.subscriber_id ASC, d.delivery_id::text ASC
		FOR UPDATE
	`, runID)
	if err != nil {
		return 0, fmt.Errorf("query pending run deliveries: %w", err)
	}
	defer rows.Close()
	type target struct {
		deliveryID     string
		eventID        string
		subscriberType string
		subscriberID   string
		retryCount     int
	}
	targets := []target{}
	for rows.Next() {
		var item target
		if err := rows.Scan(&item.deliveryID, &item.eventID, &item.subscriberType, &item.subscriberID, &item.retryCount); err != nil {
			return 0, fmt.Errorf("scan pending run delivery: %w", err)
		}
		item.deliveryID = strings.TrimSpace(item.deliveryID)
		item.eventID = strings.TrimSpace(item.eventID)
		item.subscriberType = strings.TrimSpace(item.subscriberType)
		item.subscriberID = strings.TrimSpace(item.subscriberID)
		if item.deliveryID != "" && item.eventID != "" && item.subscriberType != "" && item.subscriberID != "" {
			targets = append(targets, item)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read pending run deliveries: %w", err)
	}
	eventsTouched := map[string]struct{}{}
	abandoned := 0
	for _, item := range targets {
		applied, err := s.abandonPendingRunDeliveryTx(ctx, tx, item.deliveryID, item.eventID, item.subscriberType, item.subscriberID, item.retryCount)
		if err != nil {
			return 0, err
		}
		if !applied {
			continue
		}
		eventsTouched[item.eventID] = struct{}{}
		abandoned++
	}
	for eventID := range eventsTouched {
		var active bool
		if err := tx.QueryRowContext(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM event_deliveries
					WHERE event_id = $1::uuid
				  AND status IN ('pending', 'in_progress')
			)
		`, eventID).Scan(&active); err != nil {
			return 0, fmt.Errorf("check stopped run event active deliveries: %w", err)
		}
		if !active {
			var hasPipelineReceipt bool
			if err := tx.QueryRowContext(ctx, `
					SELECT EXISTS (
						SELECT 1
						FROM event_receipts
						WHERE event_id = $1::uuid
						  AND subscriber_type = 'platform'
						  AND subscriber_id = 'pipeline'
					)
				`, eventID).Scan(&hasPipelineReceipt); err != nil {
				return 0, fmt.Errorf("check stopped run pipeline receipt: %w", err)
			}
			if !hasPipelineReceipt {
				if err := s.upsertPipelineReceiptSpec(ctx, tx, eventID, "dead_letter", nil); err != nil {
					return 0, fmt.Errorf("mark stopped run pipeline receipt: %w", err)
				}
			}
		}
	}
	return abandoned, nil
}

func (s *PostgresStore) abandonPendingRunDeliveryTx(ctx context.Context, tx *sql.Tx, deliveryID, eventID, subscriberType, subscriberID string, retryCount int) (bool, error) {
	reasonCode := "run_stopped"
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'dead_letter',
		    retry_count = $2,
		    reason_code = $3,
		    failure = NULL,
		    active_session_id = NULL,
		    started_at = COALESCE(started_at, created_at),
		    delivered_at = now()
		WHERE delivery_id = $1::uuid
		  AND status = 'pending'
	`, deliveryID, retryCount, reasonCode)
	if err != nil {
		return false, fmt.Errorf("abandon stopped run delivery %s: %w", deliveryID, err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return false, nil
	}
	switch subscriberType {
	case "agent":
		state := agentReceiptWriteState{
			finalStatus:  runtimemanager.ReceiptStatusDeadLetter,
			retryCount:   retryCount,
			reasonCode:   reasonCode,
			deliveryCode: "dead_letter",
		}
		if err := s.upsertAgentReceiptRowTx(ctx, tx, eventID, subscriberID, state); err != nil {
			return false, fmt.Errorf("abandon stopped run agent receipt: %w", err)
		}
	case "node":
		if err := s.upsertStoppedRunNodeReceiptTx(ctx, tx, eventID, subscriberID, reasonCode); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unsupported stopped run delivery subscriber_type %q", subscriberType)
	}
	return true, nil
}

func (s *PostgresStore) upsertStoppedRunNodeReceiptTx(ctx context.Context, tx *sql.Tx, eventID, nodeID, reasonCode string) error {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'node', $2, e.entity_id, e.flow_instance,
			'dead_letter', NULLIF($3, ''), '{}'::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`, eventID, nodeID, reasonCode)
	if err != nil {
		return fmt.Errorf("upsert stopped run node receipt: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("upsert stopped run node receipt: event %s not found", eventID)
	}
	return nil
}
