package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

type sqliteScheduleExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *SQLiteRuntimeStore) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	if strings.TrimSpace(item.Type) == "" {
		return "", fmt.Errorf("mailbox item type is required")
	}
	if strings.TrimSpace(item.ID) == "" {
		item.ID = uuid.NewString()
	}
	if strings.TrimSpace(item.Priority) == "" {
		item.Priority = "normal"
	}
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "pending"
	}
	if len(item.Context) == 0 {
		item.Context = []byte("{}")
	}
	if err := validateGenericMailboxNotice(item.Type, item.Context); err != nil {
		return "", err
	}
	if strings.TrimSpace(item.ReplyContextID) == "" {
		item.ReplyContextID = events.DeliveryContextFromContext(ctx).ReplyContextID()
	}
	scope := "global"
	if entityID := coalesceMailboxEntityID(item); entityID != "" {
		scope = "entity"
	}
	status, decision := mailboxStateForStoredStatus(item.Status, item.Decision)
	if err := s.runRuntimeMutation(ctx, "sqlite mailbox insert", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			INSERT INTO mailbox (
				item_id, entity_id, flow_instance, scope, item_type, source_event_id,
				from_agent, severity, summary, payload, status, decision, decision_notes,
				notified, expires_at, reply_context_id, created_at
			)
			VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)
		`, item.ID, sqliteNullUUID(coalesceMailboxEntityID(item)), strings.Trim(strings.TrimSpace(item.FlowInstance), "/"), scope, item.Type, sqliteNullUUID(item.EventID),
			sqliteNullString(item.FromAgent), normalizeMailboxSeverity(item.Priority), sqliteNullString(item.Summary), string(item.Context),
			status, sqliteNullString(decision), sqliteNullString(item.DecisionNotes), item.Notified, sqliteNullTime(item.TimeoutAt), strings.TrimSpace(item.ReplyContextID), time.Now().UTC())
		return err
	}); err != nil {
		return "", fmt.Errorf("insert sqlite mailbox item: %w", err)
	}
	return item.ID, nil
}

func (s *SQLiteRuntimeStore) ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, sqliteMailboxSelectSQL(`status = ?`)+` ORDER BY created_at ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *SQLiteRuntimeStore) CountMailboxItems(ctx context.Context, status string) (int, error) {
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return 0, err
	}
	var n int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE status = ?`, status).Scan(&n); err != nil {
		return 0, fmt.Errorf("count sqlite mailbox items: %w", err)
	}
	return n, nil
}

func (s *SQLiteRuntimeStore) GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox id is required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return runtimetools.MailboxItem{}, err
	}
	rows, err := s.DB.QueryContext(ctx, sqliteMailboxSelectSQL(`item_id = ?`), id)
	if err != nil {
		return runtimetools.MailboxItem{}, fmt.Errorf("get sqlite mailbox item: %w", err)
	}
	defer rows.Close()
	items, err := scanSpecMailboxItems(rows)
	if err != nil {
		return runtimetools.MailboxItem{}, err
	}
	if len(items) == 0 {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox item not found: %s", id)
	}
	return items[0], nil
}

func (s *SQLiteRuntimeStore) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 200
	}
	var items []runtimetools.MailboxItem
	if err := s.runRuntimeMutation(ctx, "sqlite mailbox expiry", func(txctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(txctx, sqliteMailboxSelectSQL(`status = 'pending' AND expires_at IS NOT NULL AND expires_at <= ?`)+` ORDER BY expires_at ASC LIMIT ?`, time.Now().UTC(), limit)
		if err != nil {
			return fmt.Errorf("query expiring sqlite mailbox items: %w", err)
		}
		items, err = scanSpecMailboxItems(rows)
		rows.Close()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		for i, item := range items {
			if _, err := tx.ExecContext(txctx, `
				UPDATE mailbox
				SET status = 'expired',
				    decision = COALESCE(NULLIF(decision, ''), ''),
				    decision_notes = COALESCE(NULLIF(decision_notes, ''), 'Timed out without human decision'),
				    decided_at = COALESCE(decided_at, ?)
				WHERE item_id = ? AND status = 'pending'
			`, now, item.ID); err != nil {
				return fmt.Errorf("expire sqlite mailbox item: %w", err)
			}
			items[i].Status = "expired"
			if strings.TrimSpace(items[i].DecisionNotes) == "" {
				items[i].DecisionNotes = "Timed out without human decision"
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *SQLiteRuntimeStore) ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, sqliteMailboxSelectSQL(`status = 'pending' AND severity = 'critical' AND COALESCE(notified, false) = false`)+` ORDER BY created_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite unnotified critical mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *SQLiteRuntimeStore) MarkMailboxItemNotified(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("mailbox id is required")
	}
	if err := s.runRuntimeMutation(ctx, "sqlite mailbox notified", func(txctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(txctx, `UPDATE mailbox SET notified = true WHERE item_id = ?`, id)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows == 0 {
			return ErrMailboxV1NotFound
		}
		return nil
	}); err != nil {
		return fmt.Errorf("mark sqlite mailbox item notified: %w", err)
	}
	return nil
}

func sqliteMailboxSelectSQL(where string) string {
	return `
		SELECT item_id, COALESCE(source_event_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(from_agent, ''),
		       item_type, COALESCE(severity, 'normal'), status, COALESCE(notified, false),
		       COALESCE(payload, '{}'), COALESCE(summary, ''), expires_at,
		       COALESCE(decision, ''), COALESCE(decision_notes, ''), COALESCE(reply_context_id, '')
		FROM mailbox
		WHERE ` + where
}

func (s *SQLiteRuntimeStore) UpsertSchedule(ctx context.Context, sc runtimepipeline.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	sc = scheduleWithContextRunID(ctx, sc)
	if sc.Context.Empty() {
		sc.Context = events.DeliveryContextFromContext(ctx)
	}
	sc.NormalizeDeliveryContext()
	if !sc.Context.Empty() && strings.EqualFold(strings.TrimSpace(sc.Mode), "cron") {
		return fmt.Errorf("recurring schedules cannot carry an open reply context")
	}
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	fireAt := sc.At
	if fireAt.IsZero() {
		fireAt = time.Now().UTC()
	}
	recurring := strings.EqualFold(strings.TrimSpace(sc.Mode), "cron")
	taskType := "timer"
	if recurring {
		taskType = "scheduled_task"
		if sc.EntityID == "" {
			taskType = "global_recurring"
		}
	}
	timerName := strings.TrimSpace(sc.TaskID)
	if timerName == "" {
		timerName = strings.TrimSpace(sc.EventType)
	}
	if err := s.runRuntimeMutation(ctx, "sqlite schedule upsert", func(txctx context.Context, tx *sql.Tx) error {
		if err := s.CancelScheduleExact(txctx, sc); err != nil {
			return err
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, recurrence_cron, owner_agent, reply_context_id, task_type, status, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, 'active', ?)
		`, uuid.NewString(), sqliteNullUUID(sc.RunID), timerName, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance),
			sc.EventType, string(persistedSchedulePayload(sc)), fireAt.UTC(), recurring, sqliteNullString(sc.Cron), sc.AgentID, sc.Context.ReplyContextID(), taskType, time.Now().UTC())
		return err
	}); err != nil {
		return fmt.Errorf("insert sqlite timer: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := s.runRuntimeMutation(ctx, "sqlite schedule cancel", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			UPDATE timers
			SET status = 'cancelled'
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND status = 'active'
		`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID))
		return err
	}); err != nil {
		return fmt.Errorf("cancel sqlite timer exact: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.CancelScheduleExact(ctx, sc)
}

func (s *SQLiteRuntimeStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	exec := sqliteScheduleDBExecutor(ctx, s.DB)
	rows, err := exec.QueryContext(ctx, `
		SELECT COALESCE(run_id, ''), COALESCE(owner_agent, ''), fire_event, COALESCE(recurrence_cron, ''),
		       fire_at, COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(fire_payload, '{}'), COALESCE(reply_context_id, '')
		FROM timers
		WHERE status = 'active'
		ORDER BY fire_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("load sqlite active schedules: %w", err)
	}
	defer rows.Close()
	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var sc runtimepipeline.Schedule
		var fireAt any
		var payload any
		var replyContextID string
		if err := rows.Scan(&sc.RunID, &sc.AgentID, &sc.EventType, &sc.Cron, &fireAt, &sc.EntityID, &sc.FlowInstance, &payload, &replyContextID); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule: %w", err)
		}
		if at, ok, err := sqliteTimeValue(fireAt); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule fire_at: %w", err)
		} else if ok {
			sc.At = at
		}
		sc.Payload = jsonRawMessageValue(payload)
		if strings.TrimSpace(sc.Cron) != "" {
			sc.Mode = "cron"
		} else {
			sc.Mode = "once"
		}
		sc.TaskID = scheduleTaskIDFromPayload(sc.Payload)
		if replyContextID != "" {
			sc.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: replyContextID}}
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

func (s *SQLiteRuntimeStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := s.runRuntimeMutation(ctx, "sqlite schedule fired", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			UPDATE timers
			SET status = 'fired', fired_at = ?
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND status = 'active'
		`, time.Now().UTC(), sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID))
		return err
	}); err != nil {
		return fmt.Errorf("mark sqlite timer fired exact: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.MarkScheduleFiredExact(ctx, sc)
}

func (s *SQLiteRuntimeStore) ClaimSchedule(ctx context.Context, sc runtimepipeline.Schedule) (bool, error) {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	var active bool
	exec := sqliteScheduleDBExecutor(ctx, s.DB)
	err := exec.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND status = 'active'
		)
	`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID)).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("claim sqlite schedule ownership: %w", err)
	}
	return active, nil
}

func (s *SQLiteRuntimeStore) ReleaseSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *SQLiteRuntimeStore) ReleaseScheduleClaims(context.Context) error {
	return nil
}

func sqliteScheduleDBExecutor(ctx context.Context, db *sql.DB) sqliteScheduleExecutor {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return tx
	}
	return db
}

func scheduleTaskIDFromPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return ""
	}
	raw, ok := decoded["__schedule_task_id"]
	if !ok || raw == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func sqliteNullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func sqliteLoadRunControlState(ctx context.Context, tx *sql.Tx, runID string) (runtimeruncontrol.State, error) {
	var state runtimeruncontrol.State
	var controlStatus, reason, controlledBy sql.NullString
	var updatedAt any
	err := tx.QueryRowContext(ctx, `
		SELECT r.run_id, COALESCE(r.status, ''), COALESCE(rc.control_status, ''),
		       COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, ''), rc.updated_at
		FROM runs r
		LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = ?
	`, runID).Scan(&state.RunID, &state.Status, &controlStatus, &reason, &controlledBy, &updatedAt)
	if err == sql.ErrNoRows {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("load sqlite run control state: %w", err)
	}
	state.ControlStatus = strings.TrimSpace(controlStatus.String)
	state.Reason = strings.TrimSpace(reason.String)
	state.ControlledBy = strings.TrimSpace(controlledBy.String)
	if at, ok, err := sqliteTimeValue(updatedAt); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("scan sqlite run control updated_at: %w", err)
	} else if ok {
		state.UpdatedAt = at
	}
	return state, nil
}

func sqlitePauseRunControl(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	switch state.Status {
	case "running":
	case "paused":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyPaused, RunID: state.RunID, CurrentStatus: state.Status}
	case "completed", "failed", "cancelled", "forked":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, fmt.Errorf("unsupported run status %q", state.Status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'paused' WHERE run_id = ? AND status = 'running'`, state.RunID); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("pause sqlite run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'paused', ?, ?, ?, ?, NULL)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'paused', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, paused_at = COALESCE(run_control_state.paused_at, excluded.paused_at),
			stopped_at = NULL
	`, state.RunID, sqliteNullString(req.Reason), req.ControlledBy, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run pause control state: %w", err)
	}
	state.Status = "paused"
	state.ControlStatus = "paused"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func sqliteContinueRunControl(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	if state.Status != "paused" {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: state.RunID, CurrentStatus: state.Status}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'running' WHERE run_id = ? AND status = 'paused'`, state.RunID); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("continue sqlite run: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'running', ?, ?, ?, NULL, NULL)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'running', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, stopped_at = NULL
	`, state.RunID, sqliteNullString(req.Reason), req.ControlledBy, req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run continue control state: %w", err)
	}
	state.Status = "running"
	state.ControlStatus = "running"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	return state, nil
}

func (s *SQLiteRuntimeStore) sqliteStopRunControl(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	switch state.Status {
	case "running", "paused":
	case "completed", "failed", "cancelled", "forked":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, fmt.Errorf("unsupported run status %q", state.Status)
	}
	abandoned, err := s.sqliteQuiesceStoppedRunWorkTx(ctx, tx, state.RunID, req.Reason, req.Now.UTC())
	if err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, err := s.sqliteMarkRunTerminalTx(ctx, tx, state.RunID, "cancelled", nil, req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'stopped', ?, ?, ?, NULL, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'stopped', reason = excluded.reason, controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at, stopped_at = excluded.stopped_at
	`, state.RunID, sqliteNullString(req.Reason), req.ControlledBy, req.Now.UTC(), req.Now.UTC()); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("persist sqlite run stop control state: %w", err)
	}
	state.Status = "cancelled"
	state.ControlStatus = "stopped"
	state.Reason = req.Reason
	state.ControlledBy = req.ControlledBy
	state.UpdatedAt = req.Now.UTC()
	state.AbandonedDeliveries = abandoned
	return state, nil
}

func (s *SQLiteRuntimeStore) sqliteQuiesceStoppedRunWorkTx(ctx context.Context, tx *sql.Tx, runID, reason string, now time.Time) (int, error) {
	deliveries, err := sqliteLockActiveRunQuiescenceDeliveriesTx(ctx, tx, []string{runID})
	if err != nil {
		return 0, err
	}
	eventIDs := map[string]struct{}{}
	for _, delivery := range deliveries {
		if err := sqliteTerminalizeActiveRunQuiescenceDeliveryTx(ctx, tx, delivery, "run_stopped", reason, now); err != nil {
			return 0, err
		}
		eventIDs[delivery.EventID] = struct{}{}
	}
	for eventID := range eventIDs {
		if err := sqliteUpsertActiveRunQuiescencePipelineReceiptTx(ctx, tx, eventID, "run_stopped", reason, now); err != nil {
			return 0, err
		}
	}
	if _, err := sqliteTerminateActiveRunSessionsTx(ctx, tx, []string{runID}, "run_stopped", now); err != nil {
		return 0, err
	}
	if _, err := sqliteCancelActiveRunTimersTx(ctx, tx, []string{runID}); err != nil {
		return 0, err
	}
	return len(deliveries), nil
}

func (s *SQLiteRuntimeStore) sqliteAbandonPendingRunDeliveriesTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT delivery_id, event_id, subscriber_type, subscriber_id, COALESCE(retry_count, 0)
		FROM event_deliveries
		WHERE run_id = ?
		  AND status = 'pending'
		ORDER BY event_id ASC, subscriber_type ASC, subscriber_id ASC, delivery_id ASC
	`, runID)
	if err != nil {
		return 0, fmt.Errorf("query sqlite pending run deliveries: %w", err)
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
			return 0, fmt.Errorf("scan sqlite pending run delivery: %w", err)
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
		return 0, fmt.Errorf("read sqlite pending run deliveries: %w", err)
	}

	eventsTouched := map[string]struct{}{}
	abandoned := 0
	for _, item := range targets {
		applied, err := s.sqliteAbandonPendingRunDeliveryTx(ctx, tx, item.deliveryID, item.eventID, item.subscriberType, item.subscriberID, item.retryCount)
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
					WHERE event_id = ?
				  AND status IN ('pending', 'in_progress')
			)
		`, eventID).Scan(&active); err != nil {
			return 0, fmt.Errorf("check sqlite stopped run event active deliveries: %w", err)
		}
		if !active {
			var hasPipelineReceipt bool
			if err := tx.QueryRowContext(ctx, `
					SELECT EXISTS (
						SELECT 1
						FROM event_receipts
						WHERE event_id = ?
						  AND subscriber_type = 'platform'
						  AND subscriber_id = 'pipeline'
					)
				`, eventID).Scan(&hasPipelineReceipt); err != nil {
				return 0, fmt.Errorf("check sqlite stopped run pipeline receipt: %w", err)
			}
			if !hasPipelineReceipt {
				if err := s.UpsertPipelineReceiptTx(ctx, tx, eventID, "dead_letter", nil); err != nil {
					return 0, fmt.Errorf("mark sqlite stopped run pipeline receipt: %w", err)
				}
			}
		}
	}
	return abandoned, nil
}

func (s *SQLiteRuntimeStore) sqliteAbandonPendingRunDeliveryTx(ctx context.Context, tx *sql.Tx, deliveryID, eventID, subscriberType, subscriberID string, retryCount int) (bool, error) {
	reasonCode := "run_stopped"
	now := s.now()
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'dead_letter',
		    retry_count = ?,
		    reason_code = ?,
		    failure = NULL,
		    active_session_id = NULL,
		    started_at = COALESCE(started_at, created_at),
		    delivered_at = ?
		WHERE delivery_id = ?
		  AND status = 'pending'
	`, retryCount, reasonCode, now, deliveryID)
	if err != nil {
		return false, fmt.Errorf("abandon sqlite stopped run delivery %s: %w", deliveryID, err)
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
		if err := s.upsertSQLiteStoppedRunAgentReceiptTx(ctx, tx, eventID, subscriberID, state, now); err != nil {
			return false, err
		}
	case "node":
		if err := s.upsertSQLiteStoppedRunNodeReceiptTx(ctx, tx, eventID, subscriberID, reasonCode, now); err != nil {
			return false, err
		}
	default:
		return false, fmt.Errorf("unsupported sqlite stopped run delivery subscriber_type %q", subscriberType)
	}
	return true, nil
}

func (s *SQLiteRuntimeStore) upsertSQLiteStoppedRunAgentReceiptTx(ctx context.Context, tx *sql.Tx, eventID, agentID string, state agentReceiptWriteState, now time.Time) error {
	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(state.finalStatus, state.reasonCode, state.retryCount))
	if err != nil {
		return fmt.Errorf("marshal sqlite stopped run agent receipt side effects: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'agent', ?, e.entity_id, e.flow_instance,
			?, ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = excluded.entity_id,
			flow_instance = excluded.flow_instance,
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, uuid.NewString(), agentID, mapManagerReceiptStatusToOutcome(state.finalStatus), sqliteNullString(state.reasonCode), string(sideEffects), now, eventID); err != nil {
		return fmt.Errorf("upsert sqlite stopped run agent receipt: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) upsertSQLiteStoppedRunNodeReceiptTx(ctx context.Context, tx *sql.Tx, eventID, nodeID, reasonCode string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'node', ?, e.entity_id, e.flow_instance,
			'dead_letter', ?, '{}', ?
		FROM events e
		WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = excluded.entity_id,
			flow_instance = excluded.flow_instance,
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, uuid.NewString(), nodeID, sqliteNullString(reasonCode), now, eventID); err != nil {
		return fmt.Errorf("upsert sqlite stopped run node receipt: %w", err)
	}
	return nil
}
