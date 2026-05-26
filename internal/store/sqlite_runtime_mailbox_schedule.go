package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	runtimetools "swarm/internal/runtime/tools"
)

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
	scope := "global"
	if entityID := coalesceMailboxEntityID(item); entityID != "" {
		scope = "entity"
	}
	status, decision := mailboxStateForStoredStatus(item.Status, item.Decision)
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO mailbox (
			item_id, entity_id, flow_instance, scope, item_type, source_event_id,
			from_agent, severity, summary, payload, status, decision, decision_notes,
			notified, expires_at, created_at
		)
		VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, item.ID, sqliteNullUUID(coalesceMailboxEntityID(item)), scope, item.Type, sqliteNullUUID(item.EventID),
		sqliteNullString(item.FromAgent), normalizeMailboxSeverity(item.Priority), sqliteNullString(item.Summary), string(item.Context),
		status, sqliteNullString(decision), sqliteNullString(item.DecisionNotes), item.Notified, sqliteNullTime(item.TimeoutAt), time.Now().UTC())
	if err != nil {
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

func (s *SQLiteRuntimeStore) DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error {
	id = strings.TrimSpace(id)
	if id == "" || strings.TrimSpace(status) == "" || strings.TrimSpace(decision) == "" {
		return fmt.Errorf("mailbox id, status, and decision are required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return err
	}
	rowStatus, rowDecision := mailboxDecisionState(status, decision)
	res, err := s.DB.ExecContext(ctx, `
		UPDATE mailbox
		SET status = ?, decision = ?, decision_notes = ?, decided_at = ?
		WHERE item_id = ? AND status = 'pending'
	`, rowStatus, rowDecision, sqliteNullString(notes), time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("decide sqlite mailbox item: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("mailbox item is not pending or not found: %s", id)
	}
	return nil
}

func (s *SQLiteRuntimeStore) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.DB.QueryContext(ctx, sqliteMailboxSelectSQL(`status = 'pending' AND expires_at IS NOT NULL AND expires_at <= ?`)+` ORDER BY expires_at ASC LIMIT ?`, time.Now().UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("query expiring sqlite mailbox items: %w", err)
	}
	items, err := scanSpecMailboxItems(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	for i, item := range items {
		if _, err := s.DB.ExecContext(ctx, `
			UPDATE mailbox
			SET status = 'expired',
			    decision = COALESCE(NULLIF(decision, ''), ''),
			    decision_notes = COALESCE(NULLIF(decision_notes, ''), 'Timed out without human decision'),
			    decided_at = COALESCE(decided_at, ?)
			WHERE item_id = ? AND status = 'pending'
		`, now, item.ID); err != nil {
			return nil, fmt.Errorf("expire sqlite mailbox item: %w", err)
		}
		items[i].Status = "expired"
		if strings.TrimSpace(items[i].DecisionNotes) == "" {
			items[i].DecisionNotes = "Timed out without human decision"
		}
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
	if _, err := s.DB.ExecContext(ctx, `UPDATE mailbox SET notified = true WHERE item_id = ?`, id); err != nil {
		return fmt.Errorf("mark sqlite mailbox item notified: %w", err)
	}
	return nil
}

func sqliteMailboxSelectSQL(where string) string {
	return `
		SELECT item_id, COALESCE(source_event_id, ''), COALESCE(entity_id, ''), COALESCE(from_agent, ''),
		       item_type, COALESCE(severity, 'normal'), status, COALESCE(notified, false),
		       COALESCE(payload, '{}'), COALESCE(summary, ''), expires_at,
		       COALESCE(decision, ''), COALESCE(decision_notes, '')
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
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if err := s.CancelScheduleExact(ctx, sc); err != nil {
		return err
	}
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
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO timers (
			timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
			fire_at, recurring, recurrence_cron, owner_agent, task_type, status, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
	`, uuid.NewString(), sqliteNullUUID(sc.RunID), timerName, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance),
		sc.EventType, string(persistedSchedulePayload(sc)), fireAt.UTC(), recurring, sqliteNullString(sc.Cron), sc.AgentID, taskType, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("insert sqlite timer: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CancelScheduleExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	_, err := s.DB.ExecContext(ctx, `
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
	if err != nil {
		return fmt.Errorf("cancel sqlite timer exact: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.CancelScheduleExact(ctx, sc)
}

func (s *SQLiteRuntimeStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(run_id, ''), COALESCE(owner_agent, ''), fire_event, COALESCE(recurrence_cron, ''),
		       fire_at, COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(fire_payload, '{}')
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
		if err := rows.Scan(&sc.RunID, &sc.AgentID, &sc.EventType, &sc.Cron, &fireAt, &sc.EntityID, &sc.FlowInstance, &payload); err != nil {
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
		out = append(out, sc)
	}
	return out, rows.Err()
}

func (s *SQLiteRuntimeStore) MarkScheduleFiredExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	_, err := s.DB.ExecContext(ctx, `
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
	if err != nil {
		return fmt.Errorf("mark sqlite timer fired exact: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CompleteScheduleFireExact(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.MarkScheduleFiredExact(ctx, sc)
}

func (s *SQLiteRuntimeStore) ClaimSchedule(context.Context, runtimepipeline.Schedule) (bool, error) {
	return true, nil
}

func (s *SQLiteRuntimeStore) ReleaseSchedule(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *SQLiteRuntimeStore) ReleaseScheduleClaims(context.Context) error {
	return nil
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

func sqliteStopRunControl(ctx context.Context, tx *sql.Tx, state runtimeruncontrol.State, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	switch state.Status {
	case "running", "paused":
	case "completed", "failed", "cancelled", "forked":
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: state.RunID, CurrentStatus: state.Status}
	default:
		return runtimeruncontrol.State{}, fmt.Errorf("unsupported run status %q", state.Status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'cancelled', ended_at = COALESCE(ended_at, ?) WHERE run_id = ?`, req.Now.UTC(), state.RunID); err != nil {
		return runtimeruncontrol.State{}, fmt.Errorf("stop sqlite run: %w", err)
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
	return state, nil
}
