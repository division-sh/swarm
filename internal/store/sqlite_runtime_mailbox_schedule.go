package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer activations must be persisted by WorkflowTimerLifecycle")
	}
	timerName, err := genericScheduleTimerName(sc)
	if err != nil {
		return err
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
	if err := s.runRuntimeMutation(ctx, "sqlite schedule upsert", func(txctx context.Context, tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunlifecycle.RequireActive(txctx, tx, sc.RunID, storerunlifecycle.DialectSQLite); err != nil {
				return err
			}
		}
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
	return s.cancelSQLiteScheduleExact(ctx, sc, true)
}

func (s *SQLiteRuntimeStore) cancelSQLiteScheduleExact(ctx context.Context, sc runtimepipeline.Schedule, requireActive bool) error {
	sc = scheduleWithContextRunID(ctx, sc)
	sc.NormalizeRunID()
	sc.NormalizeEntityID()
	sc.NormalizeFlowInstance()
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer cancellation must be owned by WorkflowTimerLifecycle")
	}
	if err := s.runRuntimeMutation(ctx, "sqlite schedule cancel", func(txctx context.Context, tx *sql.Tx) error {
		if requireActive && strings.TrimSpace(sc.RunID) != "" {
			if err := storerunlifecycle.RequireActive(txctx, tx, sc.RunID, storerunlifecycle.DialectSQLite); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(txctx, `
			UPDATE timers
			SET status = 'cancelled'
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND instr(timer_name, ?) <> 1
			  AND status = 'active'
		`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix())
		return err
	}); err != nil {
		return fmt.Errorf("cancel sqlite timer exact: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) CancelScheduleExactTerminal(ctx context.Context, sc runtimepipeline.Schedule) error {
	return s.cancelSQLiteScheduleExact(ctx, sc, true)
}

func (s *SQLiteRuntimeStore) LoadActiveSchedules(ctx context.Context) ([]runtimepipeline.Schedule, error) {
	exec := sqliteScheduleDBExecutor(ctx, s.DB)
	rows, err := exec.QueryContext(ctx, `
		SELECT COALESCE(t.run_id, ''), COALESCE(t.owner_agent, ''), t.fire_event, t.recurring,
		       COALESCE(t.recurrence_cron, ''), COALESCE(t.recurrence_interval, ''),
		       t.fire_at, COALESCE(t.entity_id, ''), COALESCE(t.flow_instance, ''), COALESCE(t.fire_payload, '{}'), COALESCE(t.reply_context_id, '')
		FROM timers t
		LEFT JOIN runs run ON run.run_id = t.run_id
		WHERE t.status = 'active'
		  AND COALESCE(t.owner_agent, '') <> ''
		  AND instr(t.timer_name, ?) <> 1
		  AND (t.run_id IS NULL OR run.status IN ('running', 'paused'))
		ORDER BY t.fire_at ASC
	`, timeridentity.WorkflowTimerActivationTaskPrefix())
	if err != nil {
		return nil, fmt.Errorf("load sqlite active schedules: %w", err)
	}
	defer rows.Close()
	out := make([]runtimepipeline.Schedule, 0)
	for rows.Next() {
		var sc runtimepipeline.Schedule
		var recurring bool
		var recurrenceCron, recurrenceInterval string
		var fireAt any
		var payload any
		var replyContextID string
		if err := rows.Scan(&sc.RunID, &sc.AgentID, &sc.EventType, &recurring, &recurrenceCron, &recurrenceInterval, &fireAt, &sc.EntityID, &sc.FlowInstance, &payload, &replyContextID); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule: %w", err)
		}
		if at, ok, err := sqliteTimeValue(fireAt); err != nil {
			return nil, fmt.Errorf("scan sqlite schedule fire_at: %w", err)
		} else if ok {
			sc.At = at
		}
		sc.Payload = jsonRawMessageValue(payload)
		sc.Mode, sc.Cron, err = persistedScheduleMode(recurring, recurrenceCron, recurrenceInterval)
		if err != nil {
			return nil, fmt.Errorf("load sqlite generic timer recurrence: %w", err)
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
	if sc.EffectiveTimerID() != "" {
		return fmt.Errorf("workflow timer completion must be owned by WorkflowTimerLifecycle")
	}
	if err := s.runRuntimeMutation(ctx, "sqlite schedule fired", func(txctx context.Context, tx *sql.Tx) error {
		if strings.TrimSpace(sc.RunID) != "" {
			if err := storerunlifecycle.RequireActive(txctx, tx, sc.RunID, storerunlifecycle.DialectSQLite); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(txctx, `
			UPDATE timers
			SET status = CASE WHEN recurring THEN 'active' ELSE 'fired' END, fired_at = ?
			WHERE COALESCE(run_id, '') = COALESCE(?, '')
			  AND owner_agent = ?
			  AND fire_event = ?
			  AND COALESCE(entity_id, '') = COALESCE(?, '')
			  AND COALESCE(flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(fire_payload, '$.__schedule_task_id'), '') = ?
			  AND instr(timer_name, ?) <> 1
			  AND status = 'active'
		`, time.Now().UTC(), sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix())
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
	sc.NormalizeTimerID()
	var active bool
	exec := sqliteScheduleDBExecutor(ctx, s.DB)
	if strings.TrimSpace(sc.RunID) != "" {
		if err := storerunlifecycle.RequireActive(ctx, exec, sc.RunID, storerunlifecycle.DialectSQLite); err != nil {
			if errors.Is(err, storerunlifecycle.ErrRunNotActive) {
				return false, nil
			}
			return false, err
		}
	}
	if sc.EffectiveTimerID() != "" {
		occurrence, ok := timeridentity.ParseWorkflowTimerOccurrenceTaskID(sc.TaskID)
		if !ok || occurrence.Activation.ActivationID != sc.EffectiveTimerID() {
			return false, fmt.Errorf("workflow timer claim identity is invalid")
		}
		err := exec.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM timers t
				LEFT JOIN runs run ON run.run_id = t.run_id
				WHERE t.timer_id = ?
				  AND t.timer_name = ?
				  AND t.run_id = ?
				  AND t.owner_agent = ?
				  AND t.fire_event = ?
				  AND t.entity_id = ?
				  AND t.flow_instance = ?
				  AND t.fire_at = ?
				  AND t.status = 'active'
				  AND run.status IN ('running', 'paused')
			)
		`, sc.TimerID, occurrence.Activation.TaskID(), sc.RunID, sc.AgentID, sc.EventType,
			sc.EntityID, sc.FlowInstance, occurrence.DueAt).Scan(&active)
		if err != nil {
			return false, fmt.Errorf("claim sqlite workflow timer ownership: %w", err)
		}
		return active, nil
	}
	err := exec.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers t
			LEFT JOIN runs run ON run.run_id = t.run_id
			WHERE COALESCE(t.run_id, '') = COALESCE(?, '')
			  AND t.owner_agent = ?
			  AND t.fire_event = ?
			  AND COALESCE(t.entity_id, '') = COALESCE(?, '')
			  AND COALESCE(t.flow_instance, '') = COALESCE(?, '')
			  AND COALESCE(json_extract(t.fire_payload, '$.__schedule_task_id'), '') = ?
			  AND instr(t.timer_name, ?) <> 1
			  AND t.status = 'active'
			  AND (t.run_id IS NULL OR run.status IN ('running', 'paused'))
		)
	`, sqliteNullUUID(sc.RunID), sc.AgentID, sc.EventType, sqliteNullUUID(sc.EntityID), sqliteNullString(sc.FlowInstance), strings.TrimSpace(sc.TaskID), timeridentity.WorkflowTimerActivationTaskPrefix()).Scan(&active)
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
	deliveries, err := s.terminalizeRunDeliveriesTx(ctx, tx, runID, "run_stopped")
	if err != nil {
		return 0, err
	}
	eventIDs := map[string]struct{}{}
	for _, delivery := range deliveries {
		eventIDs[delivery.Current.EventID] = struct{}{}
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
