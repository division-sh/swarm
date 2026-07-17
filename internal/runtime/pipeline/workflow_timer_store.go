package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runforkrevision"
)

const (
	workflowTimerStatusActive    = "active"
	workflowTimerStatusFired     = "fired"
	workflowTimerStatusCancelled = "cancelled"
)

// WorkflowTimerActivation is the only workflow interpretation of a timers
// row. Generic schedule code must exclude rows carrying its timer_name type.
type WorkflowTimerActivation struct {
	Ref                 timeridentity.WorkflowTimerActivationRef
	RunID               string
	EntityID            string
	FlowInstance        string
	OwnerAgent          string
	EventType           string
	Payload             []byte
	FireAt              time.Time
	Recurring           bool
	RecurrenceInterval  time.Duration
	Status              string
	FiredAt             time.Time
	CreatedAt           time.Time
	SourceTimerID       string
	ForkedFromRunID     string
	ForkedFromEventID   string
	ReconstructionOwner string
}

func (a WorkflowTimerActivation) normalized() WorkflowTimerActivation {
	a.Ref = a.Ref.Normalize()
	a.RunID = strings.TrimSpace(a.RunID)
	a.EntityID = strings.TrimSpace(a.EntityID)
	a.FlowInstance = strings.Trim(strings.TrimSpace(a.FlowInstance), "/")
	a.OwnerAgent = strings.TrimSpace(a.OwnerAgent)
	a.EventType = strings.TrimSpace(a.EventType)
	a.Status = strings.ToLower(strings.TrimSpace(a.Status))
	a.SourceTimerID = strings.TrimSpace(a.SourceTimerID)
	a.ForkedFromRunID = strings.TrimSpace(a.ForkedFromRunID)
	a.ForkedFromEventID = strings.TrimSpace(a.ForkedFromEventID)
	a.ReconstructionOwner = strings.TrimSpace(a.ReconstructionOwner)
	if len(a.Payload) == 0 {
		a.Payload = []byte("{}")
	} else {
		a.Payload = append([]byte(nil), a.Payload...)
	}
	a.FireAt = canonicalWorkflowTimerTime(a.FireAt)
	a.FiredAt = canonicalWorkflowTimerTime(a.FiredAt)
	a.CreatedAt = canonicalWorkflowTimerTime(a.CreatedAt)
	return a
}

func (a WorkflowTimerActivation) validate() error {
	a = a.normalized()
	if !a.Ref.Valid() || a.Ref.ActivationID == "" {
		return fmt.Errorf("workflow timer activation identity is required")
	}
	if a.RunID == "" || a.EntityID == "" || a.FlowInstance == "" {
		return fmt.Errorf("workflow timer activation requires run, entity, and flow-instance scope")
	}
	if a.OwnerAgent == "" || a.EventType == "" {
		return fmt.Errorf("workflow timer activation requires owner agent and fire event")
	}
	if a.FireAt.IsZero() || a.CreatedAt.IsZero() {
		return fmt.Errorf("workflow timer activation requires created_at and fire_at")
	}
	if !json.Valid(a.Payload) {
		return fmt.Errorf("workflow timer business payload must be valid JSON")
	}
	if a.Recurring && a.RecurrenceInterval <= 0 {
		return fmt.Errorf("recurring workflow timer requires a positive interval")
	}
	if a.Recurring && !workflowTimerRecurringCoordinateValid(a) {
		return fmt.Errorf("recurring workflow timer fire_at is outside its persisted occurrence lattice")
	}
	if !a.Recurring && a.RecurrenceInterval != 0 {
		return fmt.Errorf("one-shot workflow timer cannot carry recurrence")
	}
	switch a.Status {
	case workflowTimerStatusActive, workflowTimerStatusFired, workflowTimerStatusCancelled:
	default:
		return fmt.Errorf("workflow timer activation has unsupported status %q", a.Status)
	}
	return nil
}

func (a WorkflowTimerActivation) occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	a = a.normalized()
	return timeridentity.WorkflowTimerOccurrenceRef{Activation: a.Ref, DueAt: a.FireAt}.Normalize()
}

func (a WorkflowTimerActivation) schedule() Schedule {
	a = a.normalized()
	return Schedule{
		RunID:        a.RunID,
		AgentID:      a.OwnerAgent,
		EventType:    a.EventType,
		Mode:         "once",
		At:           a.FireAt,
		EntityID:     a.EntityID,
		FlowInstance: a.FlowInstance,
		TaskID:       a.occurrence().TaskID(),
		TimerID:      a.Ref.ActivationID,
		Payload:      append([]byte(nil), a.Payload...),
	}
}

func canonicalWorkflowTimerTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func (s *WorkflowInstanceStore) insertWorkflowTimerActivation(ctx context.Context, activation WorkflowTimerActivation) (WorkflowTimerActivation, bool, error) {
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !authoractivity.InMutation(ctx, tx) {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation requires the pipeline mutation owner")
	}
	runID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	if runID != activation.RunID {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer run mismatch: context=%s activation=%s", runID, activation.RunID)
	}
	var result sql.Result
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULL, ?, ?, 'active', ?,
			        NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.FlowInstance, activation.EventType, string(activation.Payload), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskType(activation), activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
			VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7::jsonb, $8, $9, NULLIF($10, ''),
			        NULL, $11, $12, 'active', $13, NULLIF($14, '')::uuid, NULLIF($15, '')::uuid,
			        NULLIF($16, '')::uuid, NULLIF($17, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.FlowInstance, activation.EventType, string(activation.Payload), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskType(activation), activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	}
	if err != nil {
		return WorkflowTimerActivation{}, false, fmt.Errorf("insert workflow timer activation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	persisted, found, err := s.loadWorkflowTimerActivation(ctx, activation.Ref.ActivationID, true)
	if err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	if !found {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation %s disappeared after insert", activation.Ref.ActivationID)
	}
	if err := requireSameWorkflowTimerActivationFacts(persisted, activation); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	if rows > 0 {
		if err := declarePipelineRunForkRevisionChange(ctx, activation.RunID, runforkrevision.FamilyTimers); err != nil {
			return WorkflowTimerActivation{}, false, err
		}
	}
	return persisted, rows > 0, nil
}

func workflowTimerIntervalString(activation WorkflowTimerActivation) string {
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return ""
	}
	return activation.RecurrenceInterval.String()
}

func workflowTimerTaskType(activation WorkflowTimerActivation) string {
	if activation.Recurring {
		return "scheduled_task"
	}
	return "timer"
}

func requireSameWorkflowTimerActivationFacts(actual, expected WorkflowTimerActivation) error {
	actual, expected = actual.normalized(), expected.normalized()
	if actual.Ref != expected.Ref || actual.RunID != expected.RunID || actual.EntityID != expected.EntityID ||
		actual.FlowInstance != expected.FlowInstance || actual.OwnerAgent != expected.OwnerAgent ||
		actual.EventType != expected.EventType || actual.Recurring != expected.Recurring ||
		actual.RecurrenceInterval != expected.RecurrenceInterval || !actual.CreatedAt.Equal(expected.CreatedAt) ||
		actual.SourceTimerID != expected.SourceTimerID || actual.ForkedFromRunID != expected.ForkedFromRunID ||
		actual.ForkedFromEventID != expected.ForkedFromEventID || actual.ReconstructionOwner != expected.ReconstructionOwner ||
		!workflowTimerJSONEqual(actual.Payload, expected.Payload) || !workflowTimerReplayCoordinateMatches(actual, expected) {
		return fmt.Errorf("workflow timer activation %s conflicts with persisted facts", expected.Ref.ActivationID)
	}
	return nil
}

func workflowTimerReplayCoordinateMatches(actual, expected WorkflowTimerActivation) bool {
	if !expected.Recurring {
		return actual.FireAt.Equal(expected.FireAt)
	}
	if expected.RecurrenceInterval <= 0 || actual.FireAt.Before(expected.FireAt) {
		return false
	}
	return actual.FireAt.Sub(expected.FireAt)%expected.RecurrenceInterval == 0
}

func workflowTimerRecurringCoordinateValid(activation WorkflowTimerActivation) bool {
	activation = activation.normalized()
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return false
	}
	firstDue := canonicalWorkflowTimerTime(activation.CreatedAt.Add(activation.RecurrenceInterval))
	if activation.FireAt.Before(firstDue) {
		return false
	}
	return activation.FireAt.Sub(firstDue)%activation.RecurrenceInterval == 0
}

func workflowTimerJSONEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return bytes.Equal(left, right)
	}
	leftJSON, _ := json.Marshal(leftValue)
	rightJSON, _ := json.Marshal(rightValue)
	return bytes.Equal(leftJSON, rightJSON)
}

func (s *WorkflowInstanceStore) loadWorkflowTimerActivation(ctx context.Context, activationID string, lock bool) (WorkflowTimerActivation, bool, error) {
	activationID = strings.TrimSpace(activationID)
	if activationID == "" {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation id is required")
	}
	exec := workflowTimerQueryer(s.db)
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		exec = tx
	} else if lock {
		return WorkflowTimerActivation{}, false, fmt.Errorf("locking workflow timer load requires a pipeline transaction")
	}
	query := workflowTimerActivationSelect(false, s.isSQLite())
	if lock && !s.isSQLite() {
		query += " FOR UPDATE OF t"
	}
	row := exec.QueryRowContext(ctx, query, activationID)
	activation, err := scanWorkflowTimerActivation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkflowTimerActivation{}, false, nil
	}
	if err != nil {
		return WorkflowTimerActivation{}, false, fmt.Errorf("load workflow timer activation %s: %w", activationID, err)
	}
	return activation, true, nil
}

func (s *WorkflowInstanceStore) listWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]WorkflowTimerActivation, error) {
	exec := workflowTimerQueryer(s.db)
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		exec = tx
	}
	query := workflowTimerActivationSelect(true, s.isSQLite())
	runID = strings.TrimSpace(runID)
	entityID = strings.TrimSpace(entityID)
	args := []any{runID, entityID, timeridentity.WorkflowTimerActivationTaskPrefix() + "%"}
	if s.isSQLite() {
		args = []any{runID, runID, entityID, entityID, timeridentity.WorkflowTimerActivationTaskPrefix() + "%"}
	}
	if activeOnly {
		query += " AND t.status = 'active'"
	}
	query += " ORDER BY t.created_at, t.timer_id"
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list workflow timer activations: %w", err)
	}
	defer rows.Close()
	out := make([]WorkflowTimerActivation, 0)
	for rows.Next() {
		activation, err := scanWorkflowTimerActivation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workflow timer activation: %w", err)
		}
		out = append(out, activation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type workflowTimerQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type workflowTimerScanner interface {
	Scan(...any) error
}

func workflowTimerActivationSelect(list bool, sqlite bool) string {
	where := "t.timer_id = $1::uuid"
	if sqlite {
		where = "t.timer_id = ?"
	}
	if list {
		if sqlite {
			where = "(? = '' OR t.run_id = ?) AND (? = '' OR t.entity_id = ?) AND t.timer_name LIKE ? AND run.status IN ('running', 'paused')"
			// Duplicate run/entity arguments are expanded by the caller below.
			return workflowTimerSelectColumns() + " WHERE " + where
		}
		where = "(NULLIF($1, '') IS NULL OR t.run_id = NULLIF($1, '')::uuid) AND (NULLIF($2, '') IS NULL OR t.entity_id = NULLIF($2, '')::uuid) AND t.timer_name LIKE $3 AND run.status IN ('running', 'paused')"
	}
	return workflowTimerSelectColumns() + " WHERE " + where
}

func workflowTimerSelectColumns() string {
	return `
		SELECT
			CAST(t.timer_id AS TEXT), t.timer_name, COALESCE(CAST(t.run_id AS TEXT), ''),
			COALESCE(CAST(t.entity_id AS TEXT), ''), COALESCE(t.flow_instance, ''),
			t.fire_event, COALESCE(t.fire_payload, '{}'), t.fire_at, t.recurring,
			COALESCE(t.recurrence_interval, ''), COALESCE(t.owner_node, ''),
			COALESCE(t.owner_agent, ''), t.task_type, t.status, t.fired_at, t.created_at,
			COALESCE(CAST(t.source_timer_id AS TEXT), ''),
			COALESCE(CAST(t.forked_from_run_id AS TEXT), ''),
			COALESCE(CAST(t.forked_from_event_id AS TEXT), ''),
			COALESCE(t.reconstruction_owner, '')
		FROM timers t
		LEFT JOIN runs run ON run.run_id = t.run_id
	`
}

func scanWorkflowTimerActivation(scanner workflowTimerScanner) (WorkflowTimerActivation, error) {
	var (
		activation                                      WorkflowTimerActivation
		activationID, taskID, ownerNode, taskType       string
		payloadRaw, fireAtRaw, firedAtRaw, createdAtRaw any
		intervalRaw                                     string
	)
	if err := scanner.Scan(
		&activationID, &taskID, &activation.RunID, &activation.EntityID, &activation.FlowInstance,
		&activation.EventType, &payloadRaw, &fireAtRaw, &activation.Recurring, &intervalRaw,
		&ownerNode, &activation.OwnerAgent, &taskType, &activation.Status, &firedAtRaw, &createdAtRaw,
		&activation.SourceTimerID, &activation.ForkedFromRunID, &activation.ForkedFromEventID,
		&activation.ReconstructionOwner,
	); err != nil {
		return WorkflowTimerActivation{}, err
	}
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(taskID)
	if !ok || ref.ActivationID != strings.TrimSpace(activationID) {
		return WorkflowTimerActivation{}, fmt.Errorf("timer row %s has invalid workflow activation discriminator", activationID)
	}
	if strings.TrimSpace(ownerNode) != "" || strings.TrimSpace(activation.OwnerAgent) == "" {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s has invalid owner columns", activationID)
	}
	activation.Ref = ref
	activation.Payload = sqliteWorkflowJSONBytes(payloadRaw)
	var err error
	if activation.FireAt, _, err = sqliteWorkflowTimeValue(fireAtRaw); err != nil {
		return WorkflowTimerActivation{}, err
	}
	if activation.FiredAt, _, err = sqliteWorkflowTimeValue(firedAtRaw); err != nil {
		return WorkflowTimerActivation{}, err
	}
	if activation.CreatedAt, _, err = sqliteWorkflowTimeValue(createdAtRaw); err != nil {
		return WorkflowTimerActivation{}, err
	}
	intervalRaw = strings.TrimSpace(intervalRaw)
	if intervalRaw != "" {
		interval, ok := timeridentity.ParseDelayDuration(intervalRaw)
		if !ok {
			return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s has invalid recurrence interval %q", activationID, intervalRaw)
		}
		activation.RecurrenceInterval = interval
	}
	wantTaskType := workflowTimerTaskType(activation)
	if strings.TrimSpace(taskType) != wantTaskType {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s task_type=%s, want %s", activationID, taskType, wantTaskType)
	}
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, err
	}
	return activation, nil
}

func (s *WorkflowInstanceStore) cancelWorkflowTimerActivation(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) (WorkflowTimerActivation, bool, error) {
	ref = ref.Normalize()
	activation, found, err := s.loadWorkflowTimerActivation(ctx, ref.ActivationID, true)
	if err != nil || !found {
		return WorkflowTimerActivation{}, false, err
	}
	if activation.Ref != ref {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer cancellation identity mismatch")
	}
	if activation.Status != workflowTimerStatusActive {
		return activation, false, nil
	}
	tx, _ := sqlTxFromContext(ctx)
	var result sql.Result
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE timer_id = ? AND status = 'active'`, ref.ActivationID)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid AND status = 'active'`, ref.ActivationID)
	}
	if err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err == nil {
			err = fmt.Errorf("workflow timer cancellation changed %d rows", rows)
		}
		return WorkflowTimerActivation{}, false, err
	}
	if err := declarePipelineRunForkRevisionChange(ctx, activation.RunID, runforkrevision.FamilyTimers); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	activation.Status = workflowTimerStatusCancelled
	return activation, true, nil
}

func (s *WorkflowInstanceStore) completeWorkflowTimerOccurrence(ctx context.Context, activation WorkflowTimerActivation, occurrence timeridentity.WorkflowTimerOccurrenceRef, firedAt time.Time) (WorkflowTimerActivation, error) {
	activation = activation.normalized()
	occurrence = occurrence.Normalize()
	if activation.Status != workflowTimerStatusActive || occurrence.Activation != activation.Ref || !occurrence.DueAt.Equal(activation.FireAt) {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer occurrence is not the active persisted coordinate")
	}
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !authoractivity.InMutation(ctx, tx) {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer completion requires the pipeline mutation owner")
	}
	firedAt = canonicalWorkflowTimerTime(firedAt)
	if firedAt.IsZero() {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer completion time is required")
	}
	next := activation
	next.FiredAt = firedAt
	nextStatus := workflowTimerStatusFired
	if activation.Recurring {
		nextStatus = workflowTimerStatusActive
		next.FireAt = canonicalWorkflowTimerTime(activation.FireAt.Add(activation.RecurrenceInterval))
	}
	var result sql.Result
	var err error
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `
			UPDATE timers SET status = ?, fired_at = ?, fire_at = ?
			WHERE timer_id = ? AND status = 'active' AND fire_at = ?
		`, nextStatus, firedAt, next.FireAt, activation.Ref.ActivationID, activation.FireAt)
	} else {
		result, err = tx.ExecContext(ctx, `
			UPDATE timers SET status = $1, fired_at = $2, fire_at = $3
			WHERE timer_id = $4::uuid AND status = 'active' AND fire_at = $5
		`, nextStatus, firedAt, next.FireAt, activation.Ref.ActivationID, activation.FireAt)
	}
	if err != nil {
		return WorkflowTimerActivation{}, fmt.Errorf("complete workflow timer occurrence: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err == nil {
			err = fmt.Errorf("workflow timer completion changed %d rows", rows)
		}
		return WorkflowTimerActivation{}, err
	}
	if err := declarePipelineRunForkRevisionChange(ctx, activation.RunID, runforkrevision.FamilyTimers); err != nil {
		return WorkflowTimerActivation{}, err
	}
	next.Status = nextStatus
	return next.normalized(), nil
}

func (s *WorkflowInstanceStore) rejectObsoleteWorkflowTimerRows(ctx context.Context) error {
	exec := workflowTimerQueryer(s.db)
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		exec = tx
	}
	query := `SELECT EXISTS (SELECT 1 FROM timers WHERE owner_node = 'workflow_instance_store')`
	var exists bool
	if err := exec.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return fmt.Errorf("inspect obsolete workflow timer rows: %w", err)
	}
	if exists {
		return fmt.Errorf("obsolete workflow timer rows are unsupported; recreate the database")
	}
	return nil
}

func workflowTimerRunID(ctx context.Context, instance WorkflowInstance) string {
	return strings.TrimSpace(firstNonEmptyString(runtimecorrelation.RunIDFromContext(ctx), asString(instance.Metadata["run_id"])))
}
