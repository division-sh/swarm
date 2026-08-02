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

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const (
	workflowTimerStatusActive    = "active"
	workflowTimerStatusFired     = "fired"
	workflowTimerStatusCancelled = "cancelled"
)

// WorkflowTimerActivation is the only workflow interpretation of a
// task_type=workflow_timer row.
type WorkflowTimerActivation struct {
	Ref                 timeridentity.WorkflowTimerActivationRef
	RunID               string
	EntityID            string
	Route               runtimeflowidentity.Route
	RoutingSource       events.RoutingSource
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

func (a WorkflowTimerActivation) Canonical() WorkflowTimerActivation { return a.normalized() }

func (a WorkflowTimerActivation) Validate() error { return a.validate() }

func (a WorkflowTimerActivation) Occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	return a.occurrence()
}

func (a WorkflowTimerActivation) normalized() WorkflowTimerActivation {
	a.Ref = a.Ref.Normalize()
	a.RunID = strings.TrimSpace(a.RunID)
	a.EntityID = strings.TrimSpace(a.EntityID)
	a.Route = runtimeflowidentity.StoredRoute(a.Route.ScopeKey, a.Route.InstanceID, a.Route.InstancePath)
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
	if a.RunID == "" || a.EntityID == "" || !a.Route.Valid() {
		return fmt.Errorf("workflow timer activation requires run, entity, and exact route scope")
	}
	switch a.RoutingSource.Kind() {
	case events.RoutingSourceRoot:
		if a.RoutingSource.Route() != (events.RouteIdentity{EntityID: a.EntityID}) {
			return fmt.Errorf("root workflow timer activation requires its exact persisted entity source")
		}
	case events.RoutingSourceFlowOwnedControl:
		if a.RoutingSource.Route().FlowInstance != a.Route.InstancePath || a.RoutingSource.Route().EntityID != a.EntityID {
			return fmt.Errorf("flow workflow timer activation requires its exact persisted flow source")
		}
	default:
		return fmt.Errorf("workflow timer activation requires exact root or flow-owned routing provenance")
	}
	if a.OwnerAgent == "" || a.EventType == "" {
		return fmt.Errorf("workflow timer activation requires owner agent and fire event")
	}
	if _, err := events.AdmitRuntimeControlEventType(events.EventType(a.EventType), a.RoutingSource); err != nil {
		return fmt.Errorf("workflow timer activation event/source admission: %w", err)
	}
	if a.FireAt.IsZero() || a.CreatedAt.IsZero() {
		return fmt.Errorf("workflow timer activation requires created_at and fire_at")
	}
	if a.FireAt.Before(a.CreatedAt) {
		return fmt.Errorf("workflow timer fire_at cannot precede created_at")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(a.Payload, &payload); err != nil || payload == nil {
		return fmt.Errorf("workflow timer business payload must be a JSON object")
	}
	if _, reserved := payload["__schedule_task_id"]; reserved {
		return fmt.Errorf("workflow timer business payload cannot carry generic schedule identity")
	}
	lineageFacts := 0
	for _, fact := range []string{a.SourceTimerID, a.ForkedFromRunID, a.ForkedFromEventID, a.ReconstructionOwner} {
		if fact != "" {
			lineageFacts++
		}
	}
	if lineageFacts != 0 && lineageFacts != 4 {
		return fmt.Errorf("workflow timer fork lineage must be complete or absent")
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
	if a.Recurring {
		if a.Status != workflowTimerStatusActive && a.Status != workflowTimerStatusCancelled {
			return fmt.Errorf("recurring workflow timer has unreachable status %q", a.Status)
		}
		if !a.FiredAt.IsZero() {
			previousDue := canonicalWorkflowTimerTime(a.FireAt.Add(-a.RecurrenceInterval))
			if a.FiredAt.Before(previousDue) {
				return fmt.Errorf("recurring workflow timer fired_at precedes its previous occurrence")
			}
		}
		return nil
	}
	switch a.Status {
	case workflowTimerStatusActive, workflowTimerStatusCancelled:
		if !a.FiredAt.IsZero() {
			return fmt.Errorf("unfired one-shot workflow timer cannot carry fired_at")
		}
	case workflowTimerStatusFired:
		if a.FiredAt.IsZero() || a.FiredAt.Before(a.FireAt) {
			return fmt.Errorf("fired one-shot workflow timer requires fired_at at or after fire_at")
		}
	default:
		return fmt.Errorf("workflow timer activation has unsupported status %q", a.Status)
	}
	return nil
}

func (a WorkflowTimerActivation) occurrence() timeridentity.WorkflowTimerOccurrenceRef {
	a = a.normalized()
	return timeridentity.WorkflowTimerOccurrenceRef{Activation: a.Ref, DueAt: a.FireAt}.Normalize()
}

func canonicalWorkflowTimerTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func (s *workflowInstanceStore) insertWorkflowTimerActivation(ctx context.Context, activation WorkflowTimerActivation) (WorkflowTimerActivation, bool, error) {
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	routingSourceRaw, err := json.Marshal(activation.RoutingSource)
	if err != nil {
		return WorkflowTimerActivation{}, false, fmt.Errorf("encode workflow timer routing source: %w", err)
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
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULL, ?, 'system', ?, 'active', ?,
			        NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSourceRaw), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskFamily, activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
			VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, NULLIF($13, ''),
			        NULL, $14, 'system', $15, 'active', $16, NULLIF($17, '')::uuid, NULLIF($18, '')::uuid,
			        NULLIF($19, '')::uuid, NULLIF($20, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSourceRaw), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskFamily, activation.CreatedAt, activation.SourceTimerID,
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
	return persisted, rows > 0, nil
}

func workflowTimerIntervalString(activation WorkflowTimerActivation) string {
	if !activation.Recurring || activation.RecurrenceInterval <= 0 {
		return ""
	}
	return activation.RecurrenceInterval.String()
}

func requireSameWorkflowTimerActivationFacts(actual, expected WorkflowTimerActivation) error {
	actual, expected = actual.normalized(), expected.normalized()
	if actual.Ref != expected.Ref || actual.RunID != expected.RunID || actual.EntityID != expected.EntityID ||
		actual.Route != expected.Route || actual.RoutingSource.Kind() != expected.RoutingSource.Kind() || actual.RoutingSource.Route() != expected.RoutingSource.Route() || actual.OwnerAgent != expected.OwnerAgent ||
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

func (s *workflowInstanceStore) loadWorkflowTimerActivation(ctx context.Context, activationID string, lock bool) (WorkflowTimerActivation, bool, error) {
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

func (s *workflowInstanceStore) listWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]WorkflowTimerActivation, error) {
	exec := workflowTimerQueryer(s.db)
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		exec = tx
	}
	query := workflowTimerActivationSelect(true, s.isSQLite())
	runID = strings.TrimSpace(runID)
	entityID = strings.TrimSpace(entityID)
	activeStates := runtimerunlifecycle.ActiveStates()
	args := []any{runID, entityID, string(activeStates[0]), string(activeStates[1])}
	if s.isSQLite() {
		args = []any{
			runID,
			runID,
			entityID,
			entityID,
			string(activeStates[0]),
			string(activeStates[1]),
		}
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
			where = "(? = '' OR t.run_id = ?) AND (? = '' OR t.entity_id = ?) AND t.task_type = 'workflow_timer' AND run.status IN (?, ?)"
			// Duplicate run/entity arguments are expanded by the caller below.
			return workflowTimerSelectColumns() + " WHERE " + where
		}
		where = "(NULLIF($1, '') IS NULL OR t.run_id = NULLIF($1, '')::uuid) AND (NULLIF($2, '') IS NULL OR t.entity_id = NULLIF($2, '')::uuid) AND t.task_type = 'workflow_timer' AND run.status IN ($3, $4)"
	} else {
		where += " AND t.task_type = 'workflow_timer'"
	}
	return workflowTimerSelectColumns() + " WHERE " + where
}

func workflowTimerSelectColumns() string {
	return `
		SELECT
			CAST(t.timer_id AS TEXT), t.timer_name, COALESCE(CAST(t.run_id AS TEXT), ''),
			COALESCE(CAST(t.entity_id AS TEXT), ''), COALESCE(t.flow_scope_key, ''),
			COALESCE(t.flow_instance_id, ''), COALESCE(t.flow_instance, ''),
			t.fire_event, COALESCE(t.fire_payload, '{}'), t.routing_source, t.fire_at, t.recurring,
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
		activation                                                        WorkflowTimerActivation
		activationID, taskID, ownerNode, taskType                         string
		payloadRaw, routingSourceRaw, fireAtRaw, firedAtRaw, createdAtRaw any
		intervalRaw                                                       string
	)
	if err := scanner.Scan(
		&activationID, &taskID, &activation.RunID, &activation.EntityID, &activation.Route.ScopeKey,
		&activation.Route.InstanceID, &activation.Route.InstancePath,
		&activation.EventType, &payloadRaw, &routingSourceRaw, &fireAtRaw, &activation.Recurring, &intervalRaw,
		&ownerNode, &activation.OwnerAgent, &taskType, &activation.Status, &firedAtRaw, &createdAtRaw,
		&activation.SourceTimerID, &activation.ForkedFromRunID, &activation.ForkedFromEventID,
		&activation.ReconstructionOwner,
	); err != nil {
		return WorkflowTimerActivation{}, err
	}
	if strings.TrimSpace(taskType) != workflowTimerTaskFamily {
		return WorkflowTimerActivation{}, fmt.Errorf("timer row %s is not a workflow timer family", activationID)
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
	if err := json.Unmarshal(sqliteWorkflowJSONBytes(routingSourceRaw), &activation.RoutingSource); err != nil {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer %s has invalid routing source: %w", activationID, err)
	}
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
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, err
	}
	return activation, nil
}

func (s *workflowInstanceStore) cancelWorkflowTimerActivation(ctx context.Context, ref timeridentity.WorkflowTimerActivationRef) (WorkflowTimerActivation, bool, error) {
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
		result, err = tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`, ref.ActivationID)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid AND task_type = 'workflow_timer' AND status = 'active'`, ref.ActivationID)
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
	if err := s.requestRunCompletionCandidate(ctx, activation.RunID); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	activation.Status = workflowTimerStatusCancelled
	return activation, true, nil
}

func workflowTimerRunID(ctx context.Context, instance WorkflowInstance) string {
	return strings.TrimSpace(firstNonEmptyString(runtimecorrelation.RunIDFromContext(ctx), asString(instance.Metadata["run_id"])))
}
