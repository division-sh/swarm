package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

func (s *workflowInstanceStore) insertWorkflowTimerActivation(ctx context.Context, activation WorkflowTimerActivation) (WorkflowTimerActivation, bool, error) {
	activation = activation.normalized()
	if err := activation.validate(); err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || !authoractivityfixture.InMutation(ctx, tx) {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation requires the pipeline mutation owner")
	}
	runID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return WorkflowTimerActivation{}, false, err
	}
	if runID != activation.RunID {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer run mismatch: context=%s activation=%s", runID, activation.RunID)
	}
	routingSource, err := json.Marshal(activation.RoutingSource)
	if err != nil {
		return WorkflowTimerActivation{}, false, fmt.Errorf("encode workflow timer routing source: %w", err)
	}
	var result sql.Result
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				execution_mode, status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULL, ?, 'system', ?, ?, 'active', ?,
			        NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSource), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskFamily, activation.ExecutionMode, activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				execution_mode, status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, NULLIF($13, ''),
		        NULL, $14, 'system', $15, $16, 'active', $17, NULLIF($18, '')::uuid, NULLIF($19, '')::uuid,
		        NULLIF($20, '')::uuid, NULLIF($21, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSource), activation.FireAt,
			activation.Recurring, workflowTimerIntervalString(activation), activation.OwnerAgent,
			workflowTimerTaskFamily, activation.ExecutionMode, activation.CreatedAt, activation.SourceTimerID,
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

func (s *workflowInstanceStore) loadWorkflowTimerActivation(ctx context.Context, activationID string, lock bool) (WorkflowTimerActivation, bool, error) {
	activationID = strings.TrimSpace(activationID)
	if activationID == "" {
		return WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation id is required")
	}
	exec := workflowTimerQueryer(s.testDB())
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
	exec := workflowTimerQueryer(s.testDB())
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
			COALESCE(t.owner_agent, ''), t.task_type, t.execution_mode, t.status, t.fired_at, t.created_at,
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
		&ownerNode, &activation.OwnerAgent, &taskType, &activation.ExecutionMode, &activation.Status, &firedAtRaw, &createdAtRaw,
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
		return WorkflowTimerActivation{}, fmt.Errorf("decode workflow timer routing source: %w", err)
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
