package pipelinepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func commitWorkflowEngineTimerMutation(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *revisionEffects,
	mutation runtimepipeline.WorkflowTimerMutation,
) (timeridentity.WorkflowTimerActivationRef, bool, error) {
	activation := mutation.Activation.Canonical()
	if err := mutation.Validate(activation.RunID, activation.Route, activation.EntityID); err != nil {
		return timeridentity.WorkflowTimerActivationRef{}, false, err
	}
	switch mutation.Kind {
	case runtimepipeline.WorkflowTimerMutationInsert:
		changed, err := insertWorkflowEngineTimerActivation(ctx, tx, postgres, activation)
		if err == nil && changed {
			err = effects.Add(activation.RunID, privaterunforkrevision.FamilyTimers)
		}
		return activation.Ref, changed, err
	case runtimepipeline.WorkflowTimerMutationCancel:
		changed, err := cancelWorkflowEngineTimerActivation(ctx, tx, postgres, activation)
		if err == nil && changed {
			err = effects.Add(activation.RunID, privaterunforkrevision.FamilyTimers)
		}
		return activation.Ref, changed, err
	default:
		return timeridentity.WorkflowTimerActivationRef{}, false, fmt.Errorf("workflow timer mutation kind %q is unsupported", mutation.Kind)
	}
}

func insertWorkflowEngineTimerActivation(ctx context.Context, tx *sql.Tx, postgres bool, activation runtimepipeline.WorkflowTimerActivation) (bool, error) {
	var (
		result sql.Result
		err    error
	)
	interval := ""
	if activation.Recurring {
		interval = activation.RecurrenceInterval.String()
	}
	routingSource, err := json.Marshal(activation.RoutingSource)
	if err != nil {
		return false, fmt.Errorf("encode workflow engine timer routing source: %w", err)
	}
	if postgres {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source, execution_mode,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13, NULLIF($14, ''),
		        NULL, $15, 'system', 'workflow_timer', 'active', $16, NULLIF($17, '')::uuid,
		        NULLIF($18, '')::uuid, NULLIF($19, '')::uuid, NULLIF($20, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSource), activation.ExecutionMode, activation.FireAt,
			activation.Recurring, interval, activation.OwnerAgent, activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id,
				flow_instance, fire_event, fire_payload, routing_source, execution_mode,
				fire_at, recurring, recurrence_interval, owner_node, owner_agent, owner_kind, task_type,
				status, created_at, source_timer_id, forked_from_run_id, forked_from_event_id,
				reconstruction_owner
			)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULL, ?, 'system', 'workflow_timer', 'active', ?,
			        NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))
			ON CONFLICT(timer_id) DO NOTHING
		`, activation.Ref.ActivationID, activation.RunID, activation.Ref.TaskID(), activation.EntityID,
			activation.Route.ScopeKey, activation.Route.InstanceID, activation.Route.InstancePath,
			activation.EventType, string(activation.Payload), string(routingSource), activation.ExecutionMode, activation.FireAt,
			activation.Recurring, interval, activation.OwnerAgent, activation.CreatedAt, activation.SourceTimerID,
			activation.ForkedFromRunID, activation.ForkedFromEventID, activation.ReconstructionOwner)
	}
	if err != nil {
		return false, fmt.Errorf("insert workflow engine timer activation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	persisted, found, err := loadWorkflowEngineTimerActivation(ctx, tx, postgres, activation.Ref)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("workflow timer activation %s disappeared after insert", activation.Ref.ActivationID)
	}
	if !sameWorkflowEngineTimerActivation(persisted, activation) {
		return false, fmt.Errorf("workflow timer activation %s conflicts with persisted facts", activation.Ref.ActivationID)
	}
	return rows == 1, nil
}

func cancelWorkflowEngineTimerActivation(ctx context.Context, tx *sql.Tx, postgres bool, expected runtimepipeline.WorkflowTimerActivation) (bool, error) {
	persisted, found, err := loadWorkflowEngineTimerActivation(ctx, tx, postgres, expected.Ref)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("workflow timer cancellation target %s is missing", expected.Ref.ActivationID)
	}
	if !sameWorkflowEngineTimerCancellationTarget(persisted, expected) {
		return false, fmt.Errorf("workflow timer cancellation target %s changed before commit", expected.Ref.ActivationID)
	}
	if persisted.Status != "active" {
		return false, nil
	}
	query := `UPDATE timers SET status = 'cancelled' WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
	args := []any{expected.Ref.ActivationID}
	if postgres {
		query = `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid AND task_type = 'workflow_timer' AND status = 'active'`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, fmt.Errorf("workflow timer cancellation changed %d rows", rows)
	}
	return true, nil
}

func sameWorkflowEngineTimerCancellationTarget(left, right runtimepipeline.WorkflowTimerActivation) bool {
	left, right = left.Canonical(), right.Canonical()
	left.FireAt, right.FireAt = time.Time{}, time.Time{}
	left.FiredAt, right.FiredAt = time.Time{}, time.Time{}
	left.Status, right.Status = "", ""
	return sameWorkflowEngineTimerActivation(left, right)
}

func loadWorkflowEngineTimerActivation(ctx context.Context, tx *sql.Tx, postgres bool, expected timeridentity.WorkflowTimerActivationRef) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	expected = expected.Normalize()
	activationID := expected.ActivationID
	if !expected.Valid() {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer load requires exact activation identity")
	}
	query := `
		SELECT timer_name, CAST(run_id AS TEXT), CAST(entity_id AS TEXT), flow_scope_key,
		       flow_instance_id, flow_instance,
		       fire_event, fire_payload, routing_source, execution_mode, fire_at, recurring, COALESCE(recurrence_interval, ''),
		       owner_agent, status, fired_at, created_at, COALESCE(CAST(source_timer_id AS TEXT), ''),
		       COALESCE(CAST(forked_from_run_id AS TEXT), ''), COALESCE(CAST(forked_from_event_id AS TEXT), ''),
		       COALESCE(reconstruction_owner, '')
		FROM timers WHERE timer_id = ? AND task_type = 'workflow_timer'
	`
	args := []any{activationID}
	if postgres {
		query = strings.Replace(query, "timer_id = ?", "timer_id = $1::uuid", 1) + " FOR UPDATE"
	}
	var (
		taskID, interval, status                                          string
		payloadRaw, routingSourceRaw, fireAtRaw, firedAtRaw, createdAtRaw any
		activation                                                        runtimepipeline.WorkflowTimerActivation
	)
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&taskID, &activation.RunID, &activation.EntityID, &activation.Route.ScopeKey,
		&activation.Route.InstanceID, &activation.Route.InstancePath,
		&activation.EventType, &payloadRaw, &routingSourceRaw, &activation.ExecutionMode, &fireAtRaw, &activation.Recurring, &interval,
		&activation.OwnerAgent, &status, &firedAtRaw, &createdAtRaw, &activation.SourceTimerID,
		&activation.ForkedFromRunID, &activation.ForkedFromEventID, &activation.ReconstructionOwner,
	)
	if err == sql.ErrNoRows {
		return runtimepipeline.WorkflowTimerActivation{}, false, nil
	}
	if err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, err
	}
	if strings.TrimSpace(taskID) != expected.TaskID() {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer %s has invalid task identity", activationID)
	}
	activation.Ref = expected
	activation.Status = strings.TrimSpace(status)
	activation.Payload = workflowEngineJSONBytes(payloadRaw)
	if err := json.Unmarshal(workflowEngineJSONBytes(routingSourceRaw), &activation.RoutingSource); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("decode workflow timer routing source: %w", err)
	}
	var found bool
	if activation.FireAt, found, err = sqliteTimeValue(fireAtRaw); err != nil || !found {
		if err == nil {
			err = fmt.Errorf("workflow timer %s has no fire_at", activationID)
		}
		return runtimepipeline.WorkflowTimerActivation{}, false, err
	}
	if activation.CreatedAt, found, err = sqliteTimeValue(createdAtRaw); err != nil || !found {
		if err == nil {
			err = fmt.Errorf("workflow timer %s has no created_at", activationID)
		}
		return runtimepipeline.WorkflowTimerActivation{}, false, err
	}
	if activation.FiredAt, _, err = sqliteTimeValue(firedAtRaw); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, err
	}
	if interval = strings.TrimSpace(interval); interval != "" {
		duration, ok := timeridentity.ParseDelayDuration(interval)
		if !ok {
			return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer %s has invalid recurrence interval %q", activationID, interval)
		}
		activation.RecurrenceInterval = duration
	}
	activation = activation.Canonical()
	if err := activation.Validate(); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, err
	}
	return activation, true, nil
}

func workflowEngineJSONBytes(raw any) []byte {
	switch value := raw.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	default:
		encoded, _ := json.Marshal(value)
		return encoded
	}
}

func sameWorkflowEngineTimerActivation(left, right runtimepipeline.WorkflowTimerActivation) bool {
	left, right = left.Canonical(), right.Canonical()
	leftPayload, rightPayload := left.Payload, right.Payload
	left.Payload, right.Payload = nil, nil
	if !reflect.DeepEqual(left, right) {
		return false
	}
	var leftValue, rightValue any
	if json.Unmarshal(leftPayload, &leftValue) == nil && json.Unmarshal(rightPayload, &rightValue) == nil {
		return reflect.DeepEqual(leftValue, rightValue)
	}
	return bytes.Equal(leftPayload, rightPayload)
}
