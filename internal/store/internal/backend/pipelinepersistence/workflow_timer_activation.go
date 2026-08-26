package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func (s *PipelinePostgresOwner) LoadWorkflowTimerActivation(ctx context.Context, activationID string) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("postgres workflow timer reader is required")
	}
	return loadWorkflowTimerActivation(ctx, s.backend, false, activationID)
}

func (s *PipelineSQLiteOwner) LoadWorkflowTimerActivation(ctx context.Context, activationID string) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("sqlite workflow timer reader is required")
	}
	return loadWorkflowTimerActivation(ctx, s.backend, true, activationID)
}

func loadWorkflowTimerActivation(ctx context.Context, db dynamicFlowReadinessQueryer, sqlite bool, activationID string) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	activationID = strings.TrimSpace(activationID)
	if activationID == "" {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("workflow timer activation id is required")
	}
	row := db.QueryRowContext(ctx, workflowTimerActivationSelect(false, sqlite), activationID)
	activation, err := scanWorkflowTimerActivation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimepipeline.WorkflowTimerActivation{}, false, nil
	}
	if err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, false, fmt.Errorf("load workflow timer activation %s: %w", activationID, err)
	}
	return activation, true, nil
}

func (s *PipelinePostgresOwner) ListWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]runtimepipeline.WorkflowTimerActivation, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres workflow timer reader is required")
	}
	return listWorkflowTimerActivations(ctx, s.backend, false, runID, entityID, activeOnly)
}

func (s *PipelineSQLiteOwner) ListWorkflowTimerActivations(ctx context.Context, runID, entityID string, activeOnly bool) ([]runtimepipeline.WorkflowTimerActivation, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite workflow timer reader is required")
	}
	return listWorkflowTimerActivations(ctx, s.backend, true, runID, entityID, activeOnly)
}

func listWorkflowTimerActivations(ctx context.Context, db dynamicFlowReadinessQueryer, sqlite bool, runID, entityID string, activeOnly bool) ([]runtimepipeline.WorkflowTimerActivation, error) {
	query := workflowTimerActivationSelect(true, sqlite)
	runID = strings.TrimSpace(runID)
	entityID = strings.TrimSpace(entityID)
	activeStates := runtimerunlifecycle.ActiveStates()
	args := []any{runID, entityID, string(activeStates[0]), string(activeStates[1])}
	if sqlite {
		args = []any{runID, runID, entityID, entityID, string(activeStates[0]), string(activeStates[1])}
	}
	if activeOnly {
		query += " AND t.status = 'active'"
	}
	query += " ORDER BY t.created_at, t.timer_id"
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list workflow timer activations: %w", err)
	}
	defer rows.Close()
	result := make([]runtimepipeline.WorkflowTimerActivation, 0)
	for rows.Next() {
		activation, err := scanWorkflowTimerActivation(rows)
		if err != nil {
			return nil, fmt.Errorf("scan workflow timer activation: %w", err)
		}
		result = append(result, activation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type workflowTimerScanner interface {
	Scan(...any) error
}

func workflowTimerActivationSelect(list, sqlite bool) string {
	where := "t.timer_id = $1::uuid AND t.task_type = 'workflow_timer'"
	if sqlite {
		where = "t.timer_id = ? AND t.task_type = 'workflow_timer'"
	}
	if list {
		if sqlite {
			where = "(? = '' OR t.run_id = ?) AND (? = '' OR t.entity_id = ?) AND t.task_type = 'workflow_timer' AND run.status IN (?, ?)"
		} else {
			where = "(NULLIF($1, '') IS NULL OR t.run_id = NULLIF($1, '')::uuid) AND (NULLIF($2, '') IS NULL OR t.entity_id = NULLIF($2, '')::uuid) AND t.task_type = 'workflow_timer' AND run.status IN ($3, $4)"
		}
	}
	return workflowTimerSelectColumns() + " WHERE " + where
}

func workflowTimerSelectColumns() string {
	return `
		SELECT
			CAST(t.timer_id AS TEXT), t.timer_name, COALESCE(CAST(t.run_id AS TEXT), ''),
			COALESCE(CAST(t.entity_id AS TEXT), ''), COALESCE(t.flow_scope_key, ''),
			COALESCE(t.flow_instance_id, ''), COALESCE(t.flow_instance, ''),
			t.fire_event, COALESCE(t.fire_payload, '{}'), t.routing_source, t.execution_mode, t.fire_at, t.recurring,
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

func scanWorkflowTimerActivation(scanner workflowTimerScanner) (runtimepipeline.WorkflowTimerActivation, error) {
	var (
		record                                                            runtimepipeline.WorkflowTimerActivationPersistenceRecord
		payloadRaw, routingSourceRaw, fireAtRaw, firedAtRaw, createdAtRaw any
	)
	if err := scanner.Scan(
		&record.ActivationID, &record.TaskID, &record.RunID, &record.EntityID, &record.Route.ScopeKey,
		&record.Route.InstanceID, &record.Route.InstancePath,
		&record.EventType, &payloadRaw, &routingSourceRaw, &record.ExecutionMode, &fireAtRaw, &record.Recurring, &record.RecurrenceInterval,
		&record.OwnerNode, &record.OwnerAgent, &record.TaskType, &record.Status, &firedAtRaw, &createdAtRaw,
		&record.SourceTimerID, &record.ForkedFromRunID, &record.ForkedFromEventID,
		&record.ReconstructionOwner,
	); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	record.Payload = workflowEngineJSONBytes(payloadRaw)
	if err := json.Unmarshal(workflowEngineJSONBytes(routingSourceRaw), &record.RoutingSource); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, fmt.Errorf("decode workflow timer routing source: %w", err)
	}
	var err error
	if record.FireAt, _, err = sqliteTimeValue(fireAtRaw); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	if record.FiredAt, _, err = sqliteTimeValue(firedAtRaw); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	if record.CreatedAt, _, err = sqliteTimeValue(createdAtRaw); err != nil {
		return runtimepipeline.WorkflowTimerActivation{}, err
	}
	return runtimepipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
}

func commitWorkflowTimerReconciliation(
	ctx context.Context,
	decisions workflowDecisionLifecycleTxOwner,
	genericSchedules GenericScheduleTxOwner,
	postgres bool,
	effects *revisionEffects,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	reserve func(context.Context) (*runLifecycleCandidateHandoffReservation, error),
	prepare func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error,
	requestCandidate func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error),
	command runtimepipeline.WorkflowTimerReconciliationCommand,
) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	handoff, err := reserve(ctx)
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	defer handoff.Rollback()
	var result runtimepipeline.CommittedWorkflowLifecycleMutation
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		var err error
		result, err = commitWorkflowEngineLifecycle(txctx, tx, runtimeAuthorActivityMutation(story), decisions, genericSchedules, postgres, effects, command.Plan)
		if err != nil {
			return err
		}
		if !command.Plan.RequestCompletionCandidate {
			return nil
		}
		candidate, err := requestCandidate(txctx, tx, command.RunID)
		if err != nil {
			return err
		}
		return prepare(handoff, candidate)
	})
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if err := handoff.Commit(); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	return result, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowTimerReconciliation(ctx context.Context, command runtimepipeline.WorkflowTimerReconciliationCommand) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	effects := newRevisionEffects()
	return commitWorkflowTimerReconciliation(ctx, s.DecisionPostgresOwner, s.genericScheduleTxOwner(), true, effects, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
	}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestPostgresCompletionCandidateTx(ctx, tx, runID, nil, false)
		}, command)
}

func (s *PipelineSQLiteOwner) CommitWorkflowTimerReconciliation(ctx context.Context, command runtimepipeline.WorkflowTimerReconciliationCommand) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	effects := newRevisionEffects()
	return commitWorkflowTimerReconciliation(ctx, s.DecisionSQLiteOwner, s.genericScheduleTxOwner(), false, effects,
		func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
			return s.runPrivateAuthorActivityMutation(ctx, "sqlite workflow timer reconciliation", effects, fn)
		}, reserveRunLifecycleCandidateHandoff,
		func(reservation *runLifecycleCandidateHandoffReservation, result runtimerunlifecycle.CandidateRequestResult) error {
			return reservation.Prepare(s.runLifecycleCandidates, result)
		},
		func(ctx context.Context, tx *sql.Tx, runID string) (runtimerunlifecycle.CandidateRequestResult, error) {
			return requestSQLiteCompletionCandidateTx(ctx, tx, runID, nil, s.now(), false)
		}, command)
}

var _ runtimepipeline.WorkflowTimerActivationPersistence = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowTimerActivationPersistence = (*PipelineSQLiteOwner)(nil)
