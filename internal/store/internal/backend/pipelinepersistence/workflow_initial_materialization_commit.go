package pipelinepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func commitWorkflowInitialMaterialization(
	ctx context.Context,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	command runtimepipeline.WorkflowInitialMaterializationCommand,
) (runtimepipeline.CommittedWorkflowInitialMaterialization, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowInitialMaterialization{}, err
	}
	result := runtimepipeline.CommittedWorkflowInitialMaterialization{
		Result: runtimepipeline.WorkflowInitialMaterializationAlreadyExists,
	}
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		record := command.Record
		if postgres {
			if err := requirePostgresRunActive(txctx, tx, record.State.RunID); err != nil {
				return err
			}
			lockIdentity := fmt.Sprintf("%d:%s%s", len(record.State.RunID), record.State.RunID, record.State.Route.InstancePath)
			if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockIdentity); err != nil {
				return fmt.Errorf("lock workflow initial materialization route: %w", err)
			}
		} else if err := requireSQLiteRunActive(txctx, tx, record.State.RunID); err != nil {
			return err
		}

		equal, found, err := loadWorkflowInitialMaterializationEqual(txctx, tx, postgres, record)
		if err != nil {
			return err
		}
		if found {
			if !equal {
				return workflowInitialMaterializationConflict(record.State.Route.InstancePath)
			}
			complete, err := workflowInitialMaterializationSnapshotExists(txctx, tx, postgres, record)
			if err != nil {
				return err
			}
			if !complete {
				return workflowInitialMaterializationConflict(record.State.Route.InstancePath)
			}
			return nil
		}
		occupancy, err := loadWorkflowInitialMaterializationOccupancy(txctx, tx, postgres, record)
		if err != nil {
			return err
		}
		if occupancy.Entity || occupancy.Initial || occupancy.Readiness {
			return fmt.Errorf(
				"workflow initial materialization identity occupied (entity=%t initial=%t readiness=%t): %w",
				occupancy.Entity,
				occupancy.Initial,
				occupancy.Readiness,
				workflowInitialMaterializationConflict(record.State.Route.InstancePath),
			)
		}
		if occupancy.Flow {
			allowed, reason, err := workflowInitialMaterializationRouteRebindAllowed(txctx, tx, postgres, record)
			if err != nil {
				return err
			}
			if !allowed {
				return fmt.Errorf("workflow initial materialization route rebind rejected (%s): %w", reason, workflowInitialMaterializationConflict(record.State.Route.InstancePath))
			}
		}

		if err := commitWorkflowEngineState(txctx, tx, postgres, record.State, occupancy.Flow); err != nil {
			return err
		}
		if err := insertWorkflowInitialMaterializationRecord(txctx, tx, postgres, record); err != nil {
			return err
		}
		if err := insertWorkflowInitialReadinessRecord(txctx, tx, postgres, record); err != nil {
			return err
		}
		before, err := commitWorkflowEngineInitialValues(
			txctx,
			tx,
			story,
			store,
			postgres,
			record.State,
			runtimemutationlog.EntityStateProjection{},
		)
		if err != nil {
			return err
		}
		result.Lifecycle, err = commitWorkflowEngineLifecycle(txctx, tx, runtimeAuthorActivityMutation(story), store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), postgres, command.Lifecycle)
		if err != nil {
			return err
		}
		if err := commitWorkflowEngineMutationLog(txctx, tx, story, store, postgres, record.State, before); err != nil {
			return err
		}
		result.Result = runtimepipeline.WorkflowInitialMaterializationCreated
		return nil
	})
	if err != nil {
		return runtimepipeline.CommittedWorkflowInitialMaterialization{}, err
	}
	if err := result.Validate(); err != nil {
		return runtimepipeline.CommittedWorkflowInitialMaterialization{}, err
	}
	return result, nil
}

func workflowInitialMaterializationSnapshotExists(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	record runtimepipeline.WorkflowInitialMaterializationRecord,
) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM flow_instances WHERE instance_id = ?
		), EXISTS (
			SELECT 1
			FROM entity_state
			WHERE run_id = ? AND entity_id = ? AND flow_instance = ? AND entity_type = ?
		)
	`
	args := []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID, record.State.Route.InstancePath, record.State.EntityType}
	if postgres {
		query = `
			SELECT EXISTS (
				SELECT 1 FROM flow_instances WHERE instance_id = $1
			), EXISTS (
			SELECT 1
				FROM entity_state
				WHERE run_id = $2::uuid AND entity_id = $3::uuid AND flow_instance = $1 AND entity_type = $4
			)
		`
		args = []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID, record.State.EntityType}
	}
	var flow, entity bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&flow, &entity); err != nil {
		return false, fmt.Errorf("inspect workflow initial materialization snapshot: %w", err)
	}
	return flow && entity, nil
}

func (s *PipelinePostgresOwner) CommitWorkflowInitialMaterialization(ctx context.Context, command runtimepipeline.WorkflowInitialMaterializationCommand) (runtimepipeline.CommittedWorkflowInitialMaterialization, error) {
	return commitWorkflowInitialMaterialization(ctx, s, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, fn)
	}, command)
}

func (s *PipelineSQLiteOwner) CommitWorkflowInitialMaterialization(ctx context.Context, command runtimepipeline.WorkflowInitialMaterializationCommand) (runtimepipeline.CommittedWorkflowInitialMaterialization, error) {
	return commitWorkflowInitialMaterialization(ctx, s, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite workflow initial materialization", fn)
	}, command)
}

func workflowInitialMaterializationConflict(instancePath string) error {
	return runtimefailures.New(
		runtimefailures.ClassConflictingDuplicate,
		"flow_instance_already_exists",
		"workflow-instance-lifecycle",
		"materialize_initial_entry",
		map[string]any{"flow_instance": strings.Trim(strings.TrimSpace(instancePath), "/")},
	)
}

func loadWorkflowInitialMaterializationEqual(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	want runtimepipeline.WorkflowInitialMaterializationRecord,
) (bool, bool, error) {
	query := `
		SELECT projection_version, projection, occurred_at
		FROM workflow_instance_initial_materializations
		WHERE run_id = ? AND entity_id = ? AND instance_id = ?
	`
	if postgres {
		query = `
			SELECT projection_version, projection, occurred_at
			FROM workflow_instance_initial_materializations
			WHERE run_id = $1::uuid AND entity_id = $2::uuid AND instance_id = $3
		`
	}
	var version int
	var projection []byte
	var occurredAt time.Time
	var occurredAtRaw any
	destination := any(&occurredAt)
	if !postgres {
		destination = &occurredAtRaw
	}
	err := tx.QueryRowContext(ctx, query, want.State.RunID, want.State.EntityID, want.State.Route.InstancePath).Scan(&version, &projection, destination)
	if err == sql.ErrNoRows {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("load workflow initial materialization: %w", err)
	}
	if !postgres {
		var present bool
		occurredAt, present, err = sqliteTimeValue(occurredAtRaw)
		if err != nil || !present {
			if err == nil {
				err = fmt.Errorf("occurrence time is missing")
			}
			return false, true, fmt.Errorf("decode workflow initial materialization occurrence: %w", err)
		}
	}
	readinessEqual, err := workflowInitialReadinessEqual(ctx, tx, postgres, want)
	if err != nil {
		return false, true, err
	}
	return version == want.ProjectionVersion &&
		workflowCommitJSONEqual(projection, want.Projection) &&
		canonicalActivationTime(occurredAt).Equal(canonicalActivationTime(want.OccurredAt)) &&
		readinessEqual, true, nil
}

func workflowInitialReadinessEqual(ctx context.Context, tx *sql.Tx, postgres bool, want runtimepipeline.WorkflowInitialMaterializationRecord) (bool, error) {
	query := `SELECT plan, created_at FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?`
	if postgres {
		query = `SELECT plan, created_at FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_id = $2`
	}
	var plan []byte
	var createdAt time.Time
	var createdAtRaw any
	destination := any(&createdAt)
	if !postgres {
		destination = &createdAtRaw
	}
	err := tx.QueryRowContext(ctx, query, want.State.RunID, want.State.Route.InstancePath).Scan(&plan, destination)
	if err == sql.ErrNoRows {
		return len(want.Readiness) == 0, nil
	}
	if err != nil {
		return false, fmt.Errorf("load workflow initial readiness: %w", err)
	}
	if len(want.Readiness) == 0 {
		return false, nil
	}
	if !postgres {
		var present bool
		createdAt, present, err = sqliteTimeValue(createdAtRaw)
		if err != nil || !present {
			if err == nil {
				err = fmt.Errorf("creation time is missing")
			}
			return false, fmt.Errorf("decode workflow initial readiness creation: %w", err)
		}
	}
	return workflowCommitJSONEqual(plan, want.Readiness) && canonicalActivationTime(createdAt).Equal(canonicalActivationTime(want.OccurredAt)), nil
}

type workflowInitialMaterializationOccupancy struct {
	Flow      bool
	Entity    bool
	Initial   bool
	Readiness bool
}

func loadWorkflowInitialMaterializationOccupancy(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowInitialMaterializationRecord) (workflowInitialMaterializationOccupancy, error) {
	query := `
		SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = ?),
		       EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?),
		       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = ? AND entity_id = ?),
		       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?)
	`
	args := []any{
		record.State.Route.InstancePath,
		record.State.RunID, record.State.EntityID,
		record.State.RunID, record.State.EntityID,
		record.State.RunID, record.State.Route.InstancePath,
	}
	if postgres {
		query = `
			SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = $1),
			       EXISTS (SELECT 1 FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid),
			       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = $2::uuid AND entity_id = $3::uuid),
			       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = $2::uuid AND instance_id = $1)
		`
		args = []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID}
	}
	var occupancy workflowInitialMaterializationOccupancy
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&occupancy.Flow, &occupancy.Entity, &occupancy.Initial, &occupancy.Readiness); err != nil {
		return workflowInitialMaterializationOccupancy{}, fmt.Errorf("inspect workflow initial materialization identity: %w", err)
	}
	return occupancy, nil
}

func workflowInitialMaterializationRouteRebindAllowed(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowInitialMaterializationRecord) (bool, string, error) {
	query := `
		SELECT flow_template,
		       EXISTS (
			   SELECT 1 FROM entity_state AS state WHERE state.flow_instance = ?
		       ),
		       EXISTS (
			   SELECT 1
			   FROM entity_state AS state
			   JOIN runs AS run ON run.run_id = state.run_id
			   WHERE state.flow_instance = ?
			     AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		       )
		FROM flow_instances
		WHERE instance_id = ?
	`
	args := []any{record.State.Route.InstancePath, record.State.Route.InstancePath, record.State.Route.InstancePath}
	if postgres {
		query = `
			SELECT flow_template,
			       EXISTS (
				   SELECT 1 FROM entity_state AS state WHERE state.flow_instance = $1
			       ),
			       EXISTS (
				   SELECT 1
				   FROM entity_state AS state
				   JOIN runs AS run ON run.run_id = state.run_id
				   WHERE state.flow_instance = $1
				     AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
			       )
			FROM flow_instances
			WHERE instance_id = $1
			FOR UPDATE
		`
		args = []any{record.State.Route.InstancePath}
	}
	var workflowName string
	var priorReference bool
	var activeReference bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&workflowName, &priorReference, &activeReference); err != nil {
		return false, "inspection_failed", fmt.Errorf("inspect workflow initial materialization route rebind: %w", err)
	}
	if strings.TrimSpace(workflowName) != strings.TrimSpace(record.State.WorkflowName) {
		return false, "workflow_template_changed", nil
	}
	if !priorReference {
		return false, "predecessor_reference_missing", nil
	}
	if activeReference {
		return false, "active_generation_exists", nil
	}
	return true, "", nil
}

func insertWorkflowInitialMaterializationRecord(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowInitialMaterializationRecord) error {
	query := `
		INSERT INTO workflow_instance_initial_materializations (
			run_id, entity_id, instance_id, projection_version, projection, occurred_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`
	if postgres {
		query = `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_id, projection_version, projection, occurred_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6)
		`
	}
	if _, err := tx.ExecContext(ctx, query, record.State.RunID, record.State.EntityID, record.State.Route.InstancePath, record.ProjectionVersion, record.Projection, record.OccurredAt); err != nil {
		return fmt.Errorf("insert workflow initial materialization: %w", err)
	}
	return nil
}

func insertWorkflowInitialReadinessRecord(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.WorkflowInitialMaterializationRecord) error {
	if len(record.Readiness) == 0 {
		return nil
	}
	query := `
		INSERT INTO flow_instance_runtime_readiness (
			run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
		) VALUES (?, ?, ?, NULL, NULL, ?, ?)
	`
	if postgres {
		query = `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)
		`
		if _, err := tx.ExecContext(ctx, query, record.State.RunID, record.State.Route.InstancePath, record.Readiness, record.OccurredAt); err != nil {
			return fmt.Errorf("insert workflow initial readiness: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, query, record.State.RunID, record.State.Route.InstancePath, record.Readiness, record.OccurredAt, record.OccurredAt); err != nil {
		return fmt.Errorf("insert workflow initial readiness: %w", err)
	}
	return nil
}

func workflowCommitJSONEqual(actual, expected []byte) bool {
	actualValue, actualErr := runtimecanonicaljson.Decode(actual)
	expectedValue, expectedErr := runtimecanonicaljson.Decode(expected)
	if actualErr != nil || expectedErr != nil {
		return false
	}
	actualCanonical, actualErr := runtimecanonicaljson.Encode(actualValue)
	expectedCanonical, expectedErr := runtimecanonicaljson.Encode(expectedValue)
	return actualErr == nil && expectedErr == nil && bytes.Equal(actualCanonical, expectedCanonical)
}

var _ runtimepipeline.WorkflowInitialMaterializationCommitOwner = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.WorkflowInitialMaterializationCommitOwner = (*PipelineSQLiteOwner)(nil)
