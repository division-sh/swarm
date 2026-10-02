package pipelinepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func commitFlowInstanceActivations(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	postgres bool,
	plans []runtimepipeline.FlowInstanceActivationPlan,
) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	committed := make([]runtimepipeline.CommittedFlowInstanceActivation, 0, len(plans))
	seen := make(map[string]struct{}, len(plans))
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for index, plan := range plans {
			result, err := commitFlowConstructionTree(ctx, tx, attempt, store, postgres, plan, true, seen)
			if err != nil {
				return fmt.Errorf("commit flow activation %d: %w", index, err)
			}
			committed = append(committed, result)
		}
		return nil
	})
	return committed, err
}

func commitFlowConstructionTree(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, store eventCommitTxStore, postgres bool,
	plan runtimepipeline.FlowInstanceActivationPlan, mayCreate bool, seen map[string]struct{},
) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	record, err := plan.PersistenceRecord()
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	key := record.Identity.Key()
	if _, duplicate := seen[key]; duplicate {
		return runtimepipeline.CommittedFlowInstanceActivation{}, fmt.Errorf("construction repeats route %q", record.Identity.Route.InstancePath)
	}
	seen[key] = struct{}{}
	if !mayCreate {
		equal, found, err := loadFlowInstanceActivationEqual(ctx, tx, postgres, record)
		if err != nil || !found || !equal {
			return runtimepipeline.CommittedFlowInstanceActivation{}, errors.Join(err, fmt.Errorf("construction replay has missing or conflicting descendant %s", record.Identity.Route.InstancePath))
		}
	}
	created, lifecycle, err := commitFlowInstanceActivation(ctx, tx, attempt, store, postgres, plan, record)
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	readiness, found, err := loadDynamicFlowRuntimeReadiness(ctx, tx, postgres, record.Identity.RunID, record.Identity.Route, false)
	if err != nil || !found {
		return runtimepipeline.CommittedFlowInstanceActivation{}, errors.Join(err, fmt.Errorf("committed construction %s has no attachment owner", record.Identity.Route.InstancePath))
	}
	result := runtimepipeline.CommittedFlowInstanceActivation{
		Plan: plan, Created: created, Lifecycle: lifecycle, ReadinessAttemptOrdinal: readiness.AttemptOrdinal,
	}
	for _, child := range plan.Children {
		committed, err := commitFlowConstructionTree(ctx, tx, attempt, store, postgres, child, created, seen)
		if err != nil {
			return runtimepipeline.CommittedFlowInstanceActivation{}, err
		}
		result.Children = append(result.Children, committed)
	}
	return result, nil
}

func (s *PipelinePostgresOwner) CommitFlowInstanceActivationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitFlowInstanceActivations(ctx, attempt, s, true, plans)
}

func (s *PipelineSQLiteOwner) CommitFlowInstanceActivationsTx(ctx context.Context, attempt *mutationprotocol.Attempt, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitFlowInstanceActivations(ctx, attempt, s, false, plans)
}

func commitOneFlowInstanceActivation(
	ctx context.Context,
	command runtimebus.FlowInstanceActivationCommand,
	store eventCommitTxStore,
	postgres bool,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedFlowInstanceActivation, error)) mutationprotocol.Result[runtimepipeline.CommittedFlowInstanceActivation],
) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	outcome := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedFlowInstanceActivation, error) {
		committed, err := commitFlowInstanceActivations(txctx, attempt, store, postgres, []runtimepipeline.FlowInstanceActivationPlan{command.Plan})
		if err != nil {
			return runtimepipeline.CommittedFlowInstanceActivation{}, err
		}
		if len(committed) != 1 {
			return runtimepipeline.CommittedFlowInstanceActivation{}, fmt.Errorf("flow instance activation commit returned %d results", len(committed))
		}
		err = attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			_, err := replaceFlowInstanceRouteTopologyTx(txctx, tx, postgres, command.RouteTopology)
			return err
		})
		return committed[0], err
	})
	result, acknowledged := outcome.Value()
	if !acknowledged {
		return runtimepipeline.CommittedFlowInstanceActivation{}, outcome.Err()
	}
	result = result.WithCommitAcknowledgment()
	return result, errors.Join(outcome.Err(), result.Validate())
}

func (s *PipelinePostgresOwner) CommitFlowInstanceActivation(ctx context.Context, command runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitOneFlowInstanceActivation(ctx, command, s, true, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedFlowInstanceActivation, error)) mutationprotocol.Result[runtimepipeline.CommittedFlowInstanceActivation] {
		return mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	})
}

func (s *PipelineSQLiteOwner) CommitFlowInstanceActivation(ctx context.Context, command runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitOneFlowInstanceActivation(ctx, command, s, false, func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedFlowInstanceActivation, error)) mutationprotocol.Result[runtimepipeline.CommittedFlowInstanceActivation] {
		return mutationprotocol.RunSQLite(ctx, s.backend, "sqlite commit flow instance activation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	})
}

var _ runtimebus.FlowInstanceActivationCommitOwner = (*PipelinePostgresOwner)(nil)
var _ runtimebus.FlowInstanceActivationCommitOwner = (*PipelineSQLiteOwner)(nil)

func commitFlowInstanceActivation(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	postgres bool,
	plan runtimepipeline.FlowInstanceActivationPlan,
	record runtimepipeline.FlowInstanceActivationRecord,
) (bool, runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	if err := record.Validate(); err != nil {
		return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	ctx = runtimecorrelation.WithRunID(ctx, record.Identity.RunID)
	if postgres {
		if err := requirePostgresRunActive(ctx, tx, record.Identity.RunID); err != nil {
			return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
		}
		lockIdentity := fmt.Sprintf("%d:%s%s", len(record.Identity.RunID), record.Identity.RunID, record.Identity.Route.InstancePath)
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockIdentity); err != nil {
			return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("lock flow activation route: %w", err)
		}
	} else if err := requireSQLiteRunActive(ctx, tx, record.Identity.RunID); err != nil {
		return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}

	equal, found, err := loadFlowInstanceActivationEqual(ctx, tx, postgres, record)
	if err != nil {
		return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if found {
		if !equal {
			return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, runtimefailures.New(
				runtimefailures.ClassConflictingDuplicate,
				"flow_instance_already_exists",
				"flow-instance-activation",
				"commit",
				map[string]any{"flow_instance": record.Identity.Route.InstancePath},
			)
		}
		return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, nil
	}
	lifecycle, err := insertFlowInstanceActivation(ctx, tx, attempt, store, postgres, plan, record)
	if err != nil {
		return false, runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	return true, lifecycle, nil
}

func loadFlowInstanceActivationEqual(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	want runtimepipeline.FlowInstanceActivationRecord,
) (bool, bool, error) {
	query := `SELECT fi.entity_id, fi.entity_type, fi.stage_defined,
		m.projection_version, m.projection, m.occurred_at, r.plan, r.plan_hash
		FROM flow_instances fi
		JOIN workflow_instance_initial_materializations m
			ON m.run_id = fi.run_id AND m.entity_id = fi.entity_id AND m.instance_path = fi.instance_path
		JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
		WHERE fi.run_id = ? AND fi.instance_path = ?`
	if postgres {
		query = `SELECT fi.entity_id::text, fi.entity_type, fi.stage_defined,
			m.projection_version, m.projection, m.occurred_at, r.plan, r.plan_hash
			FROM flow_instances fi
			JOIN workflow_instance_initial_materializations m
				ON m.run_id = fi.run_id AND m.entity_id = fi.entity_id AND m.instance_path = fi.instance_path
			JOIN flow_instance_runtime_readiness r ON r.run_id = fi.run_id AND r.instance_path = fi.instance_path
			WHERE fi.run_id = $1::uuid AND fi.instance_path = $2`
	}
	var entityID, planHash string
	var entityType sql.NullString
	var staged bool
	var projectionVersion int
	var initial, readiness []byte
	var occurredAt any
	err := tx.QueryRowContext(ctx, query, want.Identity.RunID, want.Identity.Route.InstancePath).Scan(
		&entityID, &entityType, &staged, &projectionVersion, &initial, &occurredAt, &readiness, &planHash,
	)
	if err == sql.ErrNoRows {
		occupied, occupiedErr := flowInstanceActivationIdentityOccupied(ctx, tx, postgres, want)
		return false, occupied, occupiedErr
	}
	if err != nil {
		return false, false, fmt.Errorf("load flow construction receipt: %w", err)
	}
	target, err := loadWorkflowTargetPersistence(ctx, tx, want.Identity, runtimeidentity.NormalizeEntityID(want.EntityID), !postgres)
	if err != nil {
		return false, true, fmt.Errorf("load constructed workflow target: %w", err)
	}
	if !target.Presence.Constructed() {
		// Occupied but incomplete construction is a conflicting replay, not a
		// fresh constructor or an opportunity to repair the missing fields.
		return false, true, nil
	}
	if err := target.Validate(want.Identity.Route, runtimeidentity.NormalizeEntityID(want.EntityID)); err != nil {
		return false, true, fmt.Errorf("constructed workflow target: %w", err)
	}
	if target.Lifecycle.WorkflowName != want.WorkflowName || target.Lifecycle.Mode != want.Mode ||
		!canonicalActivationTime(target.Lifecycle.CreatedAt).Equal(canonicalActivationTime(want.CreatedAt)) {
		return false, true, nil
	}
	if _, err := runtimepipeline.DecodeFlowReadinessPlan(readiness, planHash); err != nil {
		return false, true, fmt.Errorf("constructed workflow readiness: %w", err)
	}
	timestamp, present, err := sqliteTimeValue(occurredAt)
	if err != nil || !present {
		return false, true, errors.Join(err, fmt.Errorf("construction receipt requires exact occurrence time"))
	}
	// Compare immutable construction, never current fields, stage, configuration
	// or a mutable desired attachment plan. Progress cannot change replay identity.
	var actual, expected any
	if err := runtimecanonicaljson.DecodePreservingNumberLexemes(initial, &actual); err != nil {
		return false, true, fmt.Errorf("decode construction receipt: %w", err)
	}
	if err := runtimecanonicaljson.DecodePreservingNumberLexemes(want.InitialMaterialization, &expected); err != nil {
		return false, true, fmt.Errorf("decode planned construction receipt: %w", err)
	}
	actualJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(actual)
	if err != nil {
		return false, true, err
	}
	expectedJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(expected)
	if err != nil {
		return false, true, err
	}
	equal := entityID == want.EntityID && entityType.String == want.EntityType && staged == want.State.StageDefined &&
		projectionVersion == want.InitialProjectionVersion && bytes.Equal(actualJSON, expectedJSON) &&
		canonicalActivationTime(timestamp).Equal(canonicalActivationTime(want.CreatedAt))
	return equal, true, nil
}

func flowInstanceActivationIdentityOccupied(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.FlowInstanceActivationRecord) (bool, error) {
	query := `
		SELECT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = $1::uuid AND instance_path = $2),
		       EXISTS (SELECT 1 FROM entity_state WHERE run_id = $1::uuid AND entity_id = $3::uuid),
		       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = $1::uuid AND entity_id = $3::uuid),
		       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_path = $2)
	`
	if !postgres {
		query = `
			SELECT EXISTS (SELECT 1 FROM flow_instances WHERE run_id = ? AND instance_path = ?),
			       EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?),
			       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = ? AND entity_id = ?),
			       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_path = ?)
		`
	}
	var flow, entity, initial, readiness bool
	var err error
	if postgres {
		err = tx.QueryRowContext(ctx, query, record.Identity.RunID, record.Identity.Route.InstancePath, record.EntityID).Scan(&flow, &entity, &initial, &readiness)
	} else {
		err = tx.QueryRowContext(ctx, query,
			record.Identity.RunID, record.Identity.Route.InstancePath,
			record.Identity.RunID, record.EntityID,
			record.Identity.RunID, record.EntityID,
			record.Identity.RunID, record.Identity.Route.InstancePath,
		).Scan(&flow, &entity, &initial, &readiness)
	}
	if err != nil {
		return false, err
	}
	return flow || entity || initial || readiness, nil
}

func insertFlowInstanceActivation(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	store eventCommitTxStore,
	postgres bool,
	plan runtimepipeline.FlowInstanceActivationPlan,
	record runtimepipeline.FlowInstanceActivationRecord,
) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	if err := commitWorkflowEngineState(ctx, attempt, postgres, record.State); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if postgres {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_path, projection_version, projection, occurred_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6)
		`, record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, record.InitialProjectionVersion, record.InitialMaterialization, record.CreatedAt); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("insert flow initial materialization: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_path, plan, plan_hash, activation_attempt_id, phase, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, $4, 1, 'planned', NULL, $5, $5)
		`, record.Identity.RunID, record.Identity.Route.InstancePath, record.Readiness, record.ReadinessPlanHash, record.CreatedAt); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("insert flow runtime readiness: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_path, projection_version, projection, occurred_at
			) VALUES (?, ?, ?, ?, ?, ?)
		`, record.Identity.RunID, record.EntityID, record.Identity.Route.InstancePath, record.InitialProjectionVersion, record.InitialMaterialization, record.CreatedAt); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("insert sqlite flow initial materialization: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_path, plan, plan_hash, activation_attempt_id, phase, creation_event_emitted_at, created_at, updated_at
			) VALUES (?, ?, ?, ?, 1, 'planned', NULL, ?, ?)
		`, record.Identity.RunID, record.Identity.Route.InstancePath, record.Readiness, record.ReadinessPlanHash, record.CreatedAt, record.CreatedAt); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("insert sqlite flow runtime readiness: %w", err)
		}
	}
	before, err := commitWorkflowEngineInitialValues(ctx, attempt, store, postgres, record.State, runtimemutationlog.EntityStateProjection{})
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	lifecycle, err := commitWorkflowEngineLifecycle(ctx, attempt, store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), postgres, plan.Lifecycle)
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if err := commitWorkflowEngineMutationLog(ctx, attempt, store, postgres, record.State, before); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	return lifecycle, nil
}

func canonicalActivationTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
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
