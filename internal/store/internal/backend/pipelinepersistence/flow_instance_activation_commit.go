package pipelinepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func commitFlowInstanceActivations(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
	plans []runtimepipeline.FlowInstanceActivationPlan,
) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	if tx == nil || story == nil {
		return nil, fmt.Errorf("flow instance activation commit requires private transaction and story owners")
	}
	committed := make([]runtimepipeline.CommittedFlowInstanceActivation, 0, len(plans))
	seen := make(map[string]struct{}, len(plans))
	for index, plan := range plans {
		record, err := plan.PersistenceRecord()
		if err != nil {
			return nil, fmt.Errorf("prepare flow activation %d: %w", index, err)
		}
		key := record.Identity.RunID + "\x00" + record.Identity.Route.InstancePath
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("flow activation %d repeats route %q", index, record.Identity.Route.InstancePath)
		}
		seen[key] = struct{}{}
		created, lifecycle, err := commitFlowInstanceActivation(ctx, tx, story, store, postgres, effects, plan, record)
		if err != nil {
			return nil, fmt.Errorf("commit flow activation %s: %w", record.Identity.Route.InstancePath, err)
		}
		committed = append(committed, runtimepipeline.CommittedFlowInstanceActivation{Plan: plan, Created: created, Lifecycle: lifecycle})
	}
	return committed, nil
}

func (s *PipelinePostgresOwner) CommitFlowInstanceActivationsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitFlowInstanceActivations(ctx, tx, story, s, true, effects, plans)
}

func (s *PipelineSQLiteOwner) CommitFlowInstanceActivationsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *revisionEffects, plans []runtimepipeline.FlowInstanceActivationPlan) ([]runtimepipeline.CommittedFlowInstanceActivation, error) {
	return commitFlowInstanceActivations(ctx, tx, story, s, false, effects, plans)
}

func commitOneFlowInstanceActivation(
	ctx context.Context,
	command runtimebus.FlowInstanceActivationCommand,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	if err := command.Validate(); err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	var result runtimepipeline.CommittedFlowInstanceActivation
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		committed, err := commitFlowInstanceActivations(txctx, tx, story, store, postgres, effects, []runtimepipeline.FlowInstanceActivationPlan{command.Plan})
		if err != nil {
			return err
		}
		if len(committed) != 1 {
			return fmt.Errorf("flow instance activation commit returned %d results", len(committed))
		}
		result = committed[0]
		_, err = replaceFlowInstanceRouteTopologyTx(txctx, tx, postgres, command.RouteTopology)
		return err
	})
	if err != nil {
		return runtimepipeline.CommittedFlowInstanceActivation{}, err
	}
	return result, result.Validate()
}

func (s *PipelinePostgresOwner) CommitFlowInstanceActivation(ctx context.Context, command runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	effects := newRevisionEffects()
	return commitOneFlowInstanceActivation(ctx, command, s, true, effects, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, effects, fn)
	})
}

func (s *PipelineSQLiteOwner) CommitFlowInstanceActivation(ctx context.Context, command runtimebus.FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	effects := newRevisionEffects()
	return commitOneFlowInstanceActivation(ctx, command, s, false, effects, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite commit flow instance activation", effects, fn)
	})
}

var _ runtimebus.FlowInstanceActivationCommitOwner = (*PipelinePostgresOwner)(nil)
var _ runtimebus.FlowInstanceActivationCommitOwner = (*PipelineSQLiteOwner)(nil)

func commitFlowInstanceActivation(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
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
	lifecycle, err := insertFlowInstanceActivation(ctx, tx, story, store, postgres, effects, plan, record)
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
	query := `
		SELECT
			fi.flow_template, fi.mode, fi.config, fi.status, fi.created_at,
			es.flow_instance, es.entity_type, COALESCE(es.slug, ''), COALESCE(es.name, ''),
			es.current_state, es.gates, es.fields, es.bookkeeping, es.accumulator,
			es.entered_state_at, es.created_at,
			m.projection_version, m.projection, m.occurred_at,
			r.plan, r.created_at
		FROM flow_instances fi
		JOIN entity_state es
		  ON es.flow_instance = fi.instance_path
		 AND es.run_id = fi.run_id
		 AND es.entity_id = $2::uuid
		JOIN workflow_instance_initial_materializations m
		  ON m.run_id = es.run_id
		 AND m.entity_id = es.entity_id
		 AND m.instance_path = es.flow_instance
		JOIN flow_instance_runtime_readiness r
		  ON r.run_id = es.run_id
		 AND r.instance_path = es.flow_instance
		WHERE fi.run_id = $1::uuid AND fi.instance_path = $3
	`
	if !postgres {
		query = `
			SELECT
				fi.flow_template, fi.mode, fi.config, fi.status, fi.created_at,
				es.flow_instance, es.entity_type, COALESCE(es.slug, ''), COALESCE(es.name, ''),
				es.current_state, es.gates, es.fields, es.bookkeeping, es.accumulator,
				es.entered_state_at, es.created_at,
				m.projection_version, m.projection, m.occurred_at,
				r.plan, r.created_at
			FROM flow_instances fi
			JOIN entity_state es
			  ON es.flow_instance = fi.instance_path
			 AND es.run_id = fi.run_id
			 AND es.entity_id = ?
			JOIN workflow_instance_initial_materializations m
			  ON m.run_id = es.run_id
			 AND m.entity_id = es.entity_id
			 AND m.instance_path = es.flow_instance
			JOIN flow_instance_runtime_readiness r
			  ON r.run_id = es.run_id
			 AND r.instance_path = es.flow_instance
			WHERE fi.run_id = ? AND fi.instance_path = ?
		`
	}
	var (
		workflowName, mode, status, instancePath, entityType, slug, name, state      string
		config, gates, fields, bookkeeping, accumulator, initial, readiness          []byte
		projectionVersion                                                            int
		flowCreated, enteredAt, entityCreated, initialAt, readinessAt                time.Time
		flowCreatedRaw, enteredAtRaw, entityCreatedRaw, initialAtRaw, readinessAtRaw any
	)
	destinations := []any{
		&workflowName, &mode, &config, &status, &flowCreated,
		&instancePath, &entityType, &slug, &name,
		&state, &gates, &fields, &bookkeeping, &accumulator,
		&enteredAt, &entityCreated,
		&projectionVersion, &initial, &initialAt,
		&readiness, &readinessAt,
	}
	if !postgres {
		destinations = []any{
			&workflowName, &mode, &config, &status, &flowCreatedRaw,
			&instancePath, &entityType, &slug, &name,
			&state, &gates, &fields, &bookkeeping, &accumulator,
			&enteredAtRaw, &entityCreatedRaw,
			&projectionVersion, &initial, &initialAtRaw,
			&readiness, &readinessAtRaw,
		}
	}
	args := []any{want.Identity.RunID, want.EntityID, want.Identity.Route.InstancePath}
	if !postgres {
		args = []any{want.EntityID, want.Identity.RunID, want.Identity.Route.InstancePath}
	}
	err := tx.QueryRowContext(ctx, query, args...).Scan(destinations...)
	if err == sql.ErrNoRows {
		occupied, occupiedErr := flowInstanceActivationIdentityOccupied(ctx, tx, postgres, want)
		return false, occupied, occupiedErr
	}
	if err != nil {
		return false, false, fmt.Errorf("load flow activation: %w", err)
	}
	if !postgres {
		decodeTime := func(raw any, target *time.Time) error {
			parsed, present, parseErr := sqliteTimeValue(raw)
			if parseErr != nil || !present {
				if parseErr == nil {
					parseErr = fmt.Errorf("time is missing")
				}
				return parseErr
			}
			*target = parsed
			return nil
		}
		for _, item := range []struct {
			raw    any
			target *time.Time
		}{
			{flowCreatedRaw, &flowCreated},
			{enteredAtRaw, &enteredAt},
			{entityCreatedRaw, &entityCreated},
			{initialAtRaw, &initialAt},
			{readinessAtRaw, &readinessAt},
		} {
			if err := decodeTime(item.raw, item.target); err != nil {
				return false, true, fmt.Errorf("decode flow activation time: %w", err)
			}
		}
	}
	jsonEqual := func(actual []byte, expected []byte) bool {
		actualValue, actualErr := runtimecanonicaljson.Decode(actual)
		expectedValue, expectedErr := runtimecanonicaljson.Decode(expected)
		if actualErr != nil || expectedErr != nil {
			return false
		}
		actualCanonical, actualErr := runtimecanonicaljson.Encode(actualValue)
		expectedCanonical, expectedErr := runtimecanonicaljson.Encode(expectedValue)
		return actualErr == nil && expectedErr == nil && bytes.Equal(actualCanonical, expectedCanonical)
	}
	equal := strings.TrimSpace(workflowName) == strings.TrimSpace(want.WorkflowName) &&
		strings.TrimSpace(mode) == want.Mode && strings.TrimSpace(status) == "active" &&
		strings.Trim(strings.TrimSpace(instancePath), "/") == want.Identity.Route.InstancePath &&
		strings.TrimSpace(entityType) == want.EntityType && strings.TrimSpace(slug) == want.Slug && strings.TrimSpace(name) == want.Name &&
		strings.TrimSpace(state) == want.CurrentState && projectionVersion == want.InitialProjectionVersion &&
		jsonEqual(config, want.Config) && jsonEqual(gates, want.Gates) && jsonEqual(fields, want.Fields) &&
		jsonEqual(bookkeeping, want.Bookkeeping) && jsonEqual(accumulator, want.Accumulator) && jsonEqual(initial, want.InitialMaterialization) && jsonEqual(readiness, want.Readiness) &&
		canonicalActivationTime(flowCreated).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(enteredAt).Equal(canonicalActivationTime(want.EnteredStageAt)) &&
		canonicalActivationTime(entityCreated).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(initialAt).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(readinessAt).Equal(canonicalActivationTime(want.CreatedAt))
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
	story *privateauthoractivity.Mutation,
	store eventCommitTxStore,
	postgres bool,
	effects *revisionEffects,
	plan runtimepipeline.FlowInstanceActivationPlan,
	record runtimepipeline.FlowInstanceActivationRecord,
) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	if err := commitWorkflowEngineState(ctx, tx, postgres, effects, record.State); err != nil {
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
				run_id, instance_path, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)
		`, record.Identity.RunID, record.Identity.Route.InstancePath, record.Readiness, record.CreatedAt); err != nil {
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
				run_id, instance_path, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES (?, ?, ?, NULL, NULL, ?, ?)
		`, record.Identity.RunID, record.Identity.Route.InstancePath, record.Readiness, record.CreatedAt, record.CreatedAt); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("insert sqlite flow runtime readiness: %w", err)
		}
	}
	before, err := commitWorkflowEngineInitialValues(
		ctx,
		tx,
		story,
		store,
		postgres,
		effects,
		record.State,
		runtimemutationlog.EntityStateProjection{},
	)
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	lifecycle, err := commitWorkflowEngineLifecycle(ctx, tx, runtimeAuthorActivityMutation(story), store.workflowDecisionLifecycleOwner(), store.genericScheduleTxOwner(), postgres, effects, plan.Lifecycle)
	if err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	if err := commitWorkflowEngineMutationLog(ctx, tx, story, store, postgres, effects, record.State, before); err != nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, err
	}
	return lifecycle, nil
}

func canonicalActivationTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}
