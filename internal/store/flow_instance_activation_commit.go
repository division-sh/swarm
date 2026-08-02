package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/authoractivity"
)

func commitFlowInstanceActivations(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	postgres bool,
	plans []runtimepipeline.FlowInstanceActivationPlan,
) ([]runtimebus.CommittedFlowInstanceActivation, error) {
	if len(plans) == 0 {
		return nil, nil
	}
	if tx == nil || story == nil {
		return nil, fmt.Errorf("flow instance activation commit requires private transaction and story owners")
	}
	committed := make([]runtimebus.CommittedFlowInstanceActivation, 0, len(plans))
	seen := make(map[string]struct{}, len(plans))
	for index, plan := range plans {
		record, err := plan.PersistenceRecord()
		if err != nil {
			return nil, fmt.Errorf("prepare flow activation %d: %w", index, err)
		}
		key := record.RunID + "\x00" + record.Route.InstancePath
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("flow activation %d repeats route %q", index, record.Route.InstancePath)
		}
		seen[key] = struct{}{}
		created, err := commitFlowInstanceActivation(ctx, tx, story, postgres, record)
		if err != nil {
			return nil, fmt.Errorf("commit flow activation %s: %w", record.Route.InstancePath, err)
		}
		committed = append(committed, runtimebus.CommittedFlowInstanceActivation{Plan: plan, Created: created})
	}
	return committed, nil
}

func commitFlowInstanceActivation(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	postgres bool,
	record runtimepipeline.FlowInstanceActivationRecord,
) (bool, error) {
	if err := record.Validate(); err != nil {
		return false, err
	}
	if postgres {
		if err := requirePostgresRunActive(ctx, tx, record.RunID); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, record.RunID+"\x00"+record.Route.InstancePath); err != nil {
			return false, fmt.Errorf("lock flow activation route: %w", err)
		}
	} else if err := requireSQLiteRunActive(ctx, tx, record.RunID); err != nil {
		return false, err
	}

	equal, found, err := loadFlowInstanceActivationEqual(ctx, tx, postgres, record)
	if err != nil {
		return false, err
	}
	if found {
		if !equal {
			return false, runtimefailures.New(
				runtimefailures.ClassConflictingDuplicate,
				"flow_instance_already_exists",
				"flow-instance-activation",
				"commit",
				map[string]any{"flow_instance": record.Route.InstancePath},
			)
		}
		return false, nil
	}
	if err := insertFlowInstanceActivation(ctx, tx, story, postgres, record); err != nil {
		return false, err
	}
	return true, nil
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
			es.current_state, es.gates, es.fields, es.accumulator,
			es.entered_state_at, es.created_at,
			m.projection_version, m.projection, m.occurred_at,
			r.plan, r.created_at
		FROM flow_instances fi
		JOIN entity_state es
		  ON es.flow_instance = fi.instance_id
		 AND es.run_id = $1::uuid
		 AND es.entity_id = $2::uuid
		JOIN workflow_instance_initial_materializations m
		  ON m.run_id = es.run_id
		 AND m.entity_id = es.entity_id
		 AND m.instance_id = es.flow_instance
		JOIN flow_instance_runtime_readiness r
		  ON r.run_id = es.run_id
		 AND r.instance_id = es.flow_instance
		WHERE fi.instance_id = $3
	`
	if !postgres {
		query = `
			SELECT
				fi.flow_template, fi.mode, fi.config, fi.status, fi.created_at,
				es.flow_instance, es.entity_type, COALESCE(es.slug, ''), COALESCE(es.name, ''),
				es.current_state, es.gates, es.fields, es.accumulator,
				es.entered_state_at, es.created_at,
				m.projection_version, m.projection, m.occurred_at,
				r.plan, r.created_at
			FROM flow_instances fi
			JOIN entity_state es
			  ON es.flow_instance = fi.instance_id
			 AND es.run_id = ?
			 AND es.entity_id = ?
			JOIN workflow_instance_initial_materializations m
			  ON m.run_id = es.run_id
			 AND m.entity_id = es.entity_id
			 AND m.instance_id = es.flow_instance
			JOIN flow_instance_runtime_readiness r
			  ON r.run_id = es.run_id
			 AND r.instance_id = es.flow_instance
			WHERE fi.instance_id = ?
		`
	}
	var (
		workflowName, mode, status, instancePath, entityType, slug, name, state      string
		config, gates, fields, accumulator, initial, readiness                       []byte
		projectionVersion                                                            int
		flowCreated, enteredAt, entityCreated, initialAt, readinessAt                time.Time
		flowCreatedRaw, enteredAtRaw, entityCreatedRaw, initialAtRaw, readinessAtRaw any
	)
	destinations := []any{
		&workflowName, &mode, &config, &status, &flowCreated,
		&instancePath, &entityType, &slug, &name,
		&state, &gates, &fields, &accumulator,
		&enteredAt, &entityCreated,
		&projectionVersion, &initial, &initialAt,
		&readiness, &readinessAt,
	}
	if !postgres {
		destinations = []any{
			&workflowName, &mode, &config, &status, &flowCreatedRaw,
			&instancePath, &entityType, &slug, &name,
			&state, &gates, &fields, &accumulator,
			&enteredAtRaw, &entityCreatedRaw,
			&projectionVersion, &initial, &initialAtRaw,
			&readiness, &readinessAtRaw,
		}
	}
	err := tx.QueryRowContext(ctx, query, want.RunID, want.EntityID, want.Route.InstancePath).Scan(destinations...)
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
		strings.Trim(strings.TrimSpace(instancePath), "/") == want.Route.InstancePath &&
		strings.TrimSpace(entityType) == want.EntityType && strings.TrimSpace(slug) == want.Slug && strings.TrimSpace(name) == want.Name &&
		strings.TrimSpace(state) == want.CurrentState && projectionVersion == 1 &&
		jsonEqual(config, want.Config) && jsonEqual(gates, want.Gates) && jsonEqual(fields, want.Fields) &&
		jsonEqual(accumulator, want.Accumulator) && jsonEqual(initial, want.InitialMaterialization) && jsonEqual(readiness, want.Readiness) &&
		canonicalActivationTime(flowCreated).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(enteredAt).Equal(canonicalActivationTime(want.EnteredStageAt)) &&
		canonicalActivationTime(entityCreated).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(initialAt).Equal(canonicalActivationTime(want.CreatedAt)) &&
		canonicalActivationTime(readinessAt).Equal(canonicalActivationTime(want.CreatedAt))
	return equal, true, nil
}

func flowInstanceActivationIdentityOccupied(ctx context.Context, tx *sql.Tx, postgres bool, record runtimepipeline.FlowInstanceActivationRecord) (bool, error) {
	query := `
		SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = $1),
		       EXISTS (SELECT 1 FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid),
		       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = $2::uuid AND entity_id = $3::uuid),
		       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = $2::uuid AND instance_id = $1)
	`
	if !postgres {
		query = `
			SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = ?),
			       EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?),
			       EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = ? AND entity_id = ?),
			       EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?)
		`
	}
	var flow, entity, initial, readiness bool
	var err error
	if postgres {
		err = tx.QueryRowContext(ctx, query, record.Route.InstancePath, record.RunID, record.EntityID).Scan(&flow, &entity, &initial, &readiness)
	} else {
		err = tx.QueryRowContext(ctx, query,
			record.Route.InstancePath,
			record.RunID, record.EntityID,
			record.RunID, record.EntityID,
			record.RunID, record.Route.InstancePath,
		).Scan(&flow, &entity, &initial, &readiness)
	}
	return flow || entity || initial || readiness, err
}

func insertFlowInstanceActivation(
	ctx context.Context,
	tx *sql.Tx,
	story *privateauthoractivity.Mutation,
	postgres bool,
	record runtimepipeline.FlowInstanceActivationRecord,
) error {
	if postgres {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
			VALUES ($1, $2, $3, $4::jsonb, 'active', $5)
		`, record.Route.InstancePath, record.WorkflowName, record.Mode, record.Config, record.CreatedAt); err != nil {
			return fmt.Errorf("insert flow instance: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES (
				$1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
				$7, $8::jsonb, $9::jsonb, $10::jsonb, 1, $11, $12, $12
			)
		`, record.RunID, record.EntityID, record.Route.InstancePath, record.EntityType, record.Slug, record.Name,
			record.CurrentState, record.Gates, record.Fields, record.Accumulator, record.EnteredStageAt, record.CreatedAt); err != nil {
			return fmt.Errorf("insert flow instance entity state: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workflow_instance_initial_materializations (
				run_id, entity_id, instance_id, projection_version, projection, occurred_at
			) VALUES ($1::uuid, $2::uuid, $3, 1, $4::jsonb, $5)
		`, record.RunID, record.EntityID, record.Route.InstancePath, record.InitialMaterialization, record.CreatedAt); err != nil {
			return fmt.Errorf("insert flow initial materialization: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO flow_instance_runtime_readiness (
				run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
			) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)
		`, record.RunID, record.Route.InstancePath, record.Readiness, record.CreatedAt); err != nil {
			return fmt.Errorf("insert flow runtime readiness: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES (?, ?, ?, ?, 'active', ?)
	`, record.Route.InstancePath, record.WorkflowName, record.Mode, record.Config, record.CreatedAt); err != nil {
		return fmt.Errorf("insert sqlite flow instance: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, 1, ?, ?, ?)
	`, record.RunID, record.EntityID, record.Route.InstancePath, record.EntityType, record.Slug, record.Name,
		record.CurrentState, record.Gates, record.Fields, record.Accumulator, record.EnteredStageAt, record.CreatedAt, record.CreatedAt); err != nil {
		return fmt.Errorf("insert sqlite flow instance entity state: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO workflow_instance_initial_materializations (
			run_id, entity_id, instance_id, projection_version, projection, occurred_at
		) VALUES (?, ?, ?, 1, ?, ?)
	`, record.RunID, record.EntityID, record.Route.InstancePath, record.InitialMaterialization, record.CreatedAt); err != nil {
		return fmt.Errorf("insert sqlite flow initial materialization: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_instance_runtime_readiness (
			run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at
		) VALUES (?, ?, ?, NULL, NULL, ?, ?)
	`, record.RunID, record.Route.InstancePath, record.Readiness, record.CreatedAt, record.CreatedAt); err != nil {
		return fmt.Errorf("insert sqlite flow runtime readiness: %w", err)
	}
	return nil
}

func canonicalActivationTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}
