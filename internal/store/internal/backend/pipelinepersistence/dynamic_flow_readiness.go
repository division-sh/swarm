package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func (s *PipelinePostgresOwner) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	return reconcileDynamicFlowRuntimeReadinessPlan(ctx, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, newRevisionEffects(), fn)
	}, expected, observedAt)
}

func (s *PipelineSQLiteOwner) ReconcileDynamicFlowRuntimeReadinessPlan(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, observedAt time.Time) (bool, error) {
	return reconcileDynamicFlowRuntimeReadinessPlan(ctx, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite dynamic flow readiness reconciliation", newRevisionEffects(), fn)
	}, expected, observedAt)
}

func reconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	observedAt time.Time,
) (bool, error) {
	normalized, err := expected.Normalized()
	if err != nil {
		return false, err
	}
	observedAt = observedAt.UTC().Truncate(time.Microsecond)
	if observedAt.IsZero() {
		return false, fmt.Errorf("dynamic flow runtime readiness reconciliation requires an exact occurrence time")
	}
	expectedJSON, err := runtimecanonicaljson.Bytes(normalized)
	if err != nil {
		return false, fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", normalized.Identity.InstancePath, err)
	}
	changed := false
	err = run(ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		current, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, normalized.RunID, normalized.Identity.Route(), true)
		if err != nil {
			return err
		}
		instancePath := normalized.Identity.InstancePath
		if !found || !current.Eligible() {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation requires one active eligible record: %s", instancePath)
		}
		if current.Plan.Identity != normalized.Identity || current.Plan.RunID != normalized.RunID {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation identity changed for %s", instancePath)
		}
		if current.Plan.ExecutionMode != normalized.ExecutionMode {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation execution mode changed for %s", instancePath)
		}
		actualJSON, err := runtimecanonicaljson.Bytes(current.Plan)
		if err != nil {
			return fmt.Errorf("encode persisted dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		if string(actualJSON) == string(expectedJSON) {
			return nil
		}
		if !current.CreationEventEmittedAt.IsZero() {
			actualCreationJSON, err := runtimecanonicaljson.Bytes(current.Plan.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode emitted dynamic flow creation plan %s: %w", instancePath, err)
			}
			expectedCreationJSON, err := runtimecanonicaljson.Bytes(normalized.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode revised dynamic flow creation plan %s: %w", instancePath, err)
			}
			if string(actualCreationJSON) != string(expectedCreationJSON) {
				return fmt.Errorf("dynamic flow runtime readiness cannot revise emitted creation occurrence for %s", instancePath)
			}
		}
		query := `UPDATE flow_instance_runtime_readiness SET plan = $1::jsonb, topology_ready_at = NULL, updated_at = $2 WHERE run_id = $3::uuid AND instance_id = $4`
		args := []any{expectedJSON, observedAt, normalized.RunID, instancePath}
		if !postgres {
			query = `UPDATE flow_instance_runtime_readiness SET plan = ?, topology_ready_at = NULL, updated_at = ? WHERE run_id = ? AND instance_id = ?`
		}
		result, err := tx.ExecContext(txctx, query, args...)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("count dynamic flow runtime readiness reconciliation rows for %s: %w", instancePath, err)
		}
		if rows != 1 {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation changed %d rows for %s", rows, instancePath)
		}
		changed = true
		return nil
	})
	return changed, err
}

func (s *PipelinePostgresOwner) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("postgres dynamic flow readiness reader is required")
	}
	return loadDynamicFlowRuntimeReadiness(ctx, s.backend, true, runID, route, false)
}

func (s *PipelineSQLiteOwner) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route runtimeflowidentity.Route) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("sqlite dynamic flow readiness reader is required")
	}
	return loadDynamicFlowRuntimeReadiness(ctx, s.backend, false, runID, route, false)
}

type dynamicFlowReadinessQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadDynamicFlowRuntimeReadiness(ctx context.Context, queryer dynamicFlowReadinessQueryer, postgres bool, runID string, route runtimeflowidentity.Route, lock bool) (runtimepipeline.DynamicFlowRuntimeReadiness, bool, error) {
	runID = strings.TrimSpace(runID)
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if _, err := uuid.Parse(runID); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires valid run_id: %w", err)
	}
	if !route.Valid() {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires an exact instance route")
	}
	query := `
		SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.status, instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid AND readiness.instance_id = $2`
	if lock {
		query += ` FOR UPDATE OF readiness, instance, run`
	}
	if !postgres {
		query = `
			SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.status, instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_id = ?`
	}
	record := runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord{RunID: runID, InstancePath: route.InstancePath}
	var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt any
	err := queryer.QueryRowContext(ctx, query, runID, route.InstancePath).Scan(
		&record.Plan, &topologyReadyAt, &creationEventEmittedAt,
		&record.RunStatus, &record.InstanceStatus, &instanceTerminatedAt,
	)
	if err == sql.ErrNoRows {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, nil
	}
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("load dynamic flow runtime readiness %s: %w", route.InstancePath, err)
	}
	if record.TopologyReadyAt, record.HasTopologyReadyAt, err = sqliteTimeValue(topologyReadyAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	if record.CreationEventEmittedAt, record.HasCreationEventEmittedAt, err = sqliteTimeValue(creationEventEmittedAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	if record.InstanceTerminatedAt, record.HasInstanceTerminatedAt, err = sqliteTimeValue(instanceTerminatedAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	item, err := runtimepipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(record)
	return item, err == nil, err
}

func (s *PipelinePostgresOwner) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres dynamic flow readiness reader is required")
	}
	return listDynamicFlowRuntimeReadiness(ctx, s.backend, true)
}

func (s *PipelineSQLiteOwner) ListDynamicFlowRuntimeReadiness(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite dynamic flow readiness reader is required")
	}
	return listDynamicFlowRuntimeReadiness(ctx, s.backend, false)
}

func listDynamicFlowRuntimeReadiness(ctx context.Context, db dynamicFlowReadinessQueryer, postgres bool) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan,
		       readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.status, instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY readiness.run_id, readiness.instance_id`
	if !postgres {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan,
			       readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.status, instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			ORDER BY readiness.run_id, readiness.instance_id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list dynamic flow runtime readiness: %w", err)
	}
	defer rows.Close()
	result := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0)
	for rows.Next() {
		var record runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord
		var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt any
		if err := rows.Scan(
			&record.RunID, &record.InstancePath, &record.Plan,
			&topologyReadyAt, &creationEventEmittedAt,
			&record.RunStatus, &record.InstanceStatus, &instanceTerminatedAt,
		); err != nil {
			return nil, err
		}
		if record.TopologyReadyAt, record.HasTopologyReadyAt, err = sqliteTimeValue(topologyReadyAt); err != nil {
			return nil, err
		}
		if record.CreationEventEmittedAt, record.HasCreationEventEmittedAt, err = sqliteTimeValue(creationEventEmittedAt); err != nil {
			return nil, err
		}
		if record.InstanceTerminatedAt, record.HasInstanceTerminatedAt, err = sqliteTimeValue(instanceTerminatedAt); err != nil {
			return nil, err
		}
		item, err := runtimepipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(record)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *PipelinePostgresOwner) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres dynamic flow readiness reader is required")
	}
	return listDynamicFlowRuntimeReadinessKeys(ctx, s.backend, true)
}

func (s *PipelineSQLiteOwner) ListDynamicFlowRuntimeReadinessKeys(ctx context.Context) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite dynamic flow readiness reader is required")
	}
	return listDynamicFlowRuntimeReadinessKeys(ctx, s.backend, false)
}

func listDynamicFlowRuntimeReadinessKeys(ctx context.Context, db dynamicFlowReadinessQueryer, postgres bool) ([]runtimepipeline.DynamicFlowRuntimeReadinessKey, error) {
	query := `SELECT run_id::text, instance_id FROM flow_instance_runtime_readiness ORDER BY run_id, instance_id`
	if !postgres {
		query = `SELECT run_id, instance_id FROM flow_instance_runtime_readiness ORDER BY run_id, instance_id`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list dynamic flow runtime readiness keys: %w", err)
	}
	defer rows.Close()
	result := make([]runtimepipeline.DynamicFlowRuntimeReadinessKey, 0)
	for rows.Next() {
		var key runtimepipeline.DynamicFlowRuntimeReadinessKey
		if err := rows.Scan(&key.RunID, &key.InstancePath); err != nil {
			return nil, err
		}
		key.RunID = strings.TrimSpace(key.RunID)
		key.InstancePath = strings.Trim(strings.TrimSpace(key.InstancePath), "/")
		result = append(result, key)
	}
	return result, rows.Err()
}

func (s *PipelinePostgresOwner) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	return markDynamicFlowRuntimeTopologyReady(ctx, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, newRevisionEffects(), fn)
	}, expected, readyAt)
}

func (s *PipelineSQLiteOwner) MarkDynamicFlowRuntimeTopologyReady(ctx context.Context, expected runtimepipeline.DynamicFlowRuntimeReadinessPlan, readyAt time.Time) error {
	return markDynamicFlowRuntimeTopologyReady(ctx, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite dynamic flow topology readiness", newRevisionEffects(), fn)
	}, expected, readyAt)
}

func markDynamicFlowRuntimeTopologyReady(
	ctx context.Context,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	expected runtimepipeline.DynamicFlowRuntimeReadinessPlan,
	readyAt time.Time,
) error {
	normalized, err := expected.Normalized()
	if err != nil {
		return fmt.Errorf("normalize dynamic flow runtime topology readiness plan: %w", err)
	}
	expectedJSON, err := runtimecanonicaljson.Bytes(normalized)
	if err != nil {
		return fmt.Errorf("encode dynamic flow runtime topology readiness plan: %w", err)
	}
	readyAt = readyAt.UTC()
	if readyAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime topology readiness requires an exact occurrence time")
	}
	return run(ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		query := `
			UPDATE flow_instance_runtime_readiness AS readiness
			SET topology_ready_at = COALESCE(readiness.topology_ready_at, $1), updated_at = $1
			WHERE readiness.run_id = $2::uuid AND readiness.instance_id = $3 AND readiness.plan = $4::jsonb
			  AND EXISTS (
				SELECT 1 FROM flow_instances AS instance JOIN runs AS run ON run.run_id = readiness.run_id
				WHERE instance.instance_id = readiness.instance_id
				  AND LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
				  AND LOWER(BTRIM(run.status)) IN ('running', 'paused'))`
		args := []any{readyAt, normalized.RunID, normalized.Identity.InstancePath, expectedJSON}
		if !postgres {
			query = `
				UPDATE flow_instance_runtime_readiness
				SET topology_ready_at = COALESCE(topology_ready_at, ?), updated_at = ?
				WHERE run_id = ? AND instance_id = ? AND plan = ?
				  AND EXISTS (
					SELECT 1 FROM flow_instances AS instance JOIN runs AS run ON run.run_id = flow_instance_runtime_readiness.run_id
					WHERE instance.instance_id = flow_instance_runtime_readiness.instance_id
					  AND LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
					  AND LOWER(TRIM(run.status)) IN ('running', 'paused'))`
			args = []any{readyAt, readyAt, normalized.RunID, normalized.Identity.InstancePath, expectedJSON}
		}
		result, err := tx.ExecContext(txctx, query, args...)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("dynamic flow runtime readiness %s transition topology_ready_at requires one active record", normalized.Identity.InstancePath)
		}
		return nil
	})
}

var _ runtimepipeline.DynamicFlowRuntimeReadinessPersistence = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.DynamicFlowRuntimeReadinessPersistence = (*PipelineSQLiteOwner)(nil)
