package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

func (s *PipelinePostgresOwner) ReconcileDynamicFlowRuntimeReadinessPlans(ctx context.Context, requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, observedAt time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	return reconcileDynamicFlowRuntimeReadinessPlans(ctx, true, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, newRevisionEffects(), fn)
	}, requests, observedAt)
}

func (s *PipelinePostgresOwner) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("postgres dynamic flow source projection reader is required")
	}
	return inspectDynamicFlowRuntimeReadinessForSource(ctx, s.backend, true, source)
}

func (s *PipelineSQLiteOwner) InspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("sqlite dynamic flow source projection reader is required")
	}
	return inspectDynamicFlowRuntimeReadinessForSource(ctx, s.backend, false, source)
}

func inspectDynamicFlowRuntimeReadinessForSource(ctx context.Context, db dynamicFlowReadinessQueryer, postgres bool, source runtimecorrelation.SourceArtifactFact) (runtimepipeline.DynamicFlowRuntimeReadinessProjection, error) {
	if err := source.Validate(); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("dynamic flow readiness source projection: %w", err)
	}
	bundleHash := source.BundleHash()
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan,
		       readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND run.bundle_hash = $1
		ORDER BY readiness.run_id, readiness.instance_id`
	if !postgres {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan,
			       readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			  AND run.bundle_hash = ?
			ORDER BY readiness.run_id, readiness.instance_id`
	}
	items, err := queryDynamicFlowRuntimeReadiness(ctx, db, query, bundleHash)
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadinessProjection{}, fmt.Errorf("inspect source-scoped dynamic flow readiness: %w", err)
	}
	projection := runtimepipeline.DynamicFlowRuntimeReadinessProjection{}
	for _, item := range items {
		planSource, err := runtimecorrelation.DecodeSourceArtifactFact(item.Plan.BundleHash)
		if err != nil {
			return projection, fmt.Errorf("dynamic flow readiness %s plan source: %w", item.InstancePath, err)
		}
		if !planSource.Matches(source) {
			projection.SourceTransitionRequired = append(projection.SourceTransitionRequired, item)
			continue
		}
		if item.Pending() {
			projection.CurrentPending = append(projection.CurrentPending, item)
		} else {
			projection.CurrentCompleted = append(projection.CurrentCompleted, item)
		}
	}

	invalidQuery := `
		SELECT route.flow_instance
		FROM routing_rules AS route
		JOIN flow_instances AS instance ON instance.instance_id = route.flow_instance
		JOIN entity_state AS entity ON entity.flow_instance = instance.instance_id
		JOIN runs AS run ON run.run_id = entity.run_id
		LEFT JOIN flow_instance_runtime_readiness AS readiness ON readiness.run_id = run.run_id AND readiness.instance_id = instance.instance_id
		WHERE LOWER(BTRIM(route.status)) = 'active' AND route.is_materialized = TRUE
		  AND LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(instance.mode)) = 'template'
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND run.bundle_hash = $1 AND readiness.run_id IS NULL
		LIMIT 1`
	if !postgres {
		invalidQuery = strings.ReplaceAll(invalidQuery, "$1", "?")
		invalidQuery = strings.ReplaceAll(invalidQuery, "BTRIM", "TRIM")
	}
	var invalidPath string
	err = db.QueryRowContext(ctx, invalidQuery, bundleHash).Scan(&invalidPath)
	if err == nil {
		return projection, fmt.Errorf("source-owned active flow route %s has no dynamic runtime readiness owner", strings.TrimSpace(invalidPath))
	}
	if err != sql.ErrNoRows {
		return projection, fmt.Errorf("inspect source-scoped route ownership: %w", err)
	}
	return projection, nil
}

func (s *PipelinePostgresOwner) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres dynamic flow run projection reader is required")
	}
	return inspectDynamicFlowRuntimeReadinessForRun(ctx, s.backend, true, runID, source)
}

func (s *PipelineSQLiteOwner) InspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, runID string, source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite dynamic flow run projection reader is required")
	}
	return inspectDynamicFlowRuntimeReadinessForRun(ctx, s.backend, false, runID, source)
}

func inspectDynamicFlowRuntimeReadinessForRun(ctx context.Context, db dynamicFlowReadinessQueryer, postgres bool, runID string, source runtimecorrelation.SourceArtifactFact) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return nil, fmt.Errorf("dynamic flow run projection requires valid run_id: %w", err)
	}
	if err := source.Validate(); err != nil {
		return nil, fmt.Errorf("dynamic flow run projection source: %w", err)
	}
	bundleHash := source.BundleHash()
	query := `
		SELECT readiness.run_id::text, readiness.instance_id, readiness.plan,
		       readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid
		  AND run.bundle_hash = $2
		  AND LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY readiness.instance_id`
	if !postgres {
		query = `
			SELECT readiness.run_id, readiness.instance_id, readiness.plan,
			       readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ?
			  AND run.bundle_hash = ?
			  AND LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			ORDER BY readiness.instance_id`
	}
	items, err := queryDynamicFlowRuntimeReadiness(ctx, db, query, runID, bundleHash)
	if err != nil {
		return nil, fmt.Errorf("inspect run-scoped dynamic flow readiness: %w", err)
	}
	return items, nil
}

func queryDynamicFlowRuntimeReadiness(ctx context.Context, db dynamicFlowReadinessQueryer, query string, args ...any) ([]runtimepipeline.DynamicFlowRuntimeReadiness, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]runtimepipeline.DynamicFlowRuntimeReadiness, 0)
	for rows.Next() {
		var record runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord
		var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt any
		if err := rows.Scan(
			&record.RunID, &record.InstancePath, &record.Plan,
			&topologyReadyAt, &creationEventEmittedAt,
			&record.OwningRunBundleHash, &record.RunStatus,
			&record.InstanceStatus, &instanceTerminatedAt,
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
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *PipelineSQLiteOwner) ReconcileDynamicFlowRuntimeReadinessPlans(ctx context.Context, requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, observedAt time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	return reconcileDynamicFlowRuntimeReadinessPlans(ctx, false, func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error {
		return s.runPrivateAuthorActivityMutation(ctx, "sqlite dynamic flow readiness reconciliation", newRevisionEffects(), fn)
	}, requests, observedAt)
}

type preparedDynamicFlowRuntimeReadinessReconciliation struct {
	observed     runtimepipeline.DynamicFlowRuntimeReadiness
	expected     runtimepipeline.DynamicFlowRuntimeReadinessPlan
	expectedJSON []byte
}

func reconcileDynamicFlowRuntimeReadinessPlans(
	ctx context.Context,
	postgres bool,
	run func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) error,
	requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation,
	observedAt time.Time,
) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	observedAt = observedAt.UTC().Truncate(time.Microsecond)
	if observedAt.IsZero() {
		return nil, fmt.Errorf("dynamic flow runtime readiness reconciliation requires an exact occurrence time")
	}
	prepared := make([]preparedDynamicFlowRuntimeReadinessReconciliation, 0, len(requests))
	seen := make(map[runtimepipeline.DynamicFlowRuntimeReadinessKey]struct{}, len(requests))
	var requestedSource runtimecorrelation.SourceArtifactFact
	for index, request := range requests {
		observedPlan, err := request.Observed.Plan.Normalized()
		if err != nil {
			return nil, fmt.Errorf("normalize observed dynamic flow runtime readiness: %w", err)
		}
		normalized, err := request.Expected.Normalized()
		if err != nil {
			return nil, err
		}
		instancePath := normalized.Identity.InstancePath
		if !request.Observed.Eligible() || observedPlan.RunID != normalized.RunID ||
			observedPlan.Identity.InstancePath != instancePath ||
			strings.Trim(strings.TrimSpace(request.Observed.InstancePath), "/") != instancePath {
			return nil, fmt.Errorf("dynamic flow runtime readiness reconciliation requires exact eligible observations")
		}
		if err := request.Observed.OwningRunSource.Validate(); err != nil {
			return nil, fmt.Errorf("dynamic flow runtime readiness observed owning source: %w", err)
		}
		desiredSource, err := runtimecorrelation.DecodeSourceArtifactFact(normalized.BundleHash)
		if err != nil {
			return nil, fmt.Errorf("dynamic flow runtime readiness desired source for %s: %w", instancePath, err)
		}
		if index == 0 {
			requestedSource = desiredSource
		} else if !desiredSource.Matches(requestedSource) {
			return nil, fmt.Errorf("dynamic flow runtime readiness batch requires one exact owning source")
		}
		key := runtimepipeline.DynamicFlowRuntimeReadinessKey{RunID: normalized.RunID, InstancePath: instancePath}
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("dynamic flow runtime readiness batch contains duplicate identity %s/%s", key.RunID, key.InstancePath)
		}
		seen[key] = struct{}{}
		expectedJSON, err := runtimecanonicaljson.Bytes(normalized)
		if err != nil {
			return nil, fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		prepared = append(prepared, preparedDynamicFlowRuntimeReadinessReconciliation{
			observed: request.Observed, expected: normalized, expectedJSON: expectedJSON,
		})
	}
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].expected.RunID != prepared[j].expected.RunID {
			return prepared[i].expected.RunID < prepared[j].expected.RunID
		}
		return prepared[i].expected.Identity.InstancePath < prepared[j].expected.Identity.InstancePath
	})
	results := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, len(prepared))
	err := run(ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		for index, request := range prepared {
			instancePath := request.expected.Identity.InstancePath
			loaded, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, request.expected.RunID, request.expected.Identity.Route(), true)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation requires one active eligible record: %s", instancePath)
			}
			coordinate, err := changedDynamicFlowRuntimeReadinessObservationCoordinate(request.observed, loaded)
			if err != nil {
				return err
			}
			if coordinate != "" {
				return &runtimepipeline.DynamicFlowRuntimeReadinessObservationConflict{
					RunID: request.expected.RunID, InstancePath: instancePath, Coordinate: coordinate,
				}
			}
			if !loaded.Eligible() {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation requires one active eligible record: %s", instancePath)
			}
			desiredSource, err := runtimecorrelation.DecodeSourceArtifactFact(request.expected.BundleHash)
			if err != nil || !desiredSource.Matches(loaded.OwningRunSource) {
				return fmt.Errorf("dynamic flow runtime readiness desired source is not the owning run source for %s", instancePath)
			}
			if loaded.Plan.Identity != request.expected.Identity || loaded.Plan.RunID != request.expected.RunID {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation identity changed for %s", instancePath)
			}
			if loaded.Plan.ExecutionMode != request.expected.ExecutionMode {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation execution mode changed for %s", instancePath)
			}
			actualJSON, err := runtimecanonicaljson.Bytes(loaded.Plan)
			if err != nil {
				return fmt.Errorf("encode persisted dynamic flow runtime readiness %s: %w", instancePath, err)
			}
			changed := string(actualJSON) != string(request.expectedJSON)
			if changed && !loaded.CreationEventEmittedAt.IsZero() {
				actualCreationJSON, err := runtimecanonicaljson.Bytes(loaded.Plan.CreationEvent)
				if err != nil {
					return fmt.Errorf("encode emitted dynamic flow creation plan %s: %w", instancePath, err)
				}
				expectedCreationJSON, err := runtimecanonicaljson.Bytes(request.expected.CreationEvent)
				if err != nil {
					return fmt.Errorf("encode revised dynamic flow creation plan %s: %w", instancePath, err)
				}
				if string(actualCreationJSON) != string(expectedCreationJSON) {
					return fmt.Errorf("dynamic flow runtime readiness cannot revise emitted creation occurrence for %s", instancePath)
				}
			}
			results[index] = runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult{
				RunID: request.expected.RunID, InstancePath: instancePath, Changed: changed,
			}
		}
		for index, request := range prepared {
			if !results[index].Changed {
				continue
			}
			query := `UPDATE flow_instance_runtime_readiness SET plan = $1::jsonb, topology_ready_at = NULL, updated_at = $2 WHERE run_id = $3::uuid AND instance_id = $4`
			args := []any{request.expectedJSON, observedAt, request.expected.RunID, request.expected.Identity.InstancePath}
			if !postgres {
				query = `UPDATE flow_instance_runtime_readiness SET plan = ?, topology_ready_at = NULL, updated_at = ? WHERE run_id = ? AND instance_id = ?`
			}
			result, err := tx.ExecContext(txctx, query, args...)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("count dynamic flow runtime readiness reconciliation rows for %s: %w", request.expected.Identity.InstancePath, err)
			}
			if rows != 1 {
				return fmt.Errorf("dynamic flow runtime readiness reconciliation changed %d rows for %s", rows, request.expected.Identity.InstancePath)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func changedDynamicFlowRuntimeReadinessObservationCoordinate(observed, current runtimepipeline.DynamicFlowRuntimeReadiness) (string, error) {
	observedPlan, err := observed.Plan.Normalized()
	if err != nil {
		return "", fmt.Errorf("normalize observed dynamic flow readiness plan: %w", err)
	}
	currentPlan, err := current.Plan.Normalized()
	if err != nil {
		return "", fmt.Errorf("normalize current dynamic flow readiness plan: %w", err)
	}
	observedJSON, err := runtimecanonicaljson.Bytes(observedPlan)
	if err != nil {
		return "", fmt.Errorf("encode observed dynamic flow readiness plan: %w", err)
	}
	currentJSON, err := runtimecanonicaljson.Bytes(currentPlan)
	if err != nil {
		return "", fmt.Errorf("encode current dynamic flow readiness plan: %w", err)
	}
	if string(observedJSON) != string(currentJSON) {
		return "plan", nil
	}
	if !observed.OwningRunSource.Matches(current.OwningRunSource) {
		return "owning_run_source", nil
	}
	if strings.TrimSpace(observed.RunStatus) != strings.TrimSpace(current.RunStatus) {
		return "run_status", nil
	}
	if strings.TrimSpace(observed.InstanceStatus) != strings.TrimSpace(current.InstanceStatus) {
		return "instance_status", nil
	}
	if !sameDynamicFlowRuntimeReadinessTime(observed.InstanceTerminatedAt, current.InstanceTerminatedAt) {
		return "instance_terminated_at", nil
	}
	if !sameDynamicFlowRuntimeReadinessTime(observed.TopologyReadyAt, current.TopologyReadyAt) {
		return "topology_ready_at", nil
	}
	if !sameDynamicFlowRuntimeReadinessTime(observed.CreationEventEmittedAt, current.CreationEventEmittedAt) {
		return "creation_event_emitted_at", nil
	}
	return "", nil
}

func sameDynamicFlowRuntimeReadinessTime(left, right time.Time) bool {
	if left.IsZero() || right.IsZero() {
		return left.IsZero() && right.IsZero()
	}
	return left.UTC().Equal(right.UTC())
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
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
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
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_id = ?`
	}
	record := runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord{RunID: runID, InstancePath: route.InstancePath}
	var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt any
	err := queryer.QueryRowContext(ctx, query, runID, route.InstancePath).Scan(
		&record.Plan, &topologyReadyAt, &creationEventEmittedAt,
		&record.OwningRunBundleHash, &record.RunStatus,
		&record.InstanceStatus, &instanceTerminatedAt,
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
