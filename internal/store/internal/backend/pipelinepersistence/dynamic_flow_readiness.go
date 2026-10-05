package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	runtimecanonicaljson "github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func standalonePipelineMutationError[T any](result mutationprotocol.Result[T]) error {
	if err := result.Err(); err != nil {
		return err
	}
	if !result.Acknowledged() {
		return fmt.Errorf("pipeline mutation commit was not acknowledged")
	}
	return nil
}

func (s *PipelinePostgresOwner) ReconcileDynamicFlowRuntimeReadinessPlans(ctx context.Context, requests []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliation, observedAt time.Time) ([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, error) {
	return reconcileDynamicFlowRuntimeReadinessPlans(ctx, true, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) error) error {
		if err := s.requireCurrentSchema(); err != nil {
			return err
		}
		return standalonePipelineMutationError(mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, fn(ctx, attempt)
		}))
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
		SELECT readiness.run_id::text, readiness.instance_path, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id,
		       readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND run.bundle_hash = $1
		ORDER BY readiness.run_id, readiness.instance_path`
	if !postgres {
		query = `
			SELECT readiness.run_id, readiness.instance_path, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id,
			       readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			  AND run.bundle_hash = ?
			ORDER BY readiness.run_id, readiness.instance_path`
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

	if err := inspectActiveFlowRouteReadiness(ctx, db, postgres, bundleHash, ""); err != nil {
		return projection, err
	}
	return projection, nil
}

func inspectActiveFlowRouteReadiness(ctx context.Context, db dynamicFlowReadinessQueryer, postgres bool, bundleHash, runID string) error {
	query := `
		SELECT route.flow_instance
		FROM routing_rules AS route
		JOIN flow_instances AS instance ON instance.run_id = route.run_id AND instance.instance_path = route.flow_instance
		JOIN runs AS run ON run.run_id = instance.run_id
		LEFT JOIN flow_instance_runtime_readiness AS readiness ON readiness.run_id = run.run_id AND readiness.instance_path = instance.instance_path
		WHERE LOWER(BTRIM(route.status)) = 'active' AND route.is_materialized = TRUE
		  AND LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND run.bundle_hash = $1 AND readiness.run_id IS NULL`
	args := []any{bundleHash}
	if !postgres {
		query = strings.ReplaceAll(query, "$1", "?1")
		query = strings.ReplaceAll(query, "BTRIM", "TRIM")
	}
	if runID != "" {
		if postgres {
			query += " AND run.run_id = $2::uuid"
		} else {
			query += " AND run.run_id = ?2"
		}
		args = append(args, runID)
	}
	query += " LIMIT 1"
	var invalidPath string
	err := db.QueryRowContext(ctx, query, args...).Scan(&invalidPath)
	if err == nil {
		return fmt.Errorf("source-owned active flow route %s has no dynamic runtime readiness owner", strings.TrimSpace(invalidPath))
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("inspect scoped route ownership: %w", err)
	}
	return nil
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
		SELECT readiness.run_id::text, readiness.instance_path, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id,
		       readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid
		  AND run.bundle_hash = $2
		  AND LOWER(BTRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY readiness.instance_path`
	if !postgres {
		query = `
			SELECT readiness.run_id, readiness.instance_path, readiness.plan, readiness.plan_hash, readiness.activation_attempt_id,
			       readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ?
			  AND run.bundle_hash = ?
			  AND LOWER(TRIM(instance.status)) = 'active' AND instance.terminated_at IS NULL
			  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
			ORDER BY readiness.instance_path`
	}
	items, err := queryDynamicFlowRuntimeReadiness(ctx, db, query, runID, bundleHash)
	if err != nil {
		return nil, fmt.Errorf("inspect run-scoped dynamic flow readiness: %w", err)
	}
	if err := inspectActiveFlowRouteReadiness(ctx, db, postgres, bundleHash, runID); err != nil {
		return nil, err
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
		var attemptState sql.NullString
		var creationEventEmittedAt, instanceTerminatedAt any
		if err := rows.Scan(
			&record.RunID, &record.InstancePath, &record.Plan, &record.PlanHash, &record.AttemptOrdinal,
			&record.Phase, &attemptState, &creationEventEmittedAt,
			&record.OwningRunBundleHash, &record.RunStatus,
			&record.InstanceStatus, &instanceTerminatedAt,
		); err != nil {
			return nil, err
		}
		record.AttemptState = attemptState.String
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
	return reconcileDynamicFlowRuntimeReadinessPlans(ctx, false, func(ctx context.Context, fn func(context.Context, *mutationprotocol.Attempt) error) error {
		if err := s.requireCurrentSchema(); err != nil {
			return err
		}
		return standalonePipelineMutationError(mutationprotocol.RunSQLite(ctx, s.backend, "sqlite dynamic flow readiness reconciliation", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, fn(ctx, attempt)
		}))
	}, requests, observedAt)
}

type preparedDynamicFlowRuntimeReadinessReconciliation struct {
	observed     runtimepipeline.DynamicFlowRuntimeReadiness
	expected     runtimepipeline.DynamicFlowRuntimeReadinessPlan
	expectedJSON []byte
	expectedHash string
}

func reconcileDynamicFlowRuntimeReadinessPlans(
	ctx context.Context,
	postgres bool,
	run func(context.Context, func(context.Context, *mutationprotocol.Attempt) error) error,
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
		observedHash, err := observedPlan.Hash()
		if err != nil || request.Observed.PlanHash != observedHash {
			return nil, fmt.Errorf("dynamic flow runtime readiness reconciliation requires an exact observed plan hash")
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
		expectedJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(normalized)
		if err != nil {
			return nil, fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		expectedHash, err := normalized.Hash()
		if err != nil {
			return nil, err
		}
		prepared = append(prepared, preparedDynamicFlowRuntimeReadinessReconciliation{
			observed: request.Observed, expected: normalized, expectedJSON: expectedJSON, expectedHash: expectedHash,
		})
	}
	sort.Slice(prepared, func(i, j int) bool {
		if prepared[i].expected.RunID != prepared[j].expected.RunID {
			return prepared[i].expected.RunID < prepared[j].expected.RunID
		}
		return prepared[i].expected.Identity.InstancePath < prepared[j].expected.Identity.InstancePath
	})
	var results []runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult
	err := run(ctx, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		attemptResults := make([]runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult, len(prepared))
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
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
				changed := loaded.PlanHash != request.expectedHash
				if changed && !loaded.CreationEventEmittedAt.IsZero() {
					actualCreationJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(loaded.Plan.CreationEvent)
					if err != nil {
						return fmt.Errorf("encode emitted dynamic flow creation plan %s: %w", instancePath, err)
					}
					expectedCreationJSON, err := runtimecanonicaljson.MarshalPreservingNumberKinds(request.expected.CreationEvent)
					if err != nil {
						return fmt.Errorf("encode revised dynamic flow creation plan %s: %w", instancePath, err)
					}
					if string(actualCreationJSON) != string(expectedCreationJSON) {
						return fmt.Errorf("dynamic flow runtime readiness cannot revise emitted creation occurrence for %s", instancePath)
					}
				}
				attemptResults[index] = runtimepipeline.DynamicFlowRuntimeReadinessPlanReconciliationResult{
					RunID: request.expected.RunID, InstancePath: instancePath, Changed: changed,
					AttemptOrdinal: loaded.AttemptOrdinal,
				}
				if changed && loaded.AttemptState != "accepted" && loaded.AttemptState != "superseded" {
					if loaded.AttemptOrdinal == math.MaxInt64 {
						return fmt.Errorf("flow activation attempt cannot advance its ordinal")
					}
					attemptResults[index].AttemptOrdinal++
				}
			}
			for index, request := range prepared {
				if !attemptResults[index].Changed {
					continue
				}
				query := `UPDATE flow_instance_runtime_readiness SET plan = $1::jsonb, plan_hash = $2,
					activation_attempt_id = $6,
					activation_attempt_state = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN 'superseded' ELSE 'planned' END,
					activation_attempt_grant_id = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN activation_attempt_grant_id ELSE NULL END,
					activation_request_id = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN activation_request_id ELSE NULL END,
					phase = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN phase ELSE 'planned' END,
					updated_at = $3 WHERE run_id = $4::uuid AND instance_path = $5 AND activation_attempt_id = $7 AND plan_hash = $8 AND activation_attempt_state = $9 AND phase = $10`
				args := []any{request.expectedJSON, request.expectedHash, observedAt, request.expected.RunID, request.expected.Identity.InstancePath}
				if !postgres {
					query = `UPDATE flow_instance_runtime_readiness SET plan = ?, plan_hash = ?, updated_at = ?,
						activation_attempt_id = ?,
						activation_attempt_state = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN 'superseded' ELSE 'planned' END,
						activation_attempt_grant_id = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN activation_attempt_grant_id ELSE NULL END,
						activation_request_id = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN activation_request_id ELSE NULL END,
						phase = CASE WHEN activation_attempt_state IN ('accepted', 'superseded') THEN phase ELSE 'planned' END
						WHERE run_id = ? AND instance_path = ? AND activation_attempt_id = ? AND plan_hash = ? AND activation_attempt_state = ? AND phase = ?`
					args = []any{request.expectedJSON, request.expectedHash, observedAt, attemptResults[index].AttemptOrdinal, request.expected.RunID, request.expected.Identity.InstancePath}
				} else {
					args = append(args, attemptResults[index].AttemptOrdinal)
				}
				args = append(args, request.observed.AttemptOrdinal, request.observed.PlanHash, request.observed.AttemptState, request.observed.Phase)
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
			for index, request := range prepared {
				current, found, err := loadDynamicFlowRuntimeReadiness(txctx, tx, postgres, request.expected.RunID, request.expected.Identity.Route(), false)
				if err != nil || !found {
					return errors.Join(err, errors.New("reconciled attachment row is missing"))
				}
				attemptResults[index].Readiness = current
			}
			return nil
		})
		if err == nil {
			results = attemptResults
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func changedDynamicFlowRuntimeReadinessObservationCoordinate(observed, current runtimepipeline.DynamicFlowRuntimeReadiness) (string, error) {
	if observed.AttemptOrdinal != current.AttemptOrdinal {
		return "activation_attempt_id", nil
	}
	if observed.AttemptState != current.AttemptState {
		return "activation_attempt_state", nil
	}
	if observed.PlanHash != current.PlanHash {
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
	if observed.Phase != current.Phase {
		return "phase", nil
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
		SELECT readiness.plan, readiness.plan_hash, readiness.activation_attempt_id, readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid AND readiness.instance_path = $2`
	if lock {
		query += ` FOR UPDATE OF readiness, instance, run`
	}
	if !postgres {
		query = `
			SELECT readiness.plan, readiness.plan_hash, readiness.activation_attempt_id, readiness.phase, readiness.activation_attempt_state, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.run_id = readiness.run_id AND instance.instance_path = readiness.instance_path
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_path = ?`
	}
	record := runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord{RunID: runID, InstancePath: route.InstancePath}
	var attemptState sql.NullString
	var creationEventEmittedAt, instanceTerminatedAt any
	err := queryer.QueryRowContext(ctx, query, runID, route.InstancePath).Scan(
		&record.Plan, &record.PlanHash, &record.AttemptOrdinal, &record.Phase, &attemptState, &creationEventEmittedAt,
		&record.OwningRunBundleHash, &record.RunStatus,
		&record.InstanceStatus, &instanceTerminatedAt,
	)
	if err == sql.ErrNoRows {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, nil
	}
	if err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("load dynamic flow runtime readiness %s: %w", route.InstancePath, err)
	}
	record.AttemptState = attemptState.String
	if record.CreationEventEmittedAt, record.HasCreationEventEmittedAt, err = sqliteTimeValue(creationEventEmittedAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	if record.InstanceTerminatedAt, record.HasInstanceTerminatedAt, err = sqliteTimeValue(instanceTerminatedAt); err != nil {
		return runtimepipeline.DynamicFlowRuntimeReadiness{}, false, err
	}
	item, err := runtimepipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(record)
	return item, err == nil, err
}

var _ runtimepipeline.DynamicFlowRuntimeReadinessPersistence = (*PipelinePostgresOwner)(nil)
var _ runtimepipeline.DynamicFlowRuntimeReadinessPersistence = (*PipelineSQLiteOwner)(nil)
