package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

// when an existing flow instance is ensured against a revised semantic source.
func (s *workflowInstanceStore) legacyReconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	observed DynamicFlowRuntimeReadiness,
	expected DynamicFlowRuntimeReadinessPlan,
	observedAt time.Time,
) (bool, error) {
	if s == nil || s.testDB() == nil {
		return false, fmt.Errorf("workflow instance lifecycle store is required")
	}
	normalized, err := expected.Normalized()
	if err != nil {
		return false, err
	}
	observedAt = canonicalWorkflowInstancePersistedTime(observedAt)
	if observedAt.IsZero() {
		return false, fmt.Errorf("dynamic flow runtime readiness reconciliation requires an exact occurrence time")
	}

	changed := false
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		activeRunID, err := s.requireActiveWorkflowRun(txctx, tx)
		if err != nil {
			return err
		}
		if activeRunID != normalized.RunID {
			return fmt.Errorf(
				"dynamic flow runtime readiness reconciliation run identity changed: expected=%s actual=%s",
				normalized.RunID,
				activeRunID,
			)
		}
		instancePath := normalized.Identity.InstancePath
		if err := s.lockDynamicFlowRuntimeCreationEligibility(txctx, tx, normalized.RunID, instancePath); err != nil {
			return err
		}
		current, found, err := s.LoadDynamicFlowRuntimeReadiness(txctx, normalized.RunID, runtimeflowidentity.RouteForInstancePath(instancePath))
		if err != nil {
			return err
		}
		if !found || !current.Eligible() {
			return fmt.Errorf(
				"dynamic flow runtime readiness reconciliation requires one active eligible record: %s",
				instancePath,
			)
		}
		observedJSON, err := canonicaljson.Bytes(observed.Plan)
		if err != nil {
			return err
		}
		currentObservationJSON, err := canonicaljson.Bytes(current.Plan)
		if err != nil {
			return err
		}
		if string(observedJSON) != string(currentObservationJSON) ||
			!observed.OwningRunSource.Matches(current.OwningRunSource) ||
			strings.TrimSpace(observed.RunStatus) != strings.TrimSpace(current.RunStatus) ||
			strings.TrimSpace(observed.InstanceStatus) != strings.TrimSpace(current.InstanceStatus) ||
			!observed.InstanceTerminatedAt.Equal(current.InstanceTerminatedAt) ||
			!observed.TopologyReadyAt.Equal(current.TopologyReadyAt) ||
			!observed.CreationEventEmittedAt.Equal(current.CreationEventEmittedAt) {
			return &DynamicFlowRuntimeReadinessObservationConflict{
				RunID: normalized.RunID, InstancePath: instancePath, Coordinate: "test_observation",
			}
		}
		if current.Plan.Identity != normalized.Identity || current.Plan.RunID != normalized.RunID {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation identity changed for %s", instancePath)
		}
		if current.Plan.ExecutionMode != normalized.ExecutionMode {
			return fmt.Errorf("dynamic flow runtime readiness reconciliation execution mode changed for %s", instancePath)
		}
		actualJSON, err := canonicaljson.Bytes(current.Plan)
		if err != nil {
			return fmt.Errorf("encode persisted dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		expectedJSON, err := canonicaljson.Bytes(normalized)
		if err != nil {
			return fmt.Errorf("encode expected dynamic flow runtime readiness %s: %w", instancePath, err)
		}
		if string(actualJSON) == string(expectedJSON) {
			return nil
		}
		if !current.CreationEventEmittedAt.IsZero() {
			actualCreationJSON, err := canonicaljson.Bytes(current.Plan.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode emitted dynamic flow creation plan %s: %w", instancePath, err)
			}
			expectedCreationJSON, err := canonicaljson.Bytes(normalized.CreationEvent)
			if err != nil {
				return fmt.Errorf("encode revised dynamic flow creation plan %s: %w", instancePath, err)
			}
			if string(actualCreationJSON) != string(expectedCreationJSON) {
				return fmt.Errorf("dynamic flow runtime readiness cannot revise emitted creation occurrence for %s", instancePath)
			}
		}
		if s.isSQLite() {
			result, err := tx.ExecContext(txctx, `
				UPDATE flow_instance_runtime_readiness
				SET plan = ?,
				    topology_ready_at = NULL,
				    updated_at = ?
				WHERE run_id = ? AND instance_id = ?
			`, expectedJSON, observedAt, normalized.RunID, instancePath)
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
		} else {
			result, err := tx.ExecContext(txctx, `
				UPDATE flow_instance_runtime_readiness
				SET plan = $1::jsonb,
				    topology_ready_at = NULL,
				    updated_at = $2
				WHERE run_id = $3::uuid AND instance_id = $4
			`, expectedJSON, observedAt, normalized.RunID, instancePath)
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
		}
		changed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func (s *workflowInstanceStore) ReconcileDynamicFlowRuntimeReadinessPlan(
	ctx context.Context,
	observed DynamicFlowRuntimeReadiness,
	expected DynamicFlowRuntimeReadinessPlan,
	observedAt time.Time,
) (bool, error) {
	return s.legacyReconcileDynamicFlowRuntimeReadinessPlan(ctx, observed, expected, observedAt)
}

func (s *workflowInstanceStore) legacyLoadDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	route runtimeflowidentity.Route,
) (DynamicFlowRuntimeReadiness, bool, error) {
	if s == nil || s.testDB() == nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("workflow instance store is required")
	}
	runID = strings.TrimSpace(runID)
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if _, err := uuid.Parse(runID); err != nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires valid run_id: %w", err)
	}
	if !route.Valid() {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("dynamic flow runtime readiness requires an exact instance route")
	}
	instancePath := route.InstancePath
	query := `
		SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
		       run.bundle_hash, run.status,
		       instance.status, instance.terminated_at
		FROM flow_instance_runtime_readiness AS readiness
		JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
		JOIN runs AS run ON run.run_id = readiness.run_id
		WHERE readiness.run_id = $1::uuid AND readiness.instance_id = $2
	`
	if s.isSQLite() {
		query = `
			SELECT readiness.plan, readiness.topology_ready_at, readiness.creation_event_emitted_at,
			       run.bundle_hash, run.status,
			       instance.status, instance.terminated_at
			FROM flow_instance_runtime_readiness AS readiness
			JOIN flow_instances AS instance ON instance.instance_id = readiness.instance_id
			JOIN runs AS run ON run.run_id = readiness.run_id
			WHERE readiness.run_id = ? AND readiness.instance_id = ?
		`
	}
	var raw []byte
	var bundleHash, runStatus, instanceStatus string
	var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt dynamicFlowRuntimeReadinessTime
	var err error
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		err = tx.QueryRowContext(ctx, query, runID, instancePath).Scan(
			&raw, &topologyReadyAt, &creationEventEmittedAt,
			&bundleHash, &runStatus, &instanceStatus, &instanceTerminatedAt,
		)
	} else {
		err = dbQueryRowContext(ctx, s.testDB(), query, runID, instancePath).Scan(
			&raw, &topologyReadyAt, &creationEventEmittedAt,
			&bundleHash, &runStatus, &instanceStatus, &instanceTerminatedAt,
		)
	}
	if err == sql.ErrNoRows {
		return DynamicFlowRuntimeReadiness{}, false, nil
	}
	if err != nil {
		return DynamicFlowRuntimeReadiness{}, false, fmt.Errorf("load dynamic flow runtime readiness %s: %w", instancePath, err)
	}
	item, err := decodeDynamicFlowRuntimeReadiness(
		runID, instancePath, raw, runStatus, instanceStatus, instanceTerminatedAt,
		topologyReadyAt, creationEventEmittedAt,
	)
	if err == nil {
		item.OwningRunSource, err = runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	}
	return item, err == nil, err
}

func (s *workflowInstanceStore) legacyQueryAllDynamicFlowRuntimeReadiness(ctx context.Context) ([]DynamicFlowRuntimeReadiness, error) {
	if s == nil || s.testDB() == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
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
		ORDER BY readiness.run_id, readiness.instance_id
	`
	if s.isSQLite() {
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
			ORDER BY readiness.run_id, readiness.instance_id
		`
	}
	rows, err := dbQueryContext(ctx, s.testDB(), query)
	if err != nil {
		return nil, fmt.Errorf("list dynamic flow runtime readiness: %w", err)
	}
	defer rows.Close()
	var out []DynamicFlowRuntimeReadiness
	for rows.Next() {
		var runID, instancePath string
		var raw []byte
		var bundleHash, runStatus, instanceStatus string
		var topologyReadyAt, creationEventEmittedAt, instanceTerminatedAt dynamicFlowRuntimeReadinessTime
		if err := rows.Scan(
			&runID, &instancePath, &raw,
			&topologyReadyAt, &creationEventEmittedAt,
			&bundleHash, &runStatus, &instanceStatus, &instanceTerminatedAt,
		); err != nil {
			return nil, err
		}
		item, err := decodeDynamicFlowRuntimeReadiness(
			runID, instancePath, raw, runStatus, instanceStatus, instanceTerminatedAt,
			topologyReadyAt, creationEventEmittedAt,
		)
		if err != nil {
			return nil, err
		}
		item.OwningRunSource, err = runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *workflowInstanceStore) legacyMarkDynamicFlowRuntimeTopologyReady(
	ctx context.Context,
	expected DynamicFlowRuntimeReadinessPlan,
	readyAt time.Time,
) error {
	normalized, err := expected.Normalized()
	if err != nil {
		return fmt.Errorf("normalize dynamic flow runtime topology readiness plan: %w", err)
	}
	expectedJSON, err := canonicaljson.Bytes(normalized)
	if err != nil {
		return fmt.Errorf("encode dynamic flow runtime topology readiness plan: %w", err)
	}
	return s.markDynamicFlowRuntimeReadiness(
		ctx,
		normalized.RunID,
		normalized.Identity.InstancePath,
		"topology_ready_at",
		expectedJSON,
		readyAt,
	)
}

func (s *workflowInstanceStore) lockDynamicFlowRuntimeCreationEligibility(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	instancePath string,
) error {
	if tx == nil {
		return fmt.Errorf("dynamic flow runtime creation occurrence requires selected-store transaction")
	}
	activeRunID, err := s.requireActiveWorkflowRun(ctx, tx)
	if err != nil {
		return err
	}
	if activeRunID != runID {
		return fmt.Errorf("dynamic flow runtime creation occurrence run identity changed: expected=%s actual=%s", runID, activeRunID)
	}
	if s.isSQLite() {
		result, err := tx.ExecContext(ctx, `
			UPDATE flow_instances
			SET status = status
			WHERE instance_id = ?
		`, instancePath)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("dynamic flow runtime creation occurrence lifecycle row not found: %s", instancePath)
		}
		return nil
	}
	var lockedInstancePath string
	if err := tx.QueryRowContext(ctx, `
		SELECT instance_id
		FROM flow_instances
		WHERE instance_id = $1
		FOR UPDATE
	`, instancePath).Scan(&lockedInstancePath); err != nil {
		return fmt.Errorf("lock dynamic flow runtime creation instance: %w", err)
	}
	if strings.TrimSpace(lockedInstancePath) != instancePath {
		return fmt.Errorf("dynamic flow runtime creation occurrence instance identity changed")
	}
	return nil
}

func (s *workflowInstanceStore) markDynamicFlowRuntimeReadiness(
	ctx context.Context,
	runID string,
	instancePath string,
	column string,
	expectedPlan []byte,
	observedAt time.Time,
) error {
	if s == nil || s.testDB() == nil {
		return fmt.Errorf("workflow instance store is required")
	}
	runID = strings.TrimSpace(runID)
	instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
	observedAt = observedAt.UTC()
	if _, err := uuid.Parse(runID); err != nil {
		return fmt.Errorf("dynamic flow runtime readiness transition requires valid run_id: %w", err)
	}
	if instancePath == "" || observedAt.IsZero() {
		return fmt.Errorf("dynamic flow runtime readiness transition requires exact instance and time")
	}
	if column != "topology_ready_at" && column != "creation_event_emitted_at" {
		return fmt.Errorf("unsupported dynamic flow runtime readiness transition")
	}
	return s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var result sql.Result
		var err error
		if s.isSQLite() {
			query := `UPDATE flow_instance_runtime_readiness SET ` + column + ` = COALESCE(` + column + `, ?), updated_at = ? WHERE run_id = ? AND instance_id = ?`
			args := []any{observedAt, observedAt, runID, instancePath}
			if len(expectedPlan) != 0 {
				query += ` AND plan = ?`
				args = append(args, expectedPlan)
			}
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			query += `
				AND EXISTS (
					SELECT 1
					FROM flow_instances AS instance
					JOIN runs AS run ON run.run_id = flow_instance_runtime_readiness.run_id
					WHERE instance.instance_id = flow_instance_runtime_readiness.instance_id
					  AND LOWER(TRIM(instance.status)) = 'active'
					  AND instance.terminated_at IS NULL
					  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
				)`
			result, err = tx.ExecContext(txctx, query, args...)
		} else {
			query := `UPDATE flow_instance_runtime_readiness SET ` + column + ` = COALESCE(` + column + `, $1), updated_at = $1 WHERE run_id = $2::uuid AND instance_id = $3`
			args := []any{observedAt, runID, instancePath}
			if len(expectedPlan) != 0 {
				query += ` AND plan = $4::jsonb`
				args = append(args, expectedPlan)
			}
			if column == "creation_event_emitted_at" {
				query += ` AND topology_ready_at IS NOT NULL`
			}
			query += `
				AND EXISTS (
					SELECT 1
					FROM flow_instances AS instance
					JOIN runs AS run ON run.run_id = flow_instance_runtime_readiness.run_id
					WHERE instance.instance_id = flow_instance_runtime_readiness.instance_id
					  AND LOWER(BTRIM(instance.status)) = 'active'
					  AND instance.terminated_at IS NULL
					  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
				)`
			result, err = tx.ExecContext(txctx, query, args...)
		}
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected != 1 {
			return fmt.Errorf("dynamic flow runtime readiness %s transition %s requires one active record", instancePath, column)
		}
		return nil
	})
}
