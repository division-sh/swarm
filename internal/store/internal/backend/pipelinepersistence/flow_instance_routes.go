package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type flowInstanceDescriptorQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type flowInstanceRouteExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type flowInstanceRouteDatabase interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func runPostgresFlowInstanceRouteMutation(ctx context.Context, db flowInstanceRouteDatabase, runID string, fn func(flowInstanceRouteExecutor) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requirePostgresRunActive(ctx, tx, runID); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PipelinePostgresOwner) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	var err error
	route, err = normalizeFlowInstanceRouteRecord(route)
	if err != nil {
		return err
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.backend, route.Identity.RunID, func(exec flowInstanceRouteExecutor) error {
		return upsertPostgresFlowInstanceRoute(ctx, exec, route)
	})
}

func normalizeFlowInstanceRouteRecord(route runtimebus.FlowInstanceRouteRecord) (runtimebus.FlowInstanceRouteRecord, error) {
	route.Identity = route.Identity.Normalize()
	if err := route.Identity.Validate(); err != nil {
		return runtimebus.FlowInstanceRouteRecord{}, fmt.Errorf("run_id, scope_key, instance_id, and instance_path are required")
	}
	route.EventPattern = strings.TrimSpace(route.EventPattern)
	route.SubscriberType = strings.TrimSpace(route.SubscriberType)
	route.SubscriberID = strings.TrimSpace(route.SubscriberID)
	route.SourceFlow = strings.TrimSpace(route.SourceFlow)
	if route.SourceFlow == "" {
		route.SourceFlow = route.Identity.Route.ScopeKey
	}
	if route.EventPattern == "" || route.SubscriberType == "" || route.SubscriberID == "" {
		return runtimebus.FlowInstanceRouteRecord{}, fmt.Errorf("flow-instance route record requires event pattern and subscriber identity")
	}
	return route, nil
}

func upsertPostgresFlowInstanceRoute(
	ctx context.Context,
	exec flowInstanceRouteExecutor,
	route runtimebus.FlowInstanceRouteRecord,
) error {
	var materializedFrom any
	if route.EventPattern != "" && route.SubscriberType != "" && route.SubscriberID != "" {
		_ = exec.QueryRowContext(ctx, `
				SELECT rule_id
			FROM routing_rules
			WHERE event_pattern = $1
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			  AND COALESCE(source_flow, '') = $4
			  AND is_wildcard = true
				  AND is_materialized = false
				  AND status = 'active'
				ORDER BY created_at ASC
				LIMIT 1
			`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.SourceFlow).Scan(&materializedFrom)
	}
	_, err := exec.ExecContext(ctx, `
		WITH updated AS (
			UPDATE routing_rules
			SET source_flow = NULLIF($6,''),
			    materialized_from = $7,
			    status = 'active'
			WHERE event_pattern = $1
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			  AND run_id = $4::uuid
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($5,'')
			  AND is_materialized = true
			RETURNING rule_id
		)
		INSERT INTO routing_rules (
			event_pattern,
			subscriber_type,
			subscriber_id,
			run_id,
			flow_instance,
			source_flow,
			is_wildcard,
			is_materialized,
			materialized_from,
			status,
			created_at
		)
			SELECT
				$1,
				$2,
				$3,
				$4::uuid,
				NULLIF($5,''),
			NULLIF($6,''),
			false,
			true,
			$7,
				'active',
				now()
			WHERE NOT EXISTS (SELECT 1 FROM updated)
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath, route.SourceFlow, materializedFrom)
	if err != nil {
		return fmt.Errorf("upsert flow instance route %s/%s: %w", route.Identity.Route.ScopeKey, route.Identity.Route.InstanceID, err)
	}
	return nil
}

func (s *PipelineSQLiteOwner) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	var err error
	route, err = normalizeFlowInstanceRouteRecord(route)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route upsert", newRevisionEffects(), func(txctx context.Context, tx *sql.Tx) error {
		if err := requireSQLiteRunActive(txctx, tx, route.Identity.RunID); err != nil {
			return err
		}
		return upsertSQLiteFlowInstanceRoute(txctx, tx, route)
	})
}

func upsertSQLiteFlowInstanceRoute(
	ctx context.Context,
	tx *sql.Tx,
	route runtimebus.FlowInstanceRouteRecord,
) error {
	var materializedFrom sql.NullInt64
	if route.EventPattern != "" && route.SubscriberType != "" && route.SubscriberID != "" {
		_ = tx.QueryRowContext(ctx, `
				SELECT rule_id
				FROM routing_rules
				WHERE event_pattern = ?
				  AND subscriber_type = ?
				  AND subscriber_id = ?
				  AND COALESCE(source_flow, '') = ?
				  AND is_wildcard = TRUE
				  AND is_materialized = FALSE
				  AND status = 'active'
				ORDER BY created_at ASC
				LIMIT 1
			`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.SourceFlow).Scan(&materializedFrom)
	}
	result, err := tx.ExecContext(ctx, `
			UPDATE routing_rules
			SET source_flow = NULLIF(?, ''), materialized_from = ?, status = 'active'
			WHERE event_pattern = ?
			  AND subscriber_type = ?
			  AND subscriber_id = ?
			  AND run_id = ?
			  AND COALESCE(flow_instance, '') = ?
			  AND is_materialized = TRUE
		`, route.SourceFlow, nullableSQLiteInt64(materializedFrom), route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath)
	if err != nil {
		return fmt.Errorf("update sqlite flow instance route %s/%s: %w", route.Identity.Route.ScopeKey, route.Identity.Route.InstanceID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect sqlite flow instance route update: %w", err)
	}
	if updated > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO routing_rules (
				event_pattern, subscriber_type, subscriber_id, run_id, flow_instance, source_flow,
				is_wildcard, is_materialized, materialized_from, status, created_at
			) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), FALSE, TRUE, ?, 'active', ?)
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath, route.SourceFlow,
		nullableSQLiteInt64(materializedFrom), time.Now().UTC()); err != nil {
		return fmt.Errorf("insert sqlite flow instance route %s/%s: %w", route.Identity.Route.ScopeKey, route.Identity.Route.InstanceID, err)
	}
	return nil
}

func nullableSQLiteInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func (s *PipelinePostgresOwner) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.RunScopedFlowInstance,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required for exact flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("exact flow-instance route owner is required")
	}
	normalized, err := normalizeFlowInstanceRouteSet(identity, routes)
	if err != nil {
		return err
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.backend, identity.RunID, func(exec flowInstanceRouteExecutor) error {
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE run_id = $1::uuid AND flow_instance = $2
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("inactivate postgres flow-instance route owner %s: %w", identity.Key(), err)
		}
		for _, route := range normalized {
			if err := upsertPostgresFlowInstanceRoute(ctx, exec, route); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PipelineSQLiteOwner) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.RunScopedFlowInstance,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required for exact flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("exact flow-instance route owner is required")
	}
	normalized, err := normalizeFlowInstanceRouteSet(identity, routes)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite exact flow instance route replacement", newRevisionEffects(), func(txctx context.Context, tx *sql.Tx) error {
		if err := requireSQLiteRunActive(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE run_id = ? AND flow_instance = ?
			  AND is_materialized = TRUE
			  AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("inactivate sqlite flow-instance route owner %s: %w", identity.Key(), err)
		}
		for _, route := range normalized {
			if err := upsertSQLiteFlowInstanceRoute(txctx, tx, route); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PipelinePostgresOwner) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required for flow-instance route topology")
	}
	return s.runPrivateAuthorActivityMutation(ctx, newRevisionEffects(), func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		_, err := replaceFlowInstanceRouteTopologyTx(txctx, tx, true, sets)
		return err
	})
}

func (s *PipelineSQLiteOwner) ReplaceFlowInstanceRouteTopology(
	ctx context.Context,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required for flow-instance route topology")
	}
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite flow-instance route topology replacement", newRevisionEffects(), func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		_, err := replaceFlowInstanceRouteTopologyTx(txctx, tx, false, sets)
		return err
	})
}

func replaceFlowInstanceRouteTopologyTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	sets []runtimebus.FlowInstanceRouteRecordSet,
) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	if tx == nil {
		return nil, fmt.Errorf("flow-instance route topology replacement requires private transaction ownership")
	}
	normalized, err := normalizeFlowInstanceRouteTopology(sets)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, nil
	}
	for _, set := range normalized {
		if postgres {
			if err := requirePostgresRunActive(ctx, tx, set.Identity.RunID); err != nil {
				return nil, err
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE routing_rules
				SET status = 'inactive'
				WHERE run_id = $1::uuid AND flow_instance = $2
				  AND is_materialized = true
				  AND status = 'active'
			`, set.Identity.RunID, set.Identity.Route.InstancePath); err != nil {
				return nil, fmt.Errorf("inactivate postgres flow-instance route owner %s: %w", set.Identity.Key(), err)
			}
			for _, route := range set.Routes {
				if err := upsertPostgresFlowInstanceRoute(ctx, tx, route); err != nil {
					return nil, err
				}
			}
			continue
		}
		if err := requireSQLiteRunActive(ctx, tx, set.Identity.RunID); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE run_id = ? AND flow_instance = ?
			  AND is_materialized = TRUE
			  AND status = 'active'
		`, set.Identity.RunID, set.Identity.Route.InstancePath); err != nil {
			return nil, fmt.Errorf("inactivate sqlite flow-instance route owner %s: %w", set.Identity.Key(), err)
		}
		for _, route := range set.Routes {
			if err := upsertSQLiteFlowInstanceRoute(ctx, tx, route); err != nil {
				return nil, err
			}
		}
	}
	return normalized, nil
}

func (s *PipelinePostgresOwner) ReplaceFlowInstanceRouteTopologyTx(ctx context.Context, tx *sql.Tx, sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	return replaceFlowInstanceRouteTopologyTx(ctx, tx, true, sets)
}

func (s *PipelineSQLiteOwner) ReplaceFlowInstanceRouteTopologyTx(ctx context.Context, tx *sql.Tx, sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	return replaceFlowInstanceRouteTopologyTx(ctx, tx, false, sets)
}

func normalizeFlowInstanceRouteTopology(sets []runtimebus.FlowInstanceRouteRecordSet) ([]runtimebus.FlowInstanceRouteRecordSet, error) {
	normalized := make([]runtimebus.FlowInstanceRouteRecordSet, 0, len(sets))
	seen := make(map[runtimeflowidentity.RunScopedFlowInstance]struct{}, len(sets))
	for _, set := range sets {
		identity := set.Identity.Normalize()
		if err := identity.Validate(); err != nil {
			return nil, fmt.Errorf("flow-instance route topology requires an exact owner")
		}
		if _, exists := seen[identity]; exists {
			return nil, fmt.Errorf("flow-instance route topology repeats owner %s", identity.Key())
		}
		seen[identity] = struct{}{}
		routes, err := normalizeFlowInstanceRouteSet(identity, set.Routes)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, runtimebus.FlowInstanceRouteRecordSet{Identity: identity, Routes: routes})
	}
	return normalized, nil
}

func normalizeFlowInstanceRouteSet(
	identity runtimeflowidentity.RunScopedFlowInstance,
	routes []runtimebus.FlowInstanceRouteRecord,
) ([]runtimebus.FlowInstanceRouteRecord, error) {
	normalized := make([]runtimebus.FlowInstanceRouteRecord, 0, len(routes))
	seen := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		var err error
		route, err = normalizeFlowInstanceRouteRecord(route)
		if err != nil {
			return nil, err
		}
		if route.Identity != identity {
			return nil, fmt.Errorf(
				"flow-instance route record owner %s does not match exact replacement owner %s",
				route.Identity.Route.InstancePath,
				identity.Route.InstancePath,
			)
		}
		key := strings.Join([]string{
			route.EventPattern,
			route.SubscriberType,
			route.SubscriberID,
			route.SourceFlow,
		}, "\x00")
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate flow-instance route record for owner %s", identity.Key())
		}
		seen[key] = struct{}{}
		normalized = append(normalized, route)
	}
	return normalized, nil
}

func (s *PipelinePostgresOwner) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.backend, identity.RunID, func(exec flowInstanceRouteExecutor) error {
		var status string
		err := exec.QueryRowContext(ctx, `
		SELECT status
		FROM flow_instances
		WHERE run_id = $1::uuid AND instance_path = $2
	`, identity.RunID, identity.Route.InstancePath).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("flow instance not found for route removal: %s", identity.Key())
			}
			return fmt.Errorf("load flow instance for route removal %s: %w", identity.Key(), err)
		}
		if strings.TrimSpace(status) != "terminated" {
			return fmt.Errorf("flow instance route removal requires terminal flow_instances status for %s", identity.Key())
		}
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE run_id = $1::uuid AND flow_instance = $2
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("delete flow instance route %s: %w", identity.Key(), err)
		}
		return nil
	})
}

func (s *PipelineSQLiteOwner) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route delete", newRevisionEffects(), func(txctx context.Context, tx *sql.Tx) error {
		if err := requireSQLiteRunActive(txctx, tx, identity.RunID); err != nil {
			return err
		}
		var status string
		err := tx.QueryRowContext(txctx, `SELECT status FROM flow_instances WHERE run_id = ? AND instance_path = ?`, identity.RunID, identity.Route.InstancePath).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("flow instance not found for route removal: %s", identity.Key())
			}
			return fmt.Errorf("load sqlite flow instance for route removal %s: %w", identity.Key(), err)
		}
		if strings.TrimSpace(status) != "terminated" {
			return fmt.Errorf("flow instance route removal requires terminal flow_instances status for %s", identity.Key())
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules SET status = 'inactive'
			WHERE run_id = ? AND flow_instance = ? AND is_materialized = TRUE AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("delete sqlite flow instance route %s: %w", identity.Key(), err)
		}
		return nil
	})
}

func (s *PipelinePostgresOwner) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.backend, identity.RunID, func(exec flowInstanceRouteExecutor) error {
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE run_id = $1::uuid AND flow_instance = $2
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("rollback flow instance route %s: %w", identity.Key(), err)
		}
		return nil
	})
}

func (s *PipelineSQLiteOwner) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route rollback", newRevisionEffects(), func(txctx context.Context, tx *sql.Tx) error {
		if err := requireSQLiteRunActive(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules SET status = 'inactive'
			WHERE run_id = ? AND flow_instance = ? AND is_materialized = TRUE AND status = 'active'
		`, identity.RunID, identity.Route.InstancePath); err != nil {
			return fmt.Errorf("rollback sqlite flow instance route %s: %w", identity.Key(), err)
		}
		return nil
	})
}

func (s *PipelinePostgresOwner) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.RunScopedFlowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	rows, err := q.QueryContext(ctx, `
			SELECT
				routing_rules.run_id::text,
				COALESCE(NULLIF(source_flow, ''), ''),
				flow_instance
			FROM routing_rules
			JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
			WHERE is_materialized = true
			  AND routing_rules.status = 'active'
			  AND fi.status = 'active'
			  AND flow_instance IS NOT NULL
			GROUP BY routing_rules.run_id, flow_instance, source_flow
			ORDER BY routing_rules.run_id, flow_instance ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list flow instance routes: %w", err)
	}
	defer rows.Close()

	out := []runtimeflowidentity.RunScopedFlowInstance{}
	for rows.Next() {
		var runID, sourceFlow, instancePath string
		if err := rows.Scan(&runID, &sourceFlow, &instancePath); err != nil {
			return nil, fmt.Errorf("scan flow instance route: %w", err)
		}
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.StoredRoute(sourceFlow, "", instancePath))
		if err != nil {
			continue
		}
		out = append(out, identity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate flow instance routes: %w", err)
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.RunScopedFlowInstance, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	rows, err := q.QueryContext(ctx, `
		SELECT routing_rules.run_id, COALESCE(NULLIF(source_flow, ''), ''), flow_instance
		FROM routing_rules
		JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
		WHERE is_materialized = TRUE
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		  AND flow_instance IS NOT NULL
		GROUP BY routing_rules.run_id, flow_instance, source_flow
		ORDER BY routing_rules.run_id, flow_instance ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list sqlite flow instance routes: %w", err)
	}
	defer rows.Close()
	out := []runtimeflowidentity.RunScopedFlowInstance{}
	for rows.Next() {
		var runID, sourceFlow, instancePath string
		if err := rows.Scan(&runID, &sourceFlow, &instancePath); err != nil {
			return nil, fmt.Errorf("scan sqlite flow instance route: %w", err)
		}
		identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.StoredRoute(sourceFlow, "", instancePath))
		if err == nil {
			out = append(out, identity)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite flow instance routes: %w", err)
	}
	return out, nil
}

func (s *PipelinePostgresOwner) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
		WHERE routing_rules.run_id = $1::uuid AND routing_rules.flow_instance = $2
		  AND routing_rules.is_materialized = true
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func (s *PipelineSQLiteOwner) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.backend)
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.run_id = routing_rules.run_id AND fi.instance_path = routing_rules.flow_instance
		WHERE routing_rules.run_id = ? AND routing_rules.flow_instance = ?
		  AND routing_rules.is_materialized = TRUE
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func listFlowInstanceRouteRecords(
	ctx context.Context,
	q flowInstanceDescriptorQueryer,
	identity runtimeflowidentity.RunScopedFlowInstance,
	query string,
) ([]runtimebus.FlowInstanceRouteRecord, error) {
	rows, err := q.QueryContext(ctx, query, identity.RunID, identity.Route.InstancePath)
	if err != nil {
		return nil, fmt.Errorf("list exact flow instance route records %s: %w", identity.Key(), err)
	}
	defer rows.Close()
	var out []runtimebus.FlowInstanceRouteRecord
	for rows.Next() {
		var record runtimebus.FlowInstanceRouteRecord
		record.Identity = identity
		if err := rows.Scan(&record.EventPattern, &record.SubscriberType, &record.SubscriberID, &record.SourceFlow); err != nil {
			return nil, fmt.Errorf("scan exact flow instance route record %s: %w", identity.Key(), err)
		}
		if strings.TrimSpace(record.SourceFlow) == "" {
			record.SourceFlow = identity.Route.ScopeKey
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exact flow instance route records %s: %w", identity.Key(), err)
	}
	return out, nil
}

func (s *PipelinePostgresOwner) ListActiveFlowInstanceDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT fi.run_id::text, fi.instance_path, fi.flow_template, readiness.plan,
		       run.bundle_hash, run.bundle_source, es.fields
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.run_id = fi.run_id AND readiness.instance_path = fi.instance_path
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es
		  ON es.run_id = fi.run_id
		 AND es.flow_instance = fi.instance_path
		 AND es.entity_id = NULLIF(readiness.plan #>> '{identity,EntityID}', '')::uuid
		WHERE fi.run_id = $1::uuid
		  AND fi.status = 'active' AND fi.mode = 'template'
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state owned
			  WHERE owned.run_id = fi.run_id
			    AND owned.flow_instance = fi.instance_path
		  )
		ORDER BY fi.run_id, fi.instance_path ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "active flow instance descriptor")
}

func (s *PipelineSQLiteOwner) ListActiveFlowInstanceDescriptors(ctx context.Context, runID string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for active flow instance descriptors")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("active flow instance descriptors require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT fi.run_id, fi.instance_path, fi.flow_template, readiness.plan,
		       run.bundle_hash, run.bundle_source, es.fields
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.run_id = fi.run_id AND readiness.instance_path = fi.instance_path
		JOIN runs run ON run.run_id = fi.run_id
		LEFT JOIN entity_state es
		  ON es.run_id = fi.run_id
		 AND es.flow_instance = fi.instance_path
		 AND es.entity_id = json_extract(readiness.plan, '$.identity.EntityID')
		WHERE fi.run_id = ?
		  AND fi.status = 'active' AND fi.mode = 'template'
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state owned
			  WHERE owned.run_id = fi.run_id
			    AND owned.flow_instance = fi.instance_path
		  )
		ORDER BY fi.run_id, fi.instance_path ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite active flow instance descriptors: %w", err)
	}
	return scanExactActiveFlowInstanceDescriptors(rows, "sqlite active flow instance descriptor")
}

func (s *PipelinePostgresOwner) ListSelectedRunTargetOwners(ctx context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT es.entity_id::text, es.flow_instance
		FROM entity_state es
		JOIN runs run ON run.run_id = es.run_id
		WHERE es.run_id = $1::uuid
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY es.flow_instance ASC, es.entity_id ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "selected-run target owner")
}

func (s *PipelineSQLiteOwner) ListSelectedRunTargetOwners(ctx context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for selected-run target owners")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("selected-run target owners require exact run_id")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT es.entity_id, es.flow_instance
		FROM entity_state es
		JOIN runs run ON run.run_id = es.run_id
		WHERE es.run_id = ?
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		ORDER BY es.flow_instance ASC, es.entity_id ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite selected-run target owners: %w", err)
	}
	return scanSelectedRunTargetOwners(rows, "sqlite selected-run target owner")
}

func scanSelectedRunTargetOwners(rows *sql.Rows, label string) ([]runtimebus.ActiveTargetDescriptor, error) {
	defer rows.Close()
	out := []runtimebus.ActiveTargetDescriptor{}
	for rows.Next() {
		var entityID, flowInstance string
		if err := rows.Scan(&entityID, &flowInstance); err != nil {
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		descriptor := (runtimebus.ActiveTargetDescriptor{
			ID: flowInstance, EntityID: entityID, FlowInstance: flowInstance,
		}).Normalized()
		if descriptor.EntityID == "" || descriptor.FlowInstance == "" {
			return nil, fmt.Errorf("%s is missing exact entity and flow-instance identity", label)
		}
		out = append(out, descriptor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	return out, nil
}

func scanExactActiveFlowInstanceDescriptors(rows *sql.Rows, label string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	defer rows.Close()
	out := []runtimebus.ActiveFlowInstanceDescriptor{}
	for rows.Next() {
		var runID, instancePath, templateID string
		var planRaw, bundleHash, bundleSource, fieldsRaw sql.NullString
		if err := rows.Scan(&runID, &instancePath, &templateID, &planRaw, &bundleHash, &bundleSource, &fieldsRaw); err != nil {
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		instancePath = strings.Trim(strings.TrimSpace(instancePath), "/")
		templateID = strings.TrimSpace(templateID)
		if instancePath == "" || templateID == "" {
			return nil, fmt.Errorf("%s is missing exact instance identity", label)
		}
		if !planRaw.Valid || strings.TrimSpace(planRaw.String) == "" {
			return nil, fmt.Errorf("%s %s is missing exact readiness plan", label, instancePath)
		}
		if !bundleHash.Valid || !bundleSource.Valid {
			return nil, fmt.Errorf("%s %s is missing exact run bundle source", label, instancePath)
		}
		readiness, err := runtimepipeline.DecodeDynamicFlowRuntimeReadinessPersistenceRecord(
			runtimepipeline.DynamicFlowRuntimeReadinessPersistenceRecord{
				RunID: runID, InstancePath: instancePath, Plan: []byte(planRaw.String),
				OwningRunBundleHash: bundleHash.String, OwningRunBundleSource: bundleSource.String,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("validate %s %s readiness plan: %w", label, instancePath, err)
		}
		plan := readiness.Plan
		if plan.RunID != runID || plan.Identity.InstancePath != instancePath || plan.Identity.TemplateID != templateID {
			return nil, fmt.Errorf("%s %s readiness identity does not match persisted owner", label, instancePath)
		}
		if !fieldsRaw.Valid {
			return nil, fmt.Errorf("%s %s is missing exact entity state", label, instancePath)
		}
		persistedSource, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash.String, bundleSource.String)
		if err != nil {
			return nil, fmt.Errorf("%s %s run bundle source: %w", label, instancePath, err)
		}
		planSource, err := runtimecorrelation.DecodeBundleSourceFact(plan.BundleHash, plan.BundleSource)
		if err != nil || planSource != persistedSource {
			return nil, fmt.Errorf("%s %s readiness source does not match persisted run", label, instancePath)
		}
		addressFields, err := exactDescriptorAddressFields(fieldsRaw.String)
		if err != nil {
			return nil, fmt.Errorf("%s %s entity fields: %w", label, instancePath, err)
		}
		out = append(out, runtimebus.ActiveFlowInstanceDescriptor{
			RunID:      runID,
			InstanceID: plan.Identity.InstanceID, EntityID: plan.Identity.EntityID,
			FlowInstance: instancePath, FlowTemplate: templateID,
			BundleHash: plan.BundleHash, BundleSource: plan.BundleSource,
			WorkflowVersion: plan.WorkflowVersion, AddressFields: addressFields,
		}.Normalized())
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	return out, nil
}

func exactDescriptorAddressFields(raw any) (map[string]string, error) {
	values, err := decodeDescriptorJSONMap(raw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("entity field name is empty")
		}
		scalar, ok := descriptorScalarString(value)
		if ok {
			out["entity."+key] = scalar
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func decodeDescriptorJSONMap(raw any) (map[string]any, error) {
	data := jsonRawMessageValue(raw)
	if len(data) == 0 || strings.TrimSpace(string(data)) == "" || strings.TrimSpace(string(data)) == "null" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func descriptorScalarString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed), true
	case bool:
		if typed {
			return "true", true
		}
		return "false", true
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64), true
	case json.Number:
		return typed.String(), true
	default:
		return "", false
	}
}
