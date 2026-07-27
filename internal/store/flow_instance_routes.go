package store

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
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

type flowInstanceDescriptorQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type flowInstanceRouteExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func runPostgresFlowInstanceRouteMutation(ctx context.Context, db *sql.DB, fn func(flowInstanceRouteExecutor) error) error {
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return err
	}
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		if err := storerunlifecycle.RequireActive(ctx, tx, runID, storerunlifecycle.DialectPostgres); err != nil {
			return err
		}
		return fn(tx)
	}
	var tx *sql.Tx
	if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		tx, err = conn.BeginTx(ctx, nil)
	} else {
		tx, err = db.BeginTx(ctx, nil)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := storerunlifecycle.RequireActive(ctx, tx, runID, storerunlifecycle.DialectPostgres); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	var err error
	route, err = normalizeFlowInstanceRouteRecord(route)
	if err != nil {
		return err
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.DB, func(exec flowInstanceRouteExecutor) error {
		return upsertPostgresFlowInstanceRoute(ctx, exec, route)
	})
}

func normalizeFlowInstanceRouteRecord(route runtimebus.FlowInstanceRouteRecord) (runtimebus.FlowInstanceRouteRecord, error) {
	route.Identity = runtimeflowidentity.StoredRoute(route.Identity.ScopeKey, route.Identity.InstanceID, route.Identity.InstancePath)
	if !route.Identity.Valid() {
		return runtimebus.FlowInstanceRouteRecord{}, fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	route.EventPattern = strings.TrimSpace(route.EventPattern)
	route.SubscriberType = strings.TrimSpace(route.SubscriberType)
	route.SubscriberID = strings.TrimSpace(route.SubscriberID)
	route.SourceFlow = strings.TrimSpace(route.SourceFlow)
	if route.SourceFlow == "" {
		route.SourceFlow = route.Identity.ScopeKey
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
			SET source_flow = NULLIF($5,''),
			    materialized_from = $6,
			    status = 'active'
			WHERE event_pattern = $1
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			  AND flow_instance IS NOT DISTINCT FROM NULLIF($4,'')
			  AND is_materialized = true
			RETURNING rule_id
		)
		INSERT INTO routing_rules (
			event_pattern,
			subscriber_type,
			subscriber_id,
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
				NULLIF($4,''),
			NULLIF($5,''),
			false,
			true,
			$6,
				'active',
				now()
			WHERE NOT EXISTS (SELECT 1 FROM updated)
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath, route.SourceFlow, materializedFrom)
	if err != nil {
		return fmt.Errorf("upsert flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	var err error
	route, err = normalizeFlowInstanceRouteRecord(route)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route upsert", func(txctx context.Context, tx *sql.Tx) error {
		runID, err := runtimecurrentstate.RequireRunID(txctx)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, runID, storerunlifecycle.DialectSQLite); err != nil {
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
			  AND COALESCE(flow_instance, '') = ?
			  AND is_materialized = TRUE
		`, route.SourceFlow, nullableSQLiteInt64(materializedFrom), route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath)
	if err != nil {
		return fmt.Errorf("update sqlite flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
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
				event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow,
				is_wildcard, is_materialized, materialized_from, status, created_at
			) VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), FALSE, TRUE, ?, 'active', ?)
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath, route.SourceFlow,
		nullableSQLiteInt64(materializedFrom), time.Now().UTC()); err != nil {
		return fmt.Errorf("insert sqlite flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
	}
	return nil
}

func nullableSQLiteInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func (s *PostgresStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for exact flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("exact flow-instance route owner is required")
	}
	normalized, err := normalizeFlowInstanceRouteSet(identity, routes)
	if err != nil {
		return err
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.DB, func(exec flowInstanceRouteExecutor) error {
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE flow_instance = $1
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("inactivate postgres flow-instance route owner %s: %w", identity.InstancePath, err)
		}
		for _, route := range normalized {
			if err := upsertPostgresFlowInstanceRoute(ctx, exec, route); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) ReplaceFlowInstanceRouteRecords(
	ctx context.Context,
	identity runtimeflowidentity.Route,
	routes []runtimebus.FlowInstanceRouteRecord,
) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for exact flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("exact flow-instance route owner is required")
	}
	normalized, err := normalizeFlowInstanceRouteSet(identity, routes)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite exact flow instance route replacement", func(txctx context.Context, tx *sql.Tx) error {
		runID, err := runtimecurrentstate.RequireRunID(txctx)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, runID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE flow_instance = ?
			  AND is_materialized = TRUE
			  AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("inactivate sqlite flow-instance route owner %s: %w", identity.InstancePath, err)
		}
		for _, route := range normalized {
			if err := upsertSQLiteFlowInstanceRoute(txctx, tx, route); err != nil {
				return err
			}
		}
		return nil
	})
}

func normalizeFlowInstanceRouteSet(
	identity runtimeflowidentity.Route,
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
				route.Identity.InstancePath,
				identity.InstancePath,
			)
		}
		key := strings.Join([]string{
			route.EventPattern,
			route.SubscriberType,
			route.SubscriberID,
			route.SourceFlow,
		}, "\x00")
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate flow-instance route record for owner %s", identity.InstancePath)
		}
		seen[key] = struct{}{}
		normalized = append(normalized, route)
	}
	return normalized, nil
}

func (s *PostgresStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.DB, func(exec flowInstanceRouteExecutor) error {
		var status string
		err := exec.QueryRowContext(ctx, `
		SELECT status
		FROM flow_instances
		WHERE instance_id = $1
	`, identity.InstancePath).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("flow instance not found for route removal: %s", identity.InstancePath)
			}
			return fmt.Errorf("load flow instance for route removal %s: %w", identity.InstancePath, err)
		}
		if strings.TrimSpace(status) != "terminated" {
			return fmt.Errorf("flow instance route removal requires terminal flow_instances status for %s", identity.InstancePath)
		}
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE flow_instance = $1
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("delete flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route delete", func(txctx context.Context, tx *sql.Tx) error {
		runID, err := runtimecurrentstate.RequireRunID(txctx)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, runID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		var status string
		err = tx.QueryRowContext(txctx, `SELECT status FROM flow_instances WHERE instance_id = ?`, identity.InstancePath).Scan(&status)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("flow instance not found for route removal: %s", identity.InstancePath)
			}
			return fmt.Errorf("load sqlite flow instance for route removal %s: %w", identity.InstancePath, err)
		}
		if strings.TrimSpace(status) != "terminated" {
			return fmt.Errorf("flow instance route removal requires terminal flow_instances status for %s", identity.InstancePath)
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules SET status = 'inactive'
			WHERE flow_instance = ? AND is_materialized = TRUE AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("delete sqlite flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
		}
		return nil
	})
}

func (s *PostgresStore) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return runPostgresFlowInstanceRouteMutation(ctx, s.DB, func(exec flowInstanceRouteExecutor) error {
		if _, err := exec.ExecContext(ctx, `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE flow_instance = $1
			  AND is_materialized = true
			  AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("rollback flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	return s.runRuntimeMutation(ctx, "sqlite flow instance route rollback", func(txctx context.Context, tx *sql.Tx) error {
		runID, err := runtimecurrentstate.RequireRunID(txctx)
		if err != nil {
			return err
		}
		if err := storerunlifecycle.RequireActive(txctx, tx, runID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE routing_rules SET status = 'inactive'
			WHERE flow_instance = ? AND is_materialized = TRUE AND status = 'active'
		`, identity.InstancePath); err != nil {
			return fmt.Errorf("rollback sqlite flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
		}
		return nil
	})
}

func (s *PostgresStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		q = conn
	}
	rows, err := q.QueryContext(ctx, `
			SELECT
				COALESCE(NULLIF(source_flow, ''), ''),
				flow_instance
			FROM routing_rules
			JOIN flow_instances fi ON fi.instance_id = routing_rules.flow_instance
			WHERE is_materialized = true
			  AND routing_rules.status = 'active'
			  AND fi.status = 'active'
			  AND flow_instance IS NOT NULL
			GROUP BY flow_instance, source_flow
			ORDER BY flow_instance ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list flow instance routes: %w", err)
	}
	defer rows.Close()

	out := []runtimeflowidentity.Route{}
	for rows.Next() {
		var sourceFlow, instancePath string
		if err := rows.Scan(&sourceFlow, &instancePath); err != nil {
			return nil, fmt.Errorf("scan flow instance route: %w", err)
		}
		route := runtimeflowidentity.StoredRoute(sourceFlow, "", instancePath)
		if !route.Valid() {
			continue
		}
		out = append(out, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate flow instance routes: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	}
	rows, err := q.QueryContext(ctx, `
		SELECT COALESCE(NULLIF(source_flow, ''), ''), flow_instance
		FROM routing_rules
		JOIN flow_instances fi ON fi.instance_id = routing_rules.flow_instance
		WHERE is_materialized = TRUE
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		  AND flow_instance IS NOT NULL
		GROUP BY flow_instance, source_flow
		ORDER BY flow_instance ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list sqlite flow instance routes: %w", err)
	}
	defer rows.Close()
	out := []runtimeflowidentity.Route{}
	for rows.Next() {
		var sourceFlow, instancePath string
		if err := rows.Scan(&sourceFlow, &instancePath); err != nil {
			return nil, fmt.Errorf("scan sqlite flow instance route: %w", err)
		}
		route := runtimeflowidentity.StoredRoute(sourceFlow, "", instancePath)
		if route.Valid() {
			out = append(out, route)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite flow instance routes: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		q = conn
	}
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.instance_id = routing_rules.flow_instance
		WHERE routing_rules.flow_instance = $1
		  AND routing_rules.is_materialized = true
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func (s *SQLiteRuntimeStore) ListFlowInstanceRouteRecords(ctx context.Context, identity runtimeflowidentity.Route) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return nil, fmt.Errorf("flow instance route identity is required")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	}
	return listFlowInstanceRouteRecords(ctx, q, identity, `
		SELECT event_pattern, subscriber_type, subscriber_id, COALESCE(source_flow, '')
		FROM routing_rules
		JOIN flow_instances fi ON fi.instance_id = routing_rules.flow_instance
		WHERE routing_rules.flow_instance = ?
		  AND routing_rules.is_materialized = TRUE
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		ORDER BY event_pattern, subscriber_type, subscriber_id, source_flow
	`)
}

func listFlowInstanceRouteRecords(
	ctx context.Context,
	q flowInstanceDescriptorQueryer,
	identity runtimeflowidentity.Route,
	query string,
) ([]runtimebus.FlowInstanceRouteRecord, error) {
	rows, err := q.QueryContext(ctx, query, identity.InstancePath)
	if err != nil {
		return nil, fmt.Errorf("list exact flow instance route records %s: %w", identity.InstancePath, err)
	}
	defer rows.Close()
	var out []runtimebus.FlowInstanceRouteRecord
	for rows.Next() {
		var record runtimebus.FlowInstanceRouteRecord
		record.Identity = identity
		if err := rows.Scan(&record.EventPattern, &record.SubscriberType, &record.SubscriberID, &record.SourceFlow); err != nil {
			return nil, fmt.Errorf("scan exact flow instance route record %s: %w", identity.InstancePath, err)
		}
		if strings.TrimSpace(record.SourceFlow) == "" {
			record.SourceFlow = identity.ScopeKey
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exact flow instance route records %s: %w", identity.InstancePath, err)
	}
	return out, nil
}

func (s *PostgresStore) ListActiveFlowInstanceDescriptors(ctx context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	} else if conn, ok := runtimepipeline.PipelineSQLConnFromContext(ctx); ok {
		q = conn
	}
	runID, hasRunID, err := activeFlowInstanceDescriptorRunID(ctx)
	if err != nil {
		return nil, err
	}
	if !hasRunID {
		rows, err := q.QueryContext(ctx, `
			SELECT
				COALESCE(fi.instance_id, ''),
				COALESCE(fi.flow_template, ''),
				'', '', '', '{}'::jsonb
			FROM flow_instances fi
			WHERE COALESCE(fi.status, '') = 'active'
			  AND COALESCE(fi.mode, '') = 'template'
			  AND COALESCE(fi.instance_id, '') <> ''
			ORDER BY fi.instance_id ASC
		`)
		if err != nil {
			return nil, fmt.Errorf("list unscoped active flow instance descriptors: %w", err)
		}
		return scanActiveFlowInstanceDescriptors(rows, "active flow instance descriptor")
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			COALESCE(fi.instance_id, ''),
			COALESCE(fi.flow_template, ''),
			COALESCE(run.bundle_hash, ''),
			COALESCE(run.bundle_source, ''),
			COALESCE(NULLIF(readiness.plan->>'workflow_version', ''), fi.config->>'workflow_version', ''),
			COALESCE(es.fields, '{}'::jsonb)
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.instance_id = fi.instance_id
		 AND readiness.run_id = $1::uuid
		JOIN runs run
		  ON run.run_id = $1::uuid
		JOIN LATERAL (
			SELECT fields
			FROM entity_state es
			WHERE es.flow_instance = fi.instance_id
			  AND es.run_id = $1::uuid
			ORDER BY es.updated_at DESC, es.created_at DESC, es.entity_id::text ASC
			LIMIT 1
		) es ON true
		WHERE COALESCE(fi.status, '') = 'active'
		  AND COALESCE(fi.mode, '') = 'template'
		  AND COALESCE(fi.instance_id, '') <> ''
		  AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
		ORDER BY fi.instance_id ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("list active flow instance descriptors: %w", err)
	}
	defer rows.Close()

	return scanActiveFlowInstanceDescriptors(rows, "active flow instance descriptor")
}

func (s *SQLiteRuntimeStore) ListActiveFlowInstanceDescriptors(ctx context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite runtime store is required for active flow instance descriptors")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	}
	runID, hasRunID, err := activeFlowInstanceDescriptorRunID(ctx)
	if err != nil {
		return nil, err
	}
	if !hasRunID {
		rows, err := q.QueryContext(ctx, `
			SELECT
				COALESCE(fi.instance_id, ''),
				COALESCE(fi.flow_template, ''),
				'', '', '', '{}'
			FROM flow_instances fi
			WHERE COALESCE(fi.status, '') = 'active'
			  AND COALESCE(fi.mode, '') = 'template'
			  AND COALESCE(fi.instance_id, '') <> ''
			ORDER BY fi.instance_id ASC
		`)
		if err != nil {
			return nil, fmt.Errorf("list unscoped sqlite active flow instance descriptors: %w", err)
		}
		return scanActiveFlowInstanceDescriptors(rows, "sqlite active flow instance descriptor")
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			COALESCE(fi.instance_id, ''),
			COALESCE(fi.flow_template, ''),
			COALESCE(run.bundle_hash, ''),
			COALESCE(run.bundle_source, ''),
			COALESCE(
				NULLIF(json_extract(readiness.plan, '$.workflow_version'), ''),
				json_extract(fi.config, '$.workflow_version'),
				''
			),
			COALESCE((
			SELECT es.fields
			FROM entity_state es
			WHERE es.flow_instance = fi.instance_id
			  AND es.run_id = ?
			ORDER BY es.updated_at DESC, es.created_at DESC, es.entity_id ASC
			LIMIT 1
			), '{}')
		FROM flow_instances fi
		LEFT JOIN flow_instance_runtime_readiness readiness
		  ON readiness.instance_id = fi.instance_id
		 AND readiness.run_id = ?
		JOIN runs run
		  ON run.run_id = ?
		WHERE COALESCE(fi.status, '') = 'active'
		  AND COALESCE(fi.mode, '') = 'template'
		  AND COALESCE(fi.instance_id, '') <> ''
		  AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		  AND EXISTS (
			  SELECT 1
			  FROM entity_state descriptor_entity
			  WHERE descriptor_entity.flow_instance = fi.instance_id
			    AND descriptor_entity.run_id = ?
		  )
		ORDER BY fi.instance_id ASC
	`, runID, runID, runID, runID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite active flow instance descriptors: %w", err)
	}
	defer rows.Close()

	return scanActiveFlowInstanceDescriptors(rows, "sqlite active flow instance descriptor")
}

func scanActiveFlowInstanceDescriptors(
	rows *sql.Rows,
	label string,
) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	defer rows.Close()
	out := []runtimebus.ActiveFlowInstanceDescriptor{}
	for rows.Next() {
		var descriptor runtimebus.ActiveFlowInstanceDescriptor
		var fieldsRaw any
		if err := rows.Scan(
			&descriptor.FlowInstance,
			&descriptor.FlowTemplate,
			&descriptor.BundleHash,
			&descriptor.BundleSource,
			&descriptor.WorkflowVersion,
			&fieldsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan %s: %w", label, err)
		}
		descriptor.AddressFields = descriptorAddressFields(fieldsRaw)
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" {
			continue
		}
		out = append(out, descriptor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %ss: %w", label, err)
	}
	return out, nil
}

func activeFlowInstanceDescriptorRunID(ctx context.Context) (string, bool, error) {
	runID, ok, err := runtimecurrentstate.RunIDFromContext(ctx)
	if err != nil {
		return "", false, fmt.Errorf("active flow instance descriptor run scope: %w", err)
	}
	return runID, ok, nil
}

func descriptorAddressFields(fieldsRaw any) map[string]string {
	return descriptorAddressFieldsFromJSON(fieldsRaw, "entity.")
}

func descriptorAddressFieldsFromJSON(raw any, prefix string) map[string]string {
	values, err := decodeDescriptorJSONMap(raw)
	if err != nil || len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		scalar, ok := descriptorScalarString(value)
		if !ok || scalar == "" {
			continue
		}
		out[prefix+key] = scalar
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
