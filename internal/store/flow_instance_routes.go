package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type flowInstanceDescriptorQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type flowInstanceRouteExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func flowInstanceRouteExecutorFromContext(ctx context.Context, db *sql.DB) flowInstanceRouteExecutor {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return tx
	}
	return db
}

func (s *PostgresStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	exec := flowInstanceRouteExecutorFromContext(ctx, s.DB)
	route.Identity = runtimeflowidentity.StoredRoute(route.Identity.ScopeKey, route.Identity.InstanceID, route.Identity.InstancePath)
	if !route.Identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	sourceFlow := strings.TrimSpace(route.SourceFlow)
	if sourceFlow == "" {
		sourceFlow = route.Identity.ScopeKey
	}
	var materializedFrom any
	if strings.TrimSpace(route.EventPattern) != "" && strings.TrimSpace(route.SubscriberType) != "" && strings.TrimSpace(route.SubscriberID) != "" {
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
			`, route.EventPattern, route.SubscriberType, route.SubscriberID, sourceFlow).Scan(&materializedFrom)
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
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath, sourceFlow, materializedFrom)
	if err != nil {
		return fmt.Errorf("upsert flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
	}
	return nil
}

func (s *PostgresStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	exec := flowInstanceRouteExecutorFromContext(ctx, s.DB)
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
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
}

func (s *PostgresStore) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	exec := flowInstanceRouteExecutorFromContext(ctx, s.DB)
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
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
}

func (s *PostgresStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
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

func (s *SQLiteRuntimeStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	return s.runRuntimeMutation(ctx, "upsert flow instance route", func(txctx context.Context, tx *sql.Tx) error {
		return upsertSQLiteFlowInstanceRouteTx(txctx, tx, route)
	})
}

func upsertSQLiteFlowInstanceRouteTx(ctx context.Context, tx *sql.Tx, route runtimebus.FlowInstanceRouteRecord) error {
	if tx == nil {
		return fmt.Errorf("sqlite flow instance route transaction is required")
	}
	route.Identity = runtimeflowidentity.StoredRoute(route.Identity.ScopeKey, route.Identity.InstanceID, route.Identity.InstancePath)
	if !route.Identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	sourceFlow := strings.TrimSpace(route.SourceFlow)
	if sourceFlow == "" {
		sourceFlow = route.Identity.ScopeKey
	}
	var materializedFrom sql.NullString
	if strings.TrimSpace(route.EventPattern) != "" && strings.TrimSpace(route.SubscriberType) != "" && strings.TrimSpace(route.SubscriberID) != "" {
		_ = tx.QueryRowContext(ctx, `
			SELECT CAST(rule_id AS TEXT)
			FROM routing_rules
			WHERE event_pattern = ?
			  AND subscriber_type = ?
			  AND subscriber_id = ?
			  AND COALESCE(source_flow, '') = ?
			  AND is_wildcard = true
			  AND is_materialized = false
			  AND status = 'active'
			ORDER BY created_at ASC
			LIMIT 1
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, sourceFlow).Scan(&materializedFrom)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE routing_rules
		SET source_flow = NULLIF(?, ''),
		    materialized_from = ?,
		    status = 'active'
		WHERE event_pattern = ?
		  AND subscriber_type = ?
		  AND subscriber_id = ?
		  AND COALESCE(flow_instance, '') = ?
		  AND is_materialized = true
	`, sourceFlow, nullableFlowInstanceRouteID(materializedFrom), route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath)
	if err != nil {
		return fmt.Errorf("update SQLite flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
	}
	if rows, rowsErr := res.RowsAffected(); rowsErr == nil && rows > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance,
			source_flow, is_wildcard, is_materialized, materialized_from,
			status, created_at
		)
		VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), false, true, ?, 'active', CURRENT_TIMESTAMP)
	`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.InstancePath, sourceFlow, nullableFlowInstanceRouteID(materializedFrom)); err != nil {
		return fmt.Errorf("insert SQLite flow instance route %s/%s: %w", route.Identity.ScopeKey, route.Identity.InstanceID, err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	return s.runRuntimeMutation(ctx, "delete flow instance route", func(txctx context.Context, tx *sql.Tx) error {
		return deleteSQLiteFlowInstanceRouteTx(txctx, tx, identity)
	})
}

func deleteSQLiteFlowInstanceRouteTx(ctx context.Context, tx *sql.Tx, identity runtimeflowidentity.Route) error {
	if tx == nil {
		return fmt.Errorf("sqlite flow instance route transaction is required")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	var status string
	err := tx.QueryRowContext(ctx, `
		SELECT status
		FROM flow_instances
		WHERE instance_id = ?
	`, identity.InstancePath).Scan(&status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("flow instance not found for route removal: %s", identity.InstancePath)
		}
		return fmt.Errorf("load SQLite flow instance for route removal %s: %w", identity.InstancePath, err)
	}
	if strings.TrimSpace(status) != "terminated" {
		return fmt.Errorf("flow instance route removal requires terminal flow_instances status for %s", identity.InstancePath)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE routing_rules
		SET status = 'inactive'
		WHERE flow_instance = ?
		  AND is_materialized = true
		  AND status = 'active'
	`, identity.InstancePath); err != nil {
		return fmt.Errorf("delete SQLite flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) RollbackFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required for flow instance routes")
	}
	return s.runRuntimeMutation(ctx, "rollback flow instance route", func(txctx context.Context, tx *sql.Tx) error {
		return rollbackSQLiteFlowInstanceRouteTx(txctx, tx, identity)
	})
}

func rollbackSQLiteFlowInstanceRouteTx(ctx context.Context, tx *sql.Tx, identity runtimeflowidentity.Route) error {
	if tx == nil {
		return fmt.Errorf("sqlite flow instance route transaction is required")
	}
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE routing_rules
		SET status = 'inactive'
		WHERE flow_instance = ?
		  AND is_materialized = true
		  AND status = 'active'
	`, identity.InstancePath); err != nil {
		return fmt.Errorf("rollback SQLite flow instance route %s/%s: %w", identity.ScopeKey, identity.InstanceID, err)
	}
	return nil
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
		WHERE is_materialized = true
		  AND routing_rules.status = 'active'
		  AND fi.status = 'active'
		  AND flow_instance IS NOT NULL
		GROUP BY flow_instance, source_flow
		ORDER BY flow_instance ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list SQLite flow instance routes: %w", err)
	}
	defer rows.Close()
	out := []runtimeflowidentity.Route{}
	for rows.Next() {
		var sourceFlow, instancePath string
		if err := rows.Scan(&sourceFlow, &instancePath); err != nil {
			return nil, fmt.Errorf("scan SQLite flow instance route: %w", err)
		}
		route := runtimeflowidentity.StoredRoute(sourceFlow, "", instancePath)
		if route.Valid() {
			out = append(out, route)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite flow instance routes: %w", err)
	}
	return out, nil
}

func nullableFlowInstanceRouteID(value sql.NullString) any {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil
	}
	return strings.TrimSpace(value.String)
}

func (s *PostgresStore) ListActiveFlowInstanceDescriptors(ctx context.Context) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for active flow instance descriptors")
	}
	q := flowInstanceDescriptorQueryer(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		q = tx
	}
	runID, hasRunID, err := activeFlowInstanceDescriptorRunID(ctx)
	if err != nil {
		return nil, err
	}
	query := `
		SELECT
			COALESCE(fi.instance_id, ''),
			COALESCE(fi.flow_template, ''),
`
	args := []any{}
	if hasRunID {
		query += `			COALESCE(es.fields, '{}'::jsonb)
		FROM flow_instances fi
		LEFT JOIN LATERAL (
			SELECT fields
			FROM entity_state es
			WHERE es.flow_instance = fi.instance_id
			  AND es.run_id = $1::uuid
`
		args = append(args, runID)
		query += `			ORDER BY es.updated_at DESC, es.created_at DESC, es.entity_id::text ASC
			LIMIT 1
		) es ON true
`
	} else {
		query += `			'{}'::jsonb
		FROM flow_instances fi
`
	}
	query += `		WHERE COALESCE(fi.status, '') = 'active'
		  AND COALESCE(fi.mode, '') = 'template'
		  AND COALESCE(fi.instance_id, '') <> ''
		ORDER BY fi.instance_id ASC
	`
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active flow instance descriptors: %w", err)
	}
	defer rows.Close()

	out := []runtimebus.ActiveFlowInstanceDescriptor{}
	for rows.Next() {
		var descriptor runtimebus.ActiveFlowInstanceDescriptor
		var fieldsRaw any
		if err := rows.Scan(&descriptor.FlowInstance, &descriptor.FlowTemplate, &fieldsRaw); err != nil {
			return nil, fmt.Errorf("scan active flow instance descriptor: %w", err)
		}
		descriptor.AddressFields = descriptorAddressFields(fieldsRaw)
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" {
			continue
		}
		out = append(out, descriptor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active flow instance descriptors: %w", err)
	}
	return out, nil
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
	args := []any{}
	fieldsExpr := `'{}'`
	if hasRunID {
		fieldsSubquery := `
			SELECT es.fields
			FROM entity_state es
			WHERE es.flow_instance = fi.instance_id
			  AND es.run_id = ?
`
		args = append(args, runID)
		fieldsSubquery += `			ORDER BY es.updated_at DESC, es.created_at DESC, es.entity_id ASC
			LIMIT 1
`
		fieldsExpr = `COALESCE((` + fieldsSubquery + `), '{}')`
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			COALESCE(fi.instance_id, ''),
			COALESCE(fi.flow_template, ''),
			`+fieldsExpr+`
		FROM flow_instances fi
		WHERE COALESCE(fi.status, '') = 'active'
		  AND COALESCE(fi.mode, '') = 'template'
		  AND COALESCE(fi.instance_id, '') <> ''
		ORDER BY fi.instance_id ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite active flow instance descriptors: %w", err)
	}
	defer rows.Close()

	out := []runtimebus.ActiveFlowInstanceDescriptor{}
	for rows.Next() {
		var descriptor runtimebus.ActiveFlowInstanceDescriptor
		var fieldsRaw any
		if err := rows.Scan(&descriptor.FlowInstance, &descriptor.FlowTemplate, &fieldsRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite active flow instance descriptor: %w", err)
		}
		descriptor.AddressFields = descriptorAddressFields(fieldsRaw)
		descriptor = descriptor.Normalized()
		if descriptor.FlowInstance == "" {
			continue
		}
		out = append(out, descriptor)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite active flow instance descriptors: %w", err)
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
