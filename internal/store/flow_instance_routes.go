package store

import (
	"context"
	"fmt"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
)

func (s *PostgresStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
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
		_ = s.DB.QueryRowContext(ctx, `
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
	_, err := s.DB.ExecContext(ctx, `
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
	identity = runtimeflowidentity.StoredRoute(identity.ScopeKey, identity.InstanceID, identity.InstancePath)
	if !identity.Valid() {
		return fmt.Errorf("scope_key, instance_id, and instance_path are required")
	}
	if _, err := s.DB.ExecContext(ctx, `
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

func (s *PostgresStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimeflowidentity.Route, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	rows, err := s.DB.QueryContext(ctx, `
			SELECT
				COALESCE(NULLIF(source_flow, ''), ''),
				flow_instance
			FROM routing_rules
			WHERE is_materialized = true
			  AND status = 'active'
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
