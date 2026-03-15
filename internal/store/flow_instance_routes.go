package store

import (
	"context"
	"fmt"
	"strings"

	runtimebus "empireai/internal/runtime/bus"
)

func (s *PostgresStore) UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	route.TemplateID = strings.TrimSpace(route.TemplateID)
	route.InstanceID = strings.TrimSpace(route.InstanceID)
	route.InstancePath = strings.Trim(strings.TrimSpace(route.InstancePath), "/")
	if route.TemplateID == "" || route.InstanceID == "" || route.InstancePath == "" {
		return fmt.Errorf("template_id, instance_id, and instance_path are required")
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
		`, route.EventPattern, route.SubscriberType, route.SubscriberID, strings.TrimSpace(route.SourceFlow)).Scan(&materializedFrom)
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
	`, route.EventPattern, route.SubscriberType, route.SubscriberID, route.InstancePath, route.SourceFlow, materializedFrom)
	if err != nil {
		return fmt.Errorf("upsert flow instance route %s/%s: %w", route.TemplateID, route.InstanceID, err)
	}
	return nil
}

func (s *PostgresStore) DeleteFlowInstanceRoute(ctx context.Context, templateID, instanceID string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for flow instance routes")
	}
	templateID = strings.TrimSpace(templateID)
	instanceID = strings.TrimSpace(instanceID)
	if templateID == "" || instanceID == "" {
		return fmt.Errorf("template_id and instance_id are required")
	}
	instancePath := strings.Trim(strings.TrimSpace(templateID), "/")
	if trimmedInstanceID := strings.Trim(strings.TrimSpace(instanceID), "/"); trimmedInstanceID != "" {
		if instancePath != "" {
			instancePath += "/"
		}
		instancePath += trimmedInstanceID
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE routing_rules
		SET status = 'inactive'
		WHERE flow_instance = $1
		  AND is_materialized = true
		  AND status = 'active'
	`, instancePath); err != nil {
		return fmt.Errorf("delete flow instance route %s/%s: %w", templateID, instanceID, err)
	}
	return nil
}

func (s *PostgresStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			COALESCE(NULLIF(source_flow, ''), split_part(flow_instance, '/', 1)),
			split_part(flow_instance, '/', array_length(string_to_array(flow_instance, '/'), 1)),
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

	out := []runtimebus.FlowInstanceRouteRecord{}
	for rows.Next() {
		var route runtimebus.FlowInstanceRouteRecord
		if err := rows.Scan(&route.TemplateID, &route.InstanceID, &route.InstancePath); err != nil {
			return nil, fmt.Errorf("scan flow instance route: %w", err)
		}
		route.TemplateID = strings.TrimSpace(route.TemplateID)
		route.InstanceID = strings.TrimSpace(route.InstanceID)
		route.InstancePath = strings.Trim(strings.TrimSpace(route.InstancePath), "/")
		if route.TemplateID == "" || route.InstanceID == "" || route.InstancePath == "" {
			continue
		}
		out = append(out, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate flow instance routes: %w", err)
	}
	return out, nil
}
