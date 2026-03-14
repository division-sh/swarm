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
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO flow_instance_routes (template_id, instance_id, instance_path, created_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (template_id, instance_id)
		DO UPDATE SET instance_path = EXCLUDED.instance_path
	`, route.TemplateID, route.InstanceID, route.InstancePath)
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
	if _, err := s.DB.ExecContext(ctx, `
		DELETE FROM flow_instance_routes
		WHERE template_id = $1 AND instance_id = $2
	`, templateID, instanceID); err != nil {
		return fmt.Errorf("delete flow instance route %s/%s: %w", templateID, instanceID, err)
	}
	return nil
}

func (s *PostgresStore) ListFlowInstanceRoutes(ctx context.Context) ([]runtimebus.FlowInstanceRouteRecord, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required for flow instance routes")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT template_id, instance_id, instance_path
		FROM flow_instance_routes
		ORDER BY created_at ASC, template_id ASC, instance_id ASC
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
