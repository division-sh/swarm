package store

import (
	"context"
	"fmt"
	"strings"

	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *PostgresStore) UpsertRoutingRule(ctx context.Context, rule runtimemanager.PersistedRoutingRule) error {
	entityID := strings.TrimSpace(rule.EffectiveEntityID())
	eventPattern := strings.TrimSpace(rule.EventPattern)
	subscriberID := strings.TrimSpace(rule.SubscriberID)
	if entityID == "" || eventPattern == "" || subscriberID == "" || strings.TrimSpace(rule.InstalledBy) == "" {
		return fmt.Errorf("entity_id, event_pattern, subscriber_id, and installed_by are required")
	}
	status := normalizeRoutingRuleStatus(rule.Status)
	flowInstance, err := routingRuleFlowInstance(ctx, s, entityID)
	if err != nil {
		return err
	}
	isWildcard := strings.Contains(eventPattern, "*")
	if status == "inactive" {
		const deactivateQ = `
			UPDATE routing_rules
			SET status = 'inactive'
			WHERE event_pattern = $1
			  AND subscriber_type = 'agent'
			  AND subscriber_id = $2
			  AND flow_instance = $3
			  AND is_materialized = false
			  AND status <> 'inactive'
		`
		res, err := s.DB.ExecContext(ctx, deactivateQ,
			eventPattern,
			subscriberID,
			flowInstance,
		)
		if err != nil {
			return fmt.Errorf("deactivate routing rule: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return nil
		}
		const insertDeactivatedQ = `
			INSERT INTO routing_rules (
				event_pattern, subscriber_type, subscriber_id, flow_instance,
				is_wildcard, is_materialized, status, created_at
			) VALUES (
				$1, 'agent', $2, $3, $4, false, 'inactive', now()
			)
		`
		if _, err := s.DB.ExecContext(ctx, insertDeactivatedQ,
			eventPattern,
			subscriberID,
			flowInstance,
			isWildcard,
		); err != nil {
			return fmt.Errorf("insert deactivated routing rule: %w", err)
		}
		return nil
	}

	const q = `
		WITH updated AS (
			UPDATE routing_rules
			SET status = 'active',
			    is_wildcard = $4
			WHERE event_pattern = $1
			  AND subscriber_type = 'agent'
			  AND subscriber_id = $2
			  AND flow_instance = $3
			  AND is_materialized = false
			RETURNING rule_id
		)
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance,
			is_wildcard, is_materialized, status, created_at
		)
		SELECT
			$1, 'agent', $2, $3, $4, false, 'active', now()
		WHERE NOT EXISTS (SELECT 1 FROM updated)
	`
	if _, err := s.DB.ExecContext(ctx, q,
		eventPattern,
		subscriberID,
		flowInstance,
		isWildcard,
	); err != nil {
		return fmt.Errorf("upsert routing rule: %w", err)
	}
	return nil
}

func (s *PostgresStore) LoadRoutingRules(ctx context.Context) ([]runtimemanager.PersistedRoutingRule, error) {
	const q = `
		SELECT
			'',
			rr.event_pattern,
			rr.subscriber_id,
			rr.subscriber_id,
			'',
			rr.status,
			COALESCE(rr.source_flow, ''),
			0
		FROM routing_rules rr
		WHERE rr.status = 'active'
		  AND rr.subscriber_type = 'agent'
		  AND rr.is_materialized = false
		ORDER BY rr.created_at ASC
	`
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query routing rules: %w", err)
	}
	defer rows.Close()

	out := make([]runtimemanager.PersistedRoutingRule, 0)
	for rows.Next() {
		var r runtimemanager.PersistedRoutingRule
		if err := rows.Scan(
			&r.EntityID,
			&r.EventPattern,
			&r.SubscriberID,
			&r.InstalledBy,
			&r.Reason,
			&r.Status,
			&r.Source,
			&r.BootstrapVersion,
		); err != nil {
			return nil, fmt.Errorf("scan routing rule: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read routing rule rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) DeactivateRoutingRulesByEntity(ctx context.Context, entityID string) error {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return fmt.Errorf("entity_id is required")
	}
	flowInstance, err := routingRuleFlowInstance(ctx, s, entityID)
	if err != nil {
		return err
	}
	const q = `
		UPDATE routing_rules
		SET status = 'inactive'
		WHERE flow_instance = $1
		  AND status <> 'inactive'
	`
	_, err = s.DB.ExecContext(ctx, q, flowInstance)
	if err != nil {
		return fmt.Errorf("deactivate routing rules by entity: %w", err)
	}
	return nil
}

func routingRuleFlowInstance(ctx context.Context, s *PostgresStore, entityID string) (string, error) {
	identity, err := runtimecurrentstate.RequireIdentity(ctx, entityID)
	if err != nil {
		return "", fmt.Errorf("lookup routing rule flow instance: %w", err)
	}
	var flowInstance string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT flow_instance
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, identity.RunID, identity.EntityID).Scan(&flowInstance); err != nil {
		return "", fmt.Errorf("lookup routing rule flow instance: %w", err)
	}
	flowInstance = strings.TrimSpace(flowInstance)
	if flowInstance == "" {
		return "", fmt.Errorf("entity %s has no flow_instance", entityID)
	}
	return flowInstance, nil
}

func normalizeRoutingRuleStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "active", "proposed":
		return "active"
	default:
		return "inactive"
	}
}
