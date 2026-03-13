package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimemanager "empireai/internal/runtime/manager"
)

func (s *PostgresStore) LoadLatestOrgTemplate(ctx context.Context) (runtimemanager.OrgTemplateRecord, error) {
	var rec runtimemanager.OrgTemplateRecord
	if err := s.DB.QueryRowContext(ctx, `
		SELECT
			version,
			COALESCE(agents, '[]'::jsonb),
			COALESCE(bootstrap_routes, '[]'::jsonb),
			COALESCE(seeded_routes, '[]'::jsonb),
			COALESCE(created_by, ''),
			COALESCE(description, ''),
			COALESCE(created_at, now())
		FROM org_templates
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(
		&rec.Version,
		&rec.Agents,
		&rec.BootstrapRoutes,
		&rec.SeededRoutes,
		&rec.CreatedBy,
		&rec.Description,
		&rec.CreatedAt,
	); err != nil {
		return runtimemanager.OrgTemplateRecord{}, err
	}
	return rec, nil
}

func (s *PostgresStore) LoadOrgTemplate(ctx context.Context, version string) (runtimemanager.OrgTemplateRecord, error) {
	var rec runtimemanager.OrgTemplateRecord
	version = strings.TrimSpace(version)
	if version == "" {
		return runtimemanager.OrgTemplateRecord{}, fmt.Errorf("template version is required")
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT
			version,
			COALESCE(agents, '[]'::jsonb),
			COALESCE(bootstrap_routes, '[]'::jsonb),
			COALESCE(seeded_routes, '[]'::jsonb),
			COALESCE(created_by, ''),
			COALESCE(description, ''),
			COALESCE(created_at, now())
		FROM org_templates
		WHERE version = $1
	`, version).Scan(
		&rec.Version,
		&rec.Agents,
		&rec.BootstrapRoutes,
		&rec.SeededRoutes,
		&rec.CreatedBy,
		&rec.Description,
		&rec.CreatedAt,
	); err != nil {
		return runtimemanager.OrgTemplateRecord{}, err
	}
	return rec, nil
}

func (s *PostgresStore) SetEntityTemplateVersion(ctx context.Context, entityID, version string) error {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return fmt.Errorf("entity_id is required")
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return fmt.Errorf("template version is required")
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE workflow_instances
		SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('template_version', $2::text),
		    updated_at = now()
		WHERE instance_id = $1::uuid
	`, entityID, version); err != nil {
		return fmt.Errorf("set entity template_version: %w", err)
	}
	return nil
}

func (s *PostgresStore) ResolveBootstrapVersion(ctx context.Context, templateVersion string) (int, error) {
	if s == nil || s.DB == nil {
		return 1, nil
	}
	templateVersion = strings.TrimSpace(templateVersion)

	// Preferred mapping: match template -> bootstrap_versions by exact bootstrap route payload.
	if templateVersion != "" {
		var version int
		err := s.DB.QueryRowContext(ctx, `
			SELECT bv.version
			FROM org_templates ot
			INNER JOIN bootstrap_versions bv
				ON bv.routes = ot.bootstrap_routes
			WHERE ot.version = $1
			ORDER BY bv.created_at DESC, bv.version DESC
			LIMIT 1
		`, templateVersion).Scan(&version)
		if err == nil && version > 0 {
			return version, nil
		}
		if err != nil && err != sql.ErrNoRows {
			return 0, fmt.Errorf("resolve bootstrap_version for template %s: %w", templateVersion, err)
		}
	}

	// Fallback for legacy data: use latest known bootstrap baseline.
	var latest int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 1)
		FROM bootstrap_versions
	`).Scan(&latest); err != nil {
		return 0, fmt.Errorf("resolve latest bootstrap_version: %w", err)
	}
	if latest <= 0 {
		latest = 1
	}
	return latest, nil
}

func (s *PostgresStore) UpsertRoutingRule(ctx context.Context, rule runtimemanager.PersistedRoutingRule) error {
	entityID := rule.EffectiveEntityID()
	if entityID == "" || rule.EventPattern == "" || rule.SubscriberID == "" || rule.InstalledBy == "" {
		return fmt.Errorf("entity_id, event_pattern, subscriber_id, and installed_by are required")
	}
	rule.EntityID = entityID
	status := nullable(rule.Status, "active")
	source := nullable(rule.Source, "bootstrap")

	var bootstrapVersion any
	if rule.BootstrapVersion > 0 {
		bootstrapVersion = rule.BootstrapVersion
	}

	if status == "deactivated" {
		const deactivateQ = `
			UPDATE routing_rules
			SET installed_by = $4,
			    reason = NULLIF($5,''),
			    status = 'deactivated',
			    source = $6,
			    bootstrap_version = $7,
			    deactivated_at = now()
			WHERE vertical_id = $1::uuid
			  AND event_pattern = $2
			  AND subscriber_id = $3
			  AND status <> 'deactivated'
		`
		res, err := s.DB.ExecContext(ctx, deactivateQ,
			rule.EntityID,
			rule.EventPattern,
			rule.SubscriberID,
			rule.InstalledBy,
			rule.Reason,
			source,
			bootstrapVersion,
		)
		if err != nil {
			return fmt.Errorf("deactivate routing rule: %w", err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return nil
		}
		const insertDeactivatedQ = `
			INSERT INTO routing_rules (
				vertical_id, event_pattern, subscriber_id, installed_by, reason,
				status, source, bootstrap_version, deactivated_at, created_at
			) VALUES (
				$1::uuid, $2, $3, $4, NULLIF($5,''),
				'deactivated', $6, $7, now(), now()
			)
		`
		if _, err := s.DB.ExecContext(ctx, insertDeactivatedQ,
			rule.EntityID,
			rule.EventPattern,
			rule.SubscriberID,
			rule.InstalledBy,
			rule.Reason,
			source,
			bootstrapVersion,
		); err != nil {
			return fmt.Errorf("insert deactivated routing rule: %w", err)
		}
		return nil
	}

	const q = `
		INSERT INTO routing_rules (
			vertical_id, event_pattern, subscriber_id, installed_by, reason,
			status, source, bootstrap_version, created_at
		) VALUES (
			$1::uuid, $2, $3, $4, NULLIF($5,''),
			$6, $7, $8, now()
		)
		ON CONFLICT (vertical_id, event_pattern, subscriber_id) WHERE status = 'active' DO UPDATE SET
			installed_by = EXCLUDED.installed_by,
			reason = EXCLUDED.reason,
			status = EXCLUDED.status,
			source = EXCLUDED.source,
			bootstrap_version = EXCLUDED.bootstrap_version,
			deactivated_at = NULL
	`
	if _, err := s.DB.ExecContext(ctx, q,
		rule.EntityID,
		rule.EventPattern,
		rule.SubscriberID,
		rule.InstalledBy,
		rule.Reason,
		status,
		source,
		bootstrapVersion,
	); err != nil {
		return fmt.Errorf("upsert routing rule: %w", err)
	}
	return nil
}

func (s *PostgresStore) LoadRoutingRules(ctx context.Context) ([]runtimemanager.PersistedRoutingRule, error) {
	const q = `
		SELECT
			vertical_id::text, event_pattern, subscriber_id, installed_by,
			COALESCE(reason, ''), status, source, COALESCE(bootstrap_version, 0)
		FROM routing_rules
		WHERE status IN ('active', 'proposed')
		ORDER BY created_at ASC
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
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	const q = `
		UPDATE routing_rules
		SET status = 'deactivated',
		    deactivated_at = now()
		WHERE vertical_id = $1::uuid
		  AND status <> 'deactivated'
	`
	_, err := s.DB.ExecContext(ctx, q, entityID)
	if err != nil {
		return fmt.Errorf("deactivate routing rules by entity: %w", err)
	}
	return nil
}
