package store

import (
	"context"
	"fmt"
	"strings"

	runtimemanager "empireai/internal/runtime/manager"
)

func (s *PostgresStore) UpsertAgent(ctx context.Context, rec runtimemanager.PersistedAgent) error {
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	rec.Config.NormalizeEntityID()
	cfgJSON, err := mergeAgentConfigJSON(rec.Config)
	if err != nil {
		return fmt.Errorf("marshal agent config: %w", err)
	}

	var startedAt any
	if !rec.StartedAt.IsZero() {
		startedAt = rec.StartedAt
	}

	const q = `
		INSERT INTO agents (
			id, type, role, mode, vertical_id, parent_agent_id, status,
			coordinator_id, config, budget_envelope, hired_by, template_version,
			started_at, last_active_at
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5,'')::uuid, NULLIF($6,''), $7,
			NULLIF($8,''), $9::jsonb, NULLIF($10, 0), NULLIF($11,''), NULLIF($12,''),
			COALESCE($13, now()), now()
		)
		ON CONFLICT (id) DO UPDATE SET
			type = EXCLUDED.type,
			role = EXCLUDED.role,
			mode = EXCLUDED.mode,
			vertical_id = EXCLUDED.vertical_id,
			parent_agent_id = EXCLUDED.parent_agent_id,
			status = EXCLUDED.status,
			coordinator_id = EXCLUDED.coordinator_id,
			config = EXCLUDED.config,
			budget_envelope = EXCLUDED.budget_envelope,
			hired_by = EXCLUDED.hired_by,
			template_version = EXCLUDED.template_version,
			last_active_at = now()
	`
	_, err = s.DB.ExecContext(ctx, q,
		rec.Config.ID,
		nullable(rec.Config.Type, "generic"),
		rec.Config.Role,
		nullable(rec.Config.Mode, "factory"),
		rec.Config.EffectiveEntityID(),
		nullable(rec.ParentAgentID, rec.Config.ParentAgent),
		nullable(rec.Status, "active"),
		rec.CoordinatorID,
		string(cfgJSON),
		rec.Config.BudgetEnvelope,
		rec.HiredBy,
		rec.TemplateVersion,
		startedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert agent %s: %w", rec.Config.ID, err)
	}
	return nil
}

func (s *PostgresStore) LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error) {
	const q = `
		SELECT
			id, type, role, mode,
			COALESCE(vertical_id::text, ''),
			COALESCE(parent_agent_id, ''),
			COALESCE(status, 'active'),
			COALESCE(coordinator_id, ''),
			config,
			COALESCE(budget_envelope, 0),
			COALESCE(hired_by, ''),
			COALESCE(template_version, ''),
			COALESCE(started_at, now())
		FROM agents
		WHERE status NOT IN ('terminated', 'ephemeral')
		ORDER BY started_at ASC, id ASC
	`
	rows, err := s.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query agents: %w", err)
	}
	defer rows.Close()

	var out []runtimemanager.PersistedAgent
	for rows.Next() {
		var rec runtimemanager.PersistedAgent
		var cfgRaw []byte
		if err := rows.Scan(
			&rec.Config.ID,
			&rec.Config.Type,
			&rec.Config.Role,
			&rec.Config.Mode,
			&rec.Config.EntityID,
			&rec.ParentAgentID,
			&rec.Status,
			&rec.CoordinatorID,
			&cfgRaw,
			&rec.Config.BudgetEnvelope,
			&rec.HiredBy,
			&rec.TemplateVersion,
			&rec.StartedAt,
		); err != nil {
			return nil, fmt.Errorf("scan agent row: %w", err)
		}

		rec.Config.ParentAgent = rec.ParentAgentID
		rec.Config.NormalizeEntityID()
		rec.Config.Config = cfgRaw
		rec.Config.Subscriptions = extractSubscriptions(cfgRaw)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agents rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkAgentTerminated(ctx context.Context, agentID string) error {
	if strings.TrimSpace(agentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mark agent terminated begin tx: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	const qAgent = `
		UPDATE agents
		SET status = 'terminated',
		    last_active_at = now()
		WHERE id = $1
	`
	if _, err := tx.ExecContext(ctx, qAgent, agentID); err != nil {
		return fmt.Errorf("mark agent terminated: %w", err)
	}

	// Ensure teardown removes "active" runtime state so dashboard/runtime replay
	// never treats terminated agents as still-live conversational actors.
	const qConversations = `
		UPDATE conversations
		SET status = 'terminated',
		    updated_at = now()
		WHERE agent_id = $1
		  AND status = 'active'
	`
	if _, err := tx.ExecContext(ctx, qConversations, agentID); err != nil {
		return fmt.Errorf("mark agent terminated conversations: %w", err)
	}

	const qSessions = `
		UPDATE agent_sessions
		SET status = 'rotated',
		    rotated_at = COALESCE(rotated_at, now()),
		    lock_owner = NULL,
		    lock_expires_at = NULL,
		    last_used_at = now()
		WHERE agent_id = $1
		  AND status = 'active'
	`
	if _, err := tx.ExecContext(ctx, qSessions, agentID); err != nil {
		return fmt.Errorf("mark agent terminated sessions: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mark agent terminated commit: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) EnsureEntitySchema(ctx context.Context, entityID string) error {
	if strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("entity_id is required")
	}
	var slug string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(metadata->>'slug', ''), '')
		FROM workflow_instances
		WHERE instance_id = $1::uuid
		ORDER BY created_at DESC, updated_at DESC
		LIMIT 1
	`, entityID).Scan(&slug); err != nil {
		return fmt.Errorf("lookup entity slug: %w", err)
	}
	schema := sanitizeSchemaIdent(slug)
	if schema == "" {
		return fmt.Errorf("entity %s has no valid slug for schema creation", entityID)
	}
	schema = schema + "_schema"
	if _, err := s.DB.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+quoteIdent(schema)); err != nil {
		return fmt.Errorf("create entity schema %s: %w", schema, err)
	}
	return nil
}
