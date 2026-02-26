package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
)

func (s *PostgresStore) UpsertAgent(ctx context.Context, rec runtime.PersistedAgent) error {
	if rec.Config.ID == "" {
		return fmt.Errorf("agent id is required")
	}
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
		rec.Config.VerticalID,
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

func (s *PostgresStore) LoadAgents(ctx context.Context) ([]runtime.PersistedAgent, error) {
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

	var out []runtime.PersistedAgent
	for rows.Next() {
		var rec runtime.PersistedAgent
		var cfgRaw []byte
		if err := rows.Scan(
			&rec.Config.ID,
			&rec.Config.Type,
			&rec.Config.Role,
			&rec.Config.Mode,
			&rec.Config.VerticalID,
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
		SET status = 'terminated',
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

func (s *PostgresStore) EnsureVerticalSchema(ctx context.Context, verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return fmt.Errorf("vertical_id is required")
	}
	var slug string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug); err != nil {
		return fmt.Errorf("lookup vertical slug: %w", err)
	}
	schema := sanitizeSchemaIdent(slug)
	if schema == "" {
		return fmt.Errorf("vertical %s has no valid slug for schema creation", verticalID)
	}
	schema = schema + "_schema"
	if _, err := s.DB.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+quoteIdent(schema)); err != nil {
		return fmt.Errorf("create vertical schema %s: %w", schema, err)
	}
	return nil
}

func (s *PostgresStore) LoadLatestOrgTemplate(ctx context.Context) (runtime.OrgTemplateRecord, error) {
	var rec runtime.OrgTemplateRecord
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
		return runtime.OrgTemplateRecord{}, err
	}
	return rec, nil
}

func (s *PostgresStore) LoadOrgTemplate(ctx context.Context, version string) (runtime.OrgTemplateRecord, error) {
	var rec runtime.OrgTemplateRecord
	version = strings.TrimSpace(version)
	if version == "" {
		return runtime.OrgTemplateRecord{}, fmt.Errorf("template version is required")
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
		return runtime.OrgTemplateRecord{}, err
	}
	return rec, nil
}

func (s *PostgresStore) SetVerticalTemplateVersion(ctx context.Context, verticalID, version string) error {
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return fmt.Errorf("vertical_id is required")
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return fmt.Errorf("template version is required")
	}
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE verticals
		SET template_version = $2,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, version); err != nil {
		return fmt.Errorf("set vertical template_version: %w", err)
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

func (s *PostgresStore) UpsertRoutingRule(ctx context.Context, rule runtime.PersistedRoutingRule) error {
	if rule.VerticalID == "" || rule.EventPattern == "" || rule.SubscriberID == "" || rule.InstalledBy == "" {
		return fmt.Errorf("vertical_id, event_pattern, subscriber_id, and installed_by are required")
	}
	status := nullable(rule.Status, "active")
	source := nullable(rule.Source, "bootstrap")

	var bootstrapVersion any
	if rule.BootstrapVersion > 0 {
		bootstrapVersion = rule.BootstrapVersion
	}

	const q = `
		INSERT INTO routing_rules (
			vertical_id, event_pattern, subscriber_id, installed_by, reason,
			status, source, bootstrap_version, created_at
		) VALUES (
			$1::uuid, $2, $3, $4, NULLIF($5,''),
			$6, $7, $8, now()
		)
		ON CONFLICT (vertical_id, event_pattern, subscriber_id) DO UPDATE SET
			installed_by = EXCLUDED.installed_by,
			reason = EXCLUDED.reason,
			status = EXCLUDED.status,
			source = EXCLUDED.source,
			bootstrap_version = EXCLUDED.bootstrap_version,
			deactivated_at = CASE
				WHEN EXCLUDED.status = 'deactivated' THEN now()
				ELSE NULL
			END
	`
	if _, err := s.DB.ExecContext(ctx, q,
		rule.VerticalID,
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

func (s *PostgresStore) LoadRoutingRules(ctx context.Context) ([]runtime.PersistedRoutingRule, error) {
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

	out := make([]runtime.PersistedRoutingRule, 0)
	for rows.Next() {
		var r runtime.PersistedRoutingRule
		if err := rows.Scan(
			&r.VerticalID,
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

func (s *PostgresStore) DeactivateRoutingRulesByVertical(ctx context.Context, verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return fmt.Errorf("vertical_id is required")
	}
	const q = `
		UPDATE routing_rules
		SET status = 'deactivated',
		    deactivated_at = now()
		WHERE vertical_id = $1::uuid
		  AND status <> 'deactivated'
	`
	_, err := s.DB.ExecContext(ctx, q, verticalID)
	if err != nil {
		return fmt.Errorf("deactivate routing rules by vertical: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertEventReceipt(ctx context.Context, eventID, agentID, status, errText string) error {
	if eventID == "" || agentID == "" {
		return nil
	}
	if status == "" {
		status = "processed"
	}

	const q = `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, $2, now(), $3, CASE WHEN $3 = 'error' THEN 1 ELSE 0 END, NULLIF($4,''))
		ON CONFLICT (event_id, agent_id) DO UPDATE SET
			processed_at = now(),
			status = CASE
				-- v2.0: allow 3 retries (1m, 5m, 30m) after the initial attempt.
				-- We store retry_count as the number of failures seen so far; dead-letter after the 4th failure.
				WHEN EXCLUDED.status = 'error' AND event_receipts.retry_count + 1 >= 4 THEN 'dead_letter'
				ELSE EXCLUDED.status
			END,
			error = EXCLUDED.error,
			retry_count = CASE
				WHEN EXCLUDED.status = 'error' THEN event_receipts.retry_count + 1
				ELSE event_receipts.retry_count
			END
	`
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, status, errText); err != nil {
		return fmt.Errorf("upsert event receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	if agentID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}

	const q = `
		SELECT
			e.id::text, e.type, e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			e.payload, e.created_at
		FROM event_deliveries d
		INNER JOIN events e ON e.id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.agent_id = d.agent_id
		WHERE d.agent_id = $1
		  AND e.created_at >= $2
		  AND (
				r.event_id IS NULL
				OR (
					r.status = 'error'
					AND r.retry_count <= 3
					AND (
						(r.retry_count = 1 AND r.processed_at <= now() - interval '1 minute')
						OR
						(r.retry_count = 2 AND r.processed_at <= now() - interval '5 minute')
						OR
						(r.retry_count = 3 AND r.processed_at <= now() - interval '30 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending events for %s: %w", agentID, err)
	}
	defer rows.Close()

	out := make([]events.Event, 0)
	for rows.Next() {
		var evt events.Event
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&evt.TaskID,
			&evt.VerticalID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending event: %w", err)
		}
		out = append(out, evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending events rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListPendingSubscribedEvents(
	ctx context.Context,
	agentID string,
	subscriptions []events.EventType,
	since time.Time,
	limit int,
) ([]events.Event, error) {
	if agentID == "" || len(subscriptions) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}

	const q = `
		SELECT
			e.id::text, e.type, e.source_agent,
			COALESCE(e.task_id::text, ''),
			COALESCE(e.vertical_id::text, ''),
			e.payload, e.created_at
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.id
			AND r.agent_id = $1
		WHERE e.created_at >= $2
		  AND (
				NOT EXISTS (
					SELECT 1
					FROM event_deliveries d_any
					WHERE d_any.event_id = e.id
				)
				OR EXISTS (
					SELECT 1
					FROM event_deliveries d_me
					WHERE d_me.event_id = e.id
					  AND d_me.agent_id = $1
				)
			)
		  AND (
				r.event_id IS NULL
				OR (
					r.status = 'error'
					AND r.retry_count <= 3
					AND (
						(r.retry_count = 1 AND r.processed_at <= now() - interval '1 minute')
						OR
						(r.retry_count = 2 AND r.processed_at <= now() - interval '5 minute')
						OR
						(r.retry_count = 3 AND r.processed_at <= now() - interval '30 minute')
					)
				)
			)
		ORDER BY e.created_at ASC
		LIMIT $3
	`
	rows, err := s.DB.QueryContext(ctx, q, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending subscribed events for %s: %w", agentID, err)
	}
	defer rows.Close()

	out := make([]events.Event, 0)
	for rows.Next() {
		var evt events.Event
		if err := rows.Scan(
			&evt.ID,
			&evt.Type,
			&evt.SourceAgent,
			&evt.TaskID,
			&evt.VerticalID,
			&evt.Payload,
			&evt.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending subscribed event: %w", err)
		}
		if matchesAnySubscription(string(evt.Type), subscriptions) {
			out = append(out, evt)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending subscribed events rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtime.EventReceipt, bool, error) {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtime.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	var r runtime.EventReceipt
	r.EventID = eventID
	r.AgentID = agentID
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, 'processed'), COALESCE(retry_count, 0), COALESCE(error, '')
		FROM event_receipts
		WHERE event_id = $1::uuid AND agent_id = $2
	`, eventID, agentID).Scan(&r.Status, &r.RetryCount, &r.Error); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.EventReceipt{}, false, nil
		}
		return runtime.EventReceipt{}, false, fmt.Errorf("get event receipt: %w", err)
	}
	return r, true, nil
}

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtime.AgentTurnRecord) error {
	if rec.AgentID == "" || rec.RuntimeMode == "" || rec.SessionID == "" {
		return fmt.Errorf("agent_id, runtime_mode, and session_id are required")
	}

	const q = `
		WITH s AS (
			SELECT id, turn_count
			FROM agent_sessions
			WHERE agent_id = $1
			  AND runtime_mode = $2
			  AND session_id = $3
			  AND status = 'active'
			ORDER BY created_at DESC
			LIMIT 1
		)
		INSERT INTO agent_turns (
			agent_id, session_row_id, turn_index, task_id,
			request_payload, response_payload, parse_ok, latency_ms,
			retry_count, error, created_at
		)
		SELECT
			$1,
			s.id,
			s.turn_count,
			NULLIF($4,'')::uuid,
			CASE WHEN $5 = '' THEN NULL ELSE $5::jsonb END,
			CASE WHEN $6 = '' THEN NULL ELSE $6::jsonb END,
			$7,
			$8,
			$9,
			NULLIF($10,''),
			now()
		FROM s
		ON CONFLICT (session_row_id, turn_index) DO UPDATE SET
			task_id = EXCLUDED.task_id,
			request_payload = EXCLUDED.request_payload,
			response_payload = EXCLUDED.response_payload,
			parse_ok = EXCLUDED.parse_ok,
			latency_ms = EXCLUDED.latency_ms,
			retry_count = EXCLUDED.retry_count,
			error = EXCLUDED.error,
			created_at = now()
	`

	reqPayload := normalizeJSONPayload(rec.RequestPayload)
	respPayload := normalizeJSONPayload(rec.ResponseRaw)
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}

	res, err := s.DB.ExecContext(ctx, q,
		rec.AgentID,
		rec.RuntimeMode,
		rec.SessionID,
		rec.TaskID,
		reqPayload,
		respPayload,
		rec.ParseOK,
		latencyMS,
		rec.RetryCount,
		rec.Error,
	)
	if err != nil {
		return fmt.Errorf("append agent turn: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no active session row found for agent=%s runtime=%s session=%s", rec.AgentID, rec.RuntimeMode, rec.SessionID)
	}
	return nil
}

func (s *PostgresStore) UpsertConversation(ctx context.Context, rec runtime.ConversationRecord) error {
	if strings.TrimSpace(rec.AgentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	mode := strings.TrimSpace(strings.ToLower(rec.Mode))
	if mode == "" {
		mode = "session"
	}
	status := strings.TrimSpace(strings.ToLower(rec.Status))
	if status == "" {
		status = "active"
	}
	scopeKey := strings.TrimSpace(rec.ScopeKey)
	msgs := make([]runtime.Message, 0, len(rec.Messages))
	for _, m := range rec.Messages {
		msgs = append(msgs, runtime.Message{
			Role:    strings.TrimSpace(m.Role),
			Content: redactText(m.Content),
		})
	}
	msgJSON, err := json.Marshal(msgs)
	if err != nil {
		return fmt.Errorf("marshal conversation messages: %w", err)
	}
	summary := strings.ToValidUTF8(rec.Summary, "\uFFFD")
	const upsertQuery = `
		INSERT INTO conversations (
			agent_id, task_id, scope_key, mode, messages, summary, turn_count, status, created_at, updated_at
		)
		VALUES (
			$1, NULLIF($3,'')::uuid, NULLIF($4,''), $2, $5::jsonb, NULLIF($6,''), $7, $8, now(), now()
		)
		ON CONFLICT (agent_id, mode, (COALESCE(scope_key, '')))
		WHERE status = 'active'
		DO UPDATE SET
			task_id = EXCLUDED.task_id,
			scope_key = EXCLUDED.scope_key,
			messages = EXCLUDED.messages,
			summary = EXCLUDED.summary,
			turn_count = EXCLUDED.turn_count,
			status = EXCLUDED.status,
			updated_at = now()
	`
	if _, err := s.DB.ExecContext(ctx, upsertQuery,
		rec.AgentID,
		mode,
		rec.TaskID,
		scopeKey,
		string(msgJSON),
		summary,
		rec.TurnCount,
		status,
	); err != nil {
		if shouldFallbackConversationScope(err) {
			const legacyNoScopeQuery = `
				WITH updated AS (
					UPDATE conversations
					SET task_id = NULLIF($3,'')::uuid,
						messages = $4::jsonb,
						summary = NULLIF($5,''),
						turn_count = $6,
						status = $7,
						updated_at = now()
					WHERE agent_id = $1
						AND mode = $2
						AND status = 'active'
					RETURNING id
				)
				INSERT INTO conversations (
					agent_id, task_id, mode, messages, summary, turn_count, status, created_at, updated_at
				)
				SELECT
					$1, NULLIF($3,'')::uuid, $2, $4::jsonb, NULLIF($5,''), $6, $7, now(), now()
				WHERE NOT EXISTS (SELECT 1 FROM updated)
			`
			if _, legacyErr := s.DB.ExecContext(ctx, legacyNoScopeQuery,
				rec.AgentID,
				mode,
				rec.TaskID,
				string(msgJSON),
				summary,
				rec.TurnCount,
				status,
			); legacyErr != nil {
				return fmt.Errorf("upsert conversation legacy scope fallback: %w", legacyErr)
			}
			return nil
		}
		if !shouldFallbackConversationUpsert(err) {
			return fmt.Errorf("upsert conversation: %w", err)
		}
		// Backward compatibility for databases not yet migrated to the unique active index.
		const legacyQuery = `
			WITH updated AS (
				UPDATE conversations
				SET task_id = NULLIF($3,'')::uuid,
					scope_key = NULLIF($4,''),
					messages = $5::jsonb,
					summary = NULLIF($6,''),
					turn_count = $7,
					status = $8,
					updated_at = now()
				WHERE agent_id = $1
					AND mode = $2
					AND COALESCE(scope_key, '') = COALESCE(NULLIF($4,''), '')
					AND status = 'active'
				RETURNING id
			)
			INSERT INTO conversations (
				agent_id, task_id, scope_key, mode, messages, summary, turn_count, status, created_at, updated_at
			)
			SELECT
				$1, NULLIF($3,'')::uuid, NULLIF($4,''), $2, $5::jsonb, NULLIF($6,''), $7, $8, now(), now()
			WHERE NOT EXISTS (SELECT 1 FROM updated)
		`
		if _, legacyErr := s.DB.ExecContext(ctx, legacyQuery,
			rec.AgentID,
			mode,
			rec.TaskID,
			scopeKey,
			string(msgJSON),
			summary,
			rec.TurnCount,
			status,
		); legacyErr != nil {
			return fmt.Errorf("upsert conversation fallback: %w", legacyErr)
		}
	}
	return nil
}

func shouldFallbackConversationUpsert(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "there is no unique or exclusion constraint matching")
}

func shouldFallbackConversationScope(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "scope_key") && strings.Contains(msg, "does not exist")
}

func (s *PostgresStore) LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (runtime.ConversationRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return runtime.ConversationRecord{}, false, fmt.Errorf("agent_id is required")
	}
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		mode = "session"
	}
	scopeKey = strings.TrimSpace(scopeKey)
	const q = `
		SELECT
			COALESCE(task_id::text, ''),
			COALESCE(scope_key, ''),
			COALESCE(messages, '[]'::jsonb),
			COALESCE(summary, ''),
			COALESCE(turn_count, 0),
			COALESCE(status, 'active')
		FROM conversations
		WHERE agent_id = $1
		  AND mode = $2
		  AND COALESCE(scope_key, '') = $3
		  AND status = 'active'
		ORDER BY updated_at DESC
		LIMIT 1
	`
	var rec runtime.ConversationRecord
	rec.AgentID = agentID
	rec.Mode = mode

	var rawMessages []byte
	err := s.DB.QueryRowContext(ctx, q, agentID, mode, scopeKey).Scan(
		&rec.TaskID,
		&rec.ScopeKey,
		&rawMessages,
		&rec.Summary,
		&rec.TurnCount,
		&rec.Status,
	)
	if shouldFallbackConversationScope(err) {
		const legacyQ = `
			SELECT
				COALESCE(task_id::text, ''),
				COALESCE(messages, '[]'::jsonb),
				COALESCE(summary, ''),
				COALESCE(turn_count, 0),
				COALESCE(status, 'active')
			FROM conversations
			WHERE agent_id = $1
			  AND mode = $2
			  AND status = 'active'
			ORDER BY updated_at DESC
			LIMIT 1
		`
		err = s.DB.QueryRowContext(ctx, legacyQ, agentID, mode).Scan(
			&rec.TaskID,
			&rawMessages,
			&rec.Summary,
			&rec.TurnCount,
			&rec.Status,
		)
		rec.ScopeKey = ""
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ConversationRecord{}, false, nil
		}
		return runtime.ConversationRecord{}, false, fmt.Errorf("load active conversation: %w", err)
	}
	if len(rawMessages) > 0 {
		var msgs []runtime.Message
		if json.Unmarshal(rawMessages, &msgs) == nil {
			rec.Messages = msgs
		}
	}
	return rec, true, nil
}

func mergeAgentConfigJSON(cfg models.AgentConfig) ([]byte, error) {
	obj := map[string]any{}
	if len(cfg.Config) > 0 && json.Valid(cfg.Config) {
		_ = json.Unmarshal(cfg.Config, &obj)
	}
	if len(cfg.Subscriptions) > 0 {
		obj["subscriptions"] = cfg.Subscriptions
	}
	if _, ok := obj["role"]; !ok && cfg.Role != "" {
		obj["role"] = cfg.Role
	}
	if _, ok := obj["mode"]; !ok && cfg.Mode != "" {
		obj["mode"] = cfg.Mode
	}
	if len(obj) == 0 {
		obj = map[string]any{}
	}
	return json.Marshal(obj)
}

func extractSubscriptions(raw []byte) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj struct {
		Subscriptions []string `json:"subscriptions"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Subscriptions
}

func normalizeJSONPayload(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if json.Valid(raw) {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			v = redactPayloadValue("", v)
			b, err := json.Marshal(v)
			if err == nil {
				return string(b)
			}
		}
		return string(raw)
	}
	b, _ := json.Marshal(map[string]string{"raw": redactText(string(raw))})
	return string(b)
}

func matchesAnySubscription(eventType string, patterns []events.EventType) bool {
	for _, p := range patterns {
		if subscriptionMatch(string(p), eventType) {
			return true
		}
	}
	return false
}

func subscriptionMatch(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
	default:
		return pattern == eventType
	}
}

func nullable(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func sanitizeSchemaIdent(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

var (
	emailRegex = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`)
	// Match likely phone formats while avoiding ISO timestamps (e.g. 2026-02-21T02:47:05Z).
	phoneRegex      = regexp.MustCompile(`(?:\+\d[\d\s().-]{7,}\d|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b|\(\d{3}\)\s*\d{3}[-.\s]\d{4}\b)`)
	paymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)
)

func redactPayloadValue(key string, v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = redactPayloadValue(strings.ToLower(strings.TrimSpace(k)), vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = redactPayloadValue(key, t[i])
		}
		return out
	case string:
		if isNameKey(key) {
			return redactName(t)
		}
		if isPaymentKey(key) && strings.TrimSpace(t) != "" {
			return "[PAYMENT_REF]"
		}
		return redactText(t)
	default:
		return v
	}
}

func redactText(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = emailRegex.ReplaceAllString(s, "[EMAIL]")
	s = phoneRegex.ReplaceAllString(s, "[PHONE]")
	s = paymentRefRegex.ReplaceAllString(s, "[PAYMENT_REF]")
	return strings.ToValidUTF8(s, "\uFFFD")
}

func redactName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	runes := []rune(name)
	if len(runes) == 0 {
		return name
	}
	return strings.ToUpper(string(runes[0])) + "."
}

func isNameKey(k string) bool {
	switch k {
	case "name", "full_name", "customer_name", "first_name", "last_name":
		return true
	default:
		return false
	}
}

func isPaymentKey(k string) bool {
	k = strings.ToLower(strings.TrimSpace(k))
	if k == "" {
		return false
	}
	for _, needle := range []string{
		"payment", "transaction", "charge", "invoice", "billing", "checkout",
		"payment_ref", "payment_reference", "payment_id", "transaction_id",
	} {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}

func (s *PostgresStore) UpsertSchedule(ctx context.Context, sc runtime.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	if strings.TrimSpace(sc.Mode) == "" {
		sc.Mode = "once"
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schedule tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, sc.AgentID, sc.EventType); err != nil {
		return fmt.Errorf("deactivate previous schedule: %w", err)
	}

	var atTime any
	var nextFire any
	if !sc.At.IsZero() {
		atTime = sc.At
		nextFire = sc.At
	}
	payload := sc.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO schedules (
			agent_id, vertical_id, event_type, mode, cron_expr,
			at_time, next_fire_at, payload, active, created_at
		)
		VALUES (
			$1, NULLIF($2,'')::uuid, $3, $4, NULLIF($5,''),
			$6, $7, $8::jsonb, true, now()
		)
	`, sc.AgentID, sc.VerticalID, sc.EventType, sc.Mode, sc.Cron, atTime, nextFire, string(payload)); err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schedule tx: %w", err)
	}
	return nil
}

func (s *PostgresStore) CancelSchedule(ctx context.Context, agentID, eventType string) error {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" {
		return fmt.Errorf("agent_id and event_type are required")
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET active = false,
		    cancelled_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, agentID, eventType)
	if err != nil {
		return fmt.Errorf("cancel schedule: %w", err)
	}
	return nil
}

func (s *PostgresStore) LoadActiveSchedules(ctx context.Context) ([]runtime.Schedule, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			agent_id,
			event_type,
			mode,
			COALESCE(cron_expr, ''),
			at_time,
			COALESCE(vertical_id::text, ''),
			payload
		FROM schedules
		WHERE active = true
	`)
	if err != nil {
		return nil, fmt.Errorf("query active schedules: %w", err)
	}
	defer rows.Close()

	out := make([]runtime.Schedule, 0)
	for rows.Next() {
		var sc runtime.Schedule
		var at sql.NullTime
		if err := rows.Scan(
			&sc.AgentID,
			&sc.EventType,
			&sc.Mode,
			&sc.Cron,
			&at,
			&sc.VerticalID,
			&sc.Payload,
		); err != nil {
			return nil, fmt.Errorf("scan active schedule: %w", err)
		}
		if at.Valid {
			sc.At = at.Time
		}
		out = append(out, sc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active schedules: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) MarkScheduleFired(ctx context.Context, sc runtime.Schedule) error {
	if strings.TrimSpace(sc.AgentID) == "" || strings.TrimSpace(sc.EventType) == "" {
		return nil
	}
	if sc.Mode == "once" {
		_, err := s.DB.ExecContext(ctx, `
			UPDATE schedules
			SET active = false,
			    last_fired_at = now(),
			    next_fire_at = NULL
			WHERE agent_id = $1
			  AND event_type = $2
			  AND active = true
		`, sc.AgentID, sc.EventType)
		if err != nil {
			return fmt.Errorf("mark once schedule fired: %w", err)
		}
		return nil
	}
	_, err := s.DB.ExecContext(ctx, `
		UPDATE schedules
		SET last_fired_at = now()
		WHERE agent_id = $1
		  AND event_type = $2
		  AND active = true
	`, sc.AgentID, sc.EventType)
	if err != nil {
		return fmt.Errorf("mark recurring schedule fired: %w", err)
	}
	return nil
}
