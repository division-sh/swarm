package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	runtimellm "swarm/internal/runtime/llm"
	runtimesessions "swarm/internal/runtime/sessions"
)

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	if rec.AgentID == "" || rec.RuntimeMode == "" || rec.SessionID == "" {
		return fmt.Errorf("agent_id, runtime_mode, and session_id are required")
	}
	runtimeMode := runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode)

	reqPayload := normalizeJSONPayload(rec.RequestPayload)
	respPayload := normalizeJSONPayload(rec.ResponseRaw)
	toolCallsPayload := normalizeJSONArray(rec.ToolCalls)
	availableToolsPayload := normalizeJSONArray(rec.AvailableTools)
	emittedEventsPayload := normalizeJSONArray(rec.EmittedEvents)
	mcpServersPayload := normalizeJSONObject(rec.MCPServers)
	mcpToolsListedPayload := normalizeJSONArray(rec.MCPToolsListed)
	mcpToolsVisiblePayload := normalizeJSONArray(rec.MCPToolsVisible)
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}
	runID := strings.TrimSpace(rec.RunID)

	updateQ := `
		UPDATE agent_sessions
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object(
				'last_turn',
				jsonb_build_object(
					'task_id', to_jsonb(NULLIF($4, '')::text),
					'request_payload', CASE WHEN $5 = '' THEN NULL ELSE $5::jsonb END,
					'response_payload', CASE WHEN $6 = '' THEN NULL ELSE $6::jsonb END,
					'parse_ok', to_jsonb($7::boolean),
					'latency_ms', to_jsonb($8::integer),
					'retry_count', to_jsonb($9::integer),
					'error', to_jsonb(NULLIF($10, '')::text),
					'updated_at', to_jsonb(now())
				)
			),
		    updated_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3::uuid
		  AND status = 'active'
	`
	updateArgs := []any{
		rec.AgentID,
		runtimeMode,
		rec.SessionID,
		rec.TaskID,
		reqPayload,
		respPayload,
		rec.ParseOK,
		latencyMS,
		rec.RetryCount,
		rec.Error,
	}
	if columnExists(ctx, s.DB, "agent_sessions", "run_id") {
		if err := s.ensureRunRow(ctx, nil, runID); err != nil {
			return err
		}
		updateQ = `
			UPDATE agent_sessions
			SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object(
					'last_turn',
					jsonb_build_object(
						'task_id', to_jsonb(NULLIF($5, '')::text),
						'request_payload', CASE WHEN $6 = '' THEN NULL ELSE $6::jsonb END,
						'response_payload', CASE WHEN $7 = '' THEN NULL ELSE $7::jsonb END,
						'parse_ok', to_jsonb($8::boolean),
						'latency_ms', to_jsonb($9::integer),
						'retry_count', to_jsonb($10::integer),
						'error', to_jsonb(NULLIF($11, '')::text),
						'updated_at', to_jsonb(now())
					)
				),
			    run_id = COALESCE(NULLIF($4,'')::uuid, run_id),
			    updated_at = now()
			WHERE agent_id = $1
			  AND runtime_mode = $2
			  AND session_id = $3::uuid
			  AND status = 'active'
		`
		updateArgs = []any{
			rec.AgentID,
			runtimeMode,
			rec.SessionID,
			nullUUIDString(runID),
			rec.TaskID,
			reqPayload,
			respPayload,
			rec.ParseOK,
			latencyMS,
			rec.RetryCount,
			rec.Error,
		}
	}
	res, err := s.DB.ExecContext(ctx, updateQ,
		updateArgs...,
	)
	if err != nil {
		return fmt.Errorf("append agent turn: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		if runtimeMode == runtimesessions.RuntimeModeTask {
			if err := s.ensureTaskAuditSessionRow(ctx, rec); err != nil {
				return err
			}
			res, err = s.DB.ExecContext(ctx, updateQ, updateArgs...)
			if err != nil {
				return fmt.Errorf("append agent turn: %w", err)
			}
			n, _ = res.RowsAffected()
		}
		if n == 0 {
			return fmt.Errorf("no active session row found for agent=%s runtime=%s session=%s", rec.AgentID, rec.RuntimeMode, rec.SessionID)
		}
	}

	insertTurn := `
		INSERT INTO agent_turns (
			agent_id, session_id, runtime_mode, scope_key, trace_id, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, error
		) VALUES (
			$1,
			$2::uuid,
			$3,
			NULLIF($4, ''),
			NULLIF($5, ''),
			NULLIF($6, '')::uuid,
			NULLIF($7, '')::uuid,
			NULLIF($8, ''),
			NULLIF($9, ''),
			CASE WHEN $10 = '' THEN '[]'::jsonb ELSE $10::jsonb END,
			CASE WHEN $11 = '' THEN '[]'::jsonb ELSE $11::jsonb END,
			CASE WHEN $12 = '' THEN '[]'::jsonb ELSE $12::jsonb END,
			CASE WHEN $13 = '' THEN '{}'::jsonb ELSE $13::jsonb END,
			CASE WHEN $14 = '' THEN '[]'::jsonb ELSE $14::jsonb END,
			CASE WHEN $15 = '' THEN '[]'::jsonb ELSE $15::jsonb END,
			CASE WHEN $16 = '' THEN NULL ELSE $16::jsonb END,
			CASE WHEN $17 = '' THEN NULL ELSE $17::jsonb END,
			$18,
			$19,
			$20,
			NULLIF($21, '')
		)
	`
	insertArgs := []any{
		rec.AgentID,
		rec.SessionID,
		runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode),
		rec.ScopeKey,
		rec.TraceID,
		rec.EntityID,
		rec.TriggerEventID,
		rec.TriggerEventType,
		rec.TaskID,
		availableToolsPayload,
		toolCallsPayload,
		emittedEventsPayload,
		mcpServersPayload,
		mcpToolsListedPayload,
		mcpToolsVisiblePayload,
		reqPayload,
		respPayload,
		rec.ParseOK,
		latencyMS,
		rec.RetryCount,
		rec.Error,
	}
	if columnExists(ctx, s.DB, "agent_turns", "run_id") {
		if err := s.ensureRunRow(ctx, nil, runID); err != nil {
			return err
		}
		insertTurn = `
			INSERT INTO agent_turns (
				run_id, agent_id, session_id, runtime_mode, scope_key, trace_id, entity_id,
				trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
				emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
				request_payload, response_payload, parse_ok, latency_ms, retry_count, error
			) VALUES (
				NULLIF($1,'')::uuid,
				$2,
				$3::uuid,
				$4,
				NULLIF($5, ''),
				NULLIF($6, ''),
				NULLIF($7, '')::uuid,
				NULLIF($8, '')::uuid,
				NULLIF($9, ''),
				NULLIF($10, ''),
				CASE WHEN $11 = '' THEN '[]'::jsonb ELSE $11::jsonb END,
				CASE WHEN $12 = '' THEN '[]'::jsonb ELSE $12::jsonb END,
				CASE WHEN $13 = '' THEN '[]'::jsonb ELSE $13::jsonb END,
				CASE WHEN $14 = '' THEN '{}'::jsonb ELSE $14::jsonb END,
				CASE WHEN $15 = '' THEN '[]'::jsonb ELSE $15::jsonb END,
				CASE WHEN $16 = '' THEN '[]'::jsonb ELSE $16::jsonb END,
				CASE WHEN $17 = '' THEN NULL ELSE $17::jsonb END,
				CASE WHEN $18 = '' THEN NULL ELSE $18::jsonb END,
				$19,
				$20,
				$21,
				NULLIF($22, '')
			)
		`
		insertArgs = append([]any{nullUUIDString(runID)}, insertArgs...)
	}
	if _, err := s.DB.ExecContext(ctx, insertTurn, insertArgs...); err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return nil
		}
		return fmt.Errorf("insert agent turn: %w", err)
	}
	return nil
}

func (s *PostgresStore) ensureTaskAuditSessionRow(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	scopeKey := strings.TrimSpace(rec.ScopeKey)
	if scopeKey == "" {
		scopeKey = strings.TrimSpace(rec.SessionID)
	}
	q := `
		INSERT INTO agent_sessions (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES (
			$1::uuid,
			$2,
			NULLIF($3,'')::uuid,
			NULL,
			$4,
			$5,
			'[]'::jsonb,
			0,
			$6,
			'{}'::jsonb,
			'active',
			now(),
			now()
		)
		ON CONFLICT (session_id) DO UPDATE SET
			agent_id = EXCLUDED.agent_id,
			entity_id = EXCLUDED.entity_id,
			scope_key = EXCLUDED.scope_key,
			scope = EXCLUDED.scope,
			runtime_mode = EXCLUDED.runtime_mode,
			status = 'active',
			updated_at = now()
	`
	args := []any{
		rec.SessionID,
		rec.AgentID,
		rec.EntityID,
		scopeKey,
		"global",
		runtimesessions.RuntimeModeTask,
	}
	if columnExists(ctx, s.DB, "agent_sessions", "run_id") {
		if err := s.ensureRunRow(ctx, nil, rec.RunID); err != nil {
			return err
		}
		q = `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			VALUES (
				$1::uuid,
				NULLIF($2,'')::uuid,
				$3,
				NULLIF($4,'')::uuid,
				NULL,
				$5,
				$6,
				'[]'::jsonb,
				0,
				$7,
				'{}'::jsonb,
				'active',
				now(),
				now()
			)
			ON CONFLICT (session_id) DO UPDATE SET
				run_id = COALESCE(EXCLUDED.run_id, agent_sessions.run_id),
				agent_id = EXCLUDED.agent_id,
				entity_id = EXCLUDED.entity_id,
				scope_key = EXCLUDED.scope_key,
				scope = EXCLUDED.scope,
				runtime_mode = EXCLUDED.runtime_mode,
				status = 'active',
				updated_at = now()
		`
		args = []any{
			rec.SessionID,
			nullUUIDString(rec.RunID),
			rec.AgentID,
			rec.EntityID,
			scopeKey,
			"global",
			runtimesessions.RuntimeModeTask,
		}
	}
	if _, err := s.DB.ExecContext(ctx, q,
		args...,
	); err != nil {
		return fmt.Errorf("ensure task audit session row: %w", err)
	}
	return nil
}

func mustJSON(v any) []byte {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func normalizeJSONArray(v any) string {
	raw := normalizeJSONPayload(mustJSON(v))
	if raw == "" || raw == "null" {
		return "[]"
	}
	return raw
}

func normalizeJSONObject(v any) string {
	raw := normalizeJSONPayload(mustJSON(v))
	if raw == "" || raw == "null" {
		return "{}"
	}
	return raw
}

func (s *PostgresStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	if strings.TrimSpace(rec.AgentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	mode := runtimesessions.NormalizeConversationRuntimeMode(rec.Mode)
	resolved := runtimesessions.ResolveScope(mode, rec.ScopeKey)

	status := strings.TrimSpace(strings.ToLower(rec.Status))
	if status == "" {
		status = "active"
	}

	msgs := make([]runtimellm.Message, 0, len(rec.Messages))
	for _, m := range rec.Messages {
		msgs = append(msgs, runtimellm.Message{
			Role:    strings.TrimSpace(m.Role),
			Content: redactText(m.Content),
		})
	}
	msgJSON, err := json.Marshal(msgs)
	if err != nil {
		return fmt.Errorf("marshal conversation messages: %w", err)
	}
	summary := strings.ToValidUTF8(rec.Summary, "\uFFFD")
	sessionID := strings.TrimSpace(rec.SessionID)
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	runID := nullUUIDString(rec.RunID)

	if resolved.Stateless {
		scopeKey := strings.TrimSpace(rec.ScopeKey)
		if scopeKey == "" {
			scopeKey = sessionID
		}
		scope := strings.TrimSpace(resolved.Scope)
		if scope == "" {
			scope = "global"
		}
		q := `
			INSERT INTO agent_sessions (
				session_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			VALUES (
				$1::uuid,
				$2,
				NULLIF($3,'')::uuid,
				NULLIF($4,''),
				$5,
				$6,
				$7::jsonb,
				$8,
				$9,
				jsonb_build_object('summary', NULLIF($10,'')),
				$11,
				now(),
				now()
			)
			ON CONFLICT (session_id) DO UPDATE SET
				agent_id = EXCLUDED.agent_id,
				entity_id = EXCLUDED.entity_id,
				flow_instance = EXCLUDED.flow_instance,
				scope_key = EXCLUDED.scope_key,
				scope = EXCLUDED.scope,
				conversation = EXCLUDED.conversation,
				turn_count = EXCLUDED.turn_count,
				runtime_mode = EXCLUDED.runtime_mode,
				runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || jsonb_build_object('summary', NULLIF($10,'')),
				status = EXCLUDED.status,
				updated_at = now()
		`
		args := []any{
			sessionID,
			rec.AgentID,
			resolved.EntityID,
			resolved.FlowInstance,
			scopeKey,
			scope,
			string(msgJSON),
			rec.TurnCount,
			mode,
			summary,
			status,
		}
		if columnExists(ctx, s.DB, "agent_sessions", "run_id") {
			if err := s.ensureRunRow(ctx, nil, runID); err != nil {
				return err
			}
			q = `
				INSERT INTO agent_sessions (
					session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
					conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
				)
				VALUES (
					$1::uuid,
					NULLIF($2,'')::uuid,
					$3,
					NULLIF($4,'')::uuid,
					NULLIF($5,''),
					$6,
					$7,
					$8::jsonb,
					$9,
					$10,
					jsonb_build_object('summary', NULLIF($11,'')),
					$12,
					now(),
					now()
				)
				ON CONFLICT (session_id) DO UPDATE SET
					run_id = COALESCE(EXCLUDED.run_id, agent_sessions.run_id),
					agent_id = EXCLUDED.agent_id,
					entity_id = EXCLUDED.entity_id,
					flow_instance = EXCLUDED.flow_instance,
					scope_key = EXCLUDED.scope_key,
					scope = EXCLUDED.scope,
					conversation = EXCLUDED.conversation,
					turn_count = EXCLUDED.turn_count,
					runtime_mode = EXCLUDED.runtime_mode,
					runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || jsonb_build_object('summary', NULLIF($11,'')),
					status = EXCLUDED.status,
					updated_at = now()
			`
			args = []any{
				sessionID,
				runID,
				rec.AgentID,
				resolved.EntityID,
				resolved.FlowInstance,
				scopeKey,
				scope,
				string(msgJSON),
				rec.TurnCount,
				mode,
				summary,
				status,
			}
		}
		if _, err := s.DB.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("insert stateless conversation: %w", err)
		}
		return nil
	}

	q := `
		INSERT INTO agent_sessions (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES (
			COALESCE(NULLIF($11,''), gen_random_uuid()::text)::uuid,
			$1,
			NULLIF($2,'')::uuid,
			NULLIF($3,''),
			$4,
			$5,
			$6::jsonb,
			$7,
			$8,
			jsonb_build_object('summary', NULLIF($9,'')),
			$10,
			now(),
			now()
		)
		ON CONFLICT (agent_id, scope_key) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			scope = EXCLUDED.scope,
			conversation = EXCLUDED.conversation,
			turn_count = EXCLUDED.turn_count,
			runtime_mode = EXCLUDED.runtime_mode,
			runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || jsonb_build_object('summary', NULLIF($9,'')),
			status = EXCLUDED.status,
			updated_at = now()
	`
	args := []any{
		rec.AgentID,
		resolved.EntityID,
		resolved.FlowInstance,
		resolved.ScopeKey,
		resolved.Scope,
		string(msgJSON),
		rec.TurnCount,
		mode,
		summary,
		status,
		sessionID,
	}
	if columnExists(ctx, s.DB, "agent_sessions", "run_id") {
		if err := s.ensureRunRow(ctx, nil, runID); err != nil {
			return err
		}
		q = `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			VALUES (
				COALESCE(NULLIF($12,''), gen_random_uuid()::text)::uuid,
				NULLIF($11,'')::uuid,
				$1,
				NULLIF($2,'')::uuid,
				NULLIF($3,''),
				$4,
				$5,
				$6::jsonb,
				$7,
				$8,
				jsonb_build_object('summary', NULLIF($9,'')),
				$10,
				now(),
				now()
			)
			ON CONFLICT (agent_id, scope_key) DO UPDATE SET
				run_id = COALESCE(EXCLUDED.run_id, agent_sessions.run_id),
				entity_id = EXCLUDED.entity_id,
				flow_instance = EXCLUDED.flow_instance,
				scope = EXCLUDED.scope,
				conversation = EXCLUDED.conversation,
				turn_count = EXCLUDED.turn_count,
				runtime_mode = EXCLUDED.runtime_mode,
				runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || jsonb_build_object('summary', NULLIF($9,'')),
				status = EXCLUDED.status,
				updated_at = now()
		`
		args = []any{
			rec.AgentID,
			resolved.EntityID,
			resolved.FlowInstance,
			resolved.ScopeKey,
			resolved.Scope,
			string(msgJSON),
			rec.TurnCount,
			mode,
			summary,
			status,
			runID,
			sessionID,
		}
	}
	if _, err := s.DB.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("upsert conversation: %w", err)
	}
	return nil
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func (s *PostgresStore) LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (runtimellm.ConversationRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("agent_id is required")
	}

	mode = runtimesessions.NormalizeConversationRuntimeMode(mode)
	resolved := runtimesessions.ResolveScope(mode, scopeKey)
	if resolved.Stateless {
		return runtimellm.ConversationRecord{}, false, nil
	}

	const q = `
		SELECT
			scope_key,
			COALESCE(conversation, '[]'::jsonb),
			COALESCE(runtime_state->>'summary', ''),
			COALESCE(turn_count, 0),
			COALESCE(status, 'active')
		FROM agent_sessions
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND scope_key = $3
		  AND status = 'active'
		ORDER BY updated_at DESC
		LIMIT 1
	`

	var rec runtimellm.ConversationRecord
	rec.AgentID = agentID
	rec.Mode = mode

	var rawMessages []byte
	err := s.DB.QueryRowContext(ctx, q, agentID, mode, resolved.ScopeKey).Scan(
		&rec.ScopeKey,
		&rawMessages,
		&rec.Summary,
		&rec.TurnCount,
		&rec.Status,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimellm.ConversationRecord{}, false, nil
		}
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load active conversation: %w", err)
	}
	if len(rawMessages) > 0 {
		var msgs []runtimellm.Message
		if json.Unmarshal(rawMessages, &msgs) == nil {
			rec.Messages = msgs
		}
	}
	return rec, true, nil
}
