package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimellm "swarm/internal/runtime/llm"
	runtimesessions "swarm/internal/runtime/sessions"
)

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	if rec.AgentID == "" || rec.RuntimeMode == "" || rec.SessionID == "" {
		return fmt.Errorf("agent_id, runtime_mode, and session_id are required")
	}
	if runtimesessions.ResolveScope(rec.RuntimeMode, "").Stateless {
		return nil
	}

	reqPayload := normalizeJSONPayload(rec.RequestPayload)
	respPayload := normalizeJSONPayload(rec.ResponseRaw)
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}

	const q = `
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
	res, err := s.DB.ExecContext(ctx, q,
		rec.AgentID,
		runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode),
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

func (s *PostgresStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	if strings.TrimSpace(rec.AgentID) == "" {
		return fmt.Errorf("agent_id is required")
	}
	mode := runtimesessions.NormalizeConversationRuntimeMode(rec.Mode)
	resolved := runtimesessions.ResolveScope(mode, rec.ScopeKey)
	if resolved.Stateless {
		return nil
	}

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

	const q = `
		INSERT INTO agent_sessions (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES (
			gen_random_uuid(),
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
	if _, err := s.DB.ExecContext(ctx, q,
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
	); err != nil {
		return fmt.Errorf("upsert conversation: %w", err)
	}
	return nil
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
