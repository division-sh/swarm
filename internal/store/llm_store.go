package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimellm "empireai/internal/runtime/llm"
)

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
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

func (s *PostgresStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
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

func (s *PostgresStore) LoadActiveConversation(ctx context.Context, agentID, mode, scopeKey string) (runtimellm.ConversationRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("agent_id is required")
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
	var rec runtimellm.ConversationRecord
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
