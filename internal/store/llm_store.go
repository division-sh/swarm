package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if rec.AgentID == "" || rec.RuntimeMode == "" || rec.SessionID == "" {
		return fmt.Errorf("agent_id, runtime_mode, and session_id are required")
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	runtimeMode := runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode)
	targetTable := "agent_sessions"
	hasConversationRunID := caps.Conversations.SessionRunID
	if runtimeMode.IsStateless() {
		if caps.Conversations.Audits != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
		}
		targetTable = "agent_conversation_audits"
		hasConversationRunID = caps.Conversations.AuditRunID
	} else {
		if caps.Conversations.Sessions != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
		}
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("append agent turn begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	reqPayload := normalizeJSONPayload(rec.RequestPayload)
	respPayload := normalizeJSONPayload(rec.ResponseRaw)
	toolCallsPayload := normalizeJSONArray(rec.ToolCalls)
	availableToolsPayload := normalizeJSONArray(rec.AvailableTools)
	emittedEventsPayload := normalizeJSONArray(rec.EmittedEvents)
	mcpServersPayload := normalizeJSONObject(rec.MCPServers)
	mcpToolsListedPayload := normalizeJSONArray(rec.MCPToolsListed)
	mcpToolsVisiblePayload := normalizeJSONArray(rec.MCPToolsVisible)
	failurePayload := ""
	if encodedFailure, err := encodeStoredFailure(rec.Failure); err != nil {
		return fmt.Errorf("encode agent turn failure: %w", err)
	} else if encodedFailure != nil {
		failurePayload = encodedFailure.(string)
	}
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}
	runID := strings.TrimSpace(rec.RunID)

	updateQ := fmt.Sprintf(`
		UPDATE %s
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object(
				'last_turn',
				jsonb_build_object(
					'task_id', to_jsonb(NULLIF($4, '')::text),
					'request_payload', CASE WHEN $5 = '' THEN NULL ELSE $5::jsonb END,
					'response_payload', CASE WHEN $6 = '' THEN NULL ELSE $6::jsonb END,
					'parse_ok', to_jsonb($7::boolean),
					'latency_ms', to_jsonb($8::integer),
					'retry_count', to_jsonb($9::integer),
					'failure', CASE WHEN $10 = '' THEN NULL ELSE $10::jsonb END,
					'updated_at', to_jsonb(now())
				)
			),
		    updated_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3::uuid
		  AND status = 'active'
	`, targetTable)
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
		failurePayload,
	}
	if hasConversationRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID, "", "", true); err != nil {
			return err
		}
		updateQ = fmt.Sprintf(`
			UPDATE %s
			SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object(
					'last_turn',
					jsonb_build_object(
						'task_id', to_jsonb(NULLIF($5, '')::text),
						'request_payload', CASE WHEN $6 = '' THEN NULL ELSE $6::jsonb END,
						'response_payload', CASE WHEN $7 = '' THEN NULL ELSE $7::jsonb END,
						'parse_ok', to_jsonb($8::boolean),
						'latency_ms', to_jsonb($9::integer),
						'retry_count', to_jsonb($10::integer),
						'failure', CASE WHEN $11 = '' THEN NULL ELSE $11::jsonb END,
						'updated_at', to_jsonb(now())
					)
				),
			    run_id = COALESCE(NULLIF($4,'')::uuid, run_id),
			    updated_at = now()
			WHERE agent_id = $1
			  AND runtime_mode = $2
			  AND session_id = $3::uuid
			  AND status = 'active'
		`, targetTable)
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
			failurePayload,
		}
	}
	if !runtimeMode.IsStateless() {
		updateQ = `
			UPDATE agent_sessions
			SET updated_at = now()
			WHERE agent_id = $1
			  AND runtime_mode = $2
			  AND session_id = $3::uuid
			  AND status = 'active'
		`
		updateArgs = []any{
			rec.AgentID,
			runtimeMode,
			rec.SessionID,
		}
		if hasConversationRunID {
			if err := s.ensureRunRow(ctx, caps, tx, runID, "", "", true); err != nil {
				return err
			}
			updateQ = `
				UPDATE agent_sessions
				SET run_id = COALESCE(NULLIF($4,'')::uuid, run_id),
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
			}
		}
	}
	res, err := tx.ExecContext(ctx, updateQ,
		updateArgs...,
	)
	if err != nil {
		return fmt.Errorf("append agent turn: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		if runtimeMode == runtimesessions.RuntimeModeTask {
			if err := s.ensureTaskConversationAuditRowTx(ctx, tx, caps, rec); err != nil {
				return err
			}
			res, err = tx.ExecContext(ctx, updateQ, updateArgs...)
			if err != nil {
				return fmt.Errorf("append agent turn: %w", err)
			}
			n, _ = res.RowsAffected()
		}
		if n == 0 {
			return fmt.Errorf("no persisted conversation row found for agent=%s runtime=%s session=%s", rec.AgentID, rec.RuntimeMode, rec.SessionID)
		}
	}

	hasRunID := caps.Conversations.TurnRunID
	hasTurnBlocks := caps.Conversations.TurnBlocks
	turnBlocksPayload := ""
	if hasTurnBlocks {
		rec = runtimellm.CanonicalizeTurnForPersistence(rec)
		if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
			return fmt.Errorf("validate canonical runtime_log turn_blocks: %w", err)
		}
		turnBlocksPayload = normalizeJSONArray(rec.TurnBlocks)
	}

	insertTurn := `
		INSERT INTO agent_turns (
			agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
			emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
			request_payload, response_payload, parse_ok, latency_ms, retry_count, failure
		) VALUES (
			$1,
			$2::uuid,
			$3,
			NULLIF($4, ''),
			NULLIF($5, '')::uuid,
			NULLIF($6, '')::uuid,
			NULLIF($7, ''),
			NULLIF($8, ''),
			CASE WHEN $9 = '' THEN '[]'::jsonb ELSE $9::jsonb END,
			CASE WHEN $10 = '' THEN '[]'::jsonb ELSE $10::jsonb END,
			CASE WHEN $11 = '' THEN '[]'::jsonb ELSE $11::jsonb END,
			CASE WHEN $12 = '' THEN '{}'::jsonb ELSE $12::jsonb END,
			CASE WHEN $13 = '' THEN '[]'::jsonb ELSE $13::jsonb END,
			CASE WHEN $14 = '' THEN '[]'::jsonb ELSE $14::jsonb END,
			CASE WHEN $15 = '' THEN NULL ELSE $15::jsonb END,
			CASE WHEN $16 = '' THEN NULL ELSE $16::jsonb END,
			$17,
			$18,
			$19,
			CASE WHEN $20 = '' THEN NULL ELSE $20::jsonb END
		)
	`
	insertArgs := []any{
		rec.AgentID,
		rec.SessionID,
		runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode).String(),
		rec.ScopeKey,
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
		failurePayload,
	}
	if hasTurnBlocks {
		insertTurn = `
			INSERT INTO agent_turns (
				agent_id, session_id, runtime_mode, scope_key, entity_id,
				trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
				emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
				request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure
			) VALUES (
				$1,
				$2::uuid,
				$3,
				NULLIF($4, ''),
				NULLIF($5, '')::uuid,
				NULLIF($6, '')::uuid,
				NULLIF($7, ''),
				NULLIF($8, ''),
				CASE WHEN $9 = '' THEN '[]'::jsonb ELSE $9::jsonb END,
				CASE WHEN $10 = '' THEN '[]'::jsonb ELSE $10::jsonb END,
				CASE WHEN $11 = '' THEN '[]'::jsonb ELSE $11::jsonb END,
				CASE WHEN $12 = '' THEN '{}'::jsonb ELSE $12::jsonb END,
				CASE WHEN $13 = '' THEN '[]'::jsonb ELSE $13::jsonb END,
				CASE WHEN $14 = '' THEN '[]'::jsonb ELSE $14::jsonb END,
				CASE WHEN $15 = '' THEN NULL ELSE $15::jsonb END,
				CASE WHEN $16 = '' THEN NULL ELSE $16::jsonb END,
				CASE WHEN $17 = '' THEN '[]'::jsonb ELSE $17::jsonb END,
				$18,
				$19,
				$20,
				CASE WHEN $21 = '' THEN NULL ELSE $21::jsonb END
			)
		`
		insertArgs = []any{
			rec.AgentID,
			rec.SessionID,
			runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode).String(),
			rec.ScopeKey,
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
			turnBlocksPayload,
			rec.ParseOK,
			latencyMS,
			rec.RetryCount,
			failurePayload,
		}
	}
	if hasRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID, "", "", true); err != nil {
			return err
		}
		insertTurn = `
			INSERT INTO agent_turns (
				run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
				trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
				emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
				request_payload, response_payload, parse_ok, latency_ms, retry_count, failure
			) VALUES (
				NULLIF($1,'')::uuid,
				$2,
				$3::uuid,
				$4,
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
				CASE WHEN $21 = '' THEN NULL ELSE $21::jsonb END
			)
		`
		insertArgs = []any{
			nullUUIDString(runID),
			rec.AgentID,
			rec.SessionID,
			runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode).String(),
			rec.ScopeKey,
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
			failurePayload,
		}
		if hasTurnBlocks {
			insertTurn = `
				INSERT INTO agent_turns (
					run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
					trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
					emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
					request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure
				) VALUES (
					NULLIF($1,'')::uuid,
					$2,
					$3::uuid,
					$4,
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
					CASE WHEN $18 = '' THEN '[]'::jsonb ELSE $18::jsonb END,
					$19,
					$20,
					$21,
					CASE WHEN $22 = '' THEN NULL ELSE $22::jsonb END
				)
			`
			insertArgs = []any{
				nullUUIDString(runID),
				rec.AgentID,
				rec.SessionID,
				runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode).String(),
				rec.ScopeKey,
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
				turnBlocksPayload,
				rec.ParseOK,
				latencyMS,
				rec.RetryCount,
				failurePayload,
			}
		}
	}
	if _, err := tx.ExecContext(ctx, insertTurn, insertArgs...); err != nil {
		return fmt.Errorf("insert agent turn: %w", err)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("append agent turn commit: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) ensureTaskConversationAuditRowTx(ctx context.Context, tx *sql.Tx, caps StoreSchemaCapabilities, rec runtimellm.AgentTurnRecord) error {
	if tx == nil {
		return fmt.Errorf("task conversation audit persistence requires an existing transaction")
	}
	if caps.Conversations.Audits == "" {
		var err error
		caps, err = s.schemaCapabilities(ctx)
		if err != nil {
			return err
		}
	}
	if caps.Conversations.Audits != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
	}
	identity := taskAuditIdentityFromTurn(rec)
	if caps.Conversations.AuditRunID {
		if err := s.ensureRunRow(ctx, caps, tx, rec.RunID, "", "", true); err != nil {
			return err
		}
	}
	q := `
		INSERT INTO agent_conversation_audits (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES (
			$1::uuid,
			$2,
			NULLIF($3,'')::uuid,
			NULLIF($4,''),
			NULLIF($5, ''),
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
			entity_id = COALESCE(EXCLUDED.entity_id, agent_conversation_audits.entity_id),
			flow_instance = COALESCE(EXCLUDED.flow_instance, agent_conversation_audits.flow_instance),
			scope_key = CASE
				WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope_key
				WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
				ELSE COALESCE(EXCLUDED.scope_key, agent_conversation_audits.scope_key)
			END,
			scope = CASE
				WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope
				WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
				ELSE EXCLUDED.scope
			END,
			status = 'active',
			updated_at = now()
	`
	args := []any{
		rec.SessionID,
		rec.AgentID,
		identity.EntityID,
		identity.FlowInstance,
		identity.ScopeKey,
		identity.Scope,
		runtimesessions.RuntimeModeTask.String(),
	}
	if caps.Conversations.AuditRunID {
		q = `
			INSERT INTO agent_conversation_audits (
				session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			VALUES (
				$1::uuid,
				NULLIF($2,'')::uuid,
				$3,
				NULLIF($4,'')::uuid,
				NULLIF($5,''),
				NULLIF($6, ''),
				$7,
				'[]'::jsonb,
				0,
				$8,
				'{}'::jsonb,
				'active',
				now(),
				now()
			)
			ON CONFLICT (session_id) DO UPDATE SET
				run_id = COALESCE(agent_conversation_audits.run_id, EXCLUDED.run_id),
				entity_id = COALESCE(EXCLUDED.entity_id, agent_conversation_audits.entity_id),
				flow_instance = COALESCE(EXCLUDED.flow_instance, agent_conversation_audits.flow_instance),
				scope_key = CASE
					WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope_key
					WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
					ELSE COALESCE(EXCLUDED.scope_key, agent_conversation_audits.scope_key)
				END,
				scope = CASE
					WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope
					WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
					ELSE EXCLUDED.scope
				END,
				status = 'active',
				updated_at = now()
		`
		args = []any{
			rec.SessionID,
			nullUUIDString(rec.RunID),
			rec.AgentID,
			identity.EntityID,
			identity.FlowInstance,
			identity.ScopeKey,
			identity.Scope,
			runtimesessions.RuntimeModeTask.String(),
		}
	}
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("ensure task conversation audit row: %w", err)
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
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	mode, err := runtimesessions.ParseConversationRuntimeMode(rec.Mode)
	if err != nil {
		return err
	}
	resolved, err := runtimesessions.ResolveScope(ctx, mode, runtimesessions.NormalizeSessionScope(rec.SessionScope), rec.ScopeKey)
	if err != nil {
		return err
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
	runtimeStatePatch, err := marshalConversationRuntimeStatePatch(&summary, rec.Watchdog)
	if err != nil {
		return fmt.Errorf("marshal conversation runtime_state patch: %w", err)
	}
	sessionID := strings.TrimSpace(rec.SessionID)
	runID := nullUUIDString(rec.RunID)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("upsert conversation begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if resolved.Stateless {
		if sessionID == "" {
			sessionID = uuid.NewString()
		}
		if caps.Conversations.Audits != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
		}
		identity := taskAuditIdentityFromConversation(rec)
		q := `
			INSERT INTO agent_conversation_audits (
				session_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
			)
			VALUES (
				$1::uuid,
			$2,
			NULLIF($3,'')::uuid,
			NULLIF($4,''),
			NULLIF($5, ''),
			$6,
			$7::jsonb,
			$8,
				$9,
				$10::jsonb,
				$11,
				now(),
				now()
			)
			ON CONFLICT (session_id) DO UPDATE SET
				agent_id = EXCLUDED.agent_id,
				entity_id = COALESCE(EXCLUDED.entity_id, agent_conversation_audits.entity_id),
				flow_instance = COALESCE(EXCLUDED.flow_instance, agent_conversation_audits.flow_instance),
				scope_key = CASE
					WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope_key
					WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
					ELSE COALESCE(EXCLUDED.scope_key, agent_conversation_audits.scope_key)
				END,
				scope = CASE
					WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope
					WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
					ELSE EXCLUDED.scope
				END,
				conversation = EXCLUDED.conversation,
				turn_count = EXCLUDED.turn_count,
				runtime_mode = EXCLUDED.runtime_mode,
				runtime_state = COALESCE(agent_conversation_audits.runtime_state, '{}'::jsonb) || EXCLUDED.runtime_state,
				status = EXCLUDED.status,
				updated_at = now()
		`
		args := []any{
			sessionID,
			rec.AgentID,
			identity.EntityID,
			identity.FlowInstance,
			identity.ScopeKey,
			identity.Scope,
			string(msgJSON),
			rec.TurnCount,
			mode.String(),
			runtimeStatePatch,
			status,
		}
		if caps.Conversations.AuditRunID {
			if err := s.ensureRunRow(ctx, caps, tx, runID, "", "", true); err != nil {
				return err
			}
			q = `
				INSERT INTO agent_conversation_audits (
					session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
					conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
				)
				VALUES (
					$1::uuid,
					NULLIF($2,'')::uuid,
					$3,
					NULLIF($4,'')::uuid,
					NULLIF($5,''),
					NULLIF($6, ''),
					$7,
					$8::jsonb,
					$9,
					$10,
					$11::jsonb,
					$12,
					now(),
					now()
				)
				ON CONFLICT (session_id) DO UPDATE SET
					run_id = COALESCE(EXCLUDED.run_id, agent_conversation_audits.run_id),
					agent_id = EXCLUDED.agent_id,
					entity_id = COALESCE(EXCLUDED.entity_id, agent_conversation_audits.entity_id),
					flow_instance = COALESCE(EXCLUDED.flow_instance, agent_conversation_audits.flow_instance),
					scope_key = CASE
						WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope_key
						WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
						ELSE COALESCE(EXCLUDED.scope_key, agent_conversation_audits.scope_key)
					END,
					scope = CASE
						WHEN EXCLUDED.entity_id IS NOT NULL OR EXCLUDED.flow_instance IS NOT NULL THEN EXCLUDED.scope
						WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
						ELSE EXCLUDED.scope
					END,
					conversation = EXCLUDED.conversation,
					turn_count = EXCLUDED.turn_count,
					runtime_mode = EXCLUDED.runtime_mode,
					runtime_state = COALESCE(agent_conversation_audits.runtime_state, '{}'::jsonb) || EXCLUDED.runtime_state,
					status = EXCLUDED.status,
					updated_at = now()
			`
			args = []any{
				sessionID,
				runID,
				rec.AgentID,
				identity.EntityID,
				identity.FlowInstance,
				identity.ScopeKey,
				identity.Scope,
				string(msgJSON),
				rec.TurnCount,
				mode.String(),
				runtimeStatePatch,
				status,
			}
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("insert stateless conversation: %w", err)
		}
		if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
			return fmt.Errorf("upsert stateless conversation commit: %w", err)
		}
		return nil
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if sessionID == "" {
		return fmt.Errorf("session_id is required for live session conversation persistence")
	}
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, rec.AgentID, "upsert_conversation", false); err != nil {
		return err
	}

	q := `
		UPDATE agent_sessions
		SET conversation = $1::jsonb,
			turn_count = $2,
			runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || $3::jsonb,
			updated_at = now()
		WHERE session_id = $4::uuid
		  AND agent_id = $5
		  AND scope_key = $6
		  AND runtime_mode = $7
		  AND status = 'active'
	`
	args := []any{
		string(msgJSON),
		rec.TurnCount,
		runtimeStatePatch,
		sessionID,
		rec.AgentID,
		resolved.ScopeKey,
		mode.String(),
	}
	if caps.Conversations.SessionRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID, "", "", true); err != nil {
			return err
		}
		q = `
			UPDATE agent_sessions
			SET conversation = $1::jsonb,
				turn_count = $2,
				runtime_state = COALESCE(agent_sessions.runtime_state, '{}'::jsonb) || $3::jsonb,
				run_id = COALESCE(NULLIF($4,'')::uuid, run_id),
				updated_at = now()
			WHERE session_id = $5::uuid
			  AND agent_id = $6
			  AND scope_key = $7
			  AND runtime_mode = $8
			  AND status = 'active'
		`
		args = []any{
			string(msgJSON),
			rec.TurnCount,
			runtimeStatePatch,
			runID,
			sessionID,
			rec.AgentID,
			resolved.ScopeKey,
			mode.String(),
		}
	}
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("update live conversation: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("no active live session row found for agent=%s session=%s runtime=%s scope=%s", rec.AgentID, sessionID, mode.String(), resolved.ScopeKey)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("upsert live conversation commit: %w", err)
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

func marshalConversationRuntimeStatePatch(summary *string, watchdog *runtimellm.ConversationWatchdog) (string, error) {
	payload := map[string]any{}
	if summary != nil && strings.TrimSpace(*summary) != "" {
		payload["summary"] = *summary
	}
	if watchdog != nil {
		descriptor := conversationRuntimeWatchdogDescriptorFromRuntime(watchdog)
		if err := ValidateConversationRuntimeWatchdogDescriptor(descriptor); err != nil {
			return "", err
		}
		payload["watchdog"] = map[string]any{
			"state":          descriptor.State,
			"blocking_layer": descriptor.BlockingLayer,
			"action":         descriptor.Action,
			"outcome":        descriptor.Outcome,
			"last_output_at": descriptor.LastOutputAt,
			"recorded_at":    descriptor.RecordedAt,
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func conversationRuntimeWatchdogDescriptorFromRuntime(watchdog *runtimellm.ConversationWatchdog) ConversationRuntimeWatchdogDescriptor {
	if watchdog == nil {
		return ConversationRuntimeWatchdogDescriptor{}
	}
	return ConversationRuntimeWatchdogDescriptor{
		State:         strings.TrimSpace(watchdog.State),
		BlockingLayer: strings.TrimSpace(watchdog.BlockingLayer),
		Action:        strings.TrimSpace(watchdog.Action),
		Outcome:       strings.TrimSpace(watchdog.Outcome),
		LastOutputAt:  strings.TrimSpace(watchdog.LastOutputAt),
		RecordedAt:    strings.TrimSpace(watchdog.RecordedAt),
	}
}

func (s *PostgresStore) LoadActiveConversation(ctx context.Context, agentID, mode, sessionScope, scopeKey string) (runtimellm.ConversationRecord, bool, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("agent_id is required")
	}

	resolvedMode, err := runtimesessions.ParseConversationRuntimeMode(mode)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	resolved, err := runtimesessions.ResolveScope(ctx, resolvedMode, runtimesessions.NormalizeSessionScope(sessionScope), scopeKey)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	if resolved.Stateless {
		return runtimellm.ConversationRecord{}, false, nil
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return runtimellm.ConversationRecord{}, false, unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}

	const q = `
		SELECT
			session_id::text,
			scope_key,
			COALESCE(conversation, '[]'::jsonb),
			COALESCE(runtime_state, '{}'::jsonb),
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
	rec.Mode = resolvedMode.String()
	rec.SessionScope = resolved.Scope.String()

	var rawMessages, runtimeStateRaw []byte
	err = s.DB.QueryRowContext(ctx, q, agentID, resolvedMode.String(), resolved.ScopeKey).Scan(
		&rec.SessionID,
		&rec.ScopeKey,
		&rawMessages,
		&runtimeStateRaw,
		&rec.TurnCount,
		&rec.Status,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimellm.ConversationRecord{}, false, nil
		}
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load active conversation: %w", err)
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	rec.Summary = runtimeState.Summary
	rec.RetryReason = runtimeState.RetryReason
	rec.RetriesFromSessionID = runtimeState.RetriesFromSessionID
	if runtimeState.Watchdog != nil {
		rec.Watchdog = &runtimellm.ConversationWatchdog{
			State:         runtimeState.Watchdog.State,
			BlockingLayer: runtimeState.Watchdog.BlockingLayer,
			Action:        runtimeState.Watchdog.Action,
			Outcome:       runtimeState.Watchdog.Outcome,
			LastOutputAt:  runtimeState.Watchdog.LastOutputAt,
			RecordedAt:    runtimeState.Watchdog.RecordedAt,
		}
	}
	if len(rawMessages) > 0 {
		var msgs []runtimellm.Message
		if json.Unmarshal(rawMessages, &msgs) == nil {
			rec.Messages = msgs
		}
	}
	return rec, true, nil
}

func (s *PostgresStore) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
	agentID := strings.TrimSpace(update.AgentID)
	if agentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	sessionID := strings.TrimSpace(update.SessionID)
	if sessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	mode, err := runtimesessions.ParseConversationRuntimeMode(update.Mode)
	if err != nil {
		return err
	}
	resolved, err := runtimesessions.ResolveScope(ctx, mode, runtimesessions.NormalizeSessionScope(update.SessionScope), update.ScopeKey)
	if err != nil {
		return err
	}
	if resolved.Stateless {
		return fmt.Errorf("live session watchdog updates require session runtime mode")
	}
	if update.Watchdog == nil {
		return fmt.Errorf("watchdog is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	patch, err := marshalConversationRuntimeStatePatch(nil, update.Watchdog)
	if err != nil {
		return fmt.Errorf("marshal live session watchdog patch: %w", err)
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin live session watchdog projection: %w", err)
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, agentID, "update_watchdog", false); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || $1::jsonb,
		    updated_at = now()
		WHERE session_id = $2::uuid
		  AND agent_id = $3
		  AND scope_key = $4
		  AND runtime_mode = $5
		  AND status = 'active'
	`, patch, sessionID, agentID, resolved.ScopeKey, mode.String())
	if err != nil {
		return fmt.Errorf("update live session watchdog: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("no active live session row found for agent=%s session=%s runtime=%s scope=%s", agentID, sessionID, mode.String(), resolved.ScopeKey)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("update live session watchdog commit: %w", err)
	}
	return nil
}
