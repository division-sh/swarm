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

func (s *SQLiteRuntimeStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	if strings.TrimSpace(rec.AgentID) == "" || strings.TrimSpace(rec.RuntimeMode) == "" || strings.TrimSpace(rec.SessionID) == "" {
		return fmt.Errorf("agent_id, runtime_mode, and session_id are required")
	}
	mode := runtimesessions.NormalizeConversationRuntimeMode(rec.RuntimeMode)
	if mode == "" {
		return fmt.Errorf("runtime_mode is required")
	}
	if rec.Latency < 0 {
		rec.Latency = 0
	}
	if rec.RetryCount < 0 {
		rec.RetryCount = 0
	}
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
		return fmt.Errorf("encode sqlite agent turn failure: %w", err)
	} else if encodedFailure != nil {
		failurePayload = encodedFailure.(string)
	}
	rec = runtimellm.CanonicalizeTurnForPersistence(rec)
	if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
		return fmt.Errorf("validate canonical runtime_log turn_blocks: %w", err)
	}
	turnBlocksPayload := normalizeJSONArray(rec.TurnBlocks)
	latencyMS := int(rec.Latency / time.Millisecond)
	runID := nullUUIDString(rec.RunID)
	now := s.now()

	return s.runRuntimeMutation(ctx, "sqlite append agent turn", func(txctx context.Context, tx *sql.Tx) error {
		if err := sqliteEnsureRunRow(txctx, tx, runID, rec.TriggerEventID, rec.TriggerEventType, now); err != nil {
			return err
		}
		if mode.IsStateless() {
			if err := s.ensureSQLiteTaskConversationAuditRowTx(txctx, tx, rec, now); err != nil {
				return err
			}
		} else {
			res, err := tx.ExecContext(txctx, `
				UPDATE agent_sessions
				SET run_id = COALESCE(?, run_id),
				    updated_at = ?
				WHERE agent_id = ?
				  AND runtime_mode = ?
				  AND session_id = ?
				  AND status = 'active'
			`, sqliteNullUUID(runID), now, strings.TrimSpace(rec.AgentID), mode.String(), strings.TrimSpace(rec.SessionID))
			if err != nil {
				return fmt.Errorf("append sqlite agent turn update session: %w", err)
			}
			if rows, _ := res.RowsAffected(); rows == 0 {
				return fmt.Errorf("no persisted conversation row found for agent=%s runtime=%s session=%s", rec.AgentID, mode.String(), rec.SessionID)
			}
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO agent_turns (
				turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
				trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls,
				emitted_events, mcp_servers, mcp_tools_listed, mcp_tools_visible,
				request_payload, response_payload, turn_blocks, parse_ok, latency_ms, retry_count, failure, created_at
			) VALUES (
				?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, ?,
				?, ?, ?, ?,
				?, ?, ?, ?, ?, ?, ?, ?
			)
		`, uuid.NewString(), sqliteNullUUID(runID), strings.TrimSpace(rec.AgentID), strings.TrimSpace(rec.SessionID),
			mode.String(), sqliteNullString(rec.ScopeKey), sqliteNullUUID(rec.EntityID), sqliteNullUUID(rec.TriggerEventID),
			sqliteNullString(rec.TriggerEventType), sqliteNullString(rec.TaskID), availableToolsPayload, toolCallsPayload,
			emittedEventsPayload, mcpServersPayload, mcpToolsListedPayload, mcpToolsVisiblePayload,
			sqliteNullString(reqPayload), sqliteNullString(respPayload), turnBlocksPayload, rec.ParseOK, latencyMS,
			rec.RetryCount, sqliteNullString(failurePayload), now)
		if err != nil {
			return fmt.Errorf("insert sqlite agent turn: %w", err)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	agentID := strings.TrimSpace(rec.AgentID)
	if agentID == "" {
		return fmt.Errorf("agent_id is required")
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
	msgJSON, runtimeStatePatch, err := sqliteConversationPayloads(rec)
	if err != nil {
		return err
	}
	sessionID := strings.TrimSpace(rec.SessionID)
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	runID := nullUUIDString(rec.RunID)
	now := s.now()

	return s.runRuntimeMutation(ctx, "sqlite conversation upsert", func(txctx context.Context, tx *sql.Tx) error {
		if err := sqliteEnsureRunRow(txctx, tx, runID, "", "", now); err != nil {
			return err
		}
		if resolved.Stateless {
			identity := taskAuditIdentityFromConversation(rec)
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO agent_conversation_audits (
					session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
					conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
				) VALUES (
					?, ?, ?, ?, ?, ?, ?,
					?, ?, ?, ?, ?, ?, ?
				)
				ON CONFLICT(session_id) DO UPDATE SET
					run_id = COALESCE(excluded.run_id, agent_conversation_audits.run_id),
					agent_id = excluded.agent_id,
					entity_id = COALESCE(excluded.entity_id, agent_conversation_audits.entity_id),
					flow_instance = COALESCE(excluded.flow_instance, agent_conversation_audits.flow_instance),
					scope_key = CASE
						WHEN excluded.entity_id IS NOT NULL OR excluded.flow_instance IS NOT NULL THEN excluded.scope_key
						WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
						ELSE COALESCE(excluded.scope_key, agent_conversation_audits.scope_key)
					END,
					scope = CASE
						WHEN excluded.entity_id IS NOT NULL OR excluded.flow_instance IS NOT NULL THEN excluded.scope
						WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
						ELSE excluded.scope
					END,
					conversation = excluded.conversation,
					turn_count = excluded.turn_count,
					runtime_mode = excluded.runtime_mode,
					runtime_state = json_patch(COALESCE(agent_conversation_audits.runtime_state, '{}'), excluded.runtime_state),
					status = excluded.status,
					updated_at = excluded.updated_at
			`, sessionID, sqliteNullUUID(runID), agentID, sqliteNullUUID(identity.EntityID), sqliteNullString(identity.FlowInstance),
				sqliteNullString(identity.ScopeKey), identity.Scope, string(msgJSON), rec.TurnCount, mode.String(),
				runtimeStatePatch, status, now, now); err != nil {
				return fmt.Errorf("upsert sqlite task conversation audit: %w", err)
			}
			return nil
		}
		if strings.TrimSpace(rec.SessionID) == "" {
			return fmt.Errorf("session_id is required for live session conversation persistence")
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET conversation = ?,
			    turn_count = ?,
			    runtime_state = json_patch(COALESCE(runtime_state, '{}'), ?),
			    run_id = COALESCE(?, run_id),
			    updated_at = ?
			WHERE session_id = ?
			  AND agent_id = ?
			  AND scope_key = ?
			  AND runtime_mode = ?
			  AND status = 'active'
		`, string(msgJSON), rec.TurnCount, runtimeStatePatch, sqliteNullUUID(runID), now,
			strings.TrimSpace(rec.SessionID), agentID, resolved.ScopeKey, mode.String())
		if err != nil {
			return fmt.Errorf("update sqlite live conversation: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("no active live session row found for agent=%s session=%s runtime=%s scope=%s", agentID, rec.SessionID, mode.String(), resolved.ScopeKey)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) LoadActiveConversation(ctx context.Context, agentID, mode, sessionScope, scopeKey string) (runtimellm.ConversationRecord, bool, error) {
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
	var rec runtimellm.ConversationRecord
	rec.AgentID = agentID
	rec.Mode = resolvedMode.String()
	rec.SessionScope = resolved.Scope.String()
	var rawMessages, runtimeStateRaw any
	err = s.DB.QueryRowContext(ctx, `
		SELECT session_id, scope_key, COALESCE(conversation, '[]'), COALESCE(runtime_state, '{}'), COALESCE(turn_count, 0), COALESCE(status, 'active')
		FROM agent_sessions
		WHERE agent_id = ?
		  AND runtime_mode = ?
		  AND scope_key = ?
		  AND status = 'active'
		ORDER BY updated_at DESC
		LIMIT 1
	`, agentID, resolvedMode.String(), resolved.ScopeKey).Scan(&rec.SessionID, &rec.ScopeKey, &rawMessages, &runtimeStateRaw, &rec.TurnCount, &rec.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ConversationRecord{}, false, nil
	}
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load sqlite active conversation: %w", err)
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(sqliteJSONRawMessage(runtimeStateRaw))
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("decode sqlite conversation runtime_state: %w", err)
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
	if raw := sqliteJSONRawMessage(rawMessages); len(raw) > 0 {
		var msgs []runtimellm.Message
		if json.Unmarshal(raw, &msgs) == nil {
			rec.Messages = msgs
		}
	}
	return rec, true, nil
}

func (s *SQLiteRuntimeStore) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
	agentID := strings.TrimSpace(update.AgentID)
	sessionID := strings.TrimSpace(update.SessionID)
	if agentID == "" || sessionID == "" {
		return fmt.Errorf("agent_id and session_id are required")
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
	patch, err := marshalConversationRuntimeStatePatch(nil, update.Watchdog)
	if err != nil {
		return fmt.Errorf("marshal sqlite live session watchdog patch: %w", err)
	}
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite live session watchdog update", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET runtime_state = json_patch(COALESCE(runtime_state, '{}'), ?),
			    updated_at = ?
			WHERE session_id = ?
			  AND agent_id = ?
			  AND scope_key = ?
			  AND runtime_mode = ?
			  AND status = 'active'
		`, patch, s.now(), sessionID, agentID, resolved.ScopeKey, mode.String())
		if err != nil {
			return err
		}
		rows, _ = res.RowsAffected()
		return nil
	}); err != nil {
		return fmt.Errorf("update sqlite live session watchdog: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no active live session row found for agent=%s session=%s runtime=%s scope=%s", agentID, sessionID, mode.String(), resolved.ScopeKey)
	}
	return nil
}

func (s *SQLiteRuntimeStore) ensureSQLiteTaskConversationAuditRowTx(ctx context.Context, tx *sql.Tx, rec runtimellm.AgentTurnRecord, now time.Time) error {
	sessionID := strings.TrimSpace(rec.SessionID)
	if sessionID == "" {
		return fmt.Errorf("task conversation audit session_id is required")
	}
	runID := nullUUIDString(rec.RunID)
	identity := taskAuditIdentityFromTurn(rec)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?,
			'[]', 0, 'task', '{}', 'active', ?, ?
		)
		ON CONFLICT(session_id) DO UPDATE SET
			run_id = COALESCE(agent_conversation_audits.run_id, excluded.run_id),
			entity_id = COALESCE(excluded.entity_id, agent_conversation_audits.entity_id),
			flow_instance = COALESCE(excluded.flow_instance, agent_conversation_audits.flow_instance),
			scope_key = CASE
				WHEN excluded.entity_id IS NOT NULL OR excluded.flow_instance IS NOT NULL THEN excluded.scope_key
				WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope_key
				ELSE COALESCE(excluded.scope_key, agent_conversation_audits.scope_key)
			END,
			scope = CASE
				WHEN excluded.entity_id IS NOT NULL OR excluded.flow_instance IS NOT NULL THEN excluded.scope
				WHEN agent_conversation_audits.entity_id IS NOT NULL OR agent_conversation_audits.flow_instance IS NOT NULL THEN agent_conversation_audits.scope
				ELSE excluded.scope
			END,
			status = excluded.status,
			updated_at = excluded.updated_at
	`, sessionID, sqliteNullUUID(runID), strings.TrimSpace(rec.AgentID), sqliteNullUUID(identity.EntityID), sqliteNullString(identity.FlowInstance),
		sqliteNullString(identity.ScopeKey), identity.Scope, now, now)
	if err != nil {
		return fmt.Errorf("ensure sqlite task conversation audit row: %w", err)
	}
	return nil
}

func sqliteConversationPayloads(rec runtimellm.ConversationRecord) ([]byte, string, error) {
	msgs := make([]runtimellm.Message, 0, len(rec.Messages))
	for _, m := range rec.Messages {
		msgs = append(msgs, runtimellm.Message{
			Role:    strings.TrimSpace(m.Role),
			Content: strings.ToValidUTF8(m.Content, "\uFFFD"),
		})
	}
	msgJSON, err := json.Marshal(msgs)
	if err != nil {
		return nil, "", fmt.Errorf("marshal sqlite conversation messages: %w", err)
	}
	summary := strings.ToValidUTF8(rec.Summary, "\uFFFD")
	runtimeStatePatch, err := marshalConversationRuntimeStatePatch(&summary, rec.Watchdog)
	if err != nil {
		return nil, "", fmt.Errorf("marshal sqlite conversation runtime_state patch: %w", err)
	}
	return msgJSON, runtimeStatePatch, nil
}
