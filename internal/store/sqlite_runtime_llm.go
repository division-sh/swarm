package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func (s *SQLiteRuntimeStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	executionMode, ok := runtimeeffects.ExecutionModeFromContext(ctx)
	if !ok {
		return fmt.Errorf("agent turn execution mode is required")
	}
	plan, identity, err := validateTurnMemory(rec)
	if err != nil {
		return err
	}
	rec = runtimellm.CanonicalizeTurnForPersistence(rec)
	if _, err := runtimellm.DecodeCanonicalRuntimeLogTurnBlocks(rec.TurnBlocks); err != nil {
		return fmt.Errorf("validate canonical runtime_log turn_blocks: %w", err)
	}
	failurePayload := ""
	if encoded, err := encodeStoredFailure(rec.Failure); err != nil {
		return fmt.Errorf("encode agent turn failure: %w", err)
	} else if encoded != nil {
		failurePayload = encoded.(string)
	}
	latencyMS := int(rec.Latency / time.Millisecond)
	if latencyMS < 0 {
		latencyMS = 0
	}
	now := s.now()
	return s.runAuthorActivityMutation(ctx, "sqlite append agent turn", func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunlifecycle.RequireActive(txctx, tx, identity.RunID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		if plan.Enabled {
			res, err := tx.ExecContext(txctx, `
				UPDATE agent_sessions SET updated_at=?
				WHERE session_id=? AND run_id=? AND agent_id=? AND flow_instance=?
				  AND memory_enabled=1 AND status='active'
			`, now, strings.TrimSpace(rec.SessionID), identity.RunID, identity.AgentID, identity.FlowInstance)
			if err != nil {
				return fmt.Errorf("touch exact sqlite live memory row: %w", err)
			}
			if rows, _ := res.RowsAffected(); rows != 1 {
				return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, rec.SessionID)
			}
		} else if err := ensureSQLiteStatelessAuditTx(txctx, tx, rec, plan, identity, now); err != nil {
			return err
		}
		turnID := uuid.NewString()
		_, err := tx.ExecContext(txctx, `
			INSERT INTO agent_turns (
				turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
				trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls, emitted_events,
				mcp_servers, mcp_tools_listed, mcp_tools_visible, request_payload, response_payload, turn_blocks,
				parse_ok, latency_ms, retry_count, execution_mode, failure, created_at
			) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		`, turnID, identity.RunID, identity.AgentID, strings.TrimSpace(rec.SessionID), sqliteNullString(identity.FlowInstance),
			plan.Enabled, string(plan.Source), sqliteNullUUID(rec.EntityID), sqliteNullUUID(rec.TriggerEventID), sqliteNullString(rec.TriggerEventType),
			sqliteNullString(rec.TaskID), normalizeJSONArray(rec.AvailableTools), normalizeJSONArray(rec.ToolCalls), normalizeJSONArray(rec.EmittedEvents),
			normalizeJSONObject(rec.MCPServers), normalizeJSONArray(rec.MCPToolsListed), normalizeJSONArray(rec.MCPToolsVisible),
			sqliteNullString(normalizeJSONPayload(rec.RequestPayload)), sqliteNullString(normalizeJSONPayload(rec.ResponseRaw)), normalizeJSONArray(rec.TurnBlocks),
			rec.ParseOK, latencyMS, rec.RetryCount, executionMode, sqliteNullString(failurePayload), now)
		if err != nil {
			return fmt.Errorf("insert sqlite agent turn: %w", err)
		}
		return recordAuthorActivityTurn(txctx, authorActivityTurn{
			TurnID: turnID, RunID: rec.RunID, AgentID: rec.AgentID, SessionID: rec.SessionID, EntityID: rec.EntityID,
			FlowID: identity.FlowInstance, TriggerEventType: rec.TriggerEventType, Blocks: rec.TurnBlocks,
			ParseOK: rec.ParseOK, DurationMS: latencyMS, RetryCount: rec.RetryCount, UsageExactness: "unavailable",
			ExecutionMode: string(executionMode), Failure: rec.Failure, OccurredAt: now,
		})
	})
}

func ensureSQLiteStatelessAuditTx(ctx context.Context, tx *sql.Tx, rec runtimellm.AgentTurnRecord, plan agentmemory.Plan, identity agentmemory.Identity, now time.Time) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (?,?,?,?,0,?,?, '[]',1,'{}','active',?,?)
		ON CONFLICT(session_id) DO UPDATE SET
			run_id=excluded.run_id, agent_id=excluded.agent_id, flow_instance=excluded.flow_instance,
			memory_enabled=0, memory_source=excluded.memory_source, entity_id=excluded.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=excluded.updated_at
	`, strings.TrimSpace(rec.SessionID), identity.RunID, identity.AgentID, sqliteNullString(identity.FlowInstance),
		string(plan.Source), sqliteNullUUID(rec.EntityID), now, now)
	if err != nil {
		return fmt.Errorf("ensure sqlite stateless conversation audit row: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	plan, identity, err := validateConversationMemory(rec)
	if err != nil {
		return err
	}
	messages, state, err := conversationPayloads(rec)
	if err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "sqlite upsert exact conversation", func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunlifecycle.RequireActive(txctx, tx, identity.RunID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "upsert_conversation", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET conversation=?,turn_count=?,runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=?
			WHERE session_id=? AND run_id=? AND agent_id=? AND flow_instance=?
			  AND memory_enabled=? AND memory_source=? AND status='active'
		`, string(messages), rec.TurnCount, state, s.now(), strings.TrimSpace(rec.SessionID), identity.RunID,
			identity.AgentID, identity.FlowInstance, plan.Enabled, string(plan.Source))
		if err != nil {
			return fmt.Errorf("update exact sqlite live conversation: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, rec.SessionID)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) LoadActiveConversation(ctx context.Context, identity agentmemory.Identity) (runtimellm.ConversationRecord, bool, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	var sessionID, status string
	var conversation, runtimeState any
	var turnCount int
	err := s.DB.QueryRowContext(ctx, `
		SELECT s.session_id,s.status,COALESCE(s.conversation,'[]'),COALESCE(s.runtime_state,'{}'),s.turn_count
		FROM agent_sessions s
		JOIN runs run ON run.run_id = s.run_id
		WHERE s.run_id=? AND s.agent_id=? AND s.flow_instance=?
		  AND s.memory_enabled=1 AND s.status='active'
		  AND run.status IN ('running', 'paused')
	`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&sessionID, &status, &conversation, &runtimeState, &turnCount)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ConversationRecord{}, false, nil
	}
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load exact sqlite active conversation: %w", err)
	}
	rec, err := decodeLiveConversationRecord(identity, sessionID, status, sqliteJSONRawMessage(conversation), sqliteJSONRawMessage(runtimeState), turnCount)
	return rec, err == nil, err
}

func (s *SQLiteRuntimeStore) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
	identity := update.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(update.AgentID) != identity.AgentID || strings.TrimSpace(update.SessionID) == "" {
		return fmt.Errorf("watchdog agent_id/session_id must match an exact memory identity")
	}
	if update.Watchdog == nil {
		return fmt.Errorf("watchdog is required")
	}
	patch, err := marshalConversationRuntimeStatePatch(nil, update.Watchdog)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite update exact memory watchdog", func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunlifecycle.RequireActive(txctx, tx, identity.RunID, storerunlifecycle.DialectSQLite); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "update_watchdog", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=?
			WHERE session_id=? AND run_id=? AND agent_id=? AND flow_instance=?
			  AND memory_enabled=1 AND status='active'
		`, patch, s.now(), update.SessionID, identity.RunID, identity.AgentID, identity.FlowInstance)
		if err != nil {
			return fmt.Errorf("update exact sqlite memory watchdog: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for watchdog update")
		}
		return nil
	})
}
