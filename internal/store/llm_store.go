package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/google/uuid"
)

func (s *PostgresStore) AppendAgentTurn(ctx context.Context, rec runtimellm.AgentTurnRecord) error {
	plan, identity, err := validateTurnMemory(rec)
	if err != nil {
		return err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	if plan.Enabled && caps.Conversations.Sessions != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	if !plan.Enabled && caps.Conversations.Audits != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_conversation_audits", caps.Conversations.Audits)
	}

	return s.runAuthorActivityMutation(ctx, "postgres append agent turn", func(txctx context.Context, tx *sql.Tx) error {
		ctx = txctx
		if err := s.ensureRunRow(ctx, caps, tx, identity.RunID, "", "", true); err != nil {
			return err
		}
		if plan.Enabled {
			res, err := tx.ExecContext(ctx, `
			UPDATE agent_sessions SET updated_at=now()
			WHERE session_id=$1::uuid AND run_id=$2::uuid AND agent_id=$3 AND flow_instance=$4
			  AND memory_enabled=TRUE AND status='active'
		`, strings.TrimSpace(rec.SessionID), identity.RunID, identity.AgentID, identity.FlowInstance)
			if err != nil {
				return fmt.Errorf("touch exact live memory row: %w", err)
			}
			if rows, _ := res.RowsAffected(); rows != 1 {
				return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, rec.SessionID)
			}
		} else if err := ensurePostgresStatelessAuditTx(ctx, tx, rec, plan, identity); err != nil {
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
		turnID := uuid.NewString()
		_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, entity_id,
			trigger_event_id, trigger_event_type, task_id, available_tools, tool_calls, emitted_events,
			mcp_servers, mcp_tools_listed, mcp_tools_visible, request_payload, response_payload, turn_blocks,
			parse_ok, latency_ms, retry_count, failure, created_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4::uuid,NULLIF($5,''),$6,$7,NULLIF($8,'')::uuid,
			NULLIF($9,'')::uuid,NULLIF($10,''),NULLIF($11,''),$12::jsonb,$13::jsonb,$14::jsonb,
			$15::jsonb,$16::jsonb,$17::jsonb,CASE WHEN $18='' THEN NULL ELSE $18::jsonb END,
			CASE WHEN $19='' THEN NULL ELSE $19::jsonb END,$20::jsonb,$21,$22,$23,
			CASE WHEN $24='' THEN NULL ELSE $24::jsonb END,now()
		)
	`, turnID, identity.RunID, identity.AgentID, strings.TrimSpace(rec.SessionID), identity.FlowInstance,
			plan.Enabled, string(plan.Source), strings.TrimSpace(rec.EntityID), strings.TrimSpace(rec.TriggerEventID),
			strings.TrimSpace(rec.TriggerEventType), strings.TrimSpace(rec.TaskID), normalizeJSONArray(rec.AvailableTools),
			normalizeJSONArray(rec.ToolCalls), normalizeJSONArray(rec.EmittedEvents), normalizeJSONObject(rec.MCPServers),
			normalizeJSONArray(rec.MCPToolsListed), normalizeJSONArray(rec.MCPToolsVisible), normalizeJSONPayload(rec.RequestPayload),
			normalizeJSONPayload(rec.ResponseRaw), normalizeJSONArray(rec.TurnBlocks), rec.ParseOK, latencyMS, rec.RetryCount, failurePayload)
		if err != nil {
			return fmt.Errorf("insert agent turn: %w", err)
		}
		return recordAuthorActivityTurn(ctx, authorActivityTurn{
			TurnID: turnID, RunID: rec.RunID, AgentID: rec.AgentID, SessionID: rec.SessionID, EntityID: rec.EntityID,
			FlowID: identity.FlowInstance, TriggerEventType: rec.TriggerEventType, Blocks: rec.TurnBlocks,
			ParseOK: rec.ParseOK, DurationMS: latencyMS, RetryCount: rec.RetryCount, UsageExactness: "unavailable",
			Failure: rec.Failure, OccurredAt: time.Now().UTC(),
		})
	})
}

func validateTurnMemory(rec runtimellm.AgentTurnRecord) (agentmemory.Plan, agentmemory.Identity, error) {
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return agentmemory.Plan{}, agentmemory.Identity{}, err
	}
	identity := agentmemory.Identity{RunID: rec.RunID, AgentID: rec.AgentID, FlowInstance: rec.FlowInstance}.Normalize()
	if strings.TrimSpace(rec.SessionID) == "" {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("session_id is required")
	}
	if identity.RunID == "" || identity.AgentID == "" {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("run_id and agent_id are required")
	}
	if plan.Enabled {
		if err := identity.Validate(); err != nil {
			return agentmemory.Plan{}, agentmemory.Identity{}, err
		}
	}
	return plan, identity, nil
}

func ensurePostgresStatelessAuditTx(ctx context.Context, tx *sql.Tx, rec runtimellm.AgentTurnRecord, plan agentmemory.Plan, identity agentmemory.Identity) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,$3,NULLIF($4,''),FALSE,$5,NULLIF($6,'')::uuid,'[]'::jsonb,1,'{}'::jsonb,'active',now(),now())
		ON CONFLICT (session_id) DO UPDATE SET
			run_id=EXCLUDED.run_id, agent_id=EXCLUDED.agent_id, flow_instance=EXCLUDED.flow_instance,
			memory_enabled=FALSE, memory_source=EXCLUDED.memory_source, entity_id=EXCLUDED.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=now()
	`, strings.TrimSpace(rec.SessionID), identity.RunID, identity.AgentID, identity.FlowInstance, string(plan.Source), strings.TrimSpace(rec.EntityID))
	if err != nil {
		return fmt.Errorf("ensure stateless conversation audit row: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	plan, identity, err := validateConversationMemory(rec)
	if err != nil {
		return err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
	}
	messages, state, err := conversationPayloads(rec)
	if err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "postgres upsert exact conversation", func(txctx context.Context, tx *sql.Tx) error {
		ctx = txctx
		if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "upsert_conversation", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET conversation=$1::jsonb, turn_count=$2,
			runtime_state=COALESCE(runtime_state,'{}'::jsonb) || $3::jsonb, updated_at=now()
		WHERE session_id=$4::uuid AND run_id=$5::uuid AND agent_id=$6 AND flow_instance=$7
		  AND memory_enabled=$8 AND memory_source=$9 AND status='active'
	`, string(messages), rec.TurnCount, state, strings.TrimSpace(rec.SessionID), identity.RunID,
			identity.AgentID, identity.FlowInstance, plan.Enabled, string(plan.Source))
		if err != nil {
			return fmt.Errorf("update exact live conversation: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, rec.SessionID)
		}
		return nil
	})
}

func validateConversationMemory(rec runtimellm.ConversationRecord) (agentmemory.Plan, agentmemory.Identity, error) {
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return agentmemory.Plan{}, agentmemory.Identity{}, err
	}
	if !plan.Enabled {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("conversation persistence requires memory true")
	}
	identity := rec.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return agentmemory.Plan{}, agentmemory.Identity{}, err
	}
	if strings.TrimSpace(rec.AgentID) != identity.AgentID {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("conversation agent_id does not match memory identity")
	}
	if strings.TrimSpace(rec.SessionID) == "" {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("session_id is required")
	}
	return plan, identity, nil
}

func conversationPayloads(rec runtimellm.ConversationRecord) ([]byte, string, error) {
	messages := make([]runtimellm.Message, 0, len(rec.Messages))
	for _, message := range rec.Messages {
		messages = append(messages, runtimellm.Message{Role: strings.TrimSpace(message.Role), Content: redactText(message.Content)})
	}
	raw, err := json.Marshal(messages)
	if err != nil {
		return nil, "", fmt.Errorf("marshal conversation messages: %w", err)
	}
	summary := strings.ToValidUTF8(rec.Summary, "\uFFFD")
	state, err := marshalConversationRuntimeStatePatch(&summary, rec.Watchdog)
	if err != nil {
		return nil, "", fmt.Errorf("marshal conversation runtime state: %w", err)
	}
	return raw, state, nil
}

func (s *PostgresStore) LoadActiveConversation(ctx context.Context, identity agentmemory.Identity) (runtimellm.ConversationRecord, bool, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	var sessionID, status string
	var conversation, runtimeState []byte
	var turnCount int
	err := s.DB.QueryRowContext(ctx, `
		SELECT session_id::text,status,COALESCE(conversation,'[]'::jsonb),COALESCE(runtime_state,'{}'::jsonb),turn_count
		FROM agent_sessions WHERE run_id=$1::uuid AND agent_id=$2 AND flow_instance=$3
		  AND memory_enabled=TRUE AND status='active'
	`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&sessionID, &status, &conversation, &runtimeState, &turnCount)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ConversationRecord{}, false, nil
	}
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load exact active conversation: %w", err)
	}
	rec, err := decodeLiveConversationRecord(identity, sessionID, status, conversation, runtimeState, turnCount)
	return rec, err == nil, err
}

func (s *PostgresStore) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
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
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "update_watchdog", false); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET runtime_state=COALESCE(runtime_state,'{}'::jsonb) || $1::jsonb,updated_at=now()
		WHERE session_id=$2::uuid AND run_id=$3::uuid AND agent_id=$4 AND flow_instance=$5
		  AND memory_enabled=TRUE AND status='active'
	`, patch, update.SessionID, identity.RunID, identity.AgentID, identity.FlowInstance)
	if err != nil {
		return fmt.Errorf("update exact memory watchdog: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return fmt.Errorf("no exact active memory row found for watchdog update")
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("update live session watchdog commit: %w", err)
	}
	return nil
}

func mustJSON(v any) []byte {
	if v == nil {
		return nil
	}
	raw, _ := json.Marshal(v)
	return raw
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
	patch := map[string]any{}
	if summary != nil {
		patch["summary"] = strings.ToValidUTF8(*summary, "\uFFFD")
	}
	if watchdog != nil {
		descriptor := conversationRuntimeWatchdogDescriptorFromRuntime(watchdog)
		if err := ValidateConversationRuntimeWatchdogDescriptor(descriptor); err != nil {
			return "", err
		}
		patch["watchdog"] = descriptor
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func conversationRuntimeWatchdogDescriptorFromRuntime(w *runtimellm.ConversationWatchdog) ConversationRuntimeWatchdogDescriptor {
	if w == nil {
		return ConversationRuntimeWatchdogDescriptor{}
	}
	return ConversationRuntimeWatchdogDescriptor{
		State: w.State, BlockingLayer: w.BlockingLayer, Action: w.Action, Outcome: w.Outcome,
		LastOutputAt: w.LastOutputAt, RecordedAt: w.RecordedAt,
	}
}
