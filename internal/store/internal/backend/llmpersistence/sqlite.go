package llmpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func ensureSQLiteStatelessAuditTx(ctx context.Context, tx *sql.Tx, rec runtimellm.AgentTurnRecord, plan agentmemory.Plan, identity agentmemory.Identity, now time.Time) error {
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, entity_id,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (?,?,?,?,?,?,?,?,?,0,?,?, '[]',1,'{}','active',?,?)
		ON CONFLICT(session_id) DO UPDATE SET
			run_id=excluded.run_id, agent_id=excluded.agent_id,
			agent_name_owner=excluded.agent_name_owner, agent_name_source=excluded.agent_name_source,
			agent_route_presence=excluded.agent_route_presence, flow_scope_key=excluded.flow_scope_key,
			flow_instance_id=excluded.flow_instance_id, flow_instance=excluded.flow_instance,
			memory_enabled=0, memory_source=excluded.memory_source, entity_id=excluded.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=excluded.updated_at
	`, strings.TrimSpace(rec.SessionID), identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		string(plan.Source), sqliteNullUUID(rec.EntityID), now, now)
	if err != nil {
		return fmt.Errorf("ensure sqlite stateless conversation audit row: %w", err)
	}
	return nil
}

func (s *LLMSQLiteOwner) EnsureCompletionTurnMemoryTx(ctx context.Context, tx *sql.Tx, rec runtimellm.AgentTurnRecord, now time.Time) error {
	plan, identity, err := validateTurnMemory(rec)
	if err != nil {
		return err
	}
	if !plan.Enabled {
		return ensureSQLiteStatelessAuditTx(ctx, tx, rec, plan, identity, now)
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET updated_at=?
		WHERE session_id=? AND run_id=? AND agent_id=?
		  AND agent_name_owner=? AND agent_name_source=?
		  AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=?
		  AND memory_enabled=1 AND status='active'
	`, now, rec.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
	if err != nil {
		return fmt.Errorf("touch SQLite completion live memory row: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("no exact active SQLite memory row found for completion run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), rec.SessionID)
	}
	return nil
}

func (s *LLMSQLiteOwner) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	plan, identity, err := validateConversationMemory(rec)
	if err != nil {
		return err
	}
	messages, state, err := conversationPayloads(rec)
	if err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite upsert exact conversation", func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "upsert_conversation", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET conversation=?,turn_count=?,runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=?
			WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=?
			  AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=?
			  AND flow_instance_id=? AND flow_instance=?
			  AND memory_enabled=? AND memory_source=? AND status='active'
		`, string(messages), rec.TurnCount, state, s.now(), strings.TrimSpace(rec.SessionID), identity.RunID,
			fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
			fields.FlowInstanceID, fields.FlowInstancePath, plan.Enabled, string(plan.Source))
		if err != nil {
			return fmt.Errorf("update exact sqlite live conversation: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), rec.SessionID)
		}
		return nil
	})
}

func (s *LLMSQLiteOwner) ProjectCompletionConversationTx(ctx context.Context, tx *sql.Tx, rec runtimellm.ConversationRecord, expectedTurnCount int, now time.Time) error {
	plan, identity, err := validateConversationMemory(rec)
	if err != nil {
		return err
	}
	if expectedTurnCount < 0 || rec.TurnCount != expectedTurnCount+1 {
		return fmt.Errorf("completion conversation projection requires one exact turn transition")
	}
	messages, state, err := conversationPayloads(rec)
	if err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	if err := storerunstate.RequireSQLiteActiveTx(ctx, tx, identity.RunID); err != nil {
		return err
	}
	if _, err := requireSQLiteLiveSessionAuthority(ctx, tx, identity.Agent, "project_completion_conversation", false); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET conversation=?,turn_count=?,runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=?
		WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=?
		  AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=?
		  AND memory_enabled=? AND memory_source=? AND status='active' AND turn_count=?
	`, string(messages), rec.TurnCount, state, now.UTC(), strings.TrimSpace(rec.SessionID), identity.RunID,
		fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
		fields.FlowInstanceID, fields.FlowInstancePath, plan.Enabled, string(plan.Source), expectedTurnCount)
	if err != nil {
		return fmt.Errorf("project exact sqlite completion conversation: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return fmt.Errorf("sqlite completion conversation projection turn conflict: run=%s agent=%s session=%s expected_turn=%d", identity.RunID, identity.AgentID(), rec.SessionID, expectedTurnCount)
	}
	return nil
}

func (s *LLMSQLiteOwner) LoadActiveConversation(ctx context.Context, identity agentmemory.Identity) (runtimellm.ConversationRecord, bool, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	var sessionID, status string
	var conversation, runtimeState any
	var turnCount int
	err = s.backend.QueryRowContext(ctx, `
		SELECT s.session_id,s.status,COALESCE(s.conversation,'[]'),COALESCE(s.runtime_state,'{}'),s.turn_count
		FROM agent_sessions s
		JOIN runs run ON run.run_id = s.run_id
		WHERE s.run_id=? AND s.agent_id=? AND s.agent_name_owner=?
		  AND s.agent_name_source=? AND s.agent_route_presence=? AND s.flow_scope_key=?
		  AND s.flow_instance_id=? AND s.flow_instance=?
		  AND s.memory_enabled=1 AND s.status='active'
		  AND run.status IN (`+storerunstate.ActiveStateSQLValues+`)
	`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&sessionID, &status, &conversation, &runtimeState, &turnCount)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ConversationRecord{}, false, nil
	}
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load exact sqlite active conversation: %w", err)
	}
	rec, err := decodeLiveConversationRecord(identity, sessionID, status, sqliteJSONRawMessage(conversation), sqliteJSONRawMessage(runtimeState), turnCount)
	return rec, err == nil, err
}

func (s *LLMSQLiteOwner) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
	identity := update.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(update.AgentID) != identity.AgentID() || strings.TrimSpace(update.SessionID) == "" {
		return fmt.Errorf("watchdog agent_id/session_id must match an exact memory identity")
	}
	if update.Watchdog == nil {
		return fmt.Errorf("watchdog is required")
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	patch, err := marshalConversationRuntimeStatePatch(nil, update.Watchdog)
	if err != nil {
		return err
	}
	return s.runRuntimeMutation(ctx, "sqlite update exact memory watchdog", func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "update_watchdog", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=?
			WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=?
			  AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=?
			  AND flow_instance_id=? AND flow_instance=?
			  AND memory_enabled=1 AND status='active'
		`, patch, s.now(), update.SessionID, identity.RunID, fields.AgentID, fields.NameOwner,
			fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath)
		if err != nil {
			return fmt.Errorf("update exact sqlite memory watchdog: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("no exact active memory row found for watchdog update")
		}
		return nil
	})
}
