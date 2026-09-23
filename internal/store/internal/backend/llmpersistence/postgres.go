package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sessionstore "github.com/division-sh/swarm/internal/store/internal/backend/sessions"
)

type ConversationRuntimeWatchdogDescriptor = sessionstore.ConversationRuntimeWatchdogDescriptor
type ConversationRuntimeStateDescriptor = sessionstore.ConversationRuntimeStateDescriptor

var ValidateConversationRuntimeWatchdogDescriptor = sessionstore.ValidateConversationRuntimeWatchdogDescriptor
var DecodeConversationRuntimeStateDescriptor = sessionstore.DecodeConversationRuntimeStateDescriptor

func validateTurnMemory(rec runtimellm.AgentTurnRecord) (agentmemory.Plan, agentmemory.Identity, error) {
	plan, err := rec.Memory.Normalize()
	if err != nil {
		return agentmemory.Plan{}, agentmemory.Identity{}, err
	}
	identity := rec.Identity.Normalize()
	if strings.TrimSpace(rec.SessionID) == "" {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("session_id is required")
	}
	if err := agentmemory.ValidateIdentity(identity, false); err != nil {
		return agentmemory.Plan{}, agentmemory.Identity{}, err
	}
	if plan.Enabled {
		if err := identity.Validate(); err != nil {
			return agentmemory.Plan{}, agentmemory.Identity{}, err
		}
	}
	if strings.TrimSpace(rec.RunID) != identity.RunID ||
		strings.TrimSpace(rec.AgentID) != identity.AgentID() ||
		strings.Trim(strings.TrimSpace(rec.FlowInstance), "/") != identity.FlowInstance() {
		return agentmemory.Plan{}, agentmemory.Identity{}, fmt.Errorf("agent turn display fields do not match concrete identity")
	}
	return plan, identity, nil
}

func ensurePostgresStatelessAuditTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, rec runtimellm.AgentTurnRecord, plan agentmemory.Plan, identity agentmemory.Identity) error {
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return err
	}
	sessionID := strings.TrimSpace(rec.SessionID)
	for {
		var previousRunID string
		err := tx.QueryRowContext(ctx, `SELECT run_id::text FROM agent_conversation_audits WHERE session_id=$1::uuid FOR UPDATE`, sessionID).Scan(&previousRunID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("load stateless conversation audit owner: %w", err)
		}
		conflict := `ON CONFLICT (session_id) DO UPDATE SET
			run_id=EXCLUDED.run_id, agent_id=EXCLUDED.agent_id,
			agent_name_owner=EXCLUDED.agent_name_owner, agent_name_source=EXCLUDED.agent_name_source,
			agent_route_presence=EXCLUDED.agent_route_presence, flow_scope_key=EXCLUDED.flow_scope_key,
			flow_instance_id=EXCLUDED.flow_instance_id, flow_instance=EXCLUDED.flow_instance,
			memory_enabled=FALSE, memory_source=EXCLUDED.memory_source, entity_id=EXCLUDED.entity_id,
			turn_count=agent_conversation_audits.turn_count + 1, status='active', updated_at=now()`
		if errors.Is(err, sql.ErrNoRows) {
			// A concurrent first insert must be locked/read before replacing its run.
			conflict = `ON CONFLICT (session_id) DO NOTHING`
		}
		var storedSessionID, storedRunID string
		err = tx.QueryRowContext(ctx, `
			INSERT INTO agent_conversation_audits (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
				flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source, entity_id,
				conversation, turn_count, runtime_state, status, created_at, updated_at
			) VALUES ($1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,FALSE,$10,NULLIF($11,'')::uuid,'[]'::jsonb,1,'{}'::jsonb,'active',now(),now())
		`+conflict+` RETURNING session_id::text, run_id::text`, sessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
			string(plan.Source), strings.TrimSpace(rec.EntityID)).Scan(&storedSessionID, &storedRunID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("ensure stateless conversation audit row: %w", err)
		}
		if previousRunID != "" {
			if err := attempt.AddFact(previousRunID, runforkrevision.FamilyAgentConversationAudits, storedSessionID); err != nil {
				return err
			}
		}
		return attempt.AddFact(storedRunID, runforkrevision.FamilyAgentConversationAudits, storedSessionID)
	}
}

func (s *LLMPostgresOwner) EnsureCompletionTurnMemoryTx(ctx context.Context, attempt *mutationprotocol.Attempt, rec runtimellm.AgentTurnRecord) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		plan, identity, err := validateTurnMemory(rec)
		if err != nil {
			return err
		}
		if !plan.Enabled {
			return ensurePostgresStatelessAuditTx(ctx, tx, attempt, rec, plan, identity)
		}
		fields, err := storeagent.IdentityFields(identity)
		if err != nil {
			return err
		}
		var storedSessionID, storedRunID string
		err = tx.QueryRowContext(ctx, `
		UPDATE agent_sessions SET updated_at=now()
		WHERE session_id=$1::uuid AND run_id=$2::uuid AND agent_id=$3
		  AND agent_name_owner=$4 AND agent_name_source=$5
		  AND agent_route_presence=$6 AND flow_scope_key=$7
		  AND flow_instance_id=$8 AND flow_instance=$9
		  AND memory_enabled=TRUE AND status='active'
		RETURNING session_id::text, run_id::text
	`, rec.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&storedSessionID, &storedRunID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no exact active memory row found for completion run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), rec.SessionID)
		}
		if err != nil {
			return fmt.Errorf("touch completion live memory row: %w", err)
		}
		return addAgentSessionFacts(attempt, storedRunID, storedSessionID)
	})
}

func (s *LLMPostgresOwner) UpsertConversation(ctx context.Context, rec runtimellm.ConversationRecord) error {
	plan, identity, err := validateConversationMemory(rec)
	if err != nil {
		return err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	messages, state, err := conversationPayloads(rec)
	if err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "upsert_conversation", false); err != nil {
				return err
			}
			var storedSessionID, storedRunID string
			err := tx.QueryRowContext(sqlCtx, `
		UPDATE agent_sessions SET conversation=$1::jsonb, turn_count=$2,
			runtime_state=COALESCE(runtime_state,'{}'::jsonb) || $3::jsonb, updated_at=now()
			WHERE session_id=$4::uuid AND run_id=$5::uuid AND agent_id=$6
			  AND agent_name_owner=$7 AND agent_name_source=$8 AND agent_route_presence=$9
			  AND flow_scope_key=$10 AND flow_instance_id=$11 AND flow_instance=$12
			  AND memory_enabled=$13 AND memory_source=$14 AND status='active'
			RETURNING session_id::text, run_id::text
		`, string(messages), rec.TurnCount, state, strings.TrimSpace(rec.SessionID), identity.RunID,
				fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
				fields.FlowInstanceID, fields.FlowInstancePath, plan.Enabled, string(plan.Source)).Scan(&storedSessionID, &storedRunID)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("no exact active memory row found for run=%s agent=%s flow_instance=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), rec.SessionID)
			}
			if err != nil {
				return fmt.Errorf("update exact live conversation: %w", err)
			}
			return addAgentSessionFacts(attempt, storedRunID, storedSessionID)
		})
		return struct{}{}, err
	})
	return result.Err()
}

func (s *LLMPostgresOwner) ProjectCompletionConversationTx(ctx context.Context, attempt *mutationprotocol.Attempt, rec runtimellm.ConversationRecord, expectedTurnCount int) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
		fields, err := storeagent.IdentityFields(identity)
		if err != nil {
			return err
		}
		if err := storerunstate.RequirePostgresActiveTx(ctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity, "project_completion_conversation", false); err != nil {
			return err
		}
		var storedSessionID, storedRunID string
		err = tx.QueryRowContext(ctx, `
		UPDATE agent_sessions SET conversation=$1::jsonb, turn_count=$2,
			runtime_state=COALESCE(runtime_state,'{}'::jsonb) || $3::jsonb, updated_at=now()
		WHERE session_id=$4::uuid AND run_id=$5::uuid AND agent_id=$6
		  AND agent_name_owner=$7 AND agent_name_source=$8 AND agent_route_presence=$9
		  AND flow_scope_key=$10 AND flow_instance_id=$11 AND flow_instance=$12
		  AND memory_enabled=$13 AND memory_source=$14 AND status='active' AND turn_count=$15
		RETURNING session_id::text, run_id::text
	`, string(messages), rec.TurnCount, state, strings.TrimSpace(rec.SessionID), identity.RunID,
			fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey,
			fields.FlowInstanceID, fields.FlowInstancePath, plan.Enabled, string(plan.Source), expectedTurnCount).Scan(&storedSessionID, &storedRunID)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("completion conversation projection turn conflict: run=%s agent=%s session=%s expected_turn=%d", identity.RunID, identity.AgentID(), rec.SessionID, expectedTurnCount)
		}
		if err != nil {
			return fmt.Errorf("project exact completion conversation: %w", err)
		}
		return addAgentSessionFacts(attempt, storedRunID, storedSessionID)
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
	if strings.TrimSpace(rec.AgentID) != identity.AgentID() {
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
		messages = append(messages, runtimellm.Message{Role: strings.TrimSpace(message.Role), Content: storeagent.RedactText(message.Content)})
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

func (s *LLMPostgresOwner) LoadActiveConversation(ctx context.Context, identity agentmemory.Identity) (runtimellm.ConversationRecord, bool, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return runtimellm.ConversationRecord{}, false, err
	}
	var sessionID, status string
	var conversation, runtimeState []byte
	var turnCount int
	err = s.backend.QueryRowContext(ctx, `
		SELECT s.session_id::text,s.status,COALESCE(s.conversation,'[]'::jsonb),COALESCE(s.runtime_state,'{}'::jsonb),s.turn_count
		FROM agent_sessions s
		JOIN runs run ON run.run_id = s.run_id
		WHERE s.run_id=$1::uuid AND s.agent_id=$2 AND s.agent_name_owner=$3
		  AND s.agent_name_source=$4 AND s.agent_route_presence=$5
		  AND s.flow_scope_key=$6 AND s.flow_instance_id=$7 AND s.flow_instance=$8
		  AND s.memory_enabled=TRUE AND s.status='active'
		  AND run.status IN (`+storerunstate.ActiveStateSQLValues+`)
	`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&sessionID, &status, &conversation, &runtimeState, &turnCount)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimellm.ConversationRecord{}, false, nil
	}
	if err != nil {
		return runtimellm.ConversationRecord{}, false, fmt.Errorf("load exact active conversation: %w", err)
	}
	rec, err := decodeLiveConversationRecord(identity, sessionID, status, conversation, runtimeState, turnCount)
	return rec, err == nil, err
}

func (s *LLMPostgresOwner) UpdateLiveSessionWatchdog(ctx context.Context, update runtimellm.ConversationWatchdogUpdate) error {
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
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return err
	}
	patch, err := marshalConversationRuntimeStatePatch(nil, update.Watchdog)
	if err != nil {
		return err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequirePostgresActiveTx(sqlCtx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requirePostgresLiveSessionAuthority(sqlCtx, tx, identity, "update_watchdog", false); err != nil {
				return err
			}
			var storedSessionID, storedRunID string
			err := tx.QueryRowContext(sqlCtx, `
			UPDATE agent_sessions SET runtime_state=COALESCE(runtime_state,'{}'::jsonb) || $1::jsonb,updated_at=now()
			WHERE session_id=$2::uuid AND run_id=$3::uuid AND agent_id=$4
			  AND agent_name_owner=$5 AND agent_name_source=$6 AND agent_route_presence=$7
			  AND flow_scope_key=$8 AND flow_instance_id=$9 AND flow_instance=$10
			  AND memory_enabled=TRUE AND status='active'
			RETURNING session_id::text, run_id::text
		`, patch, update.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
				fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&storedSessionID, &storedRunID)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("no exact active memory row found for watchdog update")
			}
			if err != nil {
				return fmt.Errorf("update exact memory watchdog: %w", err)
			}
			return addAgentSessionFacts(attempt, storedRunID, storedSessionID)
		})
		return struct{}{}, err
	})
	return result.Err()
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
