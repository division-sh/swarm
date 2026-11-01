package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Consumption appends only to the exact already-projected turn. A committed
// prefix remains an idempotent replay even after later turns extend memory.
func completionToolResultApplied(raw []byte, turnCount int, rec runtimellm.ConversationRecord) (bool, error) {
	if rec.TurnCount < 1 || len(rec.Messages) < 2 || rec.Messages[len(rec.Messages)-1].Role != "tool" {
		return false, fmt.Errorf("completion consumption requires the exact projected turn and tool result")
	}
	wantRaw, _, err := conversationPayloads(rec)
	if err != nil {
		return false, err
	}
	var actual, want []any
	if err := json.Unmarshal(raw, &actual); err != nil {
		return false, err
	}
	if err := json.Unmarshal(wantRaw, &want); err != nil {
		return false, err
	}
	if turnCount >= rec.TurnCount && len(actual) >= len(want) && reflect.DeepEqual(actual[:len(want)], want) {
		return true, nil
	}
	if turnCount != rec.TurnCount || !reflect.DeepEqual(actual, want[:len(want)-1]) {
		return false, fmt.Errorf("completion tool result differs from the exact projected conversation")
	}
	return false, nil
}

func (s *LLMPostgresOwner) ConsumeCompletionConversationTx(ctx context.Context, attempt *mutationprotocol.Attempt, rec runtimellm.ConversationRecord) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, identity, err := validateConversationMemory(rec)
		if err != nil {
			return err
		}
		fields, err := storeagent.IdentityFields(identity)
		if err != nil {
			return err
		}
		var raw []byte
		var turnCount int
		err = tx.QueryRowContext(ctx, `SELECT conversation,turn_count FROM agent_sessions
			WHERE session_id=$1::uuid AND run_id=$2::uuid AND agent_id=$3
			AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6
			AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9 AND memory_enabled=TRUE
			FOR UPDATE`, rec.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&raw, &turnCount)
		if err != nil {
			return fmt.Errorf("load exact completion conversation for consumption: %w", err)
		}
		applied, err := completionToolResultApplied(raw, turnCount, rec)
		if err != nil || applied {
			return err
		}
		return s.writeCompletionConversationTx(ctx, attempt, rec, rec.TurnCount)
	})
}

func (s *LLMSQLiteOwner) ConsumeCompletionConversationTx(ctx context.Context, attempt *mutationprotocol.Attempt, rec runtimellm.ConversationRecord, now time.Time) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, identity, err := validateConversationMemory(rec)
		if err != nil {
			return err
		}
		fields, err := storeagent.IdentityFields(identity)
		if err != nil {
			return err
		}
		var raw []byte
		var turnCount int
		err = tx.QueryRowContext(ctx, `SELECT conversation,turn_count FROM agent_sessions
			WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=?
			AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=? AND memory_enabled=1`,
			rec.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&raw, &turnCount)
		if err != nil {
			return fmt.Errorf("load exact sqlite completion conversation for consumption: %w", err)
		}
		applied, err := completionToolResultApplied(raw, turnCount, rec)
		if err != nil || applied {
			return err
		}
		return s.writeCompletionConversationTx(ctx, attempt, rec, rec.TurnCount, now)
	})
}
