package llmpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func (s *LLMPostgresOwner) InstallContextCompactionTx(ctx context.Context, attempt *mutationprotocol.Attempt, lease *sessions.Lease, result runtimellm.ContextCompactionResult) error {
	return installContextCompactionTx(ctx, attempt, true, lease, result, time.Now().UTC())
}

func (s *LLMSQLiteOwner) InstallContextCompactionTx(ctx context.Context, attempt *mutationprotocol.Attempt, lease *sessions.Lease, result runtimellm.ContextCompactionResult) error {
	return installContextCompactionTx(ctx, attempt, false, lease, result, s.now())
}

func installContextCompactionTx(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, lease *sessions.Lease, result runtimellm.ContextCompactionResult, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("context compaction requires its exact session grant")
	}
	if err := lease.ValidateFor(lease.Identity, result.Checkpoint.SessionID); err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return installContextCompactionSQL(ctx, tx, attempt, postgres, lease, result, now)
	})
}

func requireContextCompactionSessionTx(ctx context.Context, tx *sql.Tx, postgres bool, lease *sessions.Lease, now time.Time) error {
	identity := lease.Identity.Normalize()
	if postgres {
		if err := storerunstate.RequirePostgresActiveTx(ctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity, "install_context_compaction", false); err != nil {
			return err
		}
		return requirePostgresCurrentSessionGrantTx(ctx, tx, lease)
	}
	if err := storerunstate.RequireSQLiteActiveTx(ctx, tx, identity.RunID); err != nil {
		return err
	}
	if _, err := requireSQLiteLiveSessionAuthority(ctx, tx, identity, "install_context_compaction", false); err != nil {
		return err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE session_id=? AND run_id=? AND lease_holder=? AND lease_grant_id=? AND lease_expires_at>? AND status='active'`, lease.SessionID, identity.RunID, lease.LockOwner, lease.GrantID, now).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return sessions.ErrSessionLeased
	}
	return nil
}

func installContextCompactionSQL(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, postgres bool, lease *sessions.Lease, result runtimellm.ContextCompactionResult, now time.Time) error {
	identity := lease.Identity.Normalize()
	if err := requireContextCompactionSessionTx(ctx, tx, postgres, lease, now); err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return err
	}
	query := `SELECT status,conversation,runtime_state,turn_count FROM agent_sessions WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=? AND flow_instance=? AND memory_enabled=TRUE AND status='active'`
	if postgres {
		query = `SELECT status,conversation,runtime_state,turn_count FROM agent_sessions WHERE session_id=$1::uuid AND run_id=$2::uuid AND agent_id=$3 AND agent_name_owner=$4 AND agent_name_source=$5 AND agent_route_presence=$6 AND flow_scope_key=$7 AND flow_instance_id=$8 AND flow_instance=$9 AND memory_enabled=TRUE AND status='active' FOR UPDATE`
	}
	var status string
	var messages, state []byte
	var turns int
	if err := tx.QueryRowContext(ctx, query, lease.SessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&status, &messages, &state, &turns); err != nil {
		return err
	}
	record, err := decodeLiveConversationRecord(identity, lease.SessionID, status, messages, state, turns)
	if err != nil {
		return err
	}
	checkpoints, changed, err := runtimellm.ContextCompactionInstallation(record, result)
	if err != nil || !changed {
		return err
	}
	patch, err := json.Marshal(map[string]any{"context_checkpoints": checkpoints})
	if err != nil {
		return err
	}
	query = `UPDATE agent_sessions SET runtime_state=json_patch(COALESCE(runtime_state,'{}'),?),updated_at=? WHERE session_id=? AND run_id=? AND lease_holder=? AND lease_grant_id=? AND lease_expires_at>? AND status='active'`
	args := []any{string(patch), now, lease.SessionID, identity.RunID, lease.LockOwner, lease.GrantID, now}
	if postgres {
		query = `UPDATE agent_sessions SET runtime_state=COALESCE(runtime_state,'{}'::jsonb)||$1::jsonb,updated_at=clock_timestamp() WHERE session_id=$2::uuid AND run_id=$3::uuid AND lease_holder=$4 AND lease_grant_id=$5 AND lease_expires_at>clock_timestamp() AND status='active'`
		args = []any{string(patch), lease.SessionID, identity.RunID, lease.LockOwner, lease.GrantID}
	}
	written, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	count, err := written.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return sessions.ErrSessionLeased
	}
	return addAgentSessionFacts(attempt, identity.RunID, lease.SessionID)
}
