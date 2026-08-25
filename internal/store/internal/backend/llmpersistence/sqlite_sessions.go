package llmpersistence

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
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

func (s *LLMSQLiteOwner) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquireSQLiteLiveSession(ctx, identity, lockOwner)
	return lease, err
}

func (s *LLMSQLiteOwner) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquireSQLiteLiveSession(ctx, identity, lockOwner)
}

func (s *LLMSQLiteOwner) acquireSQLiteLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	if lockOwner == "" {
		return nil, runtimellm.ConversationRecord{}, errors.New("lockOwner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	effects, err := agentSessionEffects(identity.RunID)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}

	var lease *runtimesessions.Lease
	var conversation runtimellm.ConversationRecord
	handoff, err := runhandoff.ReserveCandidateHandoff(ctx)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	defer handoff.Rollback()
	if err := s.runRuntimeMutation(ctx, "sqlite session acquire", effects, func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "acquire_hydrate", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadMemorySession(txctx, tx, identity, "status IN ('active', 'suspended')")
		if err != nil {
			return err
		}
		now := s.now()
		expires := now.Add(s.sessionLockTTL)
		if !found {
			sessionID := uuid.NewString()
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO agent_sessions (
					session_id, run_id, agent_id, agent_name_owner, agent_name_source,
					agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
					conversation, turn_count, runtime_state, lease_holder, lease_expires_at,
					status, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', 0, '{}', ?, ?, 'active', ?, ?)
			`, sessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lockOwner, expires, now, now); err != nil {
				return fmt.Errorf("insert sqlite session row: %w", err)
			}
			lease = &runtimesessions.Lease{SessionID: sessionID, Identity: identity, LockOwner: lockOwner, ExpiresAt: expires}
			conversation, err = loadSQLiteExactConversationTx(txctx, tx, identity, sessionID)
			if err != nil {
				return err
			}
			_, err = s.lifecycle.RequestCompletionCandidateTx(txctx, tx, identity.RunID, &expires, handoff)
			return err
		}
		if rec.status == "suspended" {
			return runtimesessions.ErrSessionSuspended
		}
		if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != lockOwner {
			return runtimesessions.ErrSessionLeased
		}
		if _, err := tx.ExecContext(txctx, `UPDATE agent_sessions SET lease_holder=?, lease_expires_at=?, updated_at=? WHERE session_id=?`, lockOwner, expires, now, rec.sessionID); err != nil {
			return fmt.Errorf("update sqlite session lease: %w", err)
		}
		lease = &runtimesessions.Lease{
			SessionID: rec.sessionID, ProviderSessionID: rec.providerSessionID, Identity: identity,
			RetryReason: rec.retryReason, RetriesFromSessionID: rec.retriesFromSessionID,
			LockOwner: lockOwner, ExpiresAt: expires,
		}
		conversation, err = loadSQLiteExactConversationTx(txctx, tx, identity, rec.sessionID)
		if err != nil {
			return err
		}
		_, err = s.lifecycle.RequestCompletionCandidateTx(txctx, tx, identity.RunID, &expires, handoff)
		return err
	}); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	if err := handoff.Commit(); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	return lease, conversation, nil
}

func loadSQLiteExactConversationTx(ctx context.Context, tx *sql.Tx, identity agentmemory.Identity, sessionID string) (runtimellm.ConversationRecord, error) {
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return runtimellm.ConversationRecord{}, err
	}
	var rawMessages, runtimeState any
	var status string
	var turnCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT status, COALESCE(conversation, '[]'), COALESCE(runtime_state, '{}'), COALESCE(turn_count, 0)
		FROM agent_sessions
		WHERE session_id=? AND run_id=? AND agent_id=? AND agent_name_owner=?
		  AND agent_name_source=? AND agent_route_presence=? AND flow_scope_key=?
		  AND flow_instance_id=? AND flow_instance=? AND status='active'
	`, sessionID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&status, &rawMessages, &runtimeState, &turnCount); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("load exact sqlite live session conversation: %w", err)
	}
	return decodeLiveConversationRecord(identity, sessionID, status, sqliteJSONRawMessage(rawMessages), sqliteJSONRawMessage(runtimeState), turnCount)
}

func (s *LLMSQLiteOwner) Release(ctx context.Context, lease *runtimesessions.Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	effects, err := agentSessionEffects(identity.RunID)
	if err != nil {
		return err
	}
	var rows int64
	if err := runhandoff.WithCandidateHandoff(ctx, func(handoff *runhandoff.CandidateHandoff) error {
		return s.runRuntimeMutation(ctx, "sqlite session release", effects, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET lease_holder=NULL, lease_expires_at=NULL, updated_at=?
			WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=?
			  AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=?
			  AND flow_instance=? AND session_id=? AND lease_holder=? AND status='active'
		`, s.now(), identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
				fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, lease.SessionID, lease.LockOwner)
			if err == nil {
				rows, _ = res.RowsAffected()
			}
			if err != nil || rows == 0 {
				return err
			}
			_, err = s.lifecycle.RequestCompletionCandidateTx(txctx, tx, identity.RunID, nil, handoff)
			return err
		})
	}); err != nil {
		return fmt.Errorf("release sqlite session lease: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", identity.AgentID(), lease.SessionID)
	}
	return nil
}

func (s *LLMSQLiteOwner) Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation runtimesessions.RotationMetadata) (*runtimesessions.Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return nil, err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	if lockOwner == "" {
		return nil, errors.New("lockOwner is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	effects, err := agentSessionEffects(identity.RunID)
	if err != nil {
		return nil, err
	}
	var lease *runtimesessions.Lease
	if err := runhandoff.WithCandidateHandoff(ctx, func(handoff *runhandoff.CandidateHandoff) error {
		return s.runRuntimeMutation(ctx, "sqlite session rotate", effects, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "rotate", false); err != nil {
				return err
			}
			rec, found, err := sqliteLoadMemorySession(txctx, tx, identity, "status='active'")
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("no active session to rotate for agent=%s", identity.AgentID())
			}
			now := s.now()
			if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != lockOwner {
				return runtimesessions.ErrSessionLeased
			}
			retryReason := strings.TrimSpace(rotation.RetryReason)
			reason := rotation.TerminationReason
			if reason == "" {
				reason = runtimesessions.TerminationReasonContaminated
			}
			if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET status='terminated', termination_reason=?, termination_detail=?, terminated_at=COALESCE(terminated_at,?),
			successor_session_id=NULL, lease_holder=NULL, lease_expires_at=NULL, updated_at=? WHERE session_id=? AND status='active'
		`, reason.String(), sqliteNullString(retryReason), now, now, rec.sessionID); err != nil {
				return fmt.Errorf("terminate sqlite rotated session row: %w", err)
			}
			newID := uuid.NewString()
			expires := now.Add(s.sessionLockTTL)
			runtimeState := sqliteSessionRuntimeStateJSON(strings.TrimSpace(rotation.CheckpointSummary), retryReason, rec.sessionID, strings.TrimSpace(rotation.OperationID))
			if _, err := tx.ExecContext(txctx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, flow_scope_key, flow_instance_id, flow_instance, memory_enabled, memory_source,
				conversation, turn_count, runtime_state, lease_holder, lease_expires_at, status, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', 0, ?, ?, ?, 'active', ?, ?)
		`, newID, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
				fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, runtimeState, lockOwner, expires, now, now); err != nil {
				return fmt.Errorf("insert sqlite rotated successor session row: %w", err)
			}
			if _, err := tx.ExecContext(txctx, `UPDATE agent_sessions SET successor_session_id=?, updated_at=? WHERE session_id=? AND status='terminated'`, newID, now, rec.sessionID); err != nil {
				return fmt.Errorf("link sqlite rotated successor session row: %w", err)
			}
			if _, err := s.lifecycle.RequestCompletionCandidateTx(txctx, tx, identity.RunID, &expires, handoff); err != nil {
				return err
			}
			lease = &runtimesessions.Lease{SessionID: newID, Identity: identity, RetryReason: retryReason, RetriesFromSessionID: rec.sessionID, LockOwner: lockOwner, ExpiresAt: expires}
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (s *LLMSQLiteOwner) IncrementTurn(ctx context.Context, identity agentmemory.Identity, sessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return err
	}
	effects, err := agentSessionEffects(identity.RunID)
	if err != nil {
		return err
	}
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite session turn increment", effects, func(txctx context.Context, tx *sql.Tx) error {
		if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
			return err
		}
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "increment_turn", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
				UPDATE agent_sessions SET turn_count=turn_count+1, updated_at=?
				WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=?
				  AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=?
				  AND flow_instance=? AND session_id=? AND status='active'
			`, s.now(), identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
			fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, sessionID)
		if err == nil {
			rows, _ = res.RowsAffected()
		}
		return err
	}); err != nil {
		return fmt.Errorf("increment sqlite session turn: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("session not found for turn increment: run=%s agent=%s flow=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), sessionID)
	}
	return nil
}

func (s *LLMSQLiteOwner) AdoptSessionID(ctx context.Context, identity agentmemory.Identity, lockOwner, newSessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if lockOwner == "" || newSessionID == "" {
		return errors.New("lockOwner and newSessionID are required")
	}
	effects, err := agentSessionEffects(identity.RunID)
	if err != nil {
		return err
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	return runhandoff.WithCandidateHandoff(ctx, func(handoff *runhandoff.CandidateHandoff) error {
		return s.runRuntimeMutation(ctx, "sqlite adopt session id", effects, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.Agent, "adopt_provider_session", false); err != nil {
				return err
			}
			rec, found, err := sqliteLoadMemorySession(txctx, tx, identity, "status='active'")
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("no active session to adopt for agent=%s", identity.AgentID())
			}
			now := s.now()
			if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != lockOwner {
				return runtimesessions.ErrSessionLeased
			}
			expires := now.Add(s.sessionLockTTL)
			if _, err = tx.ExecContext(txctx, `UPDATE agent_sessions SET runtime_state=json_set(COALESCE(runtime_state,'{}'),'$.provider_session_id',?), lease_holder=?, lease_expires_at=?, updated_at=? WHERE session_id=?`, newSessionID, lockOwner, expires, now, rec.sessionID); err != nil {
				return err
			}
			_, err = s.lifecycle.RequestCompletionCandidateTx(txctx, tx, identity.RunID, &expires, handoff)
			return err
		})
	})
}

func (s *LLMSQLiteOwner) ResetAll(metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.backend == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	source := strings.TrimSpace(metadata.Source)
	now := s.now()
	summary := runtimesessions.ResetSummary{}
	ctx := context.Background()
	effects := emptyRunForkRevisionEffects()
	if err := runhandoff.WithCandidateHandoff(ctx, func(handoff *runhandoff.CandidateHandoff) error {
		return s.runRuntimeMutation(ctx, "sqlite session reset", effects, func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, `SELECT session_id, run_id, agent_id, flow_instance, status FROM agent_sessions WHERE status IN ('active','suspended') ORDER BY run_id, agent_id, flow_instance, session_id`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var d runtimesessions.ResetDisposition
				if err := rows.Scan(&d.SessionID, &d.RunID, &d.AgentID, &d.FlowInstance, &d.PreviousStatus); err != nil {
					return err
				}
				d.TerminationReason = runtimesessions.TerminationReasonOrphaned.String()
				d.TerminationDetail = source
				summary.OrphanedSessions = append(summary.OrphanedSessions, d)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE agent_sessions SET status='terminated', termination_reason=?, termination_detail=?, terminated_at=COALESCE(terminated_at,?), lease_holder=NULL, lease_expires_at=NULL, updated_at=? WHERE status IN ('active','suspended')`, runtimesessions.TerminationReasonOrphaned.String(), sqliteNullString(source), now, now); err != nil {
				return err
			}
			seenRuns := make(map[string]struct{}, len(summary.OrphanedSessions))
			for _, disposition := range summary.OrphanedSessions {
				if _, exists := seenRuns[disposition.RunID]; exists {
					continue
				}
				seenRuns[disposition.RunID] = struct{}{}
				if err := addAgentSessionEffect(effects, disposition.RunID); err != nil {
					return err
				}
				if _, err := s.lifecycle.RequestCompletionCandidateTx(ctx, tx, disposition.RunID, nil, handoff); err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("reset sqlite live sessions: %w", err)
	}
	return summary, nil
}

func (s *LLMSQLiteOwner) SetNowFnForTest(nowFn func() time.Time) {
	if s == nil {
		return
	}
	if nowFn == nil {
		s.nowFn = time.Now
		return
	}
	s.nowFn = nowFn
}

type sqliteSessionRow struct {
	sessionID, status, providerSessionID, retryReason, retriesFromSessionID, leaseHolder string
	leaseExpiresAt                                                                       time.Time
}

func sqliteLoadMemorySession(ctx context.Context, q rowQueryer, identity agentmemory.Identity, statusPredicate string) (sqliteSessionRow, bool, error) {
	fields, err := storeagent.IdentityFields(identity.Agent)
	if err != nil {
		return sqliteSessionRow{}, false, err
	}
	var rec sqliteSessionRow
	var leaseExpiresRaw any
	err = q.QueryRowContext(ctx, `
		SELECT session_id, status,
		       COALESCE(json_extract(runtime_state,'$.provider_session_id'),''),
		       COALESCE(json_extract(runtime_state,'$.retry_reason'),''),
		       COALESCE(json_extract(runtime_state,'$.retries_from_session_id'),''),
		       COALESCE(lease_holder,''), lease_expires_at
		FROM agent_sessions
		WHERE run_id=? AND agent_id=? AND agent_name_owner=? AND agent_name_source=?
		  AND agent_route_presence=? AND flow_scope_key=? AND flow_instance_id=?
		  AND flow_instance=? AND `+statusPredicate+`
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC LIMIT 1
	`, identity.RunID, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&rec.sessionID, &rec.status, &rec.providerSessionID, &rec.retryReason, &rec.retriesFromSessionID, &rec.leaseHolder, &leaseExpiresRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteSessionRow{}, false, nil
	}
	if err != nil {
		return sqliteSessionRow{}, false, fmt.Errorf("load sqlite memory session row: %w", err)
	}
	if at, ok, err := sqliteTimeValue(leaseExpiresRaw); err != nil {
		return sqliteSessionRow{}, false, fmt.Errorf("scan sqlite session lease expiry: %w", err)
	} else if ok {
		rec.leaseExpiresAt = at
	}
	return rec, true, nil
}

func sqliteSessionRuntimeStateJSON(summary, retryReason, retriesFromSessionID, operationID string) string {
	state := map[string]string{}
	if summary = strings.TrimSpace(summary); summary != "" {
		state["summary"] = summary
	}
	if retryReason = strings.TrimSpace(retryReason); retryReason != "" {
		state["retry_reason"] = retryReason
	}
	if retriesFromSessionID = strings.TrimSpace(retriesFromSessionID); retriesFromSessionID != "" {
		state["retries_from_session_id"] = retriesFromSessionID
	}
	if operationID = strings.TrimSpace(operationID); operationID != "" {
		state["rotation_operation_id"] = operationID
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return "{}"
	}
	return string(raw)
}
