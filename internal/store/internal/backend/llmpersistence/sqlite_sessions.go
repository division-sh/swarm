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
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
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
	fields, err := storeagent.IdentityFields(identity)
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
	if err := s.requireCurrentSchema(); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	type acquired struct {
		lease        *runtimesessions.Lease
		conversation runtimellm.ConversationRecord
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite session acquire", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (acquired, error) {
		var value acquired
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity, "acquire_hydrate", false); err != nil {
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
				if err := addAgentSessionFacts(attempt, identity.RunID, sessionID); err != nil {
					return err
				}
				value.lease = &runtimesessions.Lease{SessionID: sessionID, Identity: identity, LockOwner: lockOwner, ExpiresAt: expires}
				value.conversation, err = loadSQLiteExactConversationTx(txctx, tx, identity, sessionID)
				if err != nil {
					return err
				}
				_, err = attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, &expires)
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
			if err := addAgentSessionFacts(attempt, identity.RunID, rec.sessionID); err != nil {
				return err
			}
			value.lease = &runtimesessions.Lease{
				SessionID: rec.sessionID, ProviderSessionID: rec.providerSessionID, Identity: identity,
				RetryReason: rec.retryReason, RetriesFromSessionID: rec.retriesFromSessionID,
				LockOwner: lockOwner, ExpiresAt: expires,
			}
			value.conversation, err = loadSQLiteExactConversationTx(txctx, tx, identity, rec.sessionID)
			if err != nil {
				return err
			}
			_, err = attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, &expires)
			return err
		})
		return value, err
	})
	if value, ok := result.Value(); ok {
		return value.lease, value.conversation, result.Err()
	}
	return nil, runtimellm.ConversationRecord{}, result.Err()
}

func loadSQLiteExactConversationTx(ctx context.Context, tx *sql.Tx, identity agentmemory.Identity, sessionID string) (runtimellm.ConversationRecord, error) {
	fields, err := storeagent.IdentityFields(identity)
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

// ReleaseOutcome separates a durably released lease from postcommit handoff errors.
func (s *LLMSQLiteOwner) ReleaseOutcome(ctx context.Context, lease *runtimesessions.Lease) (runtimesessions.ReleaseResult, error) {
	if lease == nil {
		return runtimesessions.ReleaseResult{}, errors.New("nil lease")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimesessions.ReleaseResult{}, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return runtimesessions.ReleaseResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimesessions.ReleaseResult{}, err
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite session release", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (int64, error) {
		var rows int64
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
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
			if err := addAgentSessionFacts(attempt, identity.RunID, lease.SessionID); err != nil {
				return err
			}
			_, err = attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, nil)
			return err
		})
		return rows, err
	})
	if !result.Acknowledged() {
		return runtimesessions.ReleaseResult{}, fmt.Errorf("release sqlite session lease: %w", result.Err())
	}
	rows, _ := result.Value()
	if rows == 0 {
		return runtimesessions.ReleaseResult{}, errors.Join(fmt.Errorf("no active lease to release for agent=%s session=%s", identity.AgentID(), lease.SessionID), result.Err())
	}
	if err := result.Err(); err != nil {
		return runtimesessions.ReleaseResult{Acknowledged: true}, fmt.Errorf("release sqlite session lease committed with postcommit error: %w", err)
	}
	return runtimesessions.ReleaseResult{Acknowledged: true}, nil
}

func (s *LLMSQLiteOwner) Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation runtimesessions.RotationMetadata) (*runtimesessions.Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	fields, err := storeagent.IdentityFields(identity)
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
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite session rotate", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (*runtimesessions.Lease, error) {
		var lease *runtimesessions.Lease
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity, "rotate", false); err != nil {
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
			if _, err := attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, &expires); err != nil {
				return err
			}
			lease = &runtimesessions.Lease{SessionID: newID, Identity: identity, RetryReason: retryReason, RetriesFromSessionID: rec.sessionID, LockOwner: lockOwner, ExpiresAt: expires}
			return addAgentSessionFacts(attempt, identity.RunID, rec.sessionID, newID)
		})
		return lease, err
	})
	if lease, ok := result.Value(); ok {
		return lease, result.Err()
	}
	return nil, result.Err()
}

func (s *LLMSQLiteOwner) IncrementTurnOutcome(ctx context.Context, identity agentmemory.Identity, sessionID string) (runtimesessions.TurnIncrementResult, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return runtimesessions.TurnIncrementResult{}, err
	}
	fields, err := storeagent.IdentityFields(identity)
	if err != nil {
		return runtimesessions.TurnIncrementResult{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimesessions.TurnIncrementResult{}, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite session turn increment", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (int64, error) {
		var rows int64
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity, "increment_turn", false); err != nil {
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
			if err != nil || rows == 0 {
				return err
			}
			if err := addAgentSessionFacts(attempt, identity.RunID, sessionID); err != nil {
				return err
			}
			var expiryRaw any
			if err := tx.QueryRowContext(txctx, `SELECT lease_expires_at FROM agent_sessions WHERE session_id=?`, sessionID).Scan(&expiryRaw); err != nil {
				return err
			}
			var nextWake *time.Time
			if expiry, valid, err := sqliteTimeValue(expiryRaw); err != nil {
				return err
			} else if valid {
				nextWake = &expiry
			}
			_, err = attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, nextWake)
			return err
		})
		return rows, err
	})
	if !result.Acknowledged() {
		return runtimesessions.TurnIncrementResult{}, fmt.Errorf("increment sqlite session turn: %w", result.Err())
	}
	rows, _ := result.Value()
	if rows == 0 {
		return runtimesessions.TurnIncrementResult{}, errors.Join(fmt.Errorf("session not found for turn increment: run=%s agent=%s flow=%s session=%s", identity.RunID, identity.AgentID(), identity.FlowInstance(), sessionID), result.Err())
	}
	return runtimesessions.TurnIncrementResult{Acknowledged: true}, result.Err()
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
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite adopt session id", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, identity.RunID); err != nil {
				return err
			}
			if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity, "adopt_provider_session", false); err != nil {
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
			if err := addAgentSessionFacts(attempt, identity.RunID, rec.sessionID); err != nil {
				return err
			}
			_, err = attempt.RequestCompletion(txctx, s.lifecycle, identity.RunID, &expires)
			return err
		})
		return struct{}{}, err
	})
	return result.Err()
}

func (s *LLMSQLiteOwner) ResetAll(metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.backend == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimesessions.ResetSummary{}, err
	}
	source := strings.TrimSpace(metadata.Source)
	now := s.now()
	ctx := context.Background()
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite session reset", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimesessions.ResetSummary, error) {
		var summary runtimesessions.ResetSummary
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
				if err := addAgentSessionFacts(attempt, disposition.RunID, disposition.SessionID); err != nil {
					return err
				}
				if _, exists := seenRuns[disposition.RunID]; exists {
					continue
				}
				seenRuns[disposition.RunID] = struct{}{}
				if _, err := attempt.RequestCompletion(ctx, s.lifecycle, disposition.RunID, nil); err != nil {
					return err
				}
			}
			return nil
		})
		return summary, err
	})
	if result.Err() != nil {
		if !result.Acknowledged() {
			return runtimesessions.ResetSummary{}, fmt.Errorf("reset sqlite live sessions: %w", result.Err())
		}
		summary, _ := result.Value()
		return summary, fmt.Errorf("reset sqlite live sessions: %w", result.Err())
	}
	summary, _ := result.Value()
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
	fields, err := storeagent.IdentityFields(identity)
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
