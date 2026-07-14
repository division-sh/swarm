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
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type sqliteRuntimeStartupLease struct {
	store *SQLiteRuntimeStore
	owner string
}

func (s *SQLiteRuntimeStore) AcquireRuntimeStartupOwnership(ctx context.Context, ownerID string) (runtimestartupownership.Lease, error) {
	if s == nil || s.DB == nil {
		return nil, nil
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return nil, fmt.Errorf("runtime owner id is required")
	}
	s.startupMu.Lock()
	defer s.startupMu.Unlock()
	if s.startupOwner != "" && s.startupOwner != ownerID {
		return nil, fmt.Errorf("sqlite local runtime store already owned by another runtime instance")
	}
	s.startupOwner = ownerID
	return &sqliteRuntimeStartupLease{store: s, owner: ownerID}, nil
}

func (l *sqliteRuntimeStartupLease) Release(context.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	l.store.startupMu.Lock()
	defer l.store.startupMu.Unlock()
	if strings.TrimSpace(l.store.startupOwner) == strings.TrimSpace(l.owner) {
		l.store.startupOwner = ""
	}
	return nil
}

func (s *SQLiteRuntimeStore) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquireSQLiteLiveSession(ctx, identity, lockOwner)
	return lease, err
}

func (s *SQLiteRuntimeStore) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquireSQLiteLiveSession(ctx, identity, lockOwner)
}

func (s *SQLiteRuntimeStore) acquireSQLiteLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
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

	var lease *runtimesessions.Lease
	var conversation runtimellm.ConversationRecord
	if err := s.runRuntimeMutation(ctx, "sqlite session acquire", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "acquire_hydrate", false); err != nil {
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
					session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
					conversation, turn_count, runtime_state, lease_holder, lease_expires_at,
					status, created_at, updated_at
				) VALUES (?, ?, ?, ?, 1, 'authored', '[]', 0, '{}', ?, ?, 'active', ?, ?)
			`, sessionID, identity.RunID, identity.AgentID, identity.FlowInstance, lockOwner, expires, now, now); err != nil {
				return fmt.Errorf("insert sqlite session row: %w", err)
			}
			lease = &runtimesessions.Lease{SessionID: sessionID, Identity: identity, LockOwner: lockOwner, ExpiresAt: expires}
			conversation, err = loadSQLiteExactConversationTx(txctx, tx, identity, sessionID)
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
		return err
	}); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	return lease, conversation, nil
}

func loadSQLiteExactConversationTx(ctx context.Context, tx *sql.Tx, identity agentmemory.Identity, sessionID string) (runtimellm.ConversationRecord, error) {
	var rawMessages, runtimeState any
	var status string
	var turnCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT status, COALESCE(conversation, '[]'), COALESCE(runtime_state, '{}'), COALESCE(turn_count, 0)
		FROM agent_sessions
		WHERE session_id=? AND run_id=? AND agent_id=? AND flow_instance=? AND status='active'
	`, sessionID, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&status, &rawMessages, &runtimeState, &turnCount); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("load exact sqlite live session conversation: %w", err)
	}
	return decodeLiveConversationRecord(identity, sessionID, status, sqliteJSONRawMessage(rawMessages), sqliteJSONRawMessage(runtimeState), turnCount)
}

func (s *SQLiteRuntimeStore) Release(ctx context.Context, lease *runtimesessions.Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	identity := lease.Identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite session release", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions SET lease_holder=NULL, lease_expires_at=NULL, updated_at=?
			WHERE run_id=? AND agent_id=? AND flow_instance=? AND session_id=? AND lease_holder=? AND status='active'
		`, s.now(), identity.RunID, identity.AgentID, identity.FlowInstance, lease.SessionID, lease.LockOwner)
		if err == nil {
			rows, _ = res.RowsAffected()
		}
		return err
	}); err != nil {
		return fmt.Errorf("release sqlite session lease: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", identity.AgentID, lease.SessionID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation runtimesessions.RotationMetadata) (*runtimesessions.Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
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
	var lease *runtimesessions.Lease
	if err := s.runRuntimeMutation(ctx, "sqlite session rotate", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "rotate", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadMemorySession(txctx, tx, identity, "status='active'")
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("no active session to rotate for agent=%s", identity.AgentID)
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
				session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
				conversation, turn_count, runtime_state, lease_holder, lease_expires_at, status, created_at, updated_at
			) VALUES (?, ?, ?, ?, 1, 'authored', '[]', 0, ?, ?, ?, 'active', ?, ?)
		`, newID, identity.RunID, identity.AgentID, identity.FlowInstance, runtimeState, lockOwner, expires, now, now); err != nil {
			return fmt.Errorf("insert sqlite rotated successor session row: %w", err)
		}
		if _, err := tx.ExecContext(txctx, `UPDATE agent_sessions SET successor_session_id=?, updated_at=? WHERE session_id=? AND status='terminated'`, newID, now, rec.sessionID); err != nil {
			return fmt.Errorf("link sqlite rotated successor session row: %w", err)
		}
		lease = &runtimesessions.Lease{SessionID: newID, Identity: identity, RetryReason: retryReason, RetriesFromSessionID: rec.sessionID, LockOwner: lockOwner, ExpiresAt: expires}
		return nil
	}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (s *SQLiteRuntimeStore) IncrementTurn(ctx context.Context, identity agentmemory.Identity, sessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite session turn increment", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "increment_turn", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `UPDATE agent_sessions SET turn_count=turn_count+1, updated_at=? WHERE run_id=? AND agent_id=? AND flow_instance=? AND session_id=? AND status='active'`, s.now(), identity.RunID, identity.AgentID, identity.FlowInstance, sessionID)
		if err == nil {
			rows, _ = res.RowsAffected()
		}
		return err
	}); err != nil {
		return fmt.Errorf("increment sqlite session turn: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("session not found for turn increment: run=%s agent=%s flow=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, sessionID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) AdoptSessionID(ctx context.Context, identity agentmemory.Identity, lockOwner, newSessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if lockOwner == "" || newSessionID == "" {
		return errors.New("lockOwner and newSessionID are required")
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	return s.runRuntimeMutation(ctx, "sqlite adopt session id", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, identity.AgentID, "adopt_provider_session", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadMemorySession(txctx, tx, identity, "status='active'")
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("no active session to adopt for agent=%s", identity.AgentID)
		}
		now := s.now()
		if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != lockOwner {
			return runtimesessions.ErrSessionLeased
		}
		_, err = tx.ExecContext(txctx, `UPDATE agent_sessions SET runtime_state=json_set(COALESCE(runtime_state,'{}'),'$.provider_session_id',?), lease_holder=?, lease_expires_at=?, updated_at=? WHERE session_id=?`, newSessionID, lockOwner, now.Add(s.sessionLockTTL), now, rec.sessionID)
		return err
	})
}

func (s *SQLiteRuntimeStore) ResetAll(metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.DB == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	source := strings.TrimSpace(metadata.Source)
	now := s.now()
	summary := runtimesessions.ResetSummary{}
	if err := s.runRuntimeMutation(context.Background(), "sqlite session reset", func(ctx context.Context, tx *sql.Tx) error {
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
		_, err = tx.ExecContext(ctx, `UPDATE agent_sessions SET status='terminated', termination_reason=?, termination_detail=?, terminated_at=COALESCE(terminated_at,?), lease_holder=NULL, lease_expires_at=NULL, updated_at=? WHERE status IN ('active','suspended')`, runtimesessions.TerminationReasonOrphaned.String(), sqliteNullString(source), now, now)
		return err
	}); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("reset sqlite live sessions: %w", err)
	}
	return summary, nil
}

func (s *SQLiteRuntimeStore) SetNowFnForTest(nowFn func() time.Time) {
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
	var rec sqliteSessionRow
	var leaseExpiresRaw any
	err := q.QueryRowContext(ctx, `
		SELECT session_id, status,
		       COALESCE(json_extract(runtime_state,'$.provider_session_id'),''),
		       COALESCE(json_extract(runtime_state,'$.retry_reason'),''),
		       COALESCE(json_extract(runtime_state,'$.retries_from_session_id'),''),
		       COALESCE(lease_holder,''), lease_expires_at
		FROM agent_sessions
		WHERE run_id=? AND agent_id=? AND flow_instance=? AND `+statusPredicate+`
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC LIMIT 1
	`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&rec.sessionID, &rec.status, &rec.providerSessionID, &rec.retryReason, &rec.retriesFromSessionID, &rec.leaseHolder, &leaseExpiresRaw)
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
