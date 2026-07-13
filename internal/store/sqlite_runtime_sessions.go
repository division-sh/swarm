package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

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

func (s *SQLiteRuntimeStore) Acquire(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquireSQLiteLiveSession(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
	return lease, err
}

func (s *SQLiteRuntimeStore) AcquireLiveSession(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquireSQLiteLiveSession(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
}

func (s *SQLiteRuntimeStore) acquireSQLiteLiveSession(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	if strings.TrimSpace(agentID) == "" || runtimeMode == "" || strings.TrimSpace(lockOwner) == "" {
		return nil, runtimellm.ConversationRecord{}, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	if resolved.Stateless {
		return nil, runtimellm.ConversationRecord{}, errors.New("task-scoped sessions are stateless")
	}

	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	var lease *runtimesessions.Lease
	var conversation runtimellm.ConversationRecord
	if err := s.runRuntimeMutation(ctx, "sqlite session acquire", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, strings.TrimSpace(agentID), "acquire_hydrate", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadLiveSession(txctx, tx, strings.TrimSpace(agentID), resolved.RuntimeMode, resolved.ScopeKey)
		if err != nil {
			return err
		}
		now := s.now()
		expires := now.Add(s.sessionLockTTL)
		if !found {
			sessionID := uuid.NewString()
			if _, err := tx.ExecContext(txctx, `
				INSERT INTO agent_sessions (
					session_id, agent_id, entity_id, flow_instance, scope_key, scope,
					conversation, turn_count, runtime_mode, runtime_state,
					lease_holder, lease_expires_at, status, created_at, updated_at
				)
				VALUES (?, ?, ?, ?, ?, ?, '[]', 0, ?, '{}', ?, ?, 'active', ?, ?)
			`, sessionID, strings.TrimSpace(agentID), sqliteNullUUID(resolved.EntityID), sqliteNullString(resolved.FlowInstance),
				resolved.ScopeKey, resolved.Scope.String(), resolved.RuntimeMode.String(), strings.TrimSpace(lockOwner), expires, now, now); err != nil {
				return fmt.Errorf("insert sqlite session row: %w", err)
			}
			lease = &runtimesessions.Lease{
				SessionID:    sessionID,
				AgentID:      strings.TrimSpace(agentID),
				RuntimeMode:  resolved.RuntimeMode,
				SessionScope: resolved.Scope,
				LockOwner:    strings.TrimSpace(lockOwner),
				ScopeKey:     resolved.ScopeKey,
				ExpiresAt:    expires,
			}
			var decodeErr error
			conversation, decodeErr = loadSQLiteExactConversationTx(txctx, tx, strings.TrimSpace(agentID), resolved, sessionID)
			return decodeErr
		}
		if strings.TrimSpace(rec.status) == "suspended" {
			return runtimesessions.ErrSessionSuspended
		}
		if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != strings.TrimSpace(lockOwner) {
			return runtimesessions.ErrSessionLeased
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET lease_holder = ?, lease_expires_at = ?, updated_at = ?
			WHERE session_id = ?
		`, strings.TrimSpace(lockOwner), expires, now, rec.sessionID); err != nil {
			return fmt.Errorf("update sqlite session lease: %w", err)
		}
		lease = &runtimesessions.Lease{
			SessionID:            rec.sessionID,
			ProviderSessionID:    rec.providerSessionID,
			AgentID:              strings.TrimSpace(agentID),
			RuntimeMode:          resolved.RuntimeMode,
			SessionScope:         resolved.Scope,
			RetryReason:          rec.retryReason,
			RetriesFromSessionID: rec.retriesFromSessionID,
			LockOwner:            strings.TrimSpace(lockOwner),
			ScopeKey:             resolved.ScopeKey,
			ExpiresAt:            expires,
		}
		var decodeErr error
		conversation, decodeErr = loadSQLiteExactConversationTx(txctx, tx, strings.TrimSpace(agentID), resolved, rec.sessionID)
		return decodeErr
	}); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	return lease, conversation, nil
}

func loadSQLiteExactConversationTx(ctx context.Context, tx *sql.Tx, agentID string, resolved runtimesessions.ResolvedScope, sessionID string) (runtimellm.ConversationRecord, error) {
	var rawMessages, runtimeState any
	var scopeKey, status, runID string
	var turnCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT scope_key, status, COALESCE(run_id, ''), COALESCE(conversation, '[]'), COALESCE(runtime_state, '{}'), COALESCE(turn_count, 0)
		FROM agent_sessions
		WHERE session_id = ? AND agent_id = ? AND runtime_mode = ? AND scope_key = ? AND status = 'active'
	`, sessionID, agentID, resolved.RuntimeMode.String(), resolved.ScopeKey).Scan(&scopeKey, &status, &runID, &rawMessages, &runtimeState, &turnCount); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("load exact sqlite live session conversation: %w", err)
	}
	return decodeLiveConversationRecord(agentID, resolved.RuntimeMode.String(), resolved.Scope.String(), sessionID, scopeKey, status, runID, sqliteJSONRawMessage(rawMessages), sqliteJSONRawMessage(runtimeState), turnCount)
}

func (s *SQLiteRuntimeStore) Release(ctx context.Context, lease *runtimesessions.Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite session release", func(txctx context.Context, tx *sql.Tx) error {
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET lease_holder = NULL,
			    lease_expires_at = NULL,
			    updated_at = ?
			WHERE agent_id = ?
			  AND runtime_mode = ?
			  AND session_id = ?
			  AND scope_key = ?
			  AND lease_holder = ?
			  AND status = 'active'
		`, s.now(), strings.TrimSpace(lease.AgentID), lease.RuntimeMode.String(), strings.TrimSpace(lease.SessionID), strings.TrimSpace(lease.ScopeKey), strings.TrimSpace(lease.LockOwner))
		if err != nil {
			return err
		}
		rows, _ = res.RowsAffected()
		return nil
	}); err != nil {
		return fmt.Errorf("release sqlite session lease: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", lease.AgentID, lease.SessionID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) Rotate(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner string, rotation runtimesessions.RotationMetadata, scopeKey string) (*runtimesessions.Lease, error) {
	if strings.TrimSpace(agentID) == "" || runtimeMode == "" || strings.TrimSpace(lockOwner) == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}

	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	var lease *runtimesessions.Lease
	if err := s.runRuntimeMutation(ctx, "sqlite session rotate", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, strings.TrimSpace(agentID), "rotate", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadActiveSession(txctx, tx, strings.TrimSpace(agentID), resolved.RuntimeMode, resolved.ScopeKey)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("no active session to rotate for agent=%s", strings.TrimSpace(agentID))
		}
		now := s.now()
		if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != strings.TrimSpace(lockOwner) {
			return runtimesessions.ErrSessionLeased
		}
		retryReason := strings.TrimSpace(rotation.RetryReason)
		terminationReason := rotation.TerminationReason
		if terminationReason == "" {
			terminationReason = runtimesessions.TerminationReasonContaminated
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET status = 'terminated',
			    termination_reason = ?,
			    termination_detail = ?,
			    terminated_at = COALESCE(terminated_at, ?),
			    successor_session_id = NULL,
			    lease_holder = NULL,
			    lease_expires_at = NULL,
			    updated_at = ?
			WHERE session_id = ?
			  AND status = 'active'
		`, terminationReason.String(), sqliteNullString(retryReason), now, now, rec.sessionID); err != nil {
			return fmt.Errorf("terminate sqlite rotated session row: %w", err)
		}
		newSessionID := uuid.NewString()
		expires := now.Add(s.sessionLockTTL)
		runtimeState := sqliteSessionRuntimeStateJSON(strings.TrimSpace(rotation.CheckpointSummary), retryReason, rec.sessionID)
		if _, err := tx.ExecContext(txctx, `
			INSERT INTO agent_sessions (
				session_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state,
				lease_holder, lease_expires_at, status,
				created_at, updated_at
			)
			VALUES (?, ?, ?, ?, ?, ?, '[]', 0, ?, ?, ?, ?, 'active', ?, ?)
		`, newSessionID, strings.TrimSpace(agentID), sqliteNullUUID(resolved.EntityID), sqliteNullString(resolved.FlowInstance),
			resolved.ScopeKey, resolved.Scope.String(), resolved.RuntimeMode.String(), runtimeState, strings.TrimSpace(lockOwner), expires, now, now); err != nil {
			return fmt.Errorf("insert sqlite rotated successor session row: %w", err)
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET successor_session_id = ?, updated_at = ?
			WHERE session_id = ?
			  AND status = 'terminated'
		`, newSessionID, now, rec.sessionID); err != nil {
			return fmt.Errorf("link sqlite rotated successor session row: %w", err)
		}
		lease = &runtimesessions.Lease{
			SessionID:            newSessionID,
			AgentID:              strings.TrimSpace(agentID),
			RuntimeMode:          resolved.RuntimeMode,
			SessionScope:         resolved.Scope,
			RetryReason:          retryReason,
			RetriesFromSessionID: rec.sessionID,
			LockOwner:            strings.TrimSpace(lockOwner),
			ScopeKey:             resolved.ScopeKey,
			ExpiresAt:            expires,
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return lease, nil
}

func (s *SQLiteRuntimeStore) IncrementTurn(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, sessionID, scopeKey string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	var rows int64
	if err := s.runRuntimeMutation(ctx, "sqlite session turn increment", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, strings.TrimSpace(agentID), "increment_turn", false); err != nil {
			return err
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET turn_count = turn_count + 1,
			    updated_at = ?
			WHERE agent_id = ?
			  AND runtime_mode = ?
			  AND session_id = ?
			  AND scope_key = ?
			  AND status = 'active'
		`, s.now(), strings.TrimSpace(agentID), resolved.RuntimeMode.String(), strings.TrimSpace(sessionID), resolved.ScopeKey)
		if err != nil {
			return err
		}
		rows, _ = res.RowsAffected()
		return nil
	}); err != nil {
		return fmt.Errorf("increment sqlite session turn: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("session not found for turn increment: agent=%s runtime=%s scope=%s session=%s", agentID, runtimeMode, scopeKey, sessionID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) AdoptSessionID(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	if resolved.Stateless {
		return nil
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	return s.runRuntimeMutation(ctx, "sqlite adopt session id", func(txctx context.Context, tx *sql.Tx) error {
		if _, err := requireSQLiteLiveSessionAuthority(txctx, tx, agentID, "adopt_provider_session", false); err != nil {
			return err
		}
		rec, found, err := sqliteLoadActiveSession(txctx, tx, agentID, resolved.RuntimeMode, resolved.ScopeKey)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("no active session to adopt for agent=%s", agentID)
		}
		now := s.now()
		if rec.leaseHolder != "" && rec.leaseExpiresAt.After(now) && rec.leaseHolder != lockOwner {
			return runtimesessions.ErrSessionLeased
		}
		if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET runtime_state = json_set(COALESCE(runtime_state, '{}'), '$.provider_session_id', ?),
			    lease_holder = ?,
			    lease_expires_at = ?,
			    updated_at = ?
			WHERE session_id = ?
		`, newSessionID, lockOwner, now.Add(s.sessionLockTTL), now, rec.sessionID); err != nil {
			return fmt.Errorf("update sqlite provider session id: %w", err)
		}
		return nil
	})
}

func (s *SQLiteRuntimeStore) ResetAll(runtimeMode runtimesessions.RuntimeMode, metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.DB == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	source := strings.TrimSpace(metadata.Source)
	now := s.now()
	args := []any{runtimesessions.TerminationReasonOrphaned.String(), sqliteNullString(source), now, now}
	where := "status IN ('active', 'suspended') AND runtime_mode IN ('session', 'session_per_entity')"
	if runtimeMode == runtimesessions.RuntimeModeTask {
		return runtimesessions.ResetSummary{}, nil
	}
	if runtimeMode != "" {
		where += " AND runtime_mode = ?"
		args = append(args, runtimeMode.String())
	}
	summary := runtimesessions.ResetSummary{}
	ctx := context.Background()
	if err := s.runRuntimeMutation(ctx, "sqlite session reset", func(txctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(txctx, `
			SELECT session_id, agent_id, scope_key, runtime_mode, status
			FROM agent_sessions
			WHERE `+where+`
			ORDER BY agent_id ASC, scope_key ASC, session_id ASC
		`, args[4:]...)
		if err != nil {
			return fmt.Errorf("list sqlite sessions for reset: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var d runtimesessions.ResetDisposition
			if err := rows.Scan(&d.SessionID, &d.AgentID, &d.ScopeKey, &d.RuntimeMode, &d.PreviousStatus); err != nil {
				return fmt.Errorf("scan sqlite session reset summary: %w", err)
			}
			d.TerminationReason = runtimesessions.TerminationReasonOrphaned.String()
			d.TerminationDetail = source
			summary.OrphanedSessions = append(summary.OrphanedSessions, d)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read sqlite session reset summary: %w", err)
		}
		updateArgs := append([]any{}, args...)
		if _, err := tx.ExecContext(txctx, `
			UPDATE agent_sessions
			SET status = 'terminated',
			    termination_reason = ?,
			    termination_detail = ?,
			    terminated_at = COALESCE(terminated_at, ?),
			    lease_holder = NULL,
			    lease_expires_at = NULL,
			    updated_at = ?
			WHERE `+where, updateArgs...); err != nil {
			return fmt.Errorf("reset sqlite sessions: %w", err)
		}
		return nil
	}); err != nil {
		return runtimesessions.ResetSummary{}, err
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

func (s *SQLiteRuntimeStore) ScopeKeyEnabledForTest() bool { return true }

type sqliteSessionRow struct {
	sessionID            string
	status               string
	providerSessionID    string
	retryReason          string
	retriesFromSessionID string
	leaseHolder          string
	leaseExpiresAt       time.Time
}

func sqliteLoadLiveSession(ctx context.Context, q rowQueryer, agentID string, runtimeMode runtimesessions.RuntimeMode, scopeKey string) (sqliteSessionRow, bool, error) {
	return sqliteLoadSession(ctx, q, agentID, runtimeMode, scopeKey, "status IN ('active', 'suspended')")
}

func sqliteLoadActiveSession(ctx context.Context, q rowQueryer, agentID string, runtimeMode runtimesessions.RuntimeMode, scopeKey string) (sqliteSessionRow, bool, error) {
	return sqliteLoadSession(ctx, q, agentID, runtimeMode, scopeKey, "status = 'active'")
}

func sqliteLoadSession(ctx context.Context, q rowQueryer, agentID string, runtimeMode runtimesessions.RuntimeMode, scopeKey, statusPredicate string) (sqliteSessionRow, bool, error) {
	var rec sqliteSessionRow
	var leaseExpiresRaw any
	err := q.QueryRowContext(ctx, `
		SELECT
			session_id,
			status,
			COALESCE(json_extract(runtime_state, '$.provider_session_id'), ''),
			COALESCE(json_extract(runtime_state, '$.retry_reason'), ''),
			COALESCE(json_extract(runtime_state, '$.retries_from_session_id'), ''),
			COALESCE(lease_holder, ''),
			lease_expires_at
		FROM agent_sessions
		WHERE agent_id = ?
		  AND scope_key = ?
		  AND runtime_mode = ?
		  AND `+statusPredicate+`
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC
		LIMIT 1
	`, agentID, scopeKey, runtimeMode.String()).Scan(
		&rec.sessionID,
		&rec.status,
		&rec.providerSessionID,
		&rec.retryReason,
		&rec.retriesFromSessionID,
		&rec.leaseHolder,
		&leaseExpiresRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return sqliteSessionRow{}, false, nil
	}
	if err != nil {
		return sqliteSessionRow{}, false, fmt.Errorf("load sqlite session row: %w", err)
	}
	if at, ok, err := sqliteTimeValue(leaseExpiresRaw); err != nil {
		return sqliteSessionRow{}, false, fmt.Errorf("scan sqlite session lease expiry: %w", err)
	} else if ok {
		rec.leaseExpiresAt = at
	}
	rec.sessionID = strings.TrimSpace(rec.sessionID)
	rec.status = strings.TrimSpace(rec.status)
	rec.providerSessionID = strings.TrimSpace(rec.providerSessionID)
	rec.retryReason = strings.TrimSpace(rec.retryReason)
	rec.retriesFromSessionID = strings.TrimSpace(rec.retriesFromSessionID)
	rec.leaseHolder = strings.TrimSpace(rec.leaseHolder)
	return rec, true, nil
}

func sqliteSessionRuntimeStateJSON(summary, retryReason, retriesFromSessionID string) string {
	parts := make([]string, 0, 3)
	if summary = strings.TrimSpace(summary); summary != "" {
		parts = append(parts, fmt.Sprintf("%q:%q", "summary", summary))
	}
	if retryReason = strings.TrimSpace(retryReason); retryReason != "" {
		parts = append(parts, fmt.Sprintf("%q:%q", "retry_reason", retryReason))
	}
	if retriesFromSessionID = strings.TrimSpace(retriesFromSessionID); retriesFromSessionID != "" {
		parts = append(parts, fmt.Sprintf("%q:%q", "retries_from_session_id", retriesFromSessionID))
	}
	if len(parts) == 0 {
		return "{}"
	}
	return "{" + strings.Join(parts, ",") + "}"
}
