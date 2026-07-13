package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

var _ runtimesessions.Registry = (*PostgresStore)(nil)
var _ runtimellm.LiveSessionAcquirer = (*PostgresStore)(nil)

func (s *PostgresStore) Acquire(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquirePostgresLiveSession(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
	return lease, err
}

func (s *PostgresStore) AcquireLiveSession(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquirePostgresLiveSession(ctx, agentID, runtimeMode, sessionScope, lockOwner, scopeKey)
}

func (s *PostgresStore) acquirePostgresLiveSession(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, scopeKey string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
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
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, fmt.Errorf("begin live session acquire: %w", err)
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, agentID, "acquire_hydrate", false); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}

	type row struct {
		sessionID, scopeKey, status, runID string
		providerSessionID, retryReason     sql.NullString
		retriesFrom, leaseHolder           sql.NullString
		leaseExpires                       sql.NullTime
		conversation, runtimeState         []byte
		turnCount                          int
	}
	var current row
	err = tx.QueryRowContext(ctx, `
		SELECT session_id::text, scope_key, status, COALESCE(run_id::text, ''),
		       NULLIF(runtime_state->>'provider_session_id', ''), NULLIF(runtime_state->>'retry_reason', ''),
		       NULLIF(runtime_state->>'retries_from_session_id', ''), lease_holder, lease_expires_at,
		       COALESCE(conversation, '[]'::jsonb), COALESCE(runtime_state, '{}'::jsonb), COALESCE(turn_count, 0)
		FROM agent_sessions
		WHERE agent_id = $1 AND scope_key = $2 AND runtime_mode = $3 AND status IN ('active', 'suspended')
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey, resolved.RuntimeMode.String()).Scan(
		&current.sessionID, &current.scopeKey, &current.status, &current.runID,
		&current.providerSessionID, &current.retryReason, &current.retriesFrom,
		&current.leaseHolder, &current.leaseExpires, &current.conversation, &current.runtimeState, &current.turnCount,
	)
	now := time.Now().UTC()
	if errors.Is(err, sql.ErrNoRows) {
		current.sessionID = uuid.NewString()
		current.scopeKey = resolved.ScopeKey
		current.status = "active"
		current.conversation = []byte("[]")
		current.runtimeState = []byte("{}")
		current.leaseHolder = sql.NullString{String: lockOwner, Valid: true}
		current.leaseExpires = sql.NullTime{Time: now.Add(s.postgresSessionLockTTL()), Valid: true}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, agent_id, entity_id, flow_instance, scope_key, scope, conversation, turn_count,
				runtime_mode, runtime_state, lease_holder, lease_expires_at, status, created_at, updated_at
			) VALUES ($1::uuid, $2, NULLIF($3, '')::uuid, NULLIF($4, ''), $5, $6, '[]'::jsonb, 0, $7, '{}'::jsonb, $8, $9, 'active', $10, $10)
		`, current.sessionID, agentID, resolved.EntityID, resolved.FlowInstance, resolved.ScopeKey, resolved.Scope.String(), resolved.RuntimeMode.String(), lockOwner, current.leaseExpires.Time, now); err != nil {
			return nil, runtimellm.ConversationRecord{}, fmt.Errorf("insert live session: %w", err)
		}
	} else if err != nil {
		return nil, runtimellm.ConversationRecord{}, fmt.Errorf("load live session: %w", err)
	} else {
		if current.status == "suspended" {
			return nil, runtimellm.ConversationRecord{}, runtimesessions.ErrSessionSuspended
		}
		if current.leaseHolder.Valid && current.leaseExpires.Valid && current.leaseExpires.Time.After(now) && current.leaseHolder.String != lockOwner {
			return nil, runtimellm.ConversationRecord{}, runtimesessions.ErrSessionLeased
		}
		current.leaseHolder = sql.NullString{String: lockOwner, Valid: true}
		current.leaseExpires = sql.NullTime{Time: now.Add(s.postgresSessionLockTTL()), Valid: true}
		if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET lease_holder=$1, lease_expires_at=$2, updated_at=$3 WHERE session_id=$4::uuid`, lockOwner, current.leaseExpires.Time, now, current.sessionID); err != nil {
			return nil, runtimellm.ConversationRecord{}, fmt.Errorf("update live session lease: %w", err)
		}
	}
	record, err := decodeLiveConversationRecord(agentID, resolved.RuntimeMode.String(), resolved.Scope.String(), current.sessionID, current.scopeKey, current.status, current.runID, current.conversation, current.runtimeState, current.turnCount)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	lease := &runtimesessions.Lease{
		SessionID: current.sessionID, ProviderSessionID: strings.TrimSpace(current.providerSessionID.String), AgentID: agentID,
		RuntimeMode: resolved.RuntimeMode, SessionScope: resolved.Scope, RetryReason: strings.TrimSpace(current.retryReason.String),
		RetriesFromSessionID: strings.TrimSpace(current.retriesFrom.String), LockOwner: lockOwner,
		ScopeKey: resolved.ScopeKey, ExpiresAt: current.leaseExpires.Time,
	}
	if err := tx.Commit(); err != nil {
		return nil, runtimellm.ConversationRecord{}, fmt.Errorf("commit live session acquire: %w", err)
	}
	return lease, record, nil
}

func (s *PostgresStore) Release(ctx context.Context, lease *runtimesessions.Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE agent_sessions SET lease_holder=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE agent_id=$1 AND runtime_mode=$2 AND session_id=$3::uuid AND scope_key=$4 AND lease_holder=$5 AND status='active'
	`, lease.AgentID, lease.RuntimeMode.String(), lease.SessionID, strings.TrimSpace(lease.ScopeKey), lease.LockOwner)
	if err != nil {
		return fmt.Errorf("release live session lease: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", lease.AgentID, lease.SessionID)
	}
	return nil
}

func (s *PostgresStore) Rotate(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner string, rotation runtimesessions.RotationMetadata, scopeKey string) (*runtimesessions.Lease, error) {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, agentID, "rotate", false); err != nil {
		return nil, err
	}
	var currentID string
	var existingOwner sql.NullString
	var existingExpiry sql.NullTime
	var runtimeStateRaw []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id::text, lease_holder, lease_expires_at, runtime_state
		FROM agent_sessions WHERE agent_id=$1 AND scope_key=$2 AND runtime_mode=$3 AND status='active'
		ORDER BY created_at DESC LIMIT 1 FOR UPDATE
	`, agentID, resolved.ScopeKey, resolved.RuntimeMode.String()).Scan(&currentID, &existingOwner, &existingExpiry, &runtimeStateRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("no active session to rotate for agent=%s", agentID)
		}
		return nil, err
	}
	operationID := strings.TrimSpace(rotation.OperationID)
	if operationID != "" {
		var state map[string]any
		if err := json.Unmarshal(runtimeStateRaw, &state); err != nil {
			return nil, fmt.Errorf("decode active session runtime state: %w", err)
		}
		if strings.TrimSpace(fmt.Sprint(state["rotation_operation_id"])) == operationID {
			return &runtimesessions.Lease{SessionID: currentID, AgentID: agentID, RuntimeMode: resolved.RuntimeMode, SessionScope: resolved.Scope, LockOwner: existingOwner.String, ScopeKey: resolved.ScopeKey, ExpiresAt: existingExpiry.Time}, nil
		}
	}
	now := time.Now().UTC()
	if existingOwner.Valid && existingExpiry.Valid && existingExpiry.Time.After(now) && existingOwner.String != lockOwner {
		return nil, runtimesessions.ErrSessionLeased
	}
	reason := rotation.TerminationReason
	if reason == "" {
		reason = runtimesessions.TerminationReasonContaminated
	}
	newID := uuid.NewString()
	retryReason := strings.TrimSpace(rotation.RetryReason)
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET status='terminated', termination_reason=$2, termination_detail=NULLIF($3,''),
		terminated_at=$4, successor_session_id=NULL, lease_holder=NULL, lease_expires_at=NULL, updated_at=$4
		WHERE session_id=$1::uuid AND status='active'
	`, currentID, reason.String(), retryReason, now); err != nil {
		return nil, err
	}
	runtimeState, err := json.Marshal(map[string]any{"summary": strings.TrimSpace(rotation.CheckpointSummary), "retry_reason": retryReason, "retries_from_session_id": currentID, "rotation_operation_id": operationID})
	if err != nil {
		return nil, err
	}
	expires := now.Add(s.postgresSessionLockTTL())
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, agent_id, entity_id, flow_instance, scope_key, scope, conversation, turn_count, runtime_mode, runtime_state, lease_holder, lease_expires_at, status, created_at, updated_at)
		VALUES ($1::uuid,$2,NULLIF($3,'')::uuid,NULLIF($4,''),$5,$6,'[]'::jsonb,0,$7,$8::jsonb,$9,$10,'active',$11,$11)
	`, newID, agentID, resolved.EntityID, resolved.FlowInstance, resolved.ScopeKey, resolved.Scope.String(), resolved.RuntimeMode.String(), string(runtimeState), lockOwner, expires, now); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id=$2::uuid, updated_at=$3 WHERE session_id=$1::uuid AND status='terminated'`, currentID, newID, now); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &runtimesessions.Lease{SessionID: newID, AgentID: agentID, RuntimeMode: resolved.RuntimeMode, SessionScope: resolved.Scope, RetryReason: retryReason, RetriesFromSessionID: currentID, LockOwner: lockOwner, ScopeKey: resolved.ScopeKey, ExpiresAt: expires}, nil
}

func (s *PostgresStore) IncrementTurn(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, sessionID, scopeKey string) error {
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, agentID, "increment_turn", false); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET turn_count=turn_count+1, updated_at=now() WHERE agent_id=$1 AND runtime_mode=$2 AND session_id=$3::uuid AND scope_key=$4 AND status='active'`, agentID, resolved.RuntimeMode.String(), sessionID, resolved.ScopeKey)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("session not found for turn increment: agent=%s runtime=%s scope=%s session=%s", agentID, runtimeMode, scopeKey, sessionID)
	}
	return tx.Commit()
}

func (s *PostgresStore) AdoptSessionID(ctx context.Context, agentID string, runtimeMode runtimesessions.RuntimeMode, sessionScope runtimesessions.SessionScope, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	resolved, err := runtimesessions.ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil || resolved.Stateless {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, agentID, "adopt_provider_session", false); err != nil {
		return err
	}
	var sessionID string
	var owner sql.NullString
	var expiry sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT session_id::text, lease_holder, lease_expires_at FROM agent_sessions WHERE agent_id=$1 AND scope_key=$2 AND runtime_mode=$3 AND status='active' ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, agentID, resolved.ScopeKey, resolved.RuntimeMode.String()).Scan(&sessionID, &owner, &expiry); err != nil {
		return err
	}
	now := time.Now().UTC()
	if owner.Valid && expiry.Valid && expiry.Time.After(now) && owner.String != lockOwner {
		return runtimesessions.ErrSessionLeased
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET runtime_state=COALESCE(runtime_state,'{}'::jsonb)||jsonb_build_object('provider_session_id',$1::text), lease_holder=$2, lease_expires_at=$3, updated_at=$4 WHERE session_id=$5::uuid`, newSessionID, lockOwner, now.Add(s.postgresSessionLockTTL()), now, sessionID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) ResetAll(runtimeMode runtimesessions.RuntimeMode, metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.DB == nil || runtimeMode == runtimesessions.RuntimeModeTask {
		return runtimesessions.ResetSummary{}, nil
	}
	mode := runtimeMode.String()
	source := strings.TrimSpace(metadata.Source)
	rows, err := s.DB.QueryContext(context.Background(), `
		WITH affected AS (
			SELECT session_id, agent_id, scope_key, runtime_mode, status
			FROM agent_sessions
			WHERE status IN ('active', 'suspended')
			  AND runtime_mode IN ('session', 'session_per_entity')
			  AND (NULLIF($1, '') IS NULL OR runtime_mode = $1)
			FOR UPDATE
		), updated AS (
			UPDATE agent_sessions AS current
			SET status = 'terminated',
			    termination_reason = 'orphaned',
			    termination_detail = NULLIF($2, ''),
			    terminated_at = COALESCE(current.terminated_at, now()),
			    lease_holder = NULL,
			    lease_expires_at = NULL,
			    updated_at = now()
			FROM affected
			WHERE current.session_id = affected.session_id
			RETURNING affected.session_id::text, affected.agent_id, affected.scope_key,
			          affected.runtime_mode, affected.status
		)
		SELECT session_id, agent_id, scope_key, runtime_mode, status
		FROM updated
		ORDER BY agent_id, scope_key, session_id
	`, mode, source)
	if err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("reset postgres live sessions: %w", err)
	}
	defer rows.Close()
	summary := runtimesessions.ResetSummary{}
	for rows.Next() {
		var disposition runtimesessions.ResetDisposition
		if err := rows.Scan(&disposition.SessionID, &disposition.AgentID, &disposition.ScopeKey, &disposition.RuntimeMode, &disposition.PreviousStatus); err != nil {
			return runtimesessions.ResetSummary{}, fmt.Errorf("scan postgres live session reset: %w", err)
		}
		disposition.TerminationReason = runtimesessions.TerminationReasonOrphaned.String()
		disposition.TerminationDetail = source
		summary.OrphanedSessions = append(summary.OrphanedSessions, disposition)
	}
	if err := rows.Err(); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("read postgres live session reset: %w", err)
	}
	return summary, nil
}

func (s *PostgresStore) postgresSessionLockTTL() time.Duration {
	if s == nil || s.sessionLockTTL <= 0 {
		return 120 * time.Second
	}
	return s.sessionLockTTL
}

func decodeLiveConversationRecord(agentID, mode, sessionScope, sessionID, scopeKey, status, runID string, rawMessages, runtimeStateRaw []byte, turnCount int) (runtimellm.ConversationRecord, error) {
	record := runtimellm.ConversationRecord{
		SessionID: sessionID, AgentID: agentID, SessionScope: sessionScope, ScopeKey: scopeKey,
		RunID: runID, Mode: mode, TurnCount: turnCount, Status: status,
	}
	state, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("decode exact live session runtime_state: %w", err)
	}
	record.Summary = state.Summary
	record.RetryReason = state.RetryReason
	record.RetriesFromSessionID = state.RetriesFromSessionID
	if state.Watchdog != nil {
		record.Watchdog = &runtimellm.ConversationWatchdog{
			State: state.Watchdog.State, BlockingLayer: state.Watchdog.BlockingLayer,
			Action: state.Watchdog.Action, Outcome: state.Watchdog.Outcome,
			LastOutputAt: state.Watchdog.LastOutputAt, RecordedAt: state.Watchdog.RecordedAt,
		}
	}
	if len(rawMessages) == 0 {
		rawMessages = []byte("[]")
	}
	if err := json.Unmarshal(rawMessages, &record.Messages); err != nil {
		return runtimellm.ConversationRecord{}, fmt.Errorf("decode exact live session conversation: %w", err)
	}
	return record, nil
}
