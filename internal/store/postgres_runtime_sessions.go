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
	"github.com/google/uuid"
)

var _ runtimesessions.Registry = (*PostgresStore)(nil)
var _ runtimellm.LiveSessionAcquirer = (*PostgresStore)(nil)

func (s *PostgresStore) Acquire(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, error) {
	lease, _, err := s.acquirePostgresLiveSession(ctx, identity, lockOwner)
	return lease, err
}

func (s *PostgresStore) AcquireLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
	return s.acquirePostgresLiveSession(ctx, identity, lockOwner)
}

func (s *PostgresStore) acquirePostgresLiveSession(ctx context.Context, identity agentmemory.Identity, lockOwner string) (*runtimesessions.Lease, runtimellm.ConversationRecord, error) {
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
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, fmt.Errorf("begin live session acquire: %w", err)
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "acquire_hydrate", false); err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}

	type row struct {
		sessionID, status              string
		providerSessionID, retryReason sql.NullString
		retriesFrom, leaseHolder       sql.NullString
		leaseExpires                   sql.NullTime
		conversation, runtimeState     []byte
		turnCount                      int
	}
	var current row
	err = tx.QueryRowContext(ctx, `
		SELECT session_id::text, status,
		       NULLIF(runtime_state->>'provider_session_id', ''), NULLIF(runtime_state->>'retry_reason', ''),
		       NULLIF(runtime_state->>'retries_from_session_id', ''), lease_holder, lease_expires_at,
		       COALESCE(conversation, '[]'::jsonb), COALESCE(runtime_state, '{}'::jsonb), COALESCE(turn_count, 0)
		FROM agent_sessions
		WHERE run_id = $1::uuid AND agent_id = $2 AND flow_instance = $3 AND status IN ('active', 'suspended')
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC
		LIMIT 1 FOR UPDATE
	`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(
		&current.sessionID, &current.status, &current.providerSessionID, &current.retryReason,
		&current.retriesFrom, &current.leaseHolder, &current.leaseExpires,
		&current.conversation, &current.runtimeState, &current.turnCount,
	)
	now := time.Now().UTC()
	if errors.Is(err, sql.ErrNoRows) {
		current.sessionID = uuid.NewString()
		current.status = "active"
		current.conversation = []byte("[]")
		current.runtimeState = []byte("{}")
		current.leaseHolder = sql.NullString{String: lockOwner, Valid: true}
		current.leaseExpires = sql.NullTime{Time: now.Add(s.postgresSessionLockTTL()), Valid: true}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source,
				conversation, turn_count, runtime_state, lease_holder, lease_expires_at,
				status, created_at, updated_at
			) VALUES ($1::uuid, $2::uuid, $3, $4, TRUE, 'authored', '[]'::jsonb, 0, '{}'::jsonb, $5, $6, 'active', $7, $7)
		`, current.sessionID, identity.RunID, identity.AgentID, identity.FlowInstance, lockOwner, current.leaseExpires.Time, now); err != nil {
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
	record, err := decodeLiveConversationRecord(identity, current.sessionID, current.status, current.conversation, current.runtimeState, current.turnCount)
	if err != nil {
		return nil, runtimellm.ConversationRecord{}, err
	}
	lease := &runtimesessions.Lease{
		SessionID: current.sessionID, ProviderSessionID: strings.TrimSpace(current.providerSessionID.String), Identity: identity,
		RetryReason: strings.TrimSpace(current.retryReason.String), RetriesFromSessionID: strings.TrimSpace(current.retriesFrom.String),
		LockOwner: lockOwner, ExpiresAt: current.leaseExpires.Time,
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return nil, runtimellm.ConversationRecord{}, fmt.Errorf("commit live session acquire: %w", err)
	}
	return lease, record, nil
}

func (s *PostgresStore) Release(ctx context.Context, lease *runtimesessions.Lease) error {
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
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin live session release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions SET lease_holder=NULL, lease_expires_at=NULL, updated_at=now()
		WHERE run_id=$1::uuid AND agent_id=$2 AND flow_instance=$3 AND session_id=$4::uuid AND lease_holder=$5 AND status='active'
	`, identity.RunID, identity.AgentID, identity.FlowInstance, lease.SessionID, lease.LockOwner)
	if err != nil {
		return fmt.Errorf("release live session lease: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", identity.AgentID, lease.SessionID)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("commit live session release: %w", err)
	}
	return nil
}

func (s *PostgresStore) Rotate(ctx context.Context, identity agentmemory.Identity, lockOwner string, rotation runtimesessions.RotationMetadata) (*runtimesessions.Lease, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	if lockOwner == "" {
		return nil, errors.New("lockOwner is required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "rotate", false); err != nil {
		return nil, err
	}
	var currentID string
	var existingOwner sql.NullString
	var existingExpiry sql.NullTime
	var runtimeStateRaw []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id::text, lease_holder, lease_expires_at, runtime_state
		FROM agent_sessions WHERE run_id=$1::uuid AND agent_id=$2 AND flow_instance=$3 AND status='active'
		ORDER BY created_at DESC LIMIT 1 FOR UPDATE
	`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&currentID, &existingOwner, &existingExpiry, &runtimeStateRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("no active session to rotate for agent=%s", identity.AgentID)
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
			return &runtimesessions.Lease{SessionID: currentID, Identity: identity, LockOwner: existingOwner.String, ExpiresAt: existingExpiry.Time}, nil
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
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, conversation, turn_count, runtime_state, lease_holder, lease_expires_at, status, created_at, updated_at)
		VALUES ($1::uuid,$2::uuid,$3,$4,TRUE,'authored','[]'::jsonb,0,$5::jsonb,$6,$7,'active',$8,$8)
	`, newID, identity.RunID, identity.AgentID, identity.FlowInstance, string(runtimeState), lockOwner, expires, now); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET successor_session_id=$2::uuid, updated_at=$3 WHERE session_id=$1::uuid AND status='terminated'`, currentID, newID, now); err != nil {
		return nil, err
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return nil, err
	}
	return &runtimesessions.Lease{SessionID: newID, Identity: identity, RetryReason: retryReason, RetriesFromSessionID: currentID, LockOwner: lockOwner, ExpiresAt: expires}, nil
}

func (s *PostgresStore) IncrementTurn(ctx context.Context, identity agentmemory.Identity, sessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "increment_turn", false); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET turn_count=turn_count+1, updated_at=now() WHERE run_id=$1::uuid AND agent_id=$2 AND flow_instance=$3 AND session_id=$4::uuid AND status='active'`, identity.RunID, identity.AgentID, identity.FlowInstance, sessionID)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("session not found for turn increment: run=%s agent=%s flow=%s session=%s", identity.RunID, identity.AgentID, identity.FlowInstance, sessionID)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("commit live session turn increment: %w", err)
	}
	return nil
}

func (s *PostgresStore) AdoptSessionID(ctx context.Context, identity agentmemory.Identity, lockOwner, newSessionID string) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return err
	}
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if lockOwner == "" || newSessionID == "" {
		return errors.New("lockOwner and newSessionID are required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := requirePostgresLiveSessionAuthority(ctx, tx, identity.AgentID, "adopt_provider_session", false); err != nil {
		return err
	}
	var sessionID string
	var owner sql.NullString
	var expiry sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT session_id::text, lease_holder, lease_expires_at FROM agent_sessions WHERE run_id=$1::uuid AND agent_id=$2 AND flow_instance=$3 AND status='active' ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, identity.RunID, identity.AgentID, identity.FlowInstance).Scan(&sessionID, &owner, &expiry); err != nil {
		return err
	}
	now := time.Now().UTC()
	if owner.Valid && expiry.Valid && expiry.Time.After(now) && owner.String != lockOwner {
		return runtimesessions.ErrSessionLeased
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET runtime_state=COALESCE(runtime_state,'{}'::jsonb)||jsonb_build_object('provider_session_id',$1::text), lease_holder=$2, lease_expires_at=$3, updated_at=$4 WHERE session_id=$5::uuid`, newSessionID, lockOwner, now.Add(s.postgresSessionLockTTL()), now, sessionID); err != nil {
		return err
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return fmt.Errorf("commit live session provider adoption: %w", err)
	}
	return nil
}

func (s *PostgresStore) ResetAll(metadata runtimesessions.ResetMetadata) (runtimesessions.ResetSummary, error) {
	if s == nil || s.DB == nil {
		return runtimesessions.ResetSummary{}, nil
	}
	source := strings.TrimSpace(metadata.Source)
	ctx := context.Background()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("begin reset postgres live sessions: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
		WITH affected AS (
			SELECT session_id, run_id, agent_id, flow_instance, status FROM agent_sessions
			WHERE status IN ('active', 'suspended') FOR UPDATE
		), updated AS (
			UPDATE agent_sessions AS current SET status='terminated', termination_reason='orphaned', termination_detail=NULLIF($1,''),
			terminated_at=COALESCE(current.terminated_at,now()), lease_holder=NULL, lease_expires_at=NULL, updated_at=now()
			FROM affected WHERE current.session_id=affected.session_id
			RETURNING affected.session_id::text, affected.run_id::text, affected.agent_id, affected.flow_instance, affected.status
		)
		SELECT session_id, run_id, agent_id, flow_instance, status FROM updated ORDER BY run_id, agent_id, flow_instance, session_id
	`, source)
	if err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("reset postgres live sessions: %w", err)
	}
	defer rows.Close()
	summary := runtimesessions.ResetSummary{}
	for rows.Next() {
		var d runtimesessions.ResetDisposition
		if err := rows.Scan(&d.SessionID, &d.RunID, &d.AgentID, &d.FlowInstance, &d.PreviousStatus); err != nil {
			return runtimesessions.ResetSummary{}, fmt.Errorf("scan postgres live session reset: %w", err)
		}
		d.TerminationReason = runtimesessions.TerminationReasonOrphaned.String()
		d.TerminationDetail = source
		summary.OrphanedSessions = append(summary.OrphanedSessions, d)
	}
	if err := rows.Err(); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("read postgres live session reset: %w", err)
	}
	if err := rows.Close(); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("close postgres live session reset: %w", err)
	}
	if err := commitPostgresRunForkRevisionTx(ctx, tx); err != nil {
		return runtimesessions.ResetSummary{}, fmt.Errorf("commit postgres live session reset: %w", err)
	}
	return summary, nil
}

func (s *PostgresStore) postgresSessionLockTTL() time.Duration {
	if s == nil || s.sessionLockTTL <= 0 {
		return 120 * time.Second
	}
	return s.sessionLockTTL
}

func decodeLiveConversationRecord(identity agentmemory.Identity, sessionID, status string, rawMessages, runtimeStateRaw []byte, turnCount int) (runtimellm.ConversationRecord, error) {
	record := runtimellm.ConversationRecord{
		SessionID: sessionID, AgentID: identity.AgentID, Identity: identity, Memory: agentmemory.Authored(true),
		TurnCount: turnCount, Status: status,
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
			State: state.Watchdog.State, BlockingLayer: state.Watchdog.BlockingLayer, Action: state.Watchdog.Action,
			Outcome: state.Watchdog.Outcome, LastOutputAt: state.Watchdog.LastOutputAt, RecordedAt: state.Watchdog.RecordedAt,
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
