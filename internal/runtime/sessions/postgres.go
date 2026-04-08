package sessions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrSessionLeased = errors.New("session currently leased by another worker")

type PostgresRegistry struct {
	db      *sql.DB
	lockTTL time.Duration
	nowFn   func() time.Time
}

func NewPostgresRegistry(db *sql.DB, lockTTL time.Duration) *PostgresRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &PostgresRegistry{
		db:      db,
		lockTTL: lockTTL,
		nowFn:   time.Now,
	}
}

func (sr *PostgresRegistry) Acquire(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}

	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	type rec struct {
		sessionID         string
		scopeKey          string
		status            string
		providerSessionID sql.NullString
		retryReason       sql.NullString
		retriesFrom       sql.NullString
		leaseHolder       sql.NullString
		leaseExpires      sql.NullTime
	}
	var r rec
	err = tx.QueryRowContext(ctx, `
		SELECT
			session_id::text,
			scope_key,
			status,
			NULLIF(runtime_state->>'provider_session_id', ''),
			NULLIF(runtime_state->>'retry_reason', ''),
			NULLIF(runtime_state->>'retries_from_session_id', ''),
			lease_holder,
			lease_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND scope_key = $2
		  AND runtime_mode = $3
		  AND status IN ('active', 'suspended')
		ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey, resolved.RuntimeMode).Scan(
		&r.sessionID,
		&r.scopeKey,
		&r.status,
		&r.providerSessionID,
		&r.retryReason,
		&r.retriesFrom,
		&r.leaseHolder,
		&r.leaseExpires,
	)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("load session row: %w", err)
	}

	now := sr.nowFn()
	if errors.Is(err, sql.ErrNoRows) {
		sessionID := uuid.NewString()
		row := tx.QueryRowContext(ctx, `
			INSERT INTO agent_sessions (
				session_id, agent_id, entity_id, flow_instance, scope_key, scope,
				conversation, turn_count, runtime_mode, runtime_state,
				lease_holder, lease_expires_at, status, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6,
				'[]'::jsonb, 0, $7, '{}'::jsonb,
				$8, $9, 'active', now(), now()
			)
			RETURNING session_id::text, scope_key, lease_expires_at
		`, sessionID, agentID, resolved.EntityID, resolved.FlowInstance, resolved.ScopeKey, resolved.Scope, resolved.RuntimeMode, lockOwner, now.Add(sr.lockTTL))
		var expires time.Time
		if err := row.Scan(&r.sessionID, &r.scopeKey, &expires); err != nil {
			return nil, fmt.Errorf("insert session row: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit acquire new: %w", err)
		}
		return &Lease{
			SessionID:            r.sessionID,
			AgentID:              agentID,
			RuntimeMode:          resolved.RuntimeMode,
			SessionScope:         resolved.Scope,
			RetryReason:          "",
			RetriesFromSessionID: "",
			LockOwner:            lockOwner,
			ScopeKey:             r.scopeKey,
			ExpiresAt:            expires,
		}, nil
	}

	if strings.TrimSpace(r.status) == "suspended" {
		return nil, ErrSessionSuspended
	}

	if r.leaseHolder.Valid && r.leaseExpires.Valid && r.leaseExpires.Time.After(now) && r.leaseHolder.String != lockOwner {
		return nil, ErrSessionLeased
	}

	var expires time.Time
	if err := tx.QueryRowContext(ctx, `
		UPDATE agent_sessions
		SET lease_holder = $1,
		    lease_expires_at = $2,
		    updated_at = now()
		WHERE session_id = $3::uuid
		RETURNING lease_expires_at
	`, lockOwner, now.Add(sr.lockTTL), r.sessionID).Scan(&expires); err != nil {
		return nil, fmt.Errorf("update lock lease: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit acquire existing: %w", err)
	}

	return &Lease{
		SessionID:            r.sessionID,
		ProviderSessionID:    strings.TrimSpace(r.providerSessionID.String),
		AgentID:              agentID,
		RuntimeMode:          resolved.RuntimeMode,
		SessionScope:         resolved.Scope,
		RetryReason:          strings.TrimSpace(r.retryReason.String),
		RetriesFromSessionID: strings.TrimSpace(r.retriesFrom.String),
		LockOwner:            lockOwner,
		ScopeKey:             resolved.ScopeKey,
		ExpiresAt:            expires,
	}, nil
}

func (sr *PostgresRegistry) Release(ctx context.Context, lease *Lease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	res, err := sr.db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3::uuid
		  AND scope_key = $4
		  AND lease_holder = $5
		  AND status = 'active'
	`, lease.AgentID, lease.RuntimeMode, lease.SessionID, strings.TrimSpace(lease.ScopeKey), lease.LockOwner)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", lease.AgentID, lease.SessionID)
	}
	return nil
}

func (sr *PostgresRegistry) Rotate(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner string, rotation RotationMetadata, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return nil, err
	}
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}

	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		currentSessionID string
		existingOwner    sql.NullString
		existingExpiry   sql.NullTime
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id::text, lease_holder, lease_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND scope_key = $2
		  AND runtime_mode = $3
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey, resolved.RuntimeMode).Scan(&currentSessionID, &existingOwner, &existingExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("no active session to rotate for agent=%s", agentID)
		}
		return nil, fmt.Errorf("load active session: %w", err)
	}

	now := sr.nowFn()
	if existingOwner.Valid && existingExpiry.Valid && existingExpiry.Time.After(now) && existingOwner.String != lockOwner {
		return nil, ErrSessionLeased
	}

	newSessionID := uuid.NewString()
	retryReason := strings.TrimSpace(rotation.RetryReason)
	terminationReason := rotation.TerminationReason
	if terminationReason == "" {
		mappedReason, _, err := rotationTermination(retryReason)
		if err != nil {
			return nil, err
		}
		terminationReason = mappedReason
	}
	if err := validateRuntimeTerminationReason(terminationReason); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = $1,
		    termination_detail = NULLIF($2, ''),
		    terminated_at = COALESCE(terminated_at, $3),
		    successor_session_id = NULL,
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE session_id = $4::uuid
		  AND status = 'active'
	`, terminationReason, retryReason, now, currentSessionID); err != nil {
		return nil, fmt.Errorf("terminate rotated session row: %w", err)
	}
	var expiresAt time.Time
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state,
			lease_holder, lease_expires_at, status,
			termination_reason, termination_detail, successor_session_id, terminated_at,
			created_at, updated_at
		)
		VALUES (
			$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6,
			'[]'::jsonb, 0, $7,
			jsonb_strip_nulls(jsonb_build_object(
				'summary', NULLIF($8,''),
				'retry_reason', NULLIF($9,''),
				'retries_from_session_id', NULLIF($10,'')
			)),
			$11, $12, 'active',
			NULL, NULL, NULL, NULL,
			now(), now()
		)
		RETURNING lease_expires_at
	`, newSessionID, agentID, resolved.EntityID, resolved.FlowInstance, resolved.ScopeKey, resolved.Scope, resolved.RuntimeMode, strings.TrimSpace(rotation.CheckpointSummary), retryReason, currentSessionID, lockOwner, now.Add(sr.lockTTL)).Scan(&expiresAt); err != nil {
		return nil, fmt.Errorf("insert rotated successor session row: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET successor_session_id = $1::uuid,
		    updated_at = now()
		WHERE session_id = $2::uuid
		  AND status = 'terminated'
	`, newSessionID, currentSessionID); err != nil {
		return nil, fmt.Errorf("link rotated successor session row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit rotate: %w", err)
	}

	return &Lease{
		SessionID:            newSessionID,
		AgentID:              agentID,
		RuntimeMode:          resolved.RuntimeMode,
		SessionScope:         resolved.Scope,
		RetryReason:          retryReason,
		RetriesFromSessionID: currentSessionID,
		LockOwner:            lockOwner,
		ScopeKey:             resolved.ScopeKey,
		ExpiresAt:            expiresAt,
	}, nil
}

func (sr *PostgresRegistry) IncrementTurn(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, sessionID, scopeKey string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	res, err := sr.db.ExecContext(ctx, `
		UPDATE agent_sessions
		SET turn_count = turn_count + 1,
		    updated_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3::uuid
		  AND scope_key = $4
		  AND status = 'active'
	`, agentID, resolved.RuntimeMode, sessionID, resolved.ScopeKey)
	if err != nil {
		return fmt.Errorf("increment turn: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("session not found for turn increment: agent=%s runtime=%s scope=%s session=%s", agentID, runtimeMode, scopeKey, sessionID)
	}
	return nil
}

func (sr *PostgresRegistry) AdoptSessionID(ctx context.Context, agentID string, runtimeMode RuntimeMode, sessionScope SessionScope, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resolved, err := ResolveScope(ctx, runtimeMode, sessionScope, scopeKey)
	if err != nil {
		return err
	}
	if resolved.Stateless {
		return nil
	}

	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		sessionID      string
		existingOwner  sql.NullString
		existingExpiry sql.NullTime
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT session_id::text, lease_holder, lease_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND scope_key = $2
		  AND runtime_mode = $3
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey, resolved.RuntimeMode).Scan(&sessionID, &existingOwner, &existingExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no active session to adopt for agent=%s", agentID)
		}
		return fmt.Errorf("load active session: %w", err)
	}

	now := sr.nowFn()
	if existingOwner.Valid && existingExpiry.Valid && existingExpiry.Time.After(now) && existingOwner.String != lockOwner {
		return ErrSessionLeased
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object('provider_session_id', $1::text),
		    lease_holder = $2,
		    lease_expires_at = $3,
		    updated_at = now()
		WHERE session_id = $4::uuid
	`, newSessionID, lockOwner, now.Add(sr.lockTTL), sessionID); err != nil {
		return fmt.Errorf("update provider session id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit adopt session id: %w", err)
	}
	return nil
}

func (sr *PostgresRegistry) SetNowFnForTest(nowFn func() time.Time) {
	if sr == nil {
		return
	}
	if nowFn == nil {
		sr.nowFn = time.Now
		return
	}
	sr.nowFn = nowFn
}

func (sr *PostgresRegistry) ScopeKeyEnabledForTest() bool {
	return true
}

func (sr *PostgresRegistry) ResetAll(runtimeMode RuntimeMode) error {
	if runtimeMode == "" {
		_, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = 'orphaned',
		    terminated_at = COALESCE(terminated_at, now()),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE status IN ('active', 'suspended')
		  AND runtime_mode IN ('session', 'session_per_entity')
	`)
		if err != nil {
			return fmt.Errorf("reset all sessions: %w", err)
		}
		return nil
	}
	if runtimeMode == RuntimeModeTask {
		return nil
	}
	_, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = 'orphaned',
		    terminated_at = COALESCE(terminated_at, now()),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE status IN ('active', 'suspended') AND runtime_mode = $1
	`, runtimeMode)
	if err != nil {
		return fmt.Errorf("reset sessions runtime=%s: %w", runtimeMode.String(), err)
	}
	return nil
}
