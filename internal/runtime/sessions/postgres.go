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

func (sr *PostgresRegistry) Acquire(ctx context.Context, agentID, runtimeMode, lockOwner, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	resolved := ResolveScope(runtimeMode, scopeKey)
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	type rec struct {
		sessionID         string
		scopeKey          string
		providerSessionID sql.NullString
		leaseHolder       sql.NullString
		leaseExpires      sql.NullTime
	}
	var r rec
	err = tx.QueryRowContext(ctx, `
		SELECT
			session_id::text,
			scope_key,
			NULLIF(runtime_state->>'provider_session_id', ''),
			lease_holder,
			lease_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND scope_key = $2
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey).Scan(
		&r.sessionID,
		&r.scopeKey,
		&r.providerSessionID,
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
			SessionID:   r.sessionID,
			AgentID:     agentID,
			RuntimeMode: resolved.RuntimeMode,
			LockOwner:   lockOwner,
			ScopeKey:    r.scopeKey,
			ExpiresAt:   expires,
		}, nil
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
		SessionID:         r.sessionID,
		ProviderSessionID: strings.TrimSpace(r.providerSessionID.String),
		AgentID:           agentID,
		RuntimeMode:       resolved.RuntimeMode,
		LockOwner:         lockOwner,
		ScopeKey:          resolved.ScopeKey,
		ExpiresAt:         expires,
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
	`, lease.AgentID, NormalizeConversationRuntimeMode(lease.RuntimeMode), lease.SessionID, strings.TrimSpace(lease.ScopeKey), lease.LockOwner)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no active lease to release for agent=%s session=%s", lease.AgentID, lease.SessionID)
	}
	return nil
}

func (sr *PostgresRegistry) Rotate(ctx context.Context, agentID, runtimeMode, lockOwner, summary, scopeKey string) (*Lease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	resolved := ResolveScope(runtimeMode, scopeKey)
	if resolved.Stateless {
		return nil, errors.New("task-scoped sessions are stateless")
	}
	if ctx == nil {
		ctx = context.Background()
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
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey).Scan(&currentSessionID, &existingOwner, &existingExpiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("no active session to rotate for agent=%s", agentID)
		}
		return nil, fmt.Errorf("load active session: %w", err)
	}

	now := sr.nowFn()
	if existingOwner.Valid && existingExpiry.Valid && existingExpiry.Time.After(now) && existingOwner.String != lockOwner {
		return nil, ErrSessionLeased
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object(
		    	'checkpoint_summary', NULLIF($1, ''),
		    	'terminated_at', now()
		    ),
		    updated_at = now()
		WHERE session_id = $2::uuid
	`, summary, currentSessionID); err != nil {
		return nil, fmt.Errorf("mark terminated session: %w", err)
	}

	newSessionID := uuid.NewString()
	var expiresAt time.Time
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, agent_id, entity_id, flow_instance, scope_key, scope,
			conversation, turn_count, runtime_mode, runtime_state,
			lease_holder, lease_expires_at, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6,
			'[]'::jsonb, 0, $7, jsonb_build_object('checkpoint_summary', NULLIF($8,'')),
			$9, $10, 'active', now(), now()
		)
		RETURNING lease_expires_at
	`, newSessionID, agentID, resolved.EntityID, resolved.FlowInstance, resolved.ScopeKey, resolved.Scope, resolved.RuntimeMode, summary, lockOwner, now.Add(sr.lockTTL)).Scan(&expiresAt); err != nil {
		return nil, fmt.Errorf("insert rotated session: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit rotate: %w", err)
	}

	return &Lease{
		SessionID:   newSessionID,
		AgentID:     agentID,
		RuntimeMode: resolved.RuntimeMode,
		LockOwner:   lockOwner,
		ScopeKey:    resolved.ScopeKey,
		ExpiresAt:   expiresAt,
	}, nil
}

func (sr *PostgresRegistry) IncrementTurn(ctx context.Context, agentID, runtimeMode, sessionID, scopeKey string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resolved := ResolveScope(runtimeMode, scopeKey)
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

func (sr *PostgresRegistry) AdoptSessionID(ctx context.Context, agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	resolved := ResolveScope(runtimeMode, scopeKey)
	if resolved.Stateless {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
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
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, resolved.ScopeKey).Scan(&sessionID, &existingOwner, &existingExpiry); err != nil {
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
		SET runtime_state = COALESCE(runtime_state, '{}'::jsonb) || jsonb_build_object('provider_session_id', $1),
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

func (sr *PostgresRegistry) ResetAll(runtimeMode string) error {
	runtimeMode = strings.TrimSpace(runtimeMode)
	if runtimeMode == "" {
		_, err := sr.db.Exec(`
			UPDATE agent_sessions
			SET status = 'terminated',
			    lease_holder = NULL,
			    lease_expires_at = NULL,
			    updated_at = now()
			WHERE status = 'active'
		`)
		if err != nil {
			return fmt.Errorf("reset all sessions: %w", err)
		}
		return nil
	}
	_, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET status = 'terminated',
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE status = 'active' AND runtime_mode = $1
	`, NormalizeConversationRuntimeMode(runtimeMode))
	if err != nil {
		return fmt.Errorf("reset sessions runtime=%s: %w", runtimeMode, err)
	}
	return nil
}
