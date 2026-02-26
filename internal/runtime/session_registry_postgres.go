package runtime

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

type PostgresSessionRegistry struct {
	db      *sql.DB
	lockTTL time.Duration
	nowFn   func() time.Time
}

func NewPostgresSessionRegistry(db *sql.DB, lockTTL time.Duration) *PostgresSessionRegistry {
	if lockTTL <= 0 {
		lockTTL = 120 * time.Second
	}
	return &PostgresSessionRegistry{
		db:      db,
		lockTTL: lockTTL,
		nowFn:   time.Now,
	}
}

func (sr *PostgresSessionRegistry) Acquire(agentID, runtimeMode, lockOwner, scopeKey string) (*SessionLease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	ctx := context.Background()
	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	type rec struct {
		id          string
		sessionID   string
		scopeKey    sql.NullString
		lockOwner   sql.NullString
		lockExpires sql.NullTime
	}
	var r rec
	row := tx.QueryRowContext(ctx, `
		SELECT id::text, session_id, scope_key, lock_owner, lock_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND COALESCE(scope_key, '') = $3
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, runtimeMode, scopeKey)
	err = row.Scan(&r.id, &r.sessionID, &r.scopeKey, &r.lockOwner, &r.lockExpires)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("load session row: %w", err)
		}
		provider := providerForRuntime(runtimeMode)
		r.sessionID = uuid.NewString()
		row = tx.QueryRowContext(ctx, `
			INSERT INTO agent_sessions (
				agent_id, runtime_mode, scope_key, provider, session_id, status,
				lock_owner, lock_expires_at, last_used_at, created_at
			)
			VALUES ($1, $2, NULLIF($3,''), $4, $5, 'active', $6, now() + ($7 * interval '1 second'), now(), now())
			RETURNING id::text, session_id, scope_key
		`, agentID, runtimeMode, scopeKey, provider, r.sessionID, lockOwner, int(sr.lockTTL.Seconds()))
		if err := row.Scan(&r.id, &r.sessionID, &r.scopeKey); err != nil {
			return nil, fmt.Errorf("insert session row: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit acquire new: %w", err)
		}
		return &SessionLease{
			SessionID:   r.sessionID,
			AgentID:     agentID,
			RuntimeMode: runtimeMode,
			LockOwner:   lockOwner,
			ScopeKey:    scopeKey,
			ExpiresAt:   sr.nowFn().Add(sr.lockTTL),
		}, nil
	}

	now := sr.nowFn()
	if r.lockOwner.Valid && r.lockExpires.Valid && r.lockExpires.Time.After(now) && r.lockOwner.String != lockOwner {
		return nil, ErrSessionLeased
	}

	row = tx.QueryRowContext(ctx, `
		UPDATE agent_sessions
		SET lock_owner = $1,
		    lock_expires_at = now() + ($2 * interval '1 second'),
		    last_used_at = now()
		WHERE id = $3::uuid
		RETURNING session_id, lock_expires_at
	`, lockOwner, int(sr.lockTTL.Seconds()), r.id)
	var expires time.Time
	if err := row.Scan(&r.sessionID, &expires); err != nil {
		return nil, fmt.Errorf("update lock lease: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit acquire existing: %w", err)
	}

	return &SessionLease{
		SessionID:   r.sessionID,
		AgentID:     agentID,
		RuntimeMode: runtimeMode,
		LockOwner:   lockOwner,
		ScopeKey:    scopeKey,
		ExpiresAt:   expires,
	}, nil
}

func (sr *PostgresSessionRegistry) Release(lease *SessionLease) error {
	if lease == nil {
		return errors.New("nil lease")
	}
	res, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET lock_owner = NULL,
		    lock_expires_at = NULL,
		    last_used_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3
		  AND COALESCE(scope_key, '') = $4
		  AND lock_owner = $5
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

func (sr *PostgresSessionRegistry) Rotate(agentID, runtimeMode, lockOwner, summary, scopeKey string) (*SessionLease, error) {
	if agentID == "" || runtimeMode == "" || lockOwner == "" {
		return nil, errors.New("agentID, runtimeMode, and lockOwner are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	ctx := context.Background()
	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var id string
	var currentSessionID string
	var existingOwner sql.NullString
	var existingExpiry sql.NullTime
	row := tx.QueryRowContext(ctx, `
		SELECT id::text, session_id, lock_owner, lock_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND COALESCE(scope_key, '') = $3
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, runtimeMode, scopeKey)
	if err := row.Scan(&id, &currentSessionID, &existingOwner, &existingExpiry); err != nil {
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
		SET status = 'rotated',
		    checkpoint_summary = $1,
		    rotated_at = now(),
		    lock_owner = NULL,
		    lock_expires_at = NULL,
		    last_used_at = now()
		WHERE id = $2::uuid
	`, summary, id); err != nil {
		return nil, fmt.Errorf("mark session rotated: %w", err)
	}

	newSessionID := uuid.NewString()
	provider := providerForRuntime(runtimeMode)
	row = tx.QueryRowContext(ctx, `
		INSERT INTO agent_sessions (
			agent_id, runtime_mode, scope_key, provider, session_id, status,
			lock_owner, lock_expires_at, last_used_at, created_at
		)
		VALUES ($1, $2, NULLIF($3,''), $4, $5, 'active', $6, now() + ($7 * interval '1 second'), now(), now())
		RETURNING lock_expires_at
	`, agentID, runtimeMode, scopeKey, provider, newSessionID, lockOwner, int(sr.lockTTL.Seconds()))
	var expiresAt time.Time
	if err := row.Scan(&expiresAt); err != nil {
		return nil, fmt.Errorf("insert rotated session: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit rotate: %w", err)
	}

	return &SessionLease{
		SessionID:   newSessionID,
		AgentID:     agentID,
		RuntimeMode: runtimeMode,
		LockOwner:   lockOwner,
		ScopeKey:    scopeKey,
		ExpiresAt:   expiresAt,
	}, nil
}

func (sr *PostgresSessionRegistry) IncrementTurn(agentID, runtimeMode, sessionID, scopeKey string) error {
	res, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET turn_count = turn_count + 1,
		    last_used_at = now()
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND session_id = $3
		  AND COALESCE(scope_key, '') = $4
		  AND status = 'active'
	`, agentID, runtimeMode, sessionID, strings.TrimSpace(scopeKey))
	if err != nil {
		return fmt.Errorf("increment turn: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("session not found for turn increment: agent=%s runtime=%s scope=%s session=%s", agentID, runtimeMode, scopeKey, sessionID)
	}
	return nil
}

func (sr *PostgresSessionRegistry) AdoptSessionID(agentID, runtimeMode, lockOwner, newSessionID, scopeKey string) error {
	agentID = strings.TrimSpace(agentID)
	runtimeMode = strings.TrimSpace(runtimeMode)
	lockOwner = strings.TrimSpace(lockOwner)
	newSessionID = strings.TrimSpace(newSessionID)
	if agentID == "" || runtimeMode == "" || lockOwner == "" || newSessionID == "" {
		return errors.New("agentID, runtimeMode, lockOwner, and newSessionID are required")
	}
	scopeKey = strings.TrimSpace(scopeKey)

	ctx := context.Background()
	tx, err := sr.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var id string
	var existingOwner sql.NullString
	var existingExpiry sql.NullTime
	row := tx.QueryRowContext(ctx, `
		SELECT id::text, lock_owner, lock_expires_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND runtime_mode = $2
		  AND status = 'active'
		  AND ($3 = '' OR COALESCE(scope_key, '') = $3)
		ORDER BY created_at DESC
		LIMIT 1
		FOR UPDATE
	`, agentID, runtimeMode, scopeKey)
	if err := row.Scan(&id, &existingOwner, &existingExpiry); err != nil {
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
		SET session_id = $1,
		    lock_owner = $2,
		    lock_expires_at = now() + ($3 * interval '1 second'),
		    last_used_at = now()
		WHERE id = $4::uuid
	`, newSessionID, lockOwner, int(sr.lockTTL.Seconds()), id); err != nil {
		return fmt.Errorf("update session id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit adopt session id: %w", err)
	}
	return nil
}

func providerForRuntime(runtimeMode string) string {
	switch runtimeMode {
	case "cli_test":
		return "claude_cli"
	case "api":
		return "anthropic"
	default:
		return "unknown"
	}
}

func (sr *PostgresSessionRegistry) ResetAll(runtimeMode string) error {
	runtimeMode = strings.TrimSpace(runtimeMode)
	if runtimeMode == "" {
		_, err := sr.db.Exec(`
			UPDATE agent_sessions
			SET status = 'reset',
			    lock_owner = NULL,
			    lock_expires_at = NULL,
			    last_used_at = now()
			WHERE status = 'active'
		`)
		if err != nil {
			return fmt.Errorf("reset all sessions: %w", err)
		}
		return nil
	}
	_, err := sr.db.Exec(`
		UPDATE agent_sessions
		SET status = 'reset',
		    lock_owner = NULL,
		    lock_expires_at = NULL,
		    last_used_at = now()
		WHERE status = 'active' AND runtime_mode = $1
	`, runtimeMode)
	if err != nil {
		return fmt.Errorf("reset sessions runtime=%s: %w", runtimeMode, err)
	}
	return nil
}
