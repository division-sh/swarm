package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const (
	pipelineReplayClaimNamespace = "swarm:pipeline-replay:"
	scheduleClaimNamespace       = "swarm:schedule:"
)

type advisoryLockLease interface {
	Release(context.Context) error
}

type sqlAdvisoryLockLease struct {
	conn        *sql.Conn
	lockKey     string
	lifetime    *sharedSQLConnLifetime
	releaseConn func() error
}

type sharedSQLConnLifetimeContextKey struct{}

type sharedSQLConnLifetime struct {
	mu     sync.Mutex
	conn   *sql.Conn
	refs   int
	closed bool
}

func newSharedSQLConnLifetime(conn *sql.Conn) *sharedSQLConnLifetime {
	return &sharedSQLConnLifetime{conn: conn, refs: 1}
}

func withSharedSQLConnLifetime(ctx context.Context, lifetime *sharedSQLConnLifetime) context.Context {
	if lifetime == nil {
		return ctx
	}
	return context.WithValue(ctx, sharedSQLConnLifetimeContextKey{}, lifetime)
}

func sharedSQLConnLifetimeFromContext(ctx context.Context) (*sharedSQLConnLifetime, bool) {
	if ctx == nil {
		return nil, false
	}
	lifetime, ok := ctx.Value(sharedSQLConnLifetimeContextKey{}).(*sharedSQLConnLifetime)
	return lifetime, ok && lifetime != nil
}

func (l *sharedSQLConnLifetime) retain() (func() error, bool) {
	if l == nil {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.conn == nil {
		return nil, false
	}
	l.refs++
	return l.release, true
}

func (l *sharedSQLConnLifetime) release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.closed || l.refs <= 0 {
		l.mu.Unlock()
		return nil
	}
	l.refs--
	if l.refs > 0 {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (l *sqlAdvisoryLockLease) BindContext(ctx context.Context) context.Context {
	if l == nil || l.conn == nil {
		return ctx
	}
	ctx = runtimepipeline.WithPipelineSQLConnContext(ctx, l.conn)
	if l.lifetime != nil {
		ctx = withSharedSQLConnLifetime(ctx, l.lifetime)
	}
	return ctx
}

func (l *sqlAdvisoryLockLease) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, unlockErr := l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.lockKey)
	releaseErr := error(nil)
	if l.releaseConn != nil {
		releaseErr = l.releaseConn()
	}
	l.conn = nil
	l.releaseConn = nil
	if unlockErr != nil {
		return fmt.Errorf("release advisory lock: %w", unlockErr)
	}
	if releaseErr != nil {
		return fmt.Errorf("close advisory lock connection: %w", releaseErr)
	}
	return nil
}

func acquireAdvisoryLockLease(ctx context.Context, db *sql.DB, lockKey string) (*sqlAdvisoryLockLease, bool, error) {
	if db == nil {
		return nil, false, nil
	}
	lockKey = strings.TrimSpace(lockKey)
	if lockKey == "" {
		return nil, false, fmt.Errorf("advisory lock key is required")
	}
	conn, borrowed := runtimepipeline.PipelineSQLConnFromContext(ctx)
	var lifetime *sharedSQLConnLifetime
	var releaseConn func() error
	if !borrowed {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("acquire advisory lock connection: %w", err)
		}
		lifetime = newSharedSQLConnLifetime(conn)
		releaseConn = lifetime.release
	} else if borrowedLifetime, ok := sharedSQLConnLifetimeFromContext(ctx); ok {
		lifetime = borrowedLifetime
		var retained bool
		releaseConn, retained = lifetime.retain()
		if !retained {
			return nil, false, fmt.Errorf("acquire advisory lock connection: shared connection lifetime is closed")
		}
	}
	var acquired bool
	query := conn.QueryRowContext
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		query = tx.QueryRowContext
	}
	if err := query(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired); err != nil {
		if releaseConn != nil {
			_ = releaseConn()
		}
		return nil, false, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !acquired {
		if releaseConn != nil {
			_ = releaseConn()
		}
		return nil, false, nil
	}
	return &sqlAdvisoryLockLease{conn: conn, lockKey: lockKey, lifetime: lifetime, releaseConn: releaseConn}, true, nil
}

func replayClaimLockKey(eventID string) string {
	return pipelineReplayClaimNamespace + strings.TrimSpace(eventID)
}

func scheduleClaimLockKey(sc runtimepipeline.Schedule) string {
	return scheduleClaimNamespace + strings.Join([]string{
		strings.TrimSpace(sc.EffectiveTimerID()),
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}
