package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	conn    *sql.Conn
	lockKey string
}

func (l *sqlAdvisoryLockLease) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := l.conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.lockKey); err != nil {
		_ = l.conn.Close()
		l.conn = nil
		return fmt.Errorf("release advisory lock: %w", err)
	}
	if err := l.conn.Close(); err != nil {
		l.conn = nil
		return fmt.Errorf("close advisory lock connection: %w", err)
	}
	l.conn = nil
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
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire advisory lock connection: %w", err)
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, false, nil
	}
	return &sqlAdvisoryLockLease{conn: conn, lockKey: lockKey}, true, nil
}

func replayClaimLockKey(eventID string) string {
	return pipelineReplayClaimNamespace + strings.TrimSpace(eventID)
}

func scheduleClaimLockKey(sc runtimepipeline.Schedule) string {
	return scheduleClaimNamespace + strings.Join([]string{
		strings.TrimSpace(sc.EffectiveRunID()),
		strings.TrimSpace(sc.AgentID),
		strings.TrimSpace(sc.EventType),
		strings.TrimSpace(sc.EffectiveEntityID()),
		strings.TrimSpace(sc.EffectiveFlowInstance()),
		strings.TrimSpace(sc.TaskID),
	}, "|")
}
