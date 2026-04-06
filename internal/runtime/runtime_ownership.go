package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const runtimeSharedStoreOwnershipLock = "swarm:runtime:shared-store-owner"

type runtimeStoreOwnershipLease interface {
	Release(context.Context) error
}

type sqlRuntimeOwnershipLease struct {
	mu       sync.Mutex
	conn     *sql.Conn
	released bool
}

func acquireRuntimeStoreOwnership(ctx context.Context, db *sql.DB, ownerID string) (runtimeStoreOwnershipLease, error) {
	if db == nil {
		return nil, nil
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return nil, fmt.Errorf("runtime owner id is required")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire runtime ownership connection: %w", err)
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, runtimeSharedStoreOwnershipLock).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire shared runtime store ownership for %s: %w", ownerID, err)
	}
	if !acquired {
		_ = conn.Close()
		return nil, fmt.Errorf("shared runtime store already owned by another runtime instance")
	}
	return &sqlRuntimeOwnershipLease{conn: conn}, nil
}

func (l *sqlRuntimeOwnershipLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	conn := l.conn
	l.conn = nil
	l.mu.Unlock()
	if conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, runtimeSharedStoreOwnershipLock); err != nil {
		_ = conn.Close()
		return fmt.Errorf("release shared runtime store ownership: %w", err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close runtime ownership connection: %w", err)
	}
	return nil
}

func newRuntimeOwnerID() string {
	host, _ := os.Hostname()
	host = strings.TrimSpace(host)
	if host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString())
}
