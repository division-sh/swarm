package apiidempotency

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
)

type sqliteRequestLockKey struct {
	database string
	request  string
}

type sqliteRequestLock struct {
	permit chan struct{}
	refs   int
}

type sqliteRequestLockRegistry struct {
	sync.Mutex
	entries map[sqliteRequestLockKey]*sqliteRequestLock
}

type sqliteRequestLockLease struct {
	registry *sqliteRequestLockRegistry
	key      sqliteRequestLockKey
	entry    *sqliteRequestLock
	once     sync.Once
}

func (l *sqliteRequestLockLease) release() {
	l.once.Do(func() {
		l.entry.permit <- struct{}{}
		l.registry.releaseReference(l.key, l.entry)
	})
}

var sqliteRequestLocks = sqliteRequestLockRegistry{entries: make(map[sqliteRequestLockKey]*sqliteRequestLock)}

func sqliteIdempotencyDatabaseIdentity(path string) string {
	key := strings.TrimSpace(path)
	if key == "" {
		return "<unknown>"
	}
	if absolute, err := filepath.Abs(filepath.Clean(key)); err == nil {
		return absolute
	}
	return key
}

func (r *sqliteRequestLockRegistry) acquire(ctx context.Context, key sqliteRequestLockKey) (*sqliteRequestLockLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.Lock()
	entry := r.entries[key]
	if entry == nil {
		entry = &sqliteRequestLock{permit: make(chan struct{}, 1)}
		entry.permit <- struct{}{}
		r.entries[key] = entry
	}
	// Waiting references keep the same election alive until their disposition.
	entry.refs++
	r.Unlock()
	select {
	case <-ctx.Done():
		r.releaseReference(key, entry)
		return nil, ctx.Err()
	case <-entry.permit:
		if err := ctx.Err(); err != nil {
			entry.permit <- struct{}{}
			r.releaseReference(key, entry)
			return nil, err
		}
		return &sqliteRequestLockLease{registry: r, key: key, entry: entry}, nil
	}
}

func (r *sqliteRequestLockRegistry) releaseReference(key sqliteRequestLockKey, entry *sqliteRequestLock) {
	r.Lock()
	defer r.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(r.entries, key)
	}
}
