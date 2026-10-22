package apiidempotency

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiidempotencycontract "github.com/division-sh/swarm/internal/apiidempotency"
)

func waitSQLiteLockReferences(t *testing.T, r *sqliteRequestLockRegistry, key sqliteRequestLockKey, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		r.Lock()
		got := 0
		if entry := r.entries[key]; entry != nil {
			got = entry.refs
		}
		r.Unlock()
		if got == want {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("key references=%d, want %d", got, want)
		}
		runtime.Gosched()
	}
}

func TestSQLiteRequestLockCancellationPreservesWaiterCustody(t *testing.T) {
	r := sqliteRequestLockRegistry{entries: make(map[sqliteRequestLockKey]*sqliteRequestLock)}
	key := sqliteRequestLockKey{database: "database", request: "identity"}
	release, err := r.acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release.release)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		unlock, err := r.acquire(ctx, key)
		if unlock != nil {
			unlock.release()
		}
		done <- err
	}()
	waitSQLiteLockReferences(t, &r, key, 2)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter=%v", err)
	}
	waitSQLiteLockReferences(t, &r, key, 1)
	r.Lock()
	original := r.entries[key]
	r.Unlock()
	next := make(chan *sqliteRequestLockLease, 1)
	go func() {
		unlock, err := r.acquire(t.Context(), key)
		if err != nil {
			t.Error(err)
		}
		next <- unlock
	}()
	waitSQLiteLockReferences(t, &r, key, 2)
	r.Lock()
	same := r.entries[key] == original
	r.Unlock()
	if !same {
		t.Fatal("cancellation split the live election")
	}
	release.release()
	release.release()
	unlock := <-next
	if unlock == nil {
		t.Fatal("waiting successor did not acquire")
	}
	// Releasing the old lease again cannot unlock the successor.
	release.release()
	waitSQLiteLockReferences(t, &r, key, 1)
	unlock.release()
	waitSQLiteLockReferences(t, &r, key, 0)
	if unlock, err := r.acquire(ctx, key); unlock != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled admission=%v, %v", unlock != nil, err)
	}
	waitSQLiteLockReferences(t, &r, key, 0)
}

func TestSQLiteRequestLockConcurrentReclamationKeepsOneHolder(t *testing.T) {
	r := sqliteRequestLockRegistry{entries: make(map[sqliteRequestLockKey]*sqliteRequestLock)}
	key := sqliteRequestLockKey{database: "database", request: "identity"}
	var active, violations atomic.Int32
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			for range 32 {
				release, err := r.acquire(t.Context(), key)
				if err != nil {
					t.Error(err)
					return
				}
				if active.Add(1) != 1 {
					violations.Add(1)
				}
				runtime.Gosched()
				active.Add(-1)
				release.release()
				release.release()
			}
		})
	}
	workers.Wait()
	if active.Load() != 0 || violations.Load() != 0 {
		t.Fatalf("active=%d overlapping holders=%d", active.Load(), violations.Load())
	}
	waitSQLiteLockReferences(t, &r, key, 0)
	r.Lock()
	defer r.Unlock()
	if len(r.entries) != 0 {
		t.Fatalf("unowned registry entries=%d", len(r.entries))
	}
}

func TestSQLiteRequestLockCanonicalIdentityAndIndependence(t *testing.T) {
	r := sqliteRequestLockRegistry{entries: make(map[sqliteRequestLockKey]*sqliteRequestLock)}
	actor := apiidempotencycontract.BearerActor("actor")
	base := sqliteRequestLockKey{database: sqliteIdempotencyDatabaseIdentity(filepath.Join(t.TempDir(), "store.db")), request: apiIdempotencyLockKey("event.publish", actor, "key")}
	release, err := r.acquire(t.Context(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer release.release()
	for name, key := range map[string]sqliteRequestLockKey{
		"database":   {database: base.database + ".other", request: base.request},
		"method":     {database: base.database, request: apiIdempotencyLockKey("run.start", actor, "key")},
		"actor-id":   {database: base.database, request: apiIdempotencyLockKey("event.publish", apiidempotencycontract.BearerActor("other"), "key")},
		"actor-kind": {database: base.database, request: apiIdempotencyLockKey("event.publish", apiidempotencycontract.PrincipalActor("actor"), "key")},
		"key":        {database: base.database, request: apiIdempotencyLockKey("event.publish", actor, "other")},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			unlock, err := r.acquire(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			unlock.release()
		})
	}
	if got := sqliteIdempotencyDatabaseIdentity(filepath.Join(filepath.Dir(base.database), ".", "store.db")); got != base.database {
		t.Fatalf("database path identity changed: %s != %s", got, base.database)
	}
}
