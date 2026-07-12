package store

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStore_TryAcquireSerializesDestructiveResetLock(t *testing.T) {
	dsn, _, cleanup := testutil.AcquirePostgres(t, testutil.PostgresRowState())
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	first, acquired, err := pg.TryAcquire(ctx, "test:destructive-reset")
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	if !acquired || first == nil {
		t.Fatalf("first TryAcquire acquired=%v lease=%#v, want acquired lease", acquired, first)
	}
	second, acquired, err := pg.TryAcquire(ctx, "test:destructive-reset")
	if err != nil {
		t.Fatalf("second TryAcquire: %v", err)
	}
	if acquired || second != nil {
		t.Fatalf("second TryAcquire acquired=%v lease=%#v, want contention", acquired, second)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	third, acquired, err := pg.TryAcquire(ctx, "test:destructive-reset")
	if err != nil {
		t.Fatalf("third TryAcquire: %v", err)
	}
	if !acquired || third == nil {
		t.Fatalf("third TryAcquire acquired=%v lease=%#v, want acquired after release", acquired, third)
	}
	if err := third.Release(ctx); err != nil {
		t.Fatalf("release third lease: %v", err)
	}
}
