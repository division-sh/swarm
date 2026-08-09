package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func TestPostgresStore_AcquireDestructiveResetSerializesOperationLease(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	pg, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	bootstrapTestPostgresStore(t, pg)
	t.Cleanup(func() { _ = pg.backend.Close() })

	ctx := testAuthorActivityContext()
	first, acquired, err := pg.AcquireDestructiveReset(ctx)
	if err != nil {
		t.Fatalf("first AcquireDestructiveReset: %v", err)
	}
	if !acquired || first == nil {
		t.Fatalf("first TryAcquire acquired=%v lease=%#v, want acquired lease", acquired, first)
	}
	second, acquired, err := pg.AcquireDestructiveReset(ctx)
	if err != nil {
		t.Fatalf("second AcquireDestructiveReset: %v", err)
	}
	if acquired || second != nil {
		t.Fatalf("second TryAcquire acquired=%v lease=%#v, want contention", acquired, second)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	third, acquired, err := pg.AcquireDestructiveReset(ctx)
	if err != nil {
		t.Fatalf("third AcquireDestructiveReset: %v", err)
	}
	if !acquired || third == nil {
		t.Fatalf("third TryAcquire acquired=%v lease=%#v, want acquired after release", acquired, third)
	}
	if err := third.Release(ctx); err != nil {
		t.Fatalf("release third lease: %v", err)
	}
}
