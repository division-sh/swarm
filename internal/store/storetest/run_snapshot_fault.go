package storetest

import (
	"context"
	"testing"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

// RequireCorruptRunSnapshot is reserved for hostile persisted-state tests
// whose snapshots valid lifecycle construction forbids.
func RequireCorruptRunSnapshot(t testing.TB, ctx context.Context, selected any, snapshot runlifecyclefixture.CorruptSnapshot) {
	t.Helper()
	private.RequireCorruptRunSnapshotForTest(t, ctx, selected, snapshot)
}

func AttemptCorruptRunSnapshot(ctx context.Context, selected any, snapshot runlifecyclefixture.CorruptSnapshot) error {
	return private.AttemptCorruptRunSnapshotForTest(ctx, selected, snapshot)
}
