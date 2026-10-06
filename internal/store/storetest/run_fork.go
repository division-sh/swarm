package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedForkStorageTableSnapshot = private.SelectedForkStorageTableSnapshot

func ReadSelectedForkApplicationStorageSnapshot(ctx context.Context, selected any) (map[string]SelectedForkStorageTableSnapshot, error) {
	return private.ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
}

// RequireRunForkReplayResumeBlocker inspects the store's typed refusal without
// turning its private error representation into a production API.
func RequireRunForkReplayResumeBlocker(t testing.TB, err error, code, fact string) {
	t.Helper()
	blocker, gotFact, ok := runforkpersistence.RunForkReplayResumeBlockerFromError(err)
	if !ok || blocker.Code != code || gotFact != fact {
		t.Fatalf("fork refusal: blocker=%+v fact=%q typed=%t err=%v; want %s/%s", blocker, gotFact, ok, err, code, fact)
	}
}
