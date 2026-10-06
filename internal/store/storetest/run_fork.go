package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type SelectedForkStorageTableSnapshot = private.SelectedForkStorageTableSnapshot
type SelectedForkControlStorage = private.SelectedForkControlStorage

func ReadSelectedForkControlStorage(ctx context.Context, selected any, runID, loadedBundleHash, agentID string) (SelectedForkControlStorage, error) {
	return private.ReadSelectedForkControlStorageForTest(ctx, selected, runID, loadedBundleHash, agentID)
}

func ReadSelectedForkApplicationStorageSnapshot(ctx context.Context, selected any) (map[string]SelectedForkStorageTableSnapshot, error) {
	return private.ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
}

func ReadSelectedForkRunBundleHash(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadSelectedForkRunBundleHashForTest(ctx, selected, runID)
}

func CaptureRunForkSnapshot(t testing.TB, ctx context.Context, selected any, runID string) {
	t.Helper()
	if err := private.CaptureRunForkSnapshotForTest(ctx, selected, runID); err != nil {
		t.Fatalf("capture canonical run-fork snapshot: %v", err)
	}
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
