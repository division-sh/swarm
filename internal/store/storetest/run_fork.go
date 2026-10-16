package storetest

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	"github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
)

func ReadRunStatusStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadRunStatusStorageForTest(ctx, selected, runID)
}

func ReadSelectedSourceRefusalStorage(ctx context.Context, selected any, sourceRunID, sourceEventID string) (map[string]int, error) {
	return private.ReadSelectedSourceRefusalStorageForTest(ctx, selected, sourceRunID, sourceEventID)
}

func ReadSelectedForkRefusalStorage(ctx context.Context, selected any, forkRunID string) (map[string]int, error) {
	return private.ReadSelectedForkRefusalStorageForTest(ctx, selected, forkRunID)
}

func CountSelectedForkRunsForSource(ctx context.Context, selected any, sourceRunID string) (int, error) {
	return private.CountSelectedForkRunsForSourceForTest(ctx, selected, sourceRunID)
}

func CountRunNamedEventStorage(ctx context.Context, selected any, runID, eventName string) (int, error) {
	return private.CountRunNamedEventStorageForTest(ctx, selected, runID, eventName)
}

type SelectedForkStorageTableSnapshot = private.SelectedForkStorageTableSnapshot
type SelectedForkControlStorage = private.SelectedForkControlStorage

func ReadSelectedForkExecutableCardinality(ctx context.Context, selected any, runID string) (map[string]string, error) {
	return private.ReadSelectedForkExecutableCardinalityForTest(ctx, selected, runID)
}

func ReadSelectedForkGlobalCardinality(ctx context.Context, selected any) (map[string]int, error) {
	return private.ReadSelectedForkGlobalCardinalityForTest(ctx, selected)
}

func ReadSelectedForkDescendantCardinality(ctx context.Context, selected any, runID string) (map[string]int, error) {
	return private.ReadSelectedForkDescendantCardinalityForTest(ctx, selected, runID)
}

func ReadSelectedForkControlStorage(ctx context.Context, selected any, runID, loadedBundleHash, agentID string) (SelectedForkControlStorage, error) {
	return private.ReadSelectedForkControlStorageForTest(ctx, selected, runID, loadedBundleHash, agentID)
}

func ReadVersionOneDeliveredSettlementCount(ctx context.Context, selected any, deliveryID string) (int, error) {
	return private.ReadVersionOneDeliveredSettlementCountForTest(ctx, selected, deliveryID)
}

func ReadSelectedForkAuthoredMutation(ctx context.Context, selected any, runID, entityID, eventID string) (string, error) {
	return private.ReadSelectedForkAuthoredMutationForTest(ctx, selected, runID, entityID, eventID)
}

func ReadSelectedForkApplicationStorageSnapshot(ctx context.Context, selected any) (map[string]SelectedForkStorageTableSnapshot, error) {
	return private.ReadSelectedForkApplicationStorageSnapshotForTest(ctx, selected)
}

func ReadSelectedForkRecoveredStorageSnapshot(ctx context.Context, selected any, runID string) (map[string]SelectedForkStorageTableSnapshot, error) {
	return private.ReadSelectedForkRecoveredStorageSnapshotForTest(ctx, selected, runID)
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

func CaptureSelectedRunForkRevision(t testing.TB, ctx context.Context, selected any, runID string, families ...runforkrevisionfixture.Family) int64 {
	t.Helper()
	revision, err := private.CaptureSelectedRunForkRevisionForTest(ctx, selected, runID, families...)
	if err != nil {
		t.Fatalf("capture selected run-fork revision: %v", err)
	}
	return revision
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
