package storetest

import (
	"context"
	"testing"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type WorkflowTimerReplayStorageObservation = private.WorkflowTimerReplayStorageObservation

func ObserveWorkflowTimerReplayStorage(t testing.TB, ctx context.Context, selected any, runID, entityID string) WorkflowTimerReplayStorageObservation {
	t.Helper()
	observed, err := private.ObserveWorkflowTimerReplayStorageForTest(ctx, selected, runID, entityID)
	if err != nil {
		t.Fatalf("observe exact timer replay storage: %v", err)
	}
	return observed
}
