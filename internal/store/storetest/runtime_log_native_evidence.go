package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadRuntimeLogRunSnapshot(ctx context.Context, selected any, runID string) (runlifecycle.Snapshot, error) {
	return private.ReadRuntimeLogRunSnapshotForTest(ctx, selected, runID)
}

func ReadLatestRuntimeLogRecord(ctx context.Context, selected any) (events.Event, error) {
	return private.ReadLatestRuntimeLogRecordForTest(ctx, selected)
}

func ReadLatestStartupRecoveryDecisionRecord(ctx context.Context, selected any) (events.Event, error) {
	return private.ReadLatestStartupRecoveryDecisionRecordForTest(ctx, selected)
}

func RemoveRuntimeLogFixtureSourceArtifact(ctx context.Context, selected any, hash string) (int64, error) {
	return private.RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, selected, hash)
}

func SetRuntimeLogFixtureAppendFault(ctx context.Context, selected any, runID string, enabled bool) error {
	return private.SetRuntimeLogFixtureAppendFaultForTest(ctx, selected, runID, enabled)
}
