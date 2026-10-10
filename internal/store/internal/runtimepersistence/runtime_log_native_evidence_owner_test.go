package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestRuntimeLogNativeEvidenceOwnersRejectUninitializedAuthority(t *testing.T) {
	ctx := context.Background()
	const runID = "00000000-0000-4000-8000-000000001332"
	for _, selected := range []any{nil, (*PostgresStore)(nil), &PostgresStore{}, (*SQLiteRuntimeStore)(nil), &SQLiteRuntimeStore{}, struct{}{}} {
		if snapshot, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, runID); err == nil || snapshot.RunID != "" {
			t.Fatalf("uninitialized lifecycle read succeeded: %T/%+v/%v", selected, snapshot, err)
		}
		for _, startup := range []bool{false, true} {
			if event, err := readLatestRuntimeLogRecordForTest(ctx, selected, startup); err == nil || event.ID() != "" {
				t.Fatalf("uninitialized diagnostic read succeeded: %T/%s/%v", selected, event.ID(), err)
			}
		}
		if removed, err := RemoveRuntimeLogFixtureSourceArtifactForTest(ctx, selected, sourceartifactfixture.BundleHash); err == nil || removed != 0 {
			t.Fatalf("uninitialized source fault succeeded: %T/%d/%v", selected, removed, err)
		}
		if err := SetRuntimeLogFixtureAppendFaultForTest(ctx, selected, runID, true); err == nil {
			t.Fatalf("uninitialized append fault succeeded: %T", selected)
		}
	}
}
