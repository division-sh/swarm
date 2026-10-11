package runtimepersistence

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestSelectedRuntimeOutcomeScalarsPreservePhysicalScopeAndOriginalCoordinatorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			selected, ctx := backend.open(t).store, testAuthorActivityContext()
			event := runForkSnapshotFixtureEvent(t, ctx, selected)
			foreign := runForkSnapshotFixtureEvent(t, ctx, selected)
			probe, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, coordinate := range []struct {
				run, event string
				want       int
			}{
				{event.RunID(), event.ID(), 1},
				{foreign.RunID(), event.ID(), 0},
				{event.RunID(), foreign.ID(), 0},
				{event.RunID(), uuid.NewString(), 0},
			} {
				if count, err := CountRunExactEventStorageForTest(ctx, selected, coordinate.run, coordinate.event); err != nil || count != coordinate.want {
					t.Fatalf("physical event count changed scope: %d/%v want %d", count, err, coordinate.want)
				}
			}
			if count, err := CountSelectedContractBindingStorageForTest(ctx, selected); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated a selected binding: %d/%v", count, err)
			}
			if count, err := CountSelectedRuntimeExecutionStorageForTest(ctx, selected, event.RunID()); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated runtime execution: %d/%v", count, err)
			}
			if count, err := CountRunFlowInstanceStorageForTest(ctx, selected, event.RunID()); err != nil || count != 0 {
				t.Fatalf("ordinary source fabricated construction: %d/%v", count, err)
			}
			if state, err := ReadSelectedRuntimeExecutionStateStorageForTest(ctx, selected, event.RunID()); !errors.Is(err, sql.ErrNoRows) || state != "" {
				t.Fatalf("source run status acquired execution-state authority: %q/%v", state, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 7 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("physical outcome reads escaped original coordinator: %+v", counts)
			}
		})
	}
}
