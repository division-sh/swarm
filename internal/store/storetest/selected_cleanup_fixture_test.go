package storetest

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func TestPostgresSelectedCleanupStorageRetainsEightIndependentNativeReadsAndRefusesUnavailableOwners(t *testing.T) {
	selected := StartPostgresRuntimeStore(t)
	ctx := context.Background()
	probe := CollectTransactions(t, selected, TransactionProbeOptions{})
	if rows, err := ReadPostgresSelectedForkCleanupStorage(ctx, selected, uuid.NewString()); rows != (SelectedForkCleanupStorageCounts{}) || err != nil {
		t.Fatalf("absent cleanup storage manufactured evidence: %+v err=%v", rows, err)
	}
	if counts := probe.Snapshot(); counts.Total.ReadCommits != 8 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
		t.Fatalf("cleanup reads merged cuts, wrote state or retained work: %+v", counts)
	}
	for _, id := range []string{"", "invalid-run"} {
		if rows, err := ReadPostgresSelectedForkCleanupStorage(ctx, selected, id); rows != (SelectedForkCleanupStorageCounts{}) || err == nil {
			t.Fatalf("invalid run supplied cleanup evidence: %+v err=%v", rows, err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, err := ReadPostgresSelectedForkCleanupStorage(cancelled, selected, uuid.NewString()); rows != (SelectedForkCleanupStorageCounts{}) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup supplied provisional evidence: %+v err=%v", rows, err)
	}
	var nilPG *store.PostgresStore
	var nilSQLite *store.SQLiteRuntimeStore
	for _, owner := range []any{nil, nilPG, nilSQLite, &sql.DB{}, &sql.Tx{}, StartSQLiteRuntimeStore(t)} {
		if rows, err := ReadPostgresSelectedForkCleanupStorage(ctx, owner, uuid.NewString()); rows != (SelectedForkCleanupStorageCounts{}) || err == nil {
			t.Fatalf("foreign %T supplied cleanup evidence: %+v err=%v", owner, rows, err)
		}
	}
	if err := selected.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, err := ReadPostgresSelectedForkCleanupStorage(ctx, selected, uuid.NewString()); rows != (SelectedForkCleanupStorageCounts{}) || err == nil {
		t.Fatalf("closed cleanup owner supplied evidence: %+v err=%v", rows, err)
	}
}

func TestPostgresSelectedCleanupFaultPortsUseOriginalWriterAndRefuseUnavailableOwners(t *testing.T) {
	for _, fault := range []struct {
		name string
		set  func(context.Context, any, bool) error
	}{
		{"replay-scope-collision", SetSelectedReplayScopeCollisionFault},
		{"lineage-insert", SetSelectedExecutionLineageInsertFault},
	} {
		t.Run(fault.name, func(t *testing.T) {
			selected := StartPostgresRuntimeStore(t)
			ctx := context.Background()
			t.Cleanup(func() {
				if err := fault.set(ctx, selected, false); err != nil {
					t.Errorf("remove exact selected fault: %v", err)
				}
			})
			probe := CollectTransactions(t, selected, TransactionProbeOptions{})
			if err := fault.set(ctx, selected, true); err != nil {
				t.Fatal(err)
			}
			if err := fault.set(ctx, selected, false); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.WriteCommits != 2 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
				t.Fatalf("fault install/remove bypassed original writer or leaked work: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if err := fault.set(cancelled, selected, true); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled fault did not refuse: %v", err)
			}
			var nilPG *store.PostgresStore
			var nilSQLite *store.SQLiteRuntimeStore
			for _, owner := range []any{nil, nilPG, nilSQLite, &sql.DB{}, &sql.Tx{}, StartSQLiteRuntimeStore(t)} {
				if err := fault.set(ctx, owner, true); err == nil {
					t.Fatalf("foreign %T installed the fault", owner)
				}
			}
		})
	}
}
