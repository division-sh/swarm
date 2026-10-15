package storetest

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
)

func TestResetCompletionStorageAndFaultPortsRefuseUnavailableOwnersBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) RunFixtureStore
	}{
		{"sqlite", func(t *testing.T) RunFixtureStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) RunFixtureStore { return StartPostgresRuntimeStore(t) }},
	} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			if count, err := CountResetCompletionStorageOperations(context.Background(), selected); err != nil || count != 0 {
				t.Fatalf("empty reset physical cardinality: %d/%v", count, err)
			}
			if raw, err := ReadResetCompletionStorageRecord(context.Background(), selected); raw != nil || !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("absent reset physical record: %s/%v", raw, err)
			}
			for i, action := range resetCompletionOwnershipActions() {
				for _, foreign := range []any{nil, &sql.DB{}, &sql.Tx{}, struct{}{}} {
					if zero, err := action(context.Background(), foreign); !zero || err == nil {
						t.Fatalf("raw/foreign %T supplied evidence/fault: %t/%v", foreign, zero, err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				zero, err := action(ctx, selected)
				if !zero || err == nil || ((i < 2 || backend.name == "postgres") && !errors.Is(err, context.Canceled)) {
					t.Fatalf("cancelled owner supplied evidence/fault: %t/%v", zero, err)
				}
			}
			if backend.name == "sqlite" {
				if err := InstallPostgresResetAPICompletionLoss(context.Background(), selected); err == nil {
					t.Fatal("postgres-only completion fault admitted a foreign backend")
				}
			} else {
				assertResetCompletionFaultUsesOriginalCoordinator(t, selected)
			}
			if err := selected.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			for _, action := range resetCompletionOwnershipActions() {
				if zero, err := action(context.Background(), selected); !zero || err == nil {
					t.Fatalf("closed owner supplied evidence/fault: %t/%v", zero, err)
				}
			}
		})
	}
}

func resetCompletionOwnershipActions() []func(context.Context, any) (bool, error) {
	return []func(context.Context, any) (bool, error){
		func(ctx context.Context, selected any) (bool, error) {
			count, err := CountResetCompletionStorageOperations(ctx, selected)
			return count == 0, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			raw, err := ReadResetCompletionStorageRecord(ctx, selected)
			return raw == nil, err
		},
		func(ctx context.Context, selected any) (bool, error) {
			return true, InstallPostgresResetAPICompletionLoss(ctx, selected)
		},
		func(ctx context.Context, selected any) (bool, error) {
			return true, RemovePostgresResetAPICompletionLoss(ctx, selected)
		},
	}
}

func assertResetCompletionFaultUsesOriginalCoordinator(t *testing.T, selected any) {
	t.Helper()
	collector := CollectTransactions(t, selected, TransactionProbeOptions{})
	before := collector.Snapshot()
	if err := InstallPostgresResetAPICompletionLoss(context.Background(), selected); err != nil {
		t.Fatal(err)
	}
	restore := sync.OnceValue(func() error { return RemovePostgresResetAPICompletionLoss(context.Background(), selected) })
	defer func() {
		if err := restore(); err != nil {
			t.Error(err)
		}
	}()
	after := collector.Snapshot()
	if after.Total.Begun != before.Total.Begun+1 || after.Total.WriteCommits != before.Total.WriteCommits+1 || after.Active != 0 {
		t.Fatalf("completion fault install bypassed exact selected coordinator: %+v/%+v", before, after)
	}
	if err := restore(); err != nil {
		t.Fatal(err)
	}
	removed := collector.Snapshot()
	if removed.Total.Begun != after.Total.Begun+1 || removed.Total.WriteCommits != after.Total.WriteCommits+1 || removed.Active != 0 {
		t.Fatalf("completion fault removal bypassed exact selected coordinator: %+v/%+v", after, removed)
	}
	if err := RemovePostgresResetAPICompletionLoss(context.Background(), selected); err == nil {
		t.Fatal("missing completion fault supplied successful removal")
	}
	JoinReporterTransactions(t, collector)
}
