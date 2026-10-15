package runtimepersistence

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func requireStopSnapshotPortsRefuse(t *testing.T, ctx context.Context, selected any, runID string) {
	t.Helper()
	if rows, err := ReadStopFanOutIntentsStorageForTest(ctx, selected, runID); err == nil || rows != nil {
		t.Fatalf("stop intents returned provisional rows: %+v,%v", rows, err)
	}
	if rows, err := ReadStopFanOutOutcomesStorageForTest(ctx, selected, runID); err == nil || rows != nil {
		t.Fatalf("stop outcomes returned provisional rows: %+v,%v", rows, err)
	}
	if out, err := ReadStopRunControlClockStorageForTest(ctx, selected, runID); err == nil || out != (StopRunControlClockStorage{}) {
		t.Fatalf("stop clock returned provisional evidence: %+v,%v", out, err)
	}
	for _, read := range []func(context.Context, any, string) (int, error){CountRunStopRevisionRowsForTest, CountRunStopFactRevisionRowsForTest, CountRunStopNonterminalDeliveriesForTest} {
		if count, err := read(ctx, selected, runID); err == nil || count != 0 {
			t.Fatalf("stop cardinality returned provisional evidence: %d,%v", count, err)
		}
	}
}

func TestRunStopSnapshotPortsRefuseInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			runID, ctx := uuid.NewString(), context.Background()
			if rows, err := ReadStopFanOutIntentsStorageForTest(ctx, fixture.store, runID); err != nil || rows != nil {
				t.Fatalf("empty physical intent inventory=%+v,%v", rows, err)
			}
			if rows, err := ReadStopFanOutOutcomesStorageForTest(ctx, fixture.store, runID); err != nil || rows != nil {
				t.Fatalf("empty physical outcome inventory=%+v,%v", rows, err)
			}
			for _, read := range []func(context.Context, any, string) (int, error){CountRunStopRevisionRowsForTest, CountRunStopFactRevisionRowsForTest, CountRunStopNonterminalDeliveriesForTest} {
				if count, err := read(ctx, fixture.store, runID); err != nil || count != 0 {
					t.Fatalf("headerless physical cardinality=%d,%v", count, err)
				}
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			requireStopSnapshotPortsRefuse(t, cancelled, fixture.store, runID)
			requireStopSnapshotPortsRefuse(t, ctx, fixture.store, "foreign-invalid-run")
			switch selected := fixture.store.(type) {
			case *PostgresStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			}
			requireStopSnapshotPortsRefuse(t, ctx, fixture.store, runID)
		})
	}
	for _, selected := range []any{nil, struct{}{}, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil)} {
		requireStopSnapshotPortsRefuse(t, context.Background(), selected, uuid.NewString())
	}
}
