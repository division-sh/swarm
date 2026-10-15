package serveapp

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedSelectedRole[T any](ctx context.Context, reader servedEventCountReader, read func(context.Context) (T, error)) (T, error) {
	var out T
	if reader == nil {
		return out, fmt.Errorf("selected read role requires its original inspection reader")
	}
	missing := false
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		out, err = read(ctx)
		if err == sql.ErrNoRows {
			missing = true
			return nil
		}
		return err
	})
	if err == nil && missing {
		err = sql.ErrNoRows
	}
	if err != nil {
		var empty T
		return empty, err
	}
	return out, err
}

func readServedA2DomainCounts(t *testing.T, reader receiverProofStateReader) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, read := range []struct {
		name      string
		operation func(context.Context, any) (int, error)
	}{
		{"runs", storetest.CountGlobalRunStorage},
		{"events", storetest.CountPhysicalEventsStorage},
		{"event_deliveries", storetest.CountPhysicalEventDeliveries},
		{"entity_state", storetest.CountPhysicalEntities},
		{"api_idempotency", storetest.CountPhysicalAPIReceiptsStorage},
	} {
		count, err := readServedSelectedRole(context.Background(), reader, func(ctx context.Context) (int, error) { return read.operation(ctx, reader) })
		if err != nil {
			t.Fatal(err)
		}
		counts[read.name] = count
	}
	return counts
}

func readServedReporterSnapshot(t *testing.T, reader receiverProofStateReader, runID string) [][]sql.NullString {
	t.Helper()
	var snapshot [][]sql.NullString
	for _, read := range []func(context.Context, any, string) (storetest.ServedTemplateCompletionDiagnosticStorage, error){
		storetest.ReadReporterIntentSnapshotStorage,
		storetest.ReadReporterOutcomeSnapshotStorage,
		storetest.ReadReporterEventSnapshotStorage,
	} {
		observed, err := readServedSelectedRole(context.Background(), reader, func(ctx context.Context) (storetest.ServedTemplateCompletionDiagnosticStorage, error) {
			return read(ctx, reader, runID)
		})
		if err != nil {
			t.Fatal(err)
		}
		if observed.ColumnsError != nil {
			t.Fatal(observed.ColumnsError)
		}
		snapshot = append(snapshot, observed.Rows...)
		if observed.ScanError != nil {
			t.Fatal(observed.ScanError)
		}
		if observed.IterationError != nil {
			t.Fatal(observed.IterationError)
		}
	}
	return snapshot
}
