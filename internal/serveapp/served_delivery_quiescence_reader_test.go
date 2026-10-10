package serveapp

import (
	"context"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type servedRunDeliveryReader func(context.Context, string) (runtimedelivery.RunSummary, error)

func servedInspectionDeliveryReader(t testing.TB, inspection store.ReadOnlyInspection) servedRunDeliveryReader {
	t.Helper()
	if inspection == nil {
		t.Fatal("served delivery inspection requires its original read-only owner")
	}
	return func(ctx context.Context, runID string) (runtimedelivery.RunSummary, error) {
		var summary runtimedelivery.RunSummary
		err := inspection.InspectSnapshot(ctx, func(snapshot context.Context) error {
			var err error
			summary, err = storetest.ReadServedRunDeliverySummary(snapshot, inspection, runID)
			return err
		})
		return summary, err
	}
}

func openServedInspectionDeliveryReader(t testing.TB, backend, location string) servedRunDeliveryReader {
	t.Helper()
	inspection, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inspection.Close(); err != nil {
			t.Errorf("close served delivery inspection: %v", err)
		}
	})
	return servedInspectionDeliveryReader(t, inspection)
}

func TestServedRunDeliveryQuiescenceReaderPreservesStableStatusCut(t *testing.T) {
	for _, test := range []struct {
		name string
		read []runtimedelivery.RunSummary
	}{
		{name: "pending_and_in_progress_reset_stability", read: []runtimedelivery.RunSummary{
			{RunID: "run", Total: 1, Pending: 1},
			{RunID: "run"}, {RunID: "run"}, {RunID: "run"},
			{RunID: "run", Total: 1, InProgress: 1},
			{RunID: "run"}, {RunID: "run"}, {RunID: "run"}, {RunID: "run"},
		}},
		{name: "retry_scheduled_remains_outside_original_predicate", read: []runtimedelivery.RunSummary{
			{RunID: "run", Total: 1, RetryScheduled: 1},
			{RunID: "run", Total: 1, RetryScheduled: 1},
			{RunID: "run", Total: 1, RetryScheduled: 1},
			{RunID: "run", Total: 1, RetryScheduled: 1},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reads := 0
			waitServedRunDeliveryQuiescence(t, func(_ context.Context, runID string) (runtimedelivery.RunSummary, error) {
				if runID != "run" || reads >= len(test.read) {
					t.Fatalf("delivery summary read escaped exact run/stability cut: run=%s,reads=%d", runID, reads)
				}
				result := test.read[reads]
				reads++
				return result, nil
			}, "run")
			if reads != len(test.read) {
				t.Fatalf("delivery summary reads=%d,want%d", reads, len(test.read))
			}
		})
	}
}
