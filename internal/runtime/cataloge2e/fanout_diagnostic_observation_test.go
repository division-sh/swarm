package cataloge2e

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogFanOutDiagnosticPreservesSourceScopeBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
			var last catalogTriggerStep
			for _, input := range []struct {
				event   string
				payload map[string]any
			}{
				{"batch.opened", map[string]any{"batch_id": "native-diagnostic", "expected_item_ids": []any{"alpha"}}},
				{"batch.submitted", map[string]any{"batch_id": "native-diagnostic", "items": []any{map[string]any{"item_id": "alpha", "value": "red"}}}},
			} {
				last = catalogTriggerStep{Event: input.event, Payload: input.payload, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
				if err := h.publishRuntimeEventResultForStep(last, 20*time.Second, false); err != nil {
					t.Fatal(err)
				}
				scatterGatherWait(t, h, last, time.Time{}, time.Time{})
			}
			h.shutdown()
			h.db = nil
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			ctx := testAuthorActivityContext(context.Background())
			var selected any = h.sqlite
			if h.pg != nil {
				selected = h.pg
			}
			physicalEvents, err := storetest.ReadCausalEventStorageSince(ctx, reader, h.startedAt)
			if err != nil {
				t.Fatal(err)
			}
			wantEvents, hasRuntimeLog := 0, false
			for _, event := range physicalEvents {
				if event.SourceEventID != last.eventID {
					continue
				}
				wantEvents++
				hasRuntimeLog = hasRuntimeLog || event.Name == "platform.runtime_log"
			}
			if wantEvents != 2 || !hasRuntimeLog {
				t.Fatalf("physical source fixture must include emitted item and runtime log: events=%d log=%v", wantEvents, hasRuntimeLog)
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, reader, catalogRuntimeRunID, last.eventID)
			if err != nil || len(rows) != 1 || rows[0].Status != "closed" || rows[0].Cardinality != 1 || rows[0].Cursor != 1 || rows[0].Owner != "" {
				t.Fatalf("source fan-out diagnostic=%+v err=%v", rows, err)
			}
			facts, err := storetest.ReadSourceRouteSettlementStorage(ctx, reader, catalogRuntimeRunID, last.eventID)
			if err != nil || facts.Events != wantEvents || facts.DistinctLedgers != wantEvents || facts.Bytes == 0 {
				t.Fatalf("physical source settlements=%+v err=%v", facts, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 2 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("fan-out diagnostics escaped original coordinator: %+v", proof)
			}
			logScatterGatherProgress(t, h, last.eventID)
			requireScatterGatherNativeFrontier(t, ctx, reader, probe, last.eventID)
			requireScatterGatherConservationRead(t, ctx, reader, probe)
			requireFanOutDiagnosticRefusals(t, ctx, reader, last.eventID)
			if h.pg != nil {
				err = h.pg.Close()
			} else {
				err = h.sqlite.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, reader, catalogRuntimeRunID, last.eventID); err == nil || rows != nil {
				t.Fatalf("closed fan-out diagnostic=%v err=%v", rows, err)
			}
			if facts, err := storetest.ReadSourceRouteSettlementStorage(ctx, reader, catalogRuntimeRunID, last.eventID); err == nil || facts != (storetest.SourceRouteSettlementStorage{}) {
				t.Fatalf("closed settlement witness=%+v err=%v", facts, err)
			}
			if frontier, err := storetest.ReadCausalDeliveryFrontier(ctx, reader, catalogRuntimeRunID, last.eventID); err == nil || frontier != (storetest.CausalDeliveryFrontier{}) {
				t.Fatalf("closed frontier=%+v err=%v", frontier, err)
			}
			if counts, err := storetest.ReadScatterGatherPhysicalCounts(ctx, reader, catalogRuntimeRunID); err == nil || counts != (storetest.ScatterGatherPhysicalCounts{}) {
				t.Fatalf("closed conservation=%+v err=%v", counts, err)
			}
		})
	}
}

func requireScatterGatherConservationRead(t *testing.T, ctx context.Context, reader catalogOperatorEventLister, probe *storetest.TransactionCollector) {
	t.Helper()
	before := probe.Snapshot()
	counts, err := storetest.ReadScatterGatherPhysicalCounts(ctx, reader, catalogRuntimeRunID)
	if err != nil || counts.Entities != 3 || counts.Timers < 2 || counts.FanOutIntents != 1 || counts.DomainEvents != 3 {
		t.Fatalf("real one-item conservation=%+v err=%v", counts, err)
	}
	if proof := probe.Snapshot(); proof.Total.ReadCommits-before.Total.ReadCommits != 1 || proof.Total.WriteCommits != before.Total.WriteCommits || proof.Active != 0 {
		t.Fatalf("conservation did not use one exact native snapshot: %+v", proof)
	}
	if counts, err := storetest.ReadScatterGatherPhysicalCounts(ctx, reader, uuid.NewString()); err != nil || counts != (storetest.ScatterGatherPhysicalCounts{}) {
		t.Fatalf("foreign-run conservation=%+v err=%v", counts, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if counts, err := storetest.ReadScatterGatherPhysicalCounts(canceled, reader, catalogRuntimeRunID); !errors.Is(err, context.Canceled) || counts != (storetest.ScatterGatherPhysicalCounts{}) {
		t.Fatalf("canceled conservation=%+v err=%v", counts, err)
	}
	if _, err := storetest.ReadScatterGatherPhysicalCounts(ctx, struct{}{}, catalogRuntimeRunID); err == nil {
		t.Fatal("foreign conservation owner accepted")
	}
}

func requireScatterGatherNativeFrontier(t *testing.T, ctx context.Context, reader catalogOperatorEventLister, probe *storetest.TransactionCollector, eventID string) {
	t.Helper()
	before := probe.Snapshot()
	frontier, err := storetest.ReadCausalDeliveryFrontier(ctx, reader, catalogRuntimeRunID, eventID)
	want := storetest.CausalDeliveryFrontier{Observed: 2}
	if err != nil || frontier != want {
		t.Fatalf("real issuance frontier=%+v want=%+v err=%v", frontier, want, err)
	}
	if count, err := storetest.CountClosedSourceFanOutIssuance(ctx, reader, catalogRuntimeRunID, eventID, 1); err != nil || count != 1 {
		t.Fatalf("exact closed issuance=%d err=%v", count, err)
	}
	if proof := probe.Snapshot(); proof.Total.ReadCommits-before.Total.ReadCommits != 2 || proof.Total.WriteCommits != before.Total.WriteCommits || proof.Active != 0 {
		t.Fatalf("wait observations escaped original coordinator: %+v", proof)
	}
	if count, err := storetest.CountClosedSourceFanOutIssuance(ctx, reader, catalogRuntimeRunID, eventID, 2); err != nil || count != 0 {
		t.Fatalf("foreign cardinality issuance=%d err=%v", count, err)
	}
	if frontier, err := storetest.ReadCausalDeliveryFrontier(ctx, reader, uuid.NewString(), eventID); err != nil || frontier != (storetest.CausalDeliveryFrontier{}) {
		t.Fatalf("foreign run frontier=%+v err=%v", frontier, err)
	}
	if frontier, err := storetest.ProbeCausalDeliveryFrontier(ctx, reader, 0); err == nil || frontier != (storetest.CausalDeliveryFrontier{}) {
		t.Fatalf("unsupported probe frontier=%+v err=%v", frontier, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if frontier, err := storetest.ReadCausalDeliveryFrontier(canceled, reader, catalogRuntimeRunID, eventID); !errors.Is(err, context.Canceled) || frontier != (storetest.CausalDeliveryFrontier{}) {
		t.Fatalf("canceled frontier=%+v err=%v", frontier, err)
	}
	if count, err := storetest.CountClosedSourceFanOutIssuance(canceled, reader, catalogRuntimeRunID, eventID, 1); !errors.Is(err, context.Canceled) || count != 0 {
		t.Fatalf("canceled issuance=%d err=%v", count, err)
	}
	if _, err := storetest.ReadCausalDeliveryFrontier(ctx, struct{}{}, catalogRuntimeRunID, eventID); err == nil {
		t.Fatal("foreign frontier owner accepted")
	}
	if _, err := storetest.CountClosedSourceFanOutIssuance(ctx, struct{}{}, catalogRuntimeRunID, eventID, 1); err == nil {
		t.Fatal("foreign issuance owner accepted")
	}
}

func requireFanOutDiagnosticRefusals(t *testing.T, ctx context.Context, reader catalogOperatorEventLister, eventID string) {
	t.Helper()
	if rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, reader, uuid.NewString(), eventID); err != nil || rows != nil {
		t.Fatalf("foreign-run diagnostic=%v err=%v", rows, err)
	}
	if facts, err := storetest.ReadSourceRouteSettlementStorage(ctx, reader, catalogRuntimeRunID, uuid.NewString()); err != nil || facts != (storetest.SourceRouteSettlementStorage{}) {
		t.Fatalf("missing-source witness=%+v err=%v", facts, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(canceled, reader, catalogRuntimeRunID, eventID); !errors.Is(err, context.Canceled) || rows != nil {
		t.Fatalf("canceled fan-out diagnostic=%v err=%v", rows, err)
	}
	if facts, err := storetest.ReadSourceRouteSettlementStorage(canceled, reader, catalogRuntimeRunID, eventID); !errors.Is(err, context.Canceled) || facts != (storetest.SourceRouteSettlementStorage{}) {
		t.Fatalf("canceled settlement witness=%+v err=%v", facts, err)
	}
	if _, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, struct{}{}, catalogRuntimeRunID, eventID); err == nil {
		t.Fatal("foreign fan-out owner accepted")
	}
	if _, err := storetest.ReadSourceRouteSettlementStorage(ctx, struct{}{}, catalogRuntimeRunID, eventID); err == nil {
		t.Fatal("foreign event owner accepted")
	}
}
