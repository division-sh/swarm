package cataloge2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestCatalogLatestPipelineReceiptUsesOriginalReadOwnerBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, canonicalrouting.CopyReceiverOptionalChild(t, false), backend, true)
			if err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "work.requested", Payload: map[string]any{"seed": true}, excludeFromEmitted: true}, 20*time.Second, true); err != nil {
				t.Fatal(err)
			}
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			if len(h.publishedIDs) != 1 {
				t.Fatalf("receipt control requires one exact root, got %+v", h.publishedIDs)
			}
			var eventID string
			for id := range h.publishedIDs {
				eventID = id
			}
			h.db = nil
			replayed, err := h.loadCatalogReceipt(" " + eventID + " ")
			if err != nil || replayed == nil || replayed.Outcome != "success" {
				t.Fatalf("receipt replay changed: %+v %v", replayed, err)
			}
			h.assertTriggerReceipt(catalogTriggerStep{ReceiptOutcome: "success"})
			assertHandlerOutcomeForEntity(t, h, "success", "", false)
			h.shutdown()
			ctx := testAuthorActivityContext(context.Background())
			var selected any = h.pg
			closeOwner := h.pg.Close
			if h.sqlite != nil {
				selected, closeOwner = h.sqlite, h.sqlite.Close
			}
			probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
			row, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, reader, eventID)
			if err != nil || !row.Found || row.Outcome != "success" || row.SubscriberID != replayed.SubscriberID ||
				(row.SubscriberID != "pipeline" && !strings.HasPrefix(row.SubscriberID, "pipeline:")) {
				t.Fatalf("latest platform pipeline witness changed: %+v %v", row, err)
			}
			if proof := probe.Snapshot(); proof.Total.ReadCommits != 1 || proof.Total.WriteCommits != 0 || proof.Active != 0 {
				t.Fatalf("receipt read escaped original coordinator: %+v", proof)
			}
			if row, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, reader, uuid.NewString()); err != nil || row.Found {
				t.Fatalf("missing receipt received evidence: %+v %v", row, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if row, err := storetest.ReadLatestPlatformPipelineReceiptStorage(canceled, reader, eventID); !errors.Is(err, context.Canceled) || row.Found {
				t.Fatalf("canceled receipt received evidence: %+v %v", row, err)
			}
			if err := closeOwner(); err != nil {
				t.Fatal(err)
			}
			if row, err := storetest.ReadLatestPlatformPipelineReceiptStorage(ctx, reader, eventID); err == nil || row.Found {
				t.Fatalf("closed receipt received evidence: %+v %v", row, err)
			}
		})
	}
	if row, err := storetest.ReadLatestPlatformPipelineReceiptStorage(context.Background(), struct{}{}, uuid.NewString()); err == nil || row.Found {
		t.Fatalf("foreign receipt owner received evidence: %+v %v", row, err)
	}
}
