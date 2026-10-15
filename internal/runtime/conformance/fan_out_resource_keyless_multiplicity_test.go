package conformance

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Equal keyless rows are distinct ordinal obligations, not one deduplicated
// event or receiver delivery.
func TestDeploymentSourceKeylessEqualRowsPreserveMultiplicityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedDeploymentResourceFixtureKeyless(t, backend, "root")
			server := f.operatorServer(t)
			row := []byte(`{"account_id":"same","document":{"nested":[1,{"value":2}]}}`)
			rows := append(append(append([]byte(nil), row...), '\n'), row...)
			rows = append(rows, '\n')
			ref, err := durabledata.ParseDeclarationRef(".", selectedDeploymentEvent)
			if err != nil {
				t.Fatal(err)
			}
			bundle, ok := semanticview.Bundle(f.source)
			if !ok {
				t.Fatal("keyless fixture is not bundle-backed")
			}
			declaration, ok := bundle.DurableDataDeclarationByRef(ref)
			if !ok || declaration.BusinessKey != "" {
				t.Fatalf("keyless declaration = %+v, found=%t", declaration, ok)
			}
			version, defects := durabledata.CompileJSONL(ref, declaration.Schema, "", rows)
			if len(defects) != 0 || len(version.Rows) != 2 {
				t.Fatalf("keyless duplicate rows compiled as %d rows, defects=%+v", len(version.Rows), defects)
			}
			if version.Rows[0].Ordinal != 1 || version.Rows[1].Ordinal != 2 || version.Rows[0].BusinessKey != "" || version.Rows[1].BusinessKey != "" || !bytes.Equal(version.Rows[0].Canonical, version.Rows[1].Canonical) {
				t.Fatalf("equal keyless rows lost ordinal-only identity: %+v", version.Rows)
			}
			path := filepath.Join(t.TempDir(), "equal-keyless.jsonl")
			if err := os.WriteFile(path, rows, 0o600); err != nil {
				t.Fatal(err)
			}
			runID := startDeploymentResourceRun(t, f, server, "--data", selectedDeploymentEvent+"="+path)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)

			pin, err := storetest.ReadRootReadyResourcePinStorage(f.ctx, f.selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			if pin != string(version.VersionID) {
				t.Fatalf("keyless run pin=%s want=%s", pin, version.VersionID)
			}
			stored, err := storetest.ReadResourceVersionMetadataStorage(f.ctx, f.selected, pin)
			if err != nil {
				t.Fatal(err)
			}
			storedKeyField, storedRowCount := stored.BusinessKey, stored.RowCount
			if storedKeyField != "" || storedRowCount != 2 {
				t.Fatalf("stored keyless version has key=%q row_count=%d", storedKeyField, storedRowCount)
			}
			progress, err := storetest.ReadDeploymentIntentCardinalityStorage(f.ctx, f.selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			cursor, cardinality := progress.Cursor, progress.Cardinality
			if cursor != 2 || cardinality != 2 {
				t.Fatalf("keyless feed cursor=%d cardinality=%d, want two", cursor, cardinality)
			}
			outcomes, err := storetest.ReadDeploymentCommittedOrdinalsStorage(f.ctx, f.selected, runID)
			if err != nil {
				t.Fatal(err)
			}
			eventIDs := make(map[string]struct{}, 2)
			for ordinal, outcome := range outcomes {
				gotOrdinal, eventID := outcome.Ordinal, outcome.ID
				if gotOrdinal != ordinal || ordinal >= 2 || eventID == "" {
					t.Fatalf("keyless outcome ordinal=%d event=%q at index=%d", gotOrdinal, eventID, ordinal)
				}
				eventIDs[eventID] = struct{}{}
			}
			if len(eventIDs) != 2 {
				t.Fatalf("equal keyless rows collapsed event IDs: %+v", eventIDs)
			}
			var outcomeCount, eventCount, deliveryCount, settledCount, handoffCount, receiptCount int
			for _, check := range []struct {
				read func(context.Context, any, string) (int, error)
				into *int
			}{
				{storetest.CountNestedRunFanOutOutcomes, &outcomeCount},
				{func(ctx context.Context, owner any, run string) (int, error) {
					return storetest.CountRunNamedEventStorage(ctx, owner, run, "root.ready")
				}, &eventCount},
				{storetest.CountRunDeliveryStorage, &deliveryCount},
				{storetest.CountRunDeliveredRowsStorage, &settledCount},
				{storetest.CountDeploymentStampedHandoffsStorage, &handoffCount},
				{storetest.CountClosedReceiverPipelineSuccessStorage, &receiptCount},
			} {
				got, err := check.read(f.ctx, f.selected, runID)
				if err != nil {
					t.Fatal(err)
				}
				*check.into = got
			}
			if outcomeCount != 2 || eventCount != 2 || deliveryCount != 2 || settledCount != 2 || handoffCount != 2 || receiptCount != 2 {
				t.Fatalf("keyless multiplicity/settlement: outcomes=%d events=%d deliveries=%d delivered=%d handoffs=%d receipts=%d", outcomeCount, eventCount, deliveryCount, settledCount, handoffCount, receiptCount)
			}
			page := selectedDeploymentPublicEvents(t, f.ctx, server, runID)
			if len(page.Events) != 2 || page.NextCursor != "" {
				t.Fatalf("keyless public readback returned %d events, cursor=%q", len(page.Events), page.NextCursor)
			}
			publicDeliveries := make(map[string]struct{}, 2)
			publicEvents := make(map[string]struct{}, 2)
			wantSubscriber := conformanceNode(t, "", "root-collector").Key()
			want := map[string]any{"account_id": "same", "document": map[string]any{"nested": []any{float64(1), map[string]any{"value": float64(2)}}}}
			for _, event := range page.Events {
				if _, ok := eventIDs[event.EventID]; !ok || !reflect.DeepEqual(event.Payload, want) || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" || event.Deliveries[0].SubscriberID != wantSubscriber {
					t.Fatalf("keyless public event lost payload or receiver settlement: %+v", event)
				}
				publicEvents[event.EventID] = struct{}{}
				publicDeliveries[event.Deliveries[0].DeliveryID] = struct{}{}
			}
			if len(publicEvents) != 2 || len(publicDeliveries) != 2 {
				t.Fatalf("equal keyless rows collapsed public identities: events=%+v deliveries=%+v", publicEvents, publicDeliveries)
			}
			// Settlement of every row is not itself the authored end of this
			// reusable collector. Its explicit close must reach the final stage.
			runs, ok := f.selected.(operatorread.RunReader)
			if !ok {
				t.Fatal("selected store lacks the canonical run-read owner")
			}
			header, err := runs.LoadRunHeader(f.ctx, runID)
			if err != nil || header.Status != "running" {
				t.Fatalf("collector ended before close: status=%s error=%v", header.Status, err)
			}
			closed := textFileRPCParams2456(t, server, "event.publish", map[string]any{
				"run_id": runID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"event_name": "fixture.close.requested", "payload": map[string]any{"account_id": "same"}, "idempotency_key": uuid.NewString(),
			})
			if len(closed.Error) != 0 {
				t.Fatalf("public collector close: %s", closed.Error)
			}
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)
			instances, err := f.selected.ListWorkflowInstances(f.ctx, runID)
			if err != nil || len(instances) != 1 || !instances[0].StageDefined || instances[0].CurrentState != "done" {
				t.Fatalf("public close did not reach the declared final stage: instances=%+v error=%v", instances, err)
			}
		})
	}
}
