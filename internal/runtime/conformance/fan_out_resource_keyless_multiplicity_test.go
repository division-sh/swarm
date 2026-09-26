package conformance

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

			var pin string
			if err := f.db.QueryRowContext(f.ctx, `SELECT version_id FROM resource_version_pins WHERE run_id=$1 AND flow_path='.' AND event_name='root.ready'`, runID).Scan(&pin); err != nil {
				t.Fatal(err)
			}
			if pin != string(version.VersionID) {
				t.Fatalf("keyless run pin=%s want=%s", pin, version.VersionID)
			}
			var storedKeyField string
			var storedRowCount int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COALESCE(business_key_field,''),row_count FROM resource_versions WHERE version_id=$1`, pin).Scan(&storedKeyField, &storedRowCount); err != nil {
				t.Fatal(err)
			}
			if storedKeyField != "" || storedRowCount != 2 {
				t.Fatalf("stored keyless version has key=%q row_count=%d", storedKeyField, storedRowCount)
			}
			var cursor, cardinality int
			if err := f.db.QueryRowContext(f.ctx, `SELECT cursor,cardinality FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, runID).Scan(&cursor, &cardinality); err != nil {
				t.Fatal(err)
			}
			if cursor != 2 || cardinality != 2 {
				t.Fatalf("keyless feed cursor=%d cardinality=%d, want two", cursor, cardinality)
			}
			outcomes, err := f.db.QueryContext(f.ctx, `SELECT ordinal,CAST(event_id AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='committed' ORDER BY ordinal`, runID)
			if err != nil {
				t.Fatal(err)
			}
			defer outcomes.Close()
			eventIDs := make(map[string]struct{}, 2)
			for ordinal := 0; outcomes.Next(); ordinal++ {
				var gotOrdinal int
				var eventID string
				if err := outcomes.Scan(&gotOrdinal, &eventID); err != nil {
					t.Fatal(err)
				}
				if gotOrdinal != ordinal || ordinal >= 2 || eventID == "" {
					t.Fatalf("keyless outcome ordinal=%d event=%q at index=%d", gotOrdinal, eventID, ordinal)
				}
				eventIDs[eventID] = struct{}{}
			}
			if err := outcomes.Err(); err != nil {
				t.Fatal(err)
			}
			if len(eventIDs) != 2 {
				t.Fatalf("equal keyless rows collapsed event IDs: %+v", eventIDs)
			}
			var outcomeCount, eventCount, deliveryCount, settledCount, handoffCount, receiptCount int
			for _, check := range []struct {
				query string
				into  *int
			}{
				{`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1`, &outcomeCount},
				{`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='root.ready'`, &eventCount},
				{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, &deliveryCount},
				{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered'`, &settledCount},
				{`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND continuation_handoff_at IS NOT NULL`, &handoffCount},
				{`SELECT COUNT(*) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND r.subscriber_type='platform' AND r.subscriber_id='pipeline' AND r.outcome='success'`, &receiptCount},
			} {
				if err := f.db.QueryRowContext(f.ctx, check.query, runID).Scan(check.into); err != nil {
					t.Fatal(err)
				}
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
		})
	}
}
