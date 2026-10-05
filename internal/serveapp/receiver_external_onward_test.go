package serveapp

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionEntitylessRootExportBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyReceiverEntitylessRootExport(t))
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "external-onward"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, published.RunID)
			var eventID, rawSource string
			if err := rt.DB.QueryRow(`SELECT event_id,CAST(source_route AS TEXT) FROM events WHERE run_id=$1 AND event_name='child.finished'`, published.RunID).Scan(&eventID, &rawSource); err != nil {
				t.Fatal(err)
			}
			var source events.RouteIdentity
			if err := json.Unmarshal([]byte(rawSource), &source); err != nil {
				t.Fatal(err)
			}
			if source != (events.RouteIdentity{EntityID: published.RunID, FlowInstance: published.RunID, FlowID: "."}) {
				t.Fatalf("root export lost its exact constructed source: %s", rawSource)
			}
			var event operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
			if len(event.Deliveries) != 0 || event.NoDelivery == nil || event.NoDelivery.Reason == "" || len(event.DeadLetters) != 0 {
				t.Fatalf("root export boundary lacks exact no-delivery disposition: %#v", event)
			}
			var entities int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1`, published.RunID).Scan(&entities); err != nil {
				t.Fatal(err)
			}
			if entities != 0 {
				t.Fatal("root export fabricated receiver state")
			}
			requireReceiverConstructedInstance(t, rt, published.RunID, published.RunID, ".", published.RunID, "", "pending", "", "ready", 2, nil)
			requireReceiverPublicReadback(t, rt, published.RunID)
		})
	}
}
