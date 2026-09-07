package serveapp

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionEntitylessExternalBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyReceiverEntitylessExternal(t))
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "external-onward"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, published.RunID)
			var eventID, rawSource string
			if err := rt.DB.QueryRow(`SELECT event_id,CAST(source_route AS TEXT) FROM events WHERE run_id=$1 AND event_name='sink/child.finished'`, published.RunID).Scan(&eventID, &rawSource); err != nil {
				t.Fatal(err)
			}
			var source events.RouteIdentity
			if err := json.Unmarshal([]byte(rawSource), &source); err != nil {
				t.Fatal(err)
			}
			if source.FlowInstance != "sink" || source.EntityID != "" {
				t.Fatalf("external emission borrowed source state: %s", rawSource)
			}
			var event operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, rt.Endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
			if len(event.Deliveries) != 0 || event.NoDelivery == nil || event.NoDelivery.Reason == "" || len(event.DeadLetters) != 0 {
				t.Fatalf("external boundary lacks exact no-delivery disposition: %#v", event)
			}
			var entities int
			if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, published.RunID).Scan(&entities); err != nil {
				t.Fatal(err)
			}
			if entities != 0 {
				t.Fatal("external output fabricated receiver state")
			}
			requireReceiverPublicReadback(t, rt, published.RunID)
		})
	}
}
