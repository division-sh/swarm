package serveapp

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionEntitylessLocalOnwardBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyReceiverEntitylessLocal(t))
			published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "local-onward"})
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, published.RunID)
			for _, node := range []string{"collector", "local"} {
				var raw, status string
				if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT),status FROM event_deliveries WHERE run_id=$1 AND subscriber_id=$2`, published.RunID, identitytest.FlowNode(t, "sink", node).Key()).Scan(&raw, &status); err != nil {
					t.Fatal(err)
				}
				var owner events.DeliveryTargetOwnership
				if err := json.Unmarshal([]byte(raw), &owner); err != nil {
					t.Fatal(err)
				}
				if status != "delivered" {
					t.Fatalf("local onward receiver failed: %s %s", node, status)
				}
				if !owner.ExistingEntity() || owner.Route() != (events.RouteIdentity{EntityID: flowidentity.EntityID("sink"), FlowInstance: "sink", FlowID: "sink"}) {
					t.Fatalf("local receiver lost its existing constructed owner: %s", raw)
				}
			}
			var raw string
			if err := rt.DB.QueryRow(`SELECT CAST(source_route AS TEXT) FROM events WHERE run_id=$1 AND event_name='sink/child.finished'`, published.RunID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var source events.RouteIdentity
			if err := json.Unmarshal([]byte(raw), &source); err != nil {
				t.Fatal(err)
			}
			if source != (events.RouteIdentity{FlowID: "sink", FlowInstance: "sink", EntityID: flowidentity.EntityID("sink")}) {
				t.Fatalf("local emission borrowed source state: %s", raw)
			}
			requireReceiverConstructedInstance(t, rt, published.RunID, "sink", "sink", flowidentity.EntityID("sink"), "receipt", "done", published.RunID, "ready", 3, map[string]any{"result": "emitted"})
			requireReceiverPublicReadback(t, rt, published.RunID)
		})
	}
}
