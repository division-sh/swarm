package serveapp

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionEntitylessOnwardBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing_parent_%t", backend, existing), func(t *testing.T) {
				root := canonicalrouting.CopyReceiverEntitylessOnward(t, existing)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "onward-request"}
				if existing {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "onward-seed"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "onward-request"}
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				rows, err := rt.DB.Query(`SELECT CAST(delivery_target_route AS TEXT),status FROM event_deliveries WHERE run_id=$1`, started.RunID)
				if err != nil {
					t.Fatal(err)
				}
				var child, grandchild bool
				for rows.Next() {
					var raw, status string
					if err := rows.Scan(&raw, &status); err != nil {
						t.Fatal(err)
					}
					var owner events.DeliveryTargetOwnership
					if err := json.Unmarshal([]byte(raw), &owner); err != nil {
						t.Fatal(err)
					}
					if status != "delivered" {
						t.Errorf("onward route not delivered: %s %s", raw, status)
					}
					switch owner.Route().FlowInstance {
					case "sink":
						child = true
						if !owner.EntitylessReceiver() || owner.Route().EntityID != "" {
							t.Errorf("child inherited an entity: %s", raw)
						}
					case "sink/tail":
						grandchild = true
						if !owner.MaterializingEntity() || owner.Route().EntityID != flowidentity.EntityID("sink/tail") {
							t.Errorf("grandchild lacks canonical materialization: %s", raw)
						}
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				rows.Close()
				if !child || !grandchild {
					t.Fatalf("missing nested execution child=%t grandchild=%t", child, grandchild)
				}
				var childStates int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, started.RunID).Scan(&childStates); err != nil {
					t.Fatal(err)
				}
				if childStates != 0 {
					t.Fatal("entityless child fabricated state")
				}
				var rawFields string
				if err := rt.DB.QueryRow(`SELECT CAST(fields AS TEXT) FROM entity_state WHERE run_id=$1 AND flow_instance='sink/tail' AND current_state='done'`, started.RunID).Scan(&rawFields); err != nil {
					t.Fatal(err)
				}
				var fields map[string]any
				if err := json.Unmarshal([]byte(rawFields), &fields); err != nil {
					t.Fatal(err)
				}
				if fields["result"] != "emitted" {
					t.Fatalf("grandchild did not consume the emitted payload: %#v", fields)
				}
				requireReceiverPublicReadback(t, rt, started.RunID)
				var readback operatorread.OperatorEventListResult
				requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": started.RunID}, "limit": 100}, &readback)
				var onward int
				for _, event := range readback.Events {
					if event.EventName != "sink/child.finished" {
						continue
					}
					onward++
					// The public singular entity projection is the admitted target,
					// not the causal source. Check both rather than conflating them.
					if event.EntityID != flowidentity.EntityID("sink/tail") {
						t.Fatalf("public target projection changed: %s", event.EntityID)
					}
					var rawSource string
					if err := rt.DB.QueryRow(`SELECT CAST(source_route AS TEXT) FROM events WHERE event_id=$1`, event.EventID).Scan(&rawSource); err != nil {
						t.Fatal(err)
					}
					var source events.RouteIdentity
					if err := json.Unmarshal([]byte(rawSource), &source); err != nil {
						t.Fatal(err)
					}
					if source.FlowID != "sink" || source.FlowInstance != "sink" || source.EntityID != "" {
						t.Fatalf("entityless emitter borrowed ancestor identity: %s", rawSource)
					}
				}
				if onward != 1 {
					t.Fatalf("expected exactly one entityless onward event, found %d", onward)
				}
			})
		}
	}
}
