package serveapp

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionSharedChildJourney(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing_parent_%t", backend, existing), func(t *testing.T) {
				root := canonicalrouting.CopyReceiverOptionalChild(t, existing)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				params := map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "shared-request"}
				if existing {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "shared-seed"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "shared-request"}
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				rows, err := rt.DB.Query(`SELECT subscriber_id, status, CAST(failure AS TEXT), CAST(delivery_target_route AS TEXT) FROM event_deliveries WHERE run_id = $1`, started.RunID)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var sinkDelivered bool
				sinkNode := identitytest.FlowNode(t, "sink", "collector").Key()
				for rows.Next() {
					var subscriber, status, rawOwner string
					var failure *string
					if err := rows.Scan(&subscriber, &status, &failure, &rawOwner); err != nil {
						t.Fatal(err)
					}
					t.Logf("delivery subscriber=%s status=%s failure=%v", subscriber, status, failure)
					if status != "delivered" {
						t.Errorf("non-delivered row %s: %s", subscriber, status)
					}
					if subscriber == sinkNode && status == "delivered" {
						var owner events.DeliveryTargetOwnership
						if err := json.Unmarshal([]byte(rawOwner), &owner); err != nil {
							t.Fatal(err)
						}
						if !owner.EntitylessReceiver() || owner.Route().EntityID != "" || owner.Route().FlowInstance != "sink" {
							t.Fatalf("optional child borrowed source ownership: %s", rawOwner)
						}
						sinkDelivered = true
					}
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if !sinkDelivered {
					t.Fatal("shared static child has no successful execution")
				}
				rows.Close()
				var states int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, started.RunID).Scan(&states); err != nil {
					t.Fatal(err)
				}
				if states != 0 {
					t.Fatal("optional child fabricated receiving state")
				}
				requireReceiverPublicReadback(t, rt, started.RunID)
			})
		}
	}
}
