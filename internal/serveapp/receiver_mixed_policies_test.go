package serveapp

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestReceiverCompositionIndependentPoliciesBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, optionalFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional_first_%t", backend, optionalFirst), func(t *testing.T) {
				root := canonicalrouting.CopyReceiverMixedPolicies(t, optionalFirst)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "mixed-policies"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, published.RunID)
				optionalScope := "z-observer"
				if optionalFirst {
					optionalScope = "a-observer"
				}
				for _, scope := range []string{optionalScope, "sink"} {
					var raw, status string
					if err := rt.DB.QueryRow(`SELECT CAST(delivery_target_route AS TEXT),status FROM event_deliveries WHERE run_id=$1 AND subscriber_id=$2`, published.RunID, identitytest.FlowNode(t, scope, "collector").Key()).Scan(&raw, &status); err != nil {
						t.Fatal(err)
					}
					var owner events.DeliveryTargetOwnership
					if err := json.Unmarshal([]byte(raw), &owner); err != nil {
						t.Fatal(err)
					}
					if status != "delivered" || owner.Route().FlowInstance != scope {
						t.Fatalf("recipient did not execute exact receiving scope: %s %s", status, raw)
					}
					if scope == optionalScope {
						if !owner.EntitylessReceiver() || owner.Route().EntityID != "" {
							t.Fatalf("optional recipient borrowed sibling activation: %s", raw)
						}
					} else if !owner.MaterializingEntity() || owner.Route().EntityID != flowidentity.EntityID("sink") {
						t.Fatalf("materializer lost independent canonical target: %s", raw)
					}
				}
				requireReceiverPublicReadback(t, rt, published.RunID)
			})
		}
	}
}
