package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestMaterializingSenderReachesExistingRequiredReceiverBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, required := range []bool{false, true} {
			name := "entityless_control"
			if required {
				name = "existing_required"
			}
			t.Run(string(backend)+"/"+name, func(t *testing.T) {
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyMaterializingSenderExistingReceiver(t, required))
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"case_id": "exact"}, "idempotency_key": "seed"})
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
				var count int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM events e JOIN event_deliveries d ON d.event_id=e.event_id WHERE e.run_id=$1 AND e.event_name='work.ready' AND d.status='delivered'`, seed.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("receiver completions=%d: %s", count, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, seed.RunID))
				}
			})
		}
	}
}
