package serveapp

import (
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestTransitionReceiverOrdinaryCrossFlowMaterialization(t *testing.T) {
	proveReceiverMaterializationBothStores(t, canonicalrouting.CopyReceiverCreatingChild)
}

func TestReceiverCompositionAutoMaterializationBothStores(t *testing.T) {
	proveReceiverMaterializationBothStores(t, canonicalrouting.CopyReceiverAutoMaterializingChild)
}

func proveReceiverMaterializationBothStores(t *testing.T, source func(testing.TB, bool) string) {
	t.Helper()
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/existing_parent_%t", backend, existing), func(t *testing.T) {
				root := source(t, existing)
				rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
				params := map[string]any{
					"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "ordinary-cross-flow",
				}
				if existing {
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.seeded", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "create-seed"})
					requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "active")
					params = map[string]any{"event_name": "work.requested", "run_id": seed.RunID, "source_event_id": seed.EventID, "payload": map[string]any{"seed": true}, "idempotency_key": "ordinary-cross-flow"}
				}
				started := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, started.RunID)
				var status string
				if err := rt.DB.QueryRow(`SELECT status FROM event_deliveries WHERE event_id = $1`, started.EventID).Scan(&status); err != nil {
					t.Fatal(err)
				}
				if status != "delivered" {
					t.Fatalf("ordinary producer delivery should execute and settle: %s", status)
				}
				var count int
				if err := rt.DB.QueryRow(`SELECT count(*) FROM entity_state WHERE run_id = $1 AND current_state = 'done'`, started.RunID).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 2 {
					t.Fatalf("ordinary producer and sink should both complete; got %d entities", count)
				}
				var parentID, childID string
				if err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance='sink'`, started.RunID).Scan(&childID); err != nil {
					t.Fatal(err)
				}
				if err := rt.DB.QueryRow(`SELECT entity_id FROM entity_state WHERE run_id=$1 AND flow_instance<>'sink'`, started.RunID).Scan(&parentID); err != nil {
					t.Fatal(err)
				}
				if childID != flowidentity.EntityID("sink") || childID == parentID {
					t.Fatalf("child borrowed source state: parent=%s child=%s", parentID, childID)
				}
				duplicate := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
				if duplicate.EventID != started.EventID {
					t.Fatal("exact duplicate created a second source event")
				}
				requireReceiverPublicReadback(t, rt, started.RunID)
			})
		}
	}
}
