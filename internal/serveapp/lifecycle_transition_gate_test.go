package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledGateAdvanceOnlyOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleGateAdvanceOnly(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "advance-only"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			var result map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "approved")
			requireServedEntityReadback(t, rt.Endpoint, seed.RunID, entityID, "approved")
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, seed.RunID)
			history := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(history) != 2 {
				t.Fatalf("advance-only history=%#v", history)
			}
			compiled, ok := history[1].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "gate" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" || history[1].From != "review" || history[1].To != "approved" {
				t.Fatalf("advance-only cause=%#v", compiled)
			}
			before := lifecycleStoredSnapshot(t, rt, seed.RunID)
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != before {
				t.Fatal("duplicate advance-only verdict mutated history")
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 0)
		})
	}
}
