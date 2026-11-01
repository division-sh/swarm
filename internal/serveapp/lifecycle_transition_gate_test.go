package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedCompiledGateAdvanceOnlyOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			configureStandingLifecycleCredentials(t)
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, canonicalrouting.CopyLifecycleGateAdvanceOnly(t))
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "advance-only"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, "", "review")
			initial := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID)
			if len(initial) != 1 || initial[0].To != "review" || initial[0].TriggerEventID != seed.EventID {
				t.Fatalf("gate initial entry=%#v", initial)
			}
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			var result map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &result)
			requireServedEventPublishEntityState(t, rt.DB, rt.Backend, seed.RunID, entityID, "approved")
			var diagnosis cliapp.DiagnosticRunDiagnosisResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": seed.RunID}, &diagnosis)
			if len(diagnosis.Instances) != 1 || diagnosis.Instances[0].FlowInstance != seed.RunID || diagnosis.Instances[0].Stage != "approved" {
				t.Fatalf("advance-only public header readback: %+v", diagnosis.Instances)
			}
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			history := readLifecycleTransitionHistory(t, rt.ReceiverStateReader, seed.RunID, entityID)
			if len(history) != 1 {
				t.Fatalf("advance-only history=%#v", history)
			}
			compiled, ok := history[0].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "gate" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" || history[0].From != "review" || history[0].To != "approved" {
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
