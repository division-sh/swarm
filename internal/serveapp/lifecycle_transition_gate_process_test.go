package serveapp

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// This is a real process kill after the gate transaction and before its final
// consumer, not a graceful stop blocked by a test-owned handler hook.
func TestServedCompiledGateOutcomeRestartOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateSharedEvent)
			start, ready, _ := mailboxCompletionProcessHarness(t, backend, root)
			first, rt := start(true)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "gate-cut-seed"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, "", "review")
			params := lifecycleGateDecisionParams(t, rt, seed.RunID, "approve")
			response := make(chan gateCompletionHTTPResult, 1)
			go func() { response <- gateCompletionHTTP(context.Background(), rt.Endpoint, params) }()
			reached := make(chan error, 1)
			go func() { var b [1]byte; _, err := io.ReadFull(ready, b[:]); reached <- err }()
			select {
			case err := <-reached:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("gate commit barrier not reached\n%s", first.output.String())
			}
			requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "approved")
			before := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(before) != 2 {
				t.Fatalf("gate did not commit before crash: %#v", before)
			}
			compiled, ok := before[1].Evidence.Compiled()
			if !ok || compiled.FlowID() != "." || compiled.Edge().Source != "gate" || compiled.Edge().Verdict != "approve" {
				t.Fatalf("gate selected cause before crash=%#v", compiled)
			}
			var outcomeID string
			if err := rt.DB.QueryRow(`SELECT event_id FROM events WHERE run_id=$1 AND event_name='work.completed'`, seed.RunID).Scan(&outcomeID); err != nil {
				t.Fatal(err)
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			if first.waitError() == nil {
				t.Fatal("killed process reported graceful exit")
			}
			select {
			case lost := <-response:
				if lost.Err == nil {
					t.Fatalf("expected transport loss after kill, got %+v", lost)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("killed process retained HTTP request")
			}
			second, rt := start(false)
			requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
			after := readLifecycleTransitionHistory(t, rt, seed.RunID, entityID)
			if len(after) != 3 || !reflect.DeepEqual(after[:2], before) || after[2].TriggerEventID != outcomeID {
				t.Fatalf("restart changed gate cause or replayed route: before=%#v after=%#v", before, after)
			}
			var entity operatorread.OperatorEntityFull
			requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": seed.RunID, "entity_id": entityID}, &entity)
			if entity.Fields["result"] != "approved" {
				t.Fatalf("recovered gate consumer=%#v", entity)
			}
			settled := lifecycleStoredSnapshot(t, rt, seed.RunID)
			var decision map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", params, &decision)
			if lifecycleStoredSnapshot(t, rt, seed.RunID) != settled {
				t.Fatal("recovered gate duplicate mutated history")
			}
			requireLifecycleEventCount(t, rt, seed.RunID, "work.completed", 1)
			if err := second.stop(); err != nil {
				t.Fatalf("recovered stop: %v\n%s", err, second.output.String())
			}
		})
	}
}
