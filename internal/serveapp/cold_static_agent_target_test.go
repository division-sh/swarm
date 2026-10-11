package serveapp

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// #642's 9fc3b4b4a fixture is unchanged. This ordinary source prefix exercises
// real providers before any fork, through authenticated HTTP and native stores.
func TestServedColdStaticAgentConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, start := issue2564ServeHarness(t, backend, canonicalrouting.CopySelectedFutureProviderInput(t), true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("cold-provider shutdown=%d\n%s", code, process.outputString())
				}
			})
			params := map[string]any{
				"event_name": "task.assigned", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "one"}, "idempotency_key": "future-provider-source",
			}
			response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", params, 30*time.Second)
			if response.Error != nil {
				t.Fatalf("cold-provider source admission: %+v\n%s", response.Error, process.outputString())
			}
			var seed servedEventPublishRPCResult
			if err := json.Unmarshal(response.Result, &seed); err != nil {
				t.Fatal(err)
			}
			waitColdStaticProviderCompletion(t, rt, seed.RunID)
			rt.waitDeliveries(t, seed.RunID)
			issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
			paths := []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"}
			scope, err := pipeline.NewFlowInstanceLookupScope(rt.Runtime.Options.WorkflowModule.SemanticSource(), rt.Runtime.Options.SourceArtifactFact, seed.RunID, paths, nil)
			if err != nil {
				t.Fatal(err)
			}
			observations, err := rt.selected.ListFlowInstances(t.Context(), scope)
			if err != nil || len(observations) != len(paths) {
				t.Fatalf("cold construction inventory=%d: %v", len(observations), err)
			}
			instances := make(map[string]flowidentity.Instance, len(paths))
			for _, observed := range observations {
				instance := observed.Identity()
				if _, duplicate := instances[instance.TemplateID]; duplicate {
					t.Fatal("duplicate receiver declaration in cold construction")
				}
				instances[instance.TemplateID] = instance
			}
			for _, path := range paths {
				instance, found := instances[path]
				if !found {
					t.Fatalf("cold constructor omitted %s", path)
				}
				name := eventidentity.ExternalizeForFlow(instance.InstancePath, []string{"work.finish"}, "work.finish")
				outputs := rt.events(t, seed.RunID, name)
				if len(outputs) != 1 || outputs[0].ProducerType != events.EventProducerAgent || outputs[0].Source == "" ||
					len(outputs[0].Deliveries) != 1 || outputs[0].Deliveries[0].Status != "delivered" || !outputs[0].Deliveries[0].Terminal {
					t.Fatalf("provider did not execute its exact final consumer in %s: %+v\n%s", path, outputs, process.outputString())
				}
				turns, err := storetest.ReadManagedAgentTurnStorage(t.Context(), rt.persistence.deps.EventStore, seed.RunID, outputs[0].Source)
				if err != nil || len(turns) == 0 {
					t.Fatalf("cold actor has no actual provider-turn evidence: %v", err)
				}
			}
			for _, path := range []string{"templ/child", "templ/audit"} {
				name := eventidentity.ExternalizeForFlow(instances[path].InstancePath, []string{"work.ready"}, "work.ready")
				inputs := rt.events(t, seed.RunID, name)
				if len(inputs) != 1 || len(inputs[0].Deliveries) != 2 {
					t.Fatalf("local-plus-connected input cardinality in %s: %+v", path, inputs)
				}
				seen := make(map[string]bool)
				for _, delivery := range inputs[0].Deliveries {
					route := delivery.Route
					declaration := route.AgentIdentity.Route.ScopeKey
					instance, found := instances[declaration]
					if !found || declaration != path && declaration != path+"/leaf" || seen[declaration] ||
						!route.Recipient.IsAgent() || !route.Target.EntitylessReceiver() || route.Target.Route().EntityID != "" ||
						route.Target.Route().FlowInstance != instance.InstancePath || route.Initialization.Empty() ||
						route.Initialization.ConstructionTarget().Route() != (events.RouteIdentity{FlowID: declaration, FlowInstance: instance.InstancePath, EntityID: instance.EntityID}) ||
						route.Initialization.CreatingEventID() != seed.EventID || delivery.Status != "delivered" || !delivery.Terminal || delivery.ClaimVersion != 1 ||
						(declaration == path) != route.ConnectClaim.Empty() {
						t.Fatalf("actor/dependency or exact consuming relation mismatch: %+v", delivery)
					}
					seen[declaration] = true
				}
			}
			// Replaying the accepted command after receiver retirement consumes
			// durable authority; it cannot re-plan against the live actor catalog.
			before, err := json.Marshal(rt.events(t, seed.RunID, "task.assigned"))
			if err != nil {
				t.Fatal(err)
			}
			duplicate := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", params, 30*time.Second)
			if duplicate.Error != nil {
				t.Fatalf("accepted source replay after completion: %+v", duplicate.Error)
			}
			var reused servedEventPublishRPCResult
			if err := json.Unmarshal(duplicate.Result, &reused); err != nil || reused.EventID != seed.EventID || reused.RunID != seed.RunID {
				t.Fatalf("duplicate changed exact publication: seed=%+v reuse=%+v err=%v", seed, reused, err)
			}
			after, err := json.Marshal(rt.events(t, seed.RunID, "task.assigned"))
			if err != nil || string(before) != string(after) {
				t.Fatalf("duplicate changed exact source/settlement readback: %v", err)
			}
		})
	}
}

func waitColdStaticProviderCompletion(t *testing.T, rt issue2564ServedFixture, runID string) {
	t.Helper()
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		header, err := rt.selected.LoadRunHeader(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if header.Status == "completed" && header.EndedAt != nil && header.Failure == nil {
			return
		}
		summary, err := rt.selected.SummarizeRun(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if header.Status != "running" || header.Failure != nil || summary.DeadLetter != 0 || summary.Canceled != 0 {
			t.Fatalf("cold-provider completion rejected: header=%+v deliveries=%+v\n%s", header, summary, issue2564H2CompletionReceipt(t, rt, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("cold-provider run never completed: %s\n%s", runID, issue2564H2CompletionReceipt(t, rt, runID))
}
