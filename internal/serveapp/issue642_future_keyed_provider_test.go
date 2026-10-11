package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// This is the independent keyed-instance provider family, not a substitute for
// the static-descendant source reproducer retained under issue2413. H uses the
// real provider/gateway and native owners with authored mocks and host workspace
// proof composition; it is not registry-v5 or live-network acceptance.
func TestIssue642FutureKeyedProviderStartsAfterEmptyCatalogBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			observed := issue642ObserveKeyedProviderPreparation(t)
			_, start := issue2564ServeHarness(t, backend, canonicalrouting.CopySelectedFutureKeyedProviderInput(t), true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("keyed-provider native shutdown=%d\n%s", code, process.outputString())
				}
			})
			response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", map[string]any{
				"event_name": "start.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"work_id": "one"},
				"idempotency_key": "future-keyed-provider-source",
			}, 30*time.Second)
			if response.Error != nil {
				t.Fatalf("keyed-provider creating input: %+v", response.Error)
			}
			var seed servedEventPublishRPCResult
			if err := json.Unmarshal(response.Result, &seed); err != nil {
				t.Fatal(err)
			}
			issue642WaitKeyedProviderCompletion(t, rt, seed.RunID)
			rt.waitDeliveries(t, seed.RunID)
			issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
			issue642RequireKeyedProviderResult(t, rt, seed.RunID)
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{
				"source_run_id": seed.RunID, "at_start": true, "allow_source_freeze": true,
				"idempotency_key": "future-keyed-provider-start-cut",
			})
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID ||
				fork.ForkPointKind != string(runfork.RunForkPointRunStart) || fork.ForkEventID != "" {
				t.Fatalf("keyed-provider fork lost its exclusive start cut: %+v", fork)
			}
			select {
			case proof := <-observed:
				if proof.MaterializationRequired || !proof.MaterializationSupported || len(proof.ConfiguredAgentPlans) != 0 || len(proof.AgentRecipientPlans) != 0 {
					t.Fatalf("start-cut preparation invented an already-constructed keyed actor: %+v", proof)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("real selected activation did not return its preparation census")
			}
			issue642WaitKeyedProviderCompletion(t, rt, fork.ForkRunID)
			rt.waitDeliveries(t, fork.ForkRunID)
			issue642RequireKeyedProviderResult(t, rt, fork.ForkRunID)
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("keyed-provider child mutated the source domain: %v", err)
			}
		})
	}
}

func issue642ObserveKeyedProviderPreparation(t *testing.T) <-chan runforkexecution.SelectedContractAgentRuntimeMaterialization {
	t.Helper()
	observed := make(chan runforkexecution.SelectedContractAgentRuntimeMaterialization, 1)
	previous := buildSelectedAPICapabilities
	buildSelectedAPICapabilities = func(owner *selectedStoreOwner, request selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		caps, err := previous(owner, request)
		if err != nil {
			return caps, err
		}
		executor, exact := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
		if !exact {
			return caps, fmt.Errorf("keyed-provider proof requires the real selected executor, got %T", caps.RunFork)
		}
		execute := executor.ExecuteSelectedContractRunFork
		executor.ExecuteSelectedContractRunFork = func(ctx context.Context, request runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
			result, err := execute(ctx, request)
			if err == nil && result.AgentRuntimeMaterialization != nil {
				observed <- *result.AgentRuntimeMaterialization
			}
			return result, err
		}
		caps.RunFork = executor
		return caps, nil
	}
	t.Cleanup(func() { buildSelectedAPICapabilities = previous })
	return observed
}

func issue642WaitKeyedProviderCompletion(t *testing.T, rt issue2564ServedFixture, runID string) {
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
			t.Fatalf("keyed-provider run cannot complete: header=%+v deliveries=%+v\n%s", header, summary, issue2564H2CompletionReceipt(t, rt, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("keyed-provider run never completed: %s\n%s", runID, issue2564H2CompletionReceipt(t, rt, runID))
}

func issue642RequireKeyedProviderResult(t *testing.T, rt issue2564ServedFixture, runID string) {
	t.Helper()
	assigned := rt.events(t, runID, "work.assigned")
	if len(assigned) != 1 || len(assigned[0].Deliveries) != 1 || assigned[0].Deliveries[0].Target.FlowID != "work" || assigned[0].Deliveries[0].Target.EntityID == "" {
		t.Fatalf("creating publication did not retain its exact keyed target: %+v", assigned)
	}
	target := assigned[0].Deliveries[0].Target
	finished := rt.events(t, runID, target.FlowInstance+"/work.finished")
	if len(finished) != 1 || finished[0].ProducerType != events.EventProducerAgent ||
		finished[0].RunID != runID || finished[0].Payload["work_id"] != "one" || finished[0].Payload["result"] != "finished" ||
		len(finished[0].Deliveries) != 2 || len(finished[0].DeadLetters) != 0 {
		t.Fatalf("keyed provider did not publish once to both final consumers: assigned=%+v finished=%+v", assigned, finished)
	}
	for _, delivery := range finished[0].Deliveries {
		if !delivery.Terminal || delivery.Status != "delivered" || delivery.Failure != nil {
			t.Fatalf("keyed-provider final consumer did not settle successfully: %+v", delivery)
		}
	}
	worker, err := rt.selected.LoadOperatorEntity(t.Context(), target.EntityID, runID)
	if err != nil || worker.Entity.CurrentState != "done" || worker.Entity.FlowInstance != target.FlowInstance ||
		worker.Fields["work_id"] != "one" || worker.Fields["result"] != "finished" {
		t.Fatalf("keyed provider did not keep its exact entity result: %+v err=%v", worker, err)
	}
	root, err := rt.selected.LoadOperatorEntity(t.Context(), runID, runID)
	if err != nil || root.Entity.CurrentState != "done" || root.Fields["counter"] != float64(1) {
		t.Fatalf("root did not consume the keyed result exactly once: %+v err=%v", root, err)
	}
	reader, ok := rt.persistence.deps.EventStore.(operatorread.AgentReader)
	if !ok {
		t.Fatalf("keyed provider has no native agent read owner: %T", rt.persistence.deps.EventStore)
	}
	identity, err := reader.ResolveOperatorAgentIdentity(t.Context(), runID, finished[0].Source, target.FlowInstance)
	if err != nil || identity.RunID != runID || identity.FlowInstance() != target.FlowInstance || identity != assigned[0].Deliveries[0].Route.AgentIdentity {
		t.Fatalf("provider output lacks exact run-bound keyed identity: %+v err=%v", identity, err)
	}
	turns, err := storetest.ReadManagedAgentTurnStorage(t.Context(), rt.persistence.deps.EventStore, runID, finished[0].Source)
	if err != nil || len(turns) != 1 || turns[0].TriggerEventID != assigned[0].EventID || len(turns[0].RequestPayload) == 0 || len(turns[0].ResponsePayload) == 0 {
		t.Fatalf("keyed provider lacks its one real request/response turn: %+v err=%v", turns, err)
	}
}
