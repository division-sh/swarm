//go:build issue2413

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

// Real selected native owners and the default provider/gateway execute here.
// Mock-only H composition is explicit: this is not live-network acceptance or
// the user-owned registry-v5 fixture, and it does not yet prove process recovery.
// The new cold-source reproducer is retained under R7 child 2 per #642 ruling
// 6107827864. It does not replace or skip an existing required proof root.
func TestIssue642FutureProviderConstructsAfterEmptyCatalogBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			observed := make(chan runforkexecution.SelectedContractAgentRuntimeMaterialization, 1)
			previous := buildSelectedAPICapabilities
			buildSelectedAPICapabilities = func(owner *selectedStoreOwner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
				caps, err := previous(owner, req)
				if err != nil {
					return caps, err
				}
				executor, exact := caps.RunFork.(apiv1.SelectedContractRunForkExecutor)
				if !exact {
					return caps, fmt.Errorf("future-provider proof requires the real selected executor, got %T", caps.RunFork)
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
			_, start := issue2564ServeHarness(t, backend, canonicalrouting.CopySelectedFutureProviderInput(t), true)
			process, rt := start()
			t.Cleanup(func() {
				if code := process.stop(); code != 0 {
					t.Errorf("future-provider native shutdown=%d\n%s", code, process.outputString())
				}
			})
			// Cold native provider construction includes the real workspace probe.
			// Use the existing selected-provider operation budget, not the generic
			// five-second read/control HTTP helper; no timing acceptance is claimed.
			response := requestServedJSONRPCWithTimeout(t, rt.Endpoint, "event.publish", map[string]any{"event_name": "task.assigned", "bundle_hash": rt.BundleHash,
				"payload": map[string]any{"work_id": "one"}, "idempotency_key": "future-provider-source"}, 30*time.Second)
			if response.Error != nil {
				t.Fatalf("future-provider source admission: %+v", response.Error)
			}
			var seed servedEventPublishRPCResult
			if err := json.Unmarshal(response.Result, &seed); err != nil {
				t.Fatal(err)
			}
			issue642WaitFutureProviderCompletion(t, rt, seed.RunID)
			rt.waitDeliveries(t, seed.RunID)
			issue2564H2CompletionWaitReceipt(t, rt, seed.RunID, seed.EventID)
			sourceBefore, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil {
				t.Fatal(err)
			}
			fork := requireSelectedForkExecutionRPCResult(t, rt.Endpoint, map[string]any{"source_run_id": seed.RunID, "at_start": true,
				"allow_source_freeze": true, "idempotency_key": "future-provider-start-cut"})
			if fork.SourceRunID != seed.RunID || fork.ForkRunID == "" || fork.ForkRunID == seed.RunID || fork.ForkPointKind != string(runfork.RunForkPointRunStart) || fork.ForkEventID != "" {
				t.Fatalf("future-provider fork did not acknowledge the exclusive start cut: %+v", fork)
			}
			select {
			case proof := <-observed:
				if proof.MaterializationRequired || !proof.MaterializationSupported || len(proof.ConfiguredAgentPlans) != 0 || len(proof.AgentRecipientPlans) != 0 {
					t.Fatalf("run-start preparation invented an initial future actor: %+v", proof)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("real selected activation did not return its preparation census")
			}
			issue642WaitFutureProviderCompletion(t, rt, fork.ForkRunID)
			rt.waitDeliveries(t, fork.ForkRunID)
			for _, path := range []string{"templ/child", "templ/child/leaf", "templ/audit", "templ/audit/leaf"} {
				outputs := rt.events(t, fork.ForkRunID, path+"/work.finish")
				if len(outputs) != 1 || outputs[0].ProducerType != events.EventProducerAgent || outputs[0].Source == "" ||
					len(outputs[0].Deliveries) != 1 || outputs[0].Deliveries[0].Status != "delivered" || !outputs[0].Deliveries[0].Terminal {
					t.Fatalf("future provider did not reach its exact real final consumer in %s: %+v", path, outputs)
				}
				turns, err := storetest.ReadManagedAgentTurnStorage(t.Context(), rt.persistence.deps.EventStore, fork.ForkRunID, outputs[0].Source)
				if err != nil || len(turns) == 0 {
					t.Fatalf("future actor has no actual provider-turn evidence: %v", err)
				}
			}
			sourceAfter, err := storetest.ReadSelectedForkSourceDomain(t.Context(), rt.selected, seed.RunID)
			if err != nil || !reflect.DeepEqual(sourceBefore, sourceAfter) {
				t.Fatalf("future-provider child mutated source state: %v", err)
			}
		})
	}
}

func issue642WaitFutureProviderCompletion(t *testing.T, rt issue2564ServedFixture, runID string) operatorread.RunHeader {
	t.Helper()
	for deadline := time.Now().Add(servedProofPollDeadline); time.Now().Before(deadline); {
		header, err := rt.selected.LoadRunHeader(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if header.Status == "completed" && header.EndedAt != nil && header.Failure == nil {
			return header
		}
		summary, err := rt.selected.SummarizeRun(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if header.Status != "running" || header.Failure != nil || summary.DeadLetter != 0 || summary.Canceled != 0 {
			t.Fatalf("future-provider run cannot satisfy completion: header=%+v deliveries=%+v\n%s", header, summary, issue2564H2CompletionReceipt(t, rt, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("future-provider run never completed: %s\n%s", runID, issue2564H2CompletionReceipt(t, rt, runID))
	return operatorread.RunHeader{}
}
