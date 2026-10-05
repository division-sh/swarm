package serveapp

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// The existing H harness boots the normal serve owner and reconstructs the same
// selected store in-process. It is not a standalone process-kill or E proof.
func TestA2MapRecipeSupportedSurfaceBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.MapScatterGather)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, canonicalrouting.MapScatterGather)
			requireA2PortfolioVerification(t, root)
			opts, start := lifecycleRestartHarness(t, backend, root)
			var selected serveRuntimePersistence
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected = p })
			process, rt := start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			type retainedBatch struct {
				opened  servedEventPublishRPCResult
				params  map[string]any
				items   map[string][]int64
				members []string
				results [][]int64
				root    operatorread.OperatorEntityFull
				arm     joinruntime.Activation
				workers []operatorread.OperatorEntityFull
				events  []operatorread.OperatorEventFull
			}
			var batches []retainedBatch
			for _, tc := range []struct {
				name    string
				items   map[string][]int64
				members []string
				results [][]int64
			}{
				{"meaningful_values", map[string][]int64{"z": {9}, "A": {1, 1}}, []string{"A", "z"}, [][]int64{{1, 1}, {9}}},
				{"zero_map", map[string][]int64{}, []string{}, [][]int64{}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					params := map[string]any{"bundle_hash": rt.BundleHash, "event_name": "batch.ready", "payload": map[string]any{"items": tc.items}, "idempotency_key": "a2-map-recipe-" + tc.name}
					opened := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
					if !opened.NewRunCreated || opened.RunID == "" || opened.EventID == "" {
						t.Fatalf("root map ingress did not create a real run: %+v", opened)
					}
					waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, opened.RunID)
					waitA2PortfolioComplete(t, rt, opened.RunID, opened.RunID)
					entity, arm := requireA2MapRecipeResult(t, rt, opened, tc.items, tc.members, tc.results)
					var payload map[string]any
					raw, err := json.Marshal(map[string]any{"items": tc.items})
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(raw, &payload); err != nil {
						t.Fatal(err)
					}
					requireA2PortfolioDelivery(t, requireA2PortfolioEvent(t, rt, opened.EventID, opened.RunID, "batch.ready", payload), ".", "scatter", entity, "materializing_entity")
					requests := requireA2PortfolioEvents(t, rt, opened.RunID, "item.requested")
					if len(requests) != len(tc.members) {
						t.Fatalf("map key fan-out count=%d want=%d", len(requests), len(tc.members))
					}
					var workers []operatorread.OperatorEntityFull
					seen := map[string]bool{}
					for index, member := range tc.members {
						var matches []operatorread.OperatorEventFull
						for _, request := range requests {
							if request.Payload["member_id"] == member {
								matches = append(matches, request)
							}
						}
						if len(matches) != 1 || len(matches[0].Deliveries) != 1 || matches[0].SourceEventID != opened.EventID {
							t.Fatalf("exact map ordinal/member %s: %+v", member, matches)
						}
						var values []any
						for _, value := range tc.items[member] {
							values = append(values, float64(value))
						}
						request := requireA2PortfolioEvent(t, rt, matches[0].EventID, opened.RunID, "item.requested", map[string]any{
							"member_id": member, "values": values, "index": float64(index), "count": float64(len(tc.members)),
						})
						worker := requireA2PortfolioEntity(t, rt, opened.RunID, request.Deliveries[0].Target.FlowInstance)
						if worker.Entity.CurrentState != "complete" || worker.Entity.EntityID == entity.Entity.EntityID || worker.Fields["member_id"] != member ||
							!reflect.DeepEqual(worker.Fields["values"], values) || seen[worker.Entity.EntityID] {
							t.Fatalf("actual keyed worker construction/state/value isolation: %+v", worker)
						}
						seen[worker.Entity.EntityID] = true
						requireA2PortfolioDelivery(t, request, "workers", "worker", worker, "materializing_entity")
						report := requireA2PortfolioEmission(t, rt, opened.RunID, worker.Entity.FlowInstance+"/item.reported", request.EventID, map[string]any{"member_id": member, "result": values})
						requireA2PortfolioDelivery(t, report, ".", "collector", entity, "existing_entity")
						workers = append(workers, worker)
					}
					var entities operatorread.OperatorEntityListResult
					requireServedJSONRPCResult(t, rt.Endpoint, "entity.list", map[string]any{"run_id": opened.RunID, "limit": 10}, &entities)
					if entities.NextCursor != "" || len(entities.Entities) != len(tc.members)+1 {
						t.Fatalf("public exact root/worker census: %+v", entities)
					}
					completions := requireA2PortfolioEvents(t, rt, opened.RunID, "platform.join_complete")
					if len(completions) != 1 {
						t.Fatalf("exact durable map continuation count=%d want=1", len(completions))
					}
					handle, ref, ok := timeridentity.ParseJoinHandle(completions[0].Payload)
					if !ok || !ref.Equal(arm.JoinRef()) || handle.Kind() != timeridentity.TimerHandleJoinComplete {
						t.Fatalf("public continuation lost exact map entry/arm: %+v", completions[0])
					}
					requireA2PortfolioDelivery(t, a2ReadJoinPublicEvent(t, rt, completions[0].EventID), ".", "collector", entity, "existing_entity")
					events := requireA2MapRecipeHealthyEvents(t, rt, opened.RunID)
					batches = append(batches, retainedBatch{opened, params, tc.items, tc.members, tc.results, entity, arm, workers, events})
					t.Logf("root HTTP batch -> post-write map entry -> %d actual keyed workers -> ordinary root reports -> one fired continuation; members=%v ordered_results=%v", len(workers), arm.Members, tc.results)
				})
			}
			predecessor := rt.Runtime.Options.RuntimeInstanceID
			if code := process.stop(); code != 0 {
				t.Fatalf("map recipe predecessor serve exit=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			process, rt = start()
			rt = a2PortfolioReadbackRuntime(t, process, rt, selected)
			if rt.Runtime.Options.RuntimeInstanceID == predecessor {
				t.Fatal("same-store reconstruction reused the predecessor runtime")
			}
			for _, batch := range batches {
				entity, arm := requireA2MapRecipeResult(t, rt, batch.opened, batch.items, batch.members, batch.results)
				if !reflect.DeepEqual(batch.root, entity) || !reflect.DeepEqual(batch.arm, arm) {
					t.Fatal("normal serve restart changed retained map/result/entry evidence")
				}
				for _, worker := range batch.workers {
					if !reflect.DeepEqual(worker, requireA2PortfolioEntity(t, rt, batch.opened.RunID, worker.Entity.FlowInstance)) {
						t.Fatal("normal serve restart changed an actually created keyed worker")
					}
				}
				if !reflect.DeepEqual(batch.events, requireA2MapRecipeHealthyEvents(t, rt, batch.opened.RunID)) {
					t.Fatal("normal serve restart duplicated or changed map publications/settlements")
				}
				replay := requireServedEventPublishRPCResult(t, rt.Endpoint, batch.params)
				if replay.EventID != batch.opened.EventID || replay.RunID != batch.opened.RunID {
					t.Fatalf("same-idempotency root replay changed identity: first=%+v replay=%+v", batch.opened, replay)
				}
				waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, batch.opened.RunID)
				entity, arm = requireA2MapRecipeResult(t, rt, batch.opened, batch.items, batch.members, batch.results)
				if !reflect.DeepEqual(batch.root, entity) || !reflect.DeepEqual(batch.arm, arm) || !reflect.DeepEqual(batch.events, requireA2MapRecipeHealthyEvents(t, rt, batch.opened.RunID)) {
					t.Fatal("same-idempotency replay changed map state, arm or event/settlement evidence")
				}
			}
			t.Log("actual same-store normal serve reconstruction and authenticated idempotent replay preserved both map batches and terminal worker entities")
			if code := process.stop(); code != 0 {
				t.Fatalf("map recipe successor serve exit=%d", code)
			}
		})
	}
}

func requireA2MapRecipeResult(t *testing.T, rt servedControlProofRuntime, opened servedEventPublishRPCResult, items map[string][]int64, members []string, results [][]int64) (operatorread.OperatorEntityFull, joinruntime.Activation) {
	t.Helper()
	var raw json.RawMessage
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": opened.RunID, "entity_id": opened.RunID}, &raw)
	var entity operatorread.OperatorEntityFull
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &entity); err != nil {
		t.Fatal(err)
	}
	var typed struct {
		Fields struct {
			Items          map[string][]int64 `json:"items"`
			OrderedResults [][]int64          `json:"ordered_results"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatalf("public map/results do not retain integer-list types: %v", err)
	}
	if entity.Entity.RunID != opened.RunID || entity.Entity.EntityID != opened.RunID || entity.Entity.FlowInstance != opened.RunID || entity.Entity.CurrentState != "complete" ||
		!reflect.DeepEqual(typed.Fields.Items, items) || !reflect.DeepEqual(typed.Fields.OrderedResults, results) || len(entity.Fields) != 2 {
		t.Fatalf("public terminal one-map/ordered-result state: entity=%+v fields=%+v want=%v", entity, typed.Fields, results)
	}
	carrier, err := engine.StateCarrierFromPersisted(entity.Fields, entity.Bookkeeping, entity.Gates, entity.Accumulated)
	if err != nil {
		t.Fatal(err)
	}
	arms, err := joinruntime.List(carrier.StateBuckets)
	if err != nil || len(arms) != 1 {
		t.Fatalf("public canonical map join catalog: arms=%+v err=%v", arms, err)
	}
	arm := arms[0]
	declaration, err := timeridentity.NewJoinRef(identitytest.RootNode(t, "collector"), "item.reported", "collecting", "collecting")
	if err != nil {
		t.Fatal(err)
	}
	ref := arm.JoinRef()
	if err := ref.StageEntry().RequireOwner(opened.RunID, ".", opened.RunID, opened.RunID, opened.RunID, "collecting"); err != nil {
		t.Fatal(err)
	}
	if !ref.Declaration().Equal(declaration) || ref.StageEntry().EventID != opened.EventID || !slices.Equal(arm.Members, members) || arm.Expected() != len(members) || arm.Completed() != len(members) ||
		arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete || arm.OutcomePending || !arm.OutcomeFired || !arm.TimerCancelled || arm.DeadlineAt.Sub(arm.ArmedAt) != 5*time.Minute {
		t.Fatalf("exact post-write map membership/entry/deadline/firing: %+v", arm)
	}
	want := make([]any, len(results))
	for i, row := range results {
		values := make([]any, len(row))
		for j, value := range row {
			values[j] = value
		}
		want[i] = values
	}
	a2RequireJoinPublicResults(t, arm, want)
	return entity, arm
}

func requireA2MapRecipeHealthyEvents(t *testing.T, rt servedControlProofRuntime, runID string) []operatorread.OperatorEventFull {
	t.Helper()
	var result operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100}, &result)
	if result.NextCursor != "" || len(result.Events) == 0 {
		t.Fatalf("bounded public map event readback: %+v", result)
	}
	var business []operatorread.OperatorEventFull
	for _, event := range result.Events {
		if event.RunID != runID || len(event.DeadLetters) != 0 {
			t.Fatalf("map event lost its run or has a deadletter: %+v", event)
		}
		// Diagnostic-direct logs have no recipient; they are not business replay evidence.
		if event.EventName == "platform.runtime_log" {
			if event.ProducerType != "platform" || event.Source != "runtime" || event.EntityID != "" || len(event.Deliveries) != 0 ||
				event.NoDelivery == nil || event.NoDelivery.Reason != events.NoDeliveryNoSubscriberByDesign.Code() {
				t.Fatalf("runtime diagnostic lost its explicit no-subscriber disposition: %+v", event)
			}
			continue
		}
		if event.NoDelivery != nil || len(event.Deliveries) != 1 {
			t.Fatalf("map route/settlement has a deadletter, no-route or unexpected recipient: %+v", event)
		}
		delivery := event.Deliveries[0]
		if delivery.Status != "delivered" || !delivery.Terminal || delivery.Failure != nil || len(delivery.DeadLetters) != 0 || delivery.RetryCount != 0 {
			t.Fatalf("map delivery did not settle successfully: %+v", delivery)
		}
		business = append(business, event)
	}
	if len(business) == 0 {
		t.Fatal("map recipe has no actual business or lifecycle publications")
	}
	return business
}
