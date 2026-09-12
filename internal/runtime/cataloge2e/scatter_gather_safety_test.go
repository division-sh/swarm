package cataloge2e

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestScatterGatherSafetyBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/cataloge2e/testdata/scatter-gather-safety"))
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, variant := range []struct {
			name                    string
			order                   []int
			restart, repeat, reject bool
		}{
			{name: "ordered", order: []int{0, 1, 2}},
			{name: "reversed", order: []int{2, 1, 0}},
			{name: "duplicate_publication", order: []int{1, 0, 2}, repeat: true},
			{name: "partial_restart", order: []int{2, 0, 1}, restart: true},
			{name: "rejected_input", order: []int{0, 2, 1}, reject: true},
		} {
			t.Run(string(backend)+"/"+variant.name, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
				var groups []catalogTranscriptGroup
				publish := func(event string, payload map[string]any) catalogTriggerStep {
					t.Helper()
					step := catalogTriggerStep{Event: event, Payload: payload, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
					if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
						t.Fatal(err)
					}
					scatterGatherWait(t, h, step)
					groups = append(groups, catalogTranscriptGroup{steps: []catalogTriggerStep{step}})
					return step
				}
				items := []any{map[string]any{"item_id": "alpha", "value": "red"}, map[string]any{"item_id": "beta", "value": "green"}, map[string]any{"item_id": "gamma", "value": "blue"}}
				publish("collector/batch.opened", map[string]any{"batch_id": "batch-one", "expected_item_ids": []any{"alpha", "beta", "gamma"}})
				publish("batch.submitted", map[string]any{"batch_id": "batch-one", "items": items})
				ids := map[string]string{}
				paths := map[string]string{}
				timerIDs := map[string]string{}
				registrations, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range registrations {
					if event.EventName != "item.registered" {
						continue
					}
					key, ok := event.Payload["item_id"].(string)
					if !ok || paths[key] != "" || len(event.Deliveries) != 1 {
						t.Fatalf("invalid registration %s: %v", event.EventID, event.Payload)
					}
					route := event.Deliveries[0].Route.Target.Route()
					if route.FlowID != "workers" || route.EntityID == "" {
						t.Fatalf("invalid worker route: %+v", route)
					}
					paths[key], ids[key] = route.FlowInstance, route.EntityID
				}
				if len(paths) != len(items) {
					t.Fatalf("registration set: %v", paths)
				}
				done := map[string]bool{}
				check := func(completed int) {
					t.Helper()
					if counts := scatterGatherCounts(t, h); counts["entity_state"] != 5 {
						t.Fatalf("unexpected entity set: %v", counts)
					}
					for _, raw := range items {
						item := raw.(map[string]any)
						key := item["item_id"].(string)
						worker := scatterGatherLoad(t, h, paths[key])
						if old := ids[key]; old != "" && old != worker.EntityID {
							t.Fatalf("%s rematerialized: %s -> %s", key, old, worker.EntityID)
						}
						ids[key] = worker.EntityID
						state := "registered"
						active := 1
						if done[key] {
							state = "complete"
							active = 0
						}
						if worker.CurrentState != state || worker.Fields["item_id"] != key || worker.Fields["value"] != item["value"] || worker.Fields["batch_id"] != "batch-one" {
							t.Fatalf("worker %s: %+v", key, worker)
						}
						var timers int
						if err := h.db.QueryRowContext(h.ctx, `SELECT COUNT(*) FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer' AND status='active'`, catalogRuntimeRunID, worker.EntityID).Scan(&timers); err != nil {
							t.Fatal(err)
						}
						if timers != active {
							t.Fatalf("worker %s active timers=%d want %d", key, timers, active)
						}
						var timerID, timerStatus string
						if err := h.db.QueryRowContext(h.ctx, `SELECT timer_id,status FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer'`, catalogRuntimeRunID, worker.EntityID).Scan(&timerID, &timerStatus); err != nil {
							t.Fatal(err)
						}
						if old := timerIDs[key]; old != "" && old != timerID {
							t.Fatalf("worker %s timer recreated: %s -> %s", key, old, timerID)
						}
						timerIDs[key] = timerID
						wantTimerStatus := "active"
						if done[key] {
							wantTimerStatus = "cancelled"
						}
						if timerStatus != wantTimerStatus {
							t.Fatalf("worker %s timer=%s want %s", key, timerStatus, wantTimerStatus)
						}
					}
					collector := scatterGatherLoad(t, h, "collector")
					carrier, err := engine.StateCarrierFromPersisted(collector.Fields, collector.Bookkeeping, collector.Gates, collector.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					activation, found, err := joinruntime.Load(carrier.StateBuckets, identitytest.FlowNode(t, "collector", "gather"), joinruntime.ActivationKey("awaiting", "awaiting", "batch-one"))
					if err != nil || !found || activation.Completed() != completed || activation.Expected() != 3 {
						t.Fatalf("gather %+v found=%v err=%v", activation, found, err)
					}
					if completed == 3 {
						if collector.CurrentState != "complete" || activation.CloseReason != joinruntime.CloseReasonComplete || !reflect.DeepEqual(activation.Results(), []any{"red", "green", "blue"}) {
							t.Fatalf("final gather: %s %+v", collector.CurrentState, activation)
						}
					} else if collector.CurrentState != "awaiting" || activation.Status != joinruntime.StatusOpen {
						t.Fatalf("premature gather completion: %+v", activation)
					}
					var reports int
					if err := h.db.QueryRowContext(h.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name LIKE 'workers/%/item.reported'`, catalogRuntimeRunID).Scan(&reports); err != nil {
						t.Fatal(err)
					}
					if reports != completed {
						t.Fatalf("reports=%d want %d", reports, completed)
					}
					public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
					if err != nil {
						t.Fatal(err)
					}
					for _, event := range public {
						if len(event.DeadLetters) != 0 {
							t.Fatalf("dead letters for %s: %+v", event.EventName, event.DeadLetters)
						}
						isReport := strings.HasSuffix(event.EventName, "/item.reported")
						if event.EventName != "item.registered" && event.EventName != "item.finished" && !isReport {
							continue
						}
						key, ok := event.Payload["item_id"].(string)
						if !ok || ids[key] == "" || len(event.Deliveries) != 1 {
							t.Fatalf("unexpected item publication: %s %+v", event.EventName, event.Payload)
						}
						delivery := event.Deliveries[0]
						route := delivery.Route.Target.Route()
						wantPath, wantID, wantNode := paths[key], ids[key], identitytest.FlowNode(t, "workers", "worker").Key()
						if isReport {
							if event.EventName != paths[key]+"/item.reported" {
								t.Fatalf("report escaped its worker: %s", event.EventName)
							}
							wantPath, wantID, wantNode = "collector", collector.EntityID, identitytest.FlowNode(t, "collector", "gather").Key()
						}
						if route.FlowInstance != wantPath || route.EntityID != wantID || delivery.SubscriberID != wantNode || delivery.Status != "delivered" || delivery.ClaimVersion != 1 {
							t.Fatalf("wrong or unsettled %s receiver for %s: %+v", event.EventName, key, delivery)
						}
					}
				}
				check(0)
				if variant.reject {
					before := scatterGatherCounts(t, h)
					states := scatterGatherStates(t, h, paths)
					err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "batch.submitted", Payload: map[string]any{"batch_id": "invalid", "items": []any{map[string]any{"item_id": "bad", "value": true}}}}, 20*time.Second, false)
					if err == nil {
						t.Fatal("malformed item accepted")
					}
					if !strings.Contains(err.Error(), "schema validation failed: $.items[0].value must be string") {
						t.Fatalf("wrong refusal: %v", err)
					}
					if after := scatterGatherStates(t, h, paths); !reflect.DeepEqual(states, after) {
						t.Fatal("rejected input mutated persisted workflow state")
					}
					if after := scatterGatherCounts(t, h); !reflect.DeepEqual(before, after) {
						t.Fatalf("rejected input mutated domain: before=%v after=%v", before, after)
					}
					check(0)
				}
				for index, member := range variant.order {
					step := publish("batch.finished", map[string]any{"batch_id": "batch-one", "items": []any{items[member]}})
					done[items[member].(map[string]any)["item_id"].(string)] = true
					check(index + 1)
					if variant.repeat {
						before := scatterGatherCounts(t, h)
						states := scatterGatherStates(t, h, paths)
						if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
							t.Fatal(err)
						}
						if after := scatterGatherCounts(t, h); !reflect.DeepEqual(before, after) {
							t.Fatalf("duplicate changed domain: %v -> %v", before, after)
						}
						if after := scatterGatherStates(t, h, paths); !reflect.DeepEqual(states, after) {
							t.Fatal("duplicate mutated persisted workflow state")
						}
						check(index + 1)
					}
					if variant.restart && index == 0 {
						hash, err := contracts.BundleHash(h.bundle)
						if err != nil {
							t.Fatal(err)
						}
						digest, err := catalogReplayPlatformSpecDigest(repoRootFromCatalogE2E(t))
						if err != nil {
							t.Fatal(err)
						}
						h = h.reopenFromTranscript(&catalogExecutionTranscript{version: catalogReplayTranscriptVersion, platformSpecDigest: digest, bundleHash: hash, runID: catalogRuntimeRunID, groups: groups})
						check(index + 1)
					}
				}
			})
		}
	}
}

// A committed fan-out intent can outlive PublishAndWait's process-local tree.
// Observe exact durable descendants, not elapsed time or a stable count of loops.
func scatterGatherWait(t testing.TB, h *runtimeHarness, step catalogTriggerStep) {
	t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 20*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	want := 0
	cardinality := 0
	if items, ok := step.Payload["items"].([]any); ok {
		want = len(items)
		cardinality = len(items)
	}
	if step.Event == "batch.finished" {
		want *= 2
	}
	for {
		public, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
		if err != nil {
			t.Fatal(err)
		}
		tree := map[string]bool{step.eventID: true}
		for changed := true; changed; {
			changed = false
			for id, event := range public {
				if !tree[id] && tree[event.SourceEventID] {
					tree[id] = true
					changed = true
				}
			}
		}
		ready := len(tree) == want+1
		for id := range tree {
			event, found := public[id]
			if !found {
				ready = false
				continue
			}
			if len(event.DeadLetters) != 0 {
				t.Fatalf("descendant %s dead letter: %+v", event.EventName, event.DeadLetters)
			}
			if len(event.Deliveries) != 1 {
				ready = false
				continue
			}
			if event.Deliveries[0].Status != "delivered" {
				ready = false
			}
		}
		if ready {
			if step.Event == "batch.submitted" || step.Event == "batch.finished" {
				var count int
				if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_event_id=$2 AND status='closed' AND cardinality=$3 AND cursor=$3 AND claim_owner IS NULL`, catalogRuntimeRunID, step.eventID, cardinality).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("%s lacks exact closed %d-item issuance", step.Event, cardinality)
				}
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s descendant frontier: got %d events want %d: %v", step.Event, len(tree), want+1, ctx.Err())
		case <-tick.C:
		}
	}
}

func scatterGatherLoad(t testing.TB, h *runtimeHarness, path string) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := h.workflow.Load(catalogRunContext(h, catalogRuntimeRunID), flowidentity.RunScopedFlowInstance{RunID: catalogRuntimeRunID, Route: flowidentity.RouteForInstancePath(path)})
	if err != nil || !found {
		t.Fatalf("load %s: found=%v err=%v", path, found, err)
	}
	return instance
}

func scatterGatherCounts(t testing.TB, h *runtimeHarness) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"entity_state", "timers", "fan_out_intents"} {
		var count int
		if err := h.db.QueryRowContext(h.ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=$1", table), catalogRuntimeRunID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	var domainEvents int
	if err := h.db.QueryRowContext(h.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND (event_name IN ('batch.submitted','batch.finished','item.registered','item.finished','collector/batch.opened') OR event_name LIKE 'workers/%/item.reported')`, catalogRuntimeRunID).Scan(&domainEvents); err != nil {
		t.Fatal(err)
	}
	counts["domain_events"] = domainEvents
	return counts
}

func scatterGatherStates(t testing.TB, h *runtimeHarness, paths map[string]string) map[string]pipeline.WorkflowInstance {
	t.Helper()
	states := map[string]pipeline.WorkflowInstance{}
	for _, path := range paths {
		states[path] = scatterGatherLoad(t, h, path)
	}
	for _, path := range []string{catalogRuntimeRunID, "collector"} {
		states[path] = scatterGatherLoad(t, h, path)
	}
	return states
}
