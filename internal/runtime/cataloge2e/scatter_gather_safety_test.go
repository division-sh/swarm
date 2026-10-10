package cataloge2e

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/replayconformance"
	"github.com/google/uuid"
)

func TestScatterGatherSafetyBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/cataloge2e/testdata/scatter-gather-safety"))
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, variant := range []struct {
			name                    string
			order                   []int
			restart, repeat, reject bool
			holdFinalization        bool
			hundred                 bool
		}{
			{name: "ordered", order: []int{0, 1, 2}},
			{name: "reversed", order: []int{2, 1, 0}},
			{name: "duplicate_publication", order: []int{1, 0, 2}, repeat: true},
			{name: "held_finalization", order: []int{1, 0, 2}, repeat: true, holdFinalization: true},
			{name: "partial_restart", order: []int{2, 0, 1}, restart: true},
			{name: "rejected_input", order: []int{0, 2, 1}, reject: true},
			{name: "hundred_reverse_completion", hundred: true},
		} {
			t.Run(string(backend)+"/"+variant.name, func(t *testing.T) {
				h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
				var finalization *scatterGatherFinalizationHold
				if variant.holdFinalization {
					finalization = &scatterGatherFinalizationHold{held: make(chan events.Event, 1), release: make(chan struct{}), returned: make(chan struct{})}
					t.Cleanup(finalization.Release)
					h.rt.Pipeline.SetTestLifecycleProbe(finalization)
				}
				observePublication := scatterGatherTransactionDiagnostics(t, h)
				var groups []catalogTranscriptGroup
				var phaseStart, phaseDeadline time.Time
				var phaseCtx context.Context
				var phaseCancel context.CancelFunc
				var phaseEvent string
				var publicationDeadline time.Time
				readContext := func() context.Context {
					if phaseCtx != nil {
						return phaseCtx
					}
					return catalogRunContext(h, catalogRuntimeRunID)
				}
				finishPhase := func() {
					t.Helper()
					if phaseCtx == nil {
						return
					}
					elapsed := time.Since(phaseStart)
					t.Logf("%s complete phase elapsed=%s original_target=20s merge_ceiling=60s; original performance obligation remains open in #2394", phaseEvent, elapsed)
					phaseCancel()
					phaseCtx = nil
					if !time.Now().Before(phaseDeadline) {
						t.Fatalf("%s exceeded its non-resetting 60s phase ceiling: %s", phaseEvent, elapsed)
					}
				}
				publish := func(event string, payload map[string]any) catalogTriggerStep {
					t.Helper()
					defer observePublication(event)()
					step := catalogTriggerStep{Event: event, Payload: payload, eventID: uuid.NewString(), createdAt: time.Now().UTC(), sourceAgent: "cataloge2e", inputKind: catalogReplayInputRootIngress}
					publishTimeout := 20 * time.Second
					publicationDeadline = step.createdAt.Add(publishTimeout)
					if backend == catalogBackendPostgres && variant.hundred && (event == "batch.submitted" || event == "batch.finished") {
						// Lead exception 5752572474: one budget, never reset on progress.
						if phaseCtx != nil {
							t.Fatal("previous phase was not fully checked")
						}
						phaseStart = time.Now()
						phaseDeadline = phaseStart.Add(time.Minute)
						phaseEvent = event
						phaseCtx, phaseCancel = context.WithDeadline(catalogRunContext(h, catalogRuntimeRunID), phaseDeadline)
						t.Cleanup(phaseCancel)
						publishTimeout = time.Until(phaseDeadline)
						publicationDeadline = phaseDeadline
					}
					if err := h.publishRuntimeEventResultForStep(step, publishTimeout, false); err != nil {
						t.Fatal(err)
					}
					scatterGatherWait(t, h, step, phaseStart, phaseDeadline)
					groups = append(groups, catalogTranscriptGroup{steps: []catalogTriggerStep{step}})
					return step
				}
				items := []any{map[string]any{"item_id": "alpha", "value": "red"}, map[string]any{"item_id": "beta", "value": "green"}, map[string]any{"item_id": "gamma", "value": "blue"}}
				if variant.hundred {
					items = make([]any, 100)
					for i := range items {
						items[i] = map[string]any{"item_id": fmt.Sprintf("item-%03d", i), "value": fmt.Sprintf("value-%03d", i)}
					}
				}
				expectedIDs, expectedResults := make([]any, len(items)), make([]any, len(items))
				for i, raw := range items {
					item := raw.(map[string]any)
					expectedIDs[i], expectedResults[i] = item["item_id"], item["value"]
				}
				opened := publish("batch.opened", map[string]any{"batch_id": "batch-one", "expected_item_ids": expectedIDs})
				openedEvents, err := scatterGatherPublicEvents(h, nil)
				if err != nil {
					t.Fatal(err)
				}
				openedEvent, found := openedEvents[opened.eventID]
				if !found || openedEvent.EventName != "batch.opened" || len(openedEvent.Deliveries) != 1 {
					t.Fatalf("batch.opened lacks its exact collector delivery: found=%v event=%+v", found, openedEvent)
				}
				collectorRoute := openedEvent.Deliveries[0].Route.Target.Route()
				if collectorRoute.FlowID != "collector" || collectorRoute.FlowInstance == "" || collectorRoute.EntityID == "" {
					t.Fatalf("batch.opened collector route is incomplete: %+v", collectorRoute)
				}
				initialCollector := scatterGatherLoad(t, h, collectorRoute.FlowInstance, readContext())
				if initialCollector.StorageRef != collectorRoute.FlowInstance || initialCollector.EntityID != collectorRoute.EntityID || initialCollector.Fields["batch_id"] != "batch-one" {
					t.Fatalf("collector state disagrees with its published route: route=%+v instance=%+v", collectorRoute, initialCollector)
				}
				collectorRef, collectorID := initialCollector.StorageRef, initialCollector.EntityID
				publish("batch.submitted", map[string]any{"batch_id": "batch-one", "items": items})
				ids := map[string]string{}
				paths := map[string]string{}
				timerIDs := map[string]string{}
				registrations, err := scatterGatherPublicEvents(h, phaseCtx)
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
				var retainedJoinRef timeridentity.JoinRef
				check := func(completed int) {
					t.Helper()
					if completed == len(items) {
						scatterGatherWaitForJoinOutcome(t, h, collectorRef, retainedJoinRef, publicationDeadline)
					}
					if counts := scatterGatherCounts(t, h, readContext()); counts["entity_state"] != len(items)+2 {
						t.Fatalf("unexpected entity set: %v", counts)
					}
					for _, raw := range items {
						item := raw.(map[string]any)
						key := item["item_id"].(string)
						worker := scatterGatherLoad(t, h, paths[key], readContext())
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
						if err := h.db.QueryRowContext(readContext(), `SELECT COUNT(*) FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer' AND status='active'`, catalogRuntimeRunID, worker.EntityID).Scan(&timers); err != nil {
							t.Fatal(err)
						}
						if timers != active {
							t.Fatalf("worker %s active timers=%d want %d", key, timers, active)
						}
						var timerID, timerStatus string
						if err := h.db.QueryRowContext(readContext(), `SELECT timer_id,status FROM timers WHERE run_id=$1 AND entity_id=$2 AND task_type='workflow_timer'`, catalogRuntimeRunID, worker.EntityID).Scan(&timerID, &timerStatus); err != nil {
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
					collector := scatterGatherLoad(t, h, collectorRef, readContext())
					if collector.StorageRef != collectorRef || collector.EntityID != collectorID || collector.InstanceID != initialCollector.InstanceID {
						t.Fatalf("collector rematerialized: initial=%+v current=%+v", initialCollector, collector)
					}
					carrier, err := engine.StateCarrierFromPersisted(collector.Fields, collector.Bookkeeping, collector.Gates, collector.StateBuckets)
					if err != nil {
						t.Fatal(err)
					}
					joinNode := identitytest.FlowNode(t, "collector", "gather")
					if collector.Fields["batch_id"] != "batch-one" {
						t.Fatalf("collector business batch changed: %#v", collector.Fields["batch_id"])
					}
					if !retainedJoinRef.Valid() {
						declaration, err := timeridentity.NewJoinRef(joinNode, "item.reported", "awaiting", "awaiting")
						if err != nil {
							t.Fatal(err)
						}
						activations, err := joinruntime.List(carrier.StateBuckets)
						if err != nil {
							t.Fatal(err)
						}
						for _, activation := range activations {
							ref := activation.JoinRef()
							if !ref.Declaration().Equal(declaration) {
								continue
							}
							if retainedJoinRef.Valid() {
								t.Fatal("scatter-gather fixture has multiple arms for one declaration")
							}
							if err := ref.StageEntry().RequireOwner(catalogRuntimeRunID, "collector", collector.InstanceID, collector.StorageRef, collector.EntityID, "awaiting"); err != nil {
								t.Fatalf("collector arm lifecycle owner: %v", err)
							}
							retainedJoinRef = ref
						}
						if !retainedJoinRef.Valid() {
							t.Fatal("collector lacks its exact persisted stage-entry arm")
						}
					}
					activation, found, err := joinruntime.Load(carrier.StateBuckets, joinNode, joinruntime.ActivationKey(retainedJoinRef))
					if err != nil || !found || activation.Completed() != completed || activation.Expected() != len(items) {
						t.Fatalf("gather %+v found=%v err=%v", activation, found, err)
					}
					if completed == len(items) {
						results, err := activation.Results()
						if err != nil || collector.CurrentState != "complete" || activation.CloseReason != joinruntime.CloseReasonComplete || activation.OutcomePending || !activation.OutcomeFired || !reflect.DeepEqual(results, expectedResults) {
							t.Fatalf("final gather: %s %+v", collector.CurrentState, activation)
						}
					} else if collector.CurrentState != "awaiting" || activation.Status != joinruntime.StatusOpen {
						t.Fatalf("premature gather completion: %+v", activation)
					}
					var reports int
					if err := h.db.QueryRowContext(readContext(), `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name LIKE 'workers/%/item.reported'`, catalogRuntimeRunID).Scan(&reports); err != nil {
						t.Fatal(err)
					}
					if reports != completed {
						t.Fatalf("reports=%d want %d", reports, completed)
					}
					public, err := scatterGatherPublicEvents(h, phaseCtx)
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
							wantPath, wantID, wantNode = collectorRef, collectorID, identitytest.FlowNode(t, "collector", "gather").Key()
						}
						if route.FlowInstance != wantPath || route.EntityID != wantID || delivery.SubscriberID != wantNode || delivery.Status != "delivered" || delivery.ClaimVersion != 1 {
							logScatterGatherAttempts(t, h, delivery.DeliveryID)
							t.Fatalf("wrong or unsettled %s receiver for %s: %+v", event.EventName, key, delivery)
						}
					}
				}
				check(0)
				finishPhase()
				if variant.hundred {
					reversed := make([]any, len(items))
					for i, item := range items {
						reversed[len(items)-1-i] = item
					}
					publish("batch.finished", map[string]any{"batch_id": "batch-one", "items": reversed})
					for _, item := range items {
						done[item.(map[string]any)["item_id"].(string)] = true
					}
					check(len(items))
					finishPhase()
				}
				if variant.reject {
					before := scatterGatherCounts(t, h, h.ctx)
					states := scatterGatherStates(t, h, paths, collectorRef)
					err := h.publishRuntimeEventResultForStep(catalogTriggerStep{Event: "batch.submitted", Payload: map[string]any{"batch_id": "invalid", "items": []any{map[string]any{"item_id": "bad", "value": true}}}}, 20*time.Second, false)
					if err == nil {
						t.Fatal("malformed item accepted")
					}
					if !strings.Contains(err.Error(), "schema validation failed: $.items[0].value must be string") {
						t.Fatalf("wrong refusal: %v", err)
					}
					if after := scatterGatherStates(t, h, paths, collectorRef); !reflect.DeepEqual(states, after) {
						t.Fatalf("rejected input mutated persisted workflow state: before=%+v after=%+v", states, after)
					}
					if after := scatterGatherCounts(t, h, h.ctx); !reflect.DeepEqual(before, after) {
						t.Fatalf("rejected input mutated domain: before=%v after=%v", before, after)
					}
					check(0)
				}
				for index, member := range variant.order {
					step := publish("batch.finished", map[string]any{"batch_id": "batch-one", "items": []any{items[member]}})
					done[items[member].(map[string]any)["item_id"].(string)] = true
					check(index + 1)
					if variant.name == "ordered" {
						frontier, err := scatterGatherDeliveryProgress(h.ctx, h, step.eventID)
						if err != nil {
							t.Fatal(err)
						}
						var count int
						for _, entry := range frontier {
							if entry.status != "delivered" || entry.reason != "" {
								t.Fatalf("settled diagnostic frontier contains unfinished work: %+v", frontier)
							}
							count += entry.count
						}
						if count != 3 {
							t.Fatalf("diagnostic frontier must include ingress, worker and collector, not other publications: %+v", frontier)
						}
					}
					if variant.repeat {
						finalRoutes := map[string]string{}
						for key := range done {
							finalRoutes[paths[key]] = ids[key]
						}
						if index+1 == len(items) {
							finalRoutes[collectorRef] = collectorID
						}
						if finalization != nil && index+1 == len(items) {
							ctx, cancel := context.WithDeadline(catalogRunContext(h, catalogRuntimeRunID), publicationDeadline)
							select {
							case event := <-finalization.held:
								if event.RunID() != catalogRuntimeRunID || event.FlowInstance() != collectorRef || event.EntityID() != collectorID {
									t.Fatalf("held finalization belongs to another route: event=%s run=%s instance=%s entity=%s", event.ID(), event.RunID(), event.FlowInstance(), event.EntityID())
								}
							case <-ctx.Done():
								t.Fatalf("collector finalization was not held within the publication deadline: %v", ctx.Err())
							}
							ready, err := scatterGatherFinalStageSnapshotsReady(ctx, h, finalRoutes)
							if err != nil || !ready {
								t.Fatalf("committed final-stage snapshot was lost while cleanup was held: ready=%v err=%v", ready, err)
							}
							select {
							case <-finalization.returned:
								t.Fatal("postcommit cleanup passed the held completion barrier")
							default:
							}
							held := scatterGatherLoad(t, h, collectorRef, ctx)
							if held.CurrentState != "complete" || held.Status != "active" || !held.TerminatedAt.IsZero() {
								t.Fatalf("negative control lost exact final-stage lifecycle: %+v", held)
							}
							if len(held.TransitionHistory) == 0 {
								t.Fatal("held final-stage snapshot has no exact transition cause")
							}
							finalization.Release()
							select {
							case <-finalization.returned:
							case <-ctx.Done():
								t.Fatalf("released postcommit cleanup did not return within the publication deadline: %v", ctx.Err())
							}
							var selected any = h.pg
							if h.sqlite != nil {
								selected = h.sqlite
							}
							if receipt := storetest.ObservePipelineReceipt(t, ctx, selected, held.TransitionHistory[len(held.TransitionHistory)-1].TriggerEventID); receipt.Count > 1 {
								t.Fatalf("held finalization duplicated its exact pipeline settlement: %+v", receipt)
							}
							cancel()
						}
						scatterGatherWaitForFinalStageSnapshots(t, h, finalRoutes, publicationDeadline)
						before := scatterGatherCounts(t, h, h.ctx)
						states := scatterGatherStates(t, h, paths, collectorRef)
						if err := h.publishRuntimeEventResultForStep(step, 20*time.Second, false); err != nil {
							t.Fatal(err)
						}
						if after := scatterGatherCounts(t, h, h.ctx); !reflect.DeepEqual(before, after) {
							t.Fatalf("duplicate changed domain: %v -> %v", before, after)
						}
						if after := scatterGatherStates(t, h, paths, collectorRef); !reflect.DeepEqual(states, after) {
							t.Fatalf("duplicate mutated persisted workflow state: before=%+v after=%+v", states, after)
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
func scatterGatherWait(t testing.TB, h *runtimeHarness, step catalogTriggerStep, phaseStart, phaseDeadline time.Time) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(20 * time.Second)
	if !phaseDeadline.IsZero() {
		started, deadline = phaseStart, phaseDeadline
	}
	ctx, cancel := context.WithDeadline(h.ctx, deadline)
	defer cancel()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
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
	issued := step.Event != "batch.submitted" && step.Event != "batch.finished"
	for {
		if !issued {
			count, err := storetest.CountClosedSourceFanOutIssuance(ctx, reader, catalogRuntimeRunID, step.eventID, cardinality)
			if err != nil {
				logScatterGatherProgress(t, h, step.eventID)
				t.Fatal(err)
			}
			if count != 1 {
				select {
				case <-ctx.Done():
					logScatterGatherProgress(t, h, step.eventID)
					t.Fatalf("%s exact issuance did not close: %v", step.Event, ctx.Err())
				case <-tick.C:
				}
				continue
			}
			issued = true
		}
		// Poll only durable identities/status. Full public hydration below remains
		// mandatory, but must not compete with issuance for every growing prefix.
		// Match the public helper's explicit ExcludeRuntimeLogs filter.
		frontier, err := storetest.ReadCausalDeliveryFrontier(ctx, reader, catalogRuntimeRunID, step.eventID)
		observed, unsettled, deadLetters := frontier.Observed, frontier.Unsettled, frontier.DeadLetters
		if err != nil {
			logScatterGatherProgress(t, h, step.eventID)
			t.Fatal(err)
		}
		if deadLetters != 0 {
			logScatterGatherFailures(t, h)
			t.Fatalf("%s durable descendants have %d dead letters", step.Event, deadLetters)
		}
		if observed != want+1 || unsettled != 0 {
			select {
			case <-ctx.Done():
				logScatterGatherProgress(t, h, step.eventID)
				t.Fatalf("%s durable descendant frontier: got %d events want %d, unsettled=%d: %v", step.Event, observed, want+1, unsettled, ctx.Err())
			case <-tick.C:
			}
			continue
		}
		if cardinality == 100 {
			t.Logf("%s exact durable frontier settled after %s", step.Event, time.Since(started))
		}
		var readCtx context.Context
		if !phaseDeadline.IsZero() {
			readCtx = ctx
		}
		public, err := scatterGatherPublicEvents(h, readCtx)
		if err != nil {
			t.Fatal(err)
		}
		if cardinality == 100 {
			t.Logf("%s full public hydration finished after %s", step.Event, time.Since(started))
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
				count, err := storetest.CountClosedSourceFanOutIssuance(ctx, reader, catalogRuntimeRunID, step.eventID, cardinality)
				if err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("%s lacks exact closed %d-item issuance", step.Event, cardinality)
				}
			}
			if !phaseDeadline.IsZero() && !time.Now().Before(phaseDeadline) {
				t.Fatalf("%s exceeded its non-resetting 60s phase ceiling: %s", step.Event, time.Since(started))
			}
			return
		}
		select {
		case <-ctx.Done():
			logScatterGatherProgress(t, h, step.eventID)
			t.Fatalf("%s descendant frontier: got %d events want %d: %v", step.Event, len(tree), want+1, ctx.Err())
		case <-tick.C:
		}
	}
}

func scatterGatherWaitForJoinOutcome(t testing.TB, h *runtimeHarness, collectorRef string, ref timeridentity.JoinRef, deadline time.Time) {
	t.Helper()
	if !ref.Valid() || deadline.IsZero() {
		t.Fatal("final gather requires its retained arm and original publication deadline")
	}
	ctx, cancel := context.WithDeadline(catalogRunContext(h, catalogRuntimeRunID), deadline)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		collector := scatterGatherLoad(t, h, collectorRef, ctx)
		carrier, err := engine.StateCarrierFromPersisted(collector.Fields, collector.Bookkeeping, collector.Gates, collector.StateBuckets)
		if err != nil {
			t.Fatal(err)
		}
		activation, found, err := joinruntime.Load(carrier.StateBuckets, ref.Node(), joinruntime.ActivationKey(ref))
		if err != nil || !found {
			t.Fatalf("retained gather continuation: found=%v err=%v", found, err)
		}
		if activation.OutcomeFired && !activation.OutcomePending {
			public, err := scatterGatherPublicEvents(h, ctx)
			if err != nil {
				t.Fatal(err)
			}
			matches := 0
			settled := false
			for _, event := range public {
				if event.EventName != "platform.join_complete" {
					continue
				}
				handle, actual, ok := timeridentity.ParseJoinHandle(event.Payload)
				if !ok || !actual.Equal(ref) || handle.Kind() != timeridentity.TimerHandleJoinComplete {
					t.Fatalf("gather completion changed its retained arm: %+v", event)
				}
				matches++
				if len(event.DeadLetters) != 0 || len(event.Deliveries) != 1 {
					t.Fatalf("gather completion lacks its exact delivery: %+v", event)
				}
				delivery := event.Deliveries[0]
				route := delivery.Route.Target.Route()
				if route.FlowInstance != collectorRef || route.EntityID != collector.EntityID || delivery.SubscriberID != ref.Node().Key() || delivery.ClaimVersion != 1 {
					logScatterGatherAttempts(t, h, delivery.DeliveryID)
					t.Fatalf("gather completion has wrong receiver: %+v", delivery)
				}
				settled = delivery.Status == "delivered"
			}
			if matches != 1 {
				t.Fatalf("gather completion publications=%d want 1", matches)
			}
			if settled {
				return
			}
		}
		select {
		case <-ctx.Done():
			logScatterGatherFailures(t, h)
			t.Fatalf("gather continuation did not settle within original deadline: %+v: %v", activation, ctx.Err())
		case <-tick.C:
		}
	}
}

func logScatterGatherFailures(t testing.TB, h *runtimeHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
	defer cancel()
	public, err := scatterGatherPublicEvents(h, ctx)
	if err != nil {
		t.Logf("failed public failure readback: %v", err)
		return
	}
	for _, event := range public {
		if len(event.DeadLetters) != 0 {
			t.Logf("dead-letter publication %s/%s: %+v", event.EventName, event.EventID, event.DeadLetters)
		}
		for _, delivery := range event.Deliveries {
			if delivery.ClaimVersion > 1 || delivery.Failure != nil {
				logScatterGatherAttempts(t, h, delivery.DeliveryID)
			}
		}
	}
}

func logScatterGatherAttempts(t testing.TB, h *runtimeHarness, deliveryID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
	defer cancel()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Logf("failed attempt readback for %s: %v", deliveryID, err)
		return
	}
	rows, err := storetest.ReadDeliveryAttemptDiagnosticRows(ctx, reader, deliveryID)
	if err != nil {
		t.Logf("failed attempt readback for %s: %v", deliveryID, err)
		return
	}
	for _, row := range rows {
		t.Logf("delivery %s attempt %d: closure=%s outcome=%s reason=%s failure=%s", deliveryID, row.Version, row.Closure, row.Outcome, row.Reason, row.Failure)
	}
}

func logScatterGatherProgress(t testing.TB, h *runtimeHarness, eventID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.ctx), time.Second)
	defer cancel()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Logf("failed fan-out observation: %v", err)
		return
	}
	rows, err := storetest.ReadSourceFanOutIntentDiagnosticRows(ctx, reader, catalogRuntimeRunID, eventID)
	if err != nil {
		t.Logf("failed fan-out observation: %v", err)
		return
	}
	for _, row := range rows {
		t.Logf("fan-out at failed deadline: status=%s cursor=%d/%d owner=%q generation=%d lease=%v", row.Status, row.Cursor, row.Cardinality, row.Owner, row.Generation, row.LeaseExpiry)
	}
	facts, err := storetest.ReadSourceRouteSettlementStorage(ctx, reader, catalogRuntimeRunID, eventID)
	if err != nil {
		t.Logf("failed fan-out event size read: %v", err)
	} else {
		t.Logf("fan-out persisted settlement inputs: events=%d distinct=%d bytes=%d", facts.Events, facts.DistinctLedgers, facts.Bytes)
	}
	logScatterGatherDeliveryProgress(t, ctx, h, eventID)
}

func logScatterGatherDeliveryProgress(t testing.TB, ctx context.Context, h *runtimeHarness, eventID string) {
	t.Helper()
	frontier, err := scatterGatherDeliveryProgress(ctx, h, eventID)
	if err != nil {
		t.Logf("failed descendant delivery observation: %v", err)
		return
	}
	for _, entry := range frontier {
		t.Logf("descendant delivery frontier: class=%s subscriber=%s status=%s reason=%q count=%d", entry.class, entry.subscriber, entry.status, entry.reason, entry.count)
	}
}

type scatterGatherDeliveryCount struct {
	class, subscriber, status, reason string
	count                             int
}

func scatterGatherDeliveryProgress(ctx context.Context, h *runtimeHarness, eventID string) ([]scatterGatherDeliveryCount, error) {
	// Inspect the failed publication's entire causal tree, not only its issued
	// ordinals. A closed cursor does not imply that downstream receivers settled.
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		return nil, err
	}
	rows, err := storetest.ReadCausalDeliveryStatusCounts(ctx, reader, catalogRuntimeRunID, eventID)
	if err != nil {
		return nil, err
	}
	var result []scatterGatherDeliveryCount
	for _, row := range rows {
		result = append(result, scatterGatherDeliveryCount{class: row.Class, subscriber: row.Subscriber, status: row.Status, reason: row.Reason, count: row.Count})
	}
	return result, nil
}

func scatterGatherPublicEvents(h *runtimeHarness, ctx context.Context) (map[string]operatorread.OperatorEventFull, error) {
	if ctx == nil {
		return catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
	}
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		return nil, err
	}
	return replayconformance.LoadOperatorEvents(testAuthorActivityContext(ctx), lister, catalogRuntimeRunID)
}

type scatterGatherFinalizationHold struct {
	held     chan events.Event
	release  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func (p *scatterGatherFinalizationHold) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerCompleted || signal.Status != "completed" || signal.EventType != "platform.join_complete" {
		return
	}
	event, ok := correlation.InboundEventFromContext(ctx)
	if !ok || correlation.RunIDFromContext(ctx) != catalogRuntimeRunID {
		panic("scatter finalization control requires its exact run-scoped inbound event")
	}
	p.held <- event
	<-p.release
	close(p.returned)
}

func (p *scatterGatherFinalizationHold) Release() {
	p.once.Do(func() { close(p.release) })
}

func scatterGatherFinalStageSnapshotsReady(ctx context.Context, h *runtimeHarness, routes map[string]string) (bool, error) {
	if correlation.RunIDFromContext(ctx) != catalogRuntimeRunID || len(routes) == 0 {
		return false, fmt.Errorf("final-stage snapshot fence requires exact current-run routes")
	}
	ready := true
	for path, entityID := range routes {
		owner := catalogExactWorkflowRoute(path)
		instance, found, err := h.workflow.Load(ctx, owner)
		if err != nil {
			return false, fmt.Errorf("final-stage snapshot load %s: %w", path, err)
		}
		if !found || entityID == "" || instance.StorageRef != path || instance.InstanceID != owner.Route.InstanceID || instance.WorkflowName != owner.Route.ScopeKey || instance.EntityID != entityID {
			return false, fmt.Errorf("final-stage snapshot route %s entity %s has mismatched persisted authority: found=%v instance=%+v", path, entityID, found, instance)
		}
		if instance.Status != "active" || !instance.TerminatedAt.IsZero() {
			return false, fmt.Errorf("ordinary final entry retired exact instance %s: %+v", path, instance)
		}
		if instance.CurrentState != "complete" {
			ready = false
		}
	}
	return ready, nil
}

func scatterGatherWaitForFinalStageSnapshots(t testing.TB, h *runtimeHarness, routes map[string]string, deadline time.Time) {
	t.Helper()
	if deadline.IsZero() {
		t.Fatal("final-stage snapshot fence requires the original publication deadline")
	}
	ctx, cancel := context.WithDeadline(catalogRunContext(h, catalogRuntimeRunID), deadline)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, err := scatterGatherFinalStageSnapshotsReady(ctx, h, routes)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			var selected any = h.pg
			if h.sqlite != nil {
				selected = h.sqlite
			}
			for path := range routes {
				instance := scatterGatherLoad(t, h, path, ctx)
				if len(instance.TransitionHistory) == 0 {
					t.Fatalf("final-stage snapshot %s lacks its exact transition cause", path)
				}
				eventID := instance.TransitionHistory[len(instance.TransitionHistory)-1].TriggerEventID
				if eventID == "" {
					t.Fatalf("final-stage snapshot %s has no committed cause", path)
				}
				receipt := storetest.ObservePipelineReceipt(t, ctx, selected, eventID)
				if receipt.Count > 1 || (receipt.Count == 1 && receipt.Outcome != "success") {
					t.Fatalf("final-stage snapshot %s has inexact pipeline settlement: %+v", path, receipt)
				}
				if receipt.Count != 1 {
					ready = false
				}
			}
			if ready {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("final-stage snapshots did not settle within the original publication deadline: routes=%v err=%v", routes, ctx.Err())
		case <-ticker.C:
		}
	}
}

func scatterGatherLoad(t testing.TB, h *runtimeHarness, path string, ctx context.Context) pipeline.WorkflowInstance {
	t.Helper()
	instance, found, err := h.workflow.Load(ctx, flowidentity.RunScopedFlowInstance{RunID: catalogRuntimeRunID, Route: flowidentity.RouteForInstancePath(path)})
	if err != nil || !found {
		t.Fatalf("load %s: found=%v err=%v", path, found, err)
	}
	return instance
}

func scatterGatherCounts(t testing.TB, h *runtimeHarness, ctx context.Context) map[string]int {
	t.Helper()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	counts, err := storetest.ReadScatterGatherPhysicalCounts(ctx, reader, catalogRuntimeRunID)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]int{"entity_state": counts.Entities, "timers": counts.Timers, "fan_out_intents": counts.FanOutIntents, "domain_events": counts.DomainEvents}
}

func scatterGatherStates(t testing.TB, h *runtimeHarness, paths map[string]string, collectorRef string) map[string]pipeline.WorkflowInstance {
	t.Helper()
	states := map[string]pipeline.WorkflowInstance{}
	for _, path := range paths {
		states[path] = scatterGatherLoad(t, h, path, catalogRunContext(h, catalogRuntimeRunID))
	}
	for _, path := range []string{catalogRuntimeRunID, collectorRef} {
		states[path] = scatterGatherLoad(t, h, path, catalogRunContext(h, catalogRuntimeRunID))
	}
	return states
}
