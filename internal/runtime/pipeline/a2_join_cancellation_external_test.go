package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

type a2CancellationClaim struct {
	event events.Event
	route events.DeliveryRoute
	claim deliverylifecycle.Claim
}

// Only pause after the real owner has acquired a claim. The unchanged result
// returns to the coordinator; no delivery, settlement or claim is fabricated.
type a2CancellationClaimGate struct {
	gateRecoverySelectedStore
	ids      map[string]bool
	typeName events.EventType
	entered  chan a2CancellationClaim
	release  chan struct{}
	once     sync.Once
}

func (g *a2CancellationClaimGate) resume() { g.once.Do(func() { close(g.release) }) }

func (g *a2CancellationClaimGate) ClaimDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	result, err := g.gateRecoverySelectedStore.ClaimDelivery(ctx, authority, event, route)
	if acquired, ok := result.Acquired(); ok && (g.ids[event.ID()] || g.typeName != "" && event.Type() == g.typeName) {
		g.entered <- a2CancellationClaim{event: event, route: route, claim: acquired.Claim}
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	}
	return result, err
}

// These are existing-root component fixtures, not C/E eager construction,
// normal runtime boot, a process kill, or arbitrary context-cancellation proof.
func TestA2JoinCancellationSettlesAlreadyBoundWorkOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, mode := range []string{"stage_exit", "terminal_stage", "run_stop"} {
			for _, phase := range []string{"bound_arrival_until", "published_completion"} {
				t.Run(backend.name+"/"+mode+"/"+phase, func(t *testing.T) {
					selected := backend.open(t)
					runID, lastID, untilID := uuid.NewString(), uuid.NewString(), uuid.NewString()
					insertGateRecoveryRun(t, selected, runID)
					ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
					files := a2CountJoinFiles(2)
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "  ready: {terminal: true}", "  ready: {terminal: true}\n  dispatching: {}\n  cancelled: {terminal: true}", 1)
					files["schema.yaml"] = strings.Replace(files["schema.yaml"], "[item.completed, halt.requested]", "[item.completed, halt.requested, abort.requested, resume.requested]", 1)
					files["events.yaml"] += "abort.requested:\nresume.requested:\n"
					next := "dispatching"
					if mode != "stage_exit" {
						next = "cancelled"
					}
					files["nodes.yaml"] += "controller:\n  execution_type: system_node\n  event_handlers:\n    abort.requested: {advances_to: " + next + "}\n    resume.requested: {advances_to: awaiting}\n"
					source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
					collector := externalPipelineSourceNode(t, source, ".", "collector")
					controller := externalPipelineSourceNode(t, source, ".", "controller")
					nodes, err := pipeline.LoadWorkflowNodes(source)
					if err != nil {
						t.Fatal(err)
					}
					module := proposedEffectProofModule{source: source, nodes: nodes}
					probe, logger := lifecycleprobe.New(), &exactJoinRuntimeLogger{}
					gate := &a2CancellationClaimGate{gateRecoverySelectedStore: selected.events,
						ids: map[string]bool{}, entered: make(chan a2CancellationClaim, 2), release: make(chan struct{})}
					if phase == "bound_arrival_until" {
						gate.ids[lastID], gate.ids[untilID] = true, true
					} else {
						gate.typeName = "platform.join_complete"
					}
					t.Cleanup(gate.resume)
					newBus := func() *runtimebus.EventBus {
						t.Helper()
						bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
							ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
						}, "platform.join_complete", "platform.join_timeout")
						if err != nil {
							t.Fatal(err)
						}
						return bus
					}
					bus := newBus()
					schedules, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
					t.Cleanup(gate.resume)
					coordinatorStore := selected
					coordinatorStore.events = gate
					pc := newGateRecoveryCoordinator(bus, coordinatorStore, pipeline.PipelineCoordinatorOptions{
						Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
					})
					bus.SetInterceptors(pc)
					owner := testRunScopedWorkflowInstanceForRun(runID, runID)
					if _, err := pc.MaterializeInitialEntry(ctx, owner, pipeline.WorkflowInstance{
						InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
						CurrentState: "awaiting", EntityType: "count_state", Fields: map[string]any{
							"final_expected": int64(0), "final_completed": int64(0), "final_results": []any{}, "final_reason": "",
						},
					}, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
					load := func() pipeline.WorkflowInstance {
						t.Helper()
						instance, found, err := pc.Load(ctx, owner)
						if err != nil || !found {
							t.Fatalf("load actual cancellation receiver: found=%v err=%v", found, err)
						}
						return instance
					}
					event := func(id, name, raw string) events.Event {
						return eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(name), "operator", "", []byte(raw), 0,
							runID, events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
					}
					first := event(uuid.NewString(), "item.completed", `{"member_id":"a","result":{"value":"first"}}`)
					last := event(lastID, "item.completed", `{"member_id":"b","result":{"value":"last"}}`)
					until := event(untilID, "halt.requested", `{}`)
					if err := bus.PublishAcknowledged(ctx, first); err != nil {
						t.Fatal(err)
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe, first, collector.Key(), "completed", "delivered", logger)
					original := exactJoinPersistedArm(t, load())
					if original.Completed() != 1 || original.Status != joinruntime.StatusOpen {
						t.Fatalf("lawful first member baseline: %#v", original)
					}
					var completionSchedule genericschedule.Activation
					if err := bus.PublishAcknowledged(ctx, last); err != nil {
						t.Fatal(err)
					}
					wantClaims := 2
					if phase == "bound_arrival_until" {
						if err := bus.PublishAcknowledged(ctx, until); err != nil {
							t.Fatal(err)
						}
					} else {
						a2KnownTargetWaitForSettlement(t, ctx, bus, probe, last, collector.Key(), "completed", "delivered", logger)
						closed := exactJoinPersistedArm(t, load())
						if !closed.JoinRef().Equal(original.JoinRef()) || !closed.OutcomePending || closed.OutcomeFired || closed.Completed() != 2 {
							t.Fatalf("last arrival did not really persist its original pending close: %#v", closed)
						}
						completionSchedule = exactJoinPendingSchedule(t, selected, ctx, closed)
						if err := driver.Resume(ctx); err != nil {
							t.Fatal(err)
						}
						wantClaims = 1
					}
					var held []a2CancellationClaim
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					for len(held) < wantClaims {
						select {
						case claimed := <-gate.entered:
							held = append(held, claimed)
						case <-waitCtx.Done():
							cancel()
							t.Fatalf("real selected-store claims did not reach hold: held=%d logs=%s", len(held), logger.String())
						}
					}
					cancel()
					for _, work := range held {
						snapshot, err := selected.events.Snapshot(ctx, work.claim.DeliveryID())
						if err != nil || snapshot.Status != deliverylifecycle.StatusInProgress || snapshot.ClaimVersion != work.claim.Version() || snapshot.ClaimExpiresAt.IsZero() {
							t.Fatalf("held work has no exact real durable claim: snapshot=%#v claim=%#v err=%v", snapshot, work.claim, err)
						}
						publication, found, err := selected.events.LoadPreparedPublishEvent(ctx, work.event.ID())
						if err != nil || !found || len(publication.DeliveryRoutes) != 1 || !reflect.DeepEqual(publication.DeliveryRoutes[0], work.route) {
							t.Fatalf("claimed work differs from its real persisted route: found=%v err=%v", found, err)
						}
						if phase == "bound_arrival_until" {
							if receipts := work.route.Context.Joins; len(receipts) != 1 || receipts[0].Disposition != events.JoinAdmissionBound || !receipts[0].Ref.Equal(original.JoinRef()) {
								t.Fatalf("already-bound work lost E1 admission: %#v", receipts)
							}
						} else {
							var payload map[string]any
							if err := json.Unmarshal(work.event.Payload(), &payload); err != nil {
								t.Fatal(err)
							}
							_, ref, valid := timeridentity.ParseJoinHandle(payload)
							if !valid || !ref.Equal(original.JoinRef()) || work.event.TaskID() != completionSchedule.Command.TaskID {
								t.Fatalf("published continuation does not retain its exact E1 owner: %#v", payload)
							}
						}
					}
					beforeCancellation := load()
					abort := event(uuid.NewString(), "abort.requested", `{}`)
					var stop *runcontrol.Controller
					var reentry timeridentity.StageEntryRef
					if mode == "run_stop" {
						stop = runcontrol.NewController(selected.events.(runcontrol.Store), bus, runcontrol.Options{
							TimerCancellations: timercancellation.NewReconciler(schedules, nil),
						})
						beforeBusy := a2CancellationFootprint(t, selected, ctx, runID)
						claimsBeforeBusy := make([]deliverylifecycle.Snapshot, len(held))
						for i, work := range held {
							claimsBeforeBusy[i], err = selected.events.Snapshot(ctx, work.claim.DeliveryID())
							if err != nil {
								t.Fatal(err)
							}
						}
						_, err := stop.Stop(ctx, runcontrol.TransitionRequest{RunID: runID, Reason: "a2-bound-join-cancellation", ControlledBy: "component-proof"})
						failure, typed := failures.EnvelopeFromError(err)
						if !errors.Is(err, pipelineobligation.ErrBusy) || !typed || failure.Class != failures.ClassLifecycleConflict || failure.Detail.Code != "pipeline_parent_claim_busy" {
							t.Fatalf("foreground join work must retain the existing run-stop exclusion: failure=%#v typed=%v err=%v", failure, typed, err)
						}
						if !reflect.DeepEqual(beforeCancellation, load()) || a2CancellationFootprint(t, selected, ctx, runID) != beforeBusy {
							t.Fatal("busy run-stop changed receiver or durable history")
						}
						for i, work := range held {
							after, err := selected.events.Snapshot(ctx, work.claim.DeliveryID())
							outcomes, outcomeErr := selected.events.Outcomes(ctx, work.claim.DeliveryID())
							if err != nil || outcomeErr != nil || !reflect.DeepEqual(claimsBeforeBusy[i], after) || len(outcomes) != 0 {
								t.Fatalf("busy run-stop stole or settled the original foreground claim: snapshot=%#v outcomes=%#v err=%v/%v", after, outcomes, err, outcomeErr)
							}
						}
					}
					if err := bus.PublishAcknowledged(ctx, abort); err != nil {
						t.Fatal(err)
					}
					a2CancellationWaitForHandler(t, ctx, probe, selected, abort, controller.Key(), "completed", "delivered", logger)
					if mode == "stage_exit" {
						resume := event(uuid.NewString(), "resume.requested", `{}`)
						if err := bus.PublishAcknowledged(ctx, resume); err != nil {
							t.Fatal(err)
						}
						a2CancellationWaitForHandler(t, ctx, probe, selected, resume, controller.Key(), "completed", "delivered", logger)
						entry, found, err := workflowlifecycle.LoadStageEntry(load().Bookkeeping)
						if err != nil || !found || entry == original.JoinRef().StageEntry() {
							t.Fatalf("real cancellation/re-entry did not establish E2: entry=%#v found=%v err=%v", entry, found, err)
						}
						reentry = entry
					}
					retainedOriginal := 0
					for _, arm := range a2KnownTargetArms(t, load()) {
						if arm.JoinRef().Equal(original.JoinRef()) {
							retainedOriginal++
							if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonStageExit || arm.OutcomePending || arm.OutcomeFired || !arm.TimerCancelled {
								t.Fatalf("stage cancellation manufactured success or left a pending outcome: %#v", arm)
							}
						}
					}
					if retainedOriginal != 1 {
						t.Fatalf("canonical state-bucket owner must retain exactly one original closed StageExit arm: count=%d", retainedOriginal)
					}
					cancelled := load()
					gate.resume()
					for _, work := range held {
						status, handlerStatus := "dead_letter", "failed"
						if phase == "published_completion" && mode == "stage_exit" {
							status, handlerStatus = "delivered", "completed"
						}
						a2CancellationWaitForHandler(t, ctx, probe, selected, work.event, collector.Key(), handlerStatus, status, logger)
						assertExactJoinDeliveryStatus(t, selected, ctx, work.event.ID(), collector.Key(), status)
						snapshot, err := selected.events.Snapshot(ctx, work.claim.DeliveryID())
						outcomes, outcomeErr := selected.events.Outcomes(ctx, work.claim.DeliveryID())
						if err != nil || outcomeErr != nil || !snapshot.Terminal() || !snapshot.ClaimExpiresAt.IsZero() || len(outcomes) != 1 || outcomes[0].ClaimVersion != work.claim.Version() {
							t.Fatalf("cancelled work lost exact once-only claim settlement: snapshot=%#v outcomes=%#v err=%v/%v", snapshot, outcomes, err, outcomeErr)
						}
						if status == "dead_letter" && (snapshot.Failure == nil || snapshot.Failure.Class != failures.ClassStaleArrival || snapshot.Failure.Retryable) {
							t.Fatalf("already-bound late work lacks a counted typed refusal: %#v", snapshot)
						}
					}
					waitForGateRecoveryQuiescence(t, bus, ctx)
					if after := load(); !reflect.DeepEqual(cancelled, after) {
						t.Fatalf("cancelled E1 work mutated its closed state or acquired E2: revision=%d->%d state=%s->%s fields_equal=%v buckets_equal=%v bookkeeping_equal=%v histories_equal=%v", cancelled.Revision, after.Revision,
							cancelled.CurrentState, after.CurrentState, reflect.DeepEqual(cancelled.Fields, after.Fields), reflect.DeepEqual(cancelled.StateBuckets, after.StateBuckets),
							reflect.DeepEqual(cancelled.Bookkeeping, after.Bookkeeping), reflect.DeepEqual(cancelled.TransitionHistory, after.TransitionHistory))
					}
					if load().Fields["final_completed"] != int64(0) || len(load().Fields["final_results"].([]any)) != 0 {
						t.Fatal("cancelled continuation ran successful outcome effects")
					}
					if stop != nil {
						stopped, err := stop.Stop(ctx, runcontrol.TransitionRequest{RunID: runID, Reason: "a2-bound-join-cancellation", ControlledBy: "component-proof"})
						if err != nil || stopped.Status != runcontrol.StatusCancelled || stopped.AbandonedDeliveries != 0 || stopped.Recovery.Err != nil {
							t.Fatalf("legal run-stop after exact claim settlement: result=%#v err=%v", stopped, err)
						}
						if !reflect.DeepEqual(cancelled, load()) {
							t.Fatal("legal run-stop rewrote settled join receiver")
						}
					}
					if err := schedules.Stop(ctx); err != nil {
						t.Fatal(err)
					}
					probe = lifecycleprobe.New()
					bus = newBus()
					schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
					pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{
						Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
					})
					bus.SetInterceptors(pc)
					if restored, err := schedules.Restore(ctx); err != nil || restored != 0 {
						t.Fatalf("restart restored canceled or ownerless join continuation: count=%d err=%v", restored, err)
					}
					beforeReplay := a2CancellationFootprint(t, selected, ctx, runID)
					for _, input := range append([]events.Event{first, last}, until) {
						if phase == "published_completion" && input.ID() == until.ID() {
							continue
						}
						if err := bus.PublishAcknowledged(ctx, input); err != nil {
							t.Fatalf("duplicate original publication: %v", err)
						}
					}
					for _, work := range held {
						if err := bus.PublishAcknowledged(ctx, work.event); err != nil {
							t.Fatalf("duplicate original claimed work: %v", err)
						}
						retained, found, err := selected.events.LoadPreparedPublishEvent(ctx, work.event.ID())
						if err != nil || !found || len(retained.DeliveryRoutes) != 1 || !reflect.DeepEqual(retained.DeliveryRoutes[0], work.route) {
							t.Fatalf("restart rebound the exact original route/receipt: found=%v err=%v", found, err)
						}
					}
					waitForGateRecoveryQuiescence(t, bus, ctx)
					if !reflect.DeepEqual(cancelled, load()) || a2CancellationFootprint(t, selected, ctx, runID) != beforeReplay {
						t.Fatal("reconstructed duplicate publication changed receiver, settlement or history")
					}
					if mode == "stage_exit" {
						retainedReentry := 0
						for _, arm := range a2KnownTargetArms(t, load()) {
							if !arm.JoinRef().Equal(original.JoinRef()) && (arm.Status != joinruntime.StatusOpen || arm.Completed() != 0) {
								t.Fatalf("old bound work contributed to the new entry: %#v", arm)
							}
							if arm.JoinRef().StageEntry() == reentry && arm.JoinRef().Declaration().Equal(original.JoinRef().Declaration()) {
								retainedReentry++
								if arm.Status != joinruntime.StatusOpen || arm.Completed() != 0 || arm.OutcomePending || arm.OutcomeFired {
									t.Fatalf("exact captured E2 arm is not open and empty after replay: %#v", arm)
								}
							}
						}
						if retainedReentry != 1 {
							t.Fatalf("canonical state-bucket owner must retain exactly one arm for the actual E2 entry/declaration: count=%d", retainedReentry)
						}
					}
					var pending int
					if err := selected.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')`, runID).Scan(&pending); err != nil || pending != 0 {
						t.Fatalf("cancellation left runnable/ownerless delivery work: count=%d err=%v", pending, err)
					}
					t.Logf("actual %s/%s: exact real claims settle once; E1 route retained; no outcome effect, runnable continuation or restart/replay mutation", mode, phase)
				})
			}
		}
	}
}

func a2CancellationWaitForHandler(t *testing.T, ctx context.Context, probe *lifecycleprobe.Probe, selected gateRecoveryStoreCase, event events.Event, node, handlerStatus, deliveryStatus string, logger *exactJoinRuntimeLogger) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	signal, err := probe.WaitForHandlerCompleted(waitCtx, event.ID(), node)
	if err != nil || signal.Status != handlerStatus {
		t.Fatalf("actual cancellation handler: signal=%#v err=%v logs=%s", signal, err, logger.String())
	}
	assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), node, deliveryStatus)
}

func a2CancellationFootprint(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, runID string) [5]int {
	t.Helper()
	var result [5]int
	if err := selected.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)),
		(SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1),
		(SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1)`, runID).Scan(
		&result[0], &result[1], &result[2], &result[3], &result[4]); err != nil {
		t.Fatal(err)
	}
	return result
}
