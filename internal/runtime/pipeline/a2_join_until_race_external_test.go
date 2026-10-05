package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type a2JoinExecutionRaceGate struct {
	*pipeline.PipelineCoordinator
	events  map[string]bool
	entered chan string
	release chan struct{}
	once    sync.Once
}

func (g *a2JoinExecutionRaceGate) resume() { g.once.Do(func() { close(g.release) }) }

func (g *a2JoinExecutionRaceGate) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if g.events[delivery.Event().ID()] {
		g.entered <- delivery.Event().ID()
		select {
		case <-g.release:
		case <-ctx.Done():
			return false, nil, pipelineobligation.ExecutionOutcome{}, ctx.Err()
		}
	}
	return g.PipelineCoordinator.InterceptDeliveryRoute(ctx, delivery, route)
}

func TestA2UntilAndFinalArrivalHaveOneDurableCloseWinnerOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2CountJoinFiles(2)))
			node := externalPipelineSourceNode(t, source, ".", "collector")
			module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{{Node: node,
				Subscriptions: []events.EventType{"item.completed", "halt.requested"}, ExecutionType: contracts.SystemNodeExecutionType}}}
			probe, logger := lifecycleprobe.New(), &exactJoinRuntimeLogger{}
			newBus := func() *runtimebus.EventBus {
				t.Helper()
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger},
					"platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				return bus
			}
			bus := newBus()
			schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc := newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
			bus.SetInterceptors(pc)
			owner := testRunScopedWorkflowInstanceForRun(runID, runID)
			commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, pipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				CurrentState: "awaiting", EntityType: "count_state", Fields: map[string]any{
					"final_expected": int64(0), "final_completed": int64(0), "final_results": []any{}, "final_reason": "",
				},
			}, time.Now().UTC())
			load := func() pipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("race receiver load: found=%v err=%v", found, err)
				}
				return instance
			}
			event := func(name string, payload []byte) events.Event {
				return eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", payload, 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
			}
			first := event("item.completed", []byte(`{"member_id":"a","result":{"value":"a"}}`))
			if err := bus.PublishAcknowledged(ctx, first); err != nil {
				t.Fatal(err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, first, node.Key(), "completed", "delivered", logger)
			initial := exactJoinPersistedArm(t, load())
			last := event("item.completed", []byte(`{"member_id":"b","result":{"value":"b"}}`))
			until := event("halt.requested", []byte(`{}`))
			gate := &a2JoinExecutionRaceGate{PipelineCoordinator: pc, events: map[string]bool{last.ID(): true, until.ID(): true},
				entered: make(chan string, 2), release: make(chan struct{})}
			t.Cleanup(gate.resume)
			bus.SetInterceptors(gate)
			// Hold execution, not publication or a database write. Both real
			// deliveries retain the same arm before they contend for its owner.
			for _, input := range []events.Event{last, until} {
				if err := bus.PublishAcknowledged(ctx, input); err != nil {
					t.Fatal(err)
				}
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			seen := map[string]bool{}
			for len(seen) != 2 {
				select {
				case id := <-gate.entered:
					seen[id] = true
				case <-waitCtx.Done():
					cancel()
					t.Fatal("both durable delivery claims did not reach the race gate")
				}
			}
			cancel()
			for _, input := range []events.Event{last, until} {
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, input.ID())
				if err != nil || !found || len(prepared.DeliveryRoutes) != 1 || len(prepared.DeliveryRoutes[0].Context.Joins) != 1 ||
					!prepared.DeliveryRoutes[0].Context.Joins[0].Ref.Equal(initial.JoinRef()) {
					t.Fatalf("race input lacks immutable original arm: found=%v err=%v routes=%#v", found, err, prepared.DeliveryRoutes)
				}
			}
			gate.resume()
			winners := 0
			for _, input := range []events.Event{last, until} {
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				signal, err := probe.WaitForHandlerCompleted(waitCtx, input.ID(), node.Key())
				cancel()
				if err != nil {
					t.Fatalf("race input never completed: %v logs=%s", err, logger.String())
				}
				status := "delivered"
				if signal.Status == "completed" {
					winners++
				} else if signal.Status == "failed" {
					status = "dead_letter"
				} else {
					t.Fatalf("unexpected race outcome %s", signal.Status)
				}
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe, input, node.Key(), signal.Status, status, logger)
				if status == "dead_letter" {
					var raw string
					if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1", input.ID()).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var failure failures.Envelope
					if err := json.Unmarshal([]byte(raw), &failure); err != nil || failure.Class != failures.ClassStaleArrival {
						t.Fatalf("race loser lacks counted late refusal: %s err=%v", raw, err)
					}
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, input.ID(), node.Key(), status)
			}
			closed := load()
			arm := exactJoinPersistedArm(t, closed)
			wantCompleted := 1
			if arm.CloseReason == joinruntime.CloseReasonComplete {
				wantCompleted = 2
			} else if arm.CloseReason != joinruntime.CloseReasonUntil {
				t.Fatalf("unexpected close winner: %#v", arm)
			}
			if winners != 1 || arm.Status != joinruntime.StatusClosed || !arm.OutcomePending || arm.OutcomeFired ||
				!arm.JoinRef().Equal(initial.JoinRef()) || arm.Completed() != wantCompleted || closed.CurrentState != "awaiting" {
				t.Fatalf("race lost atomic single closure: winners=%d arm=%#v", winners, arm)
			}
			pending := exactJoinPendingSchedule(t, selected, ctx, arm)
			if err := schedules.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			bus = newBus()
			var driver *exactJoinScheduleDriver
			schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
			bus.SetInterceptors(pc)
			if n, err := schedules.Restore(ctx); err != nil || n != 1 {
				t.Fatalf("race restoration duplicates or loses closure: n=%d err=%v", n, err)
			}
			if err := driver.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
			completion, found, err := selected.events.LoadPreparedPublishEvent(ctx, completionID)
			if err != nil || !found {
				t.Fatalf("race completion readback: found=%v err=%v", found, err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, completion.Event.Event(), node.Key(), "completed", "delivered", logger)
			final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
			results, err := arm.Results()
			if err != nil || !reflect.DeepEqual(final.Fields["final_results"], results) || final.Fields["final_expected"] != int64(2) ||
				final.Fields["final_completed"] != int64(wantCompleted) || final.Fields["final_reason"] != string(arm.CloseReason) || len(final.TransitionHistory) != 1 {
				t.Fatalf("race continuation loses winner facts: fields=%#v err=%v", final.Fields, err)
			}
			assertExactJoinFiredSchedule(t, selected, ctx, pending, completionID)
			for _, input := range []events.Event{first, last, until, completion.Event.Event()} {
				if err := bus.PublishAcknowledged(ctx, input); err != nil {
					t.Fatal(err)
				}
			}
			waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			if err := bus.WaitForQuiescence(waitCtx); err != nil {
				cancel()
				t.Fatal(err)
			}
			cancel()
			if !reflect.DeepEqual(final, load()) {
				t.Fatal("race duplicate/restart replay changed the settled entry")
			}
		})
	}
}
