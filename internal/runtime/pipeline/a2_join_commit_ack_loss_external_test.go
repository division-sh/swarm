package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

const a2JoinCloseResultLost = "a2_join_close_result_transport_lost"

type a2JoinCloseResultLossOwner struct {
	pipeline.WorkflowPersistenceOwner
	calls    atomic.Int64
	lost     atomic.Bool
	observed chan pipeline.CommittedWorkflowEngineMutation
}

func (o *a2JoinCloseResultLossOwner) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	o.calls.Add(1)
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if err == nil && result.Committed {
		for _, schedule := range command.Lifecycle.Schedules {
			if schedule.Command.EventType == "platform.join_complete" && o.lost.CompareAndSwap(false, true) {
				// The driver and selected owner really acknowledged COMMIT. Lose
				// only result transport to the adapter, never feed it this evidence.
				o.observed <- result
				return pipeline.CommittedWorkflowEngineMutation{}, failures.New(failures.ClassOutcomeUncertain,
					a2JoinCloseResultLost, "a2.join_result_transport", "receive_commit_result", nil)
			}
		}
	}
	return result, err
}

type a2JoinUnverifiedOutcome struct {
	outcome pipelineobligation.ExecutionOutcome
	err     error
}

type a2JoinResultLossInterceptor struct {
	*pipeline.PipelineCoordinator
	eventID  string
	calls    *atomic.Int64
	observed chan a2JoinUnverifiedOutcome
}

func (o *a2JoinResultLossInterceptor) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if delivery.Event().ID() == o.eventID {
		o.calls.Add(1)
	}
	pass, emitted, outcome, err := o.PipelineCoordinator.InterceptDeliveryRoute(ctx, delivery, route)
	if delivery.Event().ID() == o.eventID {
		o.observed <- a2JoinUnverifiedOutcome{outcome: outcome, err: err}
	}
	return pass, emitted, outcome, err
}

// This is successful owner-result transport loss, not a driver COMMIT fault or
// an acknowledged result accompanied by a cleanup error. All effects and
// settlement readbacks come from the real selected database.
func TestA2JoinCloseLostOwnerResultRecoversOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			loss := &a2JoinCloseResultLossOwner{
				WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner),
				observed:                 make(chan pipeline.CommittedWorkflowEngineMutation, 1),
			}
			selected.persistence = pipeline.NewWorkflowPersistence(loss)
			runID := uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2CountJoinFiles(2)))
			node := externalPipelineSourceNode(t, source, ".", "collector")
			module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
				{Node: node, Subscriptions: []events.EventType{"item.completed", "halt.requested"}, ExecutionType: contracts.SystemNodeExecutionType},
			}}
			probe := lifecycleprobe.New()
			logger := &exactJoinRuntimeLogger{}
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
					t.Fatalf("load result-loss owner: found=%v err=%v", found, err)
				}
				return instance
			}
			count := func(query string) int {
				t.Helper()
				var n int
				if err := selected.db.QueryRowContext(ctx, query, runID).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			arrival := func(member string) events.Event {
				t.Helper()
				payload, err := json.Marshal(map[string]any{"member_id": member, "result": map[string]any{"value": member}})
				if err != nil {
					t.Fatal(err)
				}
				return eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "", payload, 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
			}
			first, closing := arrival("a"), arrival("b")
			var routeCalls atomic.Int64
			observations := make(chan a2JoinUnverifiedOutcome, 4)
			install := func() {
				bus.SetInterceptors(&a2JoinResultLossInterceptor{PipelineCoordinator: pc, eventID: closing.ID(), calls: &routeCalls, observed: observations})
			}
			install()
			if err := bus.PublishAcknowledged(ctx, first); err != nil {
				t.Fatal(err)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe, first, node.Key(), "completed", "delivered", logger)
			before := load()
			initial := exactJoinPersistedArm(t, before)
			if initial.Status != joinruntime.StatusOpen || initial.Completed() != 1 || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 0 {
				t.Fatalf("result-loss checkpoint is not an open one-member arm: %#v", initial)
			}
			mutations := count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1")
			if err := bus.PublishAcknowledged(ctx, closing); err != nil {
				t.Fatal(err)
			}
			var deliveryID string
			select {
			case committed := <-loss.observed:
				if !committed.Committed || committed.DeliverySuccess == nil || len(committed.Lifecycle.GenericScheduleActivations) != 1 {
					t.Fatalf("injection did not discard a real acknowledged atomic closure: %#v", committed)
				}
				deliveryID = committed.DeliverySuccess.DeliveryID()
			case <-time.After(10 * time.Second):
				t.Fatal("no real successful close result to lose")
			}
			select {
			case observed := <-observations:
				disposition, present := observed.outcome.Disposition()
				failure := disposition.Failure()
				if observed.outcome.Committed || !present || disposition.Successful() || failure == nil ||
					failure.Class != failures.ClassOutcomeUncertain || failure.Detail.Code != a2JoinCloseResultLost || failure.Retryable || failure.Deterministic {
					t.Errorf("unverified close was fabricated as success or lost typed uncertainty: outcome=%#v failure=%#v err=%v", observed.outcome, failure, observed.err)
				}
				t.Logf("unverified close accounting: disposition=%s failure=%#v err=%v", disposition.Kind(), failure, observed.err)
			case <-time.After(10 * time.Second):
				t.Fatal("no actual coordinator result for lost close acknowledgement")
			}
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			completion, err := probe.WaitForHandlerCompleted(waitCtx, closing.ID(), node.Key())
			if err == nil {
				err = bus.WaitForQuiescence(waitCtx)
			}
			cancel()
			if err != nil || completion.Status != "failed" {
				t.Fatalf("unverified handler outcome=%#v err=%v", completion, err)
			}
			closed := load()
			arm := exactJoinPersistedArm(t, closed)
			wantResults := []any{map[string]any{"value": "a"}, map[string]any{"value": "b"}}
			results, err := arm.Results()
			if err != nil || !reflect.DeepEqual(results, wantResults) || !arm.JoinRef().Equal(initial.JoinRef()) ||
				arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonComplete || !arm.OutcomePending || arm.OutcomeFired || arm.Completed() != 2 ||
				closed.CurrentState != "awaiting" || len(closed.TransitionHistory) != 0 || !reflect.DeepEqual(closed.Fields, before.Fields) ||
				count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1") != mutations+1 || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 1 ||
				count("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='platform.join_complete'") != 0 {
				t.Fatalf("ack loss lost atomic member/closed_pending/schedule or fabricated effects: arm=%#v state=%#v err=%v", arm, closed, err)
			}
			pending := exactJoinPendingSchedule(t, selected, ctx, arm)
			deliveries := selected.events.(deliverylifecycle.Store)
			deliveryBefore, err := deliveries.Snapshot(ctx, deliveryID)
			if err != nil || deliveryBefore.EventID != closing.ID() || deliveryBefore.SubscriberID != node.Key() ||
				deliveryBefore.Status != deliverylifecycle.StatusDelivered || deliveryBefore.Failure != nil || deliveryBefore.RetryCount != 0 {
				t.Fatalf("unverified local result rewrote real successful settlement: %#v err=%v", deliveryBefore, err)
			}
			outcomesBefore, err := deliveries.Outcomes(ctx, deliveryID)
			if err != nil || len(outcomesBefore) != 1 || outcomesBefore[0].Outcome != "delivered" || outcomesBefore[0].Failure != nil ||
				outcomesBefore[0].ClaimVersion != deliveryBefore.ClaimVersion || outcomesBefore[0].SettledAt.IsZero() ||
				!reflect.DeepEqual(outcomesBefore[0].SideEffects, []string{"handler_completed"}) || !deliveryBefore.FinalSelection.Present() {
				t.Fatalf("lost result changed acknowledged attempt/selection/effect evidence: outcomes=%#v snapshot=%#v err=%v", outcomesBefore, deliveryBefore, err)
			}
			if err := schedules.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			probe = lifecycleprobe.New()
			bus = newBus()
			var driver *exactJoinScheduleDriver
			schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
			install()
			callsBefore := loss.calls.Load()
			if err := bus.PublishAcknowledged(ctx, closing); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			deliveryAfter, deliveryErr := deliveries.Snapshot(ctx, deliveryID)
			outcomesAfter, outcomeErr := deliveries.Outcomes(ctx, deliveryID)
			if err != nil || deliveryErr != nil || outcomeErr != nil || routeCalls.Load() != 1 || loss.calls.Load() != callsBefore ||
				!reflect.DeepEqual(load(), closed) || !reflect.DeepEqual(deliveryAfter, deliveryBefore) || !reflect.DeepEqual(outcomesAfter, outcomesBefore) ||
				count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1") != mutations+1 || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 1 {
				t.Fatalf("exact input re-executed/rewrote committed close after reconstruction: quiet=%v delivery=%v outcomes=%v route_calls=%d", err, deliveryErr, outcomeErr, routeCalls.Load())
			}
			assertExactJoinDeliveryCount(t, selected, ctx, closing.ID(), node.Key(), 1)
			if restored, err := schedules.Restore(ctx); err != nil || restored != 1 {
				t.Fatalf("restore result-loss continuation: count=%d err=%v", restored, err)
			}
			if err := driver.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
			waitCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
			completion, err = probe.WaitForHandlerCompleted(waitCtx, completionID, node.Key())
			if err == nil {
				err = bus.WaitForQuiescence(waitCtx)
			}
			cancel()
			if err != nil || completion.Status != "completed" {
				t.Fatalf("actual restored continuation=%#v err=%v", completion, err)
			}
			final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
			fired := exactJoinPersistedArm(t, final)
			// Eight continuation diffs: four fields, stage, stage-entry and
			// accumulation bookkeeping, and the settled join bucket.
			var continuationDiffs int
			if err := selected.db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2", runID, completionID).Scan(&continuationDiffs); err != nil {
				t.Fatal(err)
			}
			if final.Fields["final_expected"] != int64(2) || final.Fields["final_completed"] != int64(2) || final.Fields["final_reason"] != string(joinruntime.CloseReasonComplete) ||
				!reflect.DeepEqual(final.Fields["final_results"], wantResults) || len(final.TransitionHistory) != 1 || !fired.OutcomeFired || fired.OutcomePending ||
				!fired.JoinRef().Equal(arm.JoinRef()) || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 1 ||
				count("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='platform.join_complete'") != 1 ||
				count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1") != mutations+1+8 || continuationDiffs != 8 || routeCalls.Load() != 1 {
				t.Fatalf("lost result duplicated/lost restored business effect: mutations=%d baseline=%d route_calls=%d state=%#v arm=%#v",
					count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1"), mutations, routeCalls.Load(), final, fired)
			}
			assertExactJoinFiredSchedule(t, selected, ctx, pending, completionID)
			assertExactJoinDeliveryStatus(t, selected, ctx, completionID, node.Key(), "delivered")
			assertExactJoinDeliveryCount(t, selected, ctx, completionID, node.Key(), 1)
		})
	}
}
