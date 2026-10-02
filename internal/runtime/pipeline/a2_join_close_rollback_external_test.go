package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type a2CloseMutationObserver struct {
	pipeline.WorkflowPersistenceOwner
	results chan error
}

func (o *a2CloseMutationObserver) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	for _, schedule := range command.Lifecycle.Schedules {
		if schedule.Command.EventType == "platform.join_complete" {
			o.results <- err
			break
		}
	}
	return result, err
}

// The injected SQL fault is not classified as a retryable lifecycle conflict.
// Preserve its terminal delivery; a new lawful arrival may close the same arm.
func TestA2JoinCloseSQLFailureRollsBackAndRecoversOnBothStores(t *testing.T) {
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
			module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{
				{Node: node, Subscriptions: []events.EventType{"item.completed", "halt.requested"}, ExecutionType: contracts.SystemNodeExecutionType},
			}}
			observer := &a2CloseMutationObserver{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), results: make(chan error, 4)}
			selected.persistence = pipeline.NewWorkflowPersistence(observer)
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
					t.Fatalf("load close owner: found=%v err=%v", found, err)
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
			publish := func(member, handlerStatus, deliveryStatus string) events.Event {
				t.Helper()
				payload, err := json.Marshal(map[string]any{"member_id": member, "result": map[string]any{"value": member}})
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "", payload, 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe, event, node.Key(), handlerStatus, deliveryStatus, logger)
				return event
			}
			publish("a", "completed", "delivered")
			before := load()
			mutations := count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1")
			if arm := exactJoinPersistedArm(t, before); arm.Status != joinruntime.StatusOpen || arm.Completed() != 1 {
				t.Fatalf("fault checkpoint lacks one admitted member: %#v", arm)
			}
			fault := `CREATE TRIGGER a2_join_close_failure BEFORE INSERT ON timers WHEN NEW.fire_event='platform.join_complete' BEGIN SELECT RAISE(ABORT,'a2_join_close_sql_fault'); END`
			if selected.postgres {
				if _, err := selected.db.ExecContext(ctx, `CREATE FUNCTION a2_join_close_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.fire_event='platform.join_complete' THEN RAISE EXCEPTION 'a2_join_close_sql_fault'; END IF; RETURN NEW; END $$`); err != nil {
					t.Fatal(err)
				}
				fault = `CREATE TRIGGER a2_join_close_failure BEFORE INSERT ON timers FOR EACH ROW EXECUTE FUNCTION a2_join_close_fail()`
			}
			if _, err := selected.db.ExecContext(ctx, fault); err != nil {
				t.Fatal(err)
			}
			failed := publish("b", "failed", "dead_letter")
			select {
			case err := <-observer.results:
				if err == nil || !strings.Contains(err.Error(), "a2_join_close_sql_fault") {
					t.Fatalf("close did not reach the real SQL fault: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("no actual close-transaction result")
			}
			if after := load(); !reflect.DeepEqual(after, before) || count("SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1") != mutations ||
				count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 0 || count("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='platform.join_complete'") != 0 {
				t.Fatal("failed completion insertion leaked membership/closure/history/continuation")
			}
			var failureJSON string
			if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1", failed.ID()).Scan(&failureJSON); err != nil {
				t.Fatal(err)
			}
			var failure failures.Envelope
			if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil || failure.Class != failures.ClassInternalFailure || failure.Retryable {
				t.Fatalf("SQL fault delivery accounting changed: %s err=%v", failureJSON, err)
			}
			drop := "DROP TRIGGER a2_join_close_failure"
			if selected.postgres {
				drop += " ON timers"
			}
			if _, err := selected.db.ExecContext(ctx, drop); err != nil {
				t.Fatal(err)
			}
			if err := schedules.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			bus = newBus()
			schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
			bus.SetInterceptors(pc)
			if err := bus.PublishAcknowledged(ctx, failed); err != nil {
				t.Fatal(err)
			}
			quietCtx, cancelQuiet := context.WithTimeout(ctx, 5*time.Second)
			quietErr := bus.WaitForQuiescence(quietCtx)
			cancelQuiet()
			if quietErr != nil || !reflect.DeepEqual(load(), before) || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 0 {
				t.Fatalf("exact failed-delivery replay closed the arm: %v", quietErr)
			}
			assertExactJoinDeliveryStatus(t, selected, ctx, failed.ID(), node.Key(), "dead_letter")
			publish("b", "completed", "delivered")
			arm := exactJoinPersistedArm(t, load())
			if arm.Status != joinruntime.StatusClosed || arm.Completed() != 2 || !arm.OutcomePending || arm.OutcomeFired ||
				!arm.JoinRef().Equal(exactJoinPersistedArm(t, before).JoinRef()) || count("SELECT COUNT(*) FROM timers WHERE run_id=$1") != 1 {
				t.Fatalf("new lawful arrival did not close the original arm once: %#v", arm)
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
			if restored, err := schedules.Restore(ctx); err != nil || restored != 1 {
				t.Fatalf("restore close continuation: count=%d err=%v", restored, err)
			}
			if err := driver.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
			completionCtx, cancelCompletion := context.WithTimeout(ctx, 5*time.Second)
			completion, completionErr := probe.WaitForHandlerCompleted(completionCtx, completionID, node.Key())
			cancelCompletion()
			if completionErr != nil || completion.Status != "completed" {
				t.Fatalf("restored close completion=%#v err=%v", completion, completionErr)
			}
			assertExactJoinDeliveryStatus(t, selected, ctx, completionID, node.Key(), "delivered")
			final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
			if final.Fields["final_completed"] != int64(2) || final.Fields["final_reason"] != string(joinruntime.CloseReasonComplete) ||
				!reflect.DeepEqual(final.Fields["final_results"], []any{map[string]any{"value": "a"}, map[string]any{"value": "b"}}) || len(final.TransitionHistory) != 1 {
				t.Fatalf("recovered close lost exact business result: %#v", final)
			}
			assertExactJoinFiredSchedule(t, selected, ctx, pending, completionID)
			if count("SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='platform.join_complete'") != 1 {
				t.Fatal("rollback/recovery duplicated completion publication")
			}
		})
	}
}
