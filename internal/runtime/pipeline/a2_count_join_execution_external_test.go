package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

var a2AcknowledgedCloseCleanupFailure = errors.New("a2 acknowledged close cleanup failure")

type a2AcknowledgedCloseCleanupObserver struct {
	pipeline.WorkflowPersistenceOwner
	once     sync.Once
	injected chan error
}

func (o *a2AcknowledgedCloseCleanupObserver) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	result, err := o.WorkflowPersistenceOwner.CommitWorkflowEngineMutation(ctx, command)
	if err == nil && result.Committed {
		for _, schedule := range command.Lifecycle.Schedules {
			if schedule.Command.EventType == "platform.join_complete" {
				// The selected store has actually committed and acknowledged. Only
				// the cleanup error at this public result boundary is injected.
				o.once.Do(func() {
					err = a2AcknowledgedCloseCleanupFailure
					o.injected <- err
				})
				break
			}
		}
	}
	return result, err
}

func TestA2CountJoinRealExecutionAndRestartOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, scenario := range []struct {
			name    string
			count   int
			members []string
			until   bool
			cleanup bool
		}{
			{"zero", 0, nil, false, false},
			{"one", 1, []string{"z"}, false, false},
			{"reverse_and_duplicates", 2, []string{"z", "a"}, false, false},
			{"exact_whitespace_keys", 2, []string{"a", " a"}, false, false},
			{"partial_until", 2, []string{"z"}, true, false},
			{"acknowledged_cleanup_failure", 2, []string{"z", "a"}, false, true},
		} {
			t.Run(backend.name+"/"+scenario.name, func(t *testing.T) {
				selected := backend.open(t)
				var cleanup *a2AcknowledgedCloseCleanupObserver
				if scenario.cleanup {
					cleanup = &a2AcknowledgedCloseCleanupObserver{WorkflowPersistenceOwner: selected.events.(pipeline.WorkflowPersistenceOwner), injected: make(chan error, 1)}
					selected.persistence = pipeline.NewWorkflowPersistence(cleanup)
				}
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2CountJoinFiles(scenario.count)))
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
						t.Fatalf("count receiver load: found=%v err=%v", found, err)
					}
					return instance
				}
				// Reconstruct runtime owners at exact durable checkpoints. This
				// is component recovery, not a claim of a process/driver crash.
				recoverCheckpoint := func(name string, wantPending int) {
					t.Helper()
					quiet, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := bus.WaitForQuiescence(quiet); err != nil {
						cancel()
						t.Fatalf("%s quiescence: %v", name, err)
					}
					cancel()
					before := load()
					var eventsBefore int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE run_id=$1", runID).Scan(&eventsBefore); err != nil {
						t.Fatal(err)
					}
					if err := schedules.Stop(ctx); err != nil {
						t.Fatal(err)
					}
					probe = lifecycleprobe.New()
					bus = newBus()
					schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
					pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
					bus.SetInterceptors(pc)
					if restored, err := schedules.Restore(ctx); err != nil || restored != wantPending {
						t.Fatalf("%s restore: count=%d want=%d err=%v", name, restored, wantPending, err)
					}
					var eventsAfter int
					if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE run_id=$1", runID).Scan(&eventsAfter); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(load(), before) || eventsAfter != eventsBefore {
						t.Fatalf("%s reconstruction changed retained state or published work", name)
					}
				}
				initial := exactJoinPersistedArm(t, load())
				if initial.MemberCount == nil || *initial.MemberCount != scenario.count || len(initial.Members) != 0 || !initial.DeadlineAt.IsZero() {
					t.Fatalf("count invented members/deadline: %#v", initial)
				}
				initialPending := 0
				if scenario.count == 0 {
					initialPending = 1
				}
				recoverCheckpoint("after_arm", initialPending)
				publish := func(name, member, result string, want failures.Class) events.Event {
					t.Helper()
					payload := []byte(`{}`)
					if name == "item.completed" {
						var err error
						payload, err = json.Marshal(map[string]any{"member_id": member, "result": map[string]any{"value": result}})
						if err != nil {
							t.Fatal(err)
						}
					}
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", payload, 0, runID,
						events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
					if err := bus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatal(err)
					}
					handlerStatus, deliveryStatus := "completed", "delivered"
					if want != "" {
						handlerStatus, deliveryStatus = "failed", "dead_letter"
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe, event, node.Key(), handlerStatus, deliveryStatus, logger)
					if want != "" {
						var raw string
						if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1", event.ID()).Scan(&raw); err != nil {
							t.Fatal(err)
						}
						var failure failures.Envelope
						if err := json.Unmarshal([]byte(raw), &failure); err != nil || failure.Class != want {
							t.Fatalf("count refusal=%s want=%s err=%v", raw, want, err)
						}
					}
					return event
				}
				var first events.Event
				for index, member := range scenario.members {
					arrival := publish("item.completed", member, member, "")
					if index == 0 {
						first = arrival
					}
					if index == 0 && scenario.name == "reverse_and_duplicates" {
						recoverCheckpoint("after_partial_member", 0)
						publish("item.completed", member, member, "")
						if arm := exactJoinPersistedArm(t, load()); arm.Completed() != 1 {
							t.Fatal("distinct duplicate delivery counted twice")
						}
						before := load()
						publish("item.completed", member, "changed", failures.ClassConflictingDuplicate)
						if after := load(); after.Revision != before.Revision || !reflect.DeepEqual(after.StateBuckets, before.StateBuckets) {
							t.Fatal("conflicting contributor changed persisted count/results")
						}
					}
				}
				if scenario.until {
					publish("halt.requested", "", "", "")
				}
				if cleanup != nil {
					select {
					case err := <-cleanup.injected:
						if !errors.Is(err, a2AcknowledgedCloseCleanupFailure) {
							t.Fatalf("wrong acknowledged cleanup fault: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("no actual acknowledged close to inject cleanup failure")
					}
				}
				closed := load()
				arm := exactJoinPersistedArm(t, closed)
				wantReason := joinruntime.CloseReasonComplete
				if scenario.until {
					wantReason = joinruntime.CloseReasonUntil
				}
				if arm.Status != joinruntime.StatusClosed || arm.CloseReason != wantReason || !arm.OutcomePending || arm.OutcomeFired ||
					arm.Completed() != len(scenario.members) || closed.CurrentState != "awaiting" || len(closed.Fields["final_results"].([]any)) != 0 {
					t.Fatalf("count did not retain truthful pending closure: arm=%#v fields=%#v", arm, closed.Fields)
				}
				pending := exactJoinPendingSchedule(t, selected, ctx, arm)
				beforeLate := load()
				publish("item.completed", "extra", "late", failures.ClassStaleArrival)
				if after := load(); after.Revision != beforeLate.Revision || !reflect.DeepEqual(after.StateBuckets, beforeLate.StateBuckets) {
					t.Fatal("late contributor changed the closed count join")
				}
				if err := schedules.Stop(ctx); err != nil {
					t.Fatal(err)
				}
				bus = newBus()
				var driver *exactJoinScheduleDriver
				schedules, driver = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc = newGateRecoveryCoordinator(bus, selected, pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe})
				bus.SetInterceptors(pc)
				if restored, err := schedules.Restore(ctx); err != nil || restored != 1 {
					t.Fatalf("restore exact count continuation: count=%d err=%v", restored, err)
				}
				if err := driver.Resume(ctx); err != nil {
					t.Fatal(err)
				}
				completionID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_complete")
				completionCtx, cancelCompletion := context.WithTimeout(ctx, 5*time.Second)
				completion, completionErr := probe.WaitForHandlerCompleted(completionCtx, completionID, node.Key())
				cancelCompletion()
				if completionErr != nil || completion.Status != "completed" {
					t.Fatalf("count continuation completion=%#v err=%v", completion, completionErr)
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, completionID, node.Key(), "delivered")
				final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
				orderedMembers := append([]string(nil), scenario.members...)
				sort.Strings(orderedMembers)
				wantResults := make([]any, 0, len(orderedMembers))
				for _, member := range orderedMembers {
					wantResults = append(wantResults, map[string]any{"value": member})
				}
				results, err := arm.Results()
				if err != nil || !reflect.DeepEqual(results, wantResults) || !reflect.DeepEqual(final.Fields["final_results"], wantResults) || final.Fields["final_expected"] != int64(scenario.count) ||
					final.Fields["final_completed"] != int64(len(scenario.members)) || final.Fields["final_reason"] != string(wantReason) || len(final.TransitionHistory) != 1 {
					t.Fatalf("count outcome lost order/count/reason: fields=%#v history=%#v err=%v", final.Fields, final.TransitionHistory, err)
				}
				fired := exactJoinPersistedArm(t, final)
				if !fired.OutcomeFired || fired.OutcomePending || !fired.JoinRef().Equal(arm.JoinRef()) {
					t.Fatal("restart did not settle the original count continuation")
				}
				assertExactJoinFiredSchedule(t, selected, ctx, pending, completionID)
				recoverCheckpoint("after_outcome_acknowledgement", 0)
				if first.ID() != "" {
					if err := bus.PublishAcknowledged(ctx, first); err != nil {
						t.Fatal(err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := bus.WaitForQuiescence(waitCtx); err != nil {
						cancel()
						t.Fatal(err)
					}
					cancel()
					assertExactJoinDeliveryCount(t, selected, ctx, first.ID(), node.Key(), 1)
					if after := load(); after.Revision != final.Revision || !reflect.DeepEqual(after.StateBuckets, final.StateBuckets) {
						t.Fatal("exact duplicate replay after completion changed count outcome")
					}
				}
			})
		}
	}
}

func a2CountJoinFiles(count int) map[string]string {
	return map[string]string{
		"schema.yaml":   "name: a2-count-join\nstages:\n  awaiting: {initial: true}\n  ready: {terminal: true}\npins:\n  inputs:\n    - item.completed\n    - halt.requested\n",
		"entities.yaml": "count_state:\n  final_expected: integer\n  final_completed: integer\n  final_results: \"[JoinResult]\"\n  final_reason: text\n",
		"types.yaml":    "types:\n  JoinResult:\n    value: text\n",
		"events.yaml":   "item.completed:\n  member_id: text\n  result: JoinResult\nhalt.requested:\n",
		"nodes.yaml": fmt.Sprintf(`collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {count: %d, by: payload.member_id}
        output: payload.result
        until: halt.requested
        on_complete:
          advances_to: ready
          data_accumulation:
            writes:
              - {target_field: final_expected, value: join.expected}
              - {target_field: final_completed, value: join.completed}
              - {target_field: final_results, value: join.results}
              - {target_field: final_reason, value: join.close_reason}
`, count),
	}
}
