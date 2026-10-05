package pipeline_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type a2HeldDeadlineDeliveryProbe struct {
	*lifecycleprobe.Probe
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (p *a2HeldDeadlineDeliveryProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	p.Probe.NotifyLifecycle(ctx, signal)
	if signal.Kind != lifecycleprobe.DeliveryStatusChanged || signal.Status != "delivered" ||
		!strings.HasSuffix(signal.EventType, "platform.join_timeout") {
		return
	}
	p.enteredOnce.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
	}
}

func (p *a2HeldDeadlineDeliveryProbe) resume() {
	p.releaseOnce.Do(func() { close(p.release) })
}

func TestA2JoinDeadlineExecutionRetainsEntryAndPartialContextOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, explicit := range []bool{false, true} {
			name := "count"
			if explicit {
				name = "explicit_members"
			}
			t.Run(backend.name+"/"+name, func(t *testing.T) {
				selected := backend.open(t)
				runID := uuid.NewString()
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				files := a2CountJoinFiles(2)
				files["schema.yaml"] = strings.Replace(files["schema.yaml"], "[item.completed, halt.requested]", "[item.completed, halt.requested, touch]", 1)
				files["events.yaml"] += "touch:\n"
				files["entities.yaml"] += "  marker: integer\n  final_missing: \"[text]\"\n  final_timed_out: boolean\n  members_list: \"[text]\"\n"
				files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "        until: halt.requested\n", "        until: halt.requested\n        deadline: {after: 1h, from: stage_entry}\n", 1)
				files["nodes.yaml"] += `        on_deadline:
          advances_to: ready
          data_accumulation:
            writes:
              - {target_field: final_expected, value: join.expected}
              - {target_field: final_completed, value: join.completed}
              - {target_field: final_results, value: join.results}
              - {target_field: final_reason, value: join.close_reason}
              - {target_field: final_missing, value: join.missing}
              - {target_field: final_timed_out, value: join.timed_out}
    touch:
      data_accumulation:
        writes: [{target_field: marker, value: 1}]
`
				if explicit {
					files["nodes.yaml"] = strings.Replace(files["nodes.yaml"], "members: {count: 2, by: payload.member_id}", "members: {from: state.members_list, by: payload.member_id}", 1)
				}
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
				node := externalPipelineSourceNode(t, source, ".", "collector")
				module := proposedEffectProofModule{source: source, nodes: []pipeline.WorkflowNode{{Node: node,
					Subscriptions: []events.EventType{"item.completed", "halt.requested", "touch"}, ExecutionType: contracts.SystemNodeExecutionType}}}
				probe := &a2HeldDeadlineDeliveryProbe{Probe: lifecycleprobe.New(), entered: make(chan struct{}), release: make(chan struct{})}
				logger := &exactJoinRuntimeLogger{}
				bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger},
					"platform.join_complete", "platform.join_timeout")
				if err != nil {
					t.Fatal(err)
				}
				schedules, driver := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				options := pipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe}
				pc := newGateRecoveryCoordinator(bus, selected, options)
				bus.SetInterceptors(pc)
				t.Cleanup(probe.resume)
				owner := testRunScopedWorkflowInstanceForRun(runID, runID)
				// Hold only wake dispatch. The real persisted entry is already due;
				// no schedule timestamp or lifecycle status is rewritten by the test.
				entryTime := time.Now().UTC().Truncate(time.Microsecond).Add(-2 * time.Hour)
				commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, pipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "awaiting", EntityType: "count_state", Fields: map[string]any{
						"final_expected": int64(0), "final_completed": int64(0), "final_results": []any{}, "final_reason": "",
						"final_missing": []any{}, "final_timed_out": false, "marker": int64(0), "members_list": []any{"a", "z"},
					},
				}, entryTime)
				load := func() pipeline.WorkflowInstance {
					t.Helper()
					instance, found, err := pc.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("deadline receiver: found=%v err=%v", found, err)
					}
					return instance
				}
				initial := exactJoinPersistedArm(t, load())
				pending := exactJoinPendingSchedule(t, selected, ctx, initial)
				if !initial.DeadlineAt.Equal(entryTime.Add(time.Hour)) || !initial.ArmedAt.Equal(entryTime) {
					t.Fatalf("deadline not anchored to exact entry: %#v", initial)
				}
				publish := func(name string, payload []byte, handlerStatus, deliveryStatus string) events.Event {
					t.Helper()
					event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(name), "operator", "", payload, 0, runID,
						events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
					if err := bus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatal(err)
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, event, node.Key(), handlerStatus, deliveryStatus, logger)
					return event
				}
				arrival := publish("item.completed", []byte(`{"member_id":"z","result":{"value":"z"}}`), "completed", "delivered")
				publish("touch", []byte(`{}`), "completed", "delivered")
				partial := exactJoinPersistedArm(t, load())
				if !partial.JoinRef().Equal(initial.JoinRef()) || partial.Completed() != 1 || !partial.DeadlineAt.Equal(initial.DeadlineAt) ||
					!reflect.DeepEqual(exactJoinPendingSchedule(t, selected, ctx, partial), pending) {
					t.Fatal("field-only revision or arrival reset the original deadline")
				}
				pc = newGateRecoveryCoordinator(bus, selected, options)
				bus.SetInterceptors(pc)
				if !reflect.DeepEqual(exactJoinPersistedArm(t, load()), partial) {
					t.Fatal("coordinator reconstruction changed exact retained arm")
				}
				if err := driver.Resume(ctx); err != nil {
					t.Fatal(err)
				}
				deadlineID := exactJoinOccurrenceEventID(t, selected, ctx, runID, "platform.join_timeout")
				waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				signal, err := probe.WaitForHandlerCompleted(waitCtx, deadlineID, node.Key())
				cancel()
				if err != nil || signal.Status != "completed" {
					t.Fatalf("actual deadline execution: status=%s err=%v logs=%s", signal.Status, err, logger.String())
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, deadlineID, node.Key(), "delivered")
				final := waitForExactJoinState(t, ctx, pc, owner.Route, "ready")
				wantMissing := []any{}
				if explicit {
					wantMissing = []any{"a"}
				}
				if final.Fields["final_expected"] != int64(2) || final.Fields["final_completed"] != int64(1) || final.Fields["final_reason"] != "deadline" ||
					final.Fields["final_timed_out"] != true || final.Fields["marker"] != int64(1) || !reflect.DeepEqual(final.Fields["final_missing"], wantMissing) ||
					!reflect.DeepEqual(final.Fields["final_results"], []any{map[string]any{"value": "z"}}) || len(final.TransitionHistory) != 1 {
					t.Fatalf("deadline context invented contributors or lost partial results: fields=%#v history=%#v", final.Fields, final.TransitionHistory)
				}
				closed := exactJoinPersistedArm(t, final)
				if !closed.JoinRef().Equal(initial.JoinRef()) || closed.CloseReason != joinruntime.CloseReasonDeadline || !closed.OutcomeFired || closed.OutcomePending {
					t.Fatalf("deadline did not settle exactly the retained arm: %#v", closed)
				}
				assertExactJoinFiredSchedule(t, selected, ctx, pending, deadlineID)
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, deadlineID)
				if err != nil || !found {
					t.Fatalf("deadline readback: found=%v err=%v", found, err)
				}
				waitCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
				select {
				case <-probe.entered:
				case <-waitCtx.Done():
					t.Fatalf("deadline delivery did not reach held publication cut: %v", waitCtx.Err())
				}
				cancel()
				if err := bus.PublishAcknowledged(ctx, prepared.Event.Event()); !errors.Is(err, pipelineobligation.ErrBusy) {
					t.Fatalf("unsettled deadline replay did not retain exact claim refusal: %v", err)
				}
				if after := load(); !reflect.DeepEqual(after, final) {
					t.Fatal("refused overlapping replay changed the settled handler outcome")
				}
				probe.resume()
				// Handler completion and delivery status precede the enclosing
				// publication claim's settlement. Join it before exact replay.
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, prepared.Event.Event(), node.Key(), "completed", "delivered", logger)
				for _, event := range []events.Event{arrival, prepared.Event.Event()} {
					if err := bus.PublishAcknowledged(ctx, event); err != nil {
						t.Fatal(err)
					}
				}
				waitForGateRecoveryQuiescence(t, bus, ctx)
				if after := load(); !reflect.DeepEqual(after, final) {
					t.Fatal("duplicate deadline/arrival re-executed the settled outcome")
				}
				late := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "",
					[]byte(`{"member_id":"a","result":{"value":"a"}}`), 0, runID,
					events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), time.Now().UTC())
				var terminal *pipeline.TerminalReceiverError
				if err := bus.PublishAcknowledged(ctx, late); !errors.As(err, &terminal) || terminal.Stage != "ready" {
					t.Fatalf("new late publication did not retain strict terminal-owner refusal: %v", err)
				}
				var publications int
				if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE event_id=$1", late.ID()).Scan(&publications); err != nil {
					t.Fatal(err)
				}
				if after := load(); !reflect.DeepEqual(after, final) || publications != 0 {
					t.Fatal("new late publication changed terminal state or persisted an unadmitted event")
				}
			})
		}
	}
}
