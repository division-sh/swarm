package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

type review2460ScheduleOwner struct {
	seen    []string
	fault   error
	panicAt int
}

func (o *review2460ScheduleOwner) ReconcileWakeupWithRecovery(_ context.Context, id string) (bool, error) {
	o.seen = append(o.seen, id)
	if len(o.seen) == o.panicAt {
		panic("first committed schedule reconciliation panicked")
	}
	if len(o.seen) == 1 {
		return true, o.fault
	}
	return false, nil
}

func TestCommittedLifecycleAttemptsCancellationAndActivationAfterPanic(t *testing.T) {
	now := time.Now().UTC()
	due := now.Add(time.Hour)
	var activations []genericschedule.Activation
	for i := 0; i < 2; i++ {
		key := uuid.NewString()
		command := genericschedule.AdmissionCommand{
			ScheduleKey: key, OwnerKind: genericschedule.OwnerSystem, OwnerID: "runtime",
			EventType: "platform.generic_schedule_proof", Payload: semanticvalue.EmptyObject(),
			RoutingSource: events.NewPlatformControlRoutingSource(), ExecutionMode: executionmode.Live,
			Due: genericschedule.AbsoluteDue(due), TaskID: key,
		}
		hash, err := command.ImmutableHash()
		if err != nil {
			t.Fatal(err)
		}
		activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
			AdmittedAt: now, InitialDueAt: due, CurrentDueAt: due, Status: genericschedule.StatusActive}
		if i == 0 {
			activation.Status = genericschedule.StatusCancelled
			activation.CancelCause = "test_cancelled"
			activation.CancelledAt = now
		}
		if err := activation.Validate(); err != nil {
			t.Fatal(err)
		}
		activations = append(activations, activation)
	}
	schedules := &review2460ScheduleOwner{panicAt: 1}
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed: true, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true,
			GenericScheduleCancellations: activations[:1], GenericScheduleActivations: activations[1:]},
	}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner},
		state: pipelineEngineStateRepo{coordinator: &PipelineCoordinator{genericSchedules: schedules}}}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, nil)
	if !result.Committed || err == nil {
		t.Fatalf("acknowledged result=%+v error=%v", result, err)
	}
	if len(schedules.seen) != 2 || schedules.seen[0] != activations[0].ID || schedules.seen[1] != activations[1].ID {
		t.Fatalf("committed cancellation and sibling activation attempts=%v", schedules.seen)
	}
}

func TestCommittedLifecycleGenericFailureStillReconcilesWorkflowTimerBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.open(t)
			pc, _, timer := seedWorkflowTimerOwnerActivationWithDelay(t, store, ctx, &recordingPipelineBus{}, false, "1h")
			var timerReads atomic.Int32
			pc.workflowTimers.testAfterWakeupLoad = func() { timerReads.Add(1) }
			pc.genericSchedules = &review2460ScheduleOwner{panicAt: 1}

			now := time.Now().UTC()
			key := uuid.NewString()
			command := genericschedule.AdmissionCommand{
				ScheduleKey: key, OwnerKind: genericschedule.OwnerSystem, OwnerID: "runtime",
				EventType: "platform.generic_schedule_proof", Payload: semanticvalue.EmptyObject(),
				RoutingSource: events.NewPlatformControlRoutingSource(), ExecutionMode: executionmode.Live,
				Due: genericschedule.AbsoluteDue(now.Add(time.Hour)), TaskID: key,
			}
			hash, err := command.ImmutableHash()
			if err != nil {
				t.Fatal(err)
			}
			cancelled := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
				AdmittedAt: now, InitialDueAt: now.Add(time.Hour), CurrentDueAt: now.Add(time.Hour),
				Status: genericschedule.StatusCancelled, CancelCause: "test_cancelled", CancelledAt: now}
			if err := cancelled.Validate(); err != nil {
				t.Fatal(err)
			}
			committed := CommittedWorkflowLifecycleMutation{Committed: true,
				GenericScheduleCancellations: []genericschedule.Activation{cancelled}, Wakeups: []timeridentity.WorkflowTimerActivationRef{timer.Ref}}
			if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed); err == nil {
				t.Fatal("generic schedule panic was not reported")
			}
			if timerReads.Load() == 0 {
				t.Fatal("committed workflow timer was skipped after independent generic schedule panic")
			}
		})
	}
}

type review2460HandlerCompletedProbe struct{}

func (review2460HandlerCompletedProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.HandlerCompleted {
		panic("handler-completed notification failed")
	}
}

type review2460FailedStatusProbe struct{}

func (review2460FailedStatusProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind == lifecycleprobe.DeliveryStatusChanged && signal.Status == string(deliverylifecycle.StatusFailed) {
		panic("failed status notification interrupted retry retention")
	}
}

func TestCommittedAttemptFailureNotificationKeepsRetryContinuationBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.open(t)
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: retry-cleanup\ninitial_state: queued\nstates: [queued, done]\nterminal_states: [done]\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "source.evt: {}\nsource.done: {}\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n      emit: {event: source.done}\n",
			})
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			module.workflowNodes = []WorkflowNode{{Node: pipelineNode(t, ".", "node-a"),
				Subscriptions: []events.EventType{"source.evt"}, Policies: map[string]WorkflowEventPolicy{"source.evt": {Consume: true}}}}
			bus := &recordingPipelineBus{}
			pc := newPostgresPipelineCoordinatorForTest(bus, store.testDB(), PipelineCoordinatorOptions{
				Module: module, DeliveryStore: newPipelineTestDeliveryOwnerForDB(t, store.testDB()), TestLifecycleProbe: review2460FailedStatusProbe{},
			})
			pc.workflowStore = store
			pc.testWorkflowNodeHandlerStartHook = func(context.Context, string, events.Event) error {
				return runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "retry_owner_unavailable", "test", "prepare_handler", nil)
			}
			owner := configurePipelineTestDeliveryOwner(t, pc)
			runID, entityID := correlation.RunIDFromContext(ctx), uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "v-test",
				CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
			if err := owner.commitInitial(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			_, err = pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err == nil {
				t.Fatal("missing notification diagnostic")
			}
			snapshot, err := owner.Snapshot(ctx, deliveryID)
			if err != nil || snapshot.Status != deliverylifecycle.StatusFailed {
				t.Fatalf("failed delivery snapshot=%+v error=%v", snapshot, err)
			}
			bus.deliveryContinuations.mu.Lock()
			held, exists := bus.deliveryContinuations.held[deliveryID]
			bus.deliveryContinuations.mu.Unlock()
			if !exists || !held {
				t.Fatalf("retry continuation not retained after diagnostic: exists=%t held=%t", exists, held)
			}
		})
	}
}

func TestReview2460HandlerCompletedPanicReleasesCommittedContinuationBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			store, ctx := backend.open(t)
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: committed-cleanup\ninitial_state: queued\nstates: [queued, done]\nterminal_states: [done]\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "source.evt: {}\nsource.done: {}\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n      emit: {event: source.done}\n",
			})
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			module.workflowNodes = []WorkflowNode{{Node: pipelineNode(t, ".", "node-a"),
				Subscriptions: []events.EventType{"source.evt"}, Policies: map[string]WorkflowEventPolicy{"source.evt": {Consume: true}}}}
			bus := &recordingPipelineBus{}
			pc := newPostgresPipelineCoordinatorForTest(bus, store.testDB(), PipelineCoordinatorOptions{
				Module: module, DeliveryStore: newPipelineTestDeliveryOwnerForDB(t, store.testDB()), TestLifecycleProbe: review2460HandlerCompletedProbe{},
			})
			pc.workflowStore = store
			owner := configurePipelineTestDeliveryOwner(t, pc)
			runID, entityID := correlation.RunIDFromContext(ctx), uuid.NewString()
			evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, "", handlerTestWorkflowEnvelope(".", runID, entityID), time.Now().UTC())
			dialect := authoractivityfixture.DialectPostgres
			if store.isSQLite() {
				dialect = authoractivityfixture.DialectSQLite
			}
			seedPipelineEventRecordForDialect(t, ctx, store.testDB(), dialect, evt)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: "v-test",
				CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
			if err := owner.commitInitial(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			var callErr error
			func() {
				defer func() {
					if value := recover(); value != nil {
						callErr = fmt.Errorf("%v", value)
					}
				}()
				_, callErr = pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			}()
			if callErr == nil {
				t.Fatal("missing fault")
			}
			assertCommittedHandlerCleanupRows(t, ctx, store, owner, bus, runID, entityID, deliveryID)
			bus.deliveryContinuations.mu.Lock()
			held, exists := bus.deliveryContinuations.held[deliveryID]
			bus.deliveryContinuations.mu.Unlock()
			if exists {
				t.Fatalf("committed continuation still exists (held=%t); missing and consumed=false are not equivalent", held)
			}
		})
	}
}

func TestReview2460LifecycleContinuesIndependentCommittedSchedules(t *testing.T) {
	now := time.Now().UTC()
	due := now.Add(time.Hour)
	var activations []genericschedule.Activation
	for i := 0; i < 2; i++ {
		key := uuid.NewString()
		command := genericschedule.AdmissionCommand{
			ScheduleKey: key, OwnerKind: genericschedule.OwnerSystem, OwnerID: "runtime",
			EventType: "platform.generic_schedule_proof", Payload: semanticvalue.EmptyObject(),
			RoutingSource: events.NewPlatformControlRoutingSource(), ExecutionMode: executionmode.Live,
			Due: genericschedule.AbsoluteDue(due), TaskID: key,
		}
		hash, err := command.ImmutableHash()
		if err != nil {
			t.Fatal(err)
		}
		activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
			AdmittedAt: now, InitialDueAt: due, CurrentDueAt: due, Status: genericschedule.StatusActive}
		if err := activation.Validate(); err != nil {
			t.Fatal(err)
		}
		activations = append(activations, activation)
	}
	fault := errors.New("first schedule enqueued recovery after read failure")
	schedules := &review2460ScheduleOwner{fault: fault}
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed: true, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true, GenericScheduleActivations: activations},
	}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner},
		state: pipelineEngineStateRepo{coordinator: &PipelineCoordinator{genericSchedules: schedules}}}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, nil)
	if !result.Committed || !errors.Is(err, fault) {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if len(schedules.seen) != 2 {
		t.Fatalf("attempted %d schedules, want 2; the second committed schedule has no wakeup or recovery attempt", len(schedules.seen))
	}
}
