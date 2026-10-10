package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func VerifyNativeHandlerCommittedCleanupErrorRetainsExactOutcomeBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: committed-cleanup\nstages:\n  queued: {}\n  done: {final: true}\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "source.evt:\nsource.done:\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      advances_to: done\n      emit: {event: source.done}\n",
			})
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			module.workflowNodes = []WorkflowNode{{
				Node: pipelineNode(t, ".", "node-a"), Subscriptions: []events.EventType{"source.evt"},
			}}
			fixture := open(t, backend, semanticview.Wrap(bundle))
			pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			runID := uuid.NewString()
			ctx = correlation.WithRunID(ctx, runID)
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			entityID := runID
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, events.EnvelopeForTargetRoute(handlerTestWorkflowEnvelope(".", runID, entityID), events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}), testWorkflowRoutingSource(".", runID, entityID), time.Now().UTC())
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
				CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			node := pipelineNode(t, ".", "node-a")
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected postcommit publication cleanup")
			bus.finalizeFailure = injected
			attemptCtx := withWorkflowNodeDeliveryRoute(ctx, route)
			handled, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt)
			if !handled || !errors.Is(err, injected) {
				t.Fatalf("handled=%t error=%v, want committed cleanup diagnostic", handled, err)
			}
			assertNativeCommittedHandlerCleanupRows(t, ctx, fixture, bus, runID, entityID, deliveryID)
			bus.finalizeFailure = nil
			if _, err := pc.dispatchWorkflowNodeEventResult(attemptCtx, evt); err != nil {
				t.Fatalf("duplicate delivery after acknowledged cleanup failure: %v", err)
			}
			assertNativeCommittedHandlerCleanupRows(t, ctx, fixture, bus, runID, entityID, deliveryID)
		})
	}
}

type releaseFailureWorkflowRuntime struct {
	WorkflowDeliveryRuntime
	failure error
}

func (r releaseFailureWorkflowRuntime) ReleaseDeliveryContinuation(deliveryID string) error {
	return errors.Join(r.WorkflowDeliveryRuntime.ReleaseDeliveryContinuation(deliveryID), r.failure)
}

func VerifyNativeGuardRejectedSettlementSurvivesContinuationCleanupFailureBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: terminal-no-engine\nstages:\n  queued: {}\n  done: {final: true}\n",
				"entities.yaml": "test_entity: {}\n",
				"events.yaml":   "source.evt:\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  subscribes_to: [source.evt]\n  event_handlers:\n    source.evt:\n      guard: {id: reject-check, check: false, on_fail: reject}\n      advances_to: done\n",
			})
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a").(*previewWorkflowModule)
			node := pipelineNode(t, ".", "node-a")
			module.workflowNodes = []WorkflowNode{{Node: node, Subscriptions: []events.EventType{"source.evt"}}}
			fixture := open(t, backend, semanticview.Wrap(bundle))
			pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			owner := fixture.Store
			runID := uuid.NewString()
			ctx = correlation.WithRunID(ctx, runID)
			if err := fixture.RequireRun(ctx, runID); err != nil {
				t.Fatal(err)
			}
			entityID := runID
			evt := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, runID, events.EnvelopeForTargetRoute(handlerTestWorkflowEnvelope(".", runID, entityID), events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID}), testWorkflowRoutingSource(".", runID, entityID), time.Now().UTC())
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: entityID, WorkflowName: ".", WorkflowVersion: pc.SemanticSource().WorkflowVersion(),
				CurrentState: "queued", EntityType: "test_entity", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: entityID})}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			deliveryID, err := deliverylifecycle.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected continuation cleanup failure")
			pc.deliveryRuntime = releaseFailureWorkflowRuntime{WorkflowDeliveryRuntime: pc.deliveryRuntime, failure: injected}
			handled, outcome, err := pc.handleEventResultWithEmissionPlan(withWorkflowNodeDeliveryRoute(ctx, route), evt, nil)
			if handled || !outcome.Committed || !errors.Is(err, injected) {
				t.Fatalf("handled=%t outcome=%+v error=%v, want guard rejection with acknowledged delivery and cleanup diagnostic", handled, outcome, err)
			}
			outcomes, err := owner.Outcomes(ctx, deliveryID)
			if err != nil || len(outcomes) != 1 || outcomes[0].Outcome != "delivered" {
				t.Fatalf("settled outcomes=%+v error=%v, want one delivered", outcomes, err)
			}
			if got := bus.committedCount(); got != 0 {
				t.Fatalf("guard rejection published %d events", got)
			}
		})
	}
}

func assertNativeCommittedHandlerCleanupRows(t *testing.T, ctx context.Context, fixture *PipelineDeliveryNativeFixtureForTest, bus *nativePipelineDeliveryBusObservationForTest, runID, entityID, deliveryID string) {
	t.Helper()
	snapshot, err := fixture.Store.Snapshot(ctx, deliveryID)
	if err != nil || snapshot.Status != deliverylifecycle.StatusDelivered {
		t.Fatalf("delivery snapshot=%+v error=%v", snapshot, err)
	}
	outcomes, err := fixture.Store.Outcomes(ctx, deliveryID)
	if err != nil || len(outcomes) != 1 {
		t.Fatalf("delivery outcomes=%+v error=%v, want one", outcomes, err)
	}
	instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
	if err != nil || !found || instance.EntityID != entityID || instance.CurrentState != "done" {
		t.Fatalf("state=%+v found=%t error=%v, want exact done", instance, found, err)
	}
	eventsCount, err := fixture.EventCount(ctx, runID, "source.done")
	if err != nil || eventsCount != 1 {
		t.Fatalf("emitted events=%d error=%v, want one", eventsCount, err)
	}
	acquisition, err := fixture.Continuations.Acquire(deliveryID)
	if err != nil || acquisition.Validate(deliveryID) != nil || acquisition.Disposition() != worklifetime.DeliveryTerminallyFenced || bus.committedCount() != 1 {
		t.Fatalf("committed delivery has executable continuation=%v/%v committed=%d", acquisition.Disposition(), err, bus.committedCount())
	}
}
