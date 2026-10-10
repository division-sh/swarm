package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func nativeReceiverPreparationFixtureForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context, events.Event, events.DeliveryRoute) {
	t.Helper()
	bundle := deliveryAuthoritySourceForTest(t)
	fixture := open(t, backend, semanticview.Wrap(bundle))
	module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a")
	nodes, err := LoadWorkflowNodes(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatal(err)
	}
	module.(*previewWorkflowModule).workflowNodes = nodes
	pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
	run := uuid.NewString()
	ctx = correlation.WithRunID(ctx, run)
	if err := fixture.RequireRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: run, StorageRef: run, EntityID: run, EntityType: "test_entity", WorkflowName: ".",
		WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "queued", Fields: map[string]any{},
	})); err != nil {
		t.Fatal(err)
	}
	target := events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "node-a")), Target: events.MustExistingEntityTarget(target)}
	if err := fixture.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	return fixture, pc, ctx, event, route
}

func consumeNativePipelineDeliveryCarrierForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, id string) {
	t.Helper()
	acquisition, err := fixture.Continuations.Acquire(id)
	if err != nil || acquisition.Validate(id) != nil {
		t.Fatalf("acquire exact native carrier: %v", err)
	}
	continuation, acquired := acquisition.Acquired()
	if !acquired {
		t.Fatal("exact native carrier was not acquired")
	}
	guard, err := worklifetime.NewDeliveryContinuationGuard(ctx, continuation)
	if err != nil {
		t.Fatal(err)
	}
	if resolution, err := guard.Consume(nil); err != nil || resolution != worklifetime.DeliveryContinuationConsumed {
		t.Fatalf("consume exact native carrier: %v/%v", resolution, err)
	}
}

func recoverNativePipelineRetryForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context) {
	t.Helper()
	if err := fixture.Continuations.Start(ctx); err != nil {
		t.Fatal(err)
	}
	join, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := fixture.Continuations.Synchronize(join); err != nil {
		t.Fatal(err)
	}
	if err := fixture.JoinExecution(join); err != nil {
		t.Fatal(err)
	}
}
