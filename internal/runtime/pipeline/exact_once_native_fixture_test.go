package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func nativeExactOnceCoordinatorForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) (*PipelineDeliveryNativeFixtureForTest, *PipelineCoordinator, context.Context) {
	t.Helper()
	root := canonicalrouting.CopyConstructedStaticHandler(t, true)
	repo := contractComplianceRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return nativePilotPipelineForTest(t, backend, bundle, open)
}

func constructNativePipelineScenarioForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, flow string) WorkflowInstance {
	t.Helper()
	instance := constructedScenarioInstanceForTest(t, pc.SemanticSource(), ctx, flow)
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	stored, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
	if err != nil || !found {
		t.Fatalf("native constructed scenario missing: %t/%v", found, err)
	}
	return stored
}

func nativeExactOncePublicationForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, event events.Event, instance WorkflowInstance) (context.Context, events.DeliveryRoute) {
	t.Helper()
	target := events.RouteIdentity{FlowID: "validation", FlowInstance: instance.StorageRef, EntityID: instance.EntityID}
	route := workflowNodeStampedConnectRoute(t, pc.SemanticSource(), "validation", "thing.created", "w-node")
	route.Target = events.MustExistingEntityTarget(target)
	if err := fixture.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	return withWorkflowNodeDeliveryRoute(correlation.WithInboundEvent(ctx, event), route), route
}

func assertNativePipelineMutationCountForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, ctx context.Context, event, path, writer, step string, want int) {
	t.Helper()
	got, err := fixture.MutationCount(ctx, correlation.RunIDFromContext(ctx), event, path, writer, step)
	if err != nil || got != want {
		t.Fatalf("mutation count event=%s path=%s writer=%s step=%s = %d, want %d (error=%v)", event, path, writer, step, got, want, err)
	}
}

func prepareNativeConstructorHandlerDeliveryForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, flow, nodeID string, event events.Event) (context.Context, WorkflowState) {
	t.Helper()
	instance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, flow)
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineSourceNode(t, pc.SemanticSource(), flow, nodeID)), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flow, FlowInstance: instance.StorageRef, EntityID: instance.EntityID})}
	if err := fixture.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	ctx = withWorkflowNodeDeliveryRoute(correlation.WithInboundEvent(ctx, event), route)
	return ctx, mustCurrentWorkflowState(t, pc, ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef).Route, instance.EntityID)
}
