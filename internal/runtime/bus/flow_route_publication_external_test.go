package bus_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestFlowRoutePublicationRetiresOnlyItsAttempt(t *testing.T) {
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		Produces: []string{"task.started"}, SubscribesTo: []string{"task.started"},
	})
	owned, err := runtimecorrelation.NewSourceArtifactFact(sourceartifactfixture.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: owned, RuntimeInstanceID: runtimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1"))
	req := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}
	binding := runtimeprocessbinding.Binding{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "publication-test", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: owned.BundleHash(),
		RuntimeInstanceID: runtimeID, RuntimeGeneration: 1,
	}
	newAttempt := func() runtimepipeline.DynamicFlowRuntimeActivationAttempt {
		attempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, binding)
		if err != nil {
			t.Fatal(err)
		}
		return attempt
	}
	first := newAttempt()
	firstHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, first)
	if err != nil {
		t.Fatal(err)
	}
	replayedHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, first)
	if err != nil || replayedHandle != firstHandle {
		t.Fatalf("same-attempt publication replay: handle=%+v err=%v", replayedHandle, err)
	}
	if _, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, newAttempt()); err == nil {
		t.Fatal("successor published before predecessor retirement")
	}
	if !eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("predecessor route disappeared during rejected successor publication")
	}
	if err := eb.RetireFlowInstanceRoutePublication(firstHandle); err != nil {
		t.Fatal(err)
	}
	secondHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, newAttempt())
	if err != nil {
		t.Fatal(err)
	}
	if err := eb.RetireFlowInstanceRoutePublication(firstHandle); err != nil {
		t.Fatal(err)
	}
	if !eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("late predecessor retirement removed successor route")
	}
	if err := eb.RetireFlowInstanceRoutePublication(secondHandle); err != nil {
		t.Fatal(err)
	}
	if eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("current publication did not retire its route")
	}
	wrongBinding := binding
	wrongBinding.BundleHash = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	wrongAttempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, wrongBinding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, wrongAttempt); err == nil {
		t.Fatal("attempt from another bundle published a route")
	}
	wrongBinding = binding
	wrongBinding.RuntimeInstanceID = uuid.NewString()
	wrongAttempt, err = runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, wrongBinding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eb.PublishPersistedFlowInstanceRouteForAttempt(context.Background(), req, wrongAttempt); err == nil {
		t.Fatal("attempt from another runtime published a route")
	}
	if eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("rejected cross-source publication left a process-visible route")
	}
}

func TestFlowRoutePublicationRejectsForeignSourceContext(t *testing.T) {
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		Produces: []string{"task.started"}, SubscribesTo: []string{"task.started"},
	})
	owned, err := runtimecorrelation.NewSourceArtifactFact(sourceartifactfixture.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := uuid.NewString()
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: owned, RuntimeInstanceID: runtimeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1"))
	binding := runtimeprocessbinding.Binding{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "publication-test", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: owned.BundleHash(),
		RuntimeInstanceID: runtimeID, RuntimeGeneration: 1,
	}
	attempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, binding)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), foreign)
	if _, err := eb.PublishPersistedFlowInstanceRouteForAttempt(ctx, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}, attempt); err == nil {
		t.Fatal("foreign source published an activation route")
	}
	if eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("foreign source left a process-visible route")
	}
}
