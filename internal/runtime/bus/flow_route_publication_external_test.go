package bus_test

import (
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprocessbinding "github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestFlowRoutePublicationRetiresOnlyItsAttempt(t *testing.T) {
	source := routeMaterializationNodeSource("review", runtimecontracts.SystemNodeContract{
		Produces: []string{"task.started"}, SubscribesTo: []string{"task.started"},
	})
	eb, err := newScopedTestEventBus(&routePersistenceTestStore{}, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	identity := testRunScopedFlowRoute(runtimeflowidentity.DeriveRoute("review", "inst-1"))
	req := runtimebus.FlowInstanceRouteMaterializationRequest{Identity: identity}
	binding := runtimeprocessbinding.Binding{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "publication-test", ProcessBootID: uuid.NewString(),
		GenerationGrantID: uuid.NewString(), BundleHash: "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		RuntimeInstanceID: uuid.NewString(), RuntimeGeneration: 1,
	}
	newAttempt := func() runtimepipeline.DynamicFlowRuntimeActivationAttempt {
		attempt, err := runtimepipeline.NewDynamicFlowRuntimeActivationAttempt(uuid.NewString(), identity.RunID, identity.Route.InstancePath, 1, binding)
		if err != nil {
			t.Fatal(err)
		}
		return attempt
	}
	first := newAttempt()
	firstHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(req, first)
	if err != nil {
		t.Fatal(err)
	}
	replayedHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(req, first)
	if err != nil || replayedHandle != firstHandle {
		t.Fatalf("same-attempt publication replay: handle=%+v err=%v", replayedHandle, err)
	}
	if _, err := eb.PublishPersistedFlowInstanceRouteForAttempt(req, newAttempt()); err == nil {
		t.Fatal("successor published before predecessor retirement")
	}
	if !eb.RouteTable().HasFlowInstanceRoute(identity) {
		t.Fatal("predecessor route disappeared during rejected successor publication")
	}
	if err := eb.RetireFlowInstanceRoutePublication(firstHandle); err != nil {
		t.Fatal(err)
	}
	secondHandle, err := eb.PublishPersistedFlowInstanceRouteForAttempt(req, newAttempt())
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
}
