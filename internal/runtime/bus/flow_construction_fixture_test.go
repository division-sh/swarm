package bus_test

import (
	"context"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

type componentFlowConstructionStore interface {
	completeEventDispatchStore
	runtimebus.FlowInstanceActivationCommitOwner
}

// This prepares component fixtures through the real lifecycle and commit
// owners. Empty route sets do not qualify public construction/readiness.
func seedComponentFlowConstruction(t *testing.T, ctx context.Context, selected componentFlowConstructionStore, source semanticview.Source, instance runtimepipeline.WorkflowInstance) {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("component construction requires an admitted bundle")
	}
	schema, found := source.FlowSchemaByID(instance.WorkflowName)
	if !found {
		t.Fatal("component construction requires its declared flow")
	}
	instance.Mode = schema.EffectiveMode()
	bus, err := newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := newEventBusWorkflowCoordinator(bus, nil, selected, newFixtureWorkflowModule(t, bundle))
	if coordinator == nil {
		t.Fatal("component construction requires the real workflow coordinator")
	}
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	identity := runtimeflowidentity.Stored(nil, instance.WorkflowName, instance.StorageRef, instance.InstanceID, instance.EntityID, instance.ParentEntityID)
	owner := testRunScopedFlowRouteForRun(runtimecorrelation.RunIDFromContext(ctx), identity.Route())
	graph, found := semanticview.WorkflowStageTopology(source, instance.WorkflowName)
	if !found {
		t.Fatal("component construction requires compiled stage topology")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	instance.CurrentState, instance.StageDefined = initial.ID(), graph.StageCount() != 0
	initialized, lifecycle, err := coordinator.PrepareInitialEntryLifecycle(ctx, owner, instance, instance.CreatedAt)
	if err != nil {
		t.Fatalf("prepare constructed fixture: %v", err)
	}
	command, err := flowactivationfixture.Command(ctx, initialized, lifecycle, instance.CreatedAt)
	if err != nil {
		t.Fatalf("prepare fixture activation command: %v", err)
	}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct fixture %s: result=%#v err=%v", instance.StorageRef, committed, err)
	}
	if err := coordinator.FinalizeInitialEntryLifecycle(ctx, committed.Lifecycle); err != nil {
		t.Fatalf("finalize constructed fixture: %v", err)
	}
}
