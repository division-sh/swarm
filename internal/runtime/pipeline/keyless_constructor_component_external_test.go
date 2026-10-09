package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Constructor preparation and the selected-store tree commit are real. The
// empty route projections and retired readiness rows are component setup, not
// evidence of installed agents, route publication or public activation.
func commitKeylessConstructorComponent(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, coordinator *pipeline.PipelineCoordinator, source semanticview.Source) {
	t.Helper()
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found {
		t.Fatal("constructor component requires its exact source fact")
	}
	runID := correlation.RunIDFromContext(ctx)
	identity := flowidentity.Stored(source, semanticview.RootExecutionFlowID(source), runID, runID, runID, "")
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: source, SourceArtifactFact: fact,
		WorkflowInstances: coordinator, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	at := time.Now().UTC().Truncate(time.Microsecond)
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: identity, OccurredAt: at,
	})
	if err != nil {
		t.Fatalf("prepare actual keyless constructor: %v", err)
	}
	command := bus.FlowInstanceActivationCommand{Plan: plan}
	committed, err := selected.events.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Created || !committed.Acknowledged {
		t.Fatalf("commit actual keyless constructor: acknowledged=%v created=%v err=%v", committed.Acknowledged, committed.Created, err)
	}
	var finish func(pipeline.CommittedFlowInstanceActivation)
	finish = func(value pipeline.CommittedFlowInstanceActivation) {
		t.Helper()
		if err := coordinator.FinalizeInitialEntryLifecycle(ctx, value.Lifecycle); err != nil {
			t.Fatal(err)
		}
		markGateRecoveryTopologyReadyFixture(t, selected, value.Plan.Readiness, at)
		for _, child := range value.Children {
			finish(child)
		}
	}
	finish(committed)
}
