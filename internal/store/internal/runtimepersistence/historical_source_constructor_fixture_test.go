package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Historical writer controls use the canonical constructor without starting
// attachment or replaying a business event to synthesize their source history.
func constructHistoricalSourceFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, req pipeline.FlowInstanceActivationRequest) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	bundle, ok := semanticview.Bundle(req.ContractBundle)
	if !ok {
		t.Fatal("historical construction requires its exact admitted bundle")
	}
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || fact.BundleHash() != bundle.SourceArtifact.BundleHash() {
		t.Fatal("historical construction source disagrees with its run context")
	}
	bus := &sqliteFlowActivationBus{}
	workflows := configureAgentFixtureFlowLifecycle(t, selected, bus, bundle)
	am := ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(bus, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact,
		SemanticSource: req.ContractBundle, WorkflowInstances: workflows, WorkOwner: storeTestWorkOwner(t),
		DeliveryStore: selected, ReceiverExecution: eventreceiver.NormalExecution(),
	}, selected))
	plan, err := am.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: selected}).CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct historical source: result=%+v err=%v", committed, err)
	}
	return plan
}
