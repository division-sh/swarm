package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
)

// Historical writer controls use the canonical constructor without starting
// attachment or replaying a business event to synthesize their source history.
func constructHistoricalSourceFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, req pipeline.FlowInstanceActivationRequest) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	ctx = originalStoreFixtureContext(t, ctx, selected)
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

func constructDefaultRootPublicationSourceFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore) events.RoutingSource {
	t.Helper()
	ctx = effects.WithExecutionMode(ctx, effects.ExecutionModeLive)
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeID := original.ProcessAuthority().RuntimeInstanceID
	ctx = correlation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, rooted.Artifact().BundleHash()))
	repo := pipeline.WorkflowRepoRoot()
	bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	constructed := constructStaticPublicationSourceFixture(t, ctx, selected, bundle, semanticview.RootExecutionFlowID(semanticview.Wrap(bundle)))
	route := constructed.Route()
	route.EntityID = "" // The clock's original carrier is flow-owned, not entity-targeted.
	routing, err := events.NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	return routing
}

func constructStaticPublicationSourceFixture(t *testing.T, ctx context.Context, selected agentFixtureFlowStore, bundle *contracts.WorkflowContractBundle, flowID string) events.RoutingSource {
	t.Helper()
	source := semanticview.Wrap(bundle)
	runID := correlation.RunIDFromContext(ctx)
	rootID := semanticview.RootExecutionFlowID(source)
	req := sqliteFlowActivationRequest(bundle, rootID, runID, "", runID)
	req.Instance = flowidentity.Stored(source, rootID, runID, runID, runID, "")
	plan := constructHistoricalSourceFixture(t, ctx, selected, req)
	for _, construction := range plan.ConstructionPlans() {
		if construction.Identity.TemplateID != flowID {
			continue
		}
		route, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
			FlowID: flowID, FlowInstance: construction.Identity.InstancePath, EntityID: construction.Identity.EntityID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return route
	}
	t.Fatalf("root construction did not create static publication source %q", flowID)
	return events.RoutingSource{}
}
