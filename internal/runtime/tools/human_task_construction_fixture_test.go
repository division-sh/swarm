package tools_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type humanTaskConstructionLoader struct{ persistence pipeline.WorkflowPersistence }

func (l humanTaskConstructionLoader) Load(ctx context.Context, owner flowidentity.RunScopedFlowInstance) (pipeline.WorkflowInstance, bool, error) {
	return l.persistence.LoadWorkflowInstance(ctx, owner)
}

// This supplies canonical component construction to the real tool/store test;
// it does not assert installed topology, live agent admission or provider proof.
func humanTaskConstructedRequester(t *testing.T, ctx context.Context, selected humanTaskToolStore, source semanticview.Source, actor actors.AgentConfig) (context.Context, actors.AgentConfig, humanTaskConstructionLoader) {
	t.Helper()
	runID := correlation.RunIDFromContext(ctx)
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("requester construction requires its admitted source artifact")
	}
	fact := sourceartifactfixture.RequireArtifact(t, ctx, selected.(sourceartifactfixture.Writer), bundle.SourceArtifact)
	if _, err := storetest.ReviseRunSource(ctx, selected.(storetest.RunFixtureStore), runlifecycle.SourceRevisionRequest{RunID: runID, Source: fact}); err != nil {
		t.Fatalf("admit requester run source: %v", err)
	}
	ctx = correlation.WithSourceArtifactFact(ctx, fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))
	ctx = effects.WithExecutionMode(ctx, executionmode.Live)
	pc := newEntityToolPipeline(t, selected, source)
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: source, SourceArtifactFact: fact,
		WorkflowInstances: pc, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	parent, err := flowidentity.StandingForGeneration(source, semanticview.RootExecutionFlowID(source), runID)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, Instance: parent, OccurredAt: at,
	})
	if err != nil {
		t.Fatalf("prepare requester constructor tree: %v", err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, bus.FlowInstanceActivationCommand{Plan: plan})
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("requester constructor tree: acknowledged=%v created=%v err=%v", committed.Acknowledged, committed.Created, err)
	}
	var finalize func(pipeline.CommittedFlowInstanceActivation)
	finalize = func(value pipeline.CommittedFlowInstanceActivation) {
		t.Helper()
		if err := pc.FinalizeInitialEntryLifecycle(ctx, value.Lifecycle); err != nil {
			t.Fatal(err)
		}
		for _, child := range value.Children {
			finalize(child)
		}
	}
	finalize(committed)

	instance, err := flowidentity.StandingForGeneration(source, actor.FlowID, runID)
	if err != nil {
		t.Fatal(err)
	}
	actor.EntityID = instance.EntityID
	actor.Identity = agentidentitytest.DeclaredForRun(t, runID, actor.Identity.AgentID(), actor.Identity.Name.Owner, instance.ScopeKey, instance.InstanceID, instance.InstancePath)
	lookup, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: runID, Route: instance.Route()})
	if err != nil {
		t.Fatal(err)
	}
	persistence := pipeline.NewWorkflowPersistence(selected.(pipeline.WorkflowPersistenceOwner))
	observed, found, err := persistence.LookupFlowInstance(ctx, lookup)
	if err != nil || !found || observed.Identity() != instance || observed.InstanceKey() != "" || !observed.SourceFact().Matches(fact) {
		t.Fatalf("requester native construction evidence: found=%v identity=%+v expected=%+v err=%v", found, observed.Identity(), instance, err)
	}
	return ctx, actor, humanTaskConstructionLoader{persistence}
}

func humanTaskImportedSource(t *testing.T, actor actors.AgentConfig) semanticview.Source {
	t.Helper()
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"gateway": {SchemaYAML: "name: gateway\n"},
		"gateway/provider": {
			SchemaYAML:   "name: provider\nstages:\n  queued: {}\n",
			EntitiesYAML: "provider_record:\n  status: text\n", AgentsYAML: entityToolAgentYAML(actor),
		},
	})
	return semanticview.Wrap(bundle)
}
