package runforkexecution

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func seedSelectedClaudeExecutionSource(t *testing.T, ctx context.Context, selected startupownership.Store, loaded LoadedSelectedContractSource, runID, eventID string, at time.Time) {
	t.Helper()
	seedSelectedAgentExecutionSource(t, ctx, selected, loaded, runID, eventID, at, executionmode.Live)
}

func seedSelectedAgentExecutionSource(t *testing.T, ctx context.Context, selected startupownership.Store, loaded LoadedSelectedContractSource, runID, eventID string, at time.Time, mode executionmode.Mode) {
	t.Helper()
	artifact := selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash())
	fixture := storetest.RunFixture{
		RunID: runID, Origin: storetest.ScenarioSetupOrigin(),
		Artifact: artifact, StartedAt: at.Add(-time.Minute),
	}
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), fixture)
	root := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	worker, err := flowidentity.KeylessChild(loaded.Source, root, "worker")
	if err != nil {
		t.Fatal(err)
	}
	entityID := worker.EntityID
	payload, err := json.Marshal(map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", payload, 0, runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(runID), at, mode)
	route := selectedExecutionTestAgentRoute(t, runID, "test-agent", "worker")
	route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker", EntityID: entityID})
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), event.ExecutionMode())
	rootCommand := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, root)
	workerCommand := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, worker)
	rootCommand.Plan.Children = append(rootCommand.Plan.Children, workerCommand.Plan)
	if err := rootCommand.Validate(); err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, rootCommand)
	if err != nil || !committed.Created || !committed.Acknowledged || len(committed.Children) != 1 || !committed.Children[0].Created || !committed.Children[0].Acknowledged {
		t.Fatalf("component source tree construction: committed=%+v err=%v", committed, err)
	}
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed")
	if err := storetest.SeedSelectedSourceOutcome(ctx, selected, storetest.SelectedSourceOutcomeFixture{
		RunID: runID, EventID: eventID, EntityID: entityID, CreatedAt: at, Failure: failure,
	}); err != nil {
		t.Fatal(err)
	}
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
}
