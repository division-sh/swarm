package serveapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// This establishes real source construction, not executable attachment or a
// public launch. The fork controls keep their original admission assertions.
func seedRunForkCLIConstruction(t *testing.T, db *sql.DB, runID, bundleHash string, at time.Time) {
	t.Helper()
	selected := storetest.AdmitPostgresRuntimeStore(t, db)
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot: repoRootForTest(), PlatformSpecPath: contracts.DefaultPlatformSpecFile(repoRootForTest()), Store: selected,
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(context.Background(), runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: bundleHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if loaded.Cleanup != nil {
			if err := loaded.Cleanup(); err != nil {
				t.Errorf("release constructor source: %v", err)
			}
		}
	})
	work := newSupervisorTestRuntimeOccurrence(t, bundleHash)
	ctx := effects.WithExecutionMode(correlation.WithRunID(correlation.WithSourceArtifactFact(
		worklifetime.WithOccurrence(context.Background(), work), loaded.SourceArtifactFact), runID), executionmode.Live)
	runtimeID := work.Identity().RuntimeInstanceID
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, bundleHash))
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(loaded.SourceArtifactFact, runtimeID, 1)
	if err != nil {
		t.Fatal(err)
	}
	workflow := pipeline.NewWorkflowPersistence(selected)
	sourceBus, err := bus.NewEventBusWithOptions(selected, bus.EventBusOptions{
		ExecutionPosture: executionposture.Live, WorkOwner: work, PipelineObligations: selected.PipelineObligations(),
		ContractBundle: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact,
		RuntimeInstanceID: runtimeID, DeliveryAuthority: authority, ReceiverExecution: eventreceiver.NormalExecution(),
		Durable: bus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRouteTopology: selected,
			ActiveAgents:      selected, ActiveFlows: selected, TargetOwners: selected, PreparedEvents: selected,
			TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
			Instances: workflow, ConstructionPublications: workflow,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := pipeline.NewPipelineCoordinatorWithOptions(sourceBus, pipeline.PipelineCoordinatorOptions{
		Module: loaded.Module, Persistence: workflow,
		SourceArtifactFact: loaded.SourceArtifactFact, WorkOwner: work,
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		RunLifecycle: selected, PipelineObligations: selected.PipelineObligations(), DeliveryStore: selected, DeadLetters: selected,
		DecisionCards: selected, ProposedEffects: selected, HumanTasks: selected,
		DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected, DeliveryRuntime: sourceBus,
	})
	if coordinator == nil {
		t.Fatal("source constructor coordinator was not admitted")
	}
	planner := manager.NewAgentManagerWithOptions(nil, nil, manager.AgentManagerOptions{
		BaseContext: ctx, SemanticSource: loaded.Source, SourceArtifactFact: loaded.SourceArtifactFact,
		WorkflowInstances: coordinator, WorkOwner: work, ExecutionPosture: executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: loaded.Source, OccurredAt: at,
		Instance: flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := bus.FlowInstanceActivationCommand{Plan: plan}
	committed, err := selected.CommitFlowInstanceActivation(ctx, command)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("source construction: acknowledged=%v created=%v err=%v", committed.Acknowledged, committed.Created, err)
	}
}
