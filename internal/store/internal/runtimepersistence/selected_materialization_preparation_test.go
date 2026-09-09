package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

// Component store tests consume real artifact/readiness/provider preparation.
// They still invoke the named materialization separately to inject SQL faults.
func prepareSelectedStoreMaterializationForTest(t *testing.T, ctx context.Context, selected any, sourceRun, at string, selection runfork.RunForkContractSelection) runforkreadiness.MaterializeRequest {
	t.Helper()
	storeTestWorkOwner(t)
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	ctx = worklifetime.WithProcess(ctx, work.process)
	capability := selectedMaterializationProcessForTest(t, work, selected)
	sourceStore, ok := selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatal("selected materialization requires artifact store")
	}
	repo := canonicalrouting.RepoRoot(t)
	owner := selectedStorePreparationOwnerForTest(t, selected)
	prepared, err := owner.Prepare(ctx, runforkexecution.SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: at, ContractSelection: selection,
		SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo), Store: sourceStore},
		AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
			ProcessCapability: capability, ExecutionPosture: executionposture.Live,
			Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
		},
	})
	if err != nil {
		t.Fatalf("prepare selected-store materialization: %v", err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func selectedStorePreparationOwnerForTest(t testing.TB, selected any) runforkexecution.SelectedContractExecutionOwner {
	t.Helper()
	switch selected := selected.(type) {
	case *PostgresStore:
		durable := runtimebus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
			ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
			TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
		}
		roles := runtimemanager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
			DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
			FlowRoutes: selected, StandingRestarts: selected,
		}
		owner, err := runforkexecution.NewSelectedContractExecutionOwner(
			runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
			selected, durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
		)
		if err != nil {
			t.Fatal(err)
		}
		return owner
	case *SQLiteRuntimeStore:
		durable := runtimebus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
			ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
			TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
		}
		roles := runtimemanager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
			DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
			FlowRoutes: selected, StandingRestarts: selected,
		}
		owner, err := runforkexecution.NewSelectedContractExecutionOwner(
			runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
			selected, durable, selected.PipelineObligations(), selected, roles,
			selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
		)
		if err != nil {
			t.Fatal(err)
		}
		return owner
	default:
		t.Fatalf("unsupported selected preparation store %T", selected)
		return runforkexecution.SelectedContractExecutionOwner{}
	}
}
