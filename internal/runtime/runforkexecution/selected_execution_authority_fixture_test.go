package runforkexecution

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Component tests bind a real retained execution before constructing a runtime;
// fabricated effect evidence alone can no longer pass generation admission.
func selectedContractTestRuntimeAuthority(t *testing.T, ctx context.Context, selected *store.PostgresStore, loaded LoadedSelectedContractSource, agents *selectedContractAgentRuntimePlan) (effects.Authority, *PreparedSelectedFork, runfork.RunForkSelectedContractExecutionAdmission) {
	t.Helper()
	source := loaded.SourceArtifactFact
	root := loaded.RootedSource.Admission()
	sourceRun, eventID := uuid.NewString(), uuid.NewString()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: sourceRun, BundleHash: source.BundleHash(), Artifact: loaded.RootedSource.Artifact(), RootAdmission: root,
	})
	now := time.Now().UTC()
	event := eventtest.ExistingRunRootIngress(eventID, "selected.proof", "selected-proof", "", []byte(`{}`), 0, sourceRun, events.EventEnvelope{Scope: events.EventScopeGlobal}, now)
	storetest.CommitSemanticForkFrontier(t, ctx, selected, event, nil, nil)
	owner := selectedContractExecutionOwnerForTest(t, selected)
	operationRequest := selectedForkFixtureOperation(t, loaded, sourceRun, eventID)
	operationRequest.AllowSourceFreeze = false
	repo := runForkExecutionRepoRoot(t)
	materialized := materializeSelectedRuntimeOutcomeFork(t, ctx, owner, SelectedContractExecutionRequest{
		SourceRunID: sourceRun, At: eventID, SourceArtifactFact: source, ContractSelection: operationRequest.ContractSelection, ForkOperation: operationRequest,
		SourceLoader: SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected},
		AgentRuntime: SelectedContractAgentRuntimeOptions{ProcessCapability: agents.Options.ProcessCapability, ExecutionPosture: executionposture.Live,
			Config: &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}}},
	})
	runID := materialized.ForkRunID
	topology, err := agenttopology.SelectedDeclarationAdmission(runID, agents.Declarations)
	if err != nil {
		t.Fatal(err)
	}
	agents.Records = nil
	for _, blueprint := range agents.Blueprints {
		record, err := blueprint.Materialize(runID)
		if err != nil {
			t.Fatal(err)
		}
		agents.Records = append(agents.Records, manager.PersistedAgent{Config: record.Config, Topology: topology, Status: record.Status, HiredBy: record.HiredBy})
	}
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRun, At: eventID})
	if err != nil {
		t.Fatal(err)
	}
	operation := selectedContractOperationForTest(t, ctx)
	prepared, err := prepareSelectedFork(ctx, operation, owner.ports, loaded, plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, *agents)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := prepared.bind(ctx, runID, loaded, *agents)
	if err != nil {
		t.Fatal(err)
	}
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, ForkRunID: runID, SourceRunID: sourceRun, ForkPoint: plan.ForkPoint, ForkEventID: eventID,
		ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
		AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
	}
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
		DeclarationPlan: agents.Declarations, Preparation: binding, Admission: admission,
		ContainerPlanFingerprint: "sha256:" + strings.Repeat("1", 64), ActorCensusFingerprint: "sha256:" + strings.Repeat("2", 64),
		EffectiveConfigFingerprint: "sha256:" + strings.Repeat("3", 64), ExecutionMode: effects.ExecutionModeLive,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "component-runtime-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return authority, prepared, admission
}
