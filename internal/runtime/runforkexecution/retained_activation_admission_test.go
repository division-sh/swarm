package runforkexecution

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

// Invariant: activating a prepared selected fork consumes its retained admission.
// Reach: native PostgreSQL state-only activation, with permanent operation evidence.
func TestSelectedForkActivationConsumesRuntimeAdmission(t *testing.T) {
	selected := storetest.StartPostgresRuntimeStore(t)
	owner := selectedContractExecutionOwnerForTest(t, selected)
	ctx := runForkTestContext(t)
	repo := runForkExecutionRepoRoot(t)
	fixtureLoader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo,
		SourceRoot:       filepath.Join(repo, "tests/tier11-flow-composition/test-sibling-both-instantiated-isolated"),
		PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := fixtureLoader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts})
	if err != nil {
		t.Fatal(err)
	}
	ctx = correlation.WithRootAdmission(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), loaded.RootedSource.Admission())
	runID, eventID := uuid.NewString(), uuid.NewString()
	at := time.Unix(1700000330, 0).UTC()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: at.Add(-time.Minute),
		BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: loaded.RootedSource.Artifact(), RootAdmission: loaded.RootedSource.Admission(),
	})
	event := eventtest.ExistingRunRootIngress(eventID, "fork.cli.activate", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, at)
	storetest.CommitSemanticEvent(t, ctx, selected, event)
	commitSelectedOperationRootFixture(t, ctx, selected, loaded, event)
	storetest.CaptureRunForkSnapshot(t, ctx, selected, runID)
	forkPointEventID := uuid.NewString()
	checkpoint := eventtest.ExistingRunRootIngress(forkPointEventID, "fork.cli.checkpoint", "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, at.Add(time.Millisecond))
	storetest.CommitSemanticForkFrontier(t, ctx, selected, checkpoint, nil, nil)
	loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo), Store: selected}
	operation := selectedForkFixtureOperation(t, loaded, runID, forkPointEventID)
	operation.AllowSourceFreeze = false
	materialized := materializeSelectedRuntimeOutcomeFork(t, ctx, owner, SelectedContractExecutionRequest{
		SourceRunID: runID, At: forkPointEventID, Owner: owner, SourceLoader: loader,
		SourceArtifactFact: loaded.SourceArtifactFact, ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source), ForkOperation: operation,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: owner.ports.contexts.capability},
	})
	activation, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID: materialized.ForkRunID, AllowSourceFreeze: true, Store: selected, ExecutionOwner: owner, SourceLoader: loader,
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: owner.ports.contexts.capability},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(activation)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		runfork.RunForkActivation
		Owner     string                                             `json:"selected_contract_activation_gate_owner"`
		Admission *runfork.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("decode activation: %v\n%s", err, raw)
	}
	if !result.Activated || !result.SourceFrozen || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation = %#v", result.RunForkActivation)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("selected activation owner = %q, want %q", result.Owner, runfork.RunForkSelectedContractExecutionActivationGateOwner)
	}
	if result.Admission == nil ||
		result.Admission.Owner != runfork.RunForkSelectedContractExecutionAdmissionOwner ||
		result.Admission.FrontierEventCount != 0 ||
		result.Admission.ContractBindingOwner != runfork.RunForkSelectedContractBindingOwner {
		t.Fatalf("selected admission = %#v", result.Admission)
	}
}
