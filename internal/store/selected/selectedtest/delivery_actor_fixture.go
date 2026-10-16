package selectedtest

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// PreparePersistedActor advances only this fixture's held native selected claim.
// Claiming a delivery never constructs or repairs its receiver prerequisites.
func (f *SelectedDeliveryFixture) PreparePersistedActor(t *testing.T) (manager.PersistedAgent, startupownership.GenerationGrant) {
	t.Helper()
	if f.prepareActor == nil {
		t.Fatal("selected delivery actor preparation is absent or already consumed")
	}
	prepare := f.prepareActor
	f.prepareActor = nil
	return prepare(t)
}

func prepareSelectedDeliveryActor(t *testing.T, fixture *SelectedDeliveryFixture, owner SelectedDeliveryFixtureStore, capability startupownership.ProcessCapability, claim runforkexecution.ManagerDeliveryExecutionClaim) (manager.PersistedAgent, startupownership.GenerationGrant) {
	t.Helper()
	ctx := correlation.WithRunID(fixture.Context, claim.Issued.ForkRunID)
	process, err := capability.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	ctx = correlation.WithRuntimeInstanceID(ctx, process.RuntimeInstanceID)
	binding, err := owner.RequireRunForkSelectedContractBinding(ctx, claim.Issued.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := capability.IssueSelectedForkGenerationGrant(ctx, startupownership.SelectedForkGrantRequest{
		BundleHash: fixture.Authority.SourceArtifact().BundleHash(), RuntimeInstanceID: process.RuntimeInstanceID,
		Binding: startupownership.SelectedForkGrantBinding{
			BindingID: binding.BindingID, ForkRunID: claim.Issued.ForkRunID,
			ExecutionID: claim.Authority.SelectedFork.ExecutionID, ExecutionGeneration: claim.Authority.SelectedFork.Generation,
			ExecutionOwner: claim.Authority.ExecutionOwner, FenceGeneration: claim.Authority.FenceGeneration,
			AdmissionFingerprint: claim.Authority.SelectedFork.AdmissionFingerprint, ContainerPlanFingerprint: claim.Authority.SelectedFork.ContainerPlanFingerprint,
			ActorCensusFingerprint: claim.Authority.SelectedFork.ActorCensusFingerprint, EffectiveConfigFingerprint: claim.Authority.SelectedFork.EffectiveConfigFingerprint,
			DeclarationPlanFingerprint: claim.Declarations.Revision, PreparationFingerprint: claim.Issued.PreparationFingerprint,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := grant.Retire(context.Background()); err != nil {
			t.Error(err)
		}
	})
	record := selectedDeliveryDeclaredActor(t, fixture, claim)
	if _, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(), Trigger: "selected-delivery-actor-preparation",
		DiagnosticOrigin: selectedDeliveryDiagnosticOrigin(claim),
		Identity:         record.Config.Identity, AgentID: record.Config.ID, ConfigRevision: claim.Declarations.Agents[0].ConfigRevision,
		TargetEpoch: 1, TargetGeneration: 1, TargetPhase: manager.AgentLifecycleRegistered, RunMode: manager.AgentRunModeStopped,
		Topology: record.Topology, Agent: &record, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	records, err := owner.LoadAgents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Context = ctx
	for _, persisted := range records {
		if persisted.Config.Identity == record.Config.Identity {
			persisted.Config.Prompt = record.Config.Prompt
			return persisted, grant
		}
	}
	t.Fatal("issued selected generation lost its persisted declared actor")
	return manager.PersistedAgent{}, nil
}

func selectedDeliveryDeclaredActor(t *testing.T, fixture *SelectedDeliveryFixture, claim runforkexecution.ManagerDeliveryExecutionClaim) manager.PersistedAgent {
	t.Helper()
	records, err := manager.StaticAgentMaterializationRecords(claim.Issued.ForkRunID, fixture.SemanticSource)
	if err != nil || len(records) != 1 || len(claim.Declarations.Agents) != 1 {
		t.Fatalf("selected delivery requires its one actual declared actor: count=%d error=%v", len(records), err)
	}
	record := records[0]
	profile, err := selection.ResolveLiveBackend(selection.BackendAnthropic)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := llm.ResolveAgentExecution(executionposture.Live, profile, selection.EffectiveModelAliases(nil), record.Config)
	if err != nil {
		t.Fatal(err)
	}
	record.Config = resolved.Actor
	revision, err := manager.AgentConfigPlanRevision(record.Config, claim.Declarations.Agents[0].Identity)
	if err != nil || revision != claim.Declarations.Agents[0].ConfigRevision || record.Config.Identity != fixture.SelectedRoute.AgentIdentity {
		t.Fatalf("prepared actor differs from the issued selected declaration: revision=%s error=%v", revision, err)
	}
	record.Topology, err = agenttopology.SelectedDeclarationAdmission(claim.Issued.ForkRunID, claim.Declarations)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func selectedDeliveryDiagnosticOrigin(claim runforkexecution.ManagerDeliveryExecutionClaim) manager.LifecycleDiagnosticOrigin {
	return manager.LifecycleDiagnosticOrigin{
		Owner: manager.LifecycleDiagnosticSelectedFork, Causality: manager.LifecycleDiagnosticObservation,
		SelectedFork: claim.Authority.SelectedFork, SourceRunID: claim.Issued.SourceRunID,
		ForkPoint: claim.Issued.ForkPoint, ForkEventID: claim.Issued.ForkEventID,
	}
}
