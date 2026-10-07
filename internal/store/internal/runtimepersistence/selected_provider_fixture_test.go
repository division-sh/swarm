package runtimepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func newSelectedProviderCompletionFixture(t *testing.T, store selectedCompletionAuthorityStore, db *sql.DB, sqlite bool) selectedCompletionFixture {
	return newSelectedProviderCompletionFixtureWithProcess(t, store, db, sqlite, selectedPreparationProcessForTest(t, store))
}

func newSelectedProviderCompletionFixtureWithProcess(t *testing.T, store selectedCompletionAuthorityStore, db *sql.DB, sqlite bool, process startupownership.ProcessCapability) selectedCompletionFixture {
	t.Helper()
	root := t.TempDir()
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: grant-receiver-root\nstages:\n  active: {initial: true}\n  done: {terminal: true}\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "global/schema.yaml"), "name: global\nstages:\n  active: {initial: true}\n  done: {terminal: true}\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "global/entities.yaml"), "receiver: {}\n")
	writeStateOnlyAcquisitionFixtureFile(t, filepath.Join(root, "events.yaml"), "test.grant_receiver:\n")
	compiled, err := contracts.LoadWorkflowContractBundleWithOverrides(pipeline.WorkflowRepoRoot(), root, contracts.DefaultPlatformSpecFile(pipeline.WorkflowRepoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(compiled)
	bundle, _ := semanticview.Bundle(source)
	f := newSelectedCompletionFixtureWithProcess(t, store, db, sqlite, process, bundle.SourceArtifact)
	f.providerSource = source
	f.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
	f.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
	f.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
	f.providerActor = agentFixtureStaticRecord(t, mustTestAgentIdentityForRun(f.forkRun, "grant-receiver", "global"))
	f.providerActor.Config.Memory = agentmemory.Plan{}
	plan, err := f.providerActor.Config.Identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := manager.AgentConfigPlanRevision(f.providerActor.Config, plan)
	if err != nil {
		t.Fatal(err)
	}
	f.request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(bundle.SourceArtifact.BundleHash(), []agenttopology.DesiredAgent{{
		Identity: plan, ConfigRevision: revision, Source: agenttopology.SourceCoordinate{BundleHash: bundle.SourceArtifact.BundleHash()},
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.request.Preparation.DeclarationPlanFingerprint = f.request.DeclarationPlan.Revision
	f.request.Preparation.Actors = []runfork.SelectedForkPreparedActor{{Plan: plan, ConfigurationRevision: revision, Backend: selection.BackendAnthropic, Mode: executionmode.Live}}
	return f
}

func selectedProviderTarget(f selectedCompletionFixture) effects.UsageTarget {
	actor := f.providerActor.Config.Identity
	return effects.UsageTarget{Kind: effects.UsageTargetAgentTurn, ID: uuid.NewString(), RunID: actor.RunID,
		AgentID: actor.AgentID(), AgentIdentity: actor, FlowInstance: actor.FlowInstance(), SessionID: uuid.NewString(), Memory: f.providerActor.Config.Memory}
}

func admitSelectedProviderFixture(t *testing.T, ctx context.Context, f selectedCompletionFixture, issued runfork.SelectedContractRuntimeExecution, authority effects.Authority) pipeline.FlowInstanceActivationPlan {
	t.Helper()
	grant := selectedFixtureGrantForAuthority(t, ctx, f, issued, authority)
	ctx = testAuthorActivityContextForBundle(f.request.DeclarationPlan.BundleHash)
	actor := f.providerActor.Config.Identity
	topology, err := agenttopology.SelectedDeclarationAdmission(f.forkRun, f.request.DeclarationPlan)
	if err != nil {
		t.Fatal(err)
	}
	record := f.providerActor
	record.Topology = topology
	origin := selectedDiagnosticOriginForTest(t, ctx, f.store, grant)
	registered, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
		DiagnosticOrigin: origin, OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(),
		Identity: actor, AgentID: actor.AgentID(), Trigger: "selected-provider-fixture", TargetEpoch: 1, TargetGeneration: 1,
		TargetPhase: manager.AgentLifecycleRegistered, ConfigRevision: f.request.DeclarationPlan.Agents[0].ConfigRevision,
		RunMode: manager.AgentRunModeStopped, Agent: &record, Topology: topology, Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
		DiagnosticOrigin: origin, OperationID: uuid.NewString(), OperationKind: "start", RequestHash: uuid.NewString(),
		Identity: actor, AgentID: actor.AgentID(), Trigger: "selected-provider-fixture",
		ExpectedEpoch: registered.RuntimeEpoch, ExpectedGeneration: registered.Generation, ExpectedPhase: registered.Phase,
		TargetEpoch: registered.RuntimeEpoch, TargetGeneration: registered.Generation + 1, TargetPhase: manager.AgentLifecycleRunning,
		ConfigRevision: f.request.DeclarationPlan.Agents[0].ConfigRevision, RunMode: manager.AgentRunModeAuthoritativeDeliveryOnly, Topology: topology, Now: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	bundle, _ := semanticview.Bundle(f.providerSource)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, actor.RunID, events.EventEnvelope{}, time.Now().UTC())
	req := sqliteFlowActivationRequest(bundle, "global", "global", "", actor.FlowInstance())
	root := flowidentity.Stored(f.providerSource, ".", actor.RunID, actor.RunID, actor.RunID, "")
	req.Instance, err = flowidentity.KeylessChild(f.providerSource, root, "global")
	if err != nil {
		t.Fatal(err)
	}
	req.TriggerEvent, req.OccurredAt = event, event.CreatedAt()
	return constructHistoricalSourceFixture(t, ctx, f.store.(agentFixtureFlowStore), req)
}

func selectedProviderClaimContext(t *testing.T, ctx context.Context, f selectedCompletionFixture, authority effects.Authority) context.Context {
	t.Helper()
	actor := f.providerActor.Config.Identity
	if actor != authority.Target.AgentIdentity || authority.SelectedFork.ForkRunID != f.forkRun {
		t.Fatal("selected provider fixture changed its admitted actor or fork")
	}
	ctx = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash))
	ctx = correlation.WithRunID(ctx, f.forkRun)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, f.request.DeclarationPlan.BundleHash))
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, f.forkRun, events.EventEnvelope{}, time.Now().UTC())
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor}
	deliveryAuthority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(f.request.DeclarationPlan.BundleHash), authority.ID, f.forkRun, authority.SelectedFork.Generation)
	if err != nil {
		t.Fatal(err)
	}
	selected := f.store.(agentFixtureFlowStore)
	commitSelectedReceiverClaimEvent(t, ctx, selected, event, route, deliveryAuthority)
	claimed, err := selected.ClaimDelivery(ctx, deliveryAuthority, event, route)
	work, acquired := claimed.Acquired()
	if err != nil || !acquired {
		t.Fatalf("claim admitted selected provider work: %+v err=%v", claimed, err)
	}
	return deliverylifecycle.WithClaim(correlation.WithInboundEvent(ctx, event), work.Claim)
}
