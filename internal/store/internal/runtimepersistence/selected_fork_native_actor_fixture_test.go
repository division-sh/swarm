package runtimepersistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// These are native selected construction facts for an existing materialized
// fork, not the unretained physical-history fixture or ordinary claim authority.
func admitMaterializedSelectedForkActor(t *testing.T, ctx context.Context, selected *PostgresStore, materialized runfork.RunForkMaterialization, source semanticview.Source, identity agentidentity.Identity) (context.Context, effects.Authority, deliverylifecycle.ExecutionAuthority) {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil || identity.RunID != materialized.ForkRunID {
		t.Fatal("selected actor fixture requires its actual fork and compiled source")
	}
	ctx = correlation.WithRunID(ctx, materialized.ForkRunID)
	ctx = correlation.WithSourceArtifactFact(ctx, mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash()))
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, bundle.SourceArtifact.BundleHash()))
	actor := agentFixtureStaticRecord(t, identity)
	actor.Config.FlowID = identity.Route.ScopeKey
	plan, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := manager.AgentConfigPlanRevision(actor.Config, plan)
	if err != nil {
		t.Fatal(err)
	}
	declarations, err := agenttopology.NewSelectedDeclarationPlan(bundle.SourceArtifact.BundleHash(), []agenttopology.DesiredAgent{{
		Identity: plan, ConfigRevision: revision, Source: agenttopology.SourceCoordinate{BundleHash: bundle.SourceArtifact.BundleHash()},
	}})
	if err != nil {
		t.Fatal(err)
	}
	storeTestWorkOwner(t)
	work, _ := storeTestWorkFixtures.Load(t)
	process := selectedMaterializationProcessForTest(t, work.(*storeTestWorkFixture), selected)
	binding, err := selected.RequireRunForkSelectedContractBinding(ctx, materialized.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	preparation := selectedPreparationForTest(t, process, binding.SourceRunID, binding.ForkRunID, binding.ForkEventID, declarations)
	preparation.ForkPoint = binding.ForkPoint
	preparation.Actors = []runfork.SelectedForkPreparedActor{{Plan: plan, ConfigurationRevision: revision, Backend: selection.BackendAnthropic, Mode: executionmode.Live}}
	f := selectedCompletionFixture{
		process: process, store: selected, sourceRun: binding.SourceRunID, forkRun: binding.ForkRunID, eventID: binding.ForkEventID,
		providerActor: actor, providerSource: source,
		request: runfork.SelectedContractRuntimeExecutionIssueRequest{
			DeclarationPlan: declarations, Preparation: preparation,
			Admission: runfork.RunForkSelectedContractExecutionAdmission{
				Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
				NonMutating: true, ForkRunID: binding.ForkRunID, SourceRunID: binding.SourceRunID,
				ForkPoint: binding.ForkPoint, ForkEventID: binding.ForkEventID, ContractSelection: binding.ContractSelection,
				ContractBindingOwner:       runfork.RunForkSelectedContractBindingOwner,
				AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
				DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
			},
			ContainerPlanFingerprint:   selectedActorFixtureFingerprint(t, materialized),
			ActorCensusFingerprint:     selectedActorFixtureFingerprint(t, []manager.PersistedAgent{actor}),
			EffectiveConfigFingerprint: selectedActorFixtureFingerprint(t, actor.Config), ExecutionMode: effects.ExecutionModeLive,
		},
	}
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := selected.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "fresh-selected-fork-fixture", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	admitSelectedProviderActor(t, ctx, f, issued, authority)
	deliveryAuthority, err := deliverylifecycle.NewSelectedExecutionAuthority(mustStoreTestSourceArtifactFact(declarations.BundleHash), authority.ID, binding.ForkRunID, authority.SelectedFork.Generation)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, authority.ID, authority.SelectedFork.Generation, binding.ForkRunID, issued.ActorCensusFingerprint, declarations.BundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	return managedexecution.WithAdmission(ctx, admission), authority, deliveryAuthority
}

func selectedActorFixtureFingerprint(t *testing.T, value any) string {
	t.Helper()
	fingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(value)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func claimNativeSelectedForkFixtureEvent(t *testing.T, ctx context.Context, selected *PostgresStore, event events.Event, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority) deliverylifecycle.ClaimedObligation {
	t.Helper()
	if err := commitDeliveryObligationFixture(ctx, selected, event, route); err != nil {
		t.Fatalf("commit native selected fixture obligation: %v", err)
	}
	result, err := selected.ClaimDelivery(ctx, authority, event, route)
	claimed, acquired := result.Acquired()
	if err != nil || !acquired {
		t.Fatalf("claim native selected fixture event: %+v err=%v", result, err)
	}
	return claimed
}

func closeNativeSelectedForkFixture(t *testing.T, ctx context.Context, selected *PostgresStore, authority effects.Authority) {
	t.Helper()
	if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.ID); err != nil {
		t.Fatal(err)
	}
}

func proveNativeSelectedTurnReadbackCoordinates(t *testing.T, ctx context.Context, authority effects.Authority) {
	t.Helper()
	expected := effects.Authority{Target: authority.Target}
	got, err := managedTurnReadbackAuthority(ctx, expected)
	if err != nil || got.ID != authority.ID || got.SelectedFork != authority.SelectedFork || got.Target != authority.Target {
		t.Fatalf("selected fixture replaced its exact issued authority: %+v err=%v", got, err)
	}
	for _, name := range []string{"missing_admission", "execution", "fork", "generation", "source", "target"} {
		t.Run("refuse_"+name, func(t *testing.T) {
			admission, _ := managedexecution.FromContext(ctx)
			want, badCtx := expected, ctx
			switch name {
			case "missing_admission":
				badCtx = effects.WithAuthority(context.Background(), authority)
			case "execution":
				admission.ExecutionAuthorityID = "00000000-0000-4000-8000-000000000001"
			case "fork":
				admission.RunID = "00000000-0000-4000-8000-000000000002"
			case "generation":
				admission.Generation++
			case "source":
				admission.BundleHash = "bundle-v2:sha256:" + strings.Repeat("e", 64)
			case "target":
				want.Target.SessionID = "00000000-0000-4000-8000-000000000003"
			}
			if name != "missing_admission" {
				badCtx = managedexecution.WithAdmission(badCtx, admission)
			}
			if _, err := managedTurnReadbackAuthority(badCtx, want); err == nil {
				t.Fatal("selected turn fixture accepted foreign or missing original authority")
			}
		})
	}
}
