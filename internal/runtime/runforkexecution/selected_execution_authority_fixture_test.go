package runforkexecution

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func selectedContractTestRuntimeFork(t testing.TB, ctx context.Context, selected *store.PostgresStore, loaded LoadedSelectedContractSource, artifact *sourceartifact.AdmittedSourceArtifact) runfork.RunForkMaterialization {
	t.Helper()
	source := loaded.SourceArtifactFact
	sourceRun, eventID := uuid.NewString(), uuid.NewString()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: sourceRun, BundleHash: source.BundleHash(), Artifact: artifact,
	})
	now := time.Now().UTC()
	storetest.CommitSemanticEvent(t, ctx, selected, eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID, "selected.proof", "selected-proof", "", []byte(`{}`), 0, sourceRun,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(sourceRun), now))
	captureSelectedExecutionSourceRevision(t, selected, sourceRun)
	fork, err := selected.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{
		SourceRunID: sourceRun, At: eventID, SourceArtifactFact: source,
		EffectiveSourceIdentity: loaded.EffectiveSourceIdentity,
		ContractSelection:       &runfork.RunForkContractSelection{Mode: "selected_contracts"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fork
}

// Component tests bind a real retained execution before constructing a runtime.
func selectedContractTestRuntimeAuthority(t testing.TB, ctx context.Context, selected *store.PostgresStore, loaded LoadedSelectedContractSource, fork runfork.RunForkMaterialization, agents selectedContractAgentRuntimePlan) (effects.Authority, *PreparedSelectedFork, runfork.RunForkSelectedContractExecutionAdmission) {
	t.Helper()
	plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fork.SourceRunID, At: fork.ForkPoint.EventID})
	if err != nil {
		t.Fatal(err)
	}
	operation := selectedContractOperationForTest(t, ctx)
	owner := selectedContractExecutionOwnerForTest(t, selected)
	prepared, err := prepareSelectedFork(ctx, operation, owner.ports, loaded, plan, runfork.RunForkContractFrontierAdmission{}, runfork.RunForkSelectedContractRecipientPlanning{}, agents)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := prepared.bind(ctx, fork.ForkRunID, loaded, agents)
	if err != nil {
		t.Fatal(err)
	}
	admission := runfork.RunForkSelectedContractExecutionAdmission{
		Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
		NonMutating: true, ForkRunID: fork.ForkRunID, SourceRunID: fork.SourceRunID, ForkPoint: plan.ForkPoint, ForkEventID: fork.ForkPoint.EventID,
		ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
		AdmissionUse:               runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
	}
	containerFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(fork)
	if err != nil {
		t.Fatal(err)
	}
	actorFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(agents.Records)
	if err != nil {
		t.Fatal(err)
	}
	configFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(agents.Options.Config)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := selected.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
		DeclarationPlan: agents.Declarations, Preparation: binding, Admission: admission,
		ContainerPlanFingerprint: containerFingerprint, ActorCensusFingerprint: actorFingerprint,
		EffectiveConfigFingerprint: configFingerprint, ExecutionMode: effects.ExecutionModeLive,
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

func TestSelectedComponentRuntimeAuthorityUsesAdmittedInputs(t *testing.T) {
	selected := storetest.StartPostgresRuntimeStore(t)
	root := t.TempDir()
	writeSelectedContractFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: selected-component-authority\n")
	writeSelectedContractFixtureFile(t, filepath.Join(root, "events.yaml"), "selected.proof:\n")
	repo := runForkExecutionRepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithSourceArtifactFact(runForkTestContext(t), fact)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, fact.BundleHash()))
	loaded := LoadedSelectedContractSource{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact, EffectiveSourceIdentity: testEffectiveSourceIdentity(fact)}
	fork := selectedContractTestRuntimeFork(t, ctx, selected, loaded, bundle.SourceArtifact)
	stored, err := selected.RequireRunForkSelectedContractBinding(ctx, fork.ForkRunID)
	if err != nil || fork.SelectedContractBinding == nil || !reflect.DeepEqual(&stored, fork.SelectedContractBinding) {
		t.Fatalf("component fork did not preserve its complete native binding: fork=%+v stored=%+v err=%v", fork, stored, err)
	}
	declarations, err := agenttopology.NewSelectedDeclarationPlan(fact.BundleHash(), nil)
	if err != nil {
		t.Fatal(err)
	}
	agents := selectedContractAgentRuntimePlan{Declarations: declarations, Options: SelectedContractAgentRuntimeOptions{
		ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected), Config: &config.Config{},
	}}
	authority, prepared, admission := selectedContractTestRuntimeAuthority(t, ctx, selected, loaded, fork, agents)
	defer func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := selected.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
			t.Error(err)
			return
		}
		if err := selected.CloseRunForkSelectedContractRuntimeExecution(ctx, authority.SelectedFork.ExecutionID); err != nil {
			t.Error(err)
		}
	}()
	for _, check := range []struct {
		name  string
		input any
		got   string
	}{
		{"container", fork, authority.SelectedFork.ContainerPlanFingerprint},
		{"actors", agents.Records, authority.SelectedFork.ActorCensusFingerprint},
		{"config", agents.Options.Config, authority.SelectedFork.EffectiveConfigFingerprint},
	} {
		want, err := runfork.RunForkSelectedContractRuntimeFingerprint(check.input)
		if err != nil || check.got != want {
			t.Fatalf("%s fingerprint is not derived from its actual fixture input: got=%s want=%s err=%v", check.name, check.got, want, err)
		}
	}
	if !authority.Valid() || authority.SelectedFork.ForkRunID != fork.ForkRunID || admission.SourceRunID != fork.SourceRunID || admission.ForkPoint != fork.ForkPoint {
		t.Fatalf("component authority escaped its materialized source/fork/point: authority=%+v admission=%+v fork=%+v", authority, admission, fork)
	}
}
