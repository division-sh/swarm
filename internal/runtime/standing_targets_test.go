package runtime

import (
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestDeriveStandingTargets_OrdinaryRootInputCreatesNoStandingTarget(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load root ingress artifact: %v", err)
	}
	declarations, err := ResolveStandingTargetDeclarations(semanticview.Wrap(bundle), nil)
	if err != nil {
		t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
	}
	if len(declarations) != 0 {
		t.Fatalf("standing targets = %#v, want none", declarations)
	}
	rt := &Runtime{Options: RuntimeOptions{
		WorkflowModule:     semanticOnlyWorkflowRuntime{source: semanticview.Wrap(bundle)},
		SourceArtifactFact: sourceartifactfixture.FactFor(bundle.SourceArtifact),
	}}
	candidates, err := rt.PlanLiveFlowScopes()
	if err != nil || len(candidates) != 1 || candidates[0].FlowPath != "." {
		t.Fatalf("ordinary root live scope=%+v err=%v", candidates, err)
	}
}

func TestA9NoProducerCannotRetain(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
		"manifest.yaml":       []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":         []byte("stages: []\n"),
		"child/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"child/schema.yaml":   []byte("stages: []\n"),
	})
	source := semanticview.Wrap(bundle)
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(".")
	if err != nil {
		t.Fatal(err)
	}
	registry, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Options: RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: registry,
		SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()), EnableDeclaredClockBinding: true,
	}}
	declarations, err := ResolveStandingTargetDeclarations(source, registry)
	if err != nil || len(declarations) != 0 {
		t.Fatalf("source without a producer declared a standing service: %+v err=%v", declarations, err)
	}
	candidates, err := rt.PlanLiveFlowScopes()
	if err != nil || len(candidates) != 2 || candidates[0].FlowPath != "." || candidates[1].FlowPath != "child" {
		t.Fatalf("source without a producer lost its live run scope: %+v err=%v", candidates, err)
	}
}

func TestRuntimeContextManagerLookupIngressDistinguishesAliasAndProvider(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw]}\n"),
	}
	for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
		if entry.ID() != "telegram" {
			continue
		}
		members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			if body, present := entry.Provider().Body(role); present {
				members["coordinator/telegram/"+role.FileName()] = body.Bytes()
			}
		}
		break
	}
	if members["coordinator/telegram/manifest.yaml"] == nil {
		t.Fatal("embedded Telegram provider is unavailable")
	}
	captured, rooted, _ := loadS1EffectiveProviderScope(t, members)
	providers, err := packadmission.AdmitProviderSource(rooted, captured.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope("coordinator")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	contextDef := rootedContextForSource(t, semanticview.Wrap(captured), rooted)
	contextDef.ProviderTriggerCatalog = catalog
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	hash := contextDef.BundleHash()
	alias, present := captured.FlowIngressAlias("coordinator")
	if !present {
		t.Fatal("captured coordinator has no compiled directory webhook alias")
	}
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, catalog)
	if err != nil || len(declarations) != 1 {
		t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
	}
	declaration := declarations[0]
	if declaration.FlowPath != "coordinator" || declaration.Alias != alias || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
		t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
	}
	binding := declaration.Ingress[0]
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, FlowPath: "coordinator", Alias: declaration.Alias, Provider: "telegram",
		RunID: "run", SigningSecret: binding.SigningSecret,
		SourcePath: declaration.SourcePath, AdmissionPlan: binding.AdmissionPlan,
	}}
	freezeTestStandingCredentialAdmission(t, contextDef, identityInboundCredentialStore{})
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if lookup := manager.LookupIngress(alias, "telegram"); !lookup.Loaded() || lookup.Target.RunID != "run" {
		t.Fatalf("telegram lookup = %#v, want loaded standing target", lookup)
	}
	if lookup := manager.LookupIngress(alias, "github"); lookup.Found || !lookup.AliasFound {
		t.Fatalf("wrong-provider lookup = %#v, want alias found without provider target", lookup)
	}
}

// Pure context-publication fixture; durable standing construction is proved
// separately through the native and served selected-store owners.
func bindStandingContextFixtureTargets(t testing.TB, source semanticview.Source, declarations []StandingTarget, runID string) []StandingTarget {
	t.Helper()
	bound := append([]StandingTarget(nil), declarations...)
	for i, target := range bound {
		if target.RunID != "" || target.FlowPath != bound[0].FlowPath {
			t.Fatal("context fixture requires unbound declarations of one flow")
		}
		bound[i].RunID = runID
	}
	return bound
}
