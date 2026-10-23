package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
	declarations, err := ResolveStandingTargetDeclarations(semanticview.Wrap(bundle), nil, nil)
	if err != nil {
		t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
	}
	if len(declarations) != 0 {
		t.Fatalf("standing targets = %#v, want none", declarations)
	}
}

func TestA9NoProducerCannotRetain(t *testing.T) {
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\n"),
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
	declarations, err := ResolveStandingTargetDeclarations(source, registry, nil)
	if err != nil || len(declarations) != 0 {
		t.Fatalf("source without a producer declared a standing service: %+v err=%v", declarations, err)
	}
	candidates, err := rt.PlanStandingServiceCandidates()
	if err != nil || len(candidates) != 0 {
		t.Fatalf("source without a producer manufactured retention: %+v err=%v", candidates, err)
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
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, catalog, nil)
	if err != nil || len(declarations) != 1 {
		t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
	}
	declaration := declarations[0]
	if declaration.FlowPath != "coordinator" || declaration.Alias != alias || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
		t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
	}
	binding := declaration.Ingress[0]
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: declaration.Alias, Provider: "telegram",
		RunID: "run", Generation: 1, SigningSecret: binding.SigningSecret,
		SourcePath: declaration.SourcePath, AdmissionPlan: binding.AdmissionPlan,
	}}
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

func TestRuntimeContextManagerSuppressesAndRepublishesCommittedStandingGeneration(t *testing.T) {
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
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, catalog, nil)
	if err != nil || len(declarations) != 1 {
		t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
	}
	declaration := declarations[0]
	if declaration.FlowPath != "coordinator" || declaration.Alias != alias || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
		t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
	}
	binding := declaration.Ingress[0]
	target := StandingTarget{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: declaration.Alias, Provider: "telegram",
		RunID: "run-1", Generation: 1, PublicationSequence: 1,
		SourcePath: declaration.SourcePath, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
	}
	contextDef.StandingTargets = []StandingTarget{target}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if err := manager.SuppressStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("SuppressStandingServiceTargets: %v", err)
	}
	if lookup := manager.LookupIngress(alias, "telegram"); !lookup.Found || lookup.Loaded() || lookup.Cause != RuntimeContextCauseStandingSuppressed {
		t.Fatalf("suppressed lookup = %#v", lookup)
	}
	if err := manager.RestoreStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("RestoreStandingServiceTargets: %v", err)
	}
	if lookup := manager.LookupIngress(alias, "telegram"); !lookup.Loaded() || lookup.Target.RunID != target.RunID {
		t.Fatalf("restored lookup = %#v", lookup)
	}
	if err := manager.SuppressStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("second SuppressStandingServiceTargets: %v", err)
	}
	if err := manager.RetireStandingServiceOccurrence(context.Background(), target.ServiceID); err != nil {
		t.Fatalf("RetireStandingServiceOccurrence: %v", err)
	}
	published := target
	published.RunID = "run-2"
	published.Generation = 2
	published.PublicationSequence = 3
	if err := manager.PublishStandingServiceTargets(target.ServiceID, []StandingTarget{published}, nil); err != nil {
		t.Fatalf("PublishStandingServiceTargets: %v", err)
	}
	lookup := manager.LookupIngress(alias, "telegram")
	if !lookup.Loaded() || lookup.Target.RunID != "run-2" || lookup.Target.Generation != 2 || lookup.Target.PublicationSequence != 3 {
		t.Fatalf("republished lookup = %#v", lookup)
	}
}

func TestRuntimeContextManagerDoesNotCreateProcessOccurrenceForSuspendedStartupTarget(t *testing.T) {
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
	declarations, err := ResolveStandingTargetDeclarations(contextDef.Source, catalog, nil)
	if err != nil || len(declarations) != 1 {
		t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
	}
	declaration := declarations[0]
	if declaration.FlowPath != "coordinator" || declaration.Alias != alias || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
		t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
	}
	binding := declaration.Ingress[0]
	target := StandingTarget{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: declaration.Alias, Provider: "telegram",
		RunID: "run-1", Generation: 1, PublicationSequence: 1,
		SourcePath: declaration.SourcePath, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
	}
	manager, err := newTestRuntimeContextManager(t, nil)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if err := manager.SuppressStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("SuppressStandingServiceTargets: %v", err)
	}
	contextDef.StandingTargets = []StandingTarget{target}
	if err := manager.Register(contextDef); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := manager.PublishStandingServiceTargets(target.ServiceID, []StandingTarget{target}, nil); err != nil {
		t.Fatalf("PublishStandingServiceTargets after suspended startup: %v", err)
	}
	if lookup := manager.LookupIngress(alias, "telegram"); !lookup.Loaded() || lookup.Target.ServiceID != target.ServiceID {
		t.Fatalf("resumed lookup = %#v, want fresh loaded process occurrence", lookup)
	}
}

// Pure context-publication fixture; durable standing construction is proved
// separately through the native and served selected-store owners.
func bindStandingContextFixtureTargets(t testing.TB, source semanticview.Source, declarations []StandingTarget, runID string) []StandingTarget {
	t.Helper()
	bound := append([]StandingTarget(nil), declarations...)
	for i, target := range bound {
		if target.RunID != "" || target.Generation != 0 || target.PublicationSequence != 0 || target.ServiceID != bound[0].ServiceID {
			t.Fatal("context fixture requires unbound declarations of one service")
		}
		bound[i].RunID, bound[i].Generation, bound[i].PublicationSequence = runID, 1, 1
	}
	return bound
}
