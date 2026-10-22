package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
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
	declarations, err := ResolveStandingTargetDeclarations(source, registry)
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
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "coordinator", Provider: "telegram", SigningSecret: "fixture-signing-value"})
	if err != nil {
		t.Fatal(err)
	}
	contextDef.StandingTargets = []StandingTarget{{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: "coordinator", Provider: "telegram",
		RunID: "run", Generation: 1, SigningSecret: "fixture-signing-value",
		AdmissionPlan: plan,
	}}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if lookup := manager.LookupIngress("coordinator", "telegram"); !lookup.Loaded() || lookup.Target.RunID != "run" {
		t.Fatalf("telegram lookup = %#v, want loaded standing target", lookup)
	}
	if lookup := manager.LookupIngress("coordinator", "github"); lookup.Found || !lookup.AliasFound {
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
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "coordinator", Provider: "telegram", SigningSecret: "fixture-signing-value"})
	if err != nil {
		t.Fatal(err)
	}
	target := StandingTarget{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: "coordinator", Provider: "telegram",
		RunID: "run-1", Generation: 1, PublicationSequence: 1,
		SigningSecret: "fixture-signing-value", AdmissionPlan: plan,
	}
	contextDef.StandingTargets = []StandingTarget{target}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if err := manager.SuppressStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("SuppressStandingServiceTargets: %v", err)
	}
	if lookup := manager.LookupIngress("coordinator", "telegram"); !lookup.Found || lookup.Loaded() || lookup.Cause != RuntimeContextCauseStandingSuppressed {
		t.Fatalf("suppressed lookup = %#v", lookup)
	}
	if err := manager.RestoreStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("RestoreStandingServiceTargets: %v", err)
	}
	if lookup := manager.LookupIngress("coordinator", "telegram"); !lookup.Loaded() || lookup.Target.RunID != target.RunID {
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
	lookup := manager.LookupIngress("coordinator", "telegram")
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
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "coordinator", Provider: "telegram", SigningSecret: "fixture-signing-value"})
	if err != nil {
		t.Fatal(err)
	}
	target := StandingTarget{
		BundleHash: hash, ServiceID: flowidentity.StandingServiceID("coordinator"), FlowPath: "coordinator", Alias: "coordinator", Provider: "telegram",
		RunID: "run-1", Generation: 1, PublicationSequence: 1,
		SigningSecret: "fixture-signing-value", AdmissionPlan: plan,
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
	if lookup := manager.LookupIngress("coordinator", "telegram"); !lookup.Loaded() || lookup.Target.ServiceID != target.ServiceID {
		t.Fatalf("resumed lookup = %#v, want fresh loaded process occurrence", lookup)
	}
}

func TestInboundGatewayConsumesCompiledTelegramRouteWithoutReinterpretingStandingPins(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "lead.observed")
	eventStore := &capturingInboundEventStore{}
	bus, err := newInboundTestEventBus(t, eventStore, InboundTarget{FlowPath: "coordinator", RunID: "41000000-0000-0000-0000-000000000001"})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{inserted: true}
	gateway := newTestInboundGateway(t, bus, nil, nil, store)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(`{"update_id":124,"message":{"chat":{"id":42}}}`))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	rec := httptest.NewRecorder()
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "telegram-secret"})
	if err != nil {
		t.Fatal(err)
	}
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: inboundTestBusBundleHash(t, bus), FlowPath: "coordinator",
		ServiceID: flowidentity.StandingServiceID("coordinator"), Generation: 1, PublicationSequence: 1,
		RunID: "41000000-0000-0000-0000-000000000001",
		Alias: "chat", Provider: "telegram",
		SigningSecret: "telegram-secret",
		AdmissionPlan: plan,
	}, source)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("response = %d %q, want compiled-plan acceptance", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded || len(eventStore.events) != 1 {
		t.Fatalf("compiled mapped event persisted marker=%v events=%d", eventStore.recorded, len(eventStore.events))
	}
}

func TestInboundGatewayConsumesCompiledGitHubRouteWithoutReinterpretingDynamicPins(t *testing.T) {
	source, catalog := standingProviderDeclarationSource(t, "github", "inbound.github.raw.issues")
	eventStore := &capturingInboundEventStore{}
	bus, err := newInboundTestEventBus(t, eventStore, InboundTarget{FlowPath: "coordinator", RunID: "42000000-0000-0000-0000-000000000001"})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	publicationStore := &recordingInboundStore{inserted: true, store: eventStore}
	gateway := NewInboundGateway(bus, nil, nil, executionposture.Live, publicationStore)
	gateway.SetCredentialAdmission(testInboundCredentialAdmission(t, identityInboundCredentialStore{}))
	body := []byte(`{"action":"created","issue":{"number":7}}`)
	mac := hmac.New(sha256.New, []byte("github-secret"))
	_, _ = mac.Write(body)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/issues/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-GitHub-Delivery", "delivery-comment-1")
	req.Header.Set("X-GitHub-Event", "issue_comment")
	rec := httptest.NewRecorder()
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "issues", Provider: "github", SigningSecret: "github-secret"})
	if err != nil {
		t.Fatal(err)
	}
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: inboundTestBusBundleHash(t, bus), FlowPath: "coordinator",
		ServiceID: flowidentity.StandingServiceID("coordinator"), Generation: 1, PublicationSequence: 1,
		RunID: "42000000-0000-0000-0000-000000000001",
		Alias: "issues", Provider: "github",
		SigningSecret: "github-secret",
		AdmissionPlan: plan,
	}, source)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("response = %d %q, want compiled-plan acceptance", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded || len(eventStore.events) != 1 {
		t.Fatalf("compiled dynamic event persisted marker=%v events=%d", eventStore.recorded, len(eventStore.events))
	}
}

func standingTelegramDeclarationSource(t testing.TB, inputEvent string) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	return standingProviderDeclarationSource(t, "telegram", inputEvent)
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

func standingProviderDeclarationSource(t testing.TB, provider, inputEvent string) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	t.Helper()
	alias := provider
	if provider == "telegram" {
		alias = "chat"
	}
	root := singletoncoordinatorpilot.Write(t, singletoncoordinatorpilot.Options{})
	// This fixture's producer is the admitted provider, not the pilot's root connection.
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: standing-provider-declaration\n"), 0600); err != nil {
		t.Fatal(err)
	}
	standingYAML := fmt.Sprintf("name: coordinator\ningress:\n  alias: %s\n  providers:\n    - provider: %s\n      signing_secret: webhook_signing.%s", alias, provider, provider)
	schemaPath := filepath.Join(root, "coordinator", "schema.yaml")
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	schemaText := strings.Replace(string(schemaBytes), "name: coordinator", strings.TrimSpace(standingYAML), 1)
	schemaText = strings.Replace(schemaText, "- lead.observed", "- "+inputEvent, 1)
	if err := os.WriteFile(schemaPath, []byte(schemaText), 0o600); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	repoRoot := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatalf("load admitted pack projection: %v", err)
	}
	return semanticview.Wrap(bundle), projection.ProviderTriggers
}

func mutateStandingCoordinatorSchema(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, mutate func(*runtimecontracts.FlowSchemaDocument)) {
	t.Helper()
	schema, ok := bundle.FlowSchemas["coordinator"]
	if !ok {
		t.Fatal("fixture coordinator schema missing")
	}
	mutate(&schema)
	bundle.FlowSchemas["coordinator"] = schema
	view, ok := bundle.FlowViewByID("coordinator")
	if !ok {
		t.Fatal("fixture coordinator flow missing")
	}
	view.Schema = schema
}
