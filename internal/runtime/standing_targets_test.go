package runtime

import (
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

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
)

func TestDeriveStandingTargets_HarnessSourceCreatesNoTarget(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection),
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("load harness injection artifact: %v", err)
	}
	declarations, err := ResolveStandingTargetDeclarations(semanticview.Wrap(bundle), nil)
	if err != nil {
		t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
	}
	if len(declarations) != 0 {
		t.Fatalf("standing targets = %#v, want none", declarations)
	}
}

func TestResolveStandingTargetDeclarationsRequiresExactProviderPin(t *testing.T) {
	source, registry := standingTelegramDeclarationSource(t, "inbound.telegram")
	declarations, err := ResolveStandingTargetDeclarations(source, registry)
	if err != nil {
		t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
	}
	if len(declarations) != 1 || declarations[0].Alias != "chat" || len(declarations[0].Ingress) != 1 {
		t.Fatalf("declarations = %#v, want one chat/telegram standing declaration", declarations)
	}

	missingPin, registry := standingTelegramDeclarationSource(t, "lead.observed")
	if _, err := ResolveStandingTargetDeclarations(missingPin, registry); err == nil || !strings.Contains(err.Error(), `add an exact external input pin for "inbound.telegram"`) {
		t.Fatalf("missing pin error = %v, want exact inbound.telegram teaching error", err)
	}
}

func TestResolveStandingTargetDeclarationsConsumesCanonicalInputAssociation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowContractBundle)
	}{
		{
			name: "non_external",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				pins := bundle.Semantics.FlowInputEventPins["coordinator"]
				pins[0].Source = "internal"
				bundle.Semantics.FlowInputEventPins["coordinator"] = pins
			},
		},
		{
			name: "ambiguous",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				pins := bundle.Semantics.FlowInputEventPins["coordinator"]
				duplicate := pins[0]
				duplicate.Name = "telegram_update_duplicate"
				bundle.Semantics.FlowInputEventPins["coordinator"] = append(pins, duplicate)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, registry := standingTelegramDeclarationSource(t, "inbound.telegram")
			bundle, _ := semanticview.Bundle(source)
			tc.mutate(bundle)
			_, err := ResolveStandingTargetDeclarations(source, registry)
			if err == nil || !strings.Contains(err.Error(), `add an exact external input pin for "inbound.telegram"`) {
				t.Fatalf("canonical input-association error = %v", err)
			}
		})
	}
}

func TestResolveStandingTargetDeclarationsRejectsImplicitActivation(t *testing.T) {
	source, registry := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, _ := semanticview.Bundle(source)
	bundle.PackageTree[0].Manifest.Flows[0].Activation = ""
	if _, err := ResolveStandingTargetDeclarations(source, registry); err == nil || !strings.Contains(err.Error(), "ingress requires activation: standing") {
		t.Fatalf("implicit activation error = %v", err)
	}
}

func TestResolveStandingTargetDeclarationsRejectsUnreachableIngressAlias(t *testing.T) {
	source, registry := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, _ := semanticview.Bundle(source)
	bundle.PackageTree[0].Manifest.Flows[0].Ingress.Alias = "chat/support"
	_, err := ResolveStandingTargetDeclarations(source, registry)
	if err == nil || !strings.Contains(err.Error(), "one URL-safe path segment") || !strings.Contains(err.Error(), "[A-Za-z0-9][A-Za-z0-9._-]*") {
		t.Fatalf("multi-segment alias error = %v", err)
	}
}

func TestStandingIngressAdmissionOmissionIsPackRequired(t *testing.T) {
	source, _ := standingTelegramDeclarationSource(t, "inbound.partner")
	bundle, _ := semanticview.Bundle(source)
	binding := &bundle.PackageTree[0].Manifest.Flows[0].Ingress.Providers[0]
	binding.Provider = "partner"
	binding.SigningSecret = ""
	binding.Admission = runtimecontracts.ProjectFlowIngressAdmission{}
	emptyCatalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	_, err = ResolveStandingTargetDeclarations(source, emptyCatalog)
	if err == nil || !strings.Contains(err.Error(), `provider "partner" is pack-required`) || !strings.Contains(err.Error(), "admission.kind: raw") {
		t.Fatalf("omitted admission error = %v", err)
	}
}

func TestValidateWorkflowContractSurfaceWarnsForUnacknowledgedUnsignedRawAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		acknowledge string
		wantWarning bool
	}{
		{name: "warning teaches acknowledgement", wantWarning: true},
		{name: "structured acknowledgement suppresses warning", acknowledge: providertriggers.UnsignedWebhookAcknowledgement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := standingTelegramDeclarationSource(t, "inbound.partner")
			bundle, _ := semanticview.Bundle(source)
			binding := &bundle.PackageTree[0].Manifest.Flows[0].Ingress.Providers[0]
			binding.Provider = "partner"
			binding.SigningSecret = ""
			binding.Admission = runtimecontracts.ProjectFlowIngressAdmission{
				Kind: "raw", Acknowledge: tc.acknowledge, Event: "inbound.partner", Payload: "json",
				Authentication: &runtimecontracts.ProjectFlowIngressAuthentication{Kind: "none"},
				DeliveryID:     &runtimecontracts.ProjectFlowIngressDeliveryID{Source: "json_path", JSONPath: "$.id"},
			}
			emptyCatalog, err := providertriggers.NewCatalogSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			declarations, err := ResolveStandingTargetDeclarations(source, emptyCatalog)
			if err != nil {
				t.Fatalf("ResolveStandingTargetDeclarations: %v", err)
			}
			warnings := unsignedRawAdmissionFindings(declarations)
			found := false
			for _, warning := range warnings {
				if warning.CheckID != "inbound_unsigned_webhook" {
					continue
				}
				found = true
				if !strings.Contains(warning.Message, "anyone who learns") || !strings.Contains(warning.Remediation, "admission.acknowledge: unsigned_webhook") {
					t.Fatalf("unsigned warning = %#v", warning)
				}
			}
			if found != tc.wantWarning {
				t.Fatalf("unsigned warning found=%t, want %t: %#v", found, tc.wantWarning, warnings)
			}
			bundleHash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
			subject, err := declarations[0].Ingress[0].AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{BundleHash: bundleHash, Alias: declarations[0].Alias})
			if err != nil {
				t.Fatal(err)
			}
			if subject.TriggerAdmission == nil || subject.TriggerAdmission.RequestAuthentication != "UNAUTHENTICATED" || len(subject.Requirements) != 0 {
				t.Fatalf("effective unsigned subject = %#v", subject)
			}
		})
	}
}

func TestRuntimeContextManagerLookupIngressDistinguishesAliasAndProvider(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	hash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram"})
	if err != nil {
		t.Fatal(err)
	}
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewRuntimeContextManagerWithAdmission(nil, ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}, BundleContext{
		BundleHash: hash,
		Source:     source,
		Runtime:    &Runtime{Bus: bus},
		StandingTargets: []StandingTarget{{
			BundleHash: hash, FlowID: "coordinator", Alias: "chat", Provider: "telegram",
			RunID: "run", FlowInstance: "coordinator/a", EntityID: "entity", SigningSecret: "webhook_signing.telegram",
			AdmissionPlan: plan,
		}},
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if lookup := manager.LookupIngress("chat", "telegram"); !lookup.Loaded() || lookup.Target.RunID != "run" {
		t.Fatalf("telegram lookup = %#v, want loaded standing target", lookup)
	}
	if lookup := manager.LookupIngress("chat", "github"); lookup.Found || !lookup.AliasFound {
		t.Fatalf("wrong-provider lookup = %#v, want alias found without provider target", lookup)
	}
}

func TestRuntimeContextManagerSuppressesAndRepublishesCommittedStandingGeneration(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	hash := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "webhook_signing.telegram"})
	if err != nil {
		t.Fatal(err)
	}
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	target := StandingTarget{
		BundleHash: hash, ServiceID: "service-1", FlowID: "coordinator", Alias: "chat", Provider: "telegram",
		RunID: "run-1", Generation: 1, PublicationSequence: 1, InstanceID: "instance-1",
		FlowInstance: "coordinator/a", EntityID: "entity", SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
	}
	manager, err := NewRuntimeContextManagerWithAdmission(nil, ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}, BundleContext{
		BundleHash: hash, Source: source, Runtime: &Runtime{Bus: bus}, StandingTargets: []StandingTarget{target},
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if err := manager.SuppressStandingServiceTargets(target.ServiceID); err != nil {
		t.Fatalf("SuppressStandingServiceTargets: %v", err)
	}
	if lookup := manager.LookupIngress("chat", "telegram"); !lookup.Found || lookup.Loaded() || lookup.Cause != RuntimeContextCauseStandingSuppressed {
		t.Fatalf("suppressed lookup = %#v", lookup)
	}
	published := target
	published.RunID = "run-2"
	published.Generation = 2
	published.PublicationSequence = 3
	if err := manager.PublishStandingServiceTargets(target.ServiceID, []StandingTarget{published}); err != nil {
		t.Fatalf("PublishStandingServiceTargets: %v", err)
	}
	lookup := manager.LookupIngress("chat", "telegram")
	if !lookup.Loaded() || lookup.Target.RunID != "run-2" || lookup.Target.Generation != 2 || lookup.Target.PublicationSequence != 3 {
		t.Fatalf("republished lookup = %#v", lookup)
	}
}

func TestInboundGatewayConsumesCompiledTelegramRouteWithoutReinterpretingStandingPins(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "lead.observed")
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
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
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), FlowID: "coordinator",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "coordinator/a",
		EntityID: "41000000-0000-0000-0000-000000000002", Alias: "chat", Provider: "telegram",
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
	source, catalog := standingProviderDeclarationSource(t, "github", "inbound.github.issues")
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	gateway := NewInboundGateway(bus, nil, nil)
	gateway.SetCredentialStore(identityInboundCredentialStore{})
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
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("b", 64), FlowID: "coordinator",
		RunID: "42000000-0000-0000-0000-000000000001", FlowInstance: "coordinator/b",
		EntityID: "42000000-0000-0000-0000-000000000002", Alias: "issues", Provider: "github",
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

func standingProviderDeclarationSource(t testing.TB, provider, inputEvent string) (semanticview.Source, *providertriggers.CatalogSnapshot) {
	t.Helper()
	alias := provider
	if provider == "telegram" {
		alias = "chat"
	}
	root := singletoncoordinatorpilot.Write(t, singletoncoordinatorpilot.Options{})
	packagePath := filepath.Join(root, "package.yaml")
	packageBytes, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatalf("read package: %v", err)
	}
	standingYAML := fmt.Sprintf("    mode: singleton\n    activation: standing\n    ingress:\n      alias: %s\n      providers:\n        - provider: %s\n          signing_secret: webhook_signing.%s", alias, provider, provider)
	packageText := strings.Replace(string(packageBytes), "    mode: singleton", standingYAML, 1)
	if err := os.WriteFile(packagePath, []byte(packageText), 0o600); err != nil {
		t.Fatalf("write package: %v", err)
	}
	schemaPath := filepath.Join(root, "flows", "coordinator", "schema.yaml")
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	schemaText := strings.Replace(string(schemaBytes), "event: lead.observed", "event: "+inputEvent, 1)
	if err := os.WriteFile(schemaPath, []byte(schemaText), 0o600); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	repoRoot := filepath.Join("..", "..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	registry, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", []string{filepath.Join(repoRoot, "packs", "provider-triggers", provider)}, nil)
	if err != nil {
		t.Fatalf("load %s pack: %v", provider, err)
	}
	return semanticview.Wrap(bundle), registry
}
