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
	"github.com/division-sh/swarm/internal/runtime/testfixtures/singletoncoordinatorpilot"
)

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

func TestResolveStandingTargetDeclarationsRejectsImplicitActivation(t *testing.T) {
	source, registry := standingTelegramDeclarationSource(t, "inbound.telegram")
	bundle, _ := semanticview.Bundle(source)
	bundle.PackageTree[0].Manifest.Flows[0].Activation = ""
	if _, err := ResolveStandingTargetDeclarations(source, registry); err == nil || !strings.Contains(err.Error(), "ingress requires activation: standing") {
		t.Fatalf("implicit activation error = %v", err)
	}
}

func TestRuntimeContextManagerLookupIngressDistinguishesAliasAndProvider(t *testing.T) {
	source, _ := standingTelegramDeclarationSource(t, "inbound.telegram")
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	hash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	manager, err := NewRuntimeContextManager(nil, BundleContext{
		BundleHash: hash,
		Source:     source,
		Runtime:    &Runtime{Bus: bus},
		StandingTargets: []StandingTarget{{
			BundleHash: hash, FlowID: "coordinator", Alias: "chat", Provider: "telegram",
			RunID: "run", FlowInstance: "coordinator/@standing/a", EntityID: "entity", SigningSecret: "webhook_signing.telegram",
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

func TestInboundGatewayRejectsMappedEventBeforeMarkerOrPublish(t *testing.T) {
	source, _ := standingTelegramDeclarationSource(t, "lead.observed")
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
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), FlowID: "coordinator",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "coordinator/@standing/a",
		EntityID: "41000000-0000-0000-0000-000000000002", Alias: "chat", Provider: "telegram",
		SigningSecret: "telegram-secret",
	}, source)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), "no exact external input pin") {
		t.Fatalf("response = %d %q, want exact-pin rejection", rec.Code, rec.Body.String())
	}
	if store.recorded || len(eventStore.events) != 0 {
		t.Fatalf("rejected mapped event persisted marker=%v events=%d", store.recorded, len(eventStore.events))
	}
}

func TestInboundGatewayRejectsUnboundGitHubDynamicEventBeforeMarkerOrPublish(t *testing.T) {
	source, registry := standingProviderDeclarationSource(t, "github", "inbound.github.issues")
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{inserted: true}
	gateway := NewInboundGatewayWithProviderRegistry(bus, nil, nil, registry, store)
	gateway.SetCredentialStore(identityInboundCredentialStore{})
	body := []byte(`{"action":"created","issue":{"number":7}}`)
	mac := hmac.New(sha256.New, []byte("github-secret"))
	_, _ = mac.Write(body)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/issues/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-GitHub-Delivery", "delivery-comment-1")
	req.Header.Set("X-GitHub-Event", "issue_comment")
	rec := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("b", 64), FlowID: "coordinator",
		RunID: "42000000-0000-0000-0000-000000000001", FlowInstance: "coordinator/@standing/b",
		EntityID: "42000000-0000-0000-0000-000000000002", Alias: "issues", Provider: "github",
		SigningSecret: "github-secret",
	}, source)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `resolved event "inbound.github.issue_comment"`) || !strings.Contains(rec.Body.String(), "has no exact external input pin") {
		t.Fatalf("response = %d %q, want exact mapped dynamic-event rejection", rec.Code, rec.Body.String())
	}
	if store.recorded || len(eventStore.events) != 0 {
		t.Fatalf("rejected dynamic event persisted marker=%v events=%d", store.recorded, len(eventStore.events))
	}
}

func standingTelegramDeclarationSource(t testing.TB, inputEvent string) (semanticview.Source, *providertriggers.Registry) {
	return standingProviderDeclarationSource(t, "telegram", inputEvent)
}

func standingProviderDeclarationSource(t testing.TB, provider, inputEvent string) (semanticview.Source, *providertriggers.Registry) {
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
	registry, _, err := providertriggers.NewRegistryFromPackDirs("0.7.0", []string{filepath.Join(repoRoot, "packs", "provider-triggers", provider)}, nil)
	if err != nil {
		t.Fatalf("load %s pack: %v", provider, err)
	}
	return semanticview.Wrap(bundle), registry
}
