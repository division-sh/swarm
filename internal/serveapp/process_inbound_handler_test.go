package serveapp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestRuntimeProcessInboundHandlerTeachesUnknownStandingAlias(t *testing.T) {
	manager, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	rec := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(`{"ok":true}`)))
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `no ingress target "chat" is declared`) {
		t.Fatalf("unknown alias status/body = %d %q, want teaching 404", rec.Code, rec.Body.String())
	}
}

func TestStandingIngressAliasGrammarMatchesProcessWebhookRouter(t *testing.T) {
	for _, alias := range []string{"chat", "chat.v2", "chat_v2", "chat-v2", "9chat"} {
		if _, err := runtimepkg.NormalizeStandingIngressAlias(alias); err != nil {
			t.Fatalf("NormalizeStandingIngressAlias(%q): %v", alias, err)
		}
		gotAlias, provider, ok := parseProcessWebhookPath("/webhooks/" + alias + "/telegram")
		if !ok || gotAlias != alias || provider != "telegram" {
			t.Fatalf("parseProcessWebhookPath(%q) = %q/%q/%v", alias, gotAlias, provider, ok)
		}
	}
	for _, alias := range []string{"chat/support", "chat support", "chat%2Fsupport", "-chat", ".chat", "chat?x"} {
		if _, err := runtimepkg.NormalizeStandingIngressAlias(alias); err == nil {
			t.Fatalf("NormalizeStandingIngressAlias(%q) error = nil", alias)
		}
	}
	if _, _, ok := parseProcessWebhookPath("/webhooks/chat/support/telegram"); ok {
		t.Fatal("parseProcessWebhookPath accepted a multi-segment alias")
	}
}

func TestRuntimeProcessInboundHandlerSelectsExactLoadedContext(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), contractsRoot, cliapp.ResolvePath(repoRootForTest(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
	for _, flow := range bundle.FlowTree.ByID {
		if flow != nil {
			flow.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
		}
	}
	source := semanticview.Wrap(bundle)
	catalog := testProviderTriggerCatalog(t)
	makeContext := func(hash, alias, runID, entityID string) (runtimepkg.BundleContext, *processIngressProofStore, *processIngressEventStore, processIngressCredentialStore) {
		persistence := &processIngressProofStore{}
		eventsStore := &processIngressEventStore{}
		persistence.store = eventsStore
		workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
		bus, err := runtimebus.NewEphemeralEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{
			BundleSourceFact:       mustServeTestEphemeralBundleSourceFact(hash),
			ProviderOutputVerifier: catalog,
			WorkOwner:              workOwner,
			ReceiverExecution:      eventreceiver.NormalExecution(),
		})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions(%s): %v", alias, err)
		}
		t.Cleanup(func() {
			if err := bus.ResetInMemoryState(); err != nil {
				t.Errorf("retire process ingress test bus %s: %v", alias, err)
			}
		})
		gateway := runtimepkg.NewInboundGateway(bus, nil, nil, executionposture.Live, persistence)
		credentialStore := processIngressCredentialStore{"webhook_signing.telegram": "telegram-secret"}
		gateway.SetCredentialStore(credentialStore)
		plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: alias, Provider: "telegram", SigningSecret: "webhook_signing.telegram"})
		if err != nil {
			t.Fatalf("CompileAdmission(%s): %v", alias, err)
		}
		installed, err := catalog.InstalledCapabilitySubjects()
		if err != nil {
			t.Fatalf("InstalledCapabilitySubjects(%s): %v", alias, err)
		}
		contextDef := runtimepkg.BundleContext{
			BundleSourceFact: mustServeTestEphemeralBundleSourceFact(hash), Source: source,
			Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway}, WorkOwner: workOwner,
			PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, ServiceID: "43000000-0000-0000-0000-000000000001", PackageKey: "telegram-package", FlowID: "telegram-chat", Alias: alias, Provider: "telegram",
				RunID: runID, FlowInstance: "telegram-chat/" + strings.TrimPrefix(alias, "chat-"), InstanceID: alias, EntityID: entityID,
				Generation: 1, PublicationSequence: 1, SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
			}},
		}
		return contextDef, persistence, eventsStore, credentialStore
	}
	hashA := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	hashB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	contextA, persistenceA, eventsA, _ := makeContext(hashA, "chat-a", "41000000-0000-0000-0000-000000000001", "41000000-0000-0000-0000-000000000002")
	contextB, persistenceB, eventsB, credentialsB := makeContext(hashB, "chat-b", "42000000-0000-0000-0000-000000000001", "42000000-0000-0000-0000-000000000002")
	manager, err := runtimepkg.NewRuntimeContextManager(nil, contextA, contextB)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
			t.Errorf("quiesce process ingress runtime contexts: %v", err)
		}
	})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat-b/telegram", strings.NewReader(`{"update_id":99,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	rec := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("selected-context response = %d %q commit_error=%v, want 202", rec.Code, rec.Body.String(), persistenceB.lastError)
	}
	if persistenceA.recorded || len(eventsA.events) != 0 {
		t.Fatalf("non-selected context A was touched: publication=%v events=%d", persistenceA.recorded, len(eventsA.events))
	}
	if !persistenceB.recorded || len(eventsB.events) != 2 {
		t.Fatalf("selected context B publication/events = %v/%d error=%v, want true and raw plus normalized", persistenceB.recorded, len(eventsB.events), persistenceB.lastError)
	}
	if got := eventsB.events[0].RunID(); got != contextB.StandingTargets[0].RunID {
		t.Fatalf("selected event run_id = %q, want %q", got, contextB.StandingTargets[0].RunID)
	}

	credentialsB["webhook_signing.telegram"] = "telegram-secret-v2"
	rotatedBody := `{"update_id":100,"message":{"message_id":8,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"rotated"}}`
	stale := httptest.NewRequest(http.MethodPost, "/webhooks/chat-b/telegram", strings.NewReader(rotatedBody))
	stale.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	staleRecorder := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(staleRecorder, stale)
	if staleRecorder.Code != http.StatusUnauthorized || len(eventsB.events) != 2 {
		t.Fatalf("stale signing secret status/events = %d/%d, want 401/2", staleRecorder.Code, len(eventsB.events))
	}
	current := httptest.NewRequest(http.MethodPost, "/webhooks/chat-b/telegram", strings.NewReader(rotatedBody))
	current.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v2")
	currentRecorder := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(currentRecorder, current)
	if currentRecorder.Code != http.StatusAccepted || len(eventsB.events) != 4 {
		t.Fatalf("current signing secret status/events = %d/%d, want 202/4", currentRecorder.Code, len(eventsB.events))
	}
}
