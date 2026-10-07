package serveapp

import (
	"context"
	"errors"
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
		if err := runtimecontracts.ValidateIngressAlias(alias); err != nil {
			t.Fatalf("ValidateIngressAlias(%q): %v", alias, err)
		}
		gotAlias, provider, ok := parseProcessWebhookPath("/webhooks/" + alias + "/telegram")
		if !ok || gotAlias != alias || provider != "telegram" {
			t.Fatalf("parseProcessWebhookPath(%q) = %q/%q/%v", alias, gotAlias, provider, ok)
		}
	}
	for _, alias := range []string{"chat/support", "chat support", "chat%2Fsupport", "-chat", ".chat", "chat?x"} {
		if err := runtimecontracts.ValidateIngressAlias(alias); err == nil {
			t.Fatalf("ValidateIngressAlias(%q) error = nil", alias)
		}
	}
	if _, _, ok := parseProcessWebhookPath("/webhooks/chat/support/telegram"); ok {
		t.Fatal("parseProcessWebhookPath accepted a multi-segment alias")
	}
	for _, alias := range []string{" chat", "chat "} {
		if parsed, _, _ := parseProcessWebhookPath("/webhooks/" + alias + "/telegram"); parsed != alias {
			t.Fatalf("parser normalized unadmitted alias %q into %q", alias, parsed)
		}
	}
}

func TestRuntimeProcessInboundHandlerSelectsExactLoadedContext(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), contractsRoot, cliapp.ResolvePath(repoRootForTest(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	catalog := testProviderTriggerCatalog(t)
	source := processIngressTransportSource(t, bundle, catalog)
	makeContext := func(hash, alias, runID, entityID string) (runtimepkg.BundleContext, *processIngressProofStore, *processIngressEventStore, processIngressCredentialStore) {
		persistence := &processIngressProofStore{}
		eventsStore := &processIngressEventStore{}
		persistence.store = eventsStore
		workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
		bus, err := runtimebus.NewEphemeralEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{
			ContractBundle:         source,
			Durable:                runtimebus.DurableDependencies{ActiveFlows: processIngressNoFlowDescriptors{}, TargetOwners: processIngressTargetOwners{{RunID: runID, FlowInstance: "telegram-ingress", EntityID: entityID}}},
			SourceArtifactFact:     mustServeTestEphemeralSourceArtifactFact(hash),
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
		setTestInboundCredentialAdmission(t, gateway, credentialStore)
		plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: alias, Provider: "telegram", SigningSecret: "webhook_signing.telegram"})
		if err != nil {
			t.Fatalf("CompileAdmission(%s): %v", alias, err)
		}
		installed, err := catalog.InstalledCapabilitySubjects()
		if err != nil {
			t.Fatalf("InstalledCapabilitySubjects(%s): %v", alias, err)
		}
		contextDef := runtimepkg.BundleContext{
			SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(hash), Source: source,
			Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway}, WorkOwner: workOwner,
			PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, ServiceID: "43000000-0000-0000-0000-000000000001", FlowPath: "telegram-ingress", Alias: alias, Provider: "telegram",
				RunID: runID, FlowInstance: "telegram-ingress", InstanceID: alias, EntityID: entityID,
				Generation: 1, PublicationSequence: 1, SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
			}},
		}
		return contextDef, persistence, eventsStore, credentialStore
	}
	hashA := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	hashB := "bundle-v2:sha256:" + strings.Repeat("b", 64)
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
	if staleRecorder.Code != http.StatusServiceUnavailable || len(eventsB.events) != 2 {
		t.Fatalf("stale credential admission status/events = %d/%d, want 503/2", staleRecorder.Code, len(eventsB.events))
	}
	current := httptest.NewRequest(http.MethodPost, "/webhooks/chat-b/telegram", strings.NewReader(rotatedBody))
	current.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-v2")
	currentRecorder := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(currentRecorder, current)
	if currentRecorder.Code != http.StatusServiceUnavailable || len(eventsB.events) != 2 {
		t.Fatalf("unadmitted replacement status/events = %d/%d, want 503/2", currentRecorder.Code, len(eventsB.events))
	}
	if !strings.Contains(currentRecorder.Body.String(), "restart or explicitly admit fresh credentials") {
		t.Fatalf("unadmitted replacement has no recovery instruction: %q", currentRecorder.Body.String())
	}
	if strings.Contains(currentRecorder.Body.String(), "telegram-secret") {
		t.Fatalf("unadmitted replacement exposed a signing value: %q", currentRecorder.Body.String())
	}
	sibling := httptest.NewRequest(http.MethodPost, "/webhooks/chat-a/telegram", strings.NewReader(rotatedBody))
	sibling.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	siblingRecorder := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(siblingRecorder, sibling)
	if siblingRecorder.Code != http.StatusAccepted || len(eventsA.events) != 2 || len(eventsB.events) != 2 {
		t.Fatalf("unaffected context status/events A/B = %d/%d/%d, want 202/2/2", siblingRecorder.Code, len(eventsA.events), len(eventsB.events))
	}
	if got := eventsA.events[0].RunID(); got != contextA.StandingTargets[0].RunID {
		t.Fatalf("unaffected context event run_id = %q, want %q", got, contextA.StandingTargets[0].RunID)
	}
}

type processIngressTargetOwners []runtimepkg.StandingTarget

type processIngressNoFlowDescriptors struct{}

func (processIngressNoFlowDescriptors) ListActiveFlowInstanceDescriptors(context.Context, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}

func (processIngressNoFlowDescriptors) ListActiveFlowInstanceDescriptorsForScope(context.Context, string, []string, []string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}

func (processIngressNoFlowDescriptors) ListActiveFlowInstanceDescriptorsForKey(context.Context, string, string, string, string) ([]runtimebus.ActiveFlowInstanceDescriptor, error) {
	return nil, nil
}

func TestProcessIngressTargetOwnersRespectSelectedScope(t *testing.T) {
	owners := processIngressTargetOwners{
		{RunID: "run-a", FlowInstance: "ingress-a", EntityID: "entity-a"},
		{RunID: "run-a", FlowInstance: "ingress-b", EntityID: "entity-b"},
		{RunID: "run-b", FlowInstance: "ingress-a", EntityID: "entity-a"},
	}
	selected, err := owners.ListSelectedRunTargetOwnersForScope(context.Background(), "run-a", []string{"ingress-b"}, "")
	if err != nil || len(selected) != 1 || selected[0].EntityID != "entity-b" {
		t.Fatalf("selected scope = %#v, %v", selected, err)
	}
	if _, err := owners.ListSelectedRunTargetOwnersForScope(context.Background(), "run-a", nil, ""); err == nil {
		t.Fatal("empty scope was accepted")
	}
}

func (owners processIngressTargetOwners) ListSelectedRunTargetOwners(_ context.Context, runID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	var out []runtimebus.ActiveTargetDescriptor
	for _, owner := range owners {
		if owner.RunID == runID {
			out = append(out, runtimebus.ActiveTargetDescriptor{FlowInstance: owner.FlowInstance, EntityID: owner.EntityID})
		}
	}
	return out, nil
}

func (owners processIngressTargetOwners) ListSelectedRunTargetOwnersForScope(ctx context.Context, runID string, instancePaths []string, sourceEntityID string) ([]runtimebus.ActiveTargetDescriptor, error) {
	if len(instancePaths) == 0 && sourceEntityID == "" {
		return nil, errors.New("target owner lookup requires a selected scope")
	}
	all, err := owners.ListSelectedRunTargetOwners(ctx, runID)
	if err != nil {
		return nil, err
	}
	var selected []runtimebus.ActiveTargetDescriptor
	for _, owner := range all {
		if owner.EntityID == sourceEntityID && sourceEntityID != "" {
			selected = append(selected, owner)
			continue
		}
		for _, path := range instancePaths {
			if owner.FlowInstance == path {
				selected = append(selected, owner)
				break
			}
		}
	}
	return selected, nil
}

// These transport/controller fixtures use real provider declarations without
// conversation work. Full source-loaded delivery is covered by the HTTP matrix.
func processIngressTransportSource(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, catalog *providertriggers.CatalogSnapshot) semanticview.Source {
	t.Helper()
	bundle.Agents = nil
	for _, flow := range bundle.FlowTree.ByID {
		if flow != nil {
			flow.Agents = nil
			flow.Nodes = nil
			flow.Schema.Connect = nil
		}
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source, err := runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	return source
}
