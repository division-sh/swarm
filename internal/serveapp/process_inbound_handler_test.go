package serveapp

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
	makeContext := func(alias, runID string) (runtimepkg.BundleContext, *processIngressProofStore, *processIngressEventStore, processIngressCredentialStore) {
		contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
		renamed := filepath.Join(filepath.Dir(contractsRoot), alias)
		if err := os.Rename(contractsRoot, renamed); err != nil {
			t.Fatal(err)
		}
		contractsRoot = renamed
		disableChannelOnboardingBusinessConsumers(t, contractsRoot)
		schemaPath := filepath.Join(contractsRoot, "schema.yaml")
		schema, err := os.ReadFile(schemaPath)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, schemaPath, string(schema)+"\n# context: "+alias+"\n")
		_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), contractsRoot)
		if err != nil {
			t.Fatalf("load standing fixture: %v", err)
		}
		rooted, catalog := registrationFixtureTriggerCatalog(t, bundle, ".")
		source := processIngressTransportSource(t, bundle, catalog)
		hash := bundle.SourceArtifact.BundleHash()
		persistence := &processIngressProofStore{}
		eventsStore := newProcessIngressEventStore(t, source, runID)
		persistence.store = eventsStore
		workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
		bus, err := runtimebus.NewEphemeralEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{
			ContractBundle:         source,
			Durable:                runtimebus.DurableDependencies{Instances: eventsStore, ConstructionPublications: eventsStore},
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
		credentialStore := processIngressCredentialStore{"telegram.webhook_signing_secret": "telegram-secret"}
		setTestInboundCredentialAdmission(t, gateway, credentialStore)
		declarations, err := runtimepkg.ResolveStandingTargetDeclarations(source, catalog, nil)
		if err != nil || len(declarations) != 1 || len(declarations[0].Ingress) != 1 || declarations[0].Alias != alias {
			t.Fatalf("standing declarations(%s): %+v err=%v", alias, declarations, err)
		}
		declaration := declarations[0]
		binding := declaration.Ingress[0]
		installed, err := catalog.InstalledCapabilitySubjects()
		if err != nil {
			t.Fatalf("InstalledCapabilitySubjects(%s): %v", alias, err)
		}
		contextDef := runtimepkg.BundleContext{
			SourceArtifactFact: mustServeTestEphemeralSourceArtifactFact(hash), RootAdmission: rooted.Admission(), Source: source,
			Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway}, WorkOwner: workOwner,
			PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerCatalog: catalog, ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, ServiceID: runtimeflowidentity.StandingServiceID("."), FlowPath: ".", Alias: alias, Provider: "telegram", SourcePath: declaration.SourcePath,
				RunID:      runID,
				Generation: 1, PublicationSequence: 1, SigningSecret: binding.SigningSecret, AdmissionPlan: binding.AdmissionPlan,
			}},
		}
		return contextDef, persistence, eventsStore, credentialStore
	}
	contextA, persistenceA, eventsA, _ := makeContext("chat-a", "41000000-0000-0000-0000-000000000001")
	contextB, persistenceB, eventsB, credentialsB := makeContext("chat-b", "42000000-0000-0000-0000-000000000001")
	contextUnavailable, persistenceUnavailable, eventsUnavailable, _ := makeContext("chat-unavailable", "43000000-0000-0000-0000-000000000001")
	if contextA.SourceArtifactFact.Matches(contextB.SourceArtifactFact) {
		t.Fatal("selected context fixture collapsed two distinct admitted sources")
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, contextA, contextB, contextUnavailable)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
			t.Errorf("quiesce process ingress runtime contexts: %v", err)
		}
	})

	t.Run("unauthenticated_acquisition_refusal", func(t *testing.T) {
		if err := contextA.WorkOwner.Fence(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := contextA.WorkOwner.Reopen(); err != nil {
				t.Errorf("restore original runtime admission: %v", err)
			}
		})
		use, lookup, err := manager.AcquireIngress(context.Background(), "chat-a", "telegram")
		if use != nil {
			_ = use.Done()
			t.Fatal("fenced runtime admitted ingress work")
		}
		if !lookup.Found || !lookup.Loaded() || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("wrong acquisition boundary: lookup=%+v error=%v", lookup, err)
		}
		processLog := &lockedBuffer{}
		previousLog := log.Writer()
		log.SetOutput(io.MultiWriter(previousLog, processLog))
		t.Cleanup(func() { log.SetOutput(previousLog) })
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/webhooks/chat-a/telegram", strings.NewReader(`{"update_id":98}`))
		runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable || response.Body.String() != "ingress target is unavailable\n" || strings.Contains(response.Body.String(), err.Error()) {
			t.Fatalf("public acquisition response disclosed internal facts: status=%d body=%q", response.Code, response.Body.String())
		}
		if !strings.Contains(processLog.String(), err.Error()) {
			t.Fatalf("acquisition cause missing from internal diagnostics: %s", processLog.String())
		}
		if persistenceA.recorded || persistenceB.recorded || persistenceUnavailable.recorded || len(eventsA.events)+len(eventsB.events)+len(eventsUnavailable.events) != 0 {
			t.Fatal("unauthenticated acquisition refusal published work")
		}
	})
	t.Run("unauthenticated_unavailable_target", func(t *testing.T) {
		const privateCause = "private-unavailable-runtime-diagnostic"
		result := manager.DeactivateBundleHash(contextUnavailable.SourceArtifactFact.BundleHash(), privateCause)
		if !result.Found || result.State != runtimepkg.RuntimeContextStateUnloaded || result.Cause != privateCause || result.ShutdownErr != nil {
			t.Fatalf("unavailable target precondition: %+v", result)
		}
		processLog := &lockedBuffer{}
		previousLog := log.Writer()
		log.SetOutput(io.MultiWriter(previousLog, processLog))
		t.Cleanup(func() { log.SetOutput(previousLog) })
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/webhooks/chat-unavailable/telegram", strings.NewReader(`{"update_id":98}`))
		runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(response, request)
		if response.Code != http.StatusServiceUnavailable || response.Body.String() != "ingress target is unavailable\n" || strings.Contains(response.Body.String(), privateCause) {
			t.Fatalf("public unavailable response disclosed internal facts: status=%d body=%q", response.Code, response.Body.String())
		}
		if !strings.Contains(processLog.String(), privateCause) {
			t.Fatalf("unavailability cause missing from internal diagnostics: %s", processLog.String())
		}
		if persistenceA.recorded || persistenceB.recorded || persistenceUnavailable.recorded || len(eventsA.events)+len(eventsB.events)+len(eventsUnavailable.events) != 0 {
			t.Fatal("unavailable target published work")
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

	credentialsB["telegram.webhook_signing_secret"] = "telegram-secret-v2"
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

func TestProcessIngressIndexRequiresSelectedScope(t *testing.T) {
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), root)
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	const runID = "41000000-0000-0000-0000-000000000001"
	store := newProcessIngressEventStore(t, source, runID)
	lookup := store.observation.Selection()
	observed, found, err := store.LookupFlowInstance(t.Context(), lookup)
	if err != nil || !found || observed.Owner() != store.observation.Owner() {
		t.Fatalf("exact index lookup = %+v found=%t err=%v", observed.Owner(), found, err)
	}
	for _, owners := range [][]runtimeflowidentity.RunScopedFlowInstance{nil, {observed.Owner()}} {
		scope, err := runtimepipeline.NewFlowInstanceLookupScope(source, observed.SourceFact(), runID, nil, owners)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := store.ListFlowInstances(t.Context(), scope)
		if err != nil || len(selected) != len(owners) {
			t.Fatalf("index ignored its exact scope: %+v err=%v", selected, err)
		}
	}
	const foreignRun = "42000000-0000-0000-0000-000000000001"
	foreignOwner, err := runtimeflowidentity.StandingForGeneration(source, ".", foreignRun)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := runtimepipeline.NewExactFlowInstanceLookup(source, observed.SourceFact(), runtimeflowidentity.RunScopedFlowInstance{RunID: foreignRun, Route: foreignOwner.Route()})
	if err != nil {
		t.Fatal(err)
	}
	if observed, found, err := store.LookupFlowInstance(t.Context(), foreign); err != nil || found || observed.Valid() {
		t.Fatalf("foreign selected run borrowed native root: valid=%t found=%t err=%v", observed.Valid(), found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := store.LookupFlowInstance(ctx, lookup); !errors.Is(err, context.Canceled) {
		t.Fatalf("index lookup lost cancellation: %v", err)
	}
}

func TestProcessIngressConstructionObservationRequiresExactOwner(t *testing.T) {
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := cliapp.NewSwarmWorkflowModule(repoRootForTest(), root)
	if err != nil {
		t.Fatal(err)
	}
	const runID = "41000000-0000-0000-0000-000000000001"
	store := newProcessIngressEventStore(t, semanticview.Wrap(bundle), runID)
	identity := store.observation.Identity()
	owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, identity.Route())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		owner  runtimeflowidentity.RunScopedFlowInstance
		entity string
		valid  bool
	}{
		{name: "exact", owner: owner, entity: identity.EntityID, valid: true},
		{name: "foreign run", owner: runtimeflowidentity.RunScopedFlowInstance{RunID: "42000000-0000-0000-0000-000000000001", Route: owner.Route}, entity: identity.EntityID},
		{name: "foreign route", owner: runtimeflowidentity.RunScopedFlowInstance{RunID: runID}, entity: identity.EntityID},
		{name: "foreign entity", owner: owner, entity: "42000000-0000-0000-0000-000000000002"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof, err := store.LoadFlowConstructionPublication(t.Context(), tc.owner, tc.entity)
			if tc.valid {
				if err != nil || proof.Identity != identity {
					t.Fatalf("exact fixture observation = %+v, %v", proof, err)
				}
			} else if err == nil || proof.Identity != (runtimeflowidentity.Instance{}) {
				t.Fatalf("foreign fixture owner accepted: %+v, %v", proof, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.LoadFlowConstructionPublication(ctx, owner, identity.EntityID); !errors.Is(err, context.Canceled) {
		t.Fatalf("fixture observer lost cancellation: %v", err)
	}
	if _, err := (&processIngressEventStore{}).LoadFlowConstructionPublication(t.Context(), runtimeflowidentity.RunScopedFlowInstance{}, ""); err == nil {
		t.Fatal("empty fixture observation authorized an empty owner")
	}
}

// These transport/controller fixtures use real provider declarations without
// conversation work. Full source-loaded delivery is covered by the HTTP matrix.
func processIngressTransportSource(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, catalog *providertriggers.CatalogSnapshot) semanticview.Source {
	t.Helper()
	source, err := runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	return source
}
