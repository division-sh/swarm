package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func runtimeDepsForServeTest(t testing.TB, stores *selectedStoreOwner, cfg *config.Config, options runtimepkg.RuntimeOptions) runtimepkg.RuntimeDeps {
	t.Helper()
	if options.WorkflowModule != nil {
		if bundle, ok := semanticview.Bundle(options.WorkflowModule.SemanticSource()); ok && bundle != nil && bundle.PackInventory != nil && bundle.PackAdmission == nil {
			projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
			if err != nil {
				t.Fatalf("admit serve test pack projection: %v", err)
			}
			bundle.PackAdmission = projection
		}
	}
	if cfg != nil && !cfg.Runtime.ExecutionPosture.Valid() {
		cfg.Runtime.ExecutionPosture = executionposture.Live
	}
	if options.ProviderCredentials == nil {
		options.ProviderCredentials = processIngressCredentialStore{}
	}
	deps := stores.RuntimeDeps()
	deps.Config = cfg
	deps.Options = options
	return deps
}

func testPlatformPackBaseGenerations(t *testing.T) *packartifact.PlatformPackBaseGenerationOwner {
	t.Helper()
	owner, err := packartifact.NewPlatformPackBaseGenerationOwner(packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

type failOnceFinalizeStartupOwnershipStore struct {
	delegate runtimestartupownership.Store

	mu               sync.Mutex
	prepareCount     int
	finalizeAttempts int
	failed           bool
}

func (s *failOnceFinalizeStartupOwnershipStore) AcquireProcessCapability(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.ProcessCapability, error) {
	return s.delegate.AcquireProcessCapability(ctx, req)
}

func (s *failOnceFinalizeStartupOwnershipStore) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepareCount, s.finalizeAttempts
}

func TestRuntimeProjectSupervisorRejectsHarnessInputReplacementBeforeQuiesce(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	spec := runtimecontracts.DefaultPlatformSpecFile(repo)
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repo, root, spec)
	if err != nil {
		t.Fatalf("load harness artifact: %v", err)
	}
	catalog := testProviderTriggerCatalog(t)
	oldRuntime := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := newRuntimeProjectSupervisor(
		repo, spec, nil, serveRuntimePersistence{}, &ready, cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{NoWorkspace: true, Source: "test"},
		nil, nil, catalog, packfixture.EmbeddedBase(t), "/old", &runtimecontracts.WorkflowContractBundle{},
		semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), oldRuntime, true,
	)
	supervisor.loadWorkflow = func(_, contractsRoot, _ string, _ *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if contractsRoot != root {
			t.Fatalf("contracts root = %q, want %q", contractsRoot, root)
		}
		return module, bundle, nil
	}
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		return cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: &config.Config{}}, candidate, nil, nil)
	})
	supervisor.validateSource = func(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) error {
		opts := runtimepkg.DefaultWorkflowContractValidationOptions(nil, executionposture.Live)
		opts.ProviderTriggerCatalog = catalog
		_, err := runtimepkg.ValidateWorkflowContractSurface(ctx, source, opts)
		return err
	}
	supervisor.quiesceRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error {
		t.Fatal("quiesce must not run after harness validation rejection")
		return nil
	}
	supervisor.createRuntime = func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		t.Fatal("replacement runtime must not be created after harness validation rejection")
		return nil, nil
	}

	_, err = supervisor.OpenProject(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness") {
		t.Fatalf("OpenProject error = %v, want harness production rejection", err)
	}
	if supervisor.CurrentRuntime() != oldRuntime || !ready.Load() {
		t.Fatal("harness replacement disturbed the ready predecessor runtime")
	}
}

func TestRuntimeProjectSupervisorRejectsExecutionPostureChangeBeforeQuiesceOrPublication(t *testing.T) {
	oldRuntime := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	candidateRuntime := &runtimepkg.Runtime{ExecutionPosture: executionposture.MockOnly}
	var ready atomic.Bool
	ready.Store(true)
	var quiesced, started atomic.Int32
	supervisor := &runtimeProjectSupervisor{
		ready:            &ready,
		currentRoot:      "/tmp/old-project",
		currentBundle:    &runtimecontracts.WorkflowContractBundle{},
		currentSource:    semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:        oldRuntime,
		executionPosture: executionposture.Live,
	}
	supervisor.quiesceRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error {
		quiesced.Add(1)
		return nil
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error {
		started.Add(1)
		return nil
	}

	_, err := supervisor.replaceCurrentRuntimeWithSource(
		context.Background(), "/tmp/candidate", semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		&runtimecontracts.WorkflowContractBundle{}, runtimecorrelation.BundleSourceFact{}, runtimecontracts.BundleIdentity{},
		candidateRuntime, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "runtime replacement cannot change process execution posture") {
		t.Fatalf("replacement error = %v, want process posture rejection", err)
	}
	if quiesced.Load() != 0 || started.Load() != 0 {
		t.Fatalf("rejected replacement lifecycle calls = quiesce:%d start:%d, want zero", quiesced.Load(), started.Load())
	}
	if supervisor.CurrentRuntime() != oldRuntime || !ready.Load() {
		t.Fatal("rejected replacement changed predecessor publication or readiness")
	}
}

func TestRuntimeProjectSupervisorReloadRecompilesAndInstallsChannelPlans(t *testing.T) {
	projectRoot := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	module, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), projectRoot, runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot()))
	if err != nil {
		t.Fatalf("NewSwarmWorkflowModule: %v", err)
	}
	var captured runtimepkg.RuntimeDeps
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		captured = deps
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.loadWorkflow = func(_, contractsRoot, _ string, _ *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if contractsRoot != projectRoot {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", contractsRoot, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	loads := 0
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		loads++
		if candidate == nil || candidate.PackInventory == nil {
			t.Fatal("bundle pack reload compiler received no effective inventory")
		}
		cfg := &config.Config{Channels: config.ChannelsConfig{
			Bindings: map[string]config.ChannelBindingConfig{
				"ops": {Pack: "provider.telegram.hitl_channel", Destination: "42"},
			},
		}}
		return cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: cfg}, candidate, nil, nil)
	})

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if loads != 1 {
		t.Fatalf("channel compiler calls = %d, want one", loads)
	}
	if len(captured.Options.ChannelPlans) != 1 {
		t.Fatalf("replacement runtime channel plans = %#v", captured.Options.ChannelPlans)
	}
	if len(captured.Options.ChannelOutboundBindings) != 1 {
		t.Fatalf("replacement runtime channel bindings = %#v", captured.Options.ChannelOutboundBindings)
	}
}

func TestRuntimeProjectSupervisorReplaceCurrentRuntime_ClearsReadinessBeforeShutdown(t *testing.T) {
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	newRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: runtimepkg.RuntimeOptions{
		BundleSourceFact: mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("1")),
	}}
	var ready atomic.Bool
	ready.Store(true)

	supervisor := &runtimeProjectSupervisor{
		ready:            &ready,
		currentRoot:      "/tmp/old-project",
		currentBundle:    &runtimecontracts.WorkflowContractBundle{},
		currentSource:    semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:        oldRT,
		executionPosture: executionposture.Live,
	}

	shutdownCalled := false
	startCalled := false
	supervisor.shutdownRuntime = func(_ context.Context, rt *runtimepkg.Runtime, opts runtimepkg.ShutdownOptions) error {
		shutdownCalled = true
		if rt != oldRT {
			t.Fatalf("shutdown runtime = %p, want old runtime %p", rt, oldRT)
		}
		if opts.Grace != runtimepkg.DefaultShutdownGrace {
			t.Fatalf("shutdown grace = %s, want default %s", opts.Grace, runtimepkg.DefaultShutdownGrace)
		}
		if got := supervisor.CurrentRuntime(); got != nil {
			t.Fatalf("CurrentRuntime during shutdown = %p, want nil", got)
		}
		if got := supervisor.CurrentProject(); got.Loaded {
			t.Fatalf("CurrentProject.Loaded during shutdown = true, want false")
		}
		if ready.Load() {
			t.Fatal("ready flag remained true during shutdown")
		}
		return nil
	}
	supervisor.startRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		startCalled = true
		if rt != newRT {
			t.Fatalf("start runtime = %p, want new runtime %p", rt, newRT)
		}
		if got := supervisor.CurrentRuntime(); got != nil {
			t.Fatalf("CurrentRuntime before attach = %p, want nil", got)
		}
		if ready.Load() {
			t.Fatal("ready flag became true before new runtime attached")
		}
		return nil
	}

	status, err := supervisor.replaceCurrentRuntime(
		context.Background(),
		"/tmp/new-project",
		semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		&runtimecontracts.WorkflowContractBundle{},
		newRT,
	)
	if err != nil {
		t.Fatalf("replaceCurrentRuntime: %v", err)
	}
	if !shutdownCalled {
		t.Fatal("expected shutdown to be called")
	}
	if !startCalled {
		t.Fatal("expected start to be called")
	}
	if !ready.Load() {
		t.Fatal("ready flag = false after attach, want true")
	}
	if got := supervisor.CurrentRuntime(); got != newRT {
		t.Fatalf("CurrentRuntime after attach = %p, want new runtime %p", got, newRT)
	}
	if !status.Loaded {
		t.Fatal("status.Loaded = false, want true")
	}
	if status.ProjectDir != "/tmp/new-project" {
		t.Fatalf("status.ProjectDir = %q, want /tmp/new-project", status.ProjectDir)
	}
}

func TestRuntimeProjectSupervisorReplaceCurrentRuntime_WaitsForRuntimeStartBeforeReady(t *testing.T) {
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	newRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: runtimepkg.RuntimeOptions{
		BundleSourceFact: mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("2")),
	}}
	var ready atomic.Bool
	ready.Store(true)
	started := make(chan struct{})
	releaseStart := make(chan struct{})

	supervisor := &runtimeProjectSupervisor{
		ready:            &ready,
		currentRoot:      "/tmp/old-project",
		currentBundle:    &runtimecontracts.WorkflowContractBundle{},
		currentSource:    semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:        oldRT,
		executionPosture: executionposture.Live,
	}
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error {
		return nil
	}
	supervisor.startRuntime = func(ctx context.Context, rt *runtimepkg.Runtime) error {
		if rt != newRT {
			t.Fatalf("start runtime = %p, want new runtime %p", rt, newRT)
		}
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseStart:
			return nil
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := supervisor.replaceCurrentRuntime(
			context.Background(),
			"/tmp/new-project",
			semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
			&runtimecontracts.WorkflowContractBundle{},
			newRT,
		)
		done <- err
	}()

	select {
	case <-started:
	case err := <-done:
		t.Fatalf("replaceCurrentRuntime returned before runtime start blocked: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runtime start")
	}
	if ready.Load() {
		t.Fatal("ready flag became true before runtime start completed")
	}

	close(releaseStart)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("replaceCurrentRuntime after start release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replaceCurrentRuntime")
	}
	if !ready.Load() {
		t.Fatal("ready flag = false after runtime start completed")
	}
}

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
	_, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), contractsRoot, cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath))
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
			BundleSourceFact: mustServeTestEphemeralBundleSourceFact(hash), Source: source, Runtime: &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, InboundGateway: gateway}, WorkOwner: workOwner,
			PackInventoryDigest:       bundle.PackInventory.Digest(),
			ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, ServiceID: "43000000-0000-0000-0000-000000000001", PackageKey: "telegram-package", FlowID: "telegram-chat", Alias: alias, Provider: "telegram",
				RunID: runID, FlowInstance: "telegram-chat/" + strings.TrimPrefix(alias, "chat-"),
				InstanceID: alias, EntityID: entityID, Generation: 1, PublicationSequence: 1, SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
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

func TestRuntimeProjectSupervisorFailedSameHashReplacementRestoresOldContext(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	oldBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: oldBus}
	newRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: newBus}
	restoredRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: oldBus}
	hash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	restoredWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
	fact := mustServeTestPersistedBundleSourceFact(hash)
	bindSupervisorTestRuntimeTopology(t, oldRT, source, fact, oldWorkOwner, runtimeInstanceID)
	bindSupervisorTestRuntimeTopology(t, newRT, source, fact, newWorkOwner, runtimeInstanceID)
	bindSupervisorTestRuntimeTopology(t, restoredRT, source, fact, restoredWorkOwner, runtimeInstanceID)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleSourceFact: fact, Source: source, Runtime: oldRT, WorkOwner: oldWorkOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: oldRT, executionPosture: executionposture.Live,
		currentBundleSourceFact: fact, runtimeContexts: manager,
	}
	installSupervisorTestProcessCapability(t, supervisor, oldRT.Manager, source, fact, runtimeInstanceID)
	supervisor.quiesceRuntime = func(_ context.Context, rt *runtimepkg.Runtime, opts runtimepkg.ShutdownOptions) error {
		return rt.QuiesceForReplacement(opts)
	}
	supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
		return restoredRT, restoredWorkOwner, nil
	}
	supervisor.startRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		if rt == newRT {
			return errors.New("candidate start failed")
		}
		return nil
	}
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	_, replacementErr := supervisor.replaceCurrentRuntimeWithSource(context.Background(), "/tmp/candidate", source, &runtimecontracts.WorkflowContractBundle{}, fact, runtimecontracts.BundleIdentity{BundleHash: hash}, newRT, newWorkOwner)
	if replacementErr == nil || !strings.Contains(replacementErr.Error(), "candidate start failed") {
		t.Fatalf("same-hash replacement error = %v", replacementErr)
	}
	lookup := manager.LookupBundleHashStatus(hash)
	if !ready.Load() || supervisor.CurrentRuntime() != restoredRT || !lookup.Loaded() {
		t.Fatalf("failed same-hash replacement mutated old authority: ready=%v runtime=%p lookup=%#v replacement_err=%v", ready.Load(), supervisor.CurrentRuntime(), lookup, replacementErr)
	}
}

func TestRuntimeProjectSupervisorSameHashReplacementPublishesCandidateEffectiveSource(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	oldBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	hash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
	fact := mustServeTestPersistedBundleSourceFact(hash)
	oldIdentity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	newIdentity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	oldCatalog, err := scenarioexecution.NewCatalog(oldIdentity, nil)
	if err != nil {
		t.Fatal(err)
	}
	newCatalog, err := scenarioexecution.NewCatalog(newIdentity, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldRT := &runtimepkg.Runtime{
		ExecutionPosture: executionposture.Live, Bus: oldBus,
		EffectiveSourceIdentity: oldIdentity, ScenarioProfileCatalog: oldCatalog,
		Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact},
	}
	newRT := &runtimepkg.Runtime{
		ExecutionPosture: executionposture.Live, Bus: newBus,
		EffectiveSourceIdentity: newIdentity, ScenarioProfileCatalog: newCatalog,
		Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact},
	}
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	runtimeInstanceID := "11111111-1111-4111-8111-111111111111"
	bindSupervisorTestRuntimeTopology(t, oldRT, source, fact, oldWorkOwner, runtimeInstanceID)
	bindSupervisorTestRuntimeTopology(t, newRT, source, fact, newWorkOwner, runtimeInstanceID)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleSourceFact: fact, Source: source, Runtime: oldRT, WorkOwner: oldWorkOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: oldRT,
		currentBundleSourceFact: fact, executionPosture: executionposture.Live, runtimeContexts: manager,
	}
	installSupervisorTestProcessCapability(t, supervisor, oldRT.Manager, source, fact, runtimeInstanceID)
	supervisor.quiesceRuntime = func(_ context.Context, rt *runtimepkg.Runtime, opts runtimepkg.ShutdownOptions) error {
		return rt.QuiesceForReplacement(opts)
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.replaceCurrentRuntimeWithSource(
		context.Background(), "/tmp/candidate", source, &runtimecontracts.WorkflowContractBundle{}, fact,
		runtimecontracts.BundleIdentity{BundleHash: hash}, newRT, newWorkOwner,
	); err != nil {
		t.Fatalf("replace same-hash runtime: %v", err)
	}
	lookup := manager.LookupBundleHashStatus(hash)
	if !ready.Load() || supervisor.CurrentRuntime() != newRT || !lookup.Loaded() {
		t.Fatalf("replacement publication = ready:%v runtime:%p lookup:%#v", ready.Load(), supervisor.CurrentRuntime(), lookup)
	}
	if !lookup.Context.EffectiveSourceIdentity.Equal(newIdentity) {
		t.Fatalf("replacement effective identity = %s, want %s", lookup.Context.EffectiveSourceIdentity.Digest(), newIdentity.Digest())
	}
	if lookup.Context.ScenarioProfileCatalog != newCatalog || !lookup.Context.ScenarioProfileCatalog.EffectiveSourceIdentity().Equal(newIdentity) {
		t.Fatal("replacement context did not publish the candidate scenario profile catalog")
	}
}

func TestRuntimeProjectSupervisorChangedNonStandingBundleReplacesManagerContext(t *testing.T) {
	oldBundle := &runtimecontracts.WorkflowContractBundle{}
	newBundle := &runtimecontracts.WorkflowContractBundle{}
	oldSource := semanticview.Wrap(oldBundle)
	newSource := semanticview.Wrap(newBundle)
	oldBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: oldBus}
	newRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: newBus}
	oldHash := "bundle-v1:sha256:" + strings.Repeat("1", 64)
	newHash := "bundle-v1:sha256:" + strings.Repeat("2", 64)
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, oldHash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, newHash)
	oldFact := mustServeTestPersistedBundleSourceFact(oldHash)
	newFact := mustServeTestPersistedBundleSourceFact(newHash)
	newIdentity := runtimecontracts.BundleIdentity{BundleHash: newHash}
	runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
	bindSupervisorTestRuntimeTopology(t, oldRT, oldSource, oldFact, oldWorkOwner, runtimeInstanceID)
	bindSupervisorTestRuntimeTopology(t, newRT, newSource, newFact, newWorkOwner, runtimeInstanceID)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleSourceFact: oldFact, Source: oldSource, Runtime: oldRT, WorkOwner: oldWorkOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: oldSource,
		currentBundle: oldBundle, currentRT: oldRT, currentBundleSourceFact: oldFact, executionPosture: executionposture.Live,
		runtimeContexts: manager,
	}
	installSupervisorTestProcessCapability(t, supervisor, oldRT.Manager, oldSource, oldFact, runtimeInstanceID)
	var started, quiesced []*runtimepkg.Runtime
	supervisor.startRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		started = append(started, rt)
		return nil
	}
	supervisor.quiesceRuntime = func(_ context.Context, rt *runtimepkg.Runtime, _ runtimepkg.ShutdownOptions) error {
		quiesced = append(quiesced, rt)
		return rt.QuiesceForReplacement(runtimepkg.DefaultShutdownOptions())
	}
	status, err := supervisor.replaceCurrentRuntimeWithSource(context.Background(), "/tmp/candidate", newSource, newBundle, newFact, newIdentity, newRT, newWorkOwner)
	if err != nil {
		t.Fatalf("replaceCurrentRuntimeWithSource: %v", err)
	}
	if status.ProjectDir != "/tmp/candidate" || !ready.Load() || supervisor.CurrentRuntime() != newRT {
		t.Fatalf("replacement status = %#v ready=%v runtime=%p", status, ready.Load(), supervisor.CurrentRuntime())
	}
	if len(started) != 1 || started[0] != newRT || len(quiesced) != 1 || quiesced[0] != oldRT {
		t.Fatalf("replacement lifecycle started=%v quiesced=%v", started, quiesced)
	}
	if lookup := manager.LookupBundleHashStatus(oldHash); lookup.Loaded() {
		t.Fatalf("old bundle context remained loaded: %#v", lookup)
	}
	lookup := manager.LookupBundleHashStatus(newHash)
	if !lookup.Loaded() || lookup.Context.Runtime != nil || lookup.Context.BundleIdentity.BundleHash != newHash {
		t.Fatalf("new bundle context = %#v", lookup)
	}
	use, _, err := manager.AcquireBundleHash(context.Background(), newHash)
	if err != nil || use == nil || use.Runtime() != newRT {
		t.Fatalf("new bundle execution authority = use:%#v err:%v", use, err)
	}
	if err := use.Done(); err != nil {
		t.Fatalf("settle new bundle execution authority: %v", err)
	}
}

func TestRuntimeProjectSupervisorReplacementPublishesDowntimeAcrossPublicSurfaces(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	source := semanticview.Wrap(bundle)
	oldBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	hash := runtimeContextTestHash("d")
	fact := mustServeTestEphemeralBundleSourceFact(hash)
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: oldBus, Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}}
	newRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: newBus, Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}}
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
	bindSupervisorTestRuntimeTopology(t, oldRT, source, fact, oldWorkOwner, runtimeInstanceID)
	bindSupervisorTestRuntimeTopology(t, newRT, source, fact, newWorkOwner, runtimeInstanceID)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{BundleSourceFact: fact, Source: source, Runtime: oldRT, WorkOwner: oldWorkOwner})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/old", currentSource: source, currentBundle: bundle,
		currentRT: oldRT, currentBundleSourceFact: fact, runtimeContexts: manager, executionPosture: executionposture.Live,
	}
	installSupervisorTestProcessCapability(t, supervisor, oldRT.Manager, source, fact, runtimeInstanceID)
	candidateStart := make(chan struct{})
	releaseCandidate := make(chan struct{})
	supervisor.startRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		if rt == newRT {
			close(candidateStart)
			<-releaseCandidate
		}
		return nil
	}

	var apiCalls, ingressCalls atomic.Int32
	server := newAPIServer(&ready,
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { apiCalls.Add(1); w.WriteHeader(http.StatusNoContent) }),
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { ingressCalls.Add(1); w.WriteHeader(http.StatusAccepted) }),
	)
	replacementDone := make(chan error, 1)
	go func() {
		_, err := supervisor.replaceCurrentRuntimeWithSource(context.Background(), "/new", source, bundle, fact, runtimecontracts.BundleIdentity{BundleHash: hash}, newRT, newWorkOwner)
		replacementDone <- err
	}()
	select {
	case <-candidateStart:
	case <-time.After(time.Second):
		t.Fatal("candidate start was not reached")
	}

	assertReplacementHTTPStatus(t, server.Handler, "/readyz", http.StatusServiceUnavailable)
	assertReplacementHTTPStatus(t, server.Handler, "/v1/rpc", http.StatusServiceUnavailable)
	assertReplacementHTTPStatus(t, server.Handler, "/webhooks/chat/telegram", http.StatusServiceUnavailable)
	if apiCalls.Load() != 0 || ingressCalls.Load() != 0 {
		t.Fatalf("unready request reached API/ingress handlers: api=%d ingress=%d", apiCalls.Load(), ingressCalls.Load())
	}
	lookup := manager.LookupBundleHashStatus(hash)
	if lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseReplacing {
		t.Fatalf("manager lookup during replacement = %#v", lookup)
	}

	close(releaseCandidate)
	if err := <-replacementDone; err != nil {
		t.Fatalf("replaceCurrentRuntimeWithSource: %v", err)
	}
	assertReplacementHTTPStatus(t, server.Handler, "/readyz", http.StatusOK)
	assertReplacementHTTPStatus(t, server.Handler, "/v1/rpc", http.StatusNoContent)
	assertReplacementHTTPStatus(t, server.Handler, "/webhooks/chat/telegram", http.StatusAccepted)
	lookup = manager.LookupBundleHashStatus(hash)
	if !ready.Load() || !lookup.Loaded() || lookup.Context.Runtime != nil || apiCalls.Load() != 1 || ingressCalls.Load() != 1 {
		t.Fatalf("replacement visibility = ready:%v lookup:%#v api:%d ingress:%d", ready.Load(), lookup, apiCalls.Load(), ingressCalls.Load())
	}
	use, _, err := manager.AcquireBundleHash(context.Background(), hash)
	if err != nil || use == nil || use.Runtime() != newRT {
		t.Fatalf("replacement execution authority = use:%#v err:%v", use, err)
	}
	if err := use.Done(); err != nil {
		t.Fatalf("settle replacement execution authority: %v", err)
	}
}

func assertReplacementHTTPStatus(t *testing.T, handler http.Handler, path string, want int) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != want {
		t.Fatalf("%s status/body = %d/%q, want %d", path, rec.Code, rec.Body.String(), want)
	}
}

func TestStandingReplacementAdoptionRestoresWorkflowTimersOnBothStores(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) *selectedStoreOwner
	}
	backends := []backend{
		{
			name: "sqlite",
			open: func(t *testing.T) *selectedStoreOwner {
				owner := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime.sqlite"), &config.Config{})
				t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
				return owner
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) *selectedStoreOwner {
				dsn, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				owner := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
				t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
				return owner
			},
		},
	}

	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
			schemaPath := filepath.Join(contractsRoot, "flows", "telegram-ingress", "schema.yaml")
			rawSchema, err := os.ReadFile(schemaPath)
			if err != nil {
				t.Fatalf("read standing schema: %v", err)
			}
			withTimer := strings.Replace(string(rawSchema), "  active:\n    initial: true\n    gate:", "  active:\n    initial: true\n    timers:\n      - after: 2s\n        advances_to: done\n    gate:", 1)
			if withTimer == string(rawSchema) {
				t.Fatal("standing initial-stage timer insertion point not found")
			}
			writeStandingCandidateFile(t, schemaPath, withTimer)

			repoRoot := cliapp.RepoRoot()
			module, bundle, err := cliapp.NewSwarmWorkflowModule(repoRoot, contractsRoot, cliapp.ResolvePath(repoRoot, defaultPlatformSpecPath))
			if err != nil {
				t.Fatalf("load standing workflow module: %v", err)
			}
			stores := backend.open(t)
			if _, err := initializeStateStores(context.Background(), stores.Schema(), bundle); err != nil {
				t.Fatalf("initialize state stores: %v", err)
			}
			bundleHash, err := runtimecontracts.BundleHash(bundle)
			if err != nil {
				t.Fatalf("BundleHash: %v", err)
			}
			fact := mustServeTestEphemeralBundleSourceFact(bundleHash)
			credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			for key, value := range map[string]string{
				"telegram_bot_token": "bot-token", "webhook_signing.telegram": "telegram-secret",
			} {
				if err := credentials.Set(context.Background(), key, value); err != nil {
					t.Fatalf("set credential %s: %v", key, err)
				}
			}
			processWorkOwner := worklifetime.NewProcess()
			var runtimes []*runtimepkg.Runtime
			runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
			newRuntime := func() *runtimepkg.Runtime {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{
					WorkflowModule: module, LLMRuntime: servedNoopLLMRuntime{},
					Credentials: credentials, ProviderCredentials: credentials,
					ProviderTriggerCatalog: testProviderTriggerCatalog(t),
					ProcessWorkOwner:       processWorkOwner,
					RuntimeInstanceID:      runtimeInstanceID, BundleSourceFact: fact,
				}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				runtimes = append(runtimes, rt)
				return rt
			}

			predecessor := newRuntime()
			processCapability, plan := installSelectedStoreTestProcessTopology(t, stores, predecessor, semanticview.Wrap(bundle), fact, runtimeInstanceID)
			t.Cleanup(func() {
				shutdownFailed := false
				for i := len(runtimes) - 1; i >= 0; i-- {
					if err := runtimes[i].ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}); err != nil {
						t.Errorf("shutdown standing replacement runtime: %v", err)
						shutdownFailed = true
					}
				}
				if shutdownFailed {
					return
				}
				if err := closeSelectedStoreTestProcess(processWorkOwner, processCapability); err != nil {
					t.Errorf("close standing replacement generation: %v", err)
				}
			})
			if err := predecessor.Start(context.Background()); err != nil {
				t.Fatalf("start standing predecessor: %v", err)
			}
			_, initial, err := predecessor.EnsureStandingTargets(context.Background())
			if err != nil {
				t.Fatalf("create standing target: %v", err)
			}
			if len(initial) != 1 || !initial[0].Created || initial[0].EffectiveState != "active" {
				t.Fatalf("initial standing activation = %#v", initial)
			}
			if err := predecessor.QuiesceForReplacement(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}); err != nil {
				t.Fatalf("quiesce standing predecessor: %v", err)
			}

			candidate := newRuntime()
			installSelectedStoreTestGeneration(t, processCapability, candidate, plan, 2)
			if err := candidate.PrepareAuthorActivityCatalog(); err != nil {
				t.Fatalf("prepare standing replacement candidate author activity: %v", err)
			}
			if err := candidate.Start(context.Background()); err != nil {
				t.Fatalf("start standing replacement candidate: %v", err)
			}
			var timerEvent, timerStatus string
			var fireAt any
			if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), `SELECT fire_event, status, fire_at FROM timers`).Scan(&timerEvent, &timerStatus, &fireAt); err != nil {
				t.Fatalf("load standing workflow timer: %v", err)
			}
			if timerStatus != "active" {
				t.Fatalf("initial standing workflow timer status = %q, want active", timerStatus)
			}

			_, adopted, err := candidate.EnsureStandingReplacementTargets(context.Background(), predecessor)
			if err != nil {
				t.Fatalf("adopt standing replacement: %v", err)
			}
			if len(adopted) != 1 || adopted[0].Created || adopted[0].RunID != initial[0].RunID {
				t.Fatalf("adopted standing activation = %#v, want existing run %s", adopted, initial[0].RunID)
			}
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), `SELECT status FROM timers`).Scan(&timerStatus); err != nil {
					t.Fatalf("reload standing workflow timer: %v", err)
				}
				if timerStatus == "fired" {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}
			if timerStatus != "fired" {
				t.Fatalf("adopted standing workflow timer status = %q at %s (due %v), want fired", timerStatus, time.Now().UTC(), fireAt)
			}
			query := `SELECT COUNT(*) FROM events WHERE event_name = ?`
			if backend.name == "postgres" {
				query = `SELECT COUNT(*) FROM events WHERE event_name = $1`
			}
			var events int
			if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), query, timerEvent).Scan(&events); err != nil {
				t.Fatalf("count adopted standing timer events: %v", err)
			}
			if events != 1 {
				t.Fatalf("adopted standing timer events = %d, want 1", events)
			}
		})
	}
}

func TestRuntimeProjectSupervisorStandingReplacementPublishesAdoptedTimerAtomicallyOnBothStores(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) *selectedStoreOwner
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) *selectedStoreOwner {
			owner := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime.sqlite"), &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
		{name: "postgres", open: func(t *testing.T) *selectedStoreOwner {
			dsn, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			owner := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			for _, changedHash := range []bool{false, true} {
				changedHash := changedHash
				name := "same_hash"
				if changedHash {
					name = "changed_hash"
				}
				t.Run(name, func(t *testing.T) {
					contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
					schemaPath := filepath.Join(contractsRoot, "flows", "telegram-ingress", "schema.yaml")
					rawSchema, err := os.ReadFile(schemaPath)
					if err != nil {
						t.Fatalf("read standing schema: %v", err)
					}
					withTimer := strings.Replace(string(rawSchema), "  active:\n    initial: true\n    gate:", "  active:\n    initial: true\n    timers:\n      - after: 5s\n        advances_to: done\n    gate:", 1)
					if withTimer == string(rawSchema) {
						t.Fatal("standing timer insertion point not found")
					}
					writeStandingCandidateFile(t, schemaPath, withTimer)

					repoRoot := cliapp.RepoRoot()
					module, bundle, err := cliapp.NewSwarmWorkflowModule(repoRoot, contractsRoot, cliapp.ResolvePath(repoRoot, defaultPlatformSpecPath))
					if err != nil {
						t.Fatalf("load standing workflow module: %v", err)
					}
					stores := backend.open(t)
					if _, err := initializeStateStores(context.Background(), stores.Schema(), bundle); err != nil {
						t.Fatalf("initialize state stores: %v", err)
					}
					oldHash, err := runtimecontracts.BundleHash(bundle)
					if err != nil {
						t.Fatalf("BundleHash: %v", err)
					}
					oldSource := semanticview.Wrap(bundle)
					candidateModule := module
					candidateBundle := bundle
					candidateSource := oldSource
					newHash := oldHash
					credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
					if err != nil {
						t.Fatalf("NewFileStore: %v", err)
					}
					for key, value := range map[string]string{
						"telegram_bot_token": "bot-token", "webhook_signing.telegram": "telegram-secret",
					} {
						if err := credentials.Set(context.Background(), key, value); err != nil {
							t.Fatalf("set credential %s: %v", key, err)
						}
					}
					catalog := testProviderTriggerCatalog(t)
					installed, err := catalog.InstalledCapabilitySubjects()
					if err != nil {
						t.Fatalf("installed capability subjects: %v", err)
					}
					processWorkOwner := worklifetime.NewProcess()
					var manager *runtimepkg.RuntimeContextManager
					var createdRuntimes []*runtimepkg.Runtime
					var processCapability runtimestartupownership.ProcessCapability
					t.Cleanup(func() {
						shutdownFailed := false
						if manager != nil {
							for _, result := range manager.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
								if result.ShutdownErr != nil {
									t.Errorf("deactivate replacement context %s: %v", result.BundleHash, result.ShutdownErr)
									shutdownFailed = true
								}
							}
						}
						for i := len(createdRuntimes) - 1; i >= 0; i-- {
							if err := createdRuntimes[i].ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second}); err != nil {
								t.Errorf("shutdown standing runtime: %v", err)
								shutdownFailed = true
							}
						}
						if shutdownFailed {
							return
						}
						if err := closeSelectedStoreTestProcess(processWorkOwner, processCapability); err != nil {
							t.Errorf("close standing selected-store generation: %v", err)
						}
					})
					newRuntime := func(hash string, workflowModule runtimepipeline.WorkflowModule) *runtimepkg.Runtime {
						rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{
							WorkflowModule: workflowModule, LLMRuntime: servedNoopLLMRuntime{},
							Credentials: credentials, ProviderCredentials: credentials,
							ProviderTriggerCatalog: catalog, ProcessWorkOwner: processWorkOwner,
							RuntimeInstanceID: "11111111-1111-1111-1111-111111111111",
							BundleSourceFact:  mustServeTestEphemeralBundleSourceFact(hash),
						}))
						if err != nil {
							t.Fatalf("NewRuntime(%s): %v", hash, err)
						}
						createdRuntimes = append(createdRuntimes, rt)
						return rt
					}

					predecessor := newRuntime(oldHash, module)
					oldFact := mustServeTestEphemeralBundleSourceFact(oldHash)
					processCapability, _ = installSelectedStoreTestProcessTopology(t, stores, predecessor, oldSource, oldFact, "11111111-1111-1111-1111-111111111111")
					if err := predecessor.Start(context.Background()); err != nil {
						t.Fatalf("start predecessor: %v", err)
					}
					targets, activations, err := predecessor.EnsureStandingTargets(context.Background())
					if err != nil {
						t.Fatalf("ensure predecessor standing targets: %v", err)
					}
					if len(targets) != 1 || len(activations) != 1 || !activations[0].Created {
						t.Fatalf("predecessor standing targets/activations = %#v/%#v", targets, activations)
					}
					manager, err = runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
						BundleSourceFact: oldFact, Source: oldSource,
						Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence(), StandingTargets: targets,
						ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
						PackInventoryDigest: bundle.PackInventory.Digest(),
					})
					if err != nil {
						t.Fatalf("NewRuntimeContextManager: %v", err)
					}
					if changedHash {
						writeStandingCandidateFile(t, filepath.Join(contractsRoot, "bot", "flows", "telegram-chat", "prompts", "phrase-bot.md"), "Reply to each Telegram message by emitting telegram.reply_requested with chat_id set to the event conversation_reference. Keep the response concise.\n")
						candidateModule, candidateBundle, err = cliapp.NewSwarmWorkflowModule(repoRoot, contractsRoot, cliapp.ResolvePath(repoRoot, defaultPlatformSpecPath))
						if err != nil {
							t.Fatalf("load changed-hash standing workflow module: %v", err)
						}
						candidateSource = semanticview.Wrap(candidateBundle)
						newHash, err = runtimecontracts.BundleHash(candidateBundle)
						if err != nil {
							t.Fatalf("changed BundleHash: %v", err)
						}
						if newHash == oldHash {
							t.Fatal("changed standing bundle retained predecessor hash")
						}
					}
					candidate := newRuntime(newHash, candidateModule)
					newFact := mustServeTestEphemeralBundleSourceFact(newHash)
					supervisor := &runtimeProjectSupervisor{
						ready: new(atomic.Bool), currentRoot: contractsRoot, currentSource: oldSource, currentBundle: bundle,
						currentRT: predecessor, currentBundleSourceFact: oldFact, runtimeContexts: manager, executionPosture: executionposture.Live,
						providerTriggers: catalog, replacementShutdown: runtimepkg.ShutdownOptions{Grace: 5 * time.Second},
					}
					supervisor.runtimeInstanceID = "11111111-1111-1111-1111-111111111111"
					supervisor.SetProcessCapability(processCapability)
					supervisor.ready.Store(true)
					packCandidate := &bundlePackCandidate{
						catalog: catalog, generation: catalog.Generation(), installedSubjects: installed,
						inventoryDigest: candidateBundle.PackInventory.Digest(),
					}
					if _, err := supervisor.replaceCurrentRuntimeWithSourceAndPacks(
						context.Background(), contractsRoot, candidateSource, candidateBundle, newFact,
						runtimecontracts.BundleIdentity{BundleHash: newHash}, candidate, candidate.WorkOccurrence(), packCandidate,
					); err != nil {
						t.Fatalf("replace standing runtime: %v", err)
					}
					if supervisor.CurrentRuntime() != candidate || !supervisor.ready.Load() {
						t.Fatal("standing candidate did not publish after aggregate timer transfer")
					}

					var timerEvent, timerStatus string
					if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), `SELECT fire_event, status FROM timers`).Scan(&timerEvent, &timerStatus); err != nil {
						t.Fatalf("load adopted timer: %v", err)
					}
					if timerStatus != "active" {
						t.Fatalf("adopted timer status at publication = %q, want active", timerStatus)
					}
					query := `SELECT COUNT(*) FROM events WHERE event_name = ?`
					if backend.name == "postgres" {
						query = `SELECT COUNT(*) FROM events WHERE event_name = $1`
					}
					var count int
					if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), query, timerEvent).Scan(&count); err != nil {
						t.Fatalf("count adopted timer events: %v", err)
					}
					if count != 0 {
						t.Fatalf("adopted timer events at publication = %d, want 0", count)
					}
					if changedHash {
						deadline := time.Now().Add(10 * time.Second)
						for time.Now().Before(deadline) {
							if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), `SELECT status FROM timers`).Scan(&timerStatus); err != nil {
								t.Fatalf("reload changed-hash adopted timer: %v", err)
							}
							if timerStatus == "fired" {
								break
							}
							time.Sleep(25 * time.Millisecond)
						}
						if timerStatus != "fired" {
							t.Fatalf("changed-hash adopted timer status = %q, want candidate lifecycle fire", timerStatus)
						}
						if err := selectedStoreDatabaseForTest(t, stores).QueryRowContext(context.Background(), query, timerEvent).Scan(&count); err != nil {
							t.Fatalf("count changed-hash adopted timer events: %v", err)
						}
						if count != 1 {
							t.Fatalf("changed-hash adopted timer events = %d, want 1 from candidate lifecycle", count)
						}
					}
				})
			}
		})
	}
}

func TestRuntimeProjectSupervisorQuiesceTimeoutRestoresFullStoreAuthority(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) *selectedStoreOwner
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) *selectedStoreOwner {
			owner := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime.sqlite"), &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
		{name: "postgres", open: func(t *testing.T) *selectedStoreOwner {
			dsn, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			owner := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			processWorkOwner := worklifetime.NewProcess()
			var runtimes []*runtimepkg.Runtime
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores.Schema(), bundle); err != nil {
				t.Fatalf("initializeStateStores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			providerRegistry := testProviderTriggerCatalog(t)
			hash := runtimeContextTestHash("f")
			runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
			fact := mustServeTestEphemeralBundleSourceFact(hash)
			newRuntime := func() *runtimepkg.Runtime {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{SelfCheck: false, WorkflowModule: stubWorkflowModule{source: source}, LLMRuntime: servedNoopLLMRuntime{}, DisablePersistentStartupRecovery: true, ProviderTriggerCatalog: providerRegistry, ProcessWorkOwner: processWorkOwner, BundleSourceFact: fact, RuntimeInstanceID: runtimeInstanceID}))
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				runtimes = append(runtimes, rt)
				return rt
			}
			var active, maxActive atomic.Int32
			blocker := newReplacementQuiesceBlockNode(&active, &maxActive)
			predecessor := newRuntime()
			predecessor.SystemNodes = []runtimepipeline.BackgroundNode{blocker}
			t.Cleanup(blocker.Release)
			processCapability, _ := installSelectedStoreTestProcessTopology(t, stores, predecessor, source, fact, runtimeInstanceID)
			t.Cleanup(func() {
				shutdownFailed := false
				for i := len(runtimes) - 1; i >= 0; i-- {
					if err := runtimes[i].Shutdown(); err != nil {
						t.Errorf("shutdown quiescence runtime: %v", err)
						shutdownFailed = true
					}
				}
				if shutdownFailed {
					return
				}
				if err := closeSelectedStoreTestProcess(processWorkOwner, processCapability); err != nil {
					t.Errorf("close quiescence selected-store generation: %v", err)
				}
			})
			if err := predecessor.Start(context.Background()); err != nil {
				t.Fatalf("start predecessor: %v", err)
			}
			installedTriggerSubjects, err := providerRegistry.InstalledCapabilitySubjects()
			if err != nil {
				t.Fatalf("installed provider-trigger subjects: %v", err)
			}
			manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
				BundleSourceFact: fact, Source: source, Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence(),
				PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: providerRegistry.Generation(),
				InstalledTriggerSubjects: installedTriggerSubjects,
			})
			if err != nil {
				t.Fatalf("NewRuntimeContextManager: %v", err)
			}
			candidate := newRuntime()
			restored := newRuntime()
			restored.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
			var ready atomic.Bool
			ready.Store(true)
			supervisor := &runtimeProjectSupervisor{
				ready: &ready, currentRoot: "/old", currentSource: source, currentBundle: bundle,
				currentRT: predecessor, currentBundleSourceFact: fact, runtimeContexts: manager, executionPosture: executionposture.Live,
				replacementShutdown: runtimepkg.ShutdownOptions{Grace: 20 * time.Millisecond},
			}
			supervisor.runtimeInstanceID = runtimeInstanceID
			supervisor.SetProcessCapability(processCapability)
			supervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
				return restored, restored.WorkOccurrence(), nil
			}
			quiesceStarted := make(chan struct{})
			var quiesceStartedOnce sync.Once
			supervisor.quiesceRuntime = func(_ context.Context, rt *runtimepkg.Runtime, opts runtimepkg.ShutdownOptions) error {
				if rt == predecessor {
					quiesceStartedOnce.Do(func() { close(quiesceStarted) })
				}
				return rt.QuiesceForReplacement(opts)
			}
			var candidateStarts atomic.Int32
			supervisor.startRuntime = func(ctx context.Context, rt *runtimepkg.Runtime) error {
				if rt == candidate {
					candidateStarts.Add(1)
				}
				return rt.Start(ctx)
			}
			server := newAPIServer(&ready, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), runtimeProcessInboundHandler{contexts: manager})
			replacementDone := make(chan error, 1)
			go func() {
				_, err := supervisor.replaceCurrentRuntimeWithSourceAndPacks(
					context.Background(), "/new", source, bundle, fact, runtimecontracts.BundleIdentity{BundleHash: hash}, candidate, candidate.WorkOccurrence(),
					&bundlePackCandidate{generation: providerRegistry.Generation(), installedSubjects: installedTriggerSubjects, inventoryDigest: bundle.PackInventory.Digest()},
				)
				replacementDone <- err
			}()
			select {
			case <-quiesceStarted:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for predecessor quiesce")
			}
			select {
			case err := <-replacementDone:
				t.Fatalf("replacement returned before delayed work joined: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			lookup := manager.LookupBundleHashStatus(hash)
			if ready.Load() || lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseReplacing {
				t.Fatalf("timeout visibility = ready:%v lookup:%#v", ready.Load(), lookup)
			}
			if predecessor.Manager.IsRunning() || predecessor.Bus.OutboxSweeperActive() || active.Load() != 1 {
				t.Fatalf("partially quiesced consumers = manager:%v outbox:%v system:%d", predecessor.Manager.IsRunning(), predecessor.Bus.OutboxSweeperActive(), active.Load())
			}
			assertReplacementHTTPStatus(t, server.Handler, "/readyz", http.StatusServiceUnavailable)
			assertReplacementHTTPStatus(t, server.Handler, "/v1/rpc", http.StatusServiceUnavailable)
			assertReplacementHTTPStatus(t, server.Handler, "/webhooks/missing/telegram", http.StatusServiceUnavailable)
			probe := newRuntime()
			if err := probe.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "generation grant is required") {
				t.Fatalf("competing start during failed quiesce = %v, want missing generation grant denial", err)
			}

			blocker.Release()
			var replacementErr error
			select {
			case replacementErr = <-replacementDone:
				if replacementErr == nil || !strings.Contains(replacementErr.Error(), "quiesce predecessor runtime before replacement") {
					t.Fatalf("replacement error = %v", replacementErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for predecessor restoration")
			}
			lookup = manager.LookupBundleHashStatus(hash)
			if !ready.Load() || !lookup.Loaded() || lookup.Context.Runtime != nil || supervisor.CurrentRuntime() != restored {
				t.Fatalf("restored visibility = ready:%v lookup:%#v runtime:%p replacement_err:%v", ready.Load(), lookup, supervisor.CurrentRuntime(), replacementErr)
			}
			use, _, acquireErr := manager.AcquireBundleHash(context.Background(), hash)
			if acquireErr != nil || use == nil || use.Runtime() != restored {
				t.Fatalf("restored execution authority = use:%#v err:%v", use, acquireErr)
			}
			if err := use.Done(); err != nil {
				t.Fatalf("settle restored execution authority: %v", err)
			}
			if !restored.Manager.IsRunning() || !restored.Bus.OutboxSweeperActive() || active.Load() != 1 || maxActive.Load() != 1 {
				t.Fatalf("restored consumers = manager:%v outbox:%v active:%d max:%d", restored.Manager.IsRunning(), restored.Bus.OutboxSweeperActive(), active.Load(), maxActive.Load())
			}
			assertReplacementHTTPStatus(t, server.Handler, "/readyz", http.StatusOK)
			assertReplacementHTTPStatus(t, server.Handler, "/v1/rpc", http.StatusNoContent)
			assertReplacementHTTPStatus(t, server.Handler, "/webhooks/missing/telegram", http.StatusNotFound)
			if candidateStarts.Load() != 0 {
				t.Fatalf("candidate started %d time(s) after predecessor quiesce failure", candidateStarts.Load())
			}
			secondProbe := newRuntime()
			if err := secondProbe.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "generation grant is required") {
				t.Fatalf("ungranted start after restoration = %v", err)
			}
			if _, exists, err := processCapability.CurrentSourceSet(context.Background()); err != nil || !exists {
				t.Fatalf("restored process capability source set = exists:%v err:%v", exists, err)
			}
			if err := restored.Shutdown(); err != nil {
				t.Fatalf("shutdown restored runtime: %v", err)
			}
		})
	}
}

type replacementOverlapProbeNode struct {
	active    *atomic.Int32
	maxActive *atomic.Int32
	mu        sync.Mutex
	hooks     []func()
}

type replacementQuiesceBlockNode struct {
	active      *atomic.Int32
	maxActive   *atomic.Int32
	release     chan struct{}
	releaseOnce sync.Once
	mu          sync.Mutex
	hooks       []func()
}

func newReplacementQuiesceBlockNode(active, maxActive *atomic.Int32) *replacementQuiesceBlockNode {
	return &replacementQuiesceBlockNode{active: active, maxActive: maxActive, release: make(chan struct{})}
}

func (n *replacementQuiesceBlockNode) String() string { return "replacement-quiesce-block" }

func (n *replacementQuiesceBlockNode) Release() {
	if n != nil {
		n.releaseOnce.Do(func() { close(n.release) })
	}
}

func (n *replacementQuiesceBlockNode) AddSubscriptionReadyHook(hook func()) {
	n.mu.Lock()
	n.hooks = append(n.hooks, hook)
	n.mu.Unlock()
}

func (n *replacementQuiesceBlockNode) Run(ctx context.Context) {
	current := n.active.Add(1)
	for {
		maximum := n.maxActive.Load()
		if current <= maximum || n.maxActive.CompareAndSwap(maximum, current) {
			break
		}
	}
	n.mu.Lock()
	hooks := append([]func(){}, n.hooks...)
	n.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
	<-ctx.Done()
	<-n.release
	n.active.Add(-1)
}

func newReplacementOverlapProbeNode(active, maxActive *atomic.Int32) *replacementOverlapProbeNode {
	return &replacementOverlapProbeNode{active: active, maxActive: maxActive}
}

func (n *replacementOverlapProbeNode) String() string { return "replacement-overlap-probe" }

func (n *replacementOverlapProbeNode) AddSubscriptionReadyHook(hook func()) {
	n.mu.Lock()
	n.hooks = append(n.hooks, hook)
	n.mu.Unlock()
}

func (n *replacementOverlapProbeNode) Run(ctx context.Context) {
	current := n.active.Add(1)
	for {
		maximum := n.maxActive.Load()
		if current <= maximum || n.maxActive.CompareAndSwap(maximum, current) {
			break
		}
	}
	n.mu.Lock()
	hooks := append([]func(){}, n.hooks...)
	n.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
	<-ctx.Done()
	n.active.Add(-1)
}

func runtimeContextTestHash(fill string) string {
	return "bundle-v1:sha256:" + strings.Repeat(fill, 64)
}

func newSupervisorTestRuntimeOccurrence(t *testing.T, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "11111111-1111-4111-8111-111111111111",
		BundleHash:        bundleHash,
	})
	if err != nil {
		t.Fatalf("create supervisor test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire supervisor test runtime occurrence: %v", err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join supervisor test process owner: %v", err)
		}
	})
	return owner
}

func newSupervisorTestProcessOwner(t *testing.T) *worklifetime.Process {
	t.Helper()
	owner := worklifetime.NewProcess()
	t.Cleanup(func() {
		owner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.Join(ctx); err != nil {
			t.Errorf("join supervisor test process owner: %v", err)
		}
	})
	return owner
}

func TestRuntimeProjectSupervisorManagerBackedClosePropagatesShutdownOptions(t *testing.T) {
	bus, err := runtimebus.NewEphemeralEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus}
	hash := "bundle-v1:sha256:" + strings.Repeat("9", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	fact := mustServeTestPersistedBundleSourceFact(hash)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{BundleSourceFact: fact, Source: source, Runtime: rt, WorkOwner: workOwner})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	supervisor := &runtimeProjectSupervisor{
		currentRoot: "/tmp/current", currentSource: source, currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: rt, executionPosture: executionposture.Live,
		currentBundleSourceFact: fact, runtimeContexts: manager,
	}
	_, err = supervisor.CloseProjectWithShutdownOptions(context.Background(), runtimepkg.ShutdownOptions{Grace: -1})
	if err == nil || !strings.Contains(err.Error(), "shutdown grace") {
		t.Fatalf("manager-backed configured shutdown error = %v", err)
	}
}

type processIngressCredentialStore map[string]string

func (s processIngressCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := s[key]
	return value, ok, nil
}
func (processIngressCredentialStore) Set(context.Context, string, string) error { return nil }
func (processIngressCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (processIngressCredentialStore) Delete(context.Context, string) error      { return nil }
func (s processIngressCredentialStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	value, present, err := s.Get(ctx, key)
	return runtimecredentials.NewAtomicSnapshot(runtimecredentials.Metadata{Key: key, Present: present}, value), err
}

type processIngressProofStore struct {
	recorded  bool
	store     runtimebus.EventStore
	lastError error
}

func (s *processIngressProofStore) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	if err := command.Validate(); err != nil {
		s.lastError = err
		return runtimeinbound.CommitResult{}, err
	}
	s.recorded = true
	owner, ok := s.store.(runtimebus.CommitPublicationOwner)
	if !ok {
		return runtimeinbound.CommitResult{}, errors.New("process ingress store does not implement closed publication commit")
	}
	committed := make([]runtimebus.CommittedPublication, len(command.Publications))
	children := make([]runtimeinbound.EventRecord, len(command.Finalization.Events))
	for i, finalized := range command.Finalization.Events {
		var err error
		committed[i], err = owner.CommitPublication(ctx, command.Publications[i])
		if err != nil {
			s.lastError = err
			return runtimeinbound.CommitResult{}, err
		}
		eventFingerprint, err := runtimeinbound.EventIntegrityFingerprint(finalized.Event, finalized.Kind, finalized.Authorization)
		if err != nil {
			return runtimeinbound.CommitResult{}, err
		}
		children[i] = runtimeinbound.EventRecord{
			Ordinal:                      finalized.Ordinal,
			EventID:                      finalized.Event.ID(),
			EventName:                    string(finalized.Event.Type()),
			Kind:                         finalized.Kind,
			Authorization:                finalized.Authorization,
			EventIntegrityFingerprint:    eventFingerprint,
			RecipientManifestFingerprint: strings.Repeat("0", 64),
			RecipientCount:               len(command.Publications[i].Commit.DeliveryRoutes),
			Event:                        finalized.Event,
		}
	}
	return runtimeinbound.CommitResult{
		Record:       runtimeinbound.Record{Request: command.Request, State: "committed", OutputCount: len(children), Events: children, Created: true},
		Publications: committed,
	}, nil
}
func (*processIngressProofStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}
func (*processIngressProofStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

type processIngressEventStore struct {
	events []events.Event
}

func (s *processIngressEventStore) CommitPublication(_ context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	event := command.Commit.Event.Event()
	for _, existing := range s.events {
		if existing.ID() != event.ID() {
			continue
		}
		if !reflect.DeepEqual(existing, event) {
			return runtimebus.CommittedPublication{}, fmt.Errorf("event %s conflicts with its committed fixture", event.ID())
		}
		return runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendExactDuplicate}, nil
	}
	s.events = append(s.events, event)
	return runtimebus.CommittedPublication{AppendOutcome: runtimebus.EventAppendInserted}, nil
}

func (s *processIngressEventStore) LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error) {
	return events.AdmittedEvent{}, false, nil
}

func (*processIngressEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestRuntimeProjectSupervisorCloseProjectWithShutdownOptionsUsesConfiguredGrace(t *testing.T) {
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	var ready atomic.Bool
	ready.Store(true)
	wantGrace := 75 * time.Millisecond

	supervisor := &runtimeProjectSupervisor{
		ready:            &ready,
		currentRoot:      "/tmp/old-project",
		currentBundle:    &runtimecontracts.WorkflowContractBundle{},
		currentSource:    semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:        oldRT,
		executionPosture: executionposture.Live,
	}

	var capturedGrace time.Duration
	supervisor.shutdownRuntime = func(_ context.Context, rt *runtimepkg.Runtime, opts runtimepkg.ShutdownOptions) error {
		if rt != oldRT {
			t.Fatalf("shutdown runtime = %p, want old runtime %p", rt, oldRT)
		}
		capturedGrace = opts.Grace
		return nil
	}

	if _, err := supervisor.CloseProjectWithShutdownOptions(context.Background(), runtimepkg.ShutdownOptions{Grace: wantGrace}); err != nil {
		t.Fatalf("CloseProjectWithShutdownOptions: %v", err)
	}
	if capturedGrace != wantGrace {
		t.Fatalf("shutdown grace = %s, want %s", capturedGrace, wantGrace)
	}
	if ready.Load() {
		t.Fatal("ready flag remained true after close")
	}
	if got := supervisor.CurrentRuntime(); got != nil {
		t.Fatalf("CurrentRuntime after close = %p, want nil", got)
	}
}

func TestStartServeRuntimeContextsRollsBackAllPreparedAuthorActivityCatalogs(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) *selectedStoreOwner
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) *selectedStoreOwner {
			owner := openSelectedSQLiteOwner(t, filepath.Join(t.TempDir(), "runtime.sqlite"), &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
		{name: "postgres", open: func(t *testing.T) *selectedStoreOwner {
			dsn, db, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			owner := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, owner) })
			return owner
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			processWorkOwner := worklifetime.NewProcess()
			runtimes := make([]*runtimepkg.Runtime, 0, 2)
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores.Schema(), bundle); err != nil {
				t.Fatalf("initializeStateStores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			providerRegistry := testProviderTriggerCatalog(t)
			runtimeInstanceID := "11111111-1111-4111-8111-111111111111"
			facts := []runtimecorrelation.BundleSourceFact{
				mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("c")),
				mustServeTestEphemeralBundleSourceFact(runtimeContextTestHash("d")),
			}
			sources := make([]runtimeagenttopology.SourceCoordinate, 0, len(facts))
			for _, fact := range facts {
				bundleHash, bundleSource := fact.StorageValues()
				sources = append(sources, runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource})
			}
			plan, err := runtimeagenttopology.NewSourceSetPlan(sources, nil)
			if err != nil {
				t.Fatalf("construct rollback source set: %v", err)
			}
			capability, err := stores.StartupOwnership().AcquireProcessCapability(context.Background(), runtimestartupownership.AcquireRequest{
				OwnerID: "serve-context-rollback-test", BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
			})
			if err != nil {
				t.Fatalf("acquire rollback process capability: %v", err)
			}
			t.Cleanup(func() {
				var closeErr error
				for _, rt := range runtimes {
					if err := rt.Shutdown(); err != nil {
						closeErr = errors.Join(closeErr, fmt.Errorf("shutdown serve-context rollback runtime: %w", err))
					}
				}
				if err := closeSelectedStoreTestProcess(processWorkOwner, capability); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
				if closeErr != nil {
					t.Errorf("close serve-context rollback generation: %v", closeErr)
				}
			})
			if _, err := capability.InstallCompleteSourceSet(context.Background(), runtimeagenttopology.SourceSetCommitRequest{
				OperationID: uuid.NewString(), Plan: plan,
			}); err != nil {
				t.Fatalf("install rollback source set: %v", err)
			}
			contexts := make([]serveRuntimeBundleContext, 0, len(facts))
			for _, fact := range facts {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{
					SelfCheck:                        false,
					WorkflowModule:                   stubWorkflowModule{source: source},
					LLMRuntime:                       servedNoopLLMRuntime{},
					DisablePersistentStartupRecovery: true,
					ProviderTriggerCatalog:           providerRegistry,
					ProcessWorkOwner:                 processWorkOwner,
					RuntimeInstanceID:                runtimeInstanceID,
					BundleSourceFact:                 fact,
				}))
				if err != nil {
					t.Fatalf("NewRuntime(%s): %v", fact.BundleHash(), err)
				}
				runtimes = append(runtimes, rt)
				_, bundleSource := fact.StorageValues()
				grant, err := capability.IssueGenerationGrant(context.Background(), runtimestartupownership.GrantRequest{
					BundleHash: fact.BundleHash(), BundleSource: bundleSource, RuntimeInstanceID: runtimeInstanceID,
					RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
				})
				if err != nil {
					t.Fatalf("issue rollback generation grant: %v", err)
				}
				if err := rt.InstallStartupGrant(grant); err != nil {
					t.Fatalf("install rollback generation grant: %v", err)
				}
				contexts = append(contexts, serveRuntimeBundleContext{runtime: rt, bundleSourceFact: fact})
			}
			contexts[0].runtime.CloseAdmission()
			if err := startServeRuntimeContexts(context.Background(), contexts, nil); err == nil || !strings.Contains(err.Error(), "shutdown already started") {
				t.Fatalf("startServeRuntimeContexts error = %v, want shutdown admission failure", err)
			}

			registrar, ok := stores.RuntimeDeps().EventStore.(interface {
				RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
			})
			if !ok {
				t.Fatalf("selected %s event store lacks author activity catalog registry", backend.name)
			}
			for _, fact := range facts {
				scope := runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash())
				lease, err := registrar.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{{
					EventType: "rollback.probe", Disposition: runtimeauthoractivity.StoryDifferent,
				}})
				if err != nil {
					t.Fatalf("prepared catalog for %s remained leased after startup rollback: %v", fact.BundleHash(), err)
				}
				lease.Release()
			}
		})
	}
}

type stubWorkflowModule struct{ source semanticview.Source }

func (m stubWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (stubWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return &runtimepipeline.WorkflowDefinition{}
}
func (stubWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode  { return nil }
func (stubWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry   { return nil }
func (stubWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry { return nil }

type replacementProviderEventSource struct {
	semanticview.Source
	generation triggergeneration.Generation
	eventName  string
}

func (s replacementProviderEventSource) SemanticCapabilities() semanticview.Capabilities {
	return s.Source.SemanticCapabilities().WithProviderTriggerEvents(s.Source, s.generation, nil)
}

func (s replacementProviderEventSource) EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	if strings.TrimSpace(eventType) == s.eventName {
		return runtimecontracts.EventCatalogEntry{Source: "test_provider_trigger_import"}, true
	}
	return s.Source.EventEntry(eventType)
}

func (s replacementProviderEventSource) EventEntries() map[string]runtimecontracts.EventCatalogEntry {
	base := s.Source.EventEntries()
	entries := make(map[string]runtimecontracts.EventCatalogEntry, len(base)+1)
	for name, entry := range base {
		entries[name] = entry
	}
	entries[s.eventName] = runtimecontracts.EventCatalogEntry{Source: "test_provider_trigger_import"}
	return entries
}

func (s replacementProviderEventSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	base := s.Source.ResolvedEventCatalog()
	entries := make(map[string]runtimecontracts.EventCatalogEntry, len(base)+1)
	for name, entry := range base {
		entries[name] = entry
	}
	entries[s.eventName] = runtimecontracts.EventCatalogEntry{Source: "test_provider_trigger_import"}
	return entries
}

type stubWorkspaceLifecycle struct {
	validateErr error
	prereqErr   error
	systemErr   error
}

func (s stubWorkspaceLifecycle) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return nil, nil
}
func (s stubWorkspaceLifecycle) ResolveWorkspaceForCapabilityAdmission(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return nil, nil
}
func (s stubWorkspaceLifecycle) ValidateSource(context.Context, semanticview.Source) error {
	return s.validateErr
}
func (s stubWorkspaceLifecycle) EnsurePrereqs(context.Context) error { return s.prereqErr }
func (s stubWorkspaceLifecycle) EnsureSystemWorkspaces(context.Context) error {
	return s.systemErr
}
func (stubWorkspaceLifecycle) EnsureEntityWorkspace(context.Context, string) error  { return nil }
func (stubWorkspaceLifecycle) StopEntityWorkspace(context.Context, string) error    { return nil }
func (stubWorkspaceLifecycle) SetDataProjectionProvider(runtimedataaccess.Provider) {}

func writeProjectRoot(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.yaml"), []byte("name: test\nversion: 1.0.0\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	return dir
}

func testBuilderSupervisorBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	dir := t.TempDir()
	repoRoot := cliapp.RepoRoot()
	platformPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	packagePath := filepath.Join(dir, "package.yaml")
	if err := os.WriteFile(packagePath, []byte("name: test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repoRoot, dir, platformPath, runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBase: packfixture.EmbeddedBase(t), AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		t.Fatalf("load supervisor workflow bundle: %v", err)
	}
	return bundle
}

func newSupervisorForLoadProjectFailureTest(
	t *testing.T,
	projectRoot string,
	lifecycle workspace.Lifecycle,
	createRuntime func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error),
) *runtimeProjectSupervisor {
	t.Helper()
	bundle := testBuilderSupervisorBundle(t)
	source := semanticview.Wrap(bundle)
	module := stubWorkflowModule{source: source}
	base := packfixture.EmbeddedBase(t)
	catalog := testProviderTriggerCatalog(t)
	supervisor := newRuntimeProjectSupervisor("", "", nil, serveRuntimePersistence{}, new(atomic.Bool), cliapp.WorkspaceMountSources{}, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil, nil, catalog, base, "", nil, nil, nil)
	supervisor.executionPosture = executionposture.Live
	supervisor.processWorkOwner = worklifetime.NewProcess()
	supervisor.providerTriggers = catalog
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		return cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: &config.Config{}}, candidate, nil, nil)
	})
	supervisor.dev = true
	supervisor.loadWorkflow = func(RepoRoot, contractsRoot, platformSpecPath string, _ *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if got := strings.TrimSpace(contractsRoot); got != strings.TrimSpace(projectRoot) {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", got, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error { return nil }
	supervisor.initStateStores = func(context.Context, store.SchemaBootstrapper, *runtimecontracts.WorkflowContractBundle) (string, error) {
		return "store wiring ready", nil
	}
	supervisor.newWorkspaces = func(workspace.Lookup, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		return lifecycle, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
	}
	if createRuntime != nil {
		supervisor.createRuntime = func(ctx context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
			rt, err := createRuntime(ctx, deps)
			if err != nil || rt == nil || rt.EffectiveSourceIdentity.Validate() == nil {
				return rt, err
			}
			projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{
				WorkflowModule: deps.Options.WorkflowModule, BundleSourceFact: deps.Options.BundleSourceFact,
				ProviderTriggerCatalog: deps.Options.ProviderTriggerCatalog, ChannelPlans: deps.Options.ChannelPlans,
				ChannelOutboundBindings: deps.Options.ChannelOutboundBindings,
			})
			if err != nil {
				return nil, err
			}
			catalog, err := scenarioexecution.NewCatalog(projection.Identity(), nil)
			if err != nil {
				return nil, err
			}
			rt.Options.BundleSourceFact = deps.Options.BundleSourceFact
			rt.Options.WorkflowModule = projection.WorkflowModule()
			rt.EffectiveSourceIdentity = projection.Identity()
			rt.ScenarioProfileCatalog = catalog
			return rt, nil
		}
	}
	return supervisor
}

func TestRuntimeProjectSupervisorCarriesProcessOwnerIntoDynamicRuntime(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	processOwner := worklifetime.NewProcess()
	var captured runtimepkg.RuntimeDeps
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		captured = deps
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.processWorkOwner = processOwner
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if captured.Options.ProcessWorkOwner != processOwner {
		t.Fatalf("dynamic runtime process owner = %p, want served owner %p", captured.Options.ProcessWorkOwner, processOwner)
	}
}

func TestRuntimeProjectSupervisorReloadSelectsFreshBaseAndRetainsPredecessorGeneration(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	embedded := packfixture.EmbeddedBase(t)
	telegram, ok := embedded.Lookup("provider.telegram")
	if !ok {
		t.Fatal("embedded Telegram pack is missing")
	}
	firstBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "first development telegram update object is required", 1))
	secondBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "second development telegram update object is required", 1))
	thirdBody := []byte(strings.Replace(string(telegram.ManifestBody()), "telegram update object is required", "rejected development telegram update object is required", 1))
	firstBase, firstDirs := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": firstBody})
	secondBase, secondDirs := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": secondBody})
	thirdBase, thirdDirs := packfixture.DevelopmentBase(t, map[string][]byte{"provider.telegram": thirdBody})
	if firstBase.Digest() == secondBase.Digest() {
		t.Fatal("development reload fixtures have the same digest")
	}

	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.RepoRoot = cliapp.RepoRoot()
	supervisor.platformSpecPath = runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot())
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	selectedDirs := firstDirs
	supervisor.SetRuntimeConfigLoader(func() (cliapp.RuntimeConfigLoadResult, error) {
		return cliapp.RuntimeConfigLoadResult{Config: &config.Config{Platform: config.PlatformConfig{Packs: config.PlatformPacksConfig{PlatformDirs: append([]string(nil), selectedDirs...)}}}}, nil
	})
	var loadedBaseDigests []string
	supervisor.loadWorkflow = func(repoRoot, contractsRoot, platformSpecPath string, base *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		loadedBaseDigests = append(loadedBaseDigests, base.Digest())
		return cliapp.NewSwarmWorkflowModuleWithPackBase(repoRoot, contractsRoot, platformSpecPath, base)
	}
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, cfg cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		if !reflect.DeepEqual(cfg.Config.Platform.Packs.PlatformDirs, selectedDirs) {
			t.Fatalf("candidate pack loader config dirs = %#v, want %#v", cfg.Config.Platform.Packs.PlatformDirs, selectedDirs)
		}
		return cliapp.LoadBundlePackRuntime(ctx, cfg, candidate, nil, nil)
	})

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("open first development generation: %v", err)
	}
	selectedDirs = secondDirs
	if _, err := supervisor.ReloadProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("reload second development generation: %v", err)
	}
	if !reflect.DeepEqual(loadedBaseDigests, []string{firstBase.Digest(), secondBase.Digest()}) {
		t.Fatalf("workflow base generations = %#v", loadedBaseDigests)
	}
	current, err := supervisor.platformPackBases.CurrentPlatformPackBase()
	if err != nil || current.Digest() != secondBase.Digest() || supervisor.platformPackBase.Digest() != secondBase.Digest() {
		t.Fatalf("current base = %v supervisor=%v err=%v, want %s", current, supervisor.platformPackBase, err, secondBase.Digest())
	}
	firstEffective, err := packartifact.NewEffectivePackInventory(firstBase, nil)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := supervisor.platformPackBases.ResolvePlatformPackBase(firstEffective.SelectionReceipt())
	if err != nil || retained.Digest() != firstBase.Digest() {
		t.Fatalf("predecessor generation = %v err=%v", retained, err)
	}
	predecessorRuntime := supervisor.CurrentRuntime()
	selectedDirs = thirdDirs
	supervisor.createRuntime = func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return nil, errors.New("reject candidate runtime")
	}
	if _, err := supervisor.ReloadProject(context.Background(), projectRoot); err == nil || !strings.Contains(err.Error(), "reject candidate runtime") {
		t.Fatalf("rejected development reload error = %v", err)
	}
	current, err = supervisor.platformPackBases.CurrentPlatformPackBase()
	if err != nil || current.Digest() != secondBase.Digest() || supervisor.CurrentRuntime() != predecessorRuntime {
		t.Fatalf("rejected reload disturbed predecessor: base=%v runtime_same=%t err=%v", current, supervisor.CurrentRuntime() == predecessorRuntime, err)
	}
	thirdEffective, err := packartifact.NewEffectivePackInventory(thirdBase, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.platformPackBases.ResolvePlatformPackBase(thirdEffective.SelectionReceipt()); err == nil || !strings.Contains(err.Error(), "not retained by this process") {
		t.Fatalf("rejected generation resolution error = %v", err)
	}
}

func TestRuntimeProjectSupervisorDerivesProcessOwnerFromInitialRuntime(t *testing.T) {
	processOwner := worklifetime.NewProcess()
	initial := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: runtimepkg.RuntimeOptions{ProcessWorkOwner: processOwner}}
	supervisor := newRuntimeProjectSupervisor(
		"", "", nil, serveRuntimePersistence{}, new(atomic.Bool), cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{}, nil, nil, nil, nil, "", nil, nil, initial,
	)
	if supervisor.processWorkOwner != processOwner {
		t.Fatalf("supervisor process owner = %p, want initial runtime owner %p", supervisor.processWorkOwner, processOwner)
	}
}

func TestRuntimeProjectSupervisorLoadProjectUsesNoAmbientWorkspaceMountSources(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	wantMountSources := cliapp.WorkspaceMountSources{}

	var gotMountSources cliapp.WorkspaceMountSources
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.mountSources = wantMountSources
	supervisor.newWorkspaces = func(_ workspace.Lookup, _ string, _ semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		gotMountSources = mountSources
		return stubWorkspaceLifecycle{}, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if gotMountSources != wantMountSources {
		t.Fatalf("workspace mount sources = %#v, want %#v", gotMountSources, wantMountSources)
	}
}

func TestRuntimeProjectSupervisorReverifiesProviderCatalogAndPublishesAdmittedSource(t *testing.T) {
	projectRoot := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	module, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), projectRoot, runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot()))
	if err != nil {
		t.Fatalf("NewSwarmWorkflowModule: %v", err)
	}
	bootCatalog := testProviderTriggerCatalog(t)
	candidateCatalog := testProviderTriggerCatalog(t)
	var gotCatalog *providertriggers.CatalogSnapshot
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotCatalog = deps.Options.ProviderTriggerCatalog
		effectiveSource, err := runtimepkg.SourceWithProviderTriggerEvents(deps.Options.WorkflowModule.SemanticSource(), deps.Options.ProviderTriggerCatalog)
		if err != nil {
			return nil, err
		}
		deps.Options.WorkflowModule = stubWorkflowModule{source: effectiveSource}
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.loadWorkflow = func(_, contractsRoot, _ string, _ *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if contractsRoot != projectRoot {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", contractsRoot, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.providerTriggers = bootCatalog
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		loaded, err := cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: &config.Config{}}, candidate, nil, nil)
		if err != nil {
			return cliapp.BundlePackRuntimeLoad{}, err
		}
		loaded.ProviderTriggers.Catalog = candidateCatalog
		return loaded, nil
	})
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if gotCatalog != candidateCatalog {
		t.Fatalf("replacement provider catalog = %p, want reverified candidate %p (boot=%p)", gotCatalog, candidateCatalog, bootCatalog)
	}
	if _, declared := bundle.EventEntry("inbound.telegram.text_message"); declared {
		t.Fatal("authored replacement bundle unexpectedly owns imported Telegram event")
	}
	generation := requireProviderTriggerEventSource(t, supervisor.CurrentSource(), "inbound.telegram.text_message")
	if !generation.Equal(candidateCatalog.Generation()) {
		t.Fatalf("replacement source generation = %s, want candidate %s", generation.Diagnostic(), candidateCatalog.Generation().Diagnostic())
	}
}

func TestRuntimeProjectSupervisorPrevalidatesProjectedSourceAndPublishesExactRuntime(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "builder-replacement-proof")
	projectRoot := writeGeneratedTelegramScenarioFixture(t, "http://127.0.0.1:1")
	writeWorkflowValidationFixtureFile(t, filepath.Join(projectRoot, "package.yaml"), `
name: schema-only-provider-trigger-replacement
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
connector_packs:
  imports:
    - {provider: telegram, tool: telegram.send_message}
provider_trigger_events:
  imports:
    - {provider: telegram, event: inbound.telegram.text_message}
flows:
  - {id: telegram-chat, flow: telegram-chat, mode: template}
`)
	module, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), projectRoot, runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot()))
	if err != nil {
		t.Fatalf("NewSwarmWorkflowModule: %v", err)
	}
	stores, err := buildStores(context.Background(), storebackend.Selection{
		Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "replacement.sqlite"),
	}, &config.Config{})
	if err != nil {
		t.Fatalf("build SQLite stores: %v", err)
	}
	t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
	cfg := &config.Config{}
	cfg.Runtime.ExecutionPosture = executionposture.Live
	candidateCatalog := testProviderTriggerCatalog(t)
	channelCfg := &config.Config{Channels: config.ChannelsConfig{
		Bindings: map[string]config.ChannelBindingConfig{
			"ops": {Pack: "provider.telegram.hitl_channel", Destination: "42"},
		},
	}}
	rawSource := module.SemanticSource()
	if rawValidationErr := newBuilderProjectSourceValidator(cfg)(context.Background(), rawSource, candidateCatalog); rawValidationErr == nil || !strings.Contains(rawValidationErr.Error(), "missing from event catalog") {
		t.Fatalf("production validation of raw replacement = %v, want missing imported event before projection", rawValidationErr)
	}
	var ready atomic.Bool
	supervisor := newRuntimeProjectSupervisor(
		cliapp.RepoRoot(), runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot()), cfg, projectServeRuntimePersistence(stores), &ready,
		cliapp.WorkspaceMountSources{}, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"},
		nil, processIngressCredentialStore{}, candidateCatalog, packfixture.EmbeddedBase(t), "", nil, nil, nil,
	)
	supervisor.executionPosture = executionposture.Live
	supervisor.processWorkOwner = newSupervisorTestProcessOwner(t)
	supervisor.runtimeInstanceID = "11111111-1111-4111-8111-111111111111"
	supervisor.createRuntime = func(ctx context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		deps.Options.LLMRuntime = servedNoopLLMRuntime{}
		deps.Options.DisablePersistentStartupRecovery = true
		return runtimepkg.NewRuntime(ctx, deps)
	}
	var workspaceSource semanticview.Source
	supervisor.newWorkspaces = func(_ workspace.Lookup, _ string, semanticSource semanticview.Source, _ cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		workspaceSource = semanticSource
		return stubWorkspaceLifecycle{}, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
	}
	var candidateChannelLoad cliapp.ChannelPackLoad
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		loaded, loadErr := cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: channelCfg}, candidate, nil, nil)
		if loadErr == nil {
			candidateChannelLoad = loaded.Channels
		}
		return loaded, loadErr
	})
	if _, declared := rawSource.EventEntry("inbound.telegram.text_message"); declared {
		t.Fatal("raw replacement source unexpectedly owns imported Telegram event")
	}
	for _, toolID := range []string{"telegram.send_message", "channel.ops.deliver"} {
		if _, declared := rawSource.ToolEntries()[toolID]; declared {
			t.Fatalf("raw replacement source unexpectedly owns projected tool %q", toolID)
		}
	}
	bundleIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("derive schema-only bundle identity: %v", err)
	}
	topologySource := semanticview.Wrap(bundle)
	topologyManager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		SemanticSource:    topologySource,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	installSupervisorTestProcessCapability(
		t,
		supervisor,
		topologyManager,
		topologySource,
		mustServeTestEphemeralBundleSourceFact(bundleIdentity.BundleHash),
		supervisor.runtimeInstanceID,
	)

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	defer func() {
		if _, closeErr := supervisor.CloseProject(context.Background()); closeErr != nil {
			t.Errorf("CloseProject: %v", closeErr)
		}
	}()
	for label, projectedSource := range map[string]semanticview.Source{"published": supervisor.CurrentSource(), "workspace": workspaceSource} {
		if projectedSource == nil {
			t.Fatalf("%s source was not observed", label)
		}
		requireProviderTriggerEventSource(t, projectedSource, "inbound.telegram.text_message")
		for _, toolID := range []string{"telegram.send_message", "channel.ops.deliver"} {
			if _, declared := projectedSource.ToolEntries()[toolID]; !declared {
				t.Fatalf("%s source omitted projected tool %q", label, toolID)
			}
		}
	}
	source := supervisor.CurrentSource()
	generation := requireProviderTriggerEventSource(t, source, "inbound.telegram.text_message")
	if !generation.Equal(candidateCatalog.Generation()) {
		t.Fatalf("schema-only replacement generation = %s, want %s", generation.Diagnostic(), candidateCatalog.Generation().Diagnostic())
	}
	if authorizations := source.SemanticCapabilities().ProviderTriggerTargetFreeAuthorizations(); len(authorizations) != 0 {
		t.Fatalf("schema-only replacement granted provider authorization: %#v", authorizations)
	}
	provenance := source.SemanticCapabilities().ProviderTriggerEventProvenance()
	if len(provenance) != 1 || provenance[0].PackID != "provider.telegram" || !provenance[0].Generation.Equal(candidateCatalog.Generation()) {
		t.Fatalf("schema-only replacement provenance = %#v", provenance)
	}

	supervisor.mu.RLock()
	replacementFact := supervisor.currentBundleSourceFact
	replacementIdentity := supervisor.currentBundleIdentity
	supervisor.mu.RUnlock()
	replacementRuntime := supervisor.CurrentRuntime()
	if replacementRuntime.ScenarioProfileCatalog == nil || !replacementRuntime.ScenarioProfileCatalog.EffectiveSourceIdentity().Equal(replacementRuntime.EffectiveSourceIdentity) {
		t.Fatal("replacement runtime did not retain the exact effective-source scenario profile catalog")
	}
	manager, err := runtimepkg.NewRuntimeContextManager(stores.RunBundleAvailability(), completeServeTestPackContext(t, runtimepkg.BundleContext{
		BundleSourceFact: replacementFact,
		BundleIdentity:   replacementIdentity,
		Source:           source,
		ContractsRoot:    projectRoot,
		PlatformSpecPath: cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath),
		Runtime:          replacementRuntime,
		WorkOwner:        replacementRuntime.WorkOccurrence(),
	}))
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	supervisor.SetRuntimeContextManager(manager, replacementFact, replacementIdentity)
	lookup := manager.LookupBundleHashStatus(replacementFact.BundleHash())
	if !lookup.Loaded() || lookup.Context.ScenarioProfileCatalog != replacementRuntime.ScenarioProfileCatalog || !lookup.Context.EffectiveSourceIdentity.Equal(replacementRuntime.EffectiveSourceIdentity) {
		t.Fatalf("published replacement profile catalog/identity = %#v, want exact runtime projection", lookup)
	}
	publishedContextRuntime := lookup.Context.Runtime

	publication := apiv1.EventPublicationOptions{
		ExecutionPosture: replacementRuntime.ExecutionPosture,
		Idempotency:      stores.Idempotency(),
		Events:           replacementRuntime.Bus,
		Acknowledged:     replacementRuntime.Bus,
		RecipientPlans:   replacementRuntime.Bus,
		BundleSource:     replacementRuntime.Bus,
		Runs:             stores.Runs(),
		Entities:         stores.Entities(),
		Observability:    stores.Observability(),
		RunBundleContext: stores.RunBundleContext(),
		RuntimeContexts:  manager,
		Source:           source,
		Bundle:           replacementIdentity,
	}
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath),
		AuthTokens:       []string{apiv1.DefaultLoopbackAPIToken},
		ProcessWorkOwner: supervisor.processWorkOwner,
		Handlers:         apiv1.OperatorEventPublishHandlers(apiv1.EventPublishHandlerOptions{Publication: publication}),
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	published := requireServedEventPublishRPCResult(t, server.URL+"/v1/rpc", map[string]any{
		"bundle_hash": replacementFact.BundleHash(),
		"event_name":  "inbound.telegram.text_message",
		"payload": map[string]any{
			"conversation_reference": "chat-42", "external_account_reference": "account-42",
			"provider_message_reference": 1, "text": "replacement proof",
		},
		"idempotency_key": "schema-only-replacement-publication",
	})
	if !published.NewRunCreated || published.EventID == "" || published.RunID == "" {
		t.Fatalf("replacement event.publish result = %#v, want a durable new run and event", published)
	}
	var persisted operatorread.OperatorEventFull
	deadline := time.Now().Add(5 * time.Second)
	for {
		persisted, err = stores.Observability().LoadOperatorEvent(context.Background(), published.EventID)
		if err == nil && len(persisted.Deliveries) == 1 && persisted.Deliveries[0].Status == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement event %s did not reach one durable delivered route: event=%#v err=%v", published.EventID, persisted, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(persisted.Deliveries) != 1 {
		t.Fatalf("replacement event deliveries = %#v, want one exact template delivery", persisted.Deliveries)
	}
	delivery := persisted.Deliveries[0]
	if delivery.SubscriberType != "node" || delivery.SubscriberID != identitytest.FlowNode(t, "telegram-chat", "telegram-input-observer").Key() ||
		delivery.Target.FlowID != "telegram-chat" || !strings.HasPrefix(delivery.Target.FlowInstance, "telegram-chat/") || delivery.Target.EntityID == "" {
		t.Fatalf("replacement event delivery = %#v, want exact telegram-chat template target", delivery)
	}

	predecessorSource := supervisor.CurrentSource()
	predecessorRuntime := supervisor.CurrentRuntime()
	predecessorCatalog := predecessorRuntime.ScenarioProfileCatalog
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		loaded, err := cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: channelCfg}, candidate, nil, nil)
		if err != nil {
			return cliapp.BundlePackRuntimeLoad{}, err
		}
		conflicting := candidateChannelLoad
		conflicting.Bindings = append(append([]packs.OutboundBindingPlan(nil), candidateChannelLoad.Bindings...), candidateChannelLoad.Bindings...)
		loaded.Channels = conflicting
		return loaded, nil
	})
	if _, reloadErr := supervisor.ReloadProject(context.Background(), projectRoot); reloadErr == nil || !strings.Contains(reloadErr.Error(), `duplicate channel runtime tool "channel.ops.`) {
		t.Fatalf("ReloadProject error = %v, want duplicate projected channel tool rejection", reloadErr)
	}
	lookup = manager.LookupBundleHashStatus(replacementFact.BundleHash())
	contextRuntime := (*runtimepkg.Runtime)(nil)
	contextCatalog := (*scenarioexecution.Catalog)(nil)
	if lookup.Context != nil {
		contextRuntime = lookup.Context.Runtime
		contextCatalog = lookup.Context.ScenarioProfileCatalog
	}
	if !supervisor.ready.Load() || supervisor.CurrentRuntime() != predecessorRuntime || !reflect.DeepEqual(supervisor.CurrentSource(), predecessorSource) ||
		!lookup.Loaded() || contextRuntime != publishedContextRuntime || contextCatalog != predecessorCatalog {
		t.Fatalf("failed projected replacement disturbed predecessor: ready=%v runtime_same=%v source_same=%v context_runtime_same=%v context_catalog_same=%v lookup=%#v", supervisor.ready.Load(), supervisor.CurrentRuntime() == predecessorRuntime, reflect.DeepEqual(supervisor.CurrentSource(), predecessorSource), contextRuntime == publishedContextRuntime, contextCatalog == predecessorCatalog, lookup)
	}
}

func TestRuntimeProjectSupervisorLoadProject_PropagatesWorkspaceAdmissionFailures(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	cases := []struct {
		name      string
		lifecycle workspace.Lifecycle
		wantErr   string
	}{
		{
			name:      "validate source",
			lifecycle: stubWorkspaceLifecycle{validateErr: errors.New("workspace validation failed: workspace image is required")},
			wantErr:   "workspace validation failed: workspace image is required",
		},
		{
			name: "ensure prereqs preserves typed recovery",
			lifecycle: stubWorkspaceLifecycle{prereqErr: &workspace.PrerequisiteError{
				Problem:     `Docker is not reachable via "/opt/docker"`,
				Remediation: "Start the Docker daemon, then verify with `/opt/docker info`",
			}},
			wantErr: "/opt/docker info",
		},
		{
			name:      "ensure system workspaces",
			lifecycle: stubWorkspaceLifecycle{systemErr: errors.New("ensure system workspace: permission denied")},
			wantErr:   "ensure system workspace: permission denied",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, tc.lifecycle, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
				t.Fatal("createRuntime should not be called when workspace admission fails")
				return nil, nil
			})

			_, err := supervisor.OpenProject(context.Background(), projectRoot)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("OpenProject err = %v, want substring %q", err, tc.wantErr)
			}
			if got := supervisor.CurrentProject(); got.Loaded {
				t.Fatalf("CurrentProject.Loaded = true after %s failure, want false", tc.name)
			}
			if supervisor.CurrentRuntime() != nil {
				t.Fatalf("CurrentRuntime = %p after %s failure, want nil", supervisor.CurrentRuntime(), tc.name)
			}
		})
	}
}

func TestRuntimeProjectSupervisorOpenProjectExecutesExplicitHostRefusal(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	bundle := testBuilderSupervisorBundle(t)
	intent := serveTestAgentConfig(runtimeactors.AgentConfig{ID: "worker"}).Intent
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"worker": {ID: "worker", ResolvedIntent: intent},
	}
	source := semanticviewtest.WrapRootAgents(bundle)
	module := stubWorkflowModule{source: source}
	cfg := testWorkspaceBackendConfig(llmselection.BackendClaudeCLI)
	supervisor := newRuntimeProjectSupervisor(
		"", "", cfg, serveRuntimePersistence{}, new(atomic.Bool), cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true},
		nil, nil, nil, packfixture.EmbeddedBase(t), "", nil, nil, nil,
	)
	supervisor.dev = true
	catalog := emptyProviderTriggerCatalog(t)
	supervisor.providerTriggers = catalog
	supervisor.SetBundlePackRuntimeLoader(func(ctx context.Context, _ cliapp.RuntimeConfigLoadResult, candidate *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error) {
		return cliapp.LoadBundlePackRuntime(ctx, cliapp.RuntimeConfigLoadResult{Config: cfg}, candidate, nil, nil)
	})
	supervisor.loadWorkflow = func(_, contractsRoot, _ string, _ *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if strings.TrimSpace(contractsRoot) != strings.TrimSpace(projectRoot) {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", contractsRoot, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error { return nil }
	supervisor.initStateStores = func(context.Context, store.SchemaBootstrapper, *runtimecontracts.WorkflowContractBundle) (string, error) {
		return "store wiring ready", nil
	}
	supervisor.createRuntime = func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		t.Fatal("createRuntime must not run after workspace backend refusal")
		return nil, nil
	}

	_, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err == nil {
		t.Fatal("OpenProject unexpectedly accepted claude_cli host execution")
	}
	assertClaudeHostRefusal(t, err.Error())
	if supervisor.CurrentProject().Loaded || supervisor.CurrentRuntime() != nil {
		t.Fatalf("failed replacement changed authority: project=%#v runtime=%p", supervisor.CurrentProject(), supervisor.CurrentRuntime())
	}
}

func TestRuntimeProjectSupervisorOpenProjectNoAgentSkipsWorkspaceLifecycle(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	var ready atomic.Bool
	var createdWorkspace bool
	var gotWorkspace workspace.Lifecycle

	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, nil, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotWorkspace = deps.Options.WorkspaceLifecycle
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.ready = &ready
	supervisor.cfg = &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}}
	supervisor.workspaceBackend = cliapp.WorkspaceBackendSelection{Source: "capability-derived"}
	supervisor.newWorkspaces = func(lookup workspace.Lookup, contractsRoot string, source semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		createdWorkspace = true
		decision, err := cliapp.DecideWorkspaceBackend(supervisor.workspaceBackend, supervisor.cfg, source)
		if err != nil {
			return nil, cliapp.WorkspaceBackendSelection{}, err
		}
		lifecycle, err := cliapp.ConfiguredWorkspaceLifecycleForBackend(lookup, supervisor.cfg, contractsRoot, source, mountSources, decision)
		if err != nil {
			return nil, decision, err
		}
		return lifecycle, decision, nil
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	status, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if !status.Loaded {
		t.Fatal("status.Loaded = false, want true")
	}
	if !ready.Load() {
		t.Fatal("ready flag = false, want true")
	}
	if !createdWorkspace {
		t.Fatal("shared backend workspace factory was not called")
	}
	if gotWorkspace != nil {
		t.Fatalf("runtime workspace lifecycle = %T, want nil for no-agent no-workspace decision", gotWorkspace)
	}
}

func TestRuntimeProjectSupervisorOpenProjectRejectsNilLifecycleWithoutNoWorkspaceDecision(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, nil, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		t.Fatal("createRuntime should not be called when lifecycle is nil without no-workspace decision")
		return nil, nil
	})
	supervisor.newWorkspaces = func(workspace.Lookup, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		return nil, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "test"}, nil
	}

	_, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err == nil || !strings.Contains(err.Error(), "no lifecycle is only valid for canonical no-workspace decision") {
		t.Fatalf("OpenProject err = %v, want nil lifecycle guard", err)
	}
}

func TestRuntimeProjectSupervisorLoadProject_PropagatesRuntimeStartFailure(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	var ready atomic.Bool
	oldRT := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.ready = &ready
	supervisor.currentRoot = "/tmp/old"
	supervisor.currentBundle = &runtimecontracts.WorkflowContractBundle{}
	supervisor.currentSource = semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	supervisor.currentRT = oldRT
	ready.Store(true)
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error {
		return errors.New("runtime start denied by workspace dependency failure")
	}

	_, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err == nil || !strings.Contains(err.Error(), "runtime start denied by workspace dependency failure") {
		t.Fatalf("OpenProject err = %v, want start failure", err)
	}
	if ready.Load() {
		t.Fatal("ready flag remained true after project.open start failure")
	}
	if got := supervisor.CurrentProject(); got.Loaded {
		t.Fatalf("CurrentProject.Loaded = true after start failure, want false")
	}
	if supervisor.CurrentRuntime() != nil {
		t.Fatalf("CurrentRuntime = %p after start failure, want nil", supervisor.CurrentRuntime())
	}
}

func TestRuntimeProjectSupervisorLoadProjectPassesBundleSourceFactToRuntime(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	expectedBundle := testBuilderSupervisorBundle(t)
	expectedHash, err := runtimecontracts.BundleHash(expectedBundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}

	var gotSourceFact runtimecorrelation.BundleSourceFact
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotSourceFact = deps.Options.BundleSourceFact
		return &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Options: deps.Options}, nil
	})
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	status, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if !status.Loaded {
		t.Fatalf("status.Loaded = false, want true")
	}
	if gotSourceFact.BundleHash() != expectedHash || !gotSourceFact.IsEphemeral() {
		t.Fatalf("BundleSourceFact = %#v, want hash=%q ephemeral source", gotSourceFact, expectedHash)
	}
}

func TestRuntimeProjectSupervisorOpenProjectFailsClosedWhenSourceReplacementDisabled(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		t.Fatal("createRuntime should not be called when DB-loaded source replacement is disabled")
		return nil, nil
	})
	supervisor.DisableSourceReplacement("DB-loaded --bundle-hash pins one catalog source for this process")

	status, err := supervisor.OpenProject(context.Background(), projectRoot)
	if err == nil || !strings.Contains(err.Error(), "project source replacement is disabled") || !strings.Contains(err.Error(), "DB-loaded --bundle-hash") {
		t.Fatalf("OpenProject err = %v, want source replacement disabled", err)
	}
	if status.Loaded {
		t.Fatalf("status.Loaded = true, want false")
	}
}

type builderControlTestAgent struct{ id string }

func (a builderControlTestAgent) ID() string                      { return a.id }
func (builderControlTestAgent) Type() string                      { return "stub" }
func (builderControlTestAgent) Subscriptions() []events.EventType { return nil }
func (builderControlTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (builderControlTestAgent) BoardStep(context.Context, runtimeagentcontrol.BoardDirective) (string, error) {
	return "ok", nil
}

func TestDashboardDynamicAgentControl_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	agent := builderControlTestAgent{id: "agent-1"}
	hash := runtimeContextTestHash("8")
	workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		BundleSourceFact:  mustServeTestEphemeralBundleSourceFact(hash),
		WorkOwner:         workOwner,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture:               executionposture.Live,
		RuntimeShutdownAdmissionClosed: func() bool { return true },
		WorkOwner:                      workOwner,
		ReceiverExecution:              eventreceiver.NormalExecution(),
	})
	t.Cleanup(func() {
		_ = manager.Shutdown()
		_ = bus.ResetInMemoryState()
	})
	registerServeTestEphemeralAgent(t, manager, serveTestAgentConfig(runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id}))

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	fact := mustServeTestEphemeralBundleSourceFact(hash)
	rt := &runtimepkg.Runtime{ExecutionPosture: executionposture.Live, Bus: bus, Manager: manager}
	contexts, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleSourceFact: fact, Source: source, Runtime: rt, WorkOwner: workOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	supervisor := &runtimeProjectSupervisor{
		currentSource: source, currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentBundleSourceFact: fact,
		currentRT: rt, runtimeContexts: contexts, executionPosture: executionposture.Live,
	}
	control := dashboardDynamicAgentControl{supervisor: supervisor}

	if _, err := control.Restart(context.Background(), runtimeagentcontrol.RestartRequest{AgentID: agent.id}); err == nil || !strings.Contains(err.Error(), "runtime shutting down") {
		t.Fatalf("Restart err = %v, want runtime shutting down", err)
	}
	if _, err := control.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, Directive: "run corpus"}); err == nil || !strings.Contains(err.Error(), "agent not running") {
		t.Fatalf("SendDirective err = %v, want agent not running", err)
	}
}
