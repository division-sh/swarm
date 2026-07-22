package serveapp

import (
	"context"
	"encoding/json"
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

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

type failOnceFinalizeStartupOwnershipStore struct {
	delegate runtimestartupownership.Store

	mu               sync.Mutex
	prepareCount     int
	finalizeAttempts int
	failed           bool
}

func (s *failOnceFinalizeStartupOwnershipStore) AcquireRuntimeStartupOwnership(ctx context.Context, req runtimestartupownership.AcquireRequest) (runtimestartupownership.Lease, error) {
	lease, err := s.delegate.AcquireRuntimeStartupOwnership(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return &failOnceFinalizeStartupOwnershipLease{delegate: lease, owner: s}, nil
}

func (s *failOnceFinalizeStartupOwnershipStore) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepareCount, s.finalizeAttempts
}

type failOnceFinalizeStartupOwnershipLease struct {
	delegate runtimestartupownership.Lease
	owner    *failOnceFinalizeStartupOwnershipStore
}

func (l *failOnceFinalizeStartupOwnershipLease) Authority() (runtimestartupownership.Authority, error) {
	return l.delegate.Authority()
}

func (l *failOnceFinalizeStartupOwnershipLease) MarkProbesSettled(ctx context.Context, surfaceIDs []string) (runtimestartupownership.Authority, error) {
	return l.delegate.MarkProbesSettled(ctx, surfaceIDs)
}

func (l *failOnceFinalizeStartupOwnershipLease) AdmitExecution(ctx context.Context) (runtimestartupownership.Authority, error) {
	return l.delegate.AdmitExecution(ctx)
}

func (l *failOnceFinalizeStartupOwnershipLease) PrepareHandoff(ctx context.Context, req runtimestartupownership.HandoffRequest) (runtimestartupownership.Handoff, error) {
	handoff, err := l.delegate.PrepareHandoff(ctx, req)
	if err != nil || handoff == nil {
		return handoff, err
	}
	l.owner.mu.Lock()
	l.owner.prepareCount++
	l.owner.mu.Unlock()
	return &failOnceFinalizeStartupOwnershipHandoff{delegate: handoff, owner: l.owner}, nil
}

func (l *failOnceFinalizeStartupOwnershipLease) Release(ctx context.Context) error {
	return l.delegate.Release(ctx)
}

type failOnceFinalizeStartupOwnershipHandoff struct {
	delegate runtimestartupownership.Handoff
	owner    *failOnceFinalizeStartupOwnershipStore
}

func (h *failOnceFinalizeStartupOwnershipHandoff) Authority() (runtimestartupownership.Authority, error) {
	return h.delegate.Authority()
}

func (h *failOnceFinalizeStartupOwnershipHandoff) MarkProbesSettled(ctx context.Context, surfaceIDs []string) (runtimestartupownership.Authority, error) {
	return h.delegate.MarkProbesSettled(ctx, surfaceIDs)
}

func (h *failOnceFinalizeStartupOwnershipHandoff) Commit(ctx context.Context) (runtimestartupownership.Authority, error) {
	return h.delegate.Commit(ctx)
}

func (h *failOnceFinalizeStartupOwnershipHandoff) Rollback(ctx context.Context) (runtimestartupownership.Authority, error) {
	return h.delegate.Rollback(ctx)
}

func (h *failOnceFinalizeStartupOwnershipHandoff) Finalize(ctx context.Context) (runtimestartupownership.Authority, error) {
	h.owner.mu.Lock()
	h.owner.finalizeAttempts++
	if !h.owner.failed {
		h.owner.failed = true
		h.owner.mu.Unlock()
		return runtimestartupownership.Authority{}, errors.New("injected startup ownership finalize failure")
	}
	h.owner.mu.Unlock()
	return h.delegate.Finalize(ctx)
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
	oldRuntime := &runtimepkg.Runtime{}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := newRuntimeProjectSupervisor(
		repo, spec, nil, storeBundle{}, &ready, cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{NoWorkspace: true, Source: "test"},
		nil, nil, catalog, "/old", &runtimecontracts.WorkflowContractBundle{},
		semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), oldRuntime,
	)
	supervisor.loadProviderCatalog = func() (*providertriggers.CatalogSnapshot, error) { return catalog, nil }
	supervisor.loadWorkflow = func(_, contractsRoot, _ string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if contractsRoot != root {
			t.Fatalf("contracts root = %q, want %q", contractsRoot, root)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) error {
		opts := runtimepkg.DefaultWorkflowContractValidationOptions(nil)
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

func TestRuntimeProjectSupervisorReloadRecompilesAndInstallsChannelPlans(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	var captured runtimepkg.RuntimeDeps
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		captured = deps
		return &runtimepkg.Runtime{Options: deps.Options}, nil
	})
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	loads := 0
	supervisor.SetChannelPackLoader(func(_ context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) (cliapp.ChannelPackLoad, error) {
		loads++
		if source == nil || catalog == nil {
			t.Fatal("channel reload compiler received nil source or accepted trigger catalog")
		}
		plan := packs.SatisfactionPlan{Channel: packs.PackIdentity{ID: "provider.mock.channel"}}
		binding := packs.OutboundBindingPlan{ID: "ops", Structural: plan}
		return cliapp.ChannelPackLoad{Plans: []packs.SatisfactionPlan{plan}, Bindings: []packs.OutboundBindingPlan{binding}}, nil
	})

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if loads != 1 {
		t.Fatalf("channel compiler calls = %d, want one", loads)
	}
	if len(captured.Options.ChannelPlans) != 1 || captured.Options.ChannelPlans[0].Channel.ID != "provider.mock.channel" {
		t.Fatalf("replacement runtime channel plans = %#v", captured.Options.ChannelPlans)
	}
	if len(captured.Options.ChannelOutboundBindings) != 1 || captured.Options.ChannelOutboundBindings[0].ID != "ops" {
		t.Fatalf("replacement runtime channel bindings = %#v", captured.Options.ChannelOutboundBindings)
	}
}

func TestRuntimeProjectSupervisorReplaceCurrentRuntime_ClearsReadinessBeforeShutdown(t *testing.T) {
	oldRT := &runtimepkg.Runtime{}
	newRT := &runtimepkg.Runtime{}
	var ready atomic.Bool
	ready.Store(true)

	supervisor := &runtimeProjectSupervisor{
		ready:         &ready,
		currentRoot:   "/tmp/old-project",
		currentBundle: &runtimecontracts.WorkflowContractBundle{},
		currentSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:     oldRT,
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
	oldRT := &runtimepkg.Runtime{}
	newRT := &runtimepkg.Runtime{}
	var ready atomic.Bool
	ready.Store(true)
	started := make(chan struct{})
	releaseStart := make(chan struct{})

	supervisor := &runtimeProjectSupervisor{
		ready:         &ready,
		currentRoot:   "/tmp/old-project",
		currentBundle: &runtimecontracts.WorkflowContractBundle{},
		currentSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:     oldRT,
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
	makeContext := func(hash, alias, runID, entityID string) (runtimepkg.BundleContext, *processIngressProofStore, *processIngressEventStore) {
		persistence := &processIngressProofStore{}
		eventsStore := &processIngressEventStore{}
		persistence.store = eventsStore
		workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
		bus, err := runtimebus.NewEventBusWithOptions(eventsStore, runtimebus.EventBusOptions{ProviderOutputVerifier: catalog, WorkOwner: workOwner})
		if err != nil {
			t.Fatalf("NewEventBusWithOptions(%s): %v", alias, err)
		}
		t.Cleanup(func() {
			if err := bus.ResetInMemoryState(); err != nil {
				t.Errorf("retire process ingress test bus %s: %v", alias, err)
			}
		})
		gateway := runtimepkg.NewInboundGateway(bus, nil, nil, persistence)
		gateway.SetCredentialStore(processIngressCredentialStore{"webhook_signing.telegram": "telegram-secret"})
		plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: alias, Provider: "telegram", SigningSecret: "webhook_signing.telegram"})
		if err != nil {
			t.Fatalf("CompileAdmission(%s): %v", alias, err)
		}
		return runtimepkg.BundleContext{
			BundleHash: hash, Source: source, Runtime: &runtimepkg.Runtime{Bus: bus, InboundGateway: gateway}, WorkOwner: workOwner,
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, ServiceID: "service-" + alias, FlowID: "telegram-chat", Alias: alias, Provider: "telegram",
				RunID: runID, FlowInstance: "telegram-chat/" + strings.TrimPrefix(alias, "chat-"),
				EntityID: entityID, Generation: 1, SigningSecret: "webhook_signing.telegram", AdmissionPlan: plan,
			}},
		}, persistence, eventsStore
	}
	hashA := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	hashB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	contextA, persistenceA, eventsA := makeContext(hashA, "chat-a", "41000000-0000-0000-0000-000000000001", "41000000-0000-0000-0000-000000000002")
	contextB, persistenceB, eventsB := makeContext(hashB, "chat-b", "42000000-0000-0000-0000-000000000001", "42000000-0000-0000-0000-000000000002")
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtimepkg.NewRuntimeContextManagerWithAdmission(nil, runtimepkg.ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}, contextA, contextB)
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
		t.Fatalf("selected-context response = %d %q, want 202", rec.Code, rec.Body.String())
	}
	if persistenceA.recorded || len(eventsA.events) != 0 {
		t.Fatalf("non-selected context A was touched: publication=%v events=%d", persistenceA.recorded, len(eventsA.events))
	}
	if !persistenceB.recorded || len(eventsB.events) != 2 {
		t.Fatalf("selected context B publication/events = %v/%d, want true and raw plus normalized", persistenceB.recorded, len(eventsB.events))
	}
	if got := eventsB.events[0].RunID(); got != contextB.StandingTargets[0].RunID {
		t.Fatalf("selected event run_id = %q, want %q", got, contextB.StandingTargets[0].RunID)
	}
}

func TestRuntimeProjectSupervisorFailedSameHashReplacementRestoresOldContext(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	oldBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	oldRT := &runtimepkg.Runtime{Bus: oldBus}
	newRT := &runtimepkg.Runtime{Bus: newBus}
	restoredRT := &runtimepkg.Runtime{Bus: oldBus}
	hash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	restoredWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: hash, Source: source, Runtime: oldRT, WorkOwner: oldWorkOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourcePersisted}
	newRT.Options = runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}
	restoredRT.Options = runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: oldRT,
		currentBundleSourceFact: fact, runtimeContexts: manager,
	}
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

func TestRuntimeProjectSupervisorChangedNonStandingBundleReplacesManagerContext(t *testing.T) {
	oldBundle := &runtimecontracts.WorkflowContractBundle{}
	newBundle := &runtimecontracts.WorkflowContractBundle{}
	oldSource := semanticview.Wrap(oldBundle)
	newSource := semanticview.Wrap(newBundle)
	oldBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	oldRT := &runtimepkg.Runtime{Bus: oldBus}
	newRT := &runtimepkg.Runtime{Bus: newBus}
	oldHash := "bundle-v1:sha256:" + strings.Repeat("1", 64)
	newHash := "bundle-v1:sha256:" + strings.Repeat("2", 64)
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, oldHash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, newHash)
	oldFact := runtimecorrelation.BundleSourceFact{BundleHash: oldHash, BundleSource: storerunlifecycle.BundleSourcePersisted}
	newFact := runtimecorrelation.BundleSourceFact{BundleHash: newHash, BundleSource: storerunlifecycle.BundleSourcePersisted}
	newIdentity := runtimecontracts.BundleIdentity{BundleHash: newHash}
	oldRT.Options = runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: oldSource}, BundleSourceFact: oldFact}
	newRT.Options = runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: newSource}, BundleSourceFact: newFact}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: oldHash, BundleSourceFact: oldFact, Source: oldSource, Runtime: oldRT, WorkOwner: oldWorkOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: oldSource,
		currentBundle: oldBundle, currentRT: oldRT, currentBundleSourceFact: oldFact,
		runtimeContexts: manager,
	}
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
	oldBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(old): %v", err)
	}
	newBus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus(new): %v", err)
	}
	hash := runtimeContextTestHash("d")
	fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
	oldRT := &runtimepkg.Runtime{Bus: oldBus, Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}}
	newRT := &runtimepkg.Runtime{Bus: newBus, Options: runtimepkg.RuntimeOptions{WorkflowModule: stubWorkflowModule{source: source}, BundleSourceFact: fact}}
	oldWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	newWorkOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{BundleHash: hash, BundleSourceFact: fact, Source: source, Runtime: oldRT, WorkOwner: oldWorkOwner})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/old", currentSource: source, currentBundle: bundle,
		currentRT: oldRT, currentBundleSourceFact: fact, runtimeContexts: manager,
	}
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

func TestRuntimeProjectSupervisorReplacementTransfersRealStartupOwnership(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) storeBundle
	}
	backends := []backend{
		{
			name: "sqlite",
			open: func(t *testing.T) storeBundle {
				stores, err := buildStores(context.Background(), storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite")}, &config.Config{})
				if err != nil {
					t.Fatalf("build SQLite stores: %v", err)
				}
				t.Cleanup(func() { _ = stores.SQLDB.Close() })
				return stores
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) storeBundle {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				store, err := store.NewPostgresStore(dsn)
				if err != nil {
					t.Fatalf("NewPostgresStore: %v", err)
				}
				t.Cleanup(func() { _ = store.DB.Close() })
				return selectedPostgresStoreBundle(store, &config.Config{})
			},
		},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			for _, changedHash := range []bool{false, true} {
				changedHash := changedHash
				name := "same_hash"
				if changedHash {
					name = "changed_nonstanding_hash"
				}
				t.Run(name, func(t *testing.T) {
					stores := backend.open(t)
					startupOwnership := &failOnceFinalizeStartupOwnershipStore{delegate: stores.StartupOwnership}
					stores.StartupOwnership = startupOwnership
					processWorkOwner := newSupervisorTestProcessOwner(t)
					runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
					var active, maxActive atomic.Int32
					bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
					if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
						t.Fatalf("initializeStateStores: %v", err)
					}
					source := semanticview.Wrap(bundle)
					module := stubWorkflowModule{source: source}
					providerRegistry := testProviderTriggerCatalog(t)
					oldHash := runtimeContextTestHash("a")
					newHash := oldHash
					if changedHash {
						newHash = runtimeContextTestHash("b")
					}
					newRuntime := func(hash string) *runtimepkg.Runtime {
						rt, err := runtimepkg.NewRuntime(context.Background(), runtimepkg.RuntimeDeps{
							Config: &config.Config{},
							Stores: stores.runtimeStores(),
							Options: runtimepkg.RuntimeOptions{
								SelfCheck:                        false,
								WorkflowModule:                   module,
								LLMRuntime:                       runtimellm.NoopRuntime{},
								DisablePersistentStartupRecovery: true,
								ProviderTriggerCatalog:           providerRegistry,
								ProcessWorkOwner:                 processWorkOwner,
								RuntimeInstanceID:                runtimeInstanceID,
								BundleSourceFact: runtimecorrelation.BundleSourceFact{
									BundleHash:   hash,
									BundleSource: storerunlifecycle.BundleSourceEphemeral,
								},
							},
						})
						if err != nil {
							t.Fatalf("NewRuntime(%s): %v", hash, err)
						}
						t.Cleanup(func() { _ = rt.Shutdown() })
						return rt
					}
					predecessor := newRuntime(oldHash)
					predecessor.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					if err := predecessor.Start(context.Background()); err != nil {
						t.Fatalf("start predecessor: %v", err)
					}
					if predecessor.Manager == nil || !predecessor.Manager.IsRunning() || !predecessor.Bus.OutboxSweeperActive() {
						t.Fatal("full-store predecessor manager/outbox consumers did not start")
					}
					if err := predecessor.Scheduler.Register(context.Background(), runtimepipeline.Schedule{
						AgentID: "replacement-proof", EventType: "platform.boot", Mode: "once", At: time.Now().Add(time.Hour),
					}); err != nil {
						t.Fatalf("register pending predecessor schedule: %v", err)
					}
					probe := newRuntime(newHash)
					if err := probe.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already owned") {
						t.Fatalf("ordinary competing start error = %v, want exclusive ownership denial", err)
					}

					oldFact := runtimecorrelation.BundleSourceFact{BundleHash: oldHash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
					newFact := runtimecorrelation.BundleSourceFact{BundleHash: newHash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
					manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
						BundleHash: oldHash, BundleSourceFact: oldFact, Source: source, Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence(),
					})
					if err != nil {
						t.Fatalf("NewRuntimeContextManager: %v", err)
					}
					candidate := newRuntime(newHash)
					candidate.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					supervisor := &runtimeProjectSupervisor{
						ready:                   new(atomic.Bool),
						currentRoot:             "/old",
						currentSource:           source,
						currentBundle:           bundle,
						currentRT:               predecessor,
						currentBundleSourceFact: oldFact,
						runtimeContexts:         manager,
					}
					supervisor.startRuntime = func(ctx context.Context, rt *runtimepkg.Runtime) error {
						if rt == candidate {
							if active.Load() != 0 || predecessor.Manager.IsRunning() || predecessor.Bus.OutboxSweeperActive() {
								t.Fatalf("candidate activation began before predecessor consumers quiesced: node=%d manager=%v outbox=%v", active.Load(), predecessor.Manager.IsRunning(), predecessor.Bus.OutboxSweeperActive())
							}
						}
						return rt.Start(ctx)
					}
					supervisor.ready.Store(true)
					status, err := supervisor.replaceCurrentRuntimeWithSource(
						context.Background(), "/new", source, bundle, newFact,
						runtimecontracts.BundleIdentity{BundleHash: newHash}, candidate, candidate.WorkOccurrence(),
					)
					if err == nil || !strings.Contains(err.Error(), "injected startup ownership finalize failure") {
						t.Fatalf("first replacement finalization error = %v", err)
					}
					if supervisor.ready.Load() || supervisor.pendingReplacement == nil || supervisor.CurrentRuntime() != predecessor {
						t.Fatalf("failed finalization visibility = status:%#v ready:%v runtime:%p pending:%#v", status, supervisor.ready.Load(), supervisor.CurrentRuntime(), supervisor.pendingReplacement)
					}
					lookup := manager.LookupBundleHashStatus(oldHash)
					if lookup.Loaded() || lookup.Cause != runtimepkg.RuntimeContextCauseReplacing {
						t.Fatalf("failed finalization selector = %#v, want unavailable replacing", lookup)
					}
					if prepareCount, finalizeAttempts := startupOwnership.counts(); prepareCount != 1 || finalizeAttempts != 1 {
						t.Fatalf("failed finalization counts = prepare:%d finalize:%d, want 1/1", prepareCount, finalizeAttempts)
					}
					if err := supervisor.completePendingReplacement(); err != nil {
						t.Fatalf("retry retained replacement finalization: %v", err)
					}
					status = supervisor.CurrentProject()
					if prepareCount, finalizeAttempts := startupOwnership.counts(); prepareCount != 1 || finalizeAttempts != 2 {
						t.Fatalf("retried finalization counts = prepare:%d finalize:%d, want exact retained handoff 1/2", prepareCount, finalizeAttempts)
					}
					if !status.Loaded || supervisor.CurrentRuntime() != candidate {
						t.Fatalf("replacement status/runtime = %#v/%p, want loaded candidate %p", status, supervisor.CurrentRuntime(), candidate)
					}
					if _, ok := manager.LookupBundleHash(newHash); !ok {
						t.Fatalf("manager does not expose replacement hash %s", newHash)
					}
					if got := maxActive.Load(); got != 1 {
						t.Fatalf("simultaneous predecessor/candidate system consumers = %d, want one", got)
					}
					successor := newRuntime(newHash)
					successor.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					status, err = supervisor.replaceCurrentRuntimeWithSource(
						context.Background(), "/successor", source, bundle, newFact,
						runtimecontracts.BundleIdentity{BundleHash: newHash}, successor, successor.WorkOccurrence(),
					)
					if err != nil || !status.Loaded || supervisor.CurrentRuntime() != successor {
						t.Fatalf("later replacement = status:%#v runtime:%p err:%v, want successor %p", status, supervisor.CurrentRuntime(), err, successor)
					}
					if prepareCount, finalizeAttempts := startupOwnership.counts(); prepareCount != 2 || finalizeAttempts != 3 {
						t.Fatalf("later replacement counts = prepare:%d finalize:%d, want 2/3", prepareCount, finalizeAttempts)
					}
					if err := successor.Shutdown(); err != nil {
						t.Fatalf("shutdown later replacement: %v", err)
					}

					rollbackPredecessor := newRuntime(newHash)
					rollbackPredecessor.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					if err := rollbackPredecessor.Start(context.Background()); err != nil {
						t.Fatalf("start rollback predecessor: %v", err)
					}
					if err := rollbackPredecessor.Scheduler.Register(context.Background(), runtimepipeline.Schedule{
						AgentID: "rollback-proof", EventType: "platform.boot", Mode: "once", At: time.Now().Add(time.Hour),
					}); err != nil {
						t.Fatalf("register pending rollback schedule: %v", err)
					}
					rollbackFact := runtimecorrelation.BundleSourceFact{BundleHash: newHash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
					rollbackManager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
						BundleHash: newHash, BundleSourceFact: rollbackFact, Source: source, Runtime: rollbackPredecessor, WorkOwner: rollbackPredecessor.WorkOccurrence(),
					})
					if err != nil {
						t.Fatalf("NewRuntimeContextManager rollback: %v", err)
					}
					failingCandidate := newRuntime(newHash)
					failingCandidate.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					restored := newRuntime(newHash)
					restored.SystemNodes = []runtimepipeline.BackgroundNode{newReplacementOverlapProbeNode(&active, &maxActive)}
					rollbackSupervisor := &runtimeProjectSupervisor{
						ready:                   new(atomic.Bool),
						currentRoot:             "/rollback-old",
						currentSource:           source,
						currentBundle:           bundle,
						currentRT:               rollbackPredecessor,
						currentBundleSourceFact: rollbackFact,
						runtimeContexts:         rollbackManager,
					}
					rollbackSupervisor.ready.Store(true)
					rollbackSupervisor.cloneRuntime = func(context.Context, *runtimepkg.Runtime) (*runtimepkg.Runtime, *worklifetime.RuntimeOccurrence, error) {
						return restored, restored.WorkOccurrence(), nil
					}
					rollbackSupervisor.startRuntime = func(ctx context.Context, rt *runtimepkg.Runtime) error {
						if rt == failingCandidate && (active.Load() != 0 || rollbackPredecessor.Manager.IsRunning() || rollbackPredecessor.Bus.OutboxSweeperActive()) {
							t.Fatalf("failing candidate activation overlapped predecessor consumers")
						}
						if err := rt.Start(ctx); err != nil {
							return err
						}
						if rt == failingCandidate {
							return errors.New("injected post-start precommit failure")
						}
						return nil
					}
					_, err = rollbackSupervisor.replaceCurrentRuntimeWithSource(
						context.Background(), "/rollback-candidate", source, bundle, rollbackFact,
						runtimecontracts.BundleIdentity{BundleHash: newHash}, failingCandidate, failingCandidate.WorkOccurrence(),
					)
					if err == nil || !strings.Contains(err.Error(), "injected post-start precommit failure") {
						t.Fatalf("precommit replacement error = %v", err)
					}
					lookup = rollbackManager.LookupBundleHashStatus(newHash)
					if !lookup.Loaded() || lookup.Context.Runtime != nil || rollbackSupervisor.CurrentRuntime() != restored {
						t.Fatalf("precommit rollback authority = %#v/%p, want restored runtime %p", lookup, rollbackSupervisor.CurrentRuntime(), restored)
					}
					use, _, acquireErr := rollbackManager.AcquireBundleHash(context.Background(), newHash)
					if acquireErr != nil || use == nil || use.Runtime() != restored {
						t.Fatalf("precommit rollback execution authority = use:%#v err:%v", use, acquireErr)
					}
					if err := use.Done(); err != nil {
						t.Fatalf("settle precommit rollback authority: %v", err)
					}
					if got := maxActive.Load(); got != 1 {
						t.Fatalf("rollback overlapped shared-store consumers: max=%d", got)
					}
					if err := restored.Shutdown(); err != nil {
						t.Fatalf("shutdown restored predecessor: %v", err)
					}
				})
			}
		})
	}
}

func TestStandingReplacementAdoptionRestoresWorkflowTimersOnBothStores(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) storeBundle
	}
	backends := []backend{
		{
			name: "sqlite",
			open: func(t *testing.T) storeBundle {
				stores, err := buildStores(context.Background(), storebackend.Selection{
					Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite"),
				}, &config.Config{})
				if err != nil {
					t.Fatalf("build SQLite stores: %v", err)
				}
				t.Cleanup(func() { _ = stores.SQLDB.Close() })
				return stores
			},
		},
		{
			name: "postgres",
			open: func(t *testing.T) storeBundle {
				dsn, _, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected, err := store.NewPostgresStore(dsn)
				if err != nil {
					t.Fatalf("NewPostgresStore: %v", err)
				}
				t.Cleanup(func() { _ = selected.DB.Close() })
				return selectedPostgresStoreBundle(selected, &config.Config{})
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
			if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
				t.Fatalf("initialize state stores: %v", err)
			}
			bundleHash, err := runtimecontracts.BundleHash(bundle)
			if err != nil {
				t.Fatalf("BundleHash: %v", err)
			}
			fact := runtimecorrelation.BundleSourceFact{
				BundleHash: bundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral,
			}
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
			processWorkOwner := newSupervisorTestProcessOwner(t)
			newRuntime := func(instanceID string) *runtimepkg.Runtime {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimepkg.RuntimeDeps{
					Config: &config.Config{}, Stores: stores.runtimeStores(),
					Options: runtimepkg.RuntimeOptions{
						WorkflowModule: module, LLMRuntime: runtimellm.NoopRuntime{},
						Credentials: credentials, ProviderCredentials: credentials,
						ProviderTriggerCatalog: testProviderTriggerCatalog(t),
						ProcessWorkOwner:       processWorkOwner,
						RuntimeInstanceID:      instanceID, BundleSourceFact: fact,
					},
				})
				if err != nil {
					t.Fatalf("NewRuntime(%s): %v", instanceID, err)
				}
				return rt
			}

			candidate := newRuntime("22222222-2222-2222-2222-222222222222")
			if err := candidate.PrepareAuthorActivityCatalog(); err != nil {
				t.Fatalf("prepare standing replacement candidate author activity: %v", err)
			}
			if err := candidate.Start(context.Background()); err != nil {
				t.Fatalf("start standing replacement candidate: %v", err)
			}
			t.Cleanup(func() {
				_ = candidate.ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second})
			})

			predecessor := newRuntime("11111111-1111-1111-1111-111111111111")
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

			var timerEvent, timerStatus string
			var fireAt any
			if err := stores.SQLDB.QueryRowContext(context.Background(), `SELECT fire_event, status, fire_at FROM timers`).Scan(&timerEvent, &timerStatus, &fireAt); err != nil {
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
				if err := stores.SQLDB.QueryRowContext(context.Background(), `SELECT status FROM timers`).Scan(&timerStatus); err != nil {
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
			if err := stores.SQLDB.QueryRowContext(context.Background(), query, timerEvent).Scan(&events); err != nil {
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
		open func(*testing.T) storeBundle
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) storeBundle {
			stores, err := buildStores(context.Background(), storebackend.Selection{
				Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite"),
			}, &config.Config{})
			if err != nil {
				t.Fatalf("build SQLite stores: %v", err)
			}
			t.Cleanup(func() { _ = stores.SQLDB.Close() })
			return stores
		}},
		{name: "postgres", open: func(t *testing.T) storeBundle {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			selected, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			t.Cleanup(func() { _ = selected.DB.Close() })
			return selectedPostgresStoreBundle(selected, &config.Config{})
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
					withTimer := strings.Replace(string(rawSchema), "  active:\n    initial: true\n    gate:", "  active:\n    initial: true\n    timers:\n      - after: 30s\n        advances_to: done\n    gate:", 1)
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
					if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
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
					if changedHash {
						writeStandingCandidateFile(t, filepath.Join(contractsRoot, "flows", "telegram-chat", "prompts", "phrase-bot.md"), "Reply to each Telegram message by emitting telegram.reply_requested with chat_id set to the event conversation_reference. Keep the response concise.\n")
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
					admissionState := runtimepkg.ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed}
					processWorkOwner := newSupervisorTestProcessOwner(t)
					var manager *runtimepkg.RuntimeContextManager
					var createdRuntimes []*runtimepkg.Runtime
					t.Cleanup(func() {
						if manager != nil {
							for _, result := range manager.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
								if result.ShutdownErr != nil {
									t.Errorf("deactivate replacement context %s: %v", result.BundleHash, result.ShutdownErr)
								}
							}
						}
						for i := len(createdRuntimes) - 1; i >= 0; i-- {
							_ = createdRuntimes[i].ShutdownWithOptions(runtimepkg.ShutdownOptions{Grace: 5 * time.Second})
						}
					})
					newRuntime := func(hash string, workflowModule runtimepipeline.WorkflowModule) *runtimepkg.Runtime {
						rt, err := runtimepkg.NewRuntime(context.Background(), runtimepkg.RuntimeDeps{
							Config: &config.Config{}, Stores: stores.runtimeStores(),
							Options: runtimepkg.RuntimeOptions{
								WorkflowModule: workflowModule, LLMRuntime: runtimellm.NoopRuntime{},
								Credentials: credentials, ProviderCredentials: credentials,
								ProviderTriggerCatalog: catalog, ProcessWorkOwner: processWorkOwner,
								RuntimeInstanceID: "11111111-1111-1111-1111-111111111111",
								BundleSourceFact: runtimecorrelation.BundleSourceFact{
									BundleHash: hash, BundleSource: storerunlifecycle.BundleSourceEphemeral,
								},
							},
						})
						if err != nil {
							t.Fatalf("NewRuntime(%s): %v", hash, err)
						}
						createdRuntimes = append(createdRuntimes, rt)
						return rt
					}

					predecessor := newRuntime(oldHash, module)
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
					oldFact := runtimecorrelation.BundleSourceFact{BundleHash: oldHash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
					manager, err = runtimepkg.NewRuntimeContextManagerWithAdmission(nil, admissionState, runtimepkg.BundleContext{
						BundleHash: oldHash, BundleSourceFact: oldFact, Source: oldSource,
						Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence(), StandingTargets: targets,
					})
					if err != nil {
						t.Fatalf("NewRuntimeContextManagerWithAdmission: %v", err)
					}
					candidate := newRuntime(newHash, candidateModule)
					newFact := runtimecorrelation.BundleSourceFact{BundleHash: newHash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
					supervisor := &runtimeProjectSupervisor{
						ready: new(atomic.Bool), currentRoot: contractsRoot, currentSource: oldSource, currentBundle: bundle,
						currentRT: predecessor, currentBundleSourceFact: oldFact, runtimeContexts: manager,
						providerTriggers: catalog, replacementShutdown: runtimepkg.ShutdownOptions{Grace: 5 * time.Second},
					}
					supervisor.ready.Store(true)
					admission := &processAdmissionCandidate{catalog: catalog, state: admissionState, survivingTargets: map[string][]runtimepkg.StandingTarget{}}
					if _, err := supervisor.replaceCurrentRuntimeWithSourceAndAdmission(
						context.Background(), contractsRoot, candidateSource, candidateBundle, newFact,
						runtimecontracts.BundleIdentity{BundleHash: newHash}, candidate, candidate.WorkOccurrence(), admission,
					); err != nil {
						t.Fatalf("replace standing runtime: %v", err)
					}
					if supervisor.CurrentRuntime() != candidate || !supervisor.ready.Load() {
						t.Fatal("standing candidate did not publish after aggregate timer transfer")
					}

					var timerEvent, timerStatus string
					if err := stores.SQLDB.QueryRowContext(context.Background(), `SELECT fire_event, status FROM timers`).Scan(&timerEvent, &timerStatus); err != nil {
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
					if err := stores.SQLDB.QueryRowContext(context.Background(), query, timerEvent).Scan(&count); err != nil {
						t.Fatalf("count adopted timer events: %v", err)
					}
					if count != 0 {
						t.Fatalf("adopted timer events at publication = %d, want 0", count)
					}
				})
			}
		})
	}
}

func TestRuntimeProjectSupervisorQuiesceTimeoutRestoresFullStoreAuthority(t *testing.T) {
	type backend struct {
		name string
		open func(*testing.T) storeBundle
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) storeBundle {
			stores, err := buildStores(context.Background(), storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite")}, &config.Config{})
			if err != nil {
				t.Fatalf("build SQLite stores: %v", err)
			}
			t.Cleanup(func() { _ = stores.SQLDB.Close() })
			return stores
		}},
		{name: "postgres", open: func(t *testing.T) storeBundle {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			t.Cleanup(func() { _ = pg.DB.Close() })
			return selectedPostgresStoreBundle(pg, &config.Config{})
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			processWorkOwner := newSupervisorTestProcessOwner(t)
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
				t.Fatalf("initializeStateStores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			providerRegistry := testProviderTriggerCatalog(t)
			hash := runtimeContextTestHash("f")
			runtimeInstanceID := "11111111-1111-1111-1111-111111111111"
			fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
			newRuntime := func() *runtimepkg.Runtime {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimepkg.RuntimeDeps{
					Config: &config.Config{}, Stores: stores.runtimeStores(),
					Options: runtimepkg.RuntimeOptions{SelfCheck: false, WorkflowModule: stubWorkflowModule{source: source}, LLMRuntime: runtimellm.NoopRuntime{}, DisablePersistentStartupRecovery: true, ProviderTriggerCatalog: providerRegistry, ProcessWorkOwner: processWorkOwner, BundleSourceFact: fact, RuntimeInstanceID: runtimeInstanceID},
				})
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				t.Cleanup(func() { _ = rt.Shutdown() })
				return rt
			}
			var active, maxActive atomic.Int32
			blocker := newReplacementQuiesceBlockNode(&active, &maxActive)
			t.Cleanup(blocker.Release)
			predecessor := newRuntime()
			predecessor.SystemNodes = []runtimepipeline.BackgroundNode{blocker}
			if err := predecessor.Start(context.Background()); err != nil {
				t.Fatalf("start predecessor: %v", err)
			}
			manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{BundleHash: hash, BundleSourceFact: fact, Source: source, Runtime: predecessor, WorkOwner: predecessor.WorkOccurrence()})
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
				currentRT: predecessor, currentBundleSourceFact: fact, runtimeContexts: manager,
				replacementShutdown: runtimepkg.ShutdownOptions{Grace: 20 * time.Millisecond},
			}
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
				_, err := supervisor.replaceCurrentRuntimeWithSource(context.Background(), "/new", source, bundle, fact, runtimecontracts.BundleIdentity{BundleHash: hash}, candidate, candidate.WorkOccurrence())
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
			if err := probe.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already owned") {
				t.Fatalf("competing start during failed quiesce = %v", err)
			}

			blocker.Release()
			select {
			case err := <-replacementDone:
				if err == nil || !strings.Contains(err.Error(), "quiesce predecessor runtime before replacement") {
					t.Fatalf("replacement error = %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for predecessor restoration")
			}
			lookup = manager.LookupBundleHashStatus(hash)
			if !ready.Load() || !lookup.Loaded() || lookup.Context.Runtime != nil || supervisor.CurrentRuntime() != restored {
				t.Fatalf("restored visibility = ready:%v lookup:%#v runtime:%p", ready.Load(), lookup, supervisor.CurrentRuntime())
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
			if err := secondProbe.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already owned") {
				t.Fatalf("competing start after restoration = %v", err)
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
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	hash := "bundle-v1:sha256:" + strings.Repeat("9", 64)
	workOwner := newSupervisorTestRuntimeOccurrence(t, hash)
	fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourcePersisted}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{BundleHash: hash, BundleSourceFact: fact, Source: source, Runtime: rt, WorkOwner: workOwner})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	supervisor := &runtimeProjectSupervisor{
		currentRoot: "/tmp/current", currentSource: source, currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: rt,
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

type processIngressProofStore struct {
	recorded bool
	store    runtimebus.EventStore
}

type processIngressMutation struct {
	ctx          context.Context
	store        runtimebus.EventStore
	finalization runtimeinbound.Finalization
	finalized    bool
}

func (m *processIngressMutation) Context() context.Context { return m.ctx }

func (m *processIngressMutation) FinalizeInboundPublication(_ context.Context, finalization runtimeinbound.Finalization) error {
	m.finalization = finalization
	m.finalized = true
	return nil
}

func (s *processIngressProofStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	s.recorded = true
	mutation := &processIngressMutation{store: s.store}
	transaction, ok := s.store.(runtimebus.CommitPublishTransaction)
	if !ok {
		return runtimeinbound.Record{}, errors.New("process ingress store does not expose its test commit transaction")
	}
	mutation.ctx = runtimebus.WithCommitPublishTransaction(ctx, transaction)
	if err := fn(mutation); err != nil {
		return runtimeinbound.Record{}, err
	}
	if !mutation.finalized {
		return runtimeinbound.Record{}, errors.New("process ingress publication was not finalized")
	}
	children := make([]runtimeinbound.EventRecord, len(mutation.finalization.Events))
	for i, finalized := range mutation.finalization.Events {
		var routes []events.DeliveryRoute
		if err := json.Unmarshal(finalized.RecipientManifest, &routes); err != nil {
			return runtimeinbound.Record{}, fmt.Errorf("decode process ingress recipient manifest: %w", err)
		}
		_, recipientFingerprint, recipientCount, err := runtimeinbound.CanonicalRecipientManifest(routes)
		if err != nil {
			return runtimeinbound.Record{}, err
		}
		eventFingerprint, err := runtimeinbound.EventIntegrityFingerprint(finalized.Event, finalized.Kind, finalized.Authorization)
		if err != nil {
			return runtimeinbound.Record{}, err
		}
		children[i] = runtimeinbound.EventRecord{
			Ordinal:                      finalized.Ordinal,
			EventID:                      finalized.Event.ID(),
			EventName:                    string(finalized.Event.Type()),
			Kind:                         finalized.Kind,
			Authorization:                finalized.Authorization,
			EventIntegrityFingerprint:    eventFingerprint,
			RecipientManifestFingerprint: recipientFingerprint,
			RecipientCount:               recipientCount,
			Event:                        finalized.Event,
		}
	}
	return runtimeinbound.Record{Request: request, State: "committed", OutputCount: len(children), Events: children, Created: true}, nil
}
func (*processIngressProofStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}
func (*processIngressProofStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

type processIngressEventStore struct {
	events []events.Event
	active []string
}

func (s *processIngressEventStore) CommitPublish(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return runtimebustest.CommitPublish(ctx, plan, s.beginPublish, s.finalizePublish)
}

func (s *processIngressEventStore) beginPublish(_ context.Context, admitted events.AdmittedEvent) (runtimebus.EventAppendOutcome, error) {
	event := admitted.Event()
	for _, existing := range s.events {
		if existing.ID() != event.ID() {
			continue
		}
		if !reflect.DeepEqual(existing, event) {
			return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("event %s conflicts with its committed fixture", event.ID())
		}
		return runtimebus.EventAppendExactDuplicate, nil
	}
	s.events = append(s.events, event)
	s.active = append(s.active, event.ID())
	return runtimebus.EventAppendInserted, nil
}

func (s *processIngressEventStore) finalizePublish(_ context.Context, req runtimebus.CommitPublishRequest) error {
	if len(s.active) == 0 || s.active[len(s.active)-1] != req.Event.ID() {
		return errors.New("prepared event finalization does not match active process ingress event")
	}
	s.active = s.active[:len(s.active)-1]
	return nil
}

func (s *processIngressEventStore) BeginPreparedPublish(ctx context.Context, prepared runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	return s.beginPublish(ctx, prepared.AdmittedEvent())
}

func (s *processIngressEventStore) FinalizePreparedPublish(ctx context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	return s.finalizePublish(ctx, finalization.Request())
}
func (*processIngressEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func TestRuntimeProjectSupervisorCloseProjectWithShutdownOptionsUsesConfiguredGrace(t *testing.T) {
	oldRT := &runtimepkg.Runtime{}
	var ready atomic.Bool
	ready.Store(true)
	wantGrace := 75 * time.Millisecond

	supervisor := &runtimeProjectSupervisor{
		ready:         &ready,
		currentRoot:   "/tmp/old-project",
		currentBundle: &runtimecontracts.WorkflowContractBundle{},
		currentSource: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}),
		currentRT:     oldRT,
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
		open func(*testing.T) storeBundle
	}
	backends := []backend{
		{name: "sqlite", open: func(t *testing.T) storeBundle {
			stores, err := buildStores(context.Background(), storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: filepath.Join(t.TempDir(), "runtime.sqlite")}, &config.Config{})
			if err != nil {
				t.Fatalf("build SQLite stores: %v", err)
			}
			t.Cleanup(func() { _ = stores.SQLDB.Close() })
			return stores
		}},
		{name: "postgres", open: func(t *testing.T) storeBundle {
			dsn, _, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				t.Fatalf("NewPostgresStore: %v", err)
			}
			t.Cleanup(func() { _ = pg.DB.Close() })
			return selectedPostgresStoreBundle(pg, &config.Config{})
		}},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			stores := backend.open(t)
			processWorkOwner := newSupervisorTestProcessOwner(t)
			bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier8-boot-verification/test-boot-success")
			if _, err := initializeStateStores(context.Background(), stores, bundle); err != nil {
				t.Fatalf("initializeStateStores: %v", err)
			}
			source := semanticview.Wrap(bundle)
			providerRegistry := testProviderTriggerCatalog(t)
			runtimeInstanceID := "11111111-1111-4111-8111-111111111111"
			facts := []runtimecorrelation.BundleSourceFact{
				{BundleHash: runtimeContextTestHash("c"), BundleSource: storerunlifecycle.BundleSourceEphemeral},
				{BundleHash: runtimeContextTestHash("d"), BundleSource: storerunlifecycle.BundleSourceEphemeral},
			}
			contexts := make([]serveRuntimeBundleContext, 0, len(facts))
			for _, fact := range facts {
				rt, err := runtimepkg.NewRuntime(context.Background(), runtimepkg.RuntimeDeps{
					Config: &config.Config{},
					Stores: stores.runtimeStores(),
					Options: runtimepkg.RuntimeOptions{
						SelfCheck:                        false,
						WorkflowModule:                   stubWorkflowModule{source: source},
						LLMRuntime:                       runtimellm.NoopRuntime{},
						DisablePersistentStartupRecovery: true,
						ProviderTriggerCatalog:           providerRegistry,
						ProcessWorkOwner:                 processWorkOwner,
						RuntimeInstanceID:                runtimeInstanceID,
						BundleSourceFact:                 fact,
					},
				})
				if err != nil {
					t.Fatalf("NewRuntime(%s): %v", fact.BundleHash, err)
				}
				t.Cleanup(func() { _ = rt.Shutdown() })
				contexts = append(contexts, serveRuntimeBundleContext{runtime: rt, bundleSourceFact: fact})
			}
			contexts[0].runtime.CloseAdmission()
			if err := startServeRuntimeContexts(context.Background(), contexts, nil); err == nil || !strings.Contains(err.Error(), "shutdown already started") {
				t.Fatalf("startServeRuntimeContexts error = %v, want shutdown admission failure", err)
			}

			registrar, ok := stores.runtimeStores().EventStore.(interface {
				RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
			})
			if !ok {
				t.Fatalf("selected %s event store lacks author activity catalog registry", backend.name)
			}
			for _, fact := range facts {
				scope := runtimeauthoractivity.BundleScope(runtimeInstanceID, fact.BundleHash)
				lease, err := registrar.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{{
					EventType: "rollback.probe", Disposition: runtimeauthoractivity.StoryDifferent,
				}})
				if err != nil {
					t.Fatalf("prepared catalog for %s remained leased after startup rollback: %v", fact.BundleHash, err)
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

type stubWorkspaceLifecycle struct {
	validateErr error
	prereqErr   error
	systemErr   error
}

func (s stubWorkspaceLifecycle) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return nil, nil
}
func (s stubWorkspaceLifecycle) ValidateSource(context.Context, semanticview.Source) error {
	return s.validateErr
}
func (s stubWorkspaceLifecycle) EnsurePrereqs(context.Context) error { return s.prereqErr }
func (s stubWorkspaceLifecycle) EnsureSystemWorkspaces(context.Context) error {
	return s.systemErr
}
func (stubWorkspaceLifecycle) EnsureEntityWorkspace(context.Context, string) error { return nil }
func (stubWorkspaceLifecycle) StopEntityWorkspace(context.Context, string) error   { return nil }

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
	platformPath := filepath.Join(t.TempDir(), "platform-spec.yaml")
	if err := os.WriteFile(platformPath, []byte("platform:\n  name: swarm\n  version: test\n"), 0o644); err != nil {
		t.Fatalf("write platform-spec.yaml: %v", err)
	}
	packagePath := filepath.Join(dir, "package.yaml")
	if err := os.WriteFile(packagePath, []byte("name: test\nversion: 1.0.0\nflows: []\n"), 0o644); err != nil {
		t.Fatalf("write package.yaml: %v", err)
	}
	bundle := testWorkflowValidationBundle()
	bundle.Paths = runtimecontracts.ResolveWorkflowContractPathsWithOverrides(filepath.Dir(dir), dir, platformPath)
	bundle.Paths.ProjectPackageFile = packagePath
	bundle.Semantics.Name = "test"
	bundle.Semantics.Version = "1.0.0"
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
	supervisor := newRuntimeProjectSupervisor("", "", nil, storeBundle{}, new(atomic.Bool), cliapp.WorkspaceMountSources{}, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil, nil, nil, "", nil, nil, nil)
	supervisor.processWorkOwner = worklifetime.NewProcess()
	catalog := testProviderTriggerCatalog(t)
	supervisor.providerTriggers = catalog
	supervisor.loadProviderCatalog = func() (*providertriggers.CatalogSnapshot, error) { return catalog, nil }
	supervisor.dev = true
	supervisor.loadWorkflow = func(RepoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if got := strings.TrimSpace(contractsRoot); got != strings.TrimSpace(projectRoot) {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", got, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error { return nil }
	supervisor.initStateStores = func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error) {
		return "store wiring ready", nil
	}
	supervisor.newWorkspaces = func(storeBundle, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		return lifecycle, cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
	}
	if createRuntime != nil {
		supervisor.createRuntime = createRuntime
	}
	return supervisor
}

func TestRuntimeProjectSupervisorCarriesProcessOwnerIntoDynamicRuntime(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	processOwner := worklifetime.NewProcess()
	var captured runtimepkg.RuntimeDeps
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		captured = deps
		return &runtimepkg.Runtime{Options: deps.Options}, nil
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

func TestRuntimeProjectSupervisorDerivesProcessOwnerFromInitialRuntime(t *testing.T) {
	processOwner := worklifetime.NewProcess()
	initial := &runtimepkg.Runtime{Options: runtimepkg.RuntimeOptions{ProcessWorkOwner: processOwner}}
	supervisor := newRuntimeProjectSupervisor(
		"", "", nil, storeBundle{}, new(atomic.Bool), cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{}, nil, nil, nil, "", nil, nil, initial,
	)
	if supervisor.processWorkOwner != processOwner {
		t.Fatalf("supervisor process owner = %p, want initial runtime owner %p", supervisor.processWorkOwner, processOwner)
	}
}

func TestRuntimeProjectSupervisorLoadProjectUsesResolvedWorkspaceMountSources(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	dataDir := t.TempDir()
	wantMountSources := cliapp.WorkspaceMountSources{
		DataSource:       dataDir,
		DataSourceSource: "--data",
	}

	var gotMountSources cliapp.WorkspaceMountSources
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.mountSources = wantMountSources
	supervisor.newWorkspaces = func(_ storeBundle, _ string, _ semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
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

func TestRuntimeProjectSupervisorReverifiesProviderCatalogForReplacement(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	bootCatalog := testProviderTriggerCatalog(t)
	candidateCatalog := testProviderTriggerCatalog(t)
	var gotCatalog *providertriggers.CatalogSnapshot
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotCatalog = deps.Options.ProviderTriggerCatalog
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.providerTriggers = bootCatalog
	supervisor.loadProviderCatalog = func() (*providertriggers.CatalogSnapshot, error) { return candidateCatalog, nil }
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if gotCatalog != candidateCatalog {
		t.Fatalf("replacement provider catalog = %p, want reverified candidate %p (boot=%p)", gotCatalog, candidateCatalog, bootCatalog)
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
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"worker": {ID: "worker"},
	}
	source := semanticview.Wrap(bundle)
	module := stubWorkflowModule{source: source}
	cfg := testWorkspaceBackendConfig(llmselection.BackendClaudeCLI)
	supervisor := newRuntimeProjectSupervisor(
		"", "", cfg, storeBundle{}, new(atomic.Bool), cliapp.WorkspaceMountSources{},
		cliapp.WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true},
		nil, nil, nil, "", nil, nil, nil,
	)
	supervisor.dev = true
	catalog := emptyProviderTriggerCatalog(t)
	supervisor.providerTriggers = catalog
	supervisor.loadProviderCatalog = func() (*providertriggers.CatalogSnapshot, error) { return catalog, nil }
	supervisor.loadWorkflow = func(_, contractsRoot, _ string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if strings.TrimSpace(contractsRoot) != strings.TrimSpace(projectRoot) {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", contractsRoot, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error { return nil }
	supervisor.initStateStores = func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error) {
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
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.ready = &ready
	supervisor.cfg = &config.Config{LLM: config.LLMConfig{Backend: "anthropic"}}
	supervisor.workspaceBackend = cliapp.WorkspaceBackendSelection{Source: "capability-derived"}
	supervisor.newWorkspaces = func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
		createdWorkspace = true
		decision, err := cliapp.DecideWorkspaceBackend(supervisor.workspaceBackend, supervisor.cfg, source)
		if err != nil {
			return nil, cliapp.WorkspaceBackendSelection{}, err
		}
		lifecycle, err := cliapp.ConfiguredWorkspaceLifecycleForBackend(stores.facade().workspaceDB(), supervisor.cfg, contractsRoot, source, mountSources, decision)
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
	supervisor.newWorkspaces = func(storeBundle, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
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
	oldRT := &runtimepkg.Runtime{}
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{}, nil
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

func TestRuntimeProjectSupervisorLoadProject_PassesBundleFingerprintToRuntime(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	expectedBundle := testBuilderSupervisorBundle(t)
	expectedIdentity, err := runtimecontracts.BootBundleIdentity(expectedBundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}
	expectedHash, err := runtimecontracts.BundleHash(expectedBundle)
	if err != nil {
		t.Fatalf("BundleHash: %v", err)
	}

	var gotFingerprint string
	var gotSourceFact runtimecorrelation.BundleSourceFact
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotFingerprint = deps.Options.BundleFingerprint
		gotSourceFact = deps.Options.BundleSourceFact
		return &runtimepkg.Runtime{}, nil
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
	if gotFingerprint != expectedIdentity.Fingerprint {
		t.Fatalf("BundleFingerprint = %q, want %q", gotFingerprint, expectedIdentity.Fingerprint)
	}
	if gotSourceFact.BundleHash != expectedHash || gotSourceFact.BundleSource != storerunlifecycle.BundleSourceEphemeral || gotSourceFact.BundleFingerprint != expectedIdentity.Fingerprint {
		t.Fatalf("BundleSourceFact = %#v, want hash=%q source=%q fingerprint=%q", gotSourceFact, expectedHash, storerunlifecycle.BundleSourceEphemeral, expectedIdentity.Fingerprint)
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
	bus, err := runtimebus.NewEventBusWithOptions(nil, runtimebus.EventBusOptions{WorkOwner: workOwner})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager := runtimemanager.NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
		WorkOwner:                      workOwner,
	})
	t.Cleanup(func() {
		_ = manager.Shutdown()
		_ = bus.ResetInMemoryState()
	})
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourceEphemeral}
	rt := &runtimepkg.Runtime{Bus: bus, Manager: manager}
	contexts, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: hash, BundleSourceFact: fact, Source: source, Runtime: rt, WorkOwner: workOwner,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	supervisor := &runtimeProjectSupervisor{
		currentSource: source, currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentBundleSourceFact: fact,
		currentRT: rt, runtimeContexts: contexts,
	}
	control := dashboardDynamicAgentControl{supervisor: supervisor}

	if _, err := control.Restart(context.Background(), runtimeagentcontrol.RestartRequest{AgentID: agent.id}); err == nil || !strings.Contains(err.Error(), "agent not running") {
		t.Fatalf("Restart err = %v, want agent not running", err)
	}
	if _, err := control.ReplayBacklog(context.Background(), runtimeagentcontrol.ReplayBacklogRequest{AgentID: agent.id}); err == nil || !strings.Contains(err.Error(), "agent not running") {
		t.Fatalf("ReplayBacklog err = %v, want agent not running", err)
	}
	if _, err := control.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{AgentID: agent.id, Directive: "run corpus"}); err == nil || !strings.Contains(err.Error(), "agent not running") {
		t.Fatalf("SendDirective err = %v, want agent not running", err)
	}
}
