package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

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

func TestRuntimeProcessInboundHandlerSelectsExactLoadedContext(t *testing.T) {
	contractsRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	_, bundle, err := newSwarmWorkflowModule(repoRoot(), contractsRoot, resolvePath(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load standing fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	registry := testProviderTriggerRegistry(t)
	makeContext := func(hash, alias, runID, entityID string) (runtimepkg.BundleContext, *processIngressProofStore, *processIngressEventStore) {
		persistence := &processIngressProofStore{}
		eventsStore := &processIngressEventStore{}
		bus, err := runtimebus.NewEventBus(eventsStore)
		if err != nil {
			t.Fatalf("NewEventBus(%s): %v", alias, err)
		}
		gateway := runtimepkg.NewInboundGatewayWithProviderRegistry(bus, nil, nil, registry, persistence)
		gateway.SetCredentialStore(processIngressCredentialStore{"webhook_signing.telegram": "telegram-secret"})
		return runtimepkg.BundleContext{
			BundleHash: hash, Source: source, Runtime: &runtimepkg.Runtime{Bus: bus, InboundGateway: gateway},
			StandingTargets: []runtimepkg.StandingTarget{{
				BundleHash: hash, FlowID: "telegram-chat", Alias: alias, Provider: "telegram",
				RunID: runID, FlowInstance: "telegram-chat/@standing/" + strings.TrimPrefix(alias, "chat-"),
				EntityID: entityID, SigningSecret: "webhook_signing.telegram",
			}},
		}, persistence, eventsStore
	}
	hashA := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	hashB := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	contextA, persistenceA, eventsA := makeContext(hashA, "chat-a", "41000000-0000-0000-0000-000000000001", "41000000-0000-0000-0000-000000000002")
	contextB, persistenceB, eventsB := makeContext(hashB, "chat-b", "42000000-0000-0000-0000-000000000001", "42000000-0000-0000-0000-000000000002")
	manager, err := runtimepkg.NewRuntimeContextManager(nil, contextA, contextB)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat-b/telegram", strings.NewReader(`{"update_id":99,"message":{"chat":{"id":42},"text":"hello"}}`))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	rec := httptest.NewRecorder()
	runtimeProcessInboundHandler{contexts: manager}.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("selected-context response = %d %q, want 202", rec.Code, rec.Body.String())
	}
	if persistenceA.recorded || len(eventsA.events) != 0 {
		t.Fatalf("non-selected context A was touched: marker=%v events=%d", persistenceA.recorded, len(eventsA.events))
	}
	if !persistenceB.recorded || len(eventsB.events) != 1 {
		t.Fatalf("selected context B marker/events = %v/%d, want true/1", persistenceB.recorded, len(eventsB.events))
	}
	if got := eventsB.events[0].RunID(); got != contextB.StandingTargets[0].RunID {
		t.Fatalf("selected event run_id = %q, want %q", got, contextB.StandingTargets[0].RunID)
	}
}

func TestRuntimeProjectSupervisorRejectsChangedStandingBundleWithoutMutation(t *testing.T) {
	standingBundle := &runtimecontracts.WorkflowContractBundle{PackageTree: []runtimecontracts.LoadedProjectPackage{{
		Manifest: runtimecontracts.ProjectPackageDocument{Flows: []runtimecontracts.ProjectFlowRef{{
			ID: "service", Mode: runtimecontracts.FlowModeSingleton, Activation: runtimecontracts.ProjectFlowActivationStanding,
		}}},
	}}}
	source := semanticview.Wrap(standingBundle)
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldRT := &runtimepkg.Runtime{Bus: bus}
	oldHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	newHash := "bundle-v1:sha256:" + strings.Repeat("b", 64)
	oldTarget := runtimepkg.StandingTarget{
		BundleHash: oldHash, FlowID: "service", Alias: "chat", Provider: "telegram",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "service/@standing/a",
		EntityID: "41000000-0000-0000-0000-000000000002", SigningSecret: "webhook_signing.telegram",
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: oldHash, Source: source, Runtime: oldRT, StandingTargets: []runtimepkg.StandingTarget{oldTarget},
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: standingBundle, currentRT: oldRT,
		currentBundleSourceFact: runtimecorrelation.BundleSourceFact{BundleHash: oldHash, BundleSource: storerunlifecycle.BundleSourcePersisted},
		runtimeContexts:         manager,
	}
	for attempt := 0; attempt < 2; attempt++ {
		err := supervisor.rejectChangedStandingBundle(newHash)
		if err == nil || !strings.Contains(err.Error(), oldHash) || !strings.Contains(err.Error(), newHash) || !strings.Contains(err.Error(), "explicit future reset/migration") {
			t.Fatalf("attempt %d changed-bundle error = %v", attempt+1, err)
		}
	}
	if !ready.Load() || supervisor.CurrentRuntime() != oldRT || supervisor.CurrentProject().ProjectDir != "/tmp/current" {
		t.Fatalf("changed-bundle rejection mutated supervisor: ready=%v runtime=%p status=%#v", ready.Load(), supervisor.CurrentRuntime(), supervisor.CurrentProject())
	}
	lookup := manager.LookupIngress("chat", "telegram")
	if !lookup.Loaded() || lookup.Context.Runtime != oldRT || lookup.Target.RunID != oldTarget.RunID {
		t.Fatalf("old context after changed-bundle rejection = %#v", lookup)
	}
}

func TestRuntimeProjectSupervisorRejectsChangedStandingBundleWithoutIngress(t *testing.T) {
	standingBundle := &runtimecontracts.WorkflowContractBundle{PackageTree: []runtimecontracts.LoadedProjectPackage{{
		Manifest: runtimecontracts.ProjectPackageDocument{Flows: []runtimecontracts.ProjectFlowRef{{
			ID: "scheduler", Mode: runtimecontracts.FlowModeSingleton, Activation: runtimecontracts.ProjectFlowActivationStanding,
		}}},
	}}}
	source := semanticview.Wrap(standingBundle)
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	oldRT := &runtimepkg.Runtime{Bus: bus}
	oldHash := "bundle-v1:sha256:" + strings.Repeat("d", 64)
	newHash := "bundle-v1:sha256:" + strings.Repeat("e", 64)
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: oldHash, Source: source, Runtime: oldRT,
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: standingBundle, currentRT: oldRT,
		currentBundleSourceFact: runtimecorrelation.BundleSourceFact{BundleHash: oldHash, BundleSource: storerunlifecycle.BundleSourcePersisted},
		runtimeContexts:         manager,
	}
	if err := supervisor.rejectChangedStandingBundle(newHash); err == nil || !strings.Contains(err.Error(), "explicit future reset/migration") {
		t.Fatalf("changed standing bundle without ingress error = %v", err)
	}
	if !ready.Load() || supervisor.CurrentRuntime() != oldRT || supervisor.CurrentProject().ProjectDir != "/tmp/current" {
		t.Fatalf("changed standing bundle without ingress mutated supervisor: ready=%v runtime=%p status=%#v", ready.Load(), supervisor.CurrentRuntime(), supervisor.CurrentProject())
	}
}

func TestRuntimeProjectSupervisorFailedSameHashReplacementPreservesOldContext(t *testing.T) {
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
	hash := "bundle-v1:sha256:" + strings.Repeat("c", 64)
	target := runtimepkg.StandingTarget{
		BundleHash: hash, FlowID: "service", Alias: "chat", Provider: "telegram",
		RunID: "43000000-0000-0000-0000-000000000001", FlowInstance: "service/@standing/c",
		EntityID: "43000000-0000-0000-0000-000000000002", SigningSecret: "webhook_signing.telegram",
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
		BundleHash: hash, Source: source, Runtime: oldRT, StandingTargets: []runtimepkg.StandingTarget{target},
	})
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	var ready atomic.Bool
	ready.Store(true)
	fact := runtimecorrelation.BundleSourceFact{BundleHash: hash, BundleSource: storerunlifecycle.BundleSourcePersisted}
	supervisor := &runtimeProjectSupervisor{
		ready: &ready, currentRoot: "/tmp/current", currentSource: source,
		currentBundle: &runtimecontracts.WorkflowContractBundle{}, currentRT: oldRT,
		currentBundleSourceFact: fact, runtimeContexts: manager,
	}
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return errors.New("candidate start failed") }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }
	if _, err := supervisor.replaceCurrentRuntimeWithSource(context.Background(), "/tmp/candidate", source, &runtimecontracts.WorkflowContractBundle{}, fact, runtimecontracts.BundleIdentity{BundleHash: hash}, newRT); err == nil || !strings.Contains(err.Error(), "candidate start failed") {
		t.Fatalf("same-hash replacement error = %v", err)
	}
	lookup := manager.LookupIngress("chat", "telegram")
	if !ready.Load() || supervisor.CurrentRuntime() != oldRT || !lookup.Loaded() || lookup.Context.Runtime != oldRT {
		t.Fatalf("failed same-hash replacement mutated old authority: ready=%v runtime=%p lookup=%#v", ready.Load(), supervisor.CurrentRuntime(), lookup)
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

type processIngressProofStore struct{ recorded bool }

func (s *processIngressProofStore) RecordInboundEvent(context.Context, string, string, string) (bool, error) {
	s.recorded = true
	return true, nil
}
func (*processIngressProofStore) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}
func (*processIngressProofStore) DeleteInboundEvent(context.Context, string, string, string) error {
	return nil
}

type processIngressEventStore struct{ events []events.Event }

func (s *processIngressEventStore) AppendEvent(_ context.Context, event events.Event) error {
	s.events = append(s.events, event)
	return nil
}
func (*processIngressEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
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
	supervisor := newRuntimeProjectSupervisor("", "", nil, storeBundle{}, new(atomic.Bool), workspaceMountSources{}, workspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil, nil, nil, "", nil, nil, nil)
	supervisor.dev = true
	supervisor.loadWorkflow = func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
		if got := strings.TrimSpace(contractsRoot); got != strings.TrimSpace(projectRoot) {
			return nil, nil, fmt.Errorf("contracts root = %q, want %q", got, projectRoot)
		}
		return module, bundle, nil
	}
	supervisor.validateSource = func(context.Context, semanticview.Source) error { return nil }
	supervisor.initStateStores = func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error) {
		return "store wiring ready", nil
	}
	supervisor.newWorkspaces = func(storeBundle, string, semanticview.Source, workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error) {
		return lifecycle, workspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
	}
	if createRuntime != nil {
		supervisor.createRuntime = createRuntime
	}
	return supervisor
}

func TestRuntimeProjectSupervisorLoadProjectUsesResolvedWorkspaceMountSources(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	dataDir := t.TempDir()
	wantMountSources := workspaceMountSources{
		DataSource:       dataDir,
		DataSourceSource: "--data",
	}

	var gotMountSources workspaceMountSources
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(context.Context, runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.mountSources = wantMountSources
	supervisor.newWorkspaces = func(_ storeBundle, _ string, _ semanticview.Source, mountSources workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error) {
		gotMountSources = mountSources
		return stubWorkspaceLifecycle{}, workspaceBackendSelection{Backend: workspace.BackendDocker, Source: "test"}, nil
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

func TestRuntimeProjectSupervisorCarriesBootAdmittedProviderRegistryIntoReplacement(t *testing.T) {
	projectRoot := writeProjectRoot(t)
	wantRegistry := testProviderTriggerRegistry(t)
	var gotRegistry *providertriggers.Registry
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(_ context.Context, deps runtimepkg.RuntimeDeps) (*runtimepkg.Runtime, error) {
		gotRegistry = deps.Options.ProviderTriggerRegistry
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.providerTriggers = wantRegistry
	supervisor.startRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error { return nil }

	if _, err := supervisor.OpenProject(context.Background(), projectRoot); err != nil {
		t.Fatalf("OpenProject: %v", err)
	}
	if gotRegistry != wantRegistry {
		t.Fatalf("replacement provider registry = %p, want boot-admitted %p", gotRegistry, wantRegistry)
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
			name:      "ensure prereqs",
			lifecycle: stubWorkspaceLifecycle{prereqErr: errors.New("docker unavailable")},
			wantErr:   "docker unavailable",
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
	supervisor.workspaceBackend = workspaceBackendSelection{Source: "capability-derived"}
	supervisor.newWorkspaces = func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error) {
		createdWorkspace = true
		decision, err := decideWorkspaceBackend(supervisor.workspaceBackend, supervisor.cfg, source)
		if err != nil {
			return nil, workspaceBackendSelection{}, err
		}
		lifecycle, err := configuredWorkspaceLifecycleForBackend(stores.facade().workspaceDB(), supervisor.cfg, contractsRoot, source, mountSources, decision)
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
	supervisor.newWorkspaces = func(storeBundle, string, semanticview.Source, workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error) {
		return nil, workspaceBackendSelection{Backend: workspace.BackendHost, Source: "test"}, nil
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
	manager := runtimemanager.NewAgentManagerWithOptions(nil, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		return agent, nil
	}, runtimemanager.AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	supervisor := &runtimeProjectSupervisor{
		currentRT: &runtimepkg.Runtime{Manager: manager},
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
