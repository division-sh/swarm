package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
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
	supervisor := newRuntimeProjectSupervisor("", "", nil, storeBundle{}, new(atomic.Bool), workspaceMountSources{}, workspaceBackendSelection{Backend: defaultWorkspaceBackend, Source: "default"}, "", nil, nil, nil)
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
	supervisor.newWorkspaces = func(storeBundle, string, semanticview.Source, workspaceMountSources) (workspace.Lifecycle, error) {
		return lifecycle, nil
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
	supervisor.newWorkspaces = func(_ storeBundle, _ string, _ semanticview.Source, mountSources workspaceMountSources) (workspace.Lifecycle, error) {
		gotMountSources = mountSources
		return stubWorkspaceLifecycle{}, nil
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
