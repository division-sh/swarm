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

	"swarm/internal/config"
	"swarm/internal/events"
	runtimepkg "swarm/internal/runtime"
	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
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
	supervisor.shutdownRuntime = func(_ context.Context, rt *runtimepkg.Runtime) error {
		shutdownCalled = true
		if rt != oldRT {
			t.Fatalf("shutdown runtime = %p, want old runtime %p", rt, oldRT)
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

func newSupervisorForLoadProjectFailureTest(
	t *testing.T,
	projectRoot string,
	lifecycle workspace.Lifecycle,
	createRuntime func(context.Context, *config.Config, runtimepkg.Stores, runtimepkg.RuntimeOptions) (*runtimepkg.Runtime, error),
) *runtimeProjectSupervisor {
	t.Helper()
	bundle := testWorkflowValidationBundle()
	source := semanticview.Wrap(bundle)
	module := stubWorkflowModule{source: source}
	supervisor := newRuntimeProjectSupervisor("", "", nil, storeBundle{}, new(atomic.Bool), "", nil, nil, nil)
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
	supervisor.newWorkspaces = func(storeBundle, string, string, semanticview.Source) workspace.Lifecycle {
		return lifecycle
	}
	if createRuntime != nil {
		supervisor.createRuntime = createRuntime
	}
	return supervisor
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
			supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, tc.lifecycle, func(context.Context, *config.Config, runtimepkg.Stores, runtimepkg.RuntimeOptions) (*runtimepkg.Runtime, error) {
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
	supervisor := newSupervisorForLoadProjectFailureTest(t, projectRoot, stubWorkspaceLifecycle{}, func(context.Context, *config.Config, runtimepkg.Stores, runtimepkg.RuntimeOptions) (*runtimepkg.Runtime, error) {
		return &runtimepkg.Runtime{}, nil
	})
	supervisor.ready = &ready
	supervisor.currentRoot = "/tmp/old"
	supervisor.currentBundle = &runtimecontracts.WorkflowContractBundle{}
	supervisor.currentSource = semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	supervisor.currentRT = oldRT
	ready.Store(true)
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime) error { return nil }
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

type builderControlTestAgent struct{ id string }

func (a builderControlTestAgent) ID() string                      { return a.id }
func (builderControlTestAgent) Type() string                      { return "stub" }
func (builderControlTestAgent) Subscriptions() []events.EventType { return nil }
func (builderControlTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (builderControlTestAgent) BoardStep(context.Context, string) (string, error) { return "ok", nil }

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
