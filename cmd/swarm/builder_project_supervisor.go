package main

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	builderpkg "swarm/internal/builder"
	"swarm/internal/config"
	"swarm/internal/runtime"
	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeingress "swarm/internal/runtime/ingress"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

type runtimeProjectSupervisor struct {
	repoRoot         string
	platformSpecPath string
	cfg              *config.Config
	stores           storeBundle
	ready            *atomic.Bool
	startRuntime     func(context.Context, *runtime.Runtime) error
	shutdownRuntime  func(context.Context, *runtime.Runtime) error
	loadWorkflow     func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource   func(context.Context, semanticview.Source) error
	initStateStores  func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces    func(storeBundle, string, string, semanticview.Source) workspace.Lifecycle
	createRuntime    func(context.Context, *config.Config, runtime.Stores, runtime.RuntimeOptions) (*runtime.Runtime, error)

	mu            sync.RWMutex
	currentRoot   string
	currentSource semanticview.Source
	currentBundle *runtimecontracts.WorkflowContractBundle
	currentRT     *runtime.Runtime
}

func newRuntimeProjectSupervisor(
	repoRoot string,
	platformSpecPath string,
	cfg *config.Config,
	stores storeBundle,
	ready *atomic.Bool,
	initialRoot string,
	initialBundle *runtimecontracts.WorkflowContractBundle,
	initialSource semanticview.Source,
	initialRT *runtime.Runtime,
) *runtimeProjectSupervisor {
	return &runtimeProjectSupervisor{
		repoRoot:         strings.TrimSpace(repoRoot),
		platformSpecPath: strings.TrimSpace(platformSpecPath),
		cfg:              cfg,
		stores:           stores,
		ready:            ready,
		startRuntime: func(ctx context.Context, rt *runtime.Runtime) error {
			return rt.Start(ctx)
		},
		shutdownRuntime: func(_ context.Context, rt *runtime.Runtime) error {
			return rt.Shutdown()
		},
		loadWorkflow: func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
			return newSwarmWorkflowModule(repoRoot, contractsRoot, platformSpecPath)
		},
		validateSource: func(ctx context.Context, source semanticview.Source) error {
			return verifyBundle(ctx, source)
		},
		initStateStores: initializeStateStores,
		newWorkspaces: func(stores storeBundle, repoRoot, contractsRoot string, source semanticview.Source) workspace.Lifecycle {
			return configuredWorkspaceLifecycle(stores.SQLDB, repoRoot, contractsRoot, source)
		},
		createRuntime: func(ctx context.Context, cfg *config.Config, stores runtime.Stores, opts runtime.RuntimeOptions) (*runtime.Runtime, error) {
			return runtime.NewRuntime(ctx, cfg, stores, opts)
		},
		currentRoot:   strings.TrimSpace(initialRoot),
		currentSource: initialSource,
		currentBundle: initialBundle,
		currentRT:     initialRT,
	}
}

func (s *runtimeProjectSupervisor) CurrentSource() semanticview.Source {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSource
}

func (s *runtimeProjectSupervisor) CurrentRuntime() *runtime.Runtime {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRT
}

func (s *runtimeProjectSupervisor) CurrentProject() builderpkg.ProjectStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.projectStatusLocked()
}

func (s *runtimeProjectSupervisor) OpenProject(ctx context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	return s.loadProject(ctx, projectDir)
}

func (s *runtimeProjectSupervisor) ReloadProject(ctx context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	projectDir = strings.TrimSpace(projectDir)
	if projectDir == "" {
		s.mu.RLock()
		projectDir = s.currentRoot
		s.mu.RUnlock()
	}
	if projectDir == "" {
		return builderpkg.ProjectStatus{}, fmt.Errorf("project is not loaded")
	}
	return s.loadProject(ctx, projectDir)
}

func (s *runtimeProjectSupervisor) CloseProject(context.Context) (builderpkg.ProjectStatus, error) {
	oldRT := s.detachCurrentRuntime()

	if oldRT != nil {
		if err := s.shutdownCurrentRuntime(context.Background(), oldRT); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
	}
	return builderpkg.ProjectStatus{}, nil
}

func (s *runtimeProjectSupervisor) loadProject(ctx context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	resolvedRoot, err := normalizeContractsRoot(resolvePath(s.repoRoot, projectDir))
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	module, bundle, err := s.loadWorkflow(s.repoRoot, resolvedRoot, s.platformSpecPath)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load project: %w", err)
	}
	source := semanticview.Wrap(bundle)
	if err := s.validateSource(ctx, source); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if _, err := s.initStateStores(ctx, s.stores, bundle); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	bundleIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("derive project bundle identity: %w", err)
	}
	workspaces := s.newWorkspaces(s.stores, s.repoRoot, resolvedRoot, source)
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsurePrereqs(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	newRT, err := s.createRuntime(ctx, s.cfg, s.stores.runtimeStores(), runtime.RuntimeOptions{
		SelfCheck:          false,
		WorkflowModule:     module,
		WorkspaceLifecycle: workspaces,
		BundleFingerprint:  bundleIdentity.Fingerprint,
	})
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	status, err := s.replaceCurrentRuntime(ctx, resolvedRoot, source, bundle, newRT)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	slog.Info("builder project loaded", "project_dir", filepath.Clean(resolvedRoot), "workflow", strings.TrimSpace(status.WorkflowName))
	return status, nil
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntime(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	newRT *runtime.Runtime,
) (builderpkg.ProjectStatus, error) {
	oldRT := s.detachCurrentRuntime()
	if oldRT != nil {
		if err := s.shutdownCurrentRuntime(ctx, oldRT); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
	}
	if err := s.startCurrentRuntime(ctx, newRT); err != nil {
		_ = s.shutdownCurrentRuntime(context.Background(), newRT)
		return builderpkg.ProjectStatus{}, err
	}
	return s.attachCurrentRuntime(resolvedRoot, source, bundle, newRT), nil
}

func (s *runtimeProjectSupervisor) detachCurrentRuntime() *runtime.Runtime {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldRT := s.currentRT
	s.currentRoot = ""
	s.currentSource = nil
	s.currentBundle = nil
	s.currentRT = nil
	if s.ready != nil {
		s.ready.Store(false)
	}
	return oldRT
}

func (s *runtimeProjectSupervisor) attachCurrentRuntime(
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	newRT *runtime.Runtime,
) builderpkg.ProjectStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentRoot = strings.TrimSpace(resolvedRoot)
	s.currentSource = source
	s.currentBundle = bundle
	s.currentRT = newRT
	if s.ready != nil {
		s.ready.Store(true)
	}
	return s.projectStatusLocked()
}

func (s *runtimeProjectSupervisor) startCurrentRuntime(ctx context.Context, rt *runtime.Runtime) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.startRuntime != nil {
		return s.startRuntime(ctx, rt)
	}
	return rt.Start(ctx)
}

func (s *runtimeProjectSupervisor) shutdownCurrentRuntime(ctx context.Context, rt *runtime.Runtime) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.shutdownRuntime != nil {
		return s.shutdownRuntime(ctx, rt)
	}
	return rt.Shutdown()
}

func (s *runtimeProjectSupervisor) projectStatusLocked() builderpkg.ProjectStatus {
	status := builderpkg.ProjectStatus{
		ProjectDir: strings.TrimSpace(s.currentRoot),
		Loaded:     s.currentSource != nil && s.currentRT != nil,
	}
	if s.currentSource != nil {
		status.WorkflowName = strings.TrimSpace(s.currentSource.WorkflowName())
		status.WorkflowVersion = strings.TrimSpace(s.currentSource.WorkflowVersion())
	}
	return status
}

type dashboardDynamicRuntimeControl struct {
	supervisor *runtimeProjectSupervisor
}

func (c dashboardDynamicRuntimeControl) PauseIngress() error {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.RuntimeIngress == nil {
		return fmt.Errorf("runtime ingress controller unavailable")
	}
	_, err := rt.RuntimeIngress.Pause(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "dashboard_action",
		ControlledBy: "dashboard",
	})
	return err
}

func (c dashboardDynamicRuntimeControl) ResumeIngress() error {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.RuntimeIngress == nil {
		return fmt.Errorf("runtime ingress controller unavailable")
	}
	_, err := rt.RuntimeIngress.Resume(context.Background(), runtimeingress.TransitionRequest{
		Reason:       "dashboard_action",
		ControlledBy: "dashboard",
	})
	return err
}

type dashboardDynamicAgentControl struct {
	supervisor *runtimeProjectSupervisor
}

func (c dashboardDynamicAgentControl) Restart(ctx context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.RestartResult{}, fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.Restart(ctx, req)
}

func (c dashboardDynamicAgentControl) ReplayBacklog(ctx context.Context, req runtimeagentcontrol.ReplayBacklogRequest) (runtimeagentcontrol.ReplayBacklogResult, error) {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.ReplayBacklogResult{}, fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.ReplayBacklog(ctx, req)
}

func (c dashboardDynamicAgentControl) SendDirective(ctx context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.SendDirectiveResult{}, fmt.Errorf("runtime manager unavailable")
	}
	if req.Source == "" {
		req.Source = runtimeagentcontrol.DirectiveSourceBuilderRuntime
	}
	return rt.Manager.SendDirective(ctx, req)
}
