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
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

type runtimeProjectSupervisor struct {
	repoRoot         string
	platformSpecPath string
	cfg              *config.Config
	stores           storeBundle
	ready            *atomic.Bool
	startRuntime     func(context.Context, *runtime.Runtime) error
	shutdownRuntime  func(context.Context, *runtime.Runtime) error

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

	module, bundle, err := newSwarmWorkflowModule(s.repoRoot, resolvedRoot, s.platformSpecPath)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load project: %w", err)
	}
	if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{})
	if report.HasErrors() {
		return builderpkg.ProjectStatus{}, fmt.Errorf("%s", report.Errors()[0].Message)
	}
	if _, err := initializeStateStores(ctx, s.stores, bundle); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	workspaces := configuredWorkspaceLifecycle(s.stores.SQLDB, s.repoRoot, resolvedRoot, source)
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsurePrereqs(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	newRT, err := runtime.NewRuntime(ctx, s.cfg, s.stores.runtimeStores(), runtime.RuntimeOptions{
		SelfCheck:          false,
		WorkflowModule:     module,
		WorkspaceLifecycle: workspaces,
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

func (c dashboardDynamicRuntimeControl) PauseIngress()  { runtimebus.PauseRuntimeIngress() }
func (c dashboardDynamicRuntimeControl) ResumeIngress() { runtimebus.ResumeRuntimeIngress() }

func (c dashboardDynamicRuntimeControl) ResetState() error {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return fmt.Errorf("runtime manager unavailable")
	}
	if resetter, ok := any(rt.Manager).(interface{ ResetRuntimeStateWithSource(string) error }); ok {
		return resetter.ResetRuntimeStateWithSource("builder_api")
	}
	return rt.Manager.ResetRuntimeState()
}

type dashboardDynamicAgentControl struct {
	supervisor *runtimeProjectSupervisor
}

func (c dashboardDynamicAgentControl) RestartAgent(agentID string) error {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.RestartAgent(agentID)
}

func (c dashboardDynamicAgentControl) ReplayAgentBacklog(ctx context.Context, agentID string) error {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.ReplayAgentBacklog(ctx, agentID)
}

func (c dashboardDynamicAgentControl) ChatWithAgent(ctx context.Context, agentID, directive string, killPrevious bool) (string, error) {
	rt := c.supervisor.CurrentRuntime()
	if rt == nil || rt.Manager == nil {
		return "", fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.ChatWithAgent(ctx, agentID, directive, killPrevious)
}
