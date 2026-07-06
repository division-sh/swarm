package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	builderpkg "github.com/division-sh/swarm/internal/builder"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type runtimeProjectSupervisor struct {
	repoRoot            string
	platformSpecPath    string
	cfg                 *config.Config
	stores              storeBundle
	ready               *atomic.Bool
	dev                 bool
	mountSources        workspaceMountSources
	workspaceBackend    workspaceBackendSelection
	credentials         runtimecredentials.Store
	providerCredentials runtimecredentials.Store
	startRuntime        func(context.Context, *runtime.Runtime) error
	shutdownRuntime     func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	loadWorkflow        func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource      func(context.Context, semanticview.Source) error
	initStateStores     func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces       func(storeBundle, string, semanticview.Source, workspaceMountSources) (workspace.Lifecycle, error)
	createRuntime       func(context.Context, runtime.RuntimeDeps) (*runtime.Runtime, error)

	mu                              sync.RWMutex
	currentRoot                     string
	currentSource                   semanticview.Source
	currentBundle                   *runtimecontracts.WorkflowContractBundle
	currentRT                       *runtime.Runtime
	sourceReplacementDisabledReason string
}

func newRuntimeProjectSupervisor(
	repoRoot string,
	platformSpecPath string,
	cfg *config.Config,
	stores storeBundle,
	ready *atomic.Bool,
	mountSources workspaceMountSources,
	workspaceBackend workspaceBackendSelection,
	credentials runtimecredentials.Store,
	providerCredentials runtimecredentials.Store,
	initialRoot string,
	initialBundle *runtimecontracts.WorkflowContractBundle,
	initialSource semanticview.Source,
	initialRT *runtime.Runtime,
	devMode ...bool,
) *runtimeProjectSupervisor {
	dev := false
	if len(devMode) > 0 {
		dev = devMode[0]
	}
	return &runtimeProjectSupervisor{
		repoRoot:            strings.TrimSpace(repoRoot),
		platformSpecPath:    strings.TrimSpace(platformSpecPath),
		cfg:                 cfg,
		stores:              stores,
		ready:               ready,
		dev:                 dev,
		mountSources:        mountSources,
		workspaceBackend:    workspaceBackend,
		credentials:         credentials,
		providerCredentials: providerCredentials,
		startRuntime: func(ctx context.Context, rt *runtime.Runtime) error {
			return rt.Start(ctx)
		},
		shutdownRuntime: func(_ context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
			return rt.ShutdownWithOptions(opts)
		},
		loadWorkflow: func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
			return newSwarmWorkflowModule(repoRoot, contractsRoot, platformSpecPath)
		},
		validateSource: func(ctx context.Context, source semanticview.Source) error {
			return verifyBundle(ctx, source)
		},
		initStateStores: func(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
			return initializeStateStores(ctx, stores, bundle, false)
		},
		newWorkspaces: func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources) (workspace.Lifecycle, error) {
			decision, err := decideWorkspaceBackend(workspaceBackend, cfg, source)
			if err != nil {
				return nil, err
			}
			return configuredWorkspaceLifecycleForBackend(stores.facade().workspaceDB(), contractsRoot, source, mountSources, decision)
		},
		createRuntime: func(ctx context.Context, deps runtime.RuntimeDeps) (*runtime.Runtime, error) {
			return runtime.NewRuntime(ctx, deps)
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

func (s *runtimeProjectSupervisor) DisableSourceReplacement(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sourceReplacementDisabledReason = strings.TrimSpace(reason)
}

func (s *runtimeProjectSupervisor) CloseProject(ctx context.Context) (builderpkg.ProjectStatus, error) {
	return s.CloseProjectWithShutdownOptions(ctx, runtime.DefaultShutdownOptions())
}

func (s *runtimeProjectSupervisor) CloseProjectWithShutdownOptions(ctx context.Context, opts runtime.ShutdownOptions) (builderpkg.ProjectStatus, error) {
	oldRT := s.detachCurrentRuntime()

	if oldRT != nil {
		if err := s.shutdownCurrentRuntimeWithOptions(ctx, oldRT, opts); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
	}
	return builderpkg.ProjectStatus{}, nil
}

func (s *runtimeProjectSupervisor) loadProject(ctx context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	if reason := s.sourceReplacementDisabled(); reason != "" {
		return s.CurrentProject(), fmt.Errorf("project source replacement is disabled: %s", reason)
	}
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
	bundleSourceFact, err := prepareServeBundleSource(ctx, s.stores, bundle, bundleIdentity.Fingerprint, s.dev)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("prepare project bundle source: %w", err)
	}
	workspaces, err := s.newWorkspaces(s.stores, resolvedRoot, source, s.mountSources)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsurePrereqs(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	newRT, err := s.createRuntime(ctx, runtime.RuntimeDeps{
		Config: s.cfg,
		Stores: s.stores.runtimeStores(),
		Options: runtime.RuntimeOptions{
			SelfCheck:           false,
			WorkflowModule:      module,
			WorkspaceLifecycle:  workspaces,
			BundleFingerprint:   bundleIdentity.Fingerprint,
			BundleSourceFact:    bundleSourceFact,
			Credentials:         s.credentials,
			ProviderCredentials: s.providerCredentials,
		},
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

func (s *runtimeProjectSupervisor) sourceReplacementDisabled() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.TrimSpace(s.sourceReplacementDisabledReason)
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
	return s.shutdownCurrentRuntimeWithOptions(ctx, rt, runtime.DefaultShutdownOptions())
}

func (s *runtimeProjectSupervisor) shutdownCurrentRuntimeWithOptions(ctx context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.shutdownRuntime != nil {
		return s.shutdownRuntime(ctx, rt, opts)
	}
	return rt.ShutdownWithOptions(opts)
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

type runtimeProjectInboundHandler struct {
	supervisor *runtimeProjectSupervisor
}

func (h runtimeProjectInboundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.supervisor == nil {
		http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
		return
	}
	rt := h.supervisor.CurrentRuntime()
	if rt == nil || rt.InboundGateway == nil {
		http.Error(w, "runtime ingress unavailable", http.StatusServiceUnavailable)
		return
	}
	rt.InboundGateway.Handler().ServeHTTP(w, r)
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
