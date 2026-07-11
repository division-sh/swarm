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
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
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
	providerTriggers    *providertriggers.Registry
	startRuntime        func(context.Context, *runtime.Runtime) error
	shutdownRuntime     func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	loadWorkflow        func(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource      func(context.Context, semanticview.Source) error
	initStateStores     func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces       func(storeBundle, string, semanticview.Source, workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error)
	createRuntime       func(context.Context, runtime.RuntimeDeps) (*runtime.Runtime, error)

	mu                              sync.RWMutex
	currentRoot                     string
	currentSource                   semanticview.Source
	currentBundle                   *runtimecontracts.WorkflowContractBundle
	currentRT                       *runtime.Runtime
	currentBundleSourceFact         runtimecorrelation.BundleSourceFact
	currentBundleIdentity           runtimecontracts.BundleIdentity
	runtimeContexts                 *runtime.RuntimeContextManager
	sourceReplacementDisabledReason string
}

func (s *runtimeProjectSupervisor) SetRuntimeContextManager(manager *runtime.RuntimeContextManager, fact runtimecorrelation.BundleSourceFact, identity runtimecontracts.BundleIdentity) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeContexts = manager
	s.currentBundleSourceFact = fact.Normalized()
	s.currentBundleIdentity = identity
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
	providerTriggers *providertriggers.Registry,
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
		providerTriggers:    providerTriggers,
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
			credentialStore, err := buildCredentialStore()
			if err != nil {
				return err
			}
			opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore)
			opts.ProviderTriggerRegistry = providerTriggers
			_, err = runtime.ValidateWorkflowContractSurface(ctx, source, opts)
			return err
		},
		initStateStores: func(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
			return initializeStateStores(ctx, stores, bundle, false)
		},
		newWorkspaces: func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources) (workspace.Lifecycle, workspaceBackendSelection, error) {
			decision, err := decideWorkspaceBackend(workspaceBackend, cfg, source)
			if err != nil {
				return nil, workspaceBackendSelection{}, err
			}
			lifecycle, err := configuredWorkspaceLifecycleForBackend(stores.facade().workspaceDB(), cfg, contractsRoot, source, mountSources, decision)
			if err != nil {
				return nil, decision, err
			}
			return lifecycle, decision, nil
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
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	s.mu.RUnlock()
	if manager != nil && bundleHash != "" {
		result := manager.DeactivateBundleHash(bundleHash, runtime.RuntimeContextCauseUnloaded)
		_ = s.detachCurrentRuntime()
		return builderpkg.ProjectStatus{}, result.ShutdownErr
	}
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
	candidateHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("derive project bundle hash: %w", err)
	}
	if err := s.rejectChangedStandingBundle(candidateHash); err != nil {
		return s.CurrentProject(), err
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
	managedCredentialStore, err := buildManagedCredentialStore()
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("configure managed credentials: %w", err)
	}
	workspaces, workspaceBackend, err := s.newWorkspaces(s.stores, resolvedRoot, source, s.mountSources)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if workspaces == nil {
		if !workspaceBackend.NoWorkspace {
			return builderpkg.ProjectStatus{}, fmt.Errorf("workspace lifecycle is not configured for backend %q; no lifecycle is only valid for canonical no-workspace decision", strings.TrimSpace(workspaceBackend.Backend))
		}
	} else {
		if err := workspaces.ValidateSource(ctx, source); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		if err := workspaces.EnsurePrereqs(ctx); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
	}

	newRT, err := s.createRuntime(ctx, runtime.RuntimeDeps{
		Config: s.cfg,
		Stores: s.stores.runtimeStores(),
		Options: runtime.RuntimeOptions{
			SelfCheck:               false,
			WorkflowModule:          module,
			WorkspaceLifecycle:      workspaces,
			BundleFingerprint:       bundleIdentity.Fingerprint,
			BundleSourceFact:        bundleSourceFact,
			Credentials:             s.credentials,
			ManagedCredentials:      managedCredentialStore,
			ProviderCredentials:     s.providerCredentials,
			ProviderTriggerRegistry: s.providerTriggers,
		},
	})
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	status, err := s.replaceCurrentRuntimeWithSource(ctx, resolvedRoot, source, bundle, bundleSourceFact, bundleIdentity, newRT)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	slog.Info("builder project loaded", "project_dir", filepath.Clean(resolvedRoot), "workflow", strings.TrimSpace(status.WorkflowName))
	return status, nil
}

func (s *runtimeProjectSupervisor) rejectChangedStandingBundle(candidateHash string) error {
	s.mu.RLock()
	manager := s.runtimeContexts
	currentHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	currentBundle := s.currentBundle
	currentRuntime := s.currentRT
	s.mu.RUnlock()
	if manager == nil || currentHash == "" || currentHash == strings.TrimSpace(candidateHash) {
		return nil
	}
	if currentRuntime == nil || !bundleHasStandingActivation(currentBundle) {
		return nil
	}
	return fmt.Errorf("standing service bundle change rejected before replacement: admitted bundle_hash=%s candidate bundle_hash=%s; serve the admitted bundle or perform an explicit future reset/migration", currentHash, strings.TrimSpace(candidateHash))
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
	fact := runtimecorrelation.BundleSourceFact{}
	identity := runtimecontracts.BundleIdentity{}
	if newRT != nil {
		fact = newRT.Options.BundleSourceFact.Normalized()
		identity.BundleHash = fact.BundleHash
	}
	return s.replaceCurrentRuntimeWithSource(ctx, resolvedRoot, source, bundle, fact, identity, newRT)
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntimeWithSource(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
) (builderpkg.ProjectStatus, error) {
	s.mu.RLock()
	manager := s.runtimeContexts
	oldHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	s.mu.RUnlock()
	newHash := strings.TrimSpace(fact.BundleHash)
	if manager != nil && oldHash != "" {
		if err := s.rejectChangedStandingBundle(newHash); err != nil {
			return s.CurrentProject(), err
		}
		plannedTargets, err := newRT.PlanStandingTargets()
		if err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		contextDef := runtime.BundleContext{
			BundleHash: newHash, BundleSourceFact: fact, BundleIdentity: identity, Source: source,
			ContractsRoot: resolvedRoot, PlatformSpecPath: s.platformSpecPath, Runtime: newRT, StandingTargets: plannedTargets,
		}
		if err := manager.ValidateReplacement(oldHash, contextDef); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		if err := s.startCurrentRuntime(ctx, newRT); err != nil {
			_ = s.shutdownCurrentRuntime(context.Background(), newRT)
			return builderpkg.ProjectStatus{}, err
		}
		targets, _, err := newRT.EnsureStandingTargets(ctx)
		if err != nil {
			_ = s.shutdownCurrentRuntime(context.Background(), newRT)
			return builderpkg.ProjectStatus{}, err
		}
		contextDef.StandingTargets = targets
		if err := manager.ReplaceBundleHash(oldHash, contextDef); err != nil {
			_ = s.shutdownCurrentRuntime(context.Background(), newRT)
			return builderpkg.ProjectStatus{}, err
		}
		oldRT := s.swapCurrentRuntime(resolvedRoot, source, bundle, fact, identity, newRT)
		if oldRT != nil {
			if err := s.shutdownCurrentRuntime(ctx, oldRT); err != nil {
				return builderpkg.ProjectStatus{}, err
			}
		}
		return s.CurrentProject(), nil
	}
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
	return s.attachCurrentRuntime(resolvedRoot, source, bundle, fact, identity, newRT), nil
}

func (s *runtimeProjectSupervisor) swapCurrentRuntime(resolvedRoot string, source semanticview.Source, bundle *runtimecontracts.WorkflowContractBundle, fact runtimecorrelation.BundleSourceFact, identity runtimecontracts.BundleIdentity, newRT *runtime.Runtime) *runtime.Runtime {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.currentRT
	s.currentRoot, s.currentSource, s.currentBundle, s.currentRT = strings.TrimSpace(resolvedRoot), source, bundle, newRT
	s.currentBundleSourceFact, s.currentBundleIdentity = fact.Normalized(), identity
	if s.ready != nil {
		s.ready.Store(true)
	}
	return old
}

func (s *runtimeProjectSupervisor) detachCurrentRuntime() *runtime.Runtime {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldRT := s.currentRT
	s.currentRoot = ""
	s.currentSource = nil
	s.currentBundle = nil
	s.currentRT = nil
	s.currentBundleSourceFact = runtimecorrelation.BundleSourceFact{}
	s.currentBundleIdentity = runtimecontracts.BundleIdentity{}
	if s.ready != nil {
		s.ready.Store(false)
	}
	return oldRT
}

func (s *runtimeProjectSupervisor) attachCurrentRuntime(
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
) builderpkg.ProjectStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentRoot = strings.TrimSpace(resolvedRoot)
	s.currentSource = source
	s.currentBundle = bundle
	s.currentRT = newRT
	s.currentBundleSourceFact = fact.Normalized()
	s.currentBundleIdentity = identity
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

type runtimeProcessInboundHandler struct {
	contexts *runtime.RuntimeContextManager
}

func (h runtimeProcessInboundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	alias, provider, ok := parseProcessWebhookPath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /webhooks/{alias}/{provider}", http.StatusBadRequest)
		return
	}
	lookup := h.contexts.LookupIngress(alias, providertriggers.NormalizeProviderName(provider))
	if !lookup.Found {
		if lookup.AliasFound {
			http.Error(w, fmt.Sprintf("ingress target %q does not declare provider %q; add that provider binding to the standing singleton flow", alias, provider), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
		return
	}
	if !lookup.Loaded() || lookup.Context.Runtime == nil || lookup.Context.Runtime.InboundGateway == nil {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q is unavailable: %s", alias, provider, lookup.Cause), http.StatusServiceUnavailable)
		return
	}
	target := lookup.Target
	lookup.Context.Runtime.InboundGateway.HandleResolvedWebhook(w, r, runtime.InboundTarget{
		BundleHash: target.BundleHash, FlowID: target.FlowID, RunID: target.RunID,
		FlowInstance: target.FlowInstance, EntityID: target.EntityID, EntitySlug: target.Alias,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret,
	}, lookup.Context.Source)
}

func parseProcessWebhookPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	alias := strings.TrimSpace(parts[1])
	provider := strings.TrimSpace(parts[2])
	return alias, provider, alias != "" && provider != ""
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
