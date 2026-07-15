package serveapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	builderpkg "github.com/division-sh/swarm/internal/builder"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
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
	RepoRoot            string
	platformSpecPath    string
	cfg                 *config.Config
	stores              storeBundle
	ready               *atomic.Bool
	dev                 bool
	mountSources        cliapp.WorkspaceMountSources
	workspaceBackend    cliapp.WorkspaceBackendSelection
	credentials         runtimecredentials.Store
	providerCredentials runtimecredentials.Store
	providerTriggers    *providertriggers.CatalogSnapshot
	loadProviderCatalog func() (*providertriggers.CatalogSnapshot, error)
	loadChannelPacks    func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) (cliapp.ChannelPackLoad, error)
	channelPlans        []packs.SatisfactionPlan
	channelBindings     []packs.OutboundBindingPlan
	startRuntime        func(context.Context, *runtime.Runtime) error
	quiesceRuntime      func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	shutdownRuntime     func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	loadWorkflow        func(RepoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource      func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error
	initStateStores     func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces       func(storeBundle, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error)
	createRuntime       func(context.Context, runtime.RuntimeDeps) (*runtime.Runtime, error)
	cloneRuntime        func(context.Context, *runtime.Runtime) (*runtime.Runtime, error)
	replacementShutdown runtime.ShutdownOptions
	runtimeLifetime     context.Context
	runtimeInstanceID   string
	operationMu         sync.Mutex

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

func (s *runtimeProjectSupervisor) SetChannelPackLoader(loader func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) (cliapp.ChannelPackLoad, error)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadChannelPacks = loader
}

func newRuntimeProjectSupervisor(
	RepoRoot string,
	platformSpecPath string,
	cfg *config.Config,
	stores storeBundle,
	ready *atomic.Bool,
	mountSources cliapp.WorkspaceMountSources,
	workspaceBackend cliapp.WorkspaceBackendSelection,
	credentials runtimecredentials.Store,
	providerCredentials runtimecredentials.Store,
	providerTriggers *providertriggers.CatalogSnapshot,
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
	supervisor := &runtimeProjectSupervisor{
		RepoRoot:            strings.TrimSpace(RepoRoot),
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
		runtimeInstanceID: func() string {
			if initialRT == nil {
				return ""
			}
			return strings.TrimSpace(initialRT.Options.RuntimeInstanceID)
		}(),
		startRuntime: func(ctx context.Context, rt *runtime.Runtime) error {
			return rt.Start(ctx)
		},
		quiesceRuntime: func(_ context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
			return rt.QuiesceForReplacement(opts)
		},
		shutdownRuntime: func(_ context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
			return rt.ShutdownWithOptions(opts)
		},
		loadWorkflow: func(RepoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
			return cliapp.NewSwarmWorkflowModule(RepoRoot, contractsRoot, platformSpecPath)
		},
		validateSource: func(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) error {
			credentialStore, err := cliapp.BuildCredentialStore()
			if err != nil {
				return err
			}
			opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore)
			opts.ProviderTriggerCatalog = catalog
			_, err = runtime.ValidateWorkflowContractSurface(ctx, source, opts)
			return err
		},
		initStateStores: func(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
			return initializeStateStores(ctx, stores, bundle)
		},
		newWorkspaces: func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
			decision, err := cliapp.DecideWorkspaceBackend(workspaceBackend, cfg, source)
			if err != nil {
				return nil, cliapp.WorkspaceBackendSelection{}, err
			}
			lifecycle, err := cliapp.ConfiguredWorkspaceLifecycleForBackend(stores.facade().workspaceDB(), cfg, contractsRoot, source, mountSources, decision)
			if err != nil {
				return nil, decision, err
			}
			return lifecycle, decision, nil
		},
		createRuntime: func(ctx context.Context, deps runtime.RuntimeDeps) (*runtime.Runtime, error) {
			return runtime.NewRuntime(ctx, deps)
		},
		cloneRuntime: func(ctx context.Context, predecessor *runtime.Runtime) (*runtime.Runtime, error) {
			if predecessor == nil {
				return nil, fmt.Errorf("predecessor runtime is required")
			}
			return runtime.NewRuntime(ctx, runtime.RuntimeDeps{Config: predecessor.Config, Stores: predecessor.Stores, Options: predecessor.Options})
		},
		currentRoot:   strings.TrimSpace(initialRoot),
		currentSource: initialSource,
		currentBundle: initialBundle,
		currentRT:     initialRT,
	}
	if initialRT != nil {
		supervisor.channelPlans = append([]packs.SatisfactionPlan(nil), initialRT.Options.ChannelPlans...)
		supervisor.channelBindings = append([]packs.OutboundBindingPlan(nil), initialRT.Options.ChannelOutboundBindings...)
	}
	return supervisor
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
	status, err := s.loadProject(ctx, projectDir)
	if err != nil {
		return s.CurrentProject(), fmt.Errorf("reload rejected: %w; previous runtime contexts and provider-trigger catalog generation remain loaded and serving", err)
	}
	return status, nil
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
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	s.mu.RUnlock()
	if manager != nil && bundleHash != "" {
		result := manager.DeactivateBundleHashWithOptions(bundleHash, runtime.RuntimeContextCauseUnloaded, opts)
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
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if reason := s.sourceReplacementDisabled(); reason != "" {
		return s.CurrentProject(), fmt.Errorf("project source replacement is disabled: %s", reason)
	}
	resolvedRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolvePath(s.RepoRoot, projectDir))
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	module, bundle, err := s.loadWorkflow(s.RepoRoot, resolvedRoot, s.platformSpecPath)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load project: %w", err)
	}
	source := semanticview.Wrap(bundle)
	candidateCatalog := s.providerTriggers
	if s.loadProviderCatalog != nil {
		candidateCatalog, err = s.loadProviderCatalog()
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate provider-trigger catalog: %w", err)
		}
	}
	if candidateCatalog == nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("candidate provider-trigger catalog is required")
	}
	candidateChannelPlans := append([]packs.SatisfactionPlan(nil), s.channelPlans...)
	candidateChannelBindings := append([]packs.OutboundBindingPlan(nil), s.channelBindings...)
	if s.loadChannelPacks != nil {
		channelLoad, loadErr := s.loadChannelPacks(ctx, source, candidateCatalog)
		if loadErr != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate channel packs: %w", loadErr)
		}
		candidateChannelPlans = channelLoad.Plans
		candidateChannelBindings = channelLoad.Bindings
	}
	if err := s.validateSource(ctx, source, candidateCatalog); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	admissionCandidate, err := s.compileProcessAdmissionCandidate(ctx, candidateCatalog)
	if err != nil {
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
	managedCredentialStore, err := cliapp.BuildManagedCredentialStore()
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
			RuntimeInstanceID:       s.runtimeInstanceID,
			Credentials:             s.credentials,
			ManagedCredentials:      managedCredentialStore,
			ProviderCredentials:     s.providerCredentials,
			ProviderTriggerCatalog:  candidateCatalog,
			ChannelPlans:            candidateChannelPlans,
			ChannelOutboundBindings: candidateChannelBindings,
		},
	})
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	admissionCandidate.catalog = candidateCatalog
	status, err := s.replaceCurrentRuntimeWithSourceAndAdmission(ctx, resolvedRoot, source, bundle, bundleSourceFact, bundleIdentity, newRT, &admissionCandidate)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	s.mu.Lock()
	s.channelPlans = append([]packs.SatisfactionPlan(nil), candidateChannelPlans...)
	s.channelBindings = append([]packs.OutboundBindingPlan(nil), candidateChannelBindings...)
	s.mu.Unlock()
	slog.Info("builder project loaded", "project_dir", filepath.Clean(resolvedRoot), "workflow", strings.TrimSpace(status.WorkflowName))
	return status, nil
}

type processAdmissionCandidate struct {
	catalog          *providertriggers.CatalogSnapshot
	state            runtime.ProcessAdmissionState
	survivingTargets map[string][]runtime.StandingTarget
}

func (s *runtimeProjectSupervisor) compileProcessAdmissionCandidate(ctx context.Context, catalog *providertriggers.CatalogSnapshot) (processAdmissionCandidate, error) {
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		return processAdmissionCandidate{}, err
	}
	candidate := processAdmissionCandidate{
		state:            runtime.ProcessAdmissionState{GenerationID: catalog.GenerationID(), InstalledSubjects: installed},
		survivingTargets: map[string][]runtime.StandingTarget{},
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	currentHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	s.mu.RUnlock()
	if manager == nil {
		return candidate, nil
	}
	for _, loaded := range manager.LoadedContexts() {
		if loaded.BundleHash == currentHash {
			continue
		}
		if err := s.validateSource(ctx, loaded.Source, catalog); err != nil {
			return processAdmissionCandidate{}, fmt.Errorf("candidate provider-trigger catalog rejected loaded runtime context %s: %w", loaded.BundleHash, err)
		}
		targets, err := runtime.RecompileStandingTargetAdmissions(loaded.Source, catalog, loaded.StandingTargets)
		if err != nil {
			return processAdmissionCandidate{}, fmt.Errorf("candidate provider-trigger catalog cannot recompile loaded runtime context %s: %w", loaded.BundleHash, err)
		}
		candidate.survivingTargets[loaded.BundleHash] = targets
	}
	return candidate, nil
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
	return s.replaceCurrentRuntimeWithSourceAndAdmission(ctx, resolvedRoot, source, bundle, fact, identity, newRT, nil)
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntimeWithSourceAndAdmission(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
	admissionCandidate *processAdmissionCandidate,
) (builderpkg.ProjectStatus, error) {
	s.mu.RLock()
	manager := s.runtimeContexts
	oldHash := strings.TrimSpace(s.currentBundleSourceFact.BundleHash)
	s.mu.RUnlock()
	newHash := strings.TrimSpace(fact.BundleHash)
	if manager != nil && oldHash != "" {
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
		if admissionCandidate != nil {
			if err := manager.ValidateProcessAdmissionReplacement(oldHash, contextDef, admissionCandidate.survivingTargets, admissionCandidate.state); err != nil {
				return builderpkg.ProjectStatus{}, err
			}
		}
		oldContext, ok := manager.LookupBundleHash(oldHash)
		if !ok || oldContext == nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("predecessor runtime context %s is not loaded", oldHash)
		}
		oldContextDef := *oldContext
		s.mu.RLock()
		oldRT := s.currentRT
		s.mu.RUnlock()
		s.setReady(false)
		if _, err := manager.BeginBundleHashReplacement(oldHash, contextDef); err != nil {
			s.setReady(true)
			return s.CurrentProject(), fmt.Errorf("withdraw predecessor runtime context for replacement: %w", err)
		}
		if err := s.quiesceCurrentRuntimeWithOptions(ctx, oldRT, s.replacementShutdown); err != nil {
			restoreErr := s.completeFailedQuiescenceAndRestore(ctx, manager, oldContextDef, oldRT)
			return s.CurrentProject(), errors.Join(fmt.Errorf("quiesce predecessor runtime before replacement: %w", err), restoreErr)
		}
		handoff, err := newRT.PrepareStartupOwnershipHandoff(oldRT)
		if err != nil {
			return s.CurrentProject(), errors.Join(err, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT))
		}
		finalized := false
		defer func() {
			if !finalized {
				_ = handoff.Rollback()
			}
		}()
		if err := s.startCurrentRuntime(ctx, newRT); err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			rollbackErr := handoff.Rollback()
			return s.CurrentProject(), errors.Join(err, rollbackErr, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT))
		}
		targets, _, err := newRT.EnsureStandingReplacementTargets(ctx, oldRT)
		if err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			rollbackErr := handoff.Rollback()
			return s.CurrentProject(), errors.Join(err, rollbackErr, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT))
		}
		contextDef.StandingTargets = targets
		if err := handoff.Commit(); err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			rollbackErr := handoff.Rollback()
			return s.CurrentProject(), errors.Join(err, rollbackErr, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT))
		}
		publish := func() error { return manager.PublishBundleHashReplacement(oldHash, contextDef) }
		if admissionCandidate != nil {
			publish = func() error {
				return manager.PublishBundleHashReplacementWithAdmission(oldHash, contextDef, admissionCandidate.survivingTargets, admissionCandidate.state)
			}
		}
		if err := publish(); err != nil {
			quiesceErr := s.quiesceCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			rollbackErr := handoff.Rollback()
			return s.CurrentProject(), errors.Join(err, quiesceErr, rollbackErr, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT))
		}
		if admissionCandidate != nil {
			s.mu.Lock()
			s.providerTriggers = admissionCandidate.catalog
			s.mu.Unlock()
		}
		oldRT = s.swapCurrentRuntime(resolvedRoot, source, bundle, fact, identity, newRT)
		if err := handoff.Finalize(); err != nil {
			finalized = true
			return s.CurrentProject(), fmt.Errorf("finalize runtime startup ownership handoff: %w", err)
		}
		finalized = true
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
	if admissionCandidate != nil {
		s.mu.Lock()
		s.providerTriggers = admissionCandidate.catalog
		s.mu.Unlock()
	}
	return s.attachCurrentRuntime(resolvedRoot, source, bundle, fact, identity, newRT), nil
}

func (s *runtimeProjectSupervisor) setReady(ready bool) {
	if s != nil && s.ready != nil {
		s.ready.Store(ready)
	}
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
	ctx = s.runtimeStartContext(ctx)
	if s.startRuntime != nil {
		return s.startRuntime(ctx, rt)
	}
	return rt.Start(ctx)
}

func (s *runtimeProjectSupervisor) shutdownCurrentRuntime(ctx context.Context, rt *runtime.Runtime) error {
	return s.shutdownCurrentRuntimeWithOptions(ctx, rt, runtime.DefaultShutdownOptions())
}

func (s *runtimeProjectSupervisor) quiesceCurrentRuntimeWithOptions(ctx context.Context, rt *runtime.Runtime, opts runtime.ShutdownOptions) error {
	if s == nil || rt == nil {
		return nil
	}
	if s.quiesceRuntime != nil {
		return s.quiesceRuntime(ctx, rt, opts)
	}
	return rt.QuiesceForReplacement(opts)
}

func (s *runtimeProjectSupervisor) restoreQuiescedPredecessor(ctx context.Context, manager *runtime.RuntimeContextManager, predecessorContext runtime.BundleContext, predecessor *runtime.Runtime) error {
	if predecessor == nil {
		return fmt.Errorf("restore predecessor runtime: predecessor is required")
	}
	restoreGrace := s.replacementShutdown.Grace
	if restoreGrace < runtime.DefaultShutdownGrace {
		restoreGrace = runtime.DefaultShutdownGrace
	}
	restoreCtx, cancelRestore := context.WithTimeout(context.Background(), restoreGrace)
	defer cancelRestore()
	ctx = restoreCtx
	s.mu.RLock()
	predecessorRoot := s.currentRoot
	predecessorBundle := s.currentBundle
	s.mu.RUnlock()
	clone := s.cloneRuntime
	if clone == nil {
		clone = func(ctx context.Context, predecessor *runtime.Runtime) (*runtime.Runtime, error) {
			return runtime.NewRuntime(ctx, runtime.RuntimeDeps{Config: predecessor.Config, Stores: predecessor.Stores, Options: predecessor.Options})
		}
	}
	restored, err := clone(ctx, predecessor)
	if err != nil {
		return fmt.Errorf("restore predecessor runtime construction: %w", err)
	}
	handoff, err := restored.PrepareStartupOwnershipHandoff(predecessor)
	if err != nil {
		return fmt.Errorf("restore predecessor ownership: %w", err)
	}
	finalized := false
	defer func() {
		if !finalized {
			_ = handoff.Rollback()
		}
	}()
	if err := s.startCurrentRuntime(s.runtimeStartContext(context.Background()), restored); err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("restart predecessor runtime: %w", err)
	}
	targets, _, err := restored.EnsureStandingTargets(ctx)
	if err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("restore predecessor standing targets: %w", err)
	}
	if err := handoff.Commit(); err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("commit predecessor ownership restoration: %w", err)
	}
	predecessorContext.Runtime = restored
	predecessorContext.StandingTargets = targets
	if err := manager.PublishRestoredBundleHashReplacement(predecessorContext.BundleHash, predecessorContext); err != nil {
		quiesceErr := s.quiesceCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		rollbackErr := handoff.Rollback()
		return errors.Join(fmt.Errorf("restore predecessor runtime context: %w", err), quiesceErr, rollbackErr)
	}
	s.swapCurrentRuntime(predecessorRoot, predecessorContext.Source, predecessorBundle, predecessorContext.BundleSourceFact, predecessorContext.BundleIdentity, restored)
	if err := handoff.Finalize(); err != nil {
		finalized = true
		return fmt.Errorf("finalize predecessor ownership restoration: %w", err)
	}
	finalized = true
	return nil
}

func (s *runtimeProjectSupervisor) completeFailedQuiescenceAndRestore(ctx context.Context, manager *runtime.RuntimeContextManager, predecessorContext runtime.BundleContext, predecessor *runtime.Runtime) error {
	if err := s.quiesceCurrentRuntimeWithOptions(context.Background(), predecessor, runtime.DefaultShutdownOptions()); err != nil {
		return fmt.Errorf("complete failed predecessor quiescence before restoration: %w", err)
	}
	return s.restoreQuiescedPredecessor(ctx, manager, predecessorContext, predecessor)
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

func (s *runtimeProjectSupervisor) runtimeStartContext(fallback context.Context) context.Context {
	if s != nil && s.runtimeLifetime != nil {
		return s.runtimeLifetime
	}
	if fallback != nil {
		return fallback
	}
	return context.Background()
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
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, PackageKey: target.PackageKey,
		FlowID: target.FlowID, RunID: target.RunID, Generation: target.Generation,
		PublicationSequence: target.PublicationSequence, InstanceID: target.InstanceID,
		FlowInstance: target.FlowInstance, EntityID: target.EntityID, EntitySlug: target.Alias,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
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
