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

	builderpkg "github.com/division-sh/swarm/internal/builder"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

type runtimeProjectSupervisor struct {
	RepoRoot             string
	platformSpecPath     string
	cfg                  *config.Config
	stores               storeBundle
	ready                serveReadiness
	dev                  bool
	mountSources         cliapp.WorkspaceMountSources
	workspaceBackend     cliapp.WorkspaceBackendSelection
	credentials          runtimecredentials.Store
	providerCredentials  runtimecredentials.Store
	providerTriggers     *providertriggers.CatalogSnapshot
	processWorkOwner     *worklifetime.Process
	processCapability    runtimestartupownership.ProcessCapability
	runtimeGeneration    uint64
	loadProviderCatalog  func() (*providertriggers.CatalogSnapshot, error)
	loadChannelPacks     func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) (cliapp.ChannelPackLoad, error)
	onRuntimePublished   func(context.Context) error
	beforeStartupHandoff func(context.Context) (func(), error)
	channelPlans         []packs.SatisfactionPlan
	channelBindings      []packs.OutboundBindingPlan
	startRuntime         func(context.Context, *runtime.Runtime) error
	quiesceRuntime       func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	shutdownRuntime      func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	loadWorkflow         func(RepoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource       func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error
	initStateStores      func(context.Context, storeBundle, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces        func(storeBundle, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error)
	createRuntime        func(context.Context, runtime.RuntimeDeps) (*runtime.Runtime, error)
	cloneRuntime         func(context.Context, *runtime.Runtime) (*runtime.Runtime, *worklifetime.RuntimeOccurrence, error)
	replacementShutdown  runtime.ShutdownOptions
	runtimeLifetime      context.Context
	runtimeInstanceID    string
	operationMu          sync.Mutex

	mu                              sync.RWMutex
	currentRoot                     string
	currentSource                   semanticview.Source
	currentBundle                   *runtimecontracts.WorkflowContractBundle
	currentRT                       *runtime.Runtime
	currentBundleSourceFact         runtimecorrelation.BundleSourceFact
	currentBundleIdentity           runtimecontracts.BundleIdentity
	runtimeContexts                 *runtime.RuntimeContextManager
	pendingReplacement              *pendingRuntimeReplacement
	sourceReplacementDisabledReason string
	executionPosture                executionposture.Posture
}

func (s *runtimeProjectSupervisor) SetProcessCapability(capability runtimestartupownership.ProcessCapability) {
	if s == nil {
		return
	}
	s.operationMu.Lock()
	s.processCapability = capability
	if s.runtimeGeneration == 0 {
		s.runtimeGeneration = 1
	}
	s.operationMu.Unlock()
}

func (s *runtimeProjectSupervisor) SetRuntimeContextManager(manager *runtime.RuntimeContextManager, fact runtimecorrelation.BundleSourceFact, identity runtimecontracts.BundleIdentity) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeContexts = manager
	s.currentBundleSourceFact = fact
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

func (s *runtimeProjectSupervisor) SetRuntimePublishedHook(hook func(context.Context) error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.onRuntimePublished = hook
	s.mu.Unlock()
}

func (s *runtimeProjectSupervisor) SetStartupOwnershipHandoffBarrier(barrier func(context.Context) (func(), error)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.beforeStartupHandoff = barrier
	s.mu.Unlock()
}

func (s *runtimeProjectSupervisor) PublicIngressState() (*runtime.Runtime, []packs.OutboundBindingPlan, *runtime.RuntimeContextManager) {
	if s == nil {
		return nil, nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRT, append([]packs.OutboundBindingPlan(nil), s.channelBindings...), s.runtimeContexts
}

func newRuntimeProjectSupervisor(
	RepoRoot string,
	platformSpecPath string,
	cfg *config.Config,
	stores storeBundle,
	ready serveReadiness,
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
		processWorkOwner: func() *worklifetime.Process {
			if initialRT == nil {
				return nil
			}
			return initialRT.Options.ProcessWorkOwner
		}(),
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
		validateSource: newBuilderProjectSourceValidator(cfg),
		initStateStores: func(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
			return initializeStateStores(ctx, stores, bundle)
		},
		newWorkspaces: func(stores storeBundle, contractsRoot string, source semanticview.Source, mountSources cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error) {
			decision, err := cliapp.DecideWorkspaceBackend(workspaceBackend, cfg, source)
			if err != nil {
				return nil, cliapp.WorkspaceBackendSelection{}, err
			}
			lifecycle, err := cliapp.ConfiguredWorkspaceLifecycleForBackend(stores.facade().workspaceLookup(), cfg, contractsRoot, source, mountSources, decision)
			if err != nil {
				return nil, decision, err
			}
			return lifecycle, decision, nil
		},
		createRuntime: func(ctx context.Context, deps runtime.RuntimeDeps) (*runtime.Runtime, error) {
			return runtime.NewRuntime(ctx, deps)
		},
		cloneRuntime: func(ctx context.Context, predecessor *runtime.Runtime) (*runtime.Runtime, *worklifetime.RuntimeOccurrence, error) {
			if predecessor == nil {
				return nil, nil, fmt.Errorf("predecessor runtime is required")
			}
			deps := stores.runtimeDeps()
			deps.Config = predecessor.Config
			deps.Options = predecessor.Options
			restored, err := runtime.NewRuntime(ctx, deps)
			if err != nil {
				return nil, nil, err
			}
			return restored, restored.WorkOccurrence(), nil
		},
		currentRoot:   strings.TrimSpace(initialRoot),
		currentSource: initialSource,
		currentBundle: initialBundle,
		currentRT:     initialRT,
		executionPosture: func() executionposture.Posture {
			if initialRT == nil {
				return ""
			}
			return initialRT.ExecutionPosture
		}(),
	}
	if initialRT != nil {
		supervisor.channelPlans = append([]packs.SatisfactionPlan(nil), initialRT.Options.ChannelPlans...)
		supervisor.channelBindings = append([]packs.OutboundBindingPlan(nil), initialRT.Options.ChannelOutboundBindings...)
	}
	return supervisor
}

func newBuilderProjectSourceValidator(cfg *config.Config) func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error {
	return func(ctx context.Context, source semanticview.Source, catalog *providertriggers.CatalogSnapshot) error {
		if cfg == nil {
			return fmt.Errorf("runtime config is required for Builder validation")
		}
		credentialStore, err := cliapp.BuildCredentialStore()
		if err != nil {
			return err
		}
		profile, err := cfg.LLMBackendProfile()
		if err != nil {
			return fmt.Errorf("resolve llm backend profile for Builder validation: %w", err)
		}
		posture, err := cfg.ProcessExecutionPosture()
		if err != nil {
			return err
		}
		opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore, posture)
		opts.LLMProfile = profile
		opts.ProviderTriggerCatalog = catalog
		_, err = runtime.ValidateWorkflowContractSurface(ctx, source, opts)
		return err
	}
}

func (s *runtimeProjectSupervisor) CurrentSource() semanticview.Source {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSource
}

func (s *runtimeProjectSupervisor) acquireCurrentRuntime(ctx context.Context) (*runtime.RuntimeContextUse, error) {
	if s == nil || s.runtimeContexts == nil {
		return nil, fmt.Errorf("runtime context manager unavailable")
	}
	s.mu.RLock()
	bundleHash := s.currentBundleSourceFact.BundleHash()
	s.mu.RUnlock()
	use, lookup, err := s.runtimeContexts.AcquireBundleHash(ctx, bundleHash)
	if err != nil {
		return nil, err
	}
	if use == nil || !lookup.Loaded() {
		return nil, fmt.Errorf("current runtime context unavailable: %s", lookup.Cause)
	}
	return use, nil
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
	if err := s.completePendingReplacement(); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime replacement before close: %w", err)
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := s.currentBundleSourceFact.BundleHash()
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
	if err := s.completePendingReplacement(); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime replacement: %w", err)
	}
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
	if module == nil {
		return builderpkg.ProjectStatus{}, errors.New("loaded project workflow module is required")
	}
	if module.SemanticSource() == nil {
		return builderpkg.ProjectStatus{}, errors.New("loaded project semantic source is required")
	}
	authoredSource := semanticview.Wrap(bundle)
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
		channelLoad, loadErr := s.loadChannelPacks(ctx, authoredSource, candidateCatalog)
		if loadErr != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate channel packs: %w", loadErr)
		}
		candidateChannelPlans = channelLoad.Plans
		candidateChannelBindings = channelLoad.Bindings
	}
	bundleSourcePlan, err := planServeBundleSource(s.stores, bundle, s.dev)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("plan project bundle source: %w", err)
	}
	projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{
		WorkflowModule: module, BundleSourceFact: bundleSourcePlan.fact,
		ProviderTriggerCatalog: candidateCatalog, ChannelPlans: candidateChannelPlans,
		ChannelOutboundBindings: candidateChannelBindings,
	})
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("admit candidate effective source projection: %w", err)
	}
	source := projection.Source()
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
	bundleSourceFact, err := persistServeBundleSourcePlan(ctx, s.stores, bundleSourcePlan)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("persist project bundle source: %w", err)
	}
	if !bundleSourceFact.Matches(bundleSourcePlan.fact) {
		return builderpkg.ProjectStatus{}, fmt.Errorf("persisted project bundle source fact changed after prevalidation")
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

	if s.processWorkOwner == nil {
		return builderpkg.ProjectStatus{}, errors.New("served process work owner is required for project runtime replacement")
	}
	deps := s.stores.runtimeDeps()
	deps.Config = s.cfg
	locatedScenarios, err := scenarioderivation.LoadDeclarations(resolvedRoot)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate scenario derivation profiles: %w", err)
	}
	scenarioDeclarations := make([]scenarioderivation.Declaration, 0, len(locatedScenarios))
	for _, located := range locatedScenarios {
		scenarioDeclarations = append(scenarioDeclarations, located.Declaration)
	}
	deps.Options = runtime.RuntimeOptions{
		SelfCheck:               false,
		ProcessWorkOwner:        s.processWorkOwner,
		WorkflowModule:          module,
		WorkspaceLifecycle:      workspaces,
		BundleSourceFact:        bundleSourceFact,
		RuntimeInstanceID:       s.runtimeInstanceID,
		Credentials:             s.credentials,
		ManagedCredentials:      managedCredentialStore,
		ProviderCredentials:     s.providerCredentials,
		ProviderTriggerCatalog:  candidateCatalog,
		ChannelPlans:            candidateChannelPlans,
		ChannelOutboundBindings: candidateChannelBindings,
		ScenarioDeclarations:    scenarioDeclarations,
	}
	newRT, err := s.createRuntime(ctx, deps)
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if newRT == nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("replacement runtime is required")
	}
	if !newRT.Options.BundleSourceFact.Matches(bundleSourceFact) {
		return builderpkg.ProjectStatus{}, fmt.Errorf("replacement runtime bundle source fact changed after prevalidation")
	}
	if !newRT.EffectiveSourceIdentity.Equal(projection.Identity()) {
		return builderpkg.ProjectStatus{}, fmt.Errorf(
			"replacement runtime effective source changed after prevalidation: validated=%s admitted=%s",
			projection.Identity().Digest(), newRT.EffectiveSourceIdentity.Digest(),
		)
	}
	if newRT.ScenarioProfileCatalog == nil || !newRT.ScenarioProfileCatalog.EffectiveSourceIdentity().Equal(projection.Identity()) {
		return builderpkg.ProjectStatus{}, fmt.Errorf("replacement runtime scenario profile catalog does not belong to the prevalidated effective source")
	}
	_, source, err = admittedRuntimeModuleAndSource(newRT)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("retain replacement runtime source: %w", err)
	}
	admissionCandidate.catalog = candidateCatalog
	admissionCandidate.channelPlans = append([]packs.SatisfactionPlan(nil), candidateChannelPlans...)
	admissionCandidate.channelBindings = append([]packs.OutboundBindingPlan(nil), candidateChannelBindings...)
	status, err := s.replaceCurrentRuntimeWithSourceAndAdmission(ctx, resolvedRoot, source, bundle, bundleSourceFact, bundleIdentity, newRT, newRT.WorkOccurrence(), &admissionCandidate)
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
	channelPlans     []packs.SatisfactionPlan
	channelBindings  []packs.OutboundBindingPlan
}

type pendingRuntimeReplacement struct {
	mu          sync.Mutex
	publication *runtime.PreparedRuntimeContextReplacement
	root        string
	source      semanticview.Source
	bundle      *runtimecontracts.WorkflowContractBundle
	fact        runtimecorrelation.BundleSourceFact
	identity    runtimecontracts.BundleIdentity
	runtime     *runtime.Runtime
	admission   *processAdmissionCandidate
	freeze      *startupHandoffFreeze
	finalized   bool
}

type sourceSetReplacementAttempt struct {
	replaceOperationID string
	restoreOperationID string
}

func newSourceSetReplacementAttempt() sourceSetReplacementAttempt {
	return sourceSetReplacementAttempt{
		replaceOperationID: uuid.NewString(),
		restoreOperationID: uuid.NewString(),
	}
}

type startupHandoffFreeze struct {
	once    sync.Once
	release func()
}

func (f *startupHandoffFreeze) Release() {
	if f == nil {
		return
	}
	f.once.Do(func() {
		if f.release != nil {
			f.release()
		}
	})
}

func cloneProcessAdmissionCandidate(candidate *processAdmissionCandidate) *processAdmissionCandidate {
	if candidate == nil {
		return nil
	}
	cloned := *candidate
	cloned.state.InstalledSubjects = packs.CloneSubjects(candidate.state.InstalledSubjects)
	cloned.survivingTargets = make(map[string][]runtime.StandingTarget, len(candidate.survivingTargets))
	for bundleHash, targets := range candidate.survivingTargets {
		cloned.survivingTargets[bundleHash] = append([]runtime.StandingTarget(nil), targets...)
	}
	cloned.channelPlans = append([]packs.SatisfactionPlan(nil), candidate.channelPlans...)
	cloned.channelBindings = append([]packs.OutboundBindingPlan(nil), candidate.channelBindings...)
	return &cloned
}

func (s *runtimeProjectSupervisor) installProcessGenerationGrant(ctx context.Context, rt *runtime.Runtime, plan runtimeagenttopology.SourceSetPlan) error {
	if s == nil || rt == nil {
		return errors.New("runtime replacement generation grant requires a runtime")
	}
	if s.processCapability == nil {
		return errors.New("runtime replacement requires the process topology capability")
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	if !exists || current.Revision != plan.Revision {
		return errors.New("runtime replacement generation grant requires the committed complete source set")
	}
	s.runtimeGeneration++
	_, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	grant, err := s.processCapability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: rt.Options.BundleSourceFact.BundleHash(), BundleSource: bundleSource,
		RuntimeInstanceID: s.runtimeInstanceID, RuntimeGeneration: s.runtimeGeneration,
		SourceSetRevision: plan.Revision,
	})
	if err != nil {
		return err
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		_ = grant.Retire(context.Background())
		return err
	}
	return nil
}

func replacementSourceSetPlan(manager *runtime.RuntimeContextManager, replacedHash string, candidate runtime.BundleContext) (runtimeagenttopology.SourceSetPlan, error) {
	if manager != nil {
		return manager.CompileReplacementSourceSetPlan(replacedHash, candidate)
	}
	sources := []runtimeagenttopology.SourceCoordinate{}
	agents := []runtimeagenttopology.DesiredAgent{}
	appendContext := func(contextDef runtime.BundleContext) error {
		bundleHash, bundleSource := contextDef.BundleSourceFact.StorageValues()
		coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
		if contextDef.Runtime == nil || contextDef.Runtime.Manager == nil {
			return errors.New("runtime replacement source-set compilation requires the candidate runtime manager")
		}
		desired, err := contextDef.Runtime.Manager.CompileStaticTopologyDesiredAgents(contextDef.Source, coordinate)
		if err != nil {
			return err
		}
		sources = append(sources, coordinate)
		agents = append(agents, desired...)
		return nil
	}
	if candidate.BundleSourceFact.Validate() == nil {
		if err := appendContext(candidate); err != nil {
			return runtimeagenttopology.SourceSetPlan{}, err
		}
	}
	return runtimeagenttopology.NewSourceSetPlan(sources, agents)
}

func (s *runtimeProjectSupervisor) replaceCommittedSourceSet(ctx context.Context, plan runtimeagenttopology.SourceSetPlan, attempt sourceSetReplacementAttempt) (runtimeagenttopology.SourceSetPlan, error) {
	if s == nil || s.processCapability == nil {
		return runtimeagenttopology.SourceSetPlan{}, errors.New("runtime replacement requires the process topology capability")
	}
	previous, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return runtimeagenttopology.SourceSetPlan{}, err
	}
	if !exists {
		return runtimeagenttopology.SourceSetPlan{}, errors.New("runtime replacement requires an installed source set")
	}
	if previous.Revision == plan.Revision {
		return previous, nil
	}
	_, err = s.processCapability.ReplaceSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
		OperationID:      attempt.replaceOperationID,
		ExpectedRevision: previous.Revision, Plan: plan,
	})
	return previous, err
}

func (s *runtimeProjectSupervisor) restoreCommittedSourceSet(ctx context.Context, expected, previous runtimeagenttopology.SourceSetPlan, attempt sourceSetReplacementAttempt) error {
	if expected.Revision == previous.Revision {
		return nil
	}
	_, err := s.processCapability.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
		OperationID:      attempt.restoreOperationID,
		ExpectedRevision: expected.Revision, Plan: previous,
	})
	return err
}

func (s *runtimeProjectSupervisor) prepareReplacementSurvivors(
	ctx context.Context,
	manager *runtime.RuntimeContextManager,
	plan runtimeagenttopology.SourceSetPlan,
) (*runtime.PreparedRuntimeSourceSetTransition, error) {
	if s == nil || s.processCapability == nil || manager == nil {
		return nil, errors.New("runtime replacement survivor refresh requires process topology and runtime context ownership")
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("runtime replacement survivor refresh requires an installed source set")
	}
	if current.Revision == plan.Revision {
		return nil, nil
	}
	return manager.PrepareSourceSetTransition(ctx, plan)
}

func (s *runtimeProjectSupervisor) restoreCommittedSourceSetAndSurvivors(
	ctx context.Context,
	manager *runtime.RuntimeContextManager,
	expected runtimeagenttopology.SourceSetPlan,
	previous runtimeagenttopology.SourceSetPlan,
	attempt sourceSetReplacementAttempt,
) error {
	if err := s.restoreCommittedSourceSet(ctx, expected, previous, attempt); err != nil {
		return err
	}
	transition, err := manager.PrepareSourceSetTransition(ctx, previous)
	if err != nil {
		return fmt.Errorf("prepare predecessor source-set survivor restoration: %w", err)
	}
	if transition == nil {
		return nil
	}
	if err := transition.Commit(ctx, s.processCapability); err != nil {
		return fmt.Errorf("restore predecessor source-set survivors: %w", err)
	}
	return nil
}

func (s *runtimeProjectSupervisor) completePendingReplacement() error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	pending := s.pendingReplacement
	s.mu.RUnlock()
	if pending == nil {
		return nil
	}
	pending.mu.Lock()
	defer pending.mu.Unlock()
	s.mu.RLock()
	current := s.pendingReplacement
	s.mu.RUnlock()
	if current != pending {
		return nil
	}
	if err := pending.publication.Publish(); err != nil {
		s.setReady(false)
		return fmt.Errorf("publish finalized runtime replacement: %w", err)
	}
	s.mu.Lock()
	if s.pendingReplacement != pending {
		s.mu.Unlock()
		return errors.New("pending runtime replacement changed before publication")
	}
	s.currentRoot = strings.TrimSpace(pending.root)
	s.currentSource = pending.source
	s.currentBundle = pending.bundle
	s.currentRT = pending.runtime
	s.currentBundleSourceFact = pending.fact
	s.currentBundleIdentity = pending.identity
	if pending.admission != nil {
		s.providerTriggers = pending.admission.catalog
		s.channelPlans = append([]packs.SatisfactionPlan(nil), pending.admission.channelPlans...)
		s.channelBindings = append([]packs.OutboundBindingPlan(nil), pending.admission.channelBindings...)
	}
	s.pendingReplacement = nil
	hook := s.onRuntimePublished
	s.mu.Unlock()
	pending.freeze.Release()
	if hook != nil {
		if err := hook(s.runtimeStartContext(context.Background())); err != nil {
			s.setReady(false)
			return fmt.Errorf("reconcile published runtime public ingress: %w", err)
		}
	}
	s.setReady(true)
	return nil
}

func (s *runtimeProjectSupervisor) compileProcessAdmissionCandidate(ctx context.Context, catalog *providertriggers.CatalogSnapshot) (processAdmissionCandidate, error) {
	installed, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		return processAdmissionCandidate{}, err
	}
	candidate := processAdmissionCandidate{
		state:            runtime.ProcessAdmissionState{Generation: catalog.Generation(), InstalledSubjects: installed},
		survivingTargets: map[string][]runtime.StandingTarget{},
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	currentHash := s.currentBundleSourceFact.BundleHash()
	s.mu.RUnlock()
	if manager == nil {
		return candidate, nil
	}
	for _, loaded := range manager.LoadedContexts() {
		if loaded.BundleHash() == currentHash {
			continue
		}
		if err := s.validateSource(ctx, loaded.Source, catalog); err != nil {
			return processAdmissionCandidate{}, fmt.Errorf("candidate provider-trigger catalog rejected loaded runtime context %s: %w", loaded.BundleHash(), err)
		}
		targets, err := runtime.RecompileStandingTargetAdmissions(loaded.Source, catalog, loaded.StandingTargets)
		if err != nil {
			return processAdmissionCandidate{}, fmt.Errorf("candidate provider-trigger catalog cannot recompile loaded runtime context %s: %w", loaded.BundleHash(), err)
		}
		candidate.survivingTargets[loaded.BundleHash()] = targets
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
		fact = newRT.Options.BundleSourceFact
		identity.BundleHash = fact.BundleHash()
	}
	return s.replaceCurrentRuntimeWithSource(ctx, resolvedRoot, source, bundle, fact, identity, newRT, newRT.WorkOccurrence())
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntimeWithSource(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
	workOwner *worklifetime.RuntimeOccurrence,
) (builderpkg.ProjectStatus, error) {
	return s.replaceCurrentRuntimeWithSourceAndAdmission(ctx, resolvedRoot, source, bundle, fact, identity, newRT, workOwner, nil)
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntimeWithSourceAndAdmission(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
	workOwner *worklifetime.RuntimeOccurrence,
	admissionCandidate *processAdmissionCandidate,
) (builderpkg.ProjectStatus, error) {
	s.mu.RLock()
	manager := s.runtimeContexts
	oldHash := s.currentBundleSourceFact.BundleHash()
	pending := s.pendingReplacement
	s.mu.RUnlock()
	if pending != nil {
		return s.CurrentProject(), errors.New("runtime replacement transition is already pending")
	}
	if newRT == nil {
		return s.CurrentProject(), errors.New("runtime replacement requires a candidate runtime")
	}
	if !s.executionPosture.Valid() || newRT.ExecutionPosture != s.executionPosture {
		return s.CurrentProject(), fmt.Errorf("runtime replacement cannot change process execution posture: current=%q candidate=%q", s.executionPosture, newRT.ExecutionPosture)
	}
	if err := fact.Validate(); err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("runtime replacement bundle source fact: %w", err)
	}
	if manager != nil && oldHash != "" {
		plannedTargets, err := newRT.PlanStandingTargets()
		if err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		contextDef := runtime.BundleContext{
			BundleSourceFact: fact, BundleIdentity: identity, Source: source,
			ContractsRoot: resolvedRoot, PlatformSpecPath: s.platformSpecPath, Runtime: newRT, WorkOwner: workOwner, StandingTargets: plannedTargets,
		}
		candidatePlan, err := replacementSourceSetPlan(manager, oldHash, contextDef)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("compile replacement source set: %w", err)
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
		s.mu.RLock()
		beforeStartupHandoff := s.beforeStartupHandoff
		s.mu.RUnlock()
		var freeze *startupHandoffFreeze
		if beforeStartupHandoff != nil {
			release, err := beforeStartupHandoff(ctx)
			if err != nil {
				s.setReady(true)
				return s.CurrentProject(), fmt.Errorf("settle public channel registration before startup ownership handoff: %w", err)
			}
			freeze = &startupHandoffFreeze{release: release}
			defer func() {
				s.mu.RLock()
				retained := s.pendingReplacement != nil && s.pendingReplacement.freeze == freeze
				s.mu.RUnlock()
				if !retained {
					freeze.Release()
				}
			}()
		}
		if _, err := manager.BeginBundleHashReplacement(ctx, oldHash, contextDef); err != nil {
			withdrawErr := fmt.Errorf("withdraw predecessor runtime context for replacement: %w", err)
			status := manager.LookupBundleHashStatus(oldHash)
			if status.Found && status.Cause == runtime.RuntimeContextCauseReplacing {
				restoreErr := s.completeFailedQuiescenceAndRestore(ctx, manager, oldContextDef, oldRT, freeze)
				return s.CurrentProject(), errors.Join(withdrawErr, restoreErr)
			}
			s.setReady(true)
			return s.CurrentProject(), withdrawErr
		}
		if err := s.quiesceCurrentRuntimeWithOptions(ctx, oldRT, s.replacementShutdown); err != nil {
			restoreErr := s.completeFailedQuiescenceAndRestore(ctx, manager, oldContextDef, oldRT, freeze)
			return s.CurrentProject(), errors.Join(fmt.Errorf("quiesce predecessor runtime before replacement: %w", err), restoreErr)
		}
		survivorTransition, err := s.prepareReplacementSurvivors(ctx, manager, candidatePlan)
		if err != nil {
			return s.CurrentProject(), errors.Join(err, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT, freeze))
		}
		survivorTransitionSettled := false
		defer func() {
			if !survivorTransitionSettled && survivorTransition != nil {
				_ = survivorTransition.Abort()
			}
		}()
		topologyAttempt := newSourceSetReplacementAttempt()
		previousPlan, err := s.replaceCommittedSourceSet(ctx, candidatePlan, topologyAttempt)
		if err != nil {
			if survivorTransition != nil {
				survivorTransitionSettled = true
				err = errors.Join(err, survivorTransition.Abort())
			}
			return s.CurrentProject(), errors.Join(err, s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT, freeze))
		}
		restorePredecessor := func(cause error) error {
			topologyErr := s.restoreCommittedSourceSet(context.Background(), candidatePlan, previousPlan, topologyAttempt)
			if survivorTransition != nil {
				survivorTransitionSettled = true
				topologyErr = errors.Join(topologyErr, survivorTransition.Abort())
			}
			restartErr := s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT, freeze)
			return errors.Join(cause, topologyErr, restartErr)
		}
		restoreAfterSurvivorCommit := func(cause error) error {
			topologyErr := s.restoreCommittedSourceSetAndSurvivors(context.Background(), manager, candidatePlan, previousPlan, topologyAttempt)
			restartErr := s.restoreQuiescedPredecessor(ctx, manager, oldContextDef, oldRT, freeze)
			return errors.Join(cause, topologyErr, restartErr)
		}
		transitionRetained := false
		var publication *runtime.PreparedRuntimeContextReplacement
		defer func() {
			if !transitionRetained {
				if publication != nil {
					_ = publication.Discard()
				}
			}
		}()
		if err := s.installProcessGenerationGrant(ctx, newRT, candidatePlan); err != nil {
			return s.CurrentProject(), restorePredecessor(err)
		}
		if err := s.startCurrentRuntime(ctx, newRT); err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			return s.CurrentProject(), restorePredecessor(err)
		}
		targets, _, err := newRT.EnsureStandingReplacementTargets(ctx, oldRT)
		if err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			return s.CurrentProject(), restorePredecessor(err)
		}
		contextDef.StandingTargets = targets
		if admissionCandidate == nil {
			publication, err = manager.PrepareBundleHashReplacementPublication(oldHash, contextDef)
		} else {
			publication, err = manager.PrepareBundleHashReplacementPublicationWithAdmission(oldHash, contextDef, admissionCandidate.survivingTargets, admissionCandidate.state)
		}
		if err != nil {
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			return s.CurrentProject(), restorePredecessor(err)
		}
		if survivorTransition != nil {
			survivorTransitionSettled = true
			if err := survivorTransition.Commit(ctx, s.processCapability); err != nil {
				_ = publication.Discard()
				publication = nil
				_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
				return s.CurrentProject(), restoreAfterSurvivorCommit(err)
			}
		}
		pending := &pendingRuntimeReplacement{
			publication: publication,
			root:        resolvedRoot, source: source, bundle: bundle, fact: fact, identity: identity, runtime: newRT,
			admission: cloneProcessAdmissionCandidate(admissionCandidate), freeze: freeze,
		}
		s.mu.Lock()
		if s.pendingReplacement != nil {
			s.mu.Unlock()
			return s.CurrentProject(), errors.New("runtime replacement transition is already pending")
		}
		s.pendingReplacement = pending
		s.mu.Unlock()
		transitionRetained = true
		if err := s.completePendingReplacement(); err != nil {
			s.mu.Lock()
			if s.pendingReplacement == pending {
				s.pendingReplacement = nil
			}
			s.mu.Unlock()
			_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), newRT, s.replacementShutdown)
			return s.CurrentProject(), restoreAfterSurvivorCommit(err)
		}
		return s.CurrentProject(), nil
	}
	oldRT := s.detachCurrentRuntime()
	if oldRT != nil {
		if err := s.shutdownCurrentRuntime(ctx, oldRT); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
	}
	var previousPlan runtimeagenttopology.SourceSetPlan
	var candidatePlan runtimeagenttopology.SourceSetPlan
	var topologyAttempt sourceSetReplacementAttempt
	if s.processCapability != nil {
		contextDef := runtime.BundleContext{
			BundleSourceFact: fact, BundleIdentity: identity, Source: source,
			ContractsRoot: resolvedRoot, PlatformSpecPath: s.platformSpecPath, Runtime: newRT, WorkOwner: workOwner,
		}
		var err error
		candidatePlan, err = replacementSourceSetPlan(nil, "", contextDef)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("compile initial runtime source set: %w", err)
		}
		topologyAttempt = newSourceSetReplacementAttempt()
		previousPlan, err = s.replaceCommittedSourceSet(ctx, candidatePlan, topologyAttempt)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("commit initial runtime source set: %w", err)
		}
		if err := s.installProcessGenerationGrant(ctx, newRT, candidatePlan); err != nil {
			return builderpkg.ProjectStatus{}, errors.Join(err, s.restoreCommittedSourceSet(context.Background(), candidatePlan, previousPlan, topologyAttempt))
		}
	}
	if err := s.startCurrentRuntime(ctx, newRT); err != nil {
		_ = s.shutdownCurrentRuntime(context.Background(), newRT)
		if s.processCapability != nil {
			return builderpkg.ProjectStatus{}, errors.Join(err, s.restoreCommittedSourceSet(context.Background(), candidatePlan, previousPlan, topologyAttempt))
		}
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
	s.currentBundleSourceFact, s.currentBundleIdentity = fact, identity
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
	s.currentBundleSourceFact = fact
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

func (s *runtimeProjectSupervisor) restoreQuiescedPredecessor(ctx context.Context, manager *runtime.RuntimeContextManager, predecessorContext runtime.BundleContext, predecessor *runtime.Runtime, freeze *startupHandoffFreeze) error {
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
		clone = func(ctx context.Context, predecessor *runtime.Runtime) (*runtime.Runtime, *worklifetime.RuntimeOccurrence, error) {
			deps := s.stores.runtimeDeps()
			deps.Config = predecessor.Config
			deps.Options = predecessor.Options
			restored, err := runtime.NewRuntime(ctx, deps)
			if err != nil {
				return nil, nil, err
			}
			return restored, restored.WorkOccurrence(), nil
		}
	}
	restored, restoredWorkOwner, err := clone(ctx, predecessor)
	if err != nil {
		return fmt.Errorf("restore predecessor runtime construction: %w", err)
	}
	if restoredWorkOwner == nil {
		return fmt.Errorf("restore predecessor runtime construction: runtime occurrence is required")
	}
	predecessorContext.Runtime = restored
	predecessorContext.WorkOwner = restoredWorkOwner
	transitionRetained := false
	var publication *runtime.PreparedRuntimeContextReplacement
	defer func() {
		if !transitionRetained {
			if publication != nil {
				_ = publication.Discard()
			}
		}
	}()
	restoredPlan, err := replacementSourceSetPlan(manager, "", predecessorContext)
	if err != nil {
		return fmt.Errorf("compile restored predecessor source set: %w", err)
	}
	if err := s.installProcessGenerationGrant(ctx, restored, restoredPlan); err != nil {
		return fmt.Errorf("restore predecessor generation grant: %w", err)
	}
	if err := s.startCurrentRuntime(s.runtimeStartContext(context.Background()), restored); err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("restart predecessor runtime: %w", err)
	}
	targets, _, err := restored.EnsureStandingTargets(ctx)
	if err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("restore predecessor standing targets: %w", err)
	}
	predecessorContext.StandingTargets = targets
	publication, err = manager.PrepareRestoredBundleHashReplacementPublication(predecessorContext.BundleHash(), predecessorContext)
	if err != nil {
		_ = s.shutdownCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return fmt.Errorf("prepare predecessor runtime context restoration: %w", err)
	}
	pending := &pendingRuntimeReplacement{
		publication: publication,
		root:        predecessorRoot, source: predecessorContext.Source, bundle: predecessorBundle,
		fact: predecessorContext.BundleSourceFact, identity: predecessorContext.BundleIdentity, runtime: restored,
		freeze: freeze,
	}
	s.mu.Lock()
	if s.pendingReplacement != nil {
		s.mu.Unlock()
		quiesceErr := s.quiesceCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return errors.Join(errors.New("runtime replacement transition is already pending"), quiesceErr)
	}
	s.pendingReplacement = pending
	s.mu.Unlock()
	transitionRetained = true
	return s.completePendingReplacement()
}

func (s *runtimeProjectSupervisor) completeFailedQuiescenceAndRestore(ctx context.Context, manager *runtime.RuntimeContextManager, predecessorContext runtime.BundleContext, predecessor *runtime.Runtime, freeze *startupHandoffFreeze) error {
	if err := s.quiesceCurrentRuntimeWithOptions(context.Background(), predecessor, runtime.DefaultShutdownOptions()); err != nil {
		return fmt.Errorf("complete failed predecessor quiescence before restoration: %w", err)
	}
	return s.restoreQuiescedPredecessor(ctx, manager, predecessorContext, predecessor, freeze)
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
	use, lookup, acquireErr := h.contexts.AcquireIngress(r.Context(), alias, providertriggers.NormalizeProviderName(provider))
	if !lookup.Found {
		if lookup.AliasFound {
			http.Error(w, fmt.Sprintf("ingress target %q does not declare provider %q; add that provider binding to the standing singleton flow", alias, provider), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
		return
	}
	if acquireErr != nil {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q cannot admit work: %v", alias, provider, acquireErr), http.StatusServiceUnavailable)
		return
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().InboundGateway == nil {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q is unavailable: %s", alias, provider, lookup.Cause), http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = use.Done() }()
	target := lookup.Target
	selectedRuntime := use.Runtime()
	selectedRuntime.InboundGateway.HandleResolvedWebhook(w, r.WithContext(use.WorkContext()), runtime.InboundTarget{
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, PackageKey: target.PackageKey,
		FlowID: target.FlowID, RunID: target.RunID, Generation: target.Generation,
		PublicationSequence: target.PublicationSequence, InstanceID: target.InstanceID,
		FlowInstance: target.FlowInstance, EntityID: target.EntityID, EntitySlug: target.Alias,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
	}, use.Context.Source)
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
	use, err := c.supervisor.acquireCurrentRuntime(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.RuntimeIngress == nil {
		return fmt.Errorf("runtime ingress controller unavailable")
	}
	_, err = rt.RuntimeIngress.Pause(use.WorkContext(), runtimeingress.TransitionRequest{
		Reason:       "dashboard_action",
		ControlledBy: "dashboard",
	})
	return err
}

func (c dashboardDynamicRuntimeControl) ResumeIngress() error {
	use, err := c.supervisor.acquireCurrentRuntime(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.RuntimeIngress == nil {
		return fmt.Errorf("runtime ingress controller unavailable")
	}
	_, err = rt.RuntimeIngress.Resume(use.WorkContext(), runtimeingress.TransitionRequest{
		Reason:       "dashboard_action",
		ControlledBy: "dashboard",
	})
	return err
}

type dashboardDynamicAgentControl struct {
	supervisor *runtimeProjectSupervisor
}

func (c dashboardDynamicAgentControl) Restart(ctx context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	use, err := c.supervisor.acquireCurrentRuntime(ctx)
	if err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.RestartResult{}, fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.Restart(use.WorkContext(), req)
}

func (c dashboardDynamicAgentControl) SendDirective(ctx context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	use, err := c.supervisor.acquireCurrentRuntime(ctx)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.SendDirectiveResult{}, fmt.Errorf("runtime manager unavailable")
	}
	if req.Source == "" {
		req.Source = runtimeagentcontrol.DirectiveSourceBuilderRuntime
	}
	return rt.Manager.SendDirective(use.WorkContext(), req)
}
