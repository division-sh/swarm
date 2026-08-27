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
	"github.com/division-sh/swarm/internal/packartifact"
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
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

type runtimeProjectSupervisor struct {
	RepoRoot             string
	platformSpecPath     string
	cfg                  *config.Config
	stores               serveRuntimePersistence
	ready                serveReadiness
	mountSources         cliapp.WorkspaceMountSources
	workspaceBackend     cliapp.WorkspaceBackendSelection
	credentials          runtimecredentials.Store
	providerCredentials  runtimecredentials.Store
	providerTriggers     *providertriggers.CatalogSnapshot
	noticePresentation   runtimetools.InformationalNoticePresentationSink
	platformPackBase     *packartifact.PlatformPackInventory
	platformPackBases    *packartifact.PlatformPackBaseGenerationOwner
	processWorkOwner     *worklifetime.Process
	processCapability    runtimestartupownership.ProcessCapability
	runtimeGeneration    uint64
	loadRuntimeConfig    func() (cliapp.RuntimeConfigLoadResult, error)
	loadBundlePacks      func(context.Context, cliapp.RuntimeConfigLoadResult, *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error)
	onRuntimePublished   func(context.Context) error
	beforeStartupHandoff func(context.Context) (func(), error)
	channelPlans         []packs.SatisfactionPlan
	channelBindings      []packs.OutboundBindingPlan
	startRuntime         func(context.Context, *runtime.Runtime) error
	quiesceRuntime       func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	shutdownRuntime      func(context.Context, *runtime.Runtime, runtime.ShutdownOptions) error
	loadWorkflow         func(RepoRoot, contractsRoot, platformSpecPath string, base *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error)
	validateSource       func(context.Context, semanticview.Source, *providertriggers.CatalogSnapshot) error
	initStateStores      func(context.Context, store.SchemaBootstrapper, *runtimecontracts.WorkflowContractBundle) (string, error)
	newWorkspaces        func(workspace.Lookup, string, semanticview.Source, cliapp.WorkspaceMountSources) (workspace.Lifecycle, cliapp.WorkspaceBackendSelection, error)
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
	pendingReplacementRollback      *pendingRuntimeReplacementRollback
	pendingSourceSetRollback        *pendingRuntimeSourceSetRollback
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

func (s *runtimeProjectSupervisor) SetBundlePackRuntimeLoader(loader func(context.Context, cliapp.RuntimeConfigLoadResult, *runtimecontracts.WorkflowContractBundle) (cliapp.BundlePackRuntimeLoad, error)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadBundlePacks = loader
}

func (s *runtimeProjectSupervisor) SetRuntimeConfigLoader(loader func() (cliapp.RuntimeConfigLoadResult, error)) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.loadRuntimeConfig = loader
	s.mu.Unlock()
}

func (s *runtimeProjectSupervisor) SetPlatformPackBaseGenerationOwner(owner *packartifact.PlatformPackBaseGenerationOwner) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.platformPackBases = owner
	s.mu.Unlock()
}

func (s *runtimeProjectSupervisor) SetRuntimePublishedHook(hook func(context.Context) error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.onRuntimePublished = hook
	s.mu.Unlock()
}

func (s *runtimeProjectSupervisor) AddRuntimePublishedHook(hook func(context.Context) error) {
	if s == nil || hook == nil {
		return
	}
	s.mu.Lock()
	previous := s.onRuntimePublished
	s.onRuntimePublished = func(ctx context.Context) error {
		if previous != nil {
			if err := previous(ctx); err != nil {
				return err
			}
		}
		return hook(ctx)
	}
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
	stores serveRuntimePersistence,
	ready serveReadiness,
	mountSources cliapp.WorkspaceMountSources,
	workspaceBackend cliapp.WorkspaceBackendSelection,
	credentials runtimecredentials.Store,
	providerCredentials runtimecredentials.Store,
	providerTriggers *providertriggers.CatalogSnapshot,
	platformPackBase *packartifact.PlatformPackInventory,
	initialRoot string,
	initialBundle *runtimecontracts.WorkflowContractBundle,
	initialSource semanticview.Source,
	initialRT *runtime.Runtime,
) *runtimeProjectSupervisor {
	packBases, _ := packartifact.NewPlatformPackBaseGenerationOwner(platformPackBase)
	supervisor := &runtimeProjectSupervisor{
		RepoRoot:            strings.TrimSpace(RepoRoot),
		platformSpecPath:    strings.TrimSpace(platformSpecPath),
		cfg:                 cfg,
		stores:              stores,
		ready:               ready,
		mountSources:        mountSources,
		workspaceBackend:    workspaceBackend,
		credentials:         credentials,
		providerCredentials: providerCredentials,
		providerTriggers:    providerTriggers,
		platformPackBase:    platformPackBase,
		platformPackBases:   packBases,
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
		loadWorkflow: func(RepoRoot, contractsRoot, platformSpecPath string, base *packartifact.PlatformPackInventory) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
			return cliapp.NewSwarmWorkflowModuleWithPackBase(RepoRoot, contractsRoot, platformSpecPath, base)
		},
		initStateStores: func(ctx context.Context, schema store.SchemaBootstrapper, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
			return initializeStateStores(ctx, schema, bundle)
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
		providerCredentialStore, err := cliapp.BuildProviderCredentialStore()
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
		opts.ProviderCredentials = providerCredentialStore
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
	var finalizationErr error
	if err := s.completePendingReplacementRollback(ctx); err != nil {
		finalizationErr = fmt.Errorf("finalize pending runtime replacement rollback before close: %w", err)
	}
	if finalizationErr == nil {
		if err := s.completePendingSourceSetRollback(ctx); err != nil {
			finalizationErr = fmt.Errorf("finalize pending runtime source-set rollback before close: %w", err)
		}
	}
	if finalizationErr == nil {
		if err := s.completePendingReplacement(); err != nil {
			finalizationErr = fmt.Errorf("finalize pending runtime replacement before close: %w", err)
		}
	}
	if finalizationErr == nil {
		if err := s.completePendingRuntimeSourceSetRefresh(ctx); err != nil {
			finalizationErr = fmt.Errorf("finalize pending runtime source-set refresh before close: %w", err)
		}
	}
	if finalizationErr != nil {
		ownershipTerminal := false
		if s.processCapability != nil {
			_, ownershipTerminal = s.processCapability.TerminalResult()
		}
		if !ownershipTerminal {
			return s.CurrentProject(), finalizationErr
		}
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	bundleHash := s.currentBundleSourceFact.BundleHash()
	s.mu.RUnlock()
	if manager != nil && bundleHash != "" {
		result := manager.DeactivateBundleHashWithOptions(bundleHash, runtime.RuntimeContextCauseUnloaded, opts)
		_ = s.detachCurrentRuntime()
		return builderpkg.ProjectStatus{}, errors.Join(finalizationErr, result.ShutdownErr)
	}
	oldRT := s.detachCurrentRuntime()

	if oldRT != nil {
		if err := s.shutdownCurrentRuntimeWithOptions(ctx, oldRT, opts); err != nil {
			return builderpkg.ProjectStatus{}, errors.Join(finalizationErr, err)
		}
	}
	return builderpkg.ProjectStatus{}, finalizationErr
}

func (s *runtimeProjectSupervisor) loadProject(ctx context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if err := s.completePendingReplacementRollback(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime replacement rollback: %w", err)
	}
	if err := s.completePendingSourceSetRollback(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime source-set rollback: %w", err)
	}
	if err := s.completePendingReplacement(); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime replacement: %w", err)
	}
	if err := s.completePendingRuntimeSourceSetRefresh(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime source-set refresh: %w", err)
	}
	if reason := s.sourceReplacementDisabled(); reason != "" {
		return s.CurrentProject(), fmt.Errorf("project source replacement is disabled: %s", reason)
	}
	resolvedRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolvePath(s.RepoRoot, projectDir))
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}

	s.mu.RLock()
	loadRuntimeConfig := s.loadRuntimeConfig
	candidateBase := s.platformPackBase
	candidateConfig := cliapp.RuntimeConfigLoadResult{Config: s.cfg}
	s.mu.RUnlock()
	if loadRuntimeConfig != nil {
		candidateConfig, err = loadRuntimeConfig()
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate runtime config: %w", err)
		}
		candidateBase, err = cliapp.LoadConfiguredPlatformPackBase(s.RepoRoot, candidateConfig)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("select candidate platform pack base: %w", err)
		}
	}
	module, bundle, err := s.loadWorkflow(s.RepoRoot, resolvedRoot, s.platformSpecPath, candidateBase)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load project: %w", err)
	}
	if module == nil {
		return builderpkg.ProjectStatus{}, errors.New("loaded project workflow module is required")
	}
	if module.SemanticSource() == nil {
		return builderpkg.ProjectStatus{}, errors.New("loaded project semantic source is required")
	}
	if s.loadBundlePacks == nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("bundle-specific pack runtime loader is required")
	}
	candidatePacks, err := s.loadBundlePacks(ctx, candidateConfig, bundle)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("load candidate bundle pack runtime: %w", err)
	}
	candidateCatalog := candidatePacks.ProviderTriggers.Catalog
	if candidateCatalog == nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("candidate provider-trigger catalog is required")
	}
	candidateChannelPlans := append([]packs.SatisfactionPlan(nil), candidatePacks.Channels.Plans...)
	candidateChannelBindings := append([]packs.OutboundBindingPlan(nil), candidatePacks.Channels.Bindings...)
	bundleSourcePlan, err := planServeBundleSource(s.stores.bundleWriter, bundle)
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
	validateSource := s.validateSource
	if validateSource == nil {
		validateSource = newBuilderProjectSourceValidator(candidateConfig.Config)
	}
	if err := validateSource(ctx, source, candidateCatalog); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	installedTriggerSubjects, err := candidateCatalog.InstalledCapabilitySubjects()
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if bundle.PackInventory == nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("candidate bundle effective pack inventory is required")
	}
	var baseSelection *packartifact.PreparedPlatformPackBaseSelection
	if s.platformPackBases != nil {
		baseSelection, err = s.platformPackBases.PrepareSelection(candidateBase)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("prepare candidate platform pack base generation: %w", err)
		}
	}
	packCandidate := bundlePackCandidate{
		catalog: candidateCatalog, generation: candidateCatalog.Generation(),
		installedSubjects: installedTriggerSubjects, inventoryDigest: bundle.PackInventory.Digest(),
		channelPlans: candidateChannelPlans, channelBindings: candidateChannelBindings,
		base: candidateBase, baseSelection: baseSelection, config: candidateConfig.Config,
	}
	if _, err := s.initStateStores(ctx, s.stores.schema, bundle); err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	bundleIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		return builderpkg.ProjectStatus{}, fmt.Errorf("derive project bundle identity: %w", err)
	}
	bundleSourceFact, err := persistServeBundleSourcePlan(ctx, s.stores.bundleWriter, bundleSourcePlan)
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
	currentConfig := s.cfg
	if candidateConfig.Config != nil {
		currentConfig = candidateConfig.Config
	}
	var workspaces workspace.Lifecycle
	var workspaceBackend cliapp.WorkspaceBackendSelection
	if s.newWorkspaces != nil {
		workspaces, workspaceBackend, err = s.newWorkspaces(s.stores.workspace, resolvedRoot, source, s.mountSources)
	} else {
		workspaceBackend, err = cliapp.DecideWorkspaceBackend(s.workspaceBackend, currentConfig, source)
		if err == nil {
			workspaces, err = cliapp.ConfiguredWorkspaceLifecycleForBackend(s.stores.workspace, currentConfig, resolvedRoot, source, s.mountSources, workspaceBackend)
		}
	}
	if err != nil {
		return builderpkg.ProjectStatus{}, err
	}
	if workspaces == nil {
		if !workspaceBackend.NoWorkspace {
			return builderpkg.ProjectStatus{}, fmt.Errorf("workspace lifecycle is not configured for backend %q; no lifecycle is only valid for canonical no-workspace decision", strings.TrimSpace(workspaceBackend.Backend))
		}
	} else {
		if err := configureWorkspaceDataProjection(workspaces, source, s.stores); err != nil {
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
	}

	if s.processWorkOwner == nil {
		return builderpkg.ProjectStatus{}, errors.New("served process work owner is required for project runtime replacement")
	}
	deps := s.stores.runtimeDeps()
	deps.Config = currentConfig
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
		NoticePresentation:      s.noticePresentation,
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
	status, err := s.replaceCurrentRuntimeWithSourceAndPacks(ctx, resolvedRoot, source, bundle, bundleSourceFact, bundleIdentity, newRT, newRT.WorkOccurrence(), &packCandidate)
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

type bundlePackCandidate struct {
	catalog           *providertriggers.CatalogSnapshot
	generation        triggergeneration.Generation
	installedSubjects []packs.Subject
	inventoryDigest   string
	channelPlans      []packs.SatisfactionPlan
	channelBindings   []packs.OutboundBindingPlan
	base              *packartifact.PlatformPackInventory
	baseSelection     *packartifact.PreparedPlatformPackBaseSelection
	config            *config.Config
}

type runtimeReplacementPublication interface {
	Publish() error
	Discard() error
	Withdraw(context.Context) error
}

type runtimeReplacementPhase uint8

const (
	runtimeReplacementPrepared runtimeReplacementPhase = iota
	runtimeReplacementPublished
	runtimeReplacementRollbackPrepared
	runtimeReplacementRollbackPublished
)

type pendingRuntimeReplacement struct {
	mu                     sync.Mutex
	publication            runtimeReplacementPublication
	phase                  runtimeReplacementPhase
	reconcilePublicIngress func(context.Context) error
	root                   string
	source                 semanticview.Source
	bundle                 *runtimecontracts.WorkflowContractBundle
	fact                   runtimecorrelation.BundleSourceFact
	identity               runtimecontracts.BundleIdentity
	runtime                *runtime.Runtime
	packCandidate          *bundlePackCandidate
	freeze                 *startupHandoffFreeze
	retainCurrentProject   bool
}

type runtimeProjectSnapshot struct {
	root             string
	source           semanticview.Source
	bundle           *runtimecontracts.WorkflowContractBundle
	runtime          *runtime.Runtime
	fact             runtimecorrelation.BundleSourceFact
	identity         runtimecontracts.BundleIdentity
	providerCatalog  *providertriggers.CatalogSnapshot
	channelPlans     []packs.SatisfactionPlan
	channelBindings  []packs.OutboundBindingPlan
	platformPackBase *packartifact.PlatformPackInventory
	config           *config.Config
}

type runtimeReplacementRollbackPhase uint8

const (
	runtimeReplacementRollbackPublication runtimeReplacementRollbackPhase = iota
	runtimeReplacementRollbackCandidate
	runtimeReplacementRollbackSourceSet
	runtimeReplacementRollbackPredecessor
	runtimeReplacementRollbackComplete
)

type pendingRuntimeReplacementRollback struct {
	mu                        sync.Mutex
	phase                     runtimeReplacementRollbackPhase
	publication               *pendingRuntimeReplacement
	candidate                 *runtime.Runtime
	manager                   *runtime.RuntimeContextManager
	predecessorContext        runtime.BundleContext
	predecessor               *runtime.Runtime
	predecessorProject        runtimeProjectSnapshot
	candidatePlan             runtimeagenttopology.SourceSetPlan
	previousPlan              runtimeagenttopology.SourceSetPlan
	topologyAttempt           sourceSetReplacementAttempt
	freeze                    *startupHandoffFreeze
	sourceSetRollbackRetained bool
	predecessorPublication    *pendingRuntimeReplacement
}

type pendingRuntimeSourceSetRollback struct {
	mu                     sync.Mutex
	manager                *runtime.RuntimeContextManager
	predecessorContext     runtime.BundleContext
	predecessor            *runtime.Runtime
	candidatePlan          runtimeagenttopology.SourceSetPlan
	previousPlan           runtimeagenttopology.SourceSetPlan
	topologyAttempt        sourceSetReplacementAttempt
	freeze                 *startupHandoffFreeze
	sourceSetRestored      bool
	survivorsRecovered     bool
	predecessorPublication *pendingRuntimeReplacement
}

type predecessorSurvivorCommitFailure struct {
	err error
}

func (e *predecessorSurvivorCommitFailure) Error() string {
	return fmt.Sprintf("restore predecessor source-set survivors: %v", e.err)
}

func (e *predecessorSurvivorCommitFailure) Unwrap() error {
	return e.err
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

func cloneBundlePackCandidate(candidate *bundlePackCandidate) *bundlePackCandidate {
	if candidate == nil {
		return nil
	}
	cloned := *candidate
	cloned.installedSubjects = packs.CloneSubjects(candidate.installedSubjects)
	cloned.channelPlans = append([]packs.SatisfactionPlan(nil), candidate.channelPlans...)
	cloned.channelBindings = append([]packs.OutboundBindingPlan(nil), candidate.channelBindings...)
	return &cloned
}

func (s *runtimeProjectSupervisor) restoreProjectSnapshot(snapshot runtimeProjectSnapshot) {
	s.mu.Lock()
	s.currentRoot = snapshot.root
	s.currentSource = snapshot.source
	s.currentBundle = snapshot.bundle
	s.currentRT = snapshot.runtime
	s.currentBundleSourceFact = snapshot.fact
	s.currentBundleIdentity = snapshot.identity
	s.providerTriggers = snapshot.providerCatalog
	s.channelPlans = append([]packs.SatisfactionPlan(nil), snapshot.channelPlans...)
	s.channelBindings = append([]packs.OutboundBindingPlan(nil), snapshot.channelBindings...)
	s.platformPackBase = snapshot.platformPackBase
	s.cfg = snapshot.config
	if s.platformPackBases != nil && snapshot.platformPackBase != nil {
		_ = s.platformPackBases.Select(snapshot.platformPackBase)
	}
	s.mu.Unlock()
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

func (s *runtimeProjectSupervisor) completePendingSourceSetSurvivors(
	ctx context.Context,
	manager *runtime.RuntimeContextManager,
) (bool, error) {
	if s == nil || s.processCapability == nil || manager == nil {
		return false, nil
	}
	current, exists, err := s.processCapability.CurrentSourceSet(ctx)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, errors.New("pending runtime source-set survivor transition requires an installed source set")
	}
	transition, err := manager.PreparePendingSourceSetTransition(ctx, current)
	if err != nil {
		return false, err
	}
	if transition == nil {
		return false, nil
	}
	if err := transition.Commit(ctx, s.processCapability); err != nil {
		return true, err
	}
	return true, nil
}

func (s *runtimeProjectSupervisor) completePendingRuntimeSourceSetRefresh(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	manager := s.runtimeContexts
	s.mu.RUnlock()
	_, err := s.completePendingSourceSetSurvivors(ctx, manager)
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
	transition, err := manager.PrepareSourceSetTransition(ctx, previous)
	if err != nil {
		return fmt.Errorf("prepare predecessor source-set survivor restoration: %w", err)
	}
	if err := s.restoreCommittedSourceSet(ctx, expected, previous, attempt); err != nil {
		if transition != nil {
			return errors.Join(err, transition.Abort())
		}
		return err
	}
	if transition == nil {
		return nil
	}
	if err := transition.Commit(ctx, s.processCapability); err != nil {
		return &predecessorSurvivorCommitFailure{err: err}
	}
	return nil
}

func (s *runtimeProjectSupervisor) retainPendingSourceSetRollback(
	manager *runtime.RuntimeContextManager,
	predecessorContext runtime.BundleContext,
	predecessor *runtime.Runtime,
	candidatePlan runtimeagenttopology.SourceSetPlan,
	previousPlan runtimeagenttopology.SourceSetPlan,
	topologyAttempt sourceSetReplacementAttempt,
	sourceSetRestored bool,
	freeze *startupHandoffFreeze,
) error {
	if s == nil || manager == nil || predecessor == nil {
		return errors.New("pending runtime source-set rollback requires supervisor, runtime context ownership, and predecessor")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingSourceSetRollback != nil {
		return errors.New("runtime source-set rollback is already pending")
	}
	s.pendingSourceSetRollback = &pendingRuntimeSourceSetRollback{
		manager:            manager,
		predecessorContext: predecessorContext,
		predecessor:        predecessor,
		candidatePlan:      candidatePlan,
		previousPlan:       previousPlan,
		topologyAttempt:    topologyAttempt,
		freeze:             freeze,
		sourceSetRestored:  sourceSetRestored,
	}
	return nil
}

func (s *runtimeProjectSupervisor) completePendingSourceSetRollback(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	pending := s.pendingSourceSetRollback
	s.mu.RUnlock()
	if pending == nil {
		return nil
	}
	pending.mu.Lock()
	defer pending.mu.Unlock()
	s.mu.RLock()
	current := s.pendingSourceSetRollback
	s.mu.RUnlock()
	if current != pending {
		return nil
	}
	if !pending.sourceSetRestored {
		err := s.restoreCommittedSourceSetAndSurvivors(
			context.Background(), pending.manager, pending.candidatePlan, pending.previousPlan, pending.topologyAttempt,
		)
		var survivorCommitFailure *predecessorSurvivorCommitFailure
		if err != nil && !errors.As(err, &survivorCommitFailure) {
			return fmt.Errorf("restore pending predecessor source set: %w", err)
		}
		pending.sourceSetRestored = true
		if err == nil {
			pending.survivorsRecovered = true
		}
	}
	if !pending.survivorsRecovered {
		recovered, err := s.completePendingSourceSetSurvivors(context.Background(), pending.manager)
		if err != nil {
			return fmt.Errorf("recover pending predecessor source-set survivors: %w", err)
		}
		if !recovered {
			return errors.New("failed predecessor survivor transition was not retained for recovery")
		}
		pending.survivorsRecovered = true
	}
	if pending.predecessorPublication != nil {
		s.mu.RLock()
		currentPublication := s.pendingReplacement
		s.mu.RUnlock()
		if currentPublication != pending.predecessorPublication {
			return errors.New("retained predecessor publication is no longer the pending runtime replacement")
		}
		if err := s.completePendingReplacement(); err != nil {
			return fmt.Errorf("resume retained predecessor publication: %w", err)
		}
	} else {
		s.mu.RLock()
		conflictingPublication := s.pendingReplacement
		s.mu.RUnlock()
		if conflictingPublication != nil {
			return errors.New("pending runtime source-set rollback conflicts with an unowned runtime replacement publication")
		}
		if err := s.restoreQuiescedPredecessorForRollback(ctx, pending); err != nil {
			return err
		}
	}
	s.mu.Lock()
	if s.pendingSourceSetRollback == pending {
		s.pendingSourceSetRollback = nil
	}
	s.mu.Unlock()
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
	if pending.phase == runtimeReplacementPrepared {
		if err := pending.publication.Publish(); err != nil {
			s.setReady(false)
			return fmt.Errorf("publish finalized runtime replacement: %w", err)
		}
		if !pending.retainCurrentProject && pending.packCandidate != nil {
			pending.packCandidate.baseSelection.Commit()
		}
		s.mu.Lock()
		if s.pendingReplacement != pending {
			s.mu.Unlock()
			return errors.New("pending runtime replacement changed before publication")
		}
		if !pending.retainCurrentProject {
			s.currentRoot = strings.TrimSpace(pending.root)
			s.currentSource = pending.source
			s.currentBundle = pending.bundle
			s.currentRT = pending.runtime
			s.currentBundleSourceFact = pending.fact
			s.currentBundleIdentity = pending.identity
			if pending.packCandidate != nil {
				s.providerTriggers = pending.packCandidate.catalog
				s.channelPlans = append([]packs.SatisfactionPlan(nil), pending.packCandidate.channelPlans...)
				s.channelBindings = append([]packs.OutboundBindingPlan(nil), pending.packCandidate.channelBindings...)
				s.platformPackBase = pending.packCandidate.base
				s.cfg = pending.packCandidate.config
			}
		}
		pending.reconcilePublicIngress = s.onRuntimePublished
		pending.phase = runtimeReplacementPublished
		s.mu.Unlock()
		pending.freeze.Release()
	}
	if pending.phase == runtimeReplacementRollbackPrepared || pending.phase == runtimeReplacementRollbackPublished {
		return errors.New("pending runtime replacement rollback is incomplete")
	}
	if pending.phase != runtimeReplacementPublished {
		return errors.New("pending runtime replacement has an invalid publication phase")
	}
	if pending.reconcilePublicIngress != nil {
		if err := pending.reconcilePublicIngress(s.runtimeStartContext(context.Background())); err != nil {
			s.setReady(false)
			return fmt.Errorf("reconcile published runtime public ingress: %w", err)
		}
	}
	s.mu.Lock()
	if s.pendingReplacement != pending {
		s.mu.Unlock()
		return errors.New("pending runtime replacement changed before ingress reconciliation")
	}
	s.pendingReplacement = nil
	s.mu.Unlock()
	s.setReady(true)
	return nil
}

func (s *runtimeProjectSupervisor) rollbackPendingReplacement(ctx context.Context, pending *pendingRuntimeReplacement) error {
	if s == nil || pending == nil {
		return errors.New("pending runtime replacement rollback requires supervisor and publication")
	}
	pending.mu.Lock()
	defer pending.mu.Unlock()
	var err error
	switch pending.phase {
	case runtimeReplacementPrepared:
		pending.phase = runtimeReplacementRollbackPrepared
		err = pending.publication.Discard()
	case runtimeReplacementRollbackPrepared:
		err = pending.publication.Discard()
	case runtimeReplacementPublished:
		pending.phase = runtimeReplacementRollbackPublished
		err = pending.publication.Withdraw(ctx)
	case runtimeReplacementRollbackPublished:
		err = pending.publication.Withdraw(ctx)
	default:
		return errors.New("pending runtime replacement has an invalid rollback phase")
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingReplacement != pending {
		return errors.New("pending runtime replacement changed before rollback completed")
	}
	s.pendingReplacement = nil
	return nil
}

func (s *runtimeProjectSupervisor) retainPendingReplacementRollback(pending *pendingRuntimeReplacementRollback) error {
	if s == nil || pending == nil || pending.publication == nil || pending.manager == nil || pending.predecessor == nil {
		return errors.New("pending runtime replacement rollback requires publication, runtime context ownership, and predecessor")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingReplacementRollback != nil {
		return errors.New("runtime replacement rollback is already pending")
	}
	s.pendingReplacementRollback = pending
	return nil
}

func (s *runtimeProjectSupervisor) completePendingReplacementRollback(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	pending := s.pendingReplacementRollback
	s.mu.RUnlock()
	if pending == nil {
		return nil
	}
	pending.mu.Lock()
	defer pending.mu.Unlock()
	s.mu.RLock()
	current := s.pendingReplacementRollback
	s.mu.RUnlock()
	if current != pending {
		return nil
	}

	for {
		switch pending.phase {
		case runtimeReplacementRollbackPublication:
			if err := s.rollbackPendingReplacement(ctx, pending.publication); err != nil {
				return fmt.Errorf("rollback retained runtime publication: %w", err)
			}
			pending.phase = runtimeReplacementRollbackCandidate

		case runtimeReplacementRollbackCandidate:
			if err := s.shutdownCurrentRuntimeWithOptions(ctx, pending.candidate, s.replacementShutdown); err != nil {
				return fmt.Errorf("shutdown retained replacement candidate: %w", err)
			}
			s.restoreProjectSnapshot(pending.predecessorProject)
			pending.phase = runtimeReplacementRollbackSourceSet

		case runtimeReplacementRollbackSourceSet:
			if pending.sourceSetRollbackRetained {
				if err := s.completePendingSourceSetRollback(ctx); err != nil {
					return fmt.Errorf("resume retained replacement source-set rollback: %w", err)
				}
				pending.phase = runtimeReplacementRollbackComplete
				continue
			}
			err := s.restoreCommittedSourceSetAndSurvivors(
				context.Background(), pending.manager, pending.candidatePlan, pending.previousPlan, pending.topologyAttempt,
			)
			var survivorCommitFailure *predecessorSurvivorCommitFailure
			if errors.As(err, &survivorCommitFailure) {
				if retainErr := s.retainPendingSourceSetRollback(
					pending.manager, pending.predecessorContext, pending.predecessor,
					pending.candidatePlan, pending.previousPlan, pending.topologyAttempt, true, pending.freeze,
				); retainErr != nil {
					return errors.Join(err, retainErr)
				}
				pending.sourceSetRollbackRetained = true
				continue
			}
			if err != nil {
				return err
			}
			pending.phase = runtimeReplacementRollbackPredecessor

		case runtimeReplacementRollbackPredecessor:
			if pending.predecessorPublication != nil {
				s.mu.RLock()
				currentPublication := s.pendingReplacement
				s.mu.RUnlock()
				if currentPublication != pending.predecessorPublication {
					return errors.New("retained predecessor publication is no longer the pending runtime replacement")
				}
				if err := s.completePendingReplacement(); err != nil {
					return fmt.Errorf("resume retained predecessor publication: %w", err)
				}
			} else if err := s.restoreQuiescedPredecessor(
				ctx, pending.manager, pending.predecessorContext, pending.predecessor, pending.freeze,
			); err != nil {
				s.mu.RLock()
				pending.predecessorPublication = s.pendingReplacement
				s.mu.RUnlock()
				return err
			}
			pending.phase = runtimeReplacementRollbackComplete

		case runtimeReplacementRollbackComplete:
			s.mu.Lock()
			if s.pendingReplacementRollback != pending {
				s.mu.Unlock()
				return errors.New("pending runtime replacement rollback changed before completion")
			}
			s.pendingReplacementRollback = nil
			s.mu.Unlock()
			return nil

		default:
			return errors.New("pending runtime replacement rollback has an invalid phase")
		}
	}
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
	return s.replaceCurrentRuntimeWithSourceAndPacks(ctx, resolvedRoot, source, bundle, fact, identity, newRT, workOwner, nil)
}

func (s *runtimeProjectSupervisor) replaceCurrentRuntimeWithSourceAndPacks(
	ctx context.Context,
	resolvedRoot string,
	source semanticview.Source,
	bundle *runtimecontracts.WorkflowContractBundle,
	fact runtimecorrelation.BundleSourceFact,
	identity runtimecontracts.BundleIdentity,
	newRT *runtime.Runtime,
	workOwner *worklifetime.RuntimeOccurrence,
	packCandidate *bundlePackCandidate,
) (builderpkg.ProjectStatus, error) {
	if err := s.completePendingReplacementRollback(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime replacement rollback before replacement: %w", err)
	}
	if err := s.completePendingSourceSetRollback(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime source-set rollback before replacement: %w", err)
	}
	if err := s.completePendingRuntimeSourceSetRefresh(ctx); err != nil {
		return s.CurrentProject(), fmt.Errorf("finalize pending runtime source-set refresh before replacement: %w", err)
	}
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
		if packCandidate != nil {
			contextDef.ProviderTriggerGeneration = packCandidate.generation
			contextDef.InstalledTriggerSubjects = packCandidate.installedSubjects
			contextDef.PackInventoryDigest = packCandidate.inventoryDigest
		}
		candidatePlan, err := replacementSourceSetPlan(manager, oldHash, contextDef)
		if err != nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("compile replacement source set: %w", err)
		}
		if err := manager.ValidateReplacement(oldHash, contextDef); err != nil {
			return builderpkg.ProjectStatus{}, err
		}
		oldContext, ok := manager.LookupBundleHash(oldHash)
		if !ok || oldContext == nil {
			return builderpkg.ProjectStatus{}, fmt.Errorf("predecessor runtime context %s is not loaded", oldHash)
		}
		oldContextDef := *oldContext
		s.mu.RLock()
		oldRoot := s.currentRoot
		oldSource := s.currentSource
		oldBundle := s.currentBundle
		oldRT := s.currentRT
		oldFact := s.currentBundleSourceFact
		oldIdentity := s.currentBundleIdentity
		oldProviderTriggers := s.providerTriggers
		oldChannelPlans := append([]packs.SatisfactionPlan(nil), s.channelPlans...)
		oldChannelBindings := append([]packs.OutboundBindingPlan(nil), s.channelBindings...)
		oldPlatformPackBase := s.platformPackBase
		oldConfig := s.cfg
		s.mu.RUnlock()
		predecessorProject := runtimeProjectSnapshot{
			root: oldRoot, source: oldSource, bundle: oldBundle, runtime: oldRT,
			fact: oldFact, identity: oldIdentity, providerCatalog: oldProviderTriggers,
			channelPlans: oldChannelPlans, channelBindings: oldChannelBindings,
			platformPackBase: oldPlatformPackBase, config: oldConfig,
		}
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
				retained := (s.pendingReplacement != nil && s.pendingReplacement.freeze == freeze) ||
					(s.pendingSourceSetRollback != nil && s.pendingSourceSetRollback.freeze == freeze) ||
					(s.pendingReplacementRollback != nil && s.pendingReplacementRollback.freeze == freeze)
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
		retainAndRestorePredecessor := func(cause error, abortPreparedTransition bool) error {
			retainErr := s.retainPendingSourceSetRollback(
				manager, oldContextDef, oldRT, candidatePlan, previousPlan, topologyAttempt, false, freeze,
			)
			var abortErr error
			if abortPreparedTransition && survivorTransition != nil {
				survivorTransitionSettled = true
				abortErr = survivorTransition.Abort()
			}
			if retainErr != nil {
				return errors.Join(cause, retainErr, abortErr)
			}
			recoveryErr := s.completePendingSourceSetRollback(context.Background())
			return errors.Join(cause, abortErr, recoveryErr)
		}
		restorePredecessor := func(cause error) error {
			return retainAndRestorePredecessor(cause, true)
		}
		restoreAfterSurvivorCommit := func(cause error) error {
			return retainAndRestorePredecessor(cause, false)
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
		publication, err = manager.PrepareBundleHashReplacementPublication(oldHash, contextDef)
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
			packCandidate: cloneBundlePackCandidate(packCandidate), freeze: freeze,
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
			rollback := &pendingRuntimeReplacementRollback{
				publication: pending, candidate: newRT, manager: manager,
				predecessorContext: oldContextDef, predecessor: oldRT, predecessorProject: predecessorProject,
				candidatePlan: candidatePlan, previousPlan: previousPlan, topologyAttempt: topologyAttempt, freeze: freeze,
			}
			if retainErr := s.retainPendingReplacementRollback(rollback); retainErr != nil {
				return s.CurrentProject(), errors.Join(err, retainErr)
			}
			recoveryErr := s.completePendingReplacementRollback(context.Background())
			return s.CurrentProject(), errors.Join(err, recoveryErr)
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
	if packCandidate != nil {
		packCandidate.baseSelection.Commit()
		s.mu.Lock()
		s.providerTriggers = packCandidate.catalog
		s.platformPackBase = packCandidate.base
		s.cfg = packCandidate.config
		s.mu.Unlock()
	}
	return s.attachCurrentRuntime(resolvedRoot, source, bundle, fact, identity, newRT), nil
}

func (s *runtimeProjectSupervisor) setReady(ready bool) {
	if s != nil && s.ready != nil {
		s.ready.Store(ready)
	}
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
	return s.restoreQuiescedPredecessorWithRollback(ctx, manager, predecessorContext, predecessor, freeze, nil)
}

func (s *runtimeProjectSupervisor) restoreQuiescedPredecessorForRollback(ctx context.Context, rollback *pendingRuntimeSourceSetRollback) error {
	if rollback == nil {
		return errors.New("pending runtime source-set rollback is required")
	}
	return s.restoreQuiescedPredecessorWithRollback(ctx, rollback.manager, rollback.predecessorContext, rollback.predecessor, rollback.freeze, rollback)
}

func (s *runtimeProjectSupervisor) restoreQuiescedPredecessorWithRollback(ctx context.Context, manager *runtime.RuntimeContextManager, predecessorContext runtime.BundleContext, predecessor *runtime.Runtime, freeze *startupHandoffFreeze, rollback *pendingRuntimeSourceSetRollback) error {
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
	publishProject := s.currentRT == predecessor
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
		freeze: freeze, retainCurrentProject: !publishProject,
	}
	s.mu.Lock()
	if s.pendingReplacement != nil {
		s.mu.Unlock()
		quiesceErr := s.quiesceCurrentRuntimeWithOptions(context.Background(), restored, s.replacementShutdown)
		return errors.Join(errors.New("runtime replacement transition is already pending"), quiesceErr)
	}
	s.pendingReplacement = pending
	if rollback != nil {
		rollback.predecessorPublication = pending
	}
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
