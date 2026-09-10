package serveapp

import (
	"context"
	"errors"
	"fmt"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
	runtimeregistration "github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/devscratch"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/versionmetadata"
	"github.com/division-sh/swarm/internal/yamlsource"
	"github.com/google/uuid"
)

const (
	serveAPIRoutes         = "/healthz /readyz /v1/rpc /v1/ws /webhooks/"
	serveMCPRoutes         = "/mcp /tools/"
	serveReadinessRoutes   = "/healthz"
	serveExitDataIntegrity = 78
)

var (
	buildStoresForServe               = buildStores
	projectRuntimePersistenceForServe = projectServeRuntimePersistence
)

type serveReadiness interface {
	Load() bool
	Store(bool)
}

type processOwnedDestructiveResetStore struct {
	capability runtimestartupownership.ProcessCapability
}

func (s processOwnedDestructiveResetStore) ApplyDestructiveResetCleanup(ctx context.Context, req runtimedestructivereset.CleanupRequest) (runtimedestructivereset.CleanupResult, error) {
	if s.capability == nil {
		return runtimedestructivereset.CleanupResult{}, errors.New("destructive reset requires the process topology capability")
	}
	if req.Result.DryRun || !req.Result.IncludeSourceArtifacts {
		return s.capability.ApplyDestructiveResetCleanup(ctx, req, nil)
	}
	current, exists, err := s.capability.CurrentSourceSet(ctx)
	if err != nil {
		return runtimedestructivereset.CleanupResult{}, err
	}
	if !exists {
		return runtimedestructivereset.CleanupResult{}, errors.New("destructive reset requires an installed source set")
	}
	empty, err := runtimeagenttopology.EmptySourceSetPlan()
	if err != nil {
		return runtimedestructivereset.CleanupResult{}, err
	}
	topology := &runtimeagenttopology.SourceSetCommitRequest{
		OperationID:      req.OperationID,
		ExpectedRevision: current.Revision,
		Plan:             empty,
	}
	return s.capability.ApplyDestructiveResetCleanup(ctx, req, topology)
}

type serveRuntimeBundle struct {
	module             runtimepipeline.WorkflowModule
	bundle             *runtimecontracts.WorkflowContractBundle
	source             semanticview.Source
	sourceProjection   *sourceartifact.RuntimeProjection
	sourceRoot         string
	platformSpecPath   string
	runningSpecPath    string
	bootIdentity       runtimecontracts.BundleIdentity
	sourceArtifactFact runtimecorrelation.SourceArtifactFact
	dbLoaded           bool
	cleanup            func() error
}

type serveRuntimeBundleContext struct {
	loaded                     serveRuntimeBundle
	stateStoreSummary          string
	sourceArtifactFact         runtimecorrelation.SourceArtifactFact
	bootIdentity               runtimecontracts.BundleIdentity
	workspaceBackend           cliapp.WorkspaceBackendSelection
	workspaces                 cliapp.ServeWorkspaceLifecycle
	validation                 runtime.WorkflowContractValidationResult
	runtime                    *runtime.Runtime
	providerTriggerGeneration  triggergeneration.Generation
	installedTriggerSubjects   []packs.Subject
	packInventoryDigest        string
	startupStandingTargets     []runtime.StandingTarget
	startupStandingActivations []runtime.StandingActivation
}

type serveRuntimeBundleContextRequest struct {
	ExecutionPosture       executionposture.Posture
	Ctx                    context.Context
	Stores                 serveRuntimePersistence
	Config                 *config.Config
	Loaded                 serveRuntimeBundle
	StateStoreSummary      string
	Options                cliapp.ServeOptions
	MountSources           cliapp.WorkspaceMountSources
	WorkspaceBackend       cliapp.WorkspaceBackendSelection
	Credentials            runtimecredentials.Store
	ManagedCredentials     runtimemanagedcredentials.Store
	ProviderCredentials    runtimecredentials.Store
	ProviderTriggerCatalog *providertriggers.CatalogSnapshot
	ChannelPlans           []packs.SatisfactionPlan
	ChannelBindings        []packs.OutboundBindingPlan
	BootStartedAt          time.Time
	BootProgress           func(runtime.BootProgressEvent)
	EnableToolGateway      bool
	ToolGatewayBinding     toolgateway.Binding
	UseStartupRecovery     bool
	RuntimeInstanceID      string
	DataProjectionRoot     string
	ProcessWorkOwner       *worklifetime.Process
	NoticePresentation     runtimetools.InformationalNoticePresentationSink
}

func (b serveRuntimeBundle) serveIdentityDetail() string {
	if b.dbLoaded && b.sourceArtifactFact.BundleHash() != "" {
		return b.sourceArtifactFact.BundleHash()
	}
	return strings.TrimSpace(b.bootIdentity.BundleHash)
}

func serveRuntimeBundleIdentitiesDetail(bundles []serveRuntimeBundle) string {
	parts := make([]string, 0, len(bundles))
	for _, bundle := range bundles {
		if detail := bundle.serveIdentityDetail(); detail != "" {
			parts = append(parts, detail)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func servePinnedBundleHashes(bundles []serveRuntimeBundle) []string {
	out := []string{}
	for _, bundle := range bundles {
		if bundle.dbLoaded {
			if hash := bundle.sourceArtifactFact.BundleHash(); hash != "" {
				out = append(out, hash)
			}
		}
	}
	sort.Strings(out)
	return out
}

func compileServeSourceSetPlan(contexts []serveRuntimeBundleContext) (runtimeagenttopology.SourceSetPlan, error) {
	sources := make([]runtimeagenttopology.SourceCoordinate, 0, len(contexts))
	agents := []runtimeagenttopology.DesiredAgent{}
	for _, contextDef := range contexts {
		bundleHash := contextDef.sourceArtifactFact.BundleHash()
		coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash}
		if contextDef.runtime == nil || contextDef.runtime.Manager == nil {
			return runtimeagenttopology.SourceSetPlan{}, fmt.Errorf("compile source topology %s: runtime manager is required", bundleHash)
		}
		desired, err := contextDef.runtime.Manager.CompileStaticTopologyDesiredAgents(contextDef.loaded.source, coordinate)
		if err != nil {
			return runtimeagenttopology.SourceSetPlan{}, fmt.Errorf("compile source topology %s: %w", bundleHash, err)
		}
		sources = append(sources, coordinate)
		agents = append(agents, desired...)
	}
	return runtimeagenttopology.NewSourceSetPlan(sources, agents)
}

func installServeSourceSet(ctx context.Context, capability runtimestartupownership.ProcessCapability, plan runtimeagenttopology.SourceSetPlan) error {
	current, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil {
		return err
	}
	if exists && current.Revision == plan.Revision {
		return nil
	}
	if !exists {
		_, err = capability.InstallCompleteSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
			OperationID: uuid.NewString(),
			Plan:        plan,
		})
		return err
	}
	_, err = capability.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
		OperationID:      uuid.NewString(),
		ExpectedRevision: current.Revision,
		Plan:             plan,
	})
	return err
}

func serveRuntimeStateStoreSummary(contexts []serveRuntimeBundleContext) string {
	seen := map[string]struct{}{}
	parts := []string{}
	for _, contextDef := range contexts {
		summary := strings.TrimSpace(contextDef.stateStoreSummary)
		if summary == "" {
			continue
		}
		if _, ok := seen[summary]; ok {
			continue
		}
		seen[summary] = struct{}{}
		parts = append(parts, summary)
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

func serveConfigLoadDetail(configDetail string, resolvedPaths cliapp.CLISourcePlatformSpecPaths, opts cliapp.ServeOptions) string {
	parts := []string{"config=" + strings.TrimSpace(configDetail)}
	hashes, _ := cliapp.ServeBundleHashes(opts)
	if len(hashes) == 1 {
		parts = append(parts, "bundle_hash="+hashes[0])
	} else if len(hashes) > 1 {
		parts = append(parts, "bundle_hashes="+strings.Join(hashes, ","))
	} else {
		parts = append(parts, "source="+filepath.Clean(resolvedPaths.SourceRoot))
	}
	return strings.Join(parts, " ")
}

func servePreCatalogPlatformSpecPath(resolvedPaths cliapp.CLISourcePlatformSpecPaths, opts cliapp.ServeOptions) (string, error) {
	hashes, err := cliapp.ServeBundleHashes(opts)
	if err != nil {
		return "", err
	}
	if len(hashes) > 0 {
		return cliapp.EmbeddedPlatformSpecPath()
	}
	return resolvedPaths.PlatformSpecPath, nil
}

type sourceArtifactReader interface {
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
}

func loadServeRuntimeBundles(ctx context.Context, repo string, artifacts sourceArtifactReader, resolvedPaths cliapp.CLISourcePlatformSpecPaths, opts cliapp.ServeOptions, packBases *packartifact.PlatformPackBaseGenerationOwner) ([]serveRuntimeBundle, error) {
	hashes, err := cliapp.ServeBundleHashes(opts)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		out := make([]serveRuntimeBundle, 0, len(hashes))
		runningPlatformSpecPath, err := cliapp.EmbeddedPlatformSpecPath()
		if err != nil {
			return nil, fmt.Errorf("resolve embedded platform spec for source artifact admission: %w", err)
		}
		for _, hash := range hashes {
			loaded, err := loadServeRuntimeBundleFromArtifact(ctx, repo, artifacts, hash, runningPlatformSpecPath, packBases, materializeServeSourceProjection)
			if err != nil {
				for _, prior := range out {
					if prior.cleanup != nil {
						_ = prior.cleanup()
					}
				}
				return nil, err
			}
			out = append(out, loaded)
		}
		return out, nil
	}
	loaded, err := loadServeRuntimeBundle(ctx, repo, artifacts, resolvedPaths, opts, packBases)
	if err != nil {
		return nil, err
	}
	return []serveRuntimeBundle{loaded}, nil
}

func loadServeRuntimeBundle(ctx context.Context, repo string, artifacts sourceArtifactReader, resolvedPaths cliapp.CLISourcePlatformSpecPaths, opts cliapp.ServeOptions, packBases *packartifact.PlatformPackBaseGenerationOwner) (serveRuntimeBundle, error) {
	hashes, err := cliapp.ServeBundleHashes(opts)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	if len(hashes) > 1 {
		return serveRuntimeBundle{}, fmt.Errorf("loadServeRuntimeBundle supports one bundle_hash; use loadServeRuntimeBundles for multi-context boot")
	}
	if len(hashes) == 1 {
		runningPlatformSpecPath, err := cliapp.EmbeddedPlatformSpecPath()
		if err != nil {
			return serveRuntimeBundle{}, fmt.Errorf("resolve embedded platform spec for source artifact admission: %w", err)
		}
		return loadServeRuntimeBundleFromArtifact(ctx, repo, artifacts, hashes[0], runningPlatformSpecPath, packBases, materializeServeSourceProjection)
	}
	sourceRoot, err := cliapp.NormalizeSourceRoot(resolvedPaths.SourceRoot)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	packBase, err := packBases.CurrentPlatformPackBase()
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	module, bundle, err := cliapp.NewSwarmWorkflowModuleWithPackBase(repo, sourceRoot, resolvedPaths.PlatformSpecPath, packBase)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	return materializeRuntimeBundle(module, bundle, sourceRoot, resolvedPaths.PlatformSpecPath)
}

func materializeRuntimeBundle(module runtimepipeline.WorkflowModule, bundle *runtimecontracts.WorkflowContractBundle, sourceRoot, platformSpecPath string) (serveRuntimeBundle, error) {
	bootIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("compute boot bundle identity: %w", err)
	}
	sourceProjection, err := sourceartifact.MaterializeRuntimeProjection(bundle.SourceArtifact)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("materialize runtime source projection: %w", err)
	}
	return serveRuntimeBundle{
		module:           module,
		bundle:           bundle,
		source:           semanticview.Wrap(bundle),
		sourceProjection: sourceProjection,
		sourceRoot:       sourceRoot,
		platformSpecPath: platformSpecPath,
		runningSpecPath:  platformSpecPath,
		bootIdentity:     bootIdentity,
		cleanup:          sourceProjection.Release,
	}, nil
}

func materializeServeSourceProjection(artifact *sourceartifact.AdmittedSourceArtifact, _ semanticview.Source) (*sourceartifact.RuntimeProjection, error) {
	return sourceartifact.MaterializeRuntimeProjection(artifact)
}

func loadServeRuntimeBundleFromArtifact(ctx context.Context, repo string, artifacts sourceArtifactReader, bundleHash, runningPlatformSpecPath string, packBases *packartifact.PlatformPackBaseGenerationOwner, materialize func(*sourceartifact.AdmittedSourceArtifact, semanticview.Source) (*sourceartifact.RuntimeProjection, error)) (serveRuntimeBundle, error) {
	if artifacts == nil {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: swarm serve --bundle-hash requires selected source artifact store")
	}
	if err := runtimecontracts.ValidateBundleHash(bundleHash); err != nil {
		return serveRuntimeBundle{}, err
	}
	record, err := artifacts.GetSourceArtifact(ctx, bundleHash)
	if errors.Is(err, sourceartifact.ErrNotFound) {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: bundle_hash %s is not present in source_artifacts", bundleHash)
	}
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	artifact, err := record.Decode()
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_DATA_INTEGRITY_ERROR: decode source artifact %s: %w", bundleHash, err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, strings.TrimSpace(runningPlatformSpecPath), runtimecontracts.WorkflowContractLoadOptions{
		PlatformPackBases:  packBases,
		AdmitPackInventory: packadmission.AdmitInventory,
	})
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	module, source, err := cliapp.NewSwarmWorkflowModuleForBundle(bundle)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	bootIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("compute DB-loaded boot bundle identity: %w", err)
	}
	sourceProjection, err := materialize(artifact, source)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("materialize DB-loaded runtime source projection: %w", err)
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		_ = sourceProjection.Release()
		return serveRuntimeBundle{}, fmt.Errorf("construct DB-loaded bundle source fact: %w", err)
	}
	return serveRuntimeBundle{
		module:             module,
		bundle:             bundle,
		source:             source,
		sourceProjection:   sourceProjection,
		sourceRoot:         ".",
		platformSpecPath:   strings.TrimSpace(runningPlatformSpecPath),
		runningSpecPath:    strings.TrimSpace(runningPlatformSpecPath),
		bootIdentity:       bootIdentity,
		sourceArtifactFact: fact,
		dbLoaded:           true,
		cleanup:            sourceProjection.Release,
	}, nil
}

func prepareLoadedServeSourceArtifact(ctx context.Context, persistence serveRuntimePersistence, loaded serveRuntimeBundle, dev bool) (runtimecorrelation.SourceArtifactFact, error) {
	if loaded.dbLoaded {
		if dev {
			return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("--bundle-hash is mutually exclusive with --dev")
		}
		fact := loaded.sourceArtifactFact
		if err := fact.Validate(); err != nil {
			return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("DB-loaded serve source artifact fact requires a canonical bundle_hash")
		}
		return fact, nil
	}
	if prepared := loaded.sourceArtifactFact; prepared.BundleHash() != "" {
		if err := prepared.Validate(); err != nil {
			return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("validate prepared serve bundle source fact: %w", err)
		}
		return prepared, nil
	}
	return prepareServeSourceArtifact(ctx, persistence.sourceWriter, loaded.bundle)
}

func buildServeRuntimeBundleContext(req serveRuntimeBundleContextRequest) (result serveRuntimeBundleContext, retErr error) {
	if !req.ExecutionPosture.Valid() {
		return serveRuntimeBundleContext{}, fmt.Errorf("command execution purpose is required")
	}
	loaded := req.Loaded
	stateStoreSummary := strings.TrimSpace(req.StateStoreSummary)
	if stateStoreSummary == "" {
		var err error
		stateStoreSummary, err = initializeStateStores(req.Ctx, req.Stores.schema, loaded.bundle)
		if err != nil {
			return serveRuntimeBundleContext{}, err
		}
	}
	sourceArtifactFact, err := prepareLoadedServeSourceArtifact(req.Ctx, req.Stores, loaded, req.Options.Dev)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("prepare bundle source: %w", err)
	}
	projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{
		WorkflowModule: loaded.module, SourceArtifactFact: sourceArtifactFact,
		ProviderTriggerCatalog: req.ProviderTriggerCatalog,
		ChannelPlans:           req.ChannelPlans,
	})
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("admit effective source projection: %w", err)
	}
	loaded.source = projection.Source()
	bootIdentity := loaded.bootIdentity
	bootIdentity.BundleHash = sourceArtifactFact.BundleHash()
	workspaces, err := cliapp.ConfiguredWorkspaceLifecycleForServe(req.Config, loaded.sourceProjection, loaded.source, req.MountSources, req.WorkspaceBackend)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("configure workspaces: %w", err)
	}
	if workspaces == nil && !req.WorkspaceBackend.NoWorkspace {
		return serveRuntimeBundleContext{}, fmt.Errorf("workspace lifecycle is not configured")
	}
	if workspaces != nil {
		if loaded.sourceProjection == nil || loaded.sourceProjection.BundleHash() != sourceArtifactFact.BundleHash() {
			return serveRuntimeBundleContext{}, fmt.Errorf("workspace runtime source projection does not match admitted bundle_hash")
		}
		if err := workspaces.BindSourceProjection(loaded.sourceProjection); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("bind workspace source projection: %w", err)
		}
		workspaceOwned := true
		defer func() {
			if workspaceOwned && retErr != nil {
				retErr = errors.Join(retErr, workspaces.ReleaseSourceProjection(context.WithoutCancel(req.Ctx)))
			}
		}()
		if err := configureWorkspaceDataProjection(workspaces, loaded.source, req.Stores, req.DataProjectionRoot); err != nil {
			return serveRuntimeBundleContext{}, err
		}
		if err := workspaces.ValidateSource(req.Ctx, loaded.source); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("validate workspaces: %w", err)
		}
		if err := workspaces.EnsurePrereqs(req.Ctx); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("prepare workspaces: %w", err)
		}
		if err := workspaces.EnsureSystemWorkspaces(req.Ctx); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("ensure system workspaces: %w", err)
		}
		defer func() {
			if retErr == nil {
				workspaceOwned = false
			}
		}()
	}
	posture := req.ExecutionPosture
	if !posture.Valid() {
		return serveRuntimeBundleContext{}, fmt.Errorf("command execution purpose is required")
	}
	validationOpts := runtime.DefaultWorkflowContractValidationOptions(req.Credentials, posture)
	validationOpts.AllowHarnessInputs = posture == executionposture.MockOnly
	validationOpts.AllowHarnessOutputs = posture == executionposture.MockOnly
	validationOpts.ModelAliases = req.Config.LLM.Models
	validationOpts.ManagedCredentials = req.ManagedCredentials
	validationOpts.ProviderCredentials = req.ProviderCredentials
	validationOpts.ProviderTriggerCatalog = req.ProviderTriggerCatalog
	validationOpts.ChannelPlans = req.ChannelPlans
	declaredChannelPublication, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(req.ChannelBindings)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("compile declared-only channel activation publication: %w", err)
	}
	validationOpts.ChannelActivationPublication = declaredChannelPublication
	profile, err := req.Config.LLMBackendProfile()
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("resolve llm backend profile for workflow validation: %w", err)
	}
	validationOpts.LLMProfile = profile
	validation, err := runtime.ValidateWorkflowContractSurface(req.Ctx, loaded.source, validationOpts)
	if err != nil {
		return serveRuntimeBundleContext{}, err
	}
	if runtimepipeline.SourceUsesArtifactRepoCommit(loaded.source) {
		if _, err := runtimepipeline.EnsureArtifactRepoRootWritable(""); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("artifact repo root startup validation failed: %w", err)
		}
	}
	runtimeDeps := req.Stores.runtimeDeps()
	runtimeDeps.Config = req.Config
	locatedScenarios, err := scenarioderivation.LoadDeclarations(loaded.bundle.SourceArtifact)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("load scenario derivation profiles: %w", err)
	}
	scenarioDeclarations := make([]scenarioderivation.Declaration, 0, len(locatedScenarios))
	for _, located := range locatedScenarios {
		scenarioDeclarations = append(scenarioDeclarations, located.Declaration)
	}
	runtimeDeps.Options = runtime.RuntimeOptions{
		ExecutionPosture:                 posture,
		SelfCheck:                        req.Options.SelfCheck,
		WorkflowModule:                   loaded.module,
		WorkspaceLifecycle:               workspaces,
		EnableToolGateway:                req.EnableToolGateway,
		ToolGatewayBinding:               req.ToolGatewayBinding,
		SourceArtifactFact:               sourceArtifactFact,
		RuntimeInstanceID:                strings.TrimSpace(req.RuntimeInstanceID),
		ProcessWorkOwner:                 req.ProcessWorkOwner,
		Credentials:                      req.Credentials,
		ManagedCredentials:               req.ManagedCredentials,
		ProviderCredentials:              req.ProviderCredentials,
		ProviderTriggerCatalog:           req.ProviderTriggerCatalog,
		NoticePresentation:               req.NoticePresentation,
		ChannelPlans:                     req.ChannelPlans,
		DeclaredChannelPublication:       declaredChannelPublication,
		ScenarioDeclarations:             scenarioDeclarations,
		BootStartedAt:                    req.BootStartedAt,
		BootProgress:                     req.BootProgress,
		SystemContainers:                 systemWorkspaceContainers(workspaces),
		DisablePersistentStartupRecovery: !req.UseStartupRecovery,
		TestEntityStateHook:              req.Options.TestEntityStateHook,
		TestWorkflowNodeHandlerStartHook: req.Options.TestWorkflowNodeHandlerStartHook,
		TestLifecycleProbe:               req.Options.TestLifecycleProbe,
		LLMRuntime:                       req.Options.TestLLMRuntime,
		TestOutboxSweeperConfig:          req.Options.TestOutboxSweeperConfig,
	}
	rt, err := runtime.NewRuntime(req.Ctx, runtimeDeps)
	if err != nil {
		return serveRuntimeBundleContext{}, err
	}
	runtimeOwned := true
	defer func() {
		if runtimeOwned {
			_ = rt.ShutdownWithOptions(runtime.ShutdownOptions{Grace: req.Options.ShutdownGrace})
		}
	}()
	if !rt.EffectiveSourceIdentity.Equal(projection.Identity()) {
		return serveRuntimeBundleContext{}, fmt.Errorf(
			"runtime effective source changed after prevalidation: validated=%s admitted=%s",
			projection.Identity().Digest(), rt.EffectiveSourceIdentity.Digest(),
		)
	}
	loaded.module, loaded.source, err = admittedRuntimeModuleAndSource(rt)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("retain admitted runtime source: %w", err)
	}
	installedTriggerSubjects, err := req.ProviderTriggerCatalog.InstalledCapabilitySubjects()
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("derive installed provider trigger subjects: %w", err)
	}
	if loaded.bundle == nil || loaded.bundle.PackInventory == nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("bundle-specific effective pack inventory is required")
	}
	runtimeOwned = false
	return serveRuntimeBundleContext{
		loaded:                    loaded,
		stateStoreSummary:         stateStoreSummary,
		sourceArtifactFact:        sourceArtifactFact,
		bootIdentity:              bootIdentity,
		workspaceBackend:          req.WorkspaceBackend,
		workspaces:                workspaces,
		validation:                validation,
		runtime:                   rt,
		providerTriggerGeneration: req.ProviderTriggerCatalog.Generation(),
		installedTriggerSubjects:  installedTriggerSubjects,
		packInventoryDigest:       loaded.bundle.PackInventory.Digest(),
	}, nil
}

func configureWorkspaceDataProjection(workspaces workspace.Lifecycle, source semanticview.Source, stores serveRuntimePersistence, root string) error {
	if workspaces == nil || source == nil {
		return nil
	}
	if source.DataProjectionRequired() && stores.data == nil {
		return fmt.Errorf("selected store does not expose durable data access projection")
	}
	if root == "" {
		root = runtimedataaccess.DefaultProjectionRoot()
	}
	materializer, err := runtimedataaccess.NewMaterializer(root, source, stores.data)
	if err != nil {
		return err
	}
	setter, ok := workspaces.(interface {
		SetDataProjectionProvider(runtimedataaccess.Provider)
	})
	if !ok {
		return fmt.Errorf("workspace lifecycle does not consume typed data projections")
	}
	setter.SetDataProjectionProvider(materializer)
	return nil
}

func buildForkChatSandboxLLMRuntimes(posture executionposture.Posture, cfg *config.Config, workspaces workspace.Resolver, binding toolgateway.Binding, providerCredentials runtimecredentials.Store, effectStore runtimeeffects.Store, completionStore runtimeeffects.CompletionStore, heartbeatStore runtimeeffects.CompletionHeartbeatStore, projector runtimeeffects.CompletionSpendProjector) (*runtimellm.AgentRuntimeSet, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
	}
	if !posture.Valid() {
		return nil, fmt.Errorf("command execution purpose is required")
	}
	registry := sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL)
	return runtimellm.NewAgentRuntimeSet(profile, runtimellm.RuntimeFactory{
		Cfg:                  cfg,
		Sessions:             registry,
		LiveSessions:         runtimellm.NewTransientLiveSessionAcquirer(registry),
		LockOwner:            "forkchat-sandbox",
		Workspaces:           workspaces,
		ToolGateway:          binding,
		Credentials:          providerCredentials,
		CompletionController: runtimeeffects.NewCompletionController(effectStore, completionStore, heartbeatStore, projector).WithExecutionPosture(posture),
	}, nil)
}

func serveOperatorChannelInterfaces(contexts []serveRuntimeBundleContext) ([]operatorchannel.InterfaceIdentity, error) {
	identities := []operatorchannel.InterfaceIdentity{}
	seen := map[string]struct{}{}
	for _, runtimeContext := range contexts {
		if runtimeContext.runtime == nil {
			return nil, fmt.Errorf("operator channel interface projection requires every loaded runtime context")
		}
		for _, plan := range runtimeContext.runtime.Options.ChannelPlans {
			identity, err := plan.InterfaceIdentity()
			if err != nil {
				return nil, err
			}
			if _, exists := seen[identity.Key()]; exists {
				continue
			}
			seen[identity.Key()] = struct{}{}
			identities = append(identities, identity)
		}
	}
	return identities, nil
}

func Run(ctx context.Context, invocationRoot cliapp.InvocationRoot, opts cliapp.ServeOptions) int {
	publicListener := opts.PublicWebhookListener
	defer func() {
		if publicListener != nil {
			_ = publicListener.Close()
		}
	}()
	repo := invocationRoot.Path()
	ctx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	bootStartedAt := time.Now().UTC()
	runtimeInstanceID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
	presenter := newServeLifecyclePresenter(opts)
	noticePresentation := newServeNoticePresentationSink(presenter)
	defer presenter.finish()
	shutdownGrace, err := runtimemanager.ResolveShutdownGrace(opts.ShutdownGrace)
	if err != nil {
		presenter.fail(1, "serve_admission", err)
		return 2
	}
	opts.ShutdownGrace = shutdownGrace
	if opts.NoFeed && !opts.Dev {
		presenter.fail(1, "serve_admission", fmt.Errorf("--no-feed requires --dev"))
		return 2
	}
	publicIngressMode, publicIngressEnabled, err := resolveServePublicIngressMode(opts)
	if err != nil {
		presenter.fail(1, "serve_admission", err)
		return 2
	}
	if publicIngressEnabled {
		if err := runtimepublicingress.ValidateConfiguration(publicIngressMode, opts.PublicWebhookBaseURL, opts.PublicWebhookListen); err != nil {
			presenter.fail(1, "serve_admission", err)
			return 2
		}
	}
	if publicListener != nil && publicIngressMode != runtimepublicingress.ModeExternalOrigin {
		presenter.fail(1, "serve_admission", fmt.Errorf("a bound public webhook listener requires external-origin mode"))
		return 2
	}
	if publicIngressMode == runtimepublicingress.ModeManagedQuickTunnel {
		if err := runtimepublicingress.PreflightCloudflared(ctx, ""); err != nil {
			presenter.fail(1, "serve_admission", err)
			return 3
		}
	}
	presenter.boot(1, "process_start", "ok", "")
	apiAuth, err := cliapp.ResolveServeAPIAuth(invocationRoot, opts)
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	if err := validateServeAPIAuthBinding(opts.APIListenAddr, apiAuth); err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	if apiAuth.UsesDefaultLoopbackToken() {
		presenter.recordDefaultAPITokenWarning()
	}
	resolvedPaths, err := cliapp.ResolveCLISourcePlatformSpecPaths(repo, cliapp.CLISourcePlatformSpecPathOptions{
		SourceRoot:       opts.SourceRoot,
		PlatformSpecPath: opts.PlatformSpecPath,
		ConfigPath:       opts.ConfigPath,
	})
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	cfgResult, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.ConfigPath,
		BackendOverride: opts.Backend,
	})
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	cfg := cfgResult.Config
	presenter.boot(2, "config_load", "ok", serveConfigLoadDetail(cfgResult.Detail(), resolvedPaths, opts))
	platformPackBase, err := cliapp.LoadConfiguredPlatformPackBase(repo, cfgResult)
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	platformPackBases, err := packartifact.NewPlatformPackBaseGenerationOwner(platformPackBase)
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	if !opts.Dev && !opts.LocalRun {
		if err := validateServeGatewayURLEnvForNonDev(); err != nil {
			presenter.fail(2, "serve_admission", err)
			return 3
		}
	}
	swarmDir, err := cliapp.ResolveServeContextRegistrationSwarmDir(invocationRoot, opts)
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	localState, err := cliapp.ResolveLocalRuntimeState(cliapp.LocalRuntimeStateOptions{
		RepoRoot:                repo,
		ResolvedPaths:           resolvedPaths,
		SwarmDir:                swarmDir,
		Config:                  cfg,
		StoreMode:               opts.StoreMode,
		StoreModeSet:            opts.StoreModeSet,
		DataSource:              opts.DataSource,
		CreateDefaultDataSource: true,
		EnforceLegacySQLite:     true,
	})
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	mountSources := localState.MountSources
	workspaceBackendPreference, err := cliapp.ResolveWorkspaceBackend(opts.WorkspaceBackend, opts.WorkspaceBackendSet, cfg)
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	storeSelection := localState.StoreSelection
	var scratchAuthority *devscratch.EpochAuthority
	var scratchRegistrationGrant devscratch.RegistrationGrant
	if opts.Dev && !opts.LocalRun {
		scratch, err := cliapp.ResolveDevScratch(localState)
		if err != nil {
			presenter.fail(2, "serve_admission", err)
			return 3
		}
		storeSelection = scratch.Selection
		scratchAuthority, err = devscratch.Acquire(scratch.Coordinate)
		if err != nil {
			presenter.fail(2, "serve_admission", err)
			return 3
		}
		defer func() {
			if scratchAuthority != nil {
				_ = scratchAuthority.AbortBeforeStoreOpen()
			}
		}()
		scratchRegistrationGrant, err = scratchAuthority.RegistrationGrant()
		if err != nil {
			presenter.fail(2, "serve_admission", err)
			return 3
		}
	}
	projectContextRegistration, err := cliapp.PrepareServeProjectContextRegistration(invocationRoot, opts, resolvedPaths, scratchRegistrationGrant)
	if err != nil {
		presenter.fail(2, "serve_admission", err)
		return 3
	}
	if scratchAuthority != nil {
		if err := scratchAuthority.PrepareFreshStore(); err != nil {
			presenter.fail(3, "db_connection", err)
			return 3
		}
	}
	managedCredentials, err := cliapp.BuildManagedCredentialStore()
	if err != nil {
		presenter.fail(5, "managed_credentials", err)
		return 1
	}
	providerCredentials, err := cliapp.BuildProviderCredentialStore()
	if err != nil {
		presenter.fail(5, "provider_credentials", err)
		return 1
	}
	credentials, err := cliapp.BuildCredentialStore()
	if err != nil {
		presenter.fail(5, "credentials", err)
		return 1
	}
	composition := runtimeCompositionRequest{
		Purpose: executionposture.Live, ProviderIngress: true,
		Repo: repo, Options: opts, Config: cfgResult, ResolvedPaths: resolvedPaths,
		LocalState: localState, SwarmDir: swarmDir, StoreSelection: storeSelection,
		WorkspacePreference: workspaceBackendPreference, MountSources: mountSources,
		PlatformPackBase: platformPackBase, PlatformPackBases: platformPackBases,
		APIAuth: apiAuth, PublicIngressMode: publicIngressMode, PublicIngressEnabled: publicIngressEnabled,
		Registration: projectContextRegistration, ScratchAuthority: scratchAuthority,
		Credentials: credentials, ManagedCredentials: managedCredentials, ProviderCredentials: providerCredentials,
		Presenter: presenter, NoticePresentation: noticePresentation, Cancel: cancelServe,
		BootStartedAt: bootStartedAt, RuntimeInstanceID: runtimeInstanceID,
	}
	scratchAuthority = nil // The common constructor now owns abort-or-joined-close.
	publicListener = nil   // Listener ownership follows the admitted composition request.
	return buildRuntimeComposition(ctx, composition)
}

// runtimeCompositionRequest contains command-admitted construction facts. It is
// internal to the composition root, not a public runtime mode/configuration API.
type runtimeCompositionRequest struct {
	Purpose              executionposture.Posture
	ProviderIngress      bool
	Repo                 string
	Options              cliapp.ServeOptions
	Config               cliapp.RuntimeConfigLoadResult
	ResolvedPaths        cliapp.CLISourcePlatformSpecPaths
	LocalState           cliapp.LocalRuntimeStateResolution
	SwarmDir             cliapp.CLISwarmDirResolution
	StoreSelection       storebackend.Selection
	WorkspacePreference  cliapp.WorkspaceBackendSelection
	MountSources         cliapp.WorkspaceMountSources
	PlatformPackBase     *packartifact.PlatformPackInventory
	PlatformPackBases    *packartifact.PlatformPackBaseGenerationOwner
	APIAuth              apiv1.AuthTokenResolution
	PublicIngressMode    string
	PublicIngressEnabled bool
	Registration         *cliapp.ServeProjectContextRegistration
	ScratchAuthority     *devscratch.EpochAuthority
	Credentials          runtimecredentials.Store
	ManagedCredentials   runtimemanagedcredentials.Store
	ProviderCredentials  runtimecredentials.Store
	Presenter            *serveLifecyclePresenter
	NoticePresentation   *serveNoticePresentationSink
	Cancel               context.CancelFunc
	BootStartedAt        time.Time
	RuntimeInstanceID    string
	OnReady              func(context.Context, string) error
	AdmittedBundle       *runtimecontracts.WorkflowContractBundle
	DataProjectionRoot   string
}

// buildRuntimeComposition owns the common acquisition/start/ready/close path for
// live serving and private test execution. Existing owners retain all semantics.
func buildRuntimeComposition(ctx context.Context, req runtimeCompositionRequest) int {
	repo, opts, cfgResult := req.Repo, req.Options, req.Config
	publicListener := opts.PublicWebhookListener
	defer func() {
		if publicListener != nil {
			_ = publicListener.Close()
		}
	}()
	cfg, resolvedPaths := cfgResult.Config, req.ResolvedPaths
	localState, swarmDir, storeSelection := req.LocalState, req.SwarmDir, req.StoreSelection
	mountSources, workspaceBackendPreference := req.MountSources, req.WorkspacePreference
	platformPackBase, platformPackBases := req.PlatformPackBase, req.PlatformPackBases
	apiAuth, projectContextRegistration := req.APIAuth, req.Registration
	publicIngressMode, publicIngressEnabled := req.PublicIngressMode, req.PublicIngressEnabled
	scratchAuthority := req.ScratchAuthority
	defer func() {
		if scratchAuthority != nil {
			if err := scratchAuthority.AbortBeforeStoreOpen(); err != nil {
				req.Presenter.cleanupFailure("scratch acquisition", err)
			}
		}
	}()
	presenter, noticePresentation, cancelServe := req.Presenter, req.NoticePresentation, req.Cancel
	bootStartedAt, runtimeInstanceID := req.BootStartedAt, req.RuntimeInstanceID
	if !req.Purpose.Valid() {
		presenter.fail(3, "runtime_composition", fmt.Errorf("command execution purpose is required"))
		return 2
	}
	stores, err := buildStoresForServe(ctx, storeSelection, cfg)
	if err != nil {
		var acquisitionErr *runtimestartupownership.AcquisitionError
		if errors.As(err, &acquisitionErr) {
			presenter.fail(3, "startup_ownership_lease", serveOwnershipAcquisitionError(err))
			return 3
		}
		presenter.fail(3, "db_connection", err)
		return 1
	}
	storeLifetime := selectedStoreLifecycle(stores)
	if scratchAuthority != nil {
		lifetime, bindErr := scratchAuthority.BindOpenedStore(stores)
		if bindErr != nil {
			_ = stores.CloseUnactivated()
			presenter.fail(3, "db_connection", bindErr)
			return 1
		}
		storeLifetime = lifetime
		scratchAuthority = nil
	}
	presenter.recordStore(storeSelection)
	runtimePersistence := projectRuntimePersistenceForServe(stores)
	closeUnactivatedStore := true
	defer func() {
		if !closeUnactivatedStore {
			return
		}
		if err := storeLifetime.CloseUnactivated(); err != nil {
			presenter.cleanupFailure("store shutdown", err)
		}
	}()
	preCatalogPlatformSpecPath, err := servePreCatalogPlatformSpecPath(resolvedPaths, opts)
	if err != nil {
		presenter.fail(4, "bundle_load", err)
		return 1
	}
	if _, err := initializeServePlatformStateStores(ctx, runtimePersistence.schema, preCatalogPlatformSpecPath); err != nil {
		presenter.fail(4, "bundle_load", err)
		return 1
	}
	var loadedBundles []serveRuntimeBundle
	cleanupLoadedSourceArtifacts := func() error {
		var cleanupErr error
		for _, loaded := range loadedBundles {
			if loaded.cleanup != nil {
				cleanupErr = errors.Join(cleanupErr, loaded.cleanup())
			}
		}
		return cleanupErr
	}
	processWorkOwner := worklifetime.NewProcess()
	selectedLifecycle, err := activateServeLifecycle(storeLifetime, processWorkOwner)
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	closeUnactivatedStore = false
	var runtimeContexts []serveRuntimeBundleContext
	var runtimeRequests []serveRuntimeBundleContextRequest
	var runtimeContextManager *runtime.RuntimeContextManager
	var supervisor *processLifecycleSupervisor
	var resetRecovery *runtimedestructivereset.PendingRecovery
	var workspaces cliapp.ServeWorkspaceLifecycle
	var processCapability runtimestartupownership.ProcessCapability
	cancelOwnershipWatch := func() {}
	var apiServer, mcpServer *http.Server
	var apiListener, mcpListener net.Listener
	httpServersStarted := false
	var publicExposure *runtimepublicingress.Controller
	var storyFollower *serveAuthorActivityFollower
	defer func() {
		cancelServe()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), opts.ShutdownGrace)
		defer cancelShutdown()
		deadline, _ := shutdownCtx.Deadline()
		if storyFollower != nil {
			storyFollower.StopAndWait()
		}
		var shutdownErr error
		if publicExposure != nil {
			shutdownErr = publicExposure.Stop(shutdownCtx)
		}
		shutdownErr = errors.Join(shutdownErr, shutdownHTTPServer(shutdownCtx, "api", apiServer))
		shutdownErr = errors.Join(shutdownErr, shutdownHTTPServer(shutdownCtx, "mcp", mcpServer))
		if !httpServersStarted {
			// Before Serve owns the listeners, startup failure still needs to
			// release them. Active listeners belong exclusively to HTTP shutdown.
			if apiListener != nil {
				shutdownErr = errors.Join(shutdownErr, apiListener.Close())
			}
			if mcpListener != nil {
				shutdownErr = errors.Join(shutdownErr, mcpListener.Close())
			}
		}
		shutdownErr = errors.Join(shutdownErr, resetRecovery.Close(shutdownCtx))
		if supervisor != nil {
			shutdownErr = errors.Join(shutdownErr, supervisor.ShutdownProcessWithOptions(context.Background(), runtime.ShutdownOptions{Grace: remainingServeShutdownGrace(opts.ShutdownGrace, deadline)}))
		} else {
			if len(runtimeContexts) > 1 {
				shutdownErr = errors.Join(shutdownErr, closeAdditionalServeRuntimeContexts(context.Background(), runtimeContexts[1:], runtimeContextManager, opts, deadline))
			}
			if len(runtimeContexts) > 0 && runtimeContexts[0].runtime != nil {
				joinErr := runtimeContexts[0].runtime.ShutdownWithOptions(runtime.ShutdownOptions{Grace: remainingServeShutdownGrace(opts.ShutdownGrace, deadline)})
				shutdownErr = errors.Join(shutdownErr, joinErr)
				if joinErr == nil && runtimeContexts[0].workspaces != nil {
					shutdownErr = errors.Join(shutdownErr, runtimeContexts[0].workspaces.ReleaseSourceProjection(context.Background()))
				}
			}
		}
		if shutdownErr == nil {
			shutdownErr = cleanupLoadedSourceArtifacts()
		}
		cancelOwnershipWatch()
		presenter.shutdown(selectedLifecycle.Finalize(shutdownCtx, shutdownErr))
	}()
	processCapability, err = stores.StartupOwnership().AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "serve:" + runtimeInstanceID, BootID: uuid.NewString(), RuntimeInstanceID: runtimeInstanceID,
	})
	if err != nil {
		presenter.fail(5, "startup_ownership_lease", serveOwnershipAcquisitionError(err))
		return 3
	}
	if err := selectedLifecycle.SetProcessCapability(processCapability); err != nil {
		presenter.fail(5, "startup_ownership_lease", err)
		return 3
	}
	processAuthority, err := processCapability.Evidence()
	if err != nil {
		presenter.fail(5, "startup_ownership_lease", err)
		return 3
	}
	if processAuthority.AcquisitionKind == runtimestartupownership.AcquisitionCrashTakeover {
		presenter.recordRecoveredPreviousSession()
	}
	ownershipWatchCtx, watchCancel := context.WithCancel(ctx)
	cancelOwnershipWatch = watchCancel
	ownershipLoss, err := startServeOwnershipWatch(ownershipWatchCtx, processWorkOwner, processCapability, presenter, cancelServe)
	if err != nil {
		presenter.fail(5, "startup_ownership_lease", err)
		return 3
	}
	channelOnboardingStore := stores.ChannelOnboarding()
	runtimeContextManager, err = runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	ready := runtimepublicingress.NewReadinessOwner(publicIngressEnabled)
	supervisor = newProcessLifecycleSupervisor(ready, nil)
	supervisor.SetProcessCapability(processCapability)
	supervisor.runtimeContexts = runtimeContextManager
	supervisor.resetStartup = true
	supervisor.resetContainerRuntime = workspace.NewDockerManager()
	supervisor.shutdownOptions = runtime.ShutdownOptions{Grace: opts.ShutdownGrace}
	if coordinator := buildSelectedResetCoordinator(stores, processCapability, supervisor); coordinator != nil {
		resetRecovery, err = coordinator.RecoverPending(ctx)
		if err != nil {
			presenter.fail(5, "pending_reset_recovery", err)
			return 3
		}
	}
	if resetRecovery != nil {
		loadedBundles, err = loadServeRecoveredResetSources(ctx, repo, stores.SourceArtifactStore(), resetRecovery, platformPackBases,
			func(artifact *sourceartifact.AdmittedSourceArtifact, source semanticview.Source) (*sourceartifact.RuntimeProjection, error) {
				backend, err := cliapp.DecideWorkspaceBackend(req.Purpose, workspaceBackendPreference, cfg, source)
				if err != nil {
					return nil, err
				}
				return supervisor.materializeResetProjection(ctx, artifact, backend.Backend == "docker")
			})
	} else {
		supervisor.resetStartup = false
		loadedBundles, err = loadRuntimeCompositionBundles(ctx, req, stores.SourceArtifactStore())
	}
	if err != nil {
		detail := err.Error()
		if _, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
			detail = cliapp.FormatCLIAPIError(err)
		}
		presenter.fail(4, "bundle_load", errors.New(detail))
		return 1
	}
	if len(loadedBundles) == 0 && resetRecovery == nil {
		presenter.fail(4, "bundle_load", errors.New("no bundle contexts loaded"))
		return 1
	}
	loadedBundle := serveRuntimeBundle{platformSpecPath: preCatalogPlatformSpecPath, runningSpecPath: preCatalogPlatformSpecPath}
	if len(loadedBundles) > 0 {
		loadedBundle = loadedBundles[0]
	}
	source := loadedBundle.source
	resolvedPlatformSpecPath := loadedBundle.platformSpecPath
	if len(loadedBundles) == 0 {
		presenter.boot(4, "bundle_load", "ok", "reset retained an empty source set; execution remains unloaded")
	} else {
		presenter.boot(4, "bundle_load", "ok", serveBootBundleLoadDetail(serveRuntimeBundleIdentitiesDetail(loadedBundles), source))
	}
	if len(loadedBundles) > 0 {
		_, err = cliapp.DecideWorkspaceBackend(req.Purpose, workspaceBackendPreference, cfg, source)
	}
	if err != nil {
		presenter.failWithDiagnostic(5, "runtime_context", err, func(out io.Writer) bool {
			cliapp.WriteWorkspaceBackendDecisionFailure(out, "serve", err)
			return true
		})
		return 3
	}
	managedCredentialStore, providerCredentialStore := req.ManagedCredentials, req.ProviderCredentials
	providerCredentialOwner, err := runtimecredentials.NewSnapshotOwner(providerCredentialStore)
	if err != nil {
		presenter.fail(5, "provider_credentials", err)
		return 1
	}
	bundlePackLoads := make([]cliapp.BundlePackRuntimeLoad, len(loadedBundles))
	for i := range loadedBundles {
		bundlePackLoads[i], err = cliapp.LoadBundlePackRuntime(ctx, cfgResult, loadedBundles[i].bundle, providerCredentialStore, managedCredentialStore)
		if err != nil {
			presenter.fail(5, "pack_inventory", fmt.Errorf("bundle %s: %w", loadedBundles[i].serveIdentityDetail(), err))
			return 1
		}
	}
	if len(bundlePackLoads) > 0 && req.Purpose == executionposture.Live && cliapp.ShouldRunServeLocalClaudeCLIPreflight(opts) {
		primaryPackLoad := bundlePackLoads[0]
		preflight := cliapp.RunServeLocalClaudeCLIPreflight(ctx, repo, opts, cfg, resolvedPaths, workspaceBackendPreference, mountSources, platformPackBase, primaryPackLoad.ProviderTriggers.Loaded, primaryPackLoad.ProviderTriggers.Catalog, providerCredentialStore, primaryPackLoad.Channels, loadedBundle.source)
		if preflight.HasBlockers() {
			detail := preflight.BlockerSummary()
			presenter.failWithDiagnostic(5, "local_preflight", errors.New(detail), func(out io.Writer) bool {
				cliapp.WriteLocalPreflightText(out, preflight)
				return true
			})
			return 3
		}
	}
	if err := validateServeMultiContextToolGatewayAdmission(cfg, loadedBundles); err != nil {
		presenter.fail(5, "runtime_context", err)
		return 3
	}
	stateStoreSummaries := make([]string, len(loadedBundles))
	summaries, err := initializeLoadedServeRuntimeStateStores(ctx, runtimePersistence.schema, loadedBundles)
	if err != nil {
		presenter.fail(4, "state_store_schema", err)
		return 1
	}
	stateStoreSummaries = summaries
	for i := range loadedBundles {
		fact, err := prepareLoadedServeSourceArtifact(ctx, runtimePersistence, loadedBundles[i], opts.Dev)
		if err != nil {
			presenter.fail(4, "source_artifact", err)
			return 1
		}
		loadedBundles[i].sourceArtifactFact = fact
	}
	if len(loadedBundles) > 0 {
		loadedBundle = loadedBundles[0]
	}
	pinnedBundleHashes := servePinnedBundleHashes(loadedBundles)
	credentialStore := req.Credentials
	apiListener, err = cliapp.ListenServeHTTPListener("api", opts.APIListenAddr)
	if err != nil {
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}
	if opts.TestAPIListenerBound != nil {
		opts.TestAPIListenerBound(apiListener.Addr())
	}
	mcpListener, err = cliapp.ListenServeHTTPListener("mcp", opts.MCPListenAddr)
	if err != nil {
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}
	toolGatewayBinding, err := createServeToolGatewayBinding(mcpListener.Addr())
	if err != nil {
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}

	workspaceLabels := serveLifecycleWorkspaceLabels(loadedBundles)
	for i, loaded := range loadedBundles {
		contextToolGatewayBinding := toolgateway.Binding{}
		if i == 0 {
			contextToolGatewayBinding = toolGatewayBinding
		}
		workspaceBackend, err := cliapp.DecideWorkspaceBackend(req.Purpose, workspaceBackendPreference, cfg, loaded.source)
		if err != nil {
			presenter.failWithDiagnostic(5, "runtime_context", err, func(out io.Writer) bool {
				cliapp.WriteWorkspaceBackendDecisionFailure(out, "serve", err)
				return true
			})
			return 3
		}
		presenter.recordWorkspace(workspaceLabels[i], workspaceBackend)
		var bootProgress func(runtime.BootProgressEvent)
		if i == 0 {
			bootProgress = presenter.runtimeSink()
		}
		packLoad := bundlePackLoads[i]
		request := serveRuntimeBundleContextRequest{
			ExecutionPosture:       req.Purpose,
			DataProjectionRoot:     req.DataProjectionRoot,
			Ctx:                    ctx,
			Stores:                 runtimePersistence,
			Config:                 cfg,
			Loaded:                 loaded,
			StateStoreSummary:      serveStateStoreSummaryAt(stateStoreSummaries, i),
			Options:                opts,
			MountSources:           mountSources,
			WorkspaceBackend:       workspaceBackend,
			Credentials:            credentialStore,
			ManagedCredentials:     managedCredentialStore,
			ProviderCredentials:    providerCredentialStore,
			ProviderTriggerCatalog: packLoad.ProviderTriggers.Catalog,
			ChannelPlans:           packLoad.Channels.Plans,
			ChannelBindings:        packLoad.Channels.Bindings,
			BootStartedAt:          bootStartedAt,
			BootProgress:           bootProgress,
			EnableToolGateway:      i == 0,
			ToolGatewayBinding:     contextToolGatewayBinding,
			UseStartupRecovery:     len(loadedBundles) == 1,
			RuntimeInstanceID:      runtimeInstanceID,
			ProcessWorkOwner:       processWorkOwner,
			NoticePresentation:     noticePresentation,
		}
		contextDef, err := buildServeRuntimeBundleContext(request)
		if err != nil {
			presenter.failWithDiagnostic(5, "runtime_context", err, func(out io.Writer) bool {
				return cliapp.WriteWorkspacePrerequisiteFailure(out, "serve", err)
			})
			return 1
		}
		runtimeContexts = append(runtimeContexts, contextDef)
		runtimeRequests = append(runtimeRequests, request)
		supervisor.resetContexts = runtimeContexts
		supervisor.resetRequests = runtimeRequests
	}
	primaryContext := serveRuntimeBundleContext{loaded: loadedBundle}
	if len(runtimeContexts) > 0 {
		primaryContext = runtimeContexts[0]
	}
	processSourceSet, err := compileServeSourceSetPlan(runtimeContexts)
	if err != nil {
		presenter.fail(5, "startup_topology", err)
		return 3
	}
	if resetRecovery != nil {
		expected, expectedErr := resetRecovery.SourceSet()
		if expectedErr != nil || expected.Revision != processSourceSet.Revision {
			presenter.fail(5, "pending_reset_recovery", errors.Join(expectedErr, errors.New("reconstructed reset sources differ from the admitted snapshot")))
			return 3
		}
	}
	if err := installServeSourceSet(ctx, processCapability, processSourceSet); err != nil {
		presenter.fail(5, "startup_topology", err)
		return 3
	}
	for i := range runtimeContexts {
		grant, grantErr := processCapability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
			BundleHash:        runtimeContexts[i].sourceArtifactFact.BundleHash(),
			RuntimeInstanceID: runtimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: processSourceSet.Revision,
		})
		if grantErr == nil {
			grantErr = runtimeContexts[i].runtime.InstallStartupGrant(grant)
		}
		if grantErr != nil {
			presenter.fail(5, "startup_ownership_lease", grantErr)
			return 3
		}
	}
	source = primaryContext.loaded.source
	workspaces = primaryContext.workspaces
	rt := primaryContext.runtime
	posture := req.Purpose
	channelInterfaces, err := serveOperatorChannelInterfaces(runtimeContexts)
	if err != nil {
		presenter.fail(5, "operator_channel", err)
		return 1
	}
	proofStore, err := operatorchannel.NewFileProofStore(swarmDir.Path)
	if err != nil {
		presenter.fail(5, "operator_channel", err)
		return 1
	}
	operatorChannels, err := operatorchannel.NewService(stores.OperatorChannels(), proofStore, providerCredentialOwner, channelInterfaces, runtimeInstanceID)
	if err != nil {
		presenter.fail(5, "operator_channel", err)
		return 1
	}
	operatorPrincipal, recoveredBindings, err := operatorChannels.Bootstrap(ctx, bootStartedAt)
	if err != nil {
		presenter.fail(5, "operator_channel", err)
		return 1
	}
	for _, binding := range recoveredBindings {
		presenter.recordOperatorChannelProofReuse(binding)
	}
	bootReport := primaryContext.validation.BootReport
	stateStoreSummary := serveRuntimeStateStoreSummary(runtimeContexts)
	preflightContexts, err := plannedServeRuntimeContexts(runtimeContexts)
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	if err := runtime.ValidateRuntimeContextSet(preflightContexts...); err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	if opts.AbandonActiveRuns {
		result, err := stores.RunQuiescence().ApplyServeAbandonActiveRunQuiescence(ctx, time.Now().UTC())
		if err != nil {
			presenter.fail(5, "run_quiescence", err)
			return 3
		}
		presenter.recordAbandonedWork(len(result.Runs), len(result.Deliveries), result.PipelineReceiptCount)
	}
	if recovery, available := stores.StartupRecovery(); available {
		if exitCode := runServeSourceArtifactStartupRecovery(ctx, recovery, stores.SourceArtifactStore(), presenter); exitCode != 0 {
			return exitCode
		}
	}
	if err := enforceServePinnedBundleAdmissionForHashes(ctx, stores.RunBundleAvailability(), serveRuntimeBundleIdentitiesDetail(loadedBundles), pinnedBundleHashes); err != nil {
		presenter.fail(5, "pinned_bundle_admission", err)
		return 3
	}
	if rt != nil {
		standingReconciliations, err := reconcileServeStandingServices(ctx, rt.Pipeline, runtimeContexts)
		if err != nil {
			presenter.fail(5, "runtime_context", err)
			return 1
		}
		for i := range runtimeContexts {
			targets, activations, err := reconcileServeRuntimeStandingTargets(runtimeContexts[i].runtime, standingReconciliations)
			if err != nil {
				presenter.fail(5, "runtime_context", err)
				return 1
			}
			runtimeContexts[i].startupStandingTargets = targets
			runtimeContexts[i].startupStandingActivations = activations
		}
	}
	supervisor.currentRT = rt
	supervisor.SetRuntimeContextManager(runtimeContextManager, primaryContext.sourceArtifactFact)
	supervisor.resetRequests = runtimeRequests
	supervisor.resetContexts = runtimeContexts
	supervisor.resetGeneration = 1
	apiCapabilityRequest := selectedAPICapabilityRequest{
		RepoRoot:                repo,
		PlatformSpecPath:        resolvedPlatformSpecPath,
		RunningPlatformSpecPath: strings.TrimSpace(loadedBundle.runningSpecPath),
		LoadedBundle:            loadedBundle,
		Source:                  source,
		Config:                  cfg,
		Workspaces:              workspaces,
		Credentials:             credentialStore,
		ManagedCredentials:      managedCredentialStore,
		ProviderCredentials:     providerCredentialStore,
		ExecutionPosture:        posture,
		ProcessCapability:       processCapability,
		PlatformPackBases:       platformPackBases,
		RuntimeContextManager:   runtimeContextManager,
		RuntimeSupervisor:       supervisor,
		NoticePresentation:      noticePresentation,
	}
	apiStoreCaps, err := buildSelectedAPICapabilities(stores, apiCapabilityRequest)
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	storeDeps := stores.RuntimeDeps()
	idempotency := stores.Idempotency()
	readyFn := func() bool { return ready.Load() }
	buildExecution := func(primary serveRuntimeBundleContext) (map[string]apiv1.MethodHandler, error) {
		return buildServeRuntimeExecution(stores, apiCapabilityRequest, primary, toolGatewayBinding)
	}
	supervisor.resetBuildExecution = buildExecution
	if rt != nil {
		supervisor.execution, err = buildExecution(primaryContext)
	}
	if err != nil {
		presenter.fail(5, "runtime_execution", err)
		return 1
	}
	runtimeIdentity := apiv1.RuntimeIdentityResult{
		RuntimeInstanceID:   runtimeInstanceID,
		StartedAt:           bootStartedAt.Format(time.RFC3339Nano),
		APIVersion:          "v1",
		SupportedTransports: []string{"tcp"},
	}
	credentialWriter, err := channelonboarding.NewCredentialWriter(providerCredentialStore)
	if err != nil {
		presenter.fail(20, "channel_onboarding", err)
		return 1
	}
	confirmationEffects, ok := stores.Effects().(channelConfirmationEffectStore)
	if !ok {
		presenter.fail(20, "channel_onboarding", fmt.Errorf("selected store does not provide channel confirmation outcome ownership"))
		return 1
	}
	confirmationDispatcher, err := newServeChannelConfirmationDispatcher(confirmationEffects, providerCredentialOwner, posture, runtimeInstanceID, nil)
	if err != nil {
		presenter.fail(20, "channel_onboarding", err)
		return 1
	}
	channelActivationRefresher := &serveChannelActivationRefresher{
		manager: runtimeContextManager, store: channelOnboardingStore, identities: operatorChannels,
		credentials: providerCredentialOwner, ingress: ready,
	}
	if !publicIngressEnabled {
		channelActivationRefresher.preflight = func(_ context.Context, intent servePrebindingActivation) error {
			return rejectWebhookPrebindingWithoutPublicIngress(serveChannelActivationSnapshot{Prebinding: []servePrebindingActivation{intent}})
		}
		channelActivationRefresher.reconcile = func(refreshCtx context.Context) error {
			snapshot, snapshotErr := compileServeChannelActivationSnapshot(refreshCtx, runtimeContextManager, channelOnboardingStore, operatorChannels, providerCredentialOwner)
			if snapshotErr != nil {
				return snapshotErr
			}
			return rejectWebhookPrebindingWithoutPublicIngress(snapshot)
		}
	}
	connectedChannelReadiness := &serveConnectedChannelReadiness{
		manager: runtimeContextManager, store: channelOnboardingStore, identities: operatorChannels,
		credentials: providerCredentialOwner, effects: confirmationEffects, ingress: ready,
	}
	channelOnboarding, err := channelonboarding.NewService(channelonboarding.ServiceOptions{
		Store: channelOnboardingStore, Identities: operatorChannels, Credentials: credentialWriter,
		Catalog: func() (*channelonboarding.CandidateCatalog, error) {
			return serveChannelOnboardingCatalog(runtimeContextManager)
		},
		Activations: channelActivationRefresher, Confirmation: confirmationDispatcher, Readiness: connectedChannelReadiness,
		Now: opts.TestChannelOnboardingNow, TestBarrier: opts.TestChannelOnboardingBarrier,
	})
	if err != nil {
		presenter.fail(20, "channel_onboarding", err)
		return 1
	}
	channelDestructive, err := channelonboarding.NewDestructiveService(channelOnboardingStore, operatorChannels, credentialWriter, channelActivationRefresher, opts.TestChannelOnboardingNow, opts.TestChannelOnboardingBarrier)
	if err != nil {
		presenter.fail(20, "channel_onboarding", err)
		return 1
	}
	handlers := apiv1.MergeOperatorHandlers(
		apiv1.OperatorHealthHandlers(apiv1.HealthHandlerOptions{ExecutionPosture: posture, Ready: readyFn, Database: apiStoreCaps.Database, Publication: runtimeContextManager}),
		apiv1.OperatorRuntimeIdentityHandlers(apiv1.RuntimeIdentityHandlerOptions{Identity: runtimeIdentity, Publication: runtimeContextManager}),
		apiv1.OperatorRunReadHandlers(apiv1.RunReadHandlerOptions{Runs: apiStoreCaps.Runs}),
		apiv1.OperatorObservabilityHandlers(apiv1.ObservabilityHandlerOptions{Observability: apiStoreCaps.Observability}),
		apiv1.OperatorEntityHandlers(apiv1.EntityHandlerOptions{Entities: apiStoreCaps.Entities}),
		apiv1.OperatorAgentConversationHandlers(apiv1.AgentConversationHandlerOptions{Agents: apiStoreCaps.Agents, Conversations: apiStoreCaps.Conversations, DeliveryLifecycle: stores.AgentDeliveryLifecycle(), Usage: stores.AgentUsage()}),
		apiv1.OperatorDataHandlers(apiv1.DataHandlerOptions{Store: apiStoreCaps.Data}),
		apiv1.OperatorMailboxHandlers(apiv1.MailboxHandlerOptions{Mailbox: stores.MailboxAPI()}),
		supervisor.executionDispatch(),
		apiv1.OperatorStandingServiceHandlers(apiv1.StandingServiceHandlerOptions{Controller: &serveStandingServiceController{manager: runtimeContextManager, supervisor: supervisor}, Idempotency: idempotency}),
		apiv1.OperatorRuntimeNukeHandlers(apiv1.RuntimeNukeHandlerOptions{Coordinator: apiStoreCaps.ResetCoordinator, Idempotency: idempotency}),
		apiv1.OperatorAgentControlHandlers(apiv1.AgentControlHandlerOptions{Controller: dashboardDynamicAgentControl{supervisor: supervisor}, Idempotency: idempotency, RuntimeContexts: apiStoreCaps.RuntimeContexts}),
		apiv1.OperatorChannelHandlers(apiv1.OperatorChannelHandlerOptions{Channels: operatorChannels, Confirmation: channelOnboarding, Destructive: channelDestructive, Readback: channelOnboarding, Idempotency: idempotency, Now: opts.TestChannelOnboardingNow}),
		apiv1.ChannelOnboardingHandlers(apiv1.ChannelOnboardingHandlerOptions{Onboarding: channelOnboarding, Channels: operatorChannels}),
	)
	apiV1Handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath:    resolvedPlatformSpecPath,
		AuthTokens:          apiAuth.Tokens,
		ProcessWorkOwner:    processWorkOwner,
		OperatorPrincipalID: operatorPrincipal.ID,
		Handlers:            handlers,
		Subscriptions: apiv1.OperatorSubscriptions(apiv1.SubscriptionOptions{
			ExecutionPosture: posture,
			Ready:            readyFn, Database: apiStoreCaps.Database, Observability: apiStoreCaps.Observability,
			DecisionCards: storeDeps.DecisionCards, ProposedEffects: storeDeps.ProposedEffects, Publication: runtimeContextManager,
		}),
	})
	if err != nil {
		presenter.fail(20, "api_initialization", err)
		return 1
	}
	var inboundHandler http.Handler
	if req.ProviderIngress && rt != nil && rt.InboundGateway != nil {
		inboundHandler = runtimeProcessInboundHandler{contexts: runtimeContextManager}
	}
	apiServer = newAPIServer(serveSupervisorReadiness{serveReadiness: ready, supervisor: supervisor}, apiV1Handler, inboundHandler, ctx)
	mcpServer = &http.Server{Handler: http.HandlerFunc(supervisor.serveMCP), ReadHeaderTimeout: 5 * time.Second}
	apiServer.Handler = processOwnedHTTPHandler(processWorkOwner, apiServer.Handler)
	mcpServer.Handler = processOwnedHTTPHandler(processWorkOwner, mcpServer.Handler)
	if err := projectContextRegistration.WriteFinal(runtimeInstanceID, apiListener.Addr(), apiAuth, resolvedPaths, storeSelection, mountSources); err != nil {
		presenter.fail(20, "context_registry", err)
		return 3
	}
	defer projectContextRegistration.Unregister()
	runtimeFailure := func(subject string, err error) {
		presenter.runtimeFailure(subject, err)
		cancelServe()
	}
	var reconcilePublicIngress func(context.Context, runtimepublicingress.Generation) error
	if publicIngressEnabled {
		registrationController, controllerErr := runtimepublicingress.NewProviderRegistrationController(runtimepublicingress.RegistrationControllerOptions{
			CredentialOwner: providerCredentialOwner, EffectsStore: stores.Effects(),
			HTTP: runtimeregistration.HTTPExecutor{}, Posture: posture, RuntimeInstanceID: runtimeInstanceID,
			StartupAuthority: func() (runtimestartupownership.GrantEvidence, error) {
				current, _ := supervisor.PublicIngressState()
				if current == nil {
					return runtimestartupownership.GrantEvidence{}, fmt.Errorf("public ingress runtime owner is unavailable")
				}
				return current.CurrentStartupGrantEvidence()
			},
			Readiness: ready,
		})
		if controllerErr != nil {
			presenter.fail(20, "public_ingress", controllerErr)
			return 1
		}
		reconcilePublicIngress = func(reconcileCtx context.Context, generation runtimepublicingress.Generation) error {
			_, manager := supervisor.PublicIngressState()
			snapshot, pairErr := compileServeChannelActivationSnapshot(reconcileCtx, manager, channelOnboardingStore, operatorChannels, providerCredentialOwner)
			if pairErr != nil {
				return pairErr
			}
			selection, pairErr := resolveServeRegistrationPairs(snapshot, manager)
			if pairErr != nil {
				return pairErr
			}
			defer selection.Release()
			return registrationController.Reconcile(reconcileCtx, generation, selection.Pairs)
		}
		publicHandler := http.NotFoundHandler()
		if inboundHandler != nil {
			publicHandler = registrationController.Handler(inboundHandler)
		}
		publicExposure, controllerErr = runtimepublicingress.NewController(runtimepublicingress.Options{
			Mode: publicIngressMode, PublicOrigin: opts.PublicWebhookBaseURL, ListenAddress: opts.PublicWebhookListen,
			Listener: publicListener,
			Handler:  publicHandler, Readiness: ready,
			StartupAuthority: func() string {
				current, _ := supervisor.PublicIngressState()
				if current == nil {
					return ""
				}
				authority, authorityErr := current.CurrentStartupGrantEvidence()
				if authorityErr != nil {
					return ""
				}
				return authority.GrantID
			},
			OnGeneration: reconcilePublicIngress,
			OnFatal:      func(exposureErr error) { runtimeFailure("public_ingress", exposureErr) },
		})
		if controllerErr != nil {
			presenter.fail(20, "public_ingress", controllerErr)
			return 1
		}
		publicListener = nil // The controller now owns shutdown, including pre-Start failures.
		channelActivationRefresher.preflight = func(preflightCtx context.Context, intent servePrebindingActivation) error {
			generation := publicExposure.Generation()
			if generation.ID == "" {
				return fmt.Errorf("public exposure generation is unavailable for channel activation preflight")
			}
			pair, pairErr := resolveServePrebindingRegistrationPair(intent)
			if pairErr != nil {
				return pairErr
			}
			return registrationController.Preflight(preflightCtx, generation, pair)
		}
		channelActivationRefresher.reconcile = func(refreshCtx context.Context) error {
			generation := publicExposure.Generation()
			if generation.ID == "" {
				// Startup teardown and stale-context retirement precede public
				// exposure. Process publication is refreshed immediately; provider
				// registration is reconciled after publicExposure.Start below.
				return nil
			}
			return reconcilePublicIngress(refreshCtx, generation)
		}
	}
	presenter.recordBootWarnings(bootReport)
	supervisor.resetRefresh = func(refreshCtx context.Context) error {
		if err := channelActivationRefresher.RefreshChannelActivations(refreshCtx); err != nil {
			return err
		}
		current := supervisor.CurrentRuntime()
		stop, err := startServeRunStalledEscalation(ctx, processWorkOwner, stores.RunStalled(), supervisor.resetContexts, current.Bus, current.ExecutionPosture)
		if err != nil {
			return err
		}
		supervisor.stopRunStalled = stop
		return nil
	}
	var releaseRuntimeContexts func() error
	if err := activateServeAfterConnectedChannelTeardownRecovery(ctx, channelDestructive, func() error {
		apiServerLease, err := processWorkOwner.Begin(ctx)
		if err != nil {
			return fmt.Errorf("admit api server: %w", err)
		}
		mcpServerLease, err := processWorkOwner.Begin(ctx)
		if err != nil {
			_ = apiServerLease.Done()
			return fmt.Errorf("admit mcp server: %w", err)
		}
		httpServersStarted = true
		go func() {
			defer func() { _ = apiServerLease.Done() }()
			serveHTTPServer("api", apiServer, apiListener, runtimeFailure)
		}()
		go func() {
			defer func() { _ = mcpServerLease.Done() }()
			serveHTTPServer("mcp", mcpServer, mcpListener, runtimeFailure)
		}()
		if resetRecovery != nil && len(runtimeContexts) > 0 {
			if err := runtimeContextManager.StageRecoveredRuntimeContexts(preflightContexts...); err != nil {
				return err
			}
			releaseRuntimeContexts, err = prepareResetServeRuntimeContexts(ctx, runtimeContexts, runtimeContextManager)
		} else {
			releaseRuntimeContexts, err = prepareServeRuntimeContexts(ctx, runtimeContexts, runtimeContextManager)
		}
		if err != nil {
			return err
		}
		supervisor.resetContextsManaged = true
		return reconcileRetiredConnectedChannelContexts(ctx, runtimeContextManager, channelOnboardingStore, channelDestructive)
	}, channelOnboarding); err != nil {
		presenter.fail(22, "channel_onboarding", err)
		return 1
	}
	if err := channelActivationRefresher.publishChannelActivations(ctx); err != nil {
		presenter.fail(22, "channel_onboarding", err)
		return 1
	}
	if resetRecovery != nil && len(runtimeContexts) > 0 {
		if err := runtimeContextManager.ReleaseResetExecution(preflightContexts...); err != nil {
			presenter.fail(22, "pending_reset_recovery", err)
			return 3
		}
	}
	if err := releaseRuntimeContexts(); err != nil {
		presenter.fail(22, "channel_onboarding", err)
		return 1
	}
	if rt != nil {
		if err := reportServeStandingReadiness(ctx, rt.Pipeline, opts.Output); err != nil {
			presenter.fail(22, "channel_onboarding", err)
			return 1
		}
	}
	initialRegistrationPairs := []runtimepublicingress.RegistrationPair{}
	if publicIngressEnabled {
		_, manager := supervisor.PublicIngressState()
		snapshot, snapshotErr := compileServeChannelActivationSnapshot(ctx, manager, channelOnboardingStore, operatorChannels, providerCredentialOwner)
		if snapshotErr != nil {
			presenter.fail(22, "public_ingress", snapshotErr)
			return 1
		}
		selection, selectionErr := resolveServeRegistrationPairs(snapshot, manager)
		if selectionErr == nil {
			initialRegistrationPairs = append(initialRegistrationPairs, selection.Pairs...)
			selection.Release()
		}
		err = selectionErr
		if err != nil {
			presenter.fail(22, "public_ingress", err)
			return 1
		}
		if err := publicExposure.Start(ctx); err != nil {
			presenter.fail(22, "public_ingress", err)
			return 1
		}
		if err := channelActivationRefresher.RefreshChannelActivations(ctx); err != nil {
			presenter.fail(22, "channel_onboarding", err)
			return 1
		}
	}
	if err := channelOnboarding.Recover(ctx); err != nil {
		presenter.fail(22, "channel_onboarding", fmt.Errorf("recover connected channel onboarding: %w", err))
		return 1
	}
	if publicIngressEnabled {
		if err := startServePublicIngressRenewal(ctx, processWorkOwner, publicExposure, reconcilePublicIngress); err != nil {
			presenter.fail(22, "public_ingress", err)
			return 1
		}
		if len(initialRegistrationPairs) == 0 {
			presenter.recordNoConnectedChannels()
		}
	}
	if opts.TestRuntimeReadyHook != nil && rt != nil {
		opts.TestRuntimeReadyHook(rt)
	}
	if opts.TestRuntimeContextsReadyHook != nil {
		opts.TestRuntimeContextsReadyHook(runtimeContextManager)
	}
	if rt != nil {
		supervisor.stopRunStalled, err = startServeRunStalledEscalation(ctx, processWorkOwner, stores.RunStalled(), runtimeContexts, rt.Bus, posture)
	}
	if err != nil {
		presenter.fail(22, "ready", err)
		return 1
	}
	presenter.boot(20, "http_listener_bind", "ok", fmt.Sprintf("api_listener=%s api_routes=%s mcp_listener=%s mcp_routes=%s", apiListener.Addr(), serveAPIRoutes, mcpListener.Addr(), serveMCPRoutes))
	if err := waitForServeHealthEndpoints(ctx, apiListener.Addr()); err != nil {
		presenter.fail(21, "health_endpoints_respond", err)
		return 1
	}
	presenter.boot(21, "health_endpoints_respond", "ok", serveReadinessRoutes)
	var standing []serveLifecycleIngressFact
	if req.ProviderIngress {
		standing, err = serveReadyStandingIngress(ctx, runtimeContextManager, providerCredentialOwner, apiListener.Addr())
		if err != nil {
			presenter.fail(22, "ready", err)
			return 1
		}
	}
	if !publicIngressEnabled && len(standing) > 0 {
		presenter.recordPublicIngressDisabledHint()
	}
	standing = publicIngressPresentation(standing, ready.Snapshot(time.Now().UTC()))
	if opts.TestBeforeReadinessCommit != nil {
		if err := opts.TestBeforeReadinessCommit(); err != nil {
			presenter.fail(22, "ready", err)
			return 1
		}
	}
	if resetRecovery != nil {
		if err := resetRecovery.Complete(ctx); err != nil {
			presenter.fail(22, "pending_reset_recovery", err)
			return 3
		}
		if err := resetRecovery.Close(ctx); err != nil {
			presenter.fail(22, "pending_reset_recovery", err)
			return 3
		}
	}
	readyAfter := time.Since(bootStartedAt)
	presenter.boot(22, "ready", "ok", fmt.Sprintf("total=%s state_stores=%s", readyAfter.Round(time.Millisecond), strings.TrimSpace(stateStoreSummary)))
	flowCount, agentCount, toolCount := serveLifecycleSourceCounts(runtimeContexts)
	feedEnabled := opts.Dev && !opts.NoFeed
	storyReader := stores.AuthorActivity()
	storyHead := int64(0)
	if feedEnabled {
		if storyReader == nil {
			presenter.storyWarning(fmt.Errorf("selected store does not expose author activity reads"))
			feedEnabled = false
		} else if storyHead, err = storyReader.HeadAuthorActivity(ctx); err != nil {
			presenter.storyWarning(err)
			feedEnabled = false
		}
	}
	if feedEnabled && opts.TestAfterAuthorActivityHead != nil {
		if err := opts.TestAfterAuthorActivityHead(); err != nil {
			presenter.storyWarning(err)
			feedEnabled = false
		}
	}
	unreadInformationalNotices, err := stores.Mailbox().CountUnreadInformationalNotices(ctx)
	if err != nil {
		presenter.fail(22, "ready", fmt.Errorf("count unread informational notices: %w", err))
		return 1
	}
	if !presenter.commitReady(serveLifecycleReadyFacts{
		ProjectName:                serveLifecycleProjectName(localState, loadedBundles),
		BundleCount:                len(loadedBundles),
		FlowCount:                  flowCount,
		AgentCount:                 agentCount,
		ToolCount:                  toolCount,
		APIListener:                addrString(apiListener.Addr()),
		MCPListener:                addrString(mcpListener.Addr()),
		ReadyAfter:                 readyAfter,
		Standing:                   standing,
		Packs:                      serveLifecyclePackFacts(runtimeContexts),
		UnreadInformationalNotices: unreadInformationalNotices,
	}, func() {
		supervisor.mu.Lock()
		supervisor.apiReady = true
		ready.Store(true)
		supervisor.mu.Unlock()
	}) {
		return 1
	}
	if feedEnabled {
		if err := presenter.writeFeedReady(); err != nil {
			presenter.storyWarning(err)
		} else {
			storyFollower, err = newServeAuthorActivityFollower(ctx, processWorkOwner, storyReader, presenter, runtimeInstanceID, runtimeContextManager, storyHead, runtimeauthoractivity.NewHumanRenderer(serveAuthorActivityRenderOptions(opts.Output, opts.NoColor)))
			if err != nil {
				presenter.fail(22, "ready", err)
				return 1
			}
		}
	}

	if req.OnReady != nil {
		err := req.OnReady(ctx, "http://"+apiListener.Addr().String())
		ready.Store(false)
		if err != nil {
			presenter.runtimeFailure("test_execution", err)
			return 1
		}
		return 0
	}
	<-ctx.Done()
	ready.Store(false)
	return serveCancellationExitCode(ownershipLoss)
}

func serveLifecyclePackFacts(contexts []serveRuntimeBundleContext) []serveLifecyclePackFact {
	facts := make([]serveLifecyclePackFact, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.loaded.bundle == nil || contextDef.loaded.bundle.PackInventory == nil {
			continue
		}
		inventory := contextDef.loaded.bundle.PackInventory
		fact := serveLifecyclePackFact{
			BundleHash:      contextDef.sourceArtifactFact.BundleHash(),
			BaseMode:        inventory.BaseSelectionMode(),
			BaseDigest:      inventory.BaseDigest(),
			BaseDirectories: inventory.BaseDirectories(),
			EffectiveDigest: inventory.Digest(),
			PackCount:       len(inventory.Entries()),
		}
		for _, entry := range inventory.Entries() {
			if entry.Source() == packartifact.ProvenanceProject {
				fact.ProjectPacks = append(fact.ProjectPacks, entry.ID())
			}
		}
		facts = append(facts, fact)
	}
	return facts
}

func serveOwnershipAcquisitionError(err error) error {
	var acquisitionErr *runtimestartupownership.AcquisitionError
	if !errors.As(err, &acquisitionErr) {
		return err
	}
	switch acquisitionErr.Failure {
	case runtimestartupownership.AcquisitionTakeoverRequired:
		started := ""
		if !acquisitionErr.RecordedAt.IsZero() {
			started = fmt.Sprintf(" (started %s ago)", conciseOwnershipAge(time.Since(acquisitionErr.RecordedAt)))
		}
		return fmt.Errorf("Another swarm serve is already running for this project%s. Stop it first, or serve a different project.", started)
	case runtimestartupownership.AcquisitionPriorOwnerAmbiguous:
		return errors.New("Swarm cannot verify who owns this project. Run `swarm store repair-authority` to inspect and repair it.")
	default:
		return err
	}
}

func conciseOwnershipAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	switch {
	case age < time.Minute:
		return "less than 1m"
	case age < time.Hour:
		return fmt.Sprintf("%dm", int(age/time.Minute))
	case age < 24*time.Hour:
		return fmt.Sprintf("%dh", int(age/time.Hour))
	default:
		return fmt.Sprintf("%dd", int(age/(24*time.Hour)))
	}
}

func startServeOwnershipWatch(
	ctx context.Context,
	workOwner *worklifetime.Process,
	capability runtimestartupownership.ProcessCapability,
	presenter *serveLifecyclePresenter,
	cancel context.CancelFunc,
) (<-chan runtimestartupownership.TerminalResult, error) {
	if workOwner == nil || capability == nil || presenter == nil || cancel == nil {
		return nil, errors.New("serve ownership watcher requires process work, capability, presenter, and cancellation")
	}
	lease, err := workOwner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("admit serve ownership watcher: %w", err)
	}
	ownershipLoss := make(chan runtimestartupownership.TerminalResult, 1)
	go func() {
		settled := false
		defer func() {
			if !settled {
				_ = lease.Done()
			}
		}()
		select {
		case <-ctx.Done():
			select {
			case <-capability.Done():
			default:
				return
			}
		case <-capability.Done():
		}
		result, ok := capability.TerminalResult()
		if !ok || result.Cause == runtimestartupownership.TerminalReleased {
			return
		}
		presenter.ownershipLost(result)
		_ = lease.Done()
		settled = true
		ownershipLoss <- result
		cancel()
	}()
	return ownershipLoss, nil
}

func serveCancellationExitCode(ownershipLoss <-chan runtimestartupownership.TerminalResult) int {
	select {
	case result := <-ownershipLoss:
		if result.Cause == runtimestartupownership.TerminalOwnershipUnprovable || result.Cause == runtimestartupownership.TerminalOwnershipSuperseded {
			return 1
		}
	default:
	}
	return 0
}

type standingServiceSetReconciler interface {
	ReconcileStandingServiceSet(context.Context, []runtimepipeline.StandingServiceCandidate) ([]runtimepipeline.StandingServiceReconciliation, error)
}

func reconcileServeStandingServices(ctx context.Context, owner standingServiceSetReconciler, contexts []serveRuntimeBundleContext) (map[string]runtimepipeline.StandingServiceReconciliation, error) {
	var candidates []runtimepipeline.StandingServiceCandidate
	for _, contextDef := range contexts {
		if contextDef.runtime == nil || owner == nil {
			continue
		}
		planned, err := contextDef.runtime.PlanStandingServiceCandidates()
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, planned...)
	}
	if owner == nil {
		return nil, nil
	}
	results, err := owner.ReconcileStandingServiceSet(ctx, candidates)
	if err != nil {
		return nil, err
	}
	byServiceID := make(map[string]runtimepipeline.StandingServiceReconciliation, len(results))
	for _, result := range results {
		if _, exists := byServiceID[result.ServiceID]; exists {
			return nil, fmt.Errorf("standing service %s reconciled more than once", result.ServiceID)
		}
		byServiceID[result.ServiceID] = result
	}
	return byServiceID, nil
}

func reconcileServeRuntimeStandingTargets(
	rt *runtime.Runtime,
	reconciliations map[string]runtimepipeline.StandingServiceReconciliation,
) ([]runtime.StandingTarget, []runtime.StandingActivation, error) {
	if rt == nil {
		return nil, nil, nil
	}
	targets, err := rt.PlanStandingTargets()
	if err != nil {
		return nil, nil, err
	}
	activations := make([]runtime.StandingActivation, 0)
	seenServices := make(map[string]struct{}, len(targets))
	for i := range targets {
		reconciliation, ok := reconciliations[targets[i].ServiceID]
		if !ok {
			return nil, nil, fmt.Errorf("standing service %s has no startup reconciliation", targets[i].ServiceID)
		}
		targets[i].RunID = reconciliation.RunID
		targets[i].Generation = reconciliation.Generation
		targets[i].PublicationSequence = reconciliation.PublicationSequence
		if _, seen := seenServices[reconciliation.ServiceID]; seen {
			continue
		}
		seenServices[reconciliation.ServiceID] = struct{}{}
		activations = append(activations, runtime.StandingActivation{
			BundleHash:          targets[i].BundleHash,
			ServiceID:           reconciliation.ServiceID,
			FlowPath:            reconciliation.FlowPath,
			RunID:               reconciliation.RunID,
			Generation:          reconciliation.Generation,
			PublicationSequence: reconciliation.PublicationSequence,
			InstanceID:          reconciliation.InstanceID,
			FlowInstance:        targets[i].FlowInstance,
			EntityID:            reconciliation.EntityID,
			EffectiveState:      reconciliation.EffectiveState,
			RestartDisposition:  reconciliation.RestartDisposition,
			Created:             reconciliation.Transition == "created",
		})
	}
	return targets, activations, nil
}

func serveRuntimeContextStandingTargets(
	executable []runtime.StandingTarget,
	declared []runtime.StandingTarget,
	activations []runtime.StandingActivation,
) []runtime.StandingTarget {
	nonExecutable := make(map[string]struct{}, len(activations))
	for _, activation := range activations {
		if !activation.RestartDisposition.Executable() {
			nonExecutable[activation.ServiceID] = struct{}{}
		}
	}
	targets := append([]runtime.StandingTarget(nil), executable...)
	for _, target := range declared {
		if _, ok := nonExecutable[target.ServiceID]; ok {
			targets = append(targets, target)
		}
	}
	return targets
}

type serveStandingServiceController struct {
	manager    *runtime.RuntimeContextManager
	supervisor *processLifecycleSupervisor
	mu         sync.Mutex
}

func (c *serveStandingServiceController) admitProcessTransition() (func(), error) {
	if c == nil || c.manager == nil || c.supervisor == nil {
		return nil, errors.New("standing service process lifecycle owner is required")
	}
	c.supervisor.mu.RLock()
	if c.supervisor.resetting {
		c.supervisor.mu.RUnlock()
		return nil, errors.New("runtime reset has not converged")
	}
	return c.supervisor.mu.RUnlock, nil
}

type serveStandingServiceTransition struct {
	occurrence       *runtime.StandingServiceTransition
	pipelineRecovery *runtimebus.PipelineParentTransition
}

func (t *serveStandingServiceTransition) Wait(ctx context.Context) error {
	if t == nil {
		return nil
	}
	return t.occurrence.Wait(ctx)
}

func (t *serveStandingServiceTransition) Restore(ctx context.Context) error {
	if t == nil {
		return nil
	}
	defer t.pipelineRecovery.Done()
	return t.occurrence.Restore(ctx)
}

func (t *serveStandingServiceTransition) Retire(ctx context.Context) error {
	if t == nil {
		return nil
	}
	defer t.pipelineRecovery.Done()
	return t.occurrence.Retire(ctx)
}

func (c *serveStandingServiceController) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	release, err := c.admitProcessTransition()
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer release()
	c.mu.Lock()
	defer c.mu.Unlock()
	use, _, err := c.manager.AcquireStandingService(ctx, operation.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer func() { _ = use.Done() }()
	ctx = use.WorkContext()
	owner := use.Runtime()
	if owner == nil || owner.Pipeline == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service %s selected runtime pipeline is unavailable", strings.TrimSpace(operation.ServiceID))
	}
	owner, transition, err := c.closeAndDrain(ctx, operation.ServiceID, owner)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	result, err := owner.Pipeline.SuspendStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, errors.Join(err, c.restoreAdmission(owner, operation.ServiceID, transition))
	}
	if err := transition.Retire(ctx); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("retire suspended standing service occurrence: %w", err)
	}
	return result, nil
}

func (c *serveStandingServiceController) ResumeStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	release, err := c.admitProcessTransition()
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer release()
	c.mu.Lock()
	defer c.mu.Unlock()
	use, _, err := c.manager.AcquireStandingService(ctx, operation.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer func() { _ = use.Done() }()
	ctx = use.WorkContext()
	owner := use.Runtime()
	if owner == nil || owner.Pipeline == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service %s selected runtime pipeline is unavailable", strings.TrimSpace(operation.ServiceID))
	}
	result, err := owner.Pipeline.ResumeStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if err := c.publishActiveService(ctx, result, owner); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if owner.InboundGateway != nil {
		if err := owner.InboundGateway.ReopenStandingServiceAdmission(result.ServiceID); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, c.failClosedAfterReopen(result.ServiceID, err)
		}
	}
	return result, nil
}

func (c *serveStandingServiceController) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	release, err := c.admitProcessTransition()
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer release()
	c.mu.Lock()
	defer c.mu.Unlock()
	use, _, err := c.manager.AcquireStandingService(ctx, operation.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	defer func() { _ = use.Done() }()
	ctx = use.WorkContext()
	owner := use.Runtime()
	if owner == nil || owner.Pipeline == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service %s selected runtime pipeline is unavailable", strings.TrimSpace(operation.ServiceID))
	}
	owner, transition, err := c.closeAndDrain(ctx, operation.ServiceID, owner)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	result, err := owner.Pipeline.ResetStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, errors.Join(err, c.restoreAdmission(owner, operation.ServiceID, transition))
	}
	if err := transition.Retire(ctx); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("retire reset standing service occurrence: %w", err)
	}
	if result.RestartDisposition.Executable() {
		if err := c.publishActiveService(ctx, result, owner); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
		if owner.InboundGateway != nil {
			if err := owner.InboundGateway.ReopenStandingServiceAdmission(result.ServiceID); err != nil {
				return runtimepipeline.StandingServiceReconciliation{}, c.failClosedAfterReopen(result.ServiceID, err)
			}
		}
	}
	return result, nil
}

func (c *serveStandingServiceController) closeAndDrain(ctx context.Context, serviceID string, owner *runtime.Runtime) (*runtime.Runtime, *serveStandingServiceTransition, error) {
	if owner == nil {
		return nil, nil, fmt.Errorf("standing service %s runtime owner is unavailable", strings.TrimSpace(serviceID))
	}
	if owner.InboundGateway != nil {
		if err := owner.InboundGateway.CloseStandingServiceAdmission(serviceID); err != nil {
			return nil, nil, err
		}
	}
	if c.manager == nil {
		return nil, nil, errors.Join(errors.New("standing service runtime context manager is required"), c.restoreAdmission(owner, serviceID, nil))
	}
	if owner.Bus == nil {
		return nil, nil, errors.Join(errors.New("standing service pipeline recovery owner is required"), c.restoreAdmission(owner, serviceID, nil))
	}
	pipelineRecovery, err := owner.Bus.BeginPipelineParentTransition(ctx)
	if err != nil {
		return nil, nil, errors.Join(err, c.restoreAdmission(owner, serviceID, nil))
	}
	occurrence, err := c.manager.BeginStandingServiceTransition(ctx, serviceID)
	if err != nil {
		pipelineRecovery.Done()
		return nil, nil, errors.Join(err, c.restoreAdmission(owner, serviceID, nil))
	}
	transition := &serveStandingServiceTransition{
		occurrence:       occurrence,
		pipelineRecovery: pipelineRecovery,
	}
	if owner.InboundGateway != nil {
		if err := owner.InboundGateway.WaitForStandingServiceAdmission(ctx, serviceID); err != nil {
			return nil, nil, errors.Join(err, c.restoreAdmission(owner, serviceID, transition))
		}
	}
	if err := transition.Wait(ctx); err != nil {
		return nil, nil, errors.Join(err, c.restoreAdmission(owner, serviceID, transition))
	}
	return owner, transition, nil
}

func (c *serveStandingServiceController) restoreAdmission(owner *runtime.Runtime, serviceID string, transition *serveStandingServiceTransition) error {
	if transition != nil {
		if err := transition.Restore(context.Background()); err != nil {
			return fmt.Errorf("restore standing service %s process targets: %w", serviceID, err)
		}
	}
	if owner != nil && owner.InboundGateway != nil {
		if err := owner.InboundGateway.ReopenStandingServiceAdmission(serviceID); err != nil {
			return c.failClosedAfterReopen(serviceID, err)
		}
	}
	return nil
}

func (c *serveStandingServiceController) failClosedAfterReopen(serviceID string, reopenErr error) error {
	reopenErr = fmt.Errorf("reopen standing service %s admission: %w", serviceID, reopenErr)
	if c.manager == nil {
		return reopenErr
	}
	if err := c.manager.SuppressStandingServiceTargets(serviceID); err != nil {
		return errors.Join(reopenErr, fmt.Errorf("restore fail-closed standing service %s suppression: %w", serviceID, err))
	}
	return reopenErr
}

func (c *serveStandingServiceController) publishActiveService(ctx context.Context, result runtimepipeline.StandingServiceReconciliation, owner *runtime.Runtime) error {
	serviceID := strings.TrimSpace(result.ServiceID)
	if !result.RestartDisposition.Executable() {
		return fmt.Errorf("standing service %s disposition %s is not executable", serviceID, result.RestartDisposition.Kind)
	}
	if owner == nil {
		return fmt.Errorf("standing service %s runtime owner is unavailable", serviceID)
	}
	if c.manager == nil {
		return errors.New("standing service runtime context manager is required")
	}
	prepared, err := c.manager.PrepareStandingServicePublication(serviceID, result.RunID, result.Generation)
	if err != nil {
		return err
	}
	targets, _, err := owner.EnsureStandingServiceTargets(prepared.WorkContext(ctx), serviceID)
	if err != nil {
		return errors.Join(err, prepared.Discard())
	}
	if err := prepared.Publish(targets); err != nil {
		return errors.Join(err, prepared.Discard())
	}
	return nil
}

type standingServiceStatusReader interface {
	ListStandingServiceStatuses(context.Context) ([]runtimepipeline.StandingServiceStatus, error)
}

func reportServeStandingReadiness(ctx context.Context, owner standingServiceStatusReader, out io.Writer) error {
	if owner == nil {
		return nil
	}
	statuses, err := owner.ListStandingServiceStatuses(ctx)
	if err != nil {
		return err
	}
	for _, status := range statuses {
		switch status.RestartDisposition.Kind {
		case runtimepipeline.StandingRestartActiveIntrinsic:
			if status.PublicationState != "published" {
				return fmt.Errorf("standing service %s is active but publication is %s", status.ServiceID, status.PublicationState)
			}
			if out != nil {
				fmt.Fprintf(out, "standing service %s %s run=%s generation=%d source=%s\n", status.ServiceID, status.Transition, status.RunID, status.Generation, status.BundleHash)
			}
		case runtimepipeline.StandingRestartSuspended:
			if out != nil {
				fmt.Fprintf(out, "standing service %s suspended by=%s at=%s reason=%s resume=`swarm standing resume %s`\n", status.ServiceID, status.OverrideActor, status.OverrideAt.Format(time.RFC3339), status.OverrideReason, status.ServiceID)
			}
		case runtimepipeline.StandingRestartOrphaned:
			if out != nil {
				fmt.Fprintf(out, "standing service %s orphaned declaration_removed=true run=%s generation=%d remediation=%s\n", status.ServiceID, status.RunID, status.Generation, status.RestartDisposition.RunControlGuidance())
			}
		case runtimepipeline.StandingRestartTerminalDeclared, runtimepipeline.StandingRestartTerminalOrphaned, runtimepipeline.StandingRestartInvalidCurrent:
			if out != nil {
				fmt.Fprintf(out, "standing service %s %s run=%s generation=%d remediation=%s\n", status.ServiceID, status.RestartDisposition.Kind, status.RunID, status.Generation, status.RestartDisposition.RunControlGuidance())
			}
		default:
			return fmt.Errorf("standing service %s has unsupported restart disposition %q", status.ServiceID, status.RestartDisposition.Kind)
		}
	}
	return nil
}

func plannedServeRuntimeContexts(contexts []serveRuntimeBundleContext) ([]runtime.BundleContext, error) {
	planned := make([]runtime.BundleContext, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		targets, err := contextDef.runtime.PlanStandingTargets()
		if err != nil {
			return nil, err
		}
		planned = append(planned, runtime.BundleContext{
			SourceArtifactFact:         contextDef.sourceArtifactFact,
			BundleIdentity:             contextDef.bootIdentity,
			Source:                     contextDef.loaded.source,
			PlatformSpecPath:           contextDef.loaded.platformSpecPath,
			Runtime:                    contextDef.runtime,
			WorkOwner:                  contextDef.runtime.WorkOccurrence(),
			StandingTargets:            targets,
			ProviderTriggerGeneration:  contextDef.providerTriggerGeneration,
			InstalledTriggerSubjects:   contextDef.installedTriggerSubjects,
			PackInventoryDigest:        contextDef.packInventoryDigest,
			ChannelPlans:               contextDef.runtime.Options.ChannelPlans,
			DeclaredChannelPublication: contextDef.runtime.Options.DeclaredChannelPublication,
		})
	}
	return planned, nil
}

func serveReadyStandingIngress(ctx context.Context, manager *runtime.RuntimeContextManager, credentials *runtimecredentials.SnapshotOwner, apiAddr net.Addr) ([]serveLifecycleIngressFact, error) {
	if manager == nil || apiAddr == nil {
		return nil, nil
	}
	subjects, err := manager.EvaluatedCapabilitySubjects(ctx, credentials)
	if err != nil {
		return nil, err
	}
	facts := []serveLifecycleIngressFact{}
	for _, subject := range subjects {
		if subject.Kind != packs.SubjectProviderTrigger || subject.Applicability != "effective" {
			continue
		}
		admission := subject.TriggerAdmission
		if admission == nil {
			return nil, fmt.Errorf("effective provider trigger %s has no compiled admission readback", subject.ID)
		}
		facts = append(facts, serveLifecycleIngressFact{
			Provider:   strings.TrimSpace(subject.Provider),
			Alias:      strings.TrimSpace(admission.Alias),
			URL:        fmt.Sprintf("http://%s/webhooks/%s/%s", apiAddr.String(), admission.Alias, subject.Provider),
			BundleHash: strings.TrimSpace(admission.BundleHash),
			Subject:    subject,
		})
	}
	return facts, nil
}

func serveLifecycleSourceCounts(contexts []serveRuntimeBundleContext) (flows, agents, tools int) {
	for _, contextDef := range contexts {
		source := contextDef.loaded.source
		if source == nil {
			continue
		}
		flows += len(source.FlowSchemaEntries())
		agents += len(semanticview.AgentDeclarations(source))
		tools += len(runtimetools.RuntimeAvailableToolNamesForSource(source))
	}
	return flows, agents, tools
}

func serveLifecycleProjectName(localState cliapp.LocalRuntimeStateResolution, bundles []serveRuntimeBundle) string {
	if root := strings.TrimSpace(localState.Project.CanonicalProjectRoot); root != "" {
		if name := strings.TrimSpace(filepath.Base(root)); name != "" && name != "." {
			return name
		}
	}
	if len(bundles) == 1 {
		if label := serveRuntimeBundleAuthorLabel(bundles[0]); label != "" {
			return label
		}
	}
	if len(bundles) > 1 {
		return fmt.Sprintf("%d persisted bundles", len(bundles))
	}
	return "runtime"
}

func serveRuntimeBundleAuthorLabel(bundle serveRuntimeBundle) string {
	name := strings.TrimSpace(bundle.bootIdentity.WorkflowName)
	version := strings.TrimSpace(bundle.bootIdentity.WorkflowVersion)
	if name == "" {
		return ""
	}
	if version == "" {
		return name
	}
	return name + " " + version
}

func serveLifecycleWorkspaceLabels(bundles []serveRuntimeBundle) []string {
	labels := make([]string, len(bundles))
	counts := map[string]int{}
	for i, bundle := range bundles {
		labels[i] = serveRuntimeBundleAuthorLabel(bundle)
		counts[labels[i]]++
	}
	for i, label := range labels {
		if label == "" {
			labels[i] = fmt.Sprintf("context %d", i+1)
		} else if counts[label] > 1 {
			labels[i] = fmt.Sprintf("%s context %d", label, i+1)
		}
	}
	return labels
}

func runServeSourceArtifactStartupRecovery(
	ctx context.Context,
	recoveryStore storeselected.StartupRecovery,
	artifacts runtimestartuprecovery.ArtifactReader,
	presenter *serveLifecyclePresenter,
) int {
	_, err := runtimestartuprecovery.Recover(ctx, runtimestartuprecovery.Request{
		AvailabilityReader: recoveryStore.Availability(),
		ArtifactReader:     artifacts,
	})
	if err != nil {
		presenter.fail(5, "startup_recovery", err)
		if runtimestartuprecovery.IsDataIntegrityError(err) {
			return serveExitDataIntegrity
		}
		return 3
	}
	return 0
}

func prepareServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager) (_ func() error, prepareErr error) {
	return prepareServeRuntimeContextSet(ctx, contexts, manager, false)
}

func prepareResetServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager) (func() error, error) {
	if manager == nil {
		return nil, errors.New("reset startup requires runtime context manager")
	}
	return prepareServeRuntimeContextSet(ctx, contexts, manager, true)
}

func prepareServeRuntimeContextSet(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager, reset bool) (_ func() error, prepareErr error) {
	// Own every candidate from entry, including ones that never prepare or
	// register. Only this boundary can fence siblings before any parent join.
	prepared := make([]*runtime.Runtime, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.runtime != nil {
			prepared = append(prepared, contextDef.runtime)
		}
	}
	rollback := func() error {
		var cleanupErr error
		for _, rt := range prepared {
			rt.CloseAdmission()
		}
		if manager != nil {
			for _, result := range manager.DeactivateAll(runtime.RuntimeContextCauseUnavailable) {
				cleanupErr = errors.Join(cleanupErr, result.ShutdownErr)
			}
		}
		for i := len(prepared) - 1; i >= 0; i-- {
			cleanupErr = errors.Join(cleanupErr, prepared[i].Shutdown())
		}
		return cleanupErr
	}
	preparedSet := false
	defer func() {
		if !preparedSet {
			prepareErr = errors.Join(prepareErr, rollback())
		}
	}()
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.PreflightDynamicTopologyStartup(ctx); err != nil {
			return nil, fmt.Errorf("preflight runtime dynamic topology: %w", err)
		}
	}
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.PrepareAuthorActivityCatalog(); err != nil {
			return nil, fmt.Errorf("prepare author activity catalog: %w", err)
		}
	}
	for _, rt := range prepared {
		if err := rt.PrepareStartupLifecycle(ctx); err != nil {
			return nil, fmt.Errorf("prepare runtime lifecycle: %w", err)
		}
	}
	releases := make([]func() error, 0, len(contexts))
	resetContexts := make([]runtime.BundleContext, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		startupStandingOwner, err := newServeStartupStandingRecoveryOwner(
			contextDef.runtime.WorkOccurrence(),
			contextDef.startupStandingTargets,
			contextDef.startupStandingActivations,
		)
		if err != nil {
			return nil, err
		}
		if startupStandingOwner != nil {
			if contextDef.runtime.Bus == nil {
				return nil, errors.New("standing startup recovery requires event bus")
			}
			contextDef.runtime.Bus.SetStandingRunWorkOwner(startupStandingOwner)
		}
		release, err := contextDef.runtime.PrepareStart(ctx)
		if err != nil {
			return nil, err
		}
		targets, activations, completeStanding, err := contextDef.runtime.PrepareStandingTargets(ctx)
		if err != nil {
			return nil, err
		}
		releases = append(releases, func() error { return release.Start(completeStanding) })
		if manager != nil {
			contextTargets := serveRuntimeContextStandingTargets(targets, contextDef.startupStandingTargets, activations)
			for _, activation := range activations {
				if activation.RestartDisposition.Executable() {
					continue
				}
				if err := manager.SuppressStandingServiceTargets(activation.ServiceID); err != nil {
					return nil, err
				}
			}
			definition := runtime.BundleContext{
				SourceArtifactFact:         contextDef.sourceArtifactFact,
				BundleIdentity:             contextDef.bootIdentity,
				Source:                     contextDef.loaded.source,
				PlatformSpecPath:           contextDef.loaded.platformSpecPath,
				Runtime:                    contextDef.runtime,
				WorkOwner:                  contextDef.runtime.WorkOccurrence(),
				StandingTargets:            contextTargets,
				ProviderTriggerGeneration:  contextDef.providerTriggerGeneration,
				InstalledTriggerSubjects:   contextDef.installedTriggerSubjects,
				PackInventoryDigest:        contextDef.packInventoryDigest,
				ChannelPlans:               contextDef.runtime.Options.ChannelPlans,
				DeclaredChannelPublication: contextDef.runtime.Options.DeclaredChannelPublication,
			}
			if reset {
				resetContexts = append(resetContexts, definition)
				continue
			}
			if err := manager.Register(definition); err != nil {
				return nil, err
			}
		} else if len(targets) > 0 {
			return nil, errors.New("standing targets require runtime context manager")
		}
	}
	if reset {
		if err := manager.PublishResetRuntimeContexts(resetContexts...); err != nil {
			return nil, err
		}
	}
	preparedSet = true
	return func() (releaseErr error) {
		released := false
		defer func() {
			if !released {
				releaseErr = errors.Join(releaseErr, rollback())
			}
		}()
		for _, release := range releases {
			if err := release(); err != nil {
				return err
			}
		}
		released = true
		return nil
	}, nil
}

type serveStartupStandingRecoveryOwner struct {
	workOwner *worklifetime.RuntimeOccurrence
	byRunID   map[string]runtime.StandingTarget
}

func newServeStartupStandingRecoveryOwner(
	workOwner *worklifetime.RuntimeOccurrence,
	targets []runtime.StandingTarget,
	activations []runtime.StandingActivation,
) (*serveStartupStandingRecoveryOwner, error) {
	active := make(map[string]struct{}, len(activations))
	for _, activation := range activations {
		if activation.RestartDisposition.Executable() {
			active[activation.ServiceID] = struct{}{}
		}
	}
	byRunID := make(map[string]runtime.StandingTarget, len(targets))
	for _, target := range targets {
		if _, ok := active[target.ServiceID]; !ok {
			continue
		}
		if existing, ok := byRunID[target.RunID]; ok {
			if existing.ServiceID != target.ServiceID || existing.Generation != target.Generation {
				return nil, fmt.Errorf("standing startup run %s has conflicting exact owners", target.RunID)
			}
			continue
		}
		byRunID[target.RunID] = target
	}
	if len(byRunID) == 0 {
		return nil, nil
	}
	if workOwner == nil {
		return nil, errors.New("standing startup recovery requires runtime work owner")
	}
	return &serveStartupStandingRecoveryOwner{workOwner: workOwner, byRunID: byRunID}, nil
}

func (o *serveStartupStandingRecoveryOwner) BeginStandingRunRecovery(
	ctx context.Context,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) (*worklifetime.Lease, error) {
	if o == nil || o.workOwner == nil {
		return nil, errors.New("standing startup recovery owner is required")
	}
	target, ok := o.byRunID[strings.TrimSpace(runID)]
	if !ok {
		return nil, fmt.Errorf("standing startup recovery run %s has no prepared owner", runID)
	}
	if origin.Kind() != runtimerunlifecycle.OriginStandingGeneration ||
		origin.ServiceID() != target.ServiceID ||
		origin.Generation() != target.Generation {
		return nil, fmt.Errorf("standing startup recovery run %s conflicts with prepared owner", runID)
	}
	return o.workOwner.BeginStanding(ctx)
}

func closeAdditionalServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager, opts cliapp.ServeOptions, deadlines ...time.Time) error {
	var shutdownErr error
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if manager != nil {
			result := manager.DeactivateBundleHashWithOptions(contextDef.sourceArtifactFact.BundleHash(), runtime.RuntimeContextCauseUnloaded, runtime.ShutdownOptions{Grace: remainingServeShutdownGrace(opts.ShutdownGrace, deadlines...)})
			shutdownErr = errors.Join(shutdownErr, result.ShutdownErr)
		} else if err := contextDef.runtime.ShutdownWithOptions(runtime.ShutdownOptions{Grace: remainingServeShutdownGrace(opts.ShutdownGrace, deadlines...)}); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
		if shutdownErr == nil && contextDef.workspaces != nil {
			shutdownErr = errors.Join(shutdownErr, contextDef.workspaces.ReleaseSourceProjection(ctx))
		}
	}
	return shutdownErr
}

func buildStores(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*storeselected.Owner, error) {
	if cfg == nil {
		return nil, errors.New("runtime config is required")
	}
	request := storeselected.RuntimeRequest{
		Selection:      selection,
		SessionLockTTL: cfg.LLM.Session.LockTTL,
	}
	if selection.Backend == storebackend.BackendPostgres {
		dsn, err := postgresDSNFromConfig(ctx, cfg.Database)
		if err != nil {
			return nil, err
		}
		request.PostgresDSN = dsn
	}
	return storeselected.OpenRuntime(ctx, request)
}

func postgresDSNFromConfig(ctx context.Context, cfg config.DatabaseConfig) (string, error) {
	var credentialStore runtimecredentials.Store
	if strings.TrimSpace(cfg.PasswordSecretKey) != "" {
		fileStore, err := cliapp.CredentialFileStore()
		if err != nil {
			return "", err
		}
		credentialStore = fileStore
	}
	password, err := store.ResolveDatabasePassword(ctx, cfg, credentialStore)
	if err != nil {
		return "", err
	}
	return store.DSNFromConfig(cfg, password), nil
}

func enforceServePinnedBundleAdmissionForHashes(ctx context.Context, availability runbundle.AvailabilityStore, bootIdentity string, pinnedBundleHashes []string) error {
	bootIdentity = strings.TrimSpace(bootIdentity)
	pinnedBundleHashes = uniqueTrimmedServeBundleHashes(pinnedBundleHashes)
	if len(pinnedBundleHashes) == 0 {
		return nil
	}
	if bootIdentity == "" {
		return fmt.Errorf("boot bundle identity is required")
	}
	if availability == nil {
		return nil
	}
	mismatches, err := activeNonStandingRunPinnedBundleHashesConflicts(ctx, availability, pinnedBundleHashes)
	if err != nil {
		return err
	}
	if len(mismatches) == 0 {
		return nil
	}
	details := make([]string, 0, len(mismatches))
	for _, mismatch := range mismatches {
		details = append(details, mismatch.DetailString())
	}
	return fmt.Errorf("active non-standing run pinned bundle_hash conflict: DB-loaded serve bundle_hash set %s cannot resume %d active non-standing run(s) with different bundle_hash: %s", strings.Join(pinnedBundleHashes, ","), len(mismatches), strings.Join(details, "; "))
}

func activeNonStandingRunPinnedBundleHashesConflicts(ctx context.Context, availability runbundle.AvailabilityStore, pinnedBundleHashes []string) ([]runbundle.Availability, error) {
	allowed := map[string]struct{}{}
	for _, hash := range uniqueTrimmedServeBundleHashes(pinnedBundleHashes) {
		allowed[hash] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, nil
	}
	availabilities, err := availability.ActiveNonStandingRunBundleAvailabilities(ctx)
	if err != nil {
		return nil, err
	}
	conflicts := make([]runbundle.Availability, 0, len(availabilities))
	for _, availability := range availabilities {
		if !availability.Available() {
			continue
		}
		if _, ok := allowed[strings.TrimSpace(availability.BundleHash)]; !ok {
			conflicts = append(conflicts, availability)
		}
	}
	return conflicts, nil
}

func uniqueTrimmedServeBundleHashes(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func initializeStateStores(ctx context.Context, schema store.SchemaBootstrapper, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
	if bundle == nil {
		return "store wiring ready", nil
	}
	plans, err := cliapp.StateStoreSchemaPlans(bundle)
	if err != nil {
		return "", err
	}
	request, err := schemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
	if err != nil {
		return "", err
	}
	if err := ensureServeSchemaTables(ctx, schema, request); err != nil {
		return "", err
	}
	return cliapp.SummarizeServeSchemaPlans(plans.All()), nil
}

func initializeServePlatformStateStores(ctx context.Context, schema store.SchemaBootstrapper, platformSpecPath string) (string, error) {
	spec, err := loadServePlatformSpecDocument(platformSpecPath)
	if err != nil {
		return "", err
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		return "", fmt.Errorf("platform-owned tables: %w", err)
	}
	request, err := schemaBootstrapRequest(spec, plans, nil)
	if err != nil {
		return "", err
	}
	if err := ensureServeSchemaTables(ctx, schema, request); err != nil {
		return "", err
	}
	return cliapp.SummarizeServeSchemaPlans(plans), nil
}

func initializeLoadedServeRuntimeStateStores(ctx context.Context, schema store.SchemaBootstrapper, loaded []serveRuntimeBundle) ([]string, error) {
	summaries := make([]string, len(loaded))
	for i, bundle := range loaded {
		summary, err := initializeStateStores(ctx, schema, bundle.bundle)
		if err != nil {
			return nil, fmt.Errorf("bundle %s state stores: %w", bundle.serveIdentityDetail(), err)
		}
		summaries[i] = summary
	}
	return summaries, nil
}

func schemaBootstrapRequest(spec runtimecontracts.PlatformSpecDocument, platformPlans, statePlans []store.SchemaTableDDL) (store.SchemaBootstrapRequest, error) {
	metadata, err := versionmetadata.Resolve(cliapp.InjectedBuildMetadata())
	if err != nil {
		return store.SchemaBootstrapRequest{}, fmt.Errorf("resolve schema bootstrap build identity: %w", err)
	}
	return store.SchemaBootstrapRequest{
		PlatformPlans: platformPlans,
		StatePlans:    statePlans,
		Origin: store.RuntimeStoreOrigin{
			SwarmVersion:    metadata.BinaryVersion,
			PlatformVersion: strings.TrimSpace(spec.Platform.Version),
			CreatedAt:       time.Now().UTC(),
		},
	}, nil
}

func ensureServeSchemaTables(ctx context.Context, schema store.SchemaBootstrapper, request store.SchemaBootstrapRequest) error {
	return schema.BootstrapSchema(ctx, request)
}

func loadServePlatformSpecDocument(platformSpecPath string) (runtimecontracts.PlatformSpecDocument, error) {
	platformSpecPath = strings.TrimSpace(platformSpecPath)
	if platformSpecPath == "" {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("platform spec path is required")
	}
	source, err := yamlsource.LoadFile(platformSpecPath)
	if err != nil {
		if cause, ok := yamlsource.ParseCause(err); ok {
			return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", cause)
		}
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := source.Decode(&spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, nil
}

func serveStateStoreSummaryAt(summaries []string, index int) string {
	if index < 0 || index >= len(summaries) {
		return ""
	}
	return strings.TrimSpace(summaries[index])
}

func serveBootRegistryDetail(source semanticview.Source) string {
	availableToolNames := runtimetools.RuntimeAvailableToolNamesForSource(source)
	return fmt.Sprintf("nodes=%d agents=%d events=%d tools=%d", len(source.ExecutableNodeRecords()), len(semanticview.AgentDeclarations(source)), len(source.ResolvedEventCatalog()), len(availableToolNames))
}

func serveBootBundleLoadDetail(fingerprint string, source semanticview.Source) string {
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return serveBootRegistryDetail(source)
	}
	return fmt.Sprintf("%s, %s", fingerprint, serveBootRegistryDetail(source))
}

type systemWorkspaceContainerLister interface {
	SystemWorkspaceContainers() []string
}

func processOwnedHTTPHandler(owner *worklifetime.Process, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if owner == nil || next == nil {
			http.Error(w, "runtime process unavailable", http.StatusServiceUnavailable)
			return
		}
		lease, err := owner.Begin(r.Context())
		if err != nil {
			http.Error(w, "runtime process is retiring", http.StatusServiceUnavailable)
			return
		}
		defer func() { _ = lease.Done() }()
		ctx := worklifetime.WithProcess(lease.Context(), owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func systemWorkspaceContainers(lifecycle workspace.Lifecycle) []string {
	lister, ok := lifecycle.(systemWorkspaceContainerLister)
	if !ok || lister == nil {
		return nil
	}
	return lister.SystemWorkspaceContainers()
}

func newAPIServer(ready serveReadiness, apiV1Handler http.Handler, inboundHandler http.Handler, baseContexts ...context.Context) *http.Server {
	mux := http.NewServeMux()
	gateReady := func(next http.Handler, control bool) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			available := ready == nil || runtimeAdmissionReady(ready)
			if control {
				if controlReady, ok := ready.(interface{ ControlReady() bool }); ok {
					available = controlReady.ControlReady()
				}
			}
			if !available {
				http.Error(w, "booting", http.StatusServiceUnavailable)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "booting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	if apiV1Handler != nil {
		mux.Handle("/v1/rpc", gateReady(apiV1Handler, true))
		mux.Handle("/v1/ws", gateReady(apiV1Handler, true))
	}
	if inboundHandler != nil {
		mux.Handle("/webhooks/", gateReady(inboundHandler, false))
	}
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if len(baseContexts) > 0 && baseContexts[0] != nil {
		base := baseContexts[0]
		server.BaseContext = func(net.Listener) context.Context { return base }
	}
	return server
}

func runtimeAdmissionReady(ready serveReadiness) bool {
	if runtimeReady, ok := ready.(interface{ RuntimeLoad() bool }); ok {
		return runtimeReady.RuntimeLoad()
	}
	return ready.Load()
}

func validateServeAPIAuthBinding(apiListenAddr string, auth apiv1.AuthTokenResolution) error {
	if !auth.UsesDefaultLoopbackToken() {
		return nil
	}
	if err := cliapp.ValidateServeListenAddr("--api-listen-addr", apiListenAddr); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(apiListenAddr))
	if err != nil {
		return fmt.Errorf("--api-listen-addr must be a host:port listen address: %w", err)
	}
	if apiv1.DefaultLoopbackAPITokenAllowedHost(host) {
		return nil
	}
	return fmt.Errorf("non-loopback API bind %s requires --api-token-file or config serve.api_token_file", strings.TrimSpace(apiListenAddr))
}

func serveHTTPServer(name string, server *http.Server, listener net.Listener, onFailure func(string, error)) {
	if server == nil || listener == nil {
		return
	}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		if onFailure != nil {
			onFailure(name+" server", err)
		}
	}
}

func waitForServeHealthEndpoints(ctx context.Context, addr net.Addr) error {
	baseURL, err := serveHealthProbeBaseURL(addr)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for {
		lastErr = probeServeHealthEndpoint(ctx, client, baseURL+"/healthz")
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func serveHealthProbeBaseURL(addr net.Addr) (string, error) {
	if addr == nil {
		return "", errors.New("health listener address is unavailable")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", fmt.Errorf("parse health listener address: %w", err)
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

func probeServeHealthEndpoint(ctx context.Context, client *http.Client, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned HTTP %d", endpoint, resp.StatusCode)
	}
	return nil
}

func shutdownHTTPServer(ctx context.Context, name string, server *http.Server) error {
	if server == nil {
		return nil
	}
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		closeErr := server.Close()
		return errors.Join(fmt.Errorf("%s server shutdown: %w", name, err), closeErr)
	}
	return nil
}

func remainingServeShutdownGrace(fallback time.Duration, deadlines ...time.Time) time.Duration {
	if len(deadlines) == 0 || deadlines[0].IsZero() {
		return fallback
	}
	remaining := time.Until(deadlines[0])
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

func createServeToolGatewayBinding(mcpAddr net.Addr) (toolgateway.Binding, error) {
	if mcpAddr == nil {
		return toolgateway.Binding{}, errors.New("mcp listener address is unavailable")
	}
	mcpHostURL, err := serveListenerHTTPURL(mcpAddr, "127.0.0.1")
	if err != nil {
		return toolgateway.Binding{}, err
	}
	mcpContainerURL, err := serveMCPContainerGatewayURL(mcpAddr)
	if err != nil {
		return toolgateway.Binding{}, err
	}
	if strings.TrimSpace(os.Getenv(toolgateway.RetiredAuthTokenEnvName)) != "" {
		return toolgateway.Binding{}, toolgateway.RetiredAuthTokenEnvError()
	}
	gatewayToken, err := toolgateway.GenerateAuthToken()
	if err != nil {
		return toolgateway.Binding{}, fmt.Errorf("generate mcp gateway token: %w", err)
	}
	return toolgateway.NewRuntimeOwnedBinding(
		toolgateway.TransportHTTP,
		mcpHostURL,
		mcpContainerURL,
		gatewayToken,
		toolgateway.LifecycleOwnerServeBoot,
		toolgateway.SourceBoundMCPListener,
	)
}

func validateServeMultiContextToolGatewayAdmission(cfg *config.Config, loadedBundles []serveRuntimeBundle) error {
	if len(loadedBundles) <= 1 {
		return nil
	}
	if cfg == nil {
		return fmt.Errorf("runtime config is required for multi-context tool gateway admission")
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return fmt.Errorf("resolve llm backend for multi-context tool gateway admission: %w", err)
	}
	if profile.ID != llmselection.BackendClaudeCLI {
		return nil
	}
	return fmt.Errorf("multi-context swarm serve --bundle-hash with llm.backend=claude_cli is not supported in this configuration: ToolGatewayBinding, MCP /mcp and /tools routes, and forkchat sandbox runtime are single-context; use one --bundle-hash or a non-claude_cli backend")
}

func validateServeGatewayURLEnvForNonDev() error {
	for _, name := range cliapp.RetiredToolGatewayURLEnvNames {
		if err := cliapp.ValidateRetiredToolGatewayURLEnv(name, os.Getenv(name)); err != nil {
			return fmt.Errorf("non-dev serve rejects retired gateway URL env: %w", err)
		}
	}
	return nil
}

func serveMCPContainerGatewayURL(addr net.Addr) (string, error) {
	host, _, err := splitListenerHostPort(addr)
	if err != nil {
		return "", err
	}
	containerHost := host
	if isLocalListenerHost(host) {
		containerHost = "host.docker.internal"
	}
	return serveListenerHTTPURLWithHost(addr, containerHost)
}

func serveListenerHTTPURL(addr net.Addr, localHost string) (string, error) {
	host, _, err := splitListenerHostPort(addr)
	if err != nil {
		return "", err
	}
	if isLocalListenerHost(host) {
		host = strings.TrimSpace(localHost)
		if host == "" {
			host = "127.0.0.1"
		}
	}
	return serveListenerHTTPURLWithHost(addr, host)
}

func serveListenerHTTPURLWithHost(addr net.Addr, host string) (string, error) {
	_, port, err := splitListenerHostPort(addr)
	if err != nil {
		return "", err
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	if host == "" {
		return "", errors.New("listener host is unavailable")
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

func splitListenerHostPort(addr net.Addr) (string, string, error) {
	if addr == nil {
		return "", "", errors.New("listener address is unavailable")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", "", fmt.Errorf("parse listener address: %w", err)
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return "", "", fmt.Errorf("listener address %q must include host and port", addr.String())
	}
	return host, port, nil
}

func isLocalListenerHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	return host == "" || host == "::" || host == "0.0.0.0" || host == "127.0.0.1" || host == "::1" || strings.EqualFold(host, "localhost")
}
