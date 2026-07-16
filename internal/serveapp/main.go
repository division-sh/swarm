package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunforkadmission "github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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
	buildStoresForServe = buildStores
)

type serveStartupRecoveryContainers struct {
	lifecycle cliapp.ServeWorkspaceLifecycle
}

type noWorkspaceStartupRecoveryContainers struct{}

func (s serveStartupRecoveryContainers) ManagedContainers(ctx context.Context) ([]runtimestartuprecovery.ManagedContainer, error) {
	if s.lifecycle == nil {
		return nil, fmt.Errorf("workspace lifecycle is not configured")
	}
	refs, err := s.lifecycle.ManagedResetContainerInventory(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]runtimestartuprecovery.ManagedContainer, 0, len(refs))
	for _, ref := range refs {
		out = append(out, runtimestartuprecovery.ManagedContainer{
			Name:  strings.TrimSpace(ref.Name),
			RunID: strings.TrimSpace(ref.RunID),
			Kind:  strings.TrimSpace(ref.Kind),
		})
	}
	return out, nil
}

func (s serveStartupRecoveryContainers) StopManagedContainer(ctx context.Context, name string) error {
	if s.lifecycle == nil {
		return fmt.Errorf("workspace lifecycle is not configured")
	}
	return s.lifecycle.StopManagedContainer(ctx, name)
}

func (noWorkspaceStartupRecoveryContainers) ManagedContainers(context.Context) ([]runtimestartuprecovery.ManagedContainer, error) {
	return nil, nil
}

func (noWorkspaceStartupRecoveryContainers) StopManagedContainer(context.Context, string) error {
	return fmt.Errorf("workspace lifecycle is not configured")
}

type storeBundle struct {
	Postgres                     *store.PostgresStore
	SQLDB                        *sql.DB
	RuntimeSQLDB                 *sql.DB
	Database                     apiv1.Pinger
	RuntimeLogStore              runtime.RuntimeLogPersistence
	RuntimeBlocker               string
	SchemaBootstrapper           store.SchemaBootstrapper
	EventStore                   runtimebus.EventStore
	PipelineStore                *runtimepipeline.WorkflowInstanceStore
	SessionRegistry              sessions.Registry
	ConversationStore            runtimellm.ConversationPersistence
	ManagerStore                 runtimemanager.ManagerPersistence
	ScheduleStore                runtimepipeline.SchedulePersistence
	MailboxMaterializer          runtimepipeline.MailboxWriteMaterializationStore
	MailboxStore                 runtimetools.MailboxPersistence
	ToolEntityStore              runtimetools.EntityPersistence
	HumanTaskStore               runtimetools.HumanTaskCardStore
	BudgetSpendStore             budgetspend.Store
	InboundStore                 runtime.InboundPersistence
	MailboxAPIStore              apiv1.MailboxAPIStore
	DecisionCards                decisioncard.Store
	ObservabilityStore           apiv1.ObservabilityReadStore
	AgentUsageStore              apiv1.AgentUsageReadStore
	AgentDeliveryLifecycleStore  apiv1.AgentDeliveryLifecycleReadStore
	RuntimeIngressStore          runtimeingress.Store
	IdempotencyStore             apiv1.APIIdempotencyStore
	StartupOwnership             runtimestartupownership.Store
	RunQuiescenceStore           runtimerunquiescence.ServeAbandonStore
	RunReadStore                 apiv1.RunReadStore
	EntityReadStore              apiv1.EntityReadStore
	AgentConversationReadStore   apiv1.AgentConversationReadStore
	RunBundleContextStore        apiv1.RunBundleContextStore
	BundleRuntimeCatalogStore    selectedBundleRuntimeCatalogStore
	BundleSourceCatalogStore     selectedBundleSourceCatalogStore
	RunBundleAvailabilityStore   selectedRunBundleAvailabilityStore
	StartupRecoveryStore         selectedStartupRecoveryStore
	RunStalledReader             runStalledReadStore
	APIOptionalCapabilityBuilder selectedAPIOptionalCapabilityBuilder
	RunForkRuntimeOwner          selectedRunForkRuntimeOwner
}

type sqlDBPinger struct {
	db *sql.DB
}

func (p sqlDBPinger) Ping(ctx context.Context) error {
	if p.db == nil {
		return errors.New("sql database is not configured")
	}
	return p.db.PingContext(ctx)
}

func selectedPostgresAPIOptionalCapabilityBuilder(pg *store.PostgresStore, stores storeBundle) selectedAPIOptionalCapabilityBuilder {
	if pg == nil {
		return nil
	}
	return func(req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		resetPlanner := runtimedestructivereset.InventoryPlanner{
			Reader: runtimedestructivereset.CompositeInventoryReader{
				Reader:     pg,
				Containers: req.Workspaces,
			},
		}
		runForkSourceLoader := runtimerunforkexecution.SelectedContractSourceLoader(runtimerunforkexecution.ContractBundleSourceLoader{
			RepoRoot:         req.RepoRoot,
			PlatformSpecPath: req.PlatformSpecPath,
		})
		if req.LoadedBundle.dbLoaded {
			runForkSourceLoader = runtimerunforkexecution.BundleCatalogSelectedContractSourceLoader{
				RepoRoot:         req.RepoRoot,
				PlatformSpecPath: req.RunningPlatformSpecPath,
				Store:            pg,
			}
		}
		return selectedAPICapabilities{
			BundleCatalog: pg,
			BundleDelete: &runtimebundledelete.Coordinator{
				Planner:            pg,
				Cleaner:            pg,
				Finalizer:          pg,
				Locks:              pg,
				ContainerInventory: req.Workspaces,
				Containers:         runtimedestructivereset.ManagedContainerStopper{Runtime: req.Workspaces},
				RuntimeQuiescer:    req.RuntimeContextManager,
			},
			ConversationForks:         pg,
			ConversationForkLifecycle: pg,
			RunForkAvailability:       pg,
			RunFork: apiv1.SelectedContractRunForkExecutor{
				ExecuteSelectedContractRunFork: selectedPostgresRunForkExecutionFunc(pg),
				SourceLoader:                   runForkSourceLoader,
				ContractSelection:              runtimerunforkadmission.SelectedContractSelection(req.Source, req.ContractsRoot),
				AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
					Config:              req.Config,
					EntityStore:         stores.ToolEntityStore,
					HumanTaskStore:      stores.HumanTaskStore,
					SessionRegistry:     stores.SessionRegistry,
					ConversationStore:   stores.ConversationStore,
					ScheduleStore:       stores.ScheduleStore,
					MailboxStore:        stores.MailboxStore,
					Workspace:           req.Workspaces,
					Credentials:         req.Credentials,
					ManagedCredentials:  req.ManagedCredentials,
					ProviderCredentials: req.ProviderCredentials,
				},
			},
			RuntimeContexts:  req.RuntimeContextManager,
			ResetCoordinator: &runtimedestructivereset.Coordinator{Planner: resetPlanner, Locks: pg},
			ResetQuiescer:    runtimedestructivereset.Quiescer{Store: pg},
			ResetCleaner:     runtimedestructivereset.Cleaner{Store: pg},
		}, nil
	}
}

func selectedSQLiteAPIOptionalCapabilityBuilder(sqliteStore *store.SQLiteRuntimeStore) selectedAPIOptionalCapabilityBuilder {
	if sqliteStore == nil {
		return nil
	}
	return func(req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
		return selectedAPICapabilities{
			BundleCatalog:             sqliteStore,
			ConversationForks:         sqliteStore,
			ConversationForkLifecycle: sqliteStore,
			RuntimeContexts:           req.RuntimeContextManager,
		}, nil
	}
}

func selectedPostgresRunForkExecutionFunc(pg *store.PostgresStore) apiv1.SelectedContractRunForkExecutionFunc {
	if pg == nil {
		return nil
	}
	return func(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
		req.Store = pg
		return runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, req)
	}
}

func selectedPostgresRunForkRuntimeOwner(pg *store.PostgresStore) selectedRunForkRuntimeOwner {
	if pg == nil {
		return selectedRunForkRuntimeOwner{}
	}
	return selectedRunForkRuntimeOwner{
		activateFunc: func(ctx context.Context, req runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error) {
			req.Store = pg
			return runtimerunforkexecution.ActivateSelectedContractRunFork(ctx, req)
		},
		materializeFunc: pg.MaterializeRunFork,
		executeFunc: func(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
			req.Store = pg
			return runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, req)
		},
		planFunc: pg.PlanRunFork,
	}
}

func selectedPostgresStoreBundle(pg *store.PostgresStore, cfg *config.Config) storeBundle {
	if pg == nil {
		return storeBundle{}
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	pg.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
	bundle := storeBundle{
		Postgres:                    pg,
		SQLDB:                       pg.DB,
		RuntimeSQLDB:                pg.DB,
		Database:                    pg,
		RuntimeLogStore:             pg,
		SchemaBootstrapper:          pg,
		EventStore:                  pg,
		PipelineStore:               runtimepipeline.NewWorkflowInstanceStore(pg.DB),
		SessionRegistry:             pg,
		ConversationStore:           pg,
		ManagerStore:                pg,
		ScheduleStore:               pg,
		MailboxMaterializer:         pg,
		MailboxStore:                pg,
		ToolEntityStore:             pg,
		HumanTaskStore:              pg,
		BudgetSpendStore:            pg,
		InboundStore:                pg,
		MailboxAPIStore:             pg,
		DecisionCards:               pg,
		ObservabilityStore:          pg,
		AgentUsageStore:             pg,
		AgentDeliveryLifecycleStore: pg,
		RuntimeIngressStore:         pg,
		IdempotencyStore:            pg,
		StartupOwnership:            pg,
		RunQuiescenceStore:          pg,
		RunReadStore:                pg,
		EntityReadStore:             pg,
		AgentConversationReadStore:  pg,
		RunBundleContextStore:       pg,
		BundleRuntimeCatalogStore:   pg,
		BundleSourceCatalogStore:    pg,
		RunBundleAvailabilityStore:  pg,
		StartupRecoveryStore:        pg,
		RunStalledReader:            pg,
		RunForkRuntimeOwner:         selectedPostgresRunForkRuntimeOwner(pg),
	}
	bundle.APIOptionalCapabilityBuilder = selectedPostgresAPIOptionalCapabilityBuilder(pg, bundle)
	return bundle
}

func (s storeBundle) runtimeStores() runtime.Stores {
	return s.facade().runtimeStores()
}

type serveRuntimeBundle struct {
	module           runtimepipeline.WorkflowModule
	bundle           *runtimecontracts.WorkflowContractBundle
	source           semanticview.Source
	contractsRoot    string
	platformSpecPath string
	runningSpecPath  string
	bootIdentity     runtimecontracts.BundleIdentity
	bundleSourceFact runtimecorrelation.BundleSourceFact
	dbLoaded         bool
	cleanup          func() error
}

type serveRuntimeBundleContext struct {
	loaded            serveRuntimeBundle
	stateStoreSummary string
	bundleSourceFact  runtimecorrelation.BundleSourceFact
	bootIdentity      runtimecontracts.BundleIdentity
	workspaceBackend  cliapp.WorkspaceBackendSelection
	workspaces        cliapp.ServeWorkspaceLifecycle
	validation        runtime.WorkflowContractValidationResult
	runtime           *runtime.Runtime
}

type serveRuntimeBundleContextRequest struct {
	Ctx                    context.Context
	Stores                 storeBundle
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
	UseStartupOwnership    bool
	UseStartupRecovery     bool
	RequireBundleScopeName bool
	RuntimeInstanceID      string
}

func (b serveRuntimeBundle) serveIdentityDetail() string {
	if b.dbLoaded && strings.TrimSpace(b.bundleSourceFact.BundleHash) != "" {
		return strings.TrimSpace(b.bundleSourceFact.BundleHash)
	}
	return strings.TrimSpace(b.bootIdentity.Fingerprint)
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
			if hash := strings.TrimSpace(bundle.bundleSourceFact.BundleHash); hash != "" {
				out = append(out, hash)
			}
		}
	}
	sort.Strings(out)
	return out
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

func serveConfigLoadDetail(configDetail string, resolvedPaths cliapp.CLIContractPlatformSpecPaths, opts cliapp.ServeOptions) string {
	parts := []string{"config=" + strings.TrimSpace(configDetail)}
	hashes, _ := cliapp.ServeBundleHashes(opts)
	if len(hashes) == 1 {
		parts = append(parts, "bundle_hash="+hashes[0])
	} else if len(hashes) > 1 {
		parts = append(parts, "bundle_hashes="+strings.Join(hashes, ","))
	} else {
		parts = append(parts, "contracts="+filepath.Clean(resolvedPaths.ContractsPath))
	}
	return strings.Join(parts, " ")
}

func servePreCatalogPlatformSpecPath(resolvedPaths cliapp.CLIContractPlatformSpecPaths, opts cliapp.ServeOptions) (string, error) {
	hashes, err := cliapp.ServeBundleHashes(opts)
	if err != nil {
		return "", err
	}
	if len(hashes) > 0 {
		return cliapp.EmbeddedPlatformSpecPath()
	}
	return resolvedPaths.PlatformSpecPath, nil
}

func loadServeRuntimeBundles(ctx context.Context, repo string, stores storeBundle, resolvedPaths cliapp.CLIContractPlatformSpecPaths, opts cliapp.ServeOptions) ([]serveRuntimeBundle, error) {
	hashes, err := cliapp.ServeBundleHashes(opts)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		out := make([]serveRuntimeBundle, 0, len(hashes))
		runningPlatformSpecPath, err := cliapp.EmbeddedPlatformSpecPath()
		if err != nil {
			return nil, fmt.Errorf("resolve embedded platform spec for bundle catalog admission: %w", err)
		}
		for _, hash := range hashes {
			loaded, err := loadServeRuntimeBundleFromCatalog(ctx, repo, stores, hash, runningPlatformSpecPath)
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
	loaded, err := loadServeRuntimeBundle(ctx, repo, stores, resolvedPaths, opts)
	if err != nil {
		return nil, err
	}
	return []serveRuntimeBundle{loaded}, nil
}

func loadServeRuntimeBundle(ctx context.Context, repo string, stores storeBundle, resolvedPaths cliapp.CLIContractPlatformSpecPaths, opts cliapp.ServeOptions) (serveRuntimeBundle, error) {
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
			return serveRuntimeBundle{}, fmt.Errorf("resolve embedded platform spec for bundle catalog admission: %w", err)
		}
		return loadServeRuntimeBundleFromCatalog(ctx, repo, stores, hashes[0], runningPlatformSpecPath)
	}
	contractsRoot, err := cliapp.NormalizeContractsRoot(resolvedPaths.ContractsPath)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	module, bundle, err := cliapp.NewSwarmWorkflowModule(repo, contractsRoot, resolvedPaths.PlatformSpecPath)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	bootIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("compute boot bundle identity: %w", err)
	}
	return serveRuntimeBundle{
		module:           module,
		bundle:           bundle,
		source:           semanticview.Wrap(bundle),
		contractsRoot:    contractsRoot,
		platformSpecPath: resolvedPaths.PlatformSpecPath,
		runningSpecPath:  resolvedPaths.PlatformSpecPath,
		bootIdentity:     bootIdentity,
	}, nil
}

func loadServeRuntimeBundleFromCatalog(ctx context.Context, repo string, stores storeBundle, bundleHash, runningPlatformSpecPath string) (serveRuntimeBundle, error) {
	catalog := stores.facade().bundleRuntimeCatalogStore()
	if catalog == nil {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: swarm serve --bundle-hash requires selected bundle catalog store")
	}
	if err := runtimecontracts.ValidateBundleHash(bundleHash); err != nil {
		return serveRuntimeBundle{}, err
	}
	record, err := catalog.LoadBundleCatalogRuntimeRecord(ctx, bundleHash)
	if errors.Is(err, store.ErrBundleNotFound) {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: bundle_hash %s is not present in bundles", bundleHash)
	}
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	runtimeSource, err := runtimecontracts.LoadBundleCatalogRuntimeSource(repo, runtimecontracts.BundleCatalogRuntimeLoadRequest{
		BundleHash:              record.BundleHash,
		ContentYAML:             record.ContentYAML,
		DataBlob:                record.DataBlob,
		RunningPlatformSpecPath: strings.TrimSpace(runningPlatformSpecPath),
	})
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			_ = runtimeSource.Cleanup()
		}
	}()
	module, source, err := cliapp.NewSwarmWorkflowModuleForBundle(runtimeSource.Bundle)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	bootIdentity, err := runtimecontracts.BootBundleIdentity(runtimeSource.Bundle)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("compute DB-loaded boot bundle identity: %w", err)
	}
	fact := runtimecorrelation.BundleSourceFact{
		BundleHash:   runtimeSource.BundleHash,
		BundleSource: storerunlifecycle.BundleSourcePersisted,
	}.Normalized()
	cleanupOnError = false
	return serveRuntimeBundle{
		module:           module,
		bundle:           runtimeSource.Bundle,
		source:           source,
		contractsRoot:    runtimeSource.ContractsRoot,
		platformSpecPath: runtimeSource.PlatformSpecPath,
		runningSpecPath:  strings.TrimSpace(runningPlatformSpecPath),
		bootIdentity:     bootIdentity,
		bundleSourceFact: fact,
		dbLoaded:         true,
		cleanup:          runtimeSource.Cleanup,
	}, nil
}

func prepareLoadedServeBundleSource(ctx context.Context, stores storeBundle, loaded serveRuntimeBundle, dev bool) (runtimecorrelation.BundleSourceFact, error) {
	if loaded.dbLoaded {
		if dev {
			return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("--bundle-hash is mutually exclusive with --dev")
		}
		fact := loaded.bundleSourceFact.Normalized()
		if fact.BundleSource != storerunlifecycle.BundleSourcePersisted || strings.TrimSpace(fact.BundleHash) == "" {
			return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("DB-loaded serve bundle source fact must be persisted with bundle_hash")
		}
		return fact, nil
	}
	if prepared := loaded.bundleSourceFact.Normalized(); strings.TrimSpace(prepared.BundleHash) != "" {
		return prepared, nil
	}
	return prepareServeBundleSource(ctx, stores, loaded.bundle, loaded.bootIdentity.Fingerprint, dev && !bundleHasStandingActivation(loaded.bundle))
}

func bundleHasStandingActivation(bundle *runtimecontracts.WorkflowContractBundle) bool {
	if bundle == nil {
		return false
	}
	for _, pkg := range bundle.PackageTree {
		for _, ref := range pkg.Manifest.Flows {
			if ref.HasStandingActivation() {
				return true
			}
		}
	}
	return false
}

func buildServeRuntimeBundleContext(req serveRuntimeBundleContextRequest) (serveRuntimeBundleContext, error) {
	loaded := req.Loaded
	stateStoreSummary := strings.TrimSpace(req.StateStoreSummary)
	if stateStoreSummary == "" {
		var err error
		stateStoreSummary, err = initializeStateStores(req.Ctx, req.Stores, loaded.bundle)
		if err != nil {
			return serveRuntimeBundleContext{}, err
		}
	}
	bundleSourceFact, err := prepareLoadedServeBundleSource(req.Ctx, req.Stores, loaded, req.Options.Dev)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("prepare bundle source: %w", err)
	}
	bootIdentity := loaded.bootIdentity
	bootIdentity.BundleHash = strings.TrimSpace(bundleSourceFact.BundleHash)
	workspaces, err := cliapp.ConfiguredWorkspaceLifecycleForServe(req.Stores.SQLDB, req.Config, loaded.contractsRoot, loaded.source, req.MountSources, req.WorkspaceBackend)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("configure workspaces: %w", err)
	}
	if workspaces == nil && !req.WorkspaceBackend.NoWorkspace {
		return serveRuntimeBundleContext{}, fmt.Errorf("workspace lifecycle is not configured")
	}
	if req.RequireBundleScopeName {
		if scoper, ok := workspaces.(interface{ SetBundleScope(string) }); ok && scoper != nil {
			scoper.SetBundleScope(bundleSourceFact.BundleHash)
		}
	}
	if workspaces != nil {
		if err := workspaces.ValidateSource(req.Ctx, loaded.source); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("validate workspaces: %w", err)
		}
		if err := workspaces.EnsurePrereqs(req.Ctx); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("prepare workspaces: %w", err)
		}
		if err := workspaces.EnsureSystemWorkspaces(req.Ctx); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("ensure system workspaces: %w", err)
		}
	}
	validationOpts := runtime.DefaultWorkflowContractValidationOptions(req.Credentials)
	validationOpts.ManagedCredentials = req.ManagedCredentials
	validationOpts.ProviderTriggerCatalog = req.ProviderTriggerCatalog
	validationOpts.ChannelPlans = req.ChannelPlans
	validationOpts.ChannelOutboundBindings = req.ChannelBindings
	profile, err := req.Config.LLMBackendProfile()
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("resolve llm backend profile for workflow validation: %w", err)
	}
	validationOpts.ExecutionMode, err = llmselection.ExecutionModeForProfile(profile)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("resolve workflow execution mode: %w", err)
	}
	validation, err := runtime.ValidateWorkflowContractSurface(req.Ctx, loaded.source, validationOpts)
	if err != nil {
		return serveRuntimeBundleContext{}, err
	}
	if runtimepipeline.SourceUsesArtifactRepoCommit(loaded.source) {
		if _, err := runtimepipeline.EnsureArtifactRepoRootWritable(""); err != nil {
			return serveRuntimeBundleContext{}, fmt.Errorf("artifact repo root startup validation failed: %w", err)
		}
	}
	runtimeStores := req.Stores.runtimeStores()
	if !req.UseStartupOwnership {
		runtimeStores.StartupOwnership = nil
	}
	rt, err := runtime.NewRuntime(req.Ctx, runtime.RuntimeDeps{
		Config: req.Config,
		Stores: runtimeStores,
		Options: runtime.RuntimeOptions{
			SelfCheck:                        req.Options.SelfCheck,
			WorkflowModule:                   loaded.module,
			WorkspaceLifecycle:               workspaces,
			EnableToolGateway:                req.EnableToolGateway,
			ToolGatewayBinding:               req.ToolGatewayBinding,
			BundleFingerprint:                bootIdentity.Fingerprint,
			BundleSourceFact:                 bundleSourceFact,
			RuntimeInstanceID:                strings.TrimSpace(req.RuntimeInstanceID),
			Credentials:                      req.Credentials,
			ManagedCredentials:               req.ManagedCredentials,
			ProviderCredentials:              req.ProviderCredentials,
			ProviderTriggerCatalog:           req.ProviderTriggerCatalog,
			ChannelPlans:                     req.ChannelPlans,
			ChannelOutboundBindings:          req.ChannelBindings,
			BootStartedAt:                    req.BootStartedAt,
			BootProgress:                     req.BootProgress,
			SystemContainers:                 systemWorkspaceContainers(workspaces),
			DisablePersistentStartupRecovery: !req.UseStartupRecovery,
			TestEntityStateHook:              req.Options.TestEntityStateHook,
			TestWorkflowNodeHandlerStartHook: req.Options.TestWorkflowNodeHandlerStartHook,
			TestLifecycleProbe:               req.Options.TestLifecycleProbe,
			LLMRuntime:                       req.Options.TestLLMRuntime,
			TestOutboxSweeperConfig:          req.Options.TestOutboxSweeperConfig,
		},
	})
	if err != nil {
		return serveRuntimeBundleContext{}, err
	}
	return serveRuntimeBundleContext{
		loaded:            loaded,
		stateStoreSummary: stateStoreSummary,
		bundleSourceFact:  bundleSourceFact,
		bootIdentity:      bootIdentity,
		workspaceBackend:  req.WorkspaceBackend,
		workspaces:        workspaces,
		validation:        validation,
		runtime:           rt,
	}, nil
}

func buildForkChatSandboxLLMRuntime(cfg *config.Config, workspaces workspace.Resolver, binding toolgateway.Binding, providerCredentials runtimecredentials.Store, effectStore runtimeeffects.Store, projector runtimeeffects.CompletionSpendProjector) (runtimellm.Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	return runtimellm.RuntimeFactory{
		Cfg:                  cfg,
		Sessions:             sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL),
		LockOwner:            "forkchat-sandbox",
		Workspaces:           workspaces,
		ToolGateway:          binding,
		Credentials:          providerCredentials,
		CompletionController: runtimeeffects.NewCompletionController(effectStore, projector),
	}.Build()
}

func Run(ctx context.Context, repo string, opts cliapp.ServeOptions) int {
	ctx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	bootStartedAt := time.Now().UTC()
	runtimeInstanceID := uuid.NewString()
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeInstanceID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
	presenter := newServeLifecyclePresenter(opts)
	defer presenter.finish()
	if opts.NoFeed && !opts.Dev {
		presenter.fail(1, "serve_admission", fmt.Errorf("--no-feed requires --dev"))
		return 2
	}
	presenter.boot(1, "process_start", "ok", "")
	apiAuth, err := cliapp.ResolveServeAPIAuth(repo, opts)
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
	resolvedPaths, err := cliapp.ResolveCLIContractPlatformSpecPaths(repo, cliapp.CLIContractPlatformSpecPathOptions{
		ContractsPath:    opts.ContractsPath,
		PlatformSpecPath: opts.PlatformSpecPath,
		ConfigPath:       opts.ConfigPath,
	})
	if err != nil {
		presenter.fail(2, "config_load", err)
		return 1
	}
	projectContextRegistration, err := cliapp.PrepareServeProjectContextRegistration(ctx, repo, opts, resolvedPaths)
	if err != nil {
		presenter.fail(2, "serve_admission", err)
		return 3
	}
	defer projectContextRegistration.Release()

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
	providerPackLoad, err := cliapp.LoadConfiguredProviderTriggerPacks(repo, cfgResult)
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
	swarmDir, err := cliapp.ResolveServeContextRegistrationSwarmDir(opts)
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
	stores, err := buildStoresForServe(ctx, storeSelection, cfg)
	if err != nil {
		presenter.fail(3, "db_connection", err)
		return 1
	}
	presenter.recordStore(storeSelection)
	storeFacade := stores.facade()
	storeClosed := false
	defer func() {
		if storeClosed {
			return
		}
		if err := storeFacade.closeWithError(); err != nil {
			presenter.cleanupFailure("store shutdown", err)
		}
	}()
	if stores.SchemaBootstrapper != nil {
		preCatalogPlatformSpecPath, err := servePreCatalogPlatformSpecPath(resolvedPaths, opts)
		if err != nil {
			presenter.fail(4, "bundle_load", err)
			return 1
		}
		if _, err := initializeServePlatformStateStores(ctx, stores, preCatalogPlatformSpecPath); err != nil {
			presenter.fail(4, "bundle_load", err)
			return 1
		}
	}
	loadedBundles, err := loadServeRuntimeBundles(ctx, repo, stores, resolvedPaths, opts)
	if err != nil {
		detail := err.Error()
		if _, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
			detail = cliapp.FormatCLIAPIError(err)
		}
		presenter.fail(4, "bundle_load", errors.New(detail))
		return 1
	}
	if len(loadedBundles) == 0 {
		presenter.fail(4, "bundle_load", errors.New("no bundle contexts loaded"))
		return 1
	}
	bundleSourcesCleaned := false
	cleanupLoadedBundleSources := func() error {
		var cleanupErr error
		for _, loaded := range loadedBundles {
			if loaded.cleanup != nil {
				cleanupErr = errors.Join(cleanupErr, loaded.cleanup())
			}
		}
		return cleanupErr
	}
	defer func() {
		if bundleSourcesCleaned {
			return
		}
		if err := cleanupLoadedBundleSources(); err != nil {
			presenter.cleanupFailure("bundle source cleanup", err)
		}
	}()
	loadedBundle := loadedBundles[0]
	source := loadedBundle.source
	resolvedPlatformSpecPath := loadedBundle.platformSpecPath
	presenter.boot(4, "bundle_load", "ok", serveBootBundleLoadDetail(serveRuntimeBundleIdentitiesDetail(loadedBundles), source))
	primaryWorkspaceBackend, err := cliapp.DecideWorkspaceBackend(workspaceBackendPreference, cfg, source)
	if err != nil {
		presenter.failWithDiagnostic(5, "runtime_context", err, func(out io.Writer) bool {
			cliapp.WriteWorkspaceBackendDecisionFailure(out, "serve", err)
			return true
		})
		return 3
	}
	managedCredentialStore, err := cliapp.BuildManagedCredentialStore()
	if err != nil {
		presenter.fail(5, "managed_credentials", err)
		return 1
	}
	providerCredentialStore, err := cliapp.BuildProviderCredentialStore()
	if err != nil {
		presenter.fail(5, "provider_credentials", err)
		return 1
	}
	channelPacks, err := cliapp.LoadConfiguredChannelPacks(ctx, repo, cfgResult, source.PlatformSpec(), providerPackLoad.Catalog, providerCredentialStore, managedCredentialStore)
	if err != nil {
		presenter.fail(5, "channel_packs", err)
		return 1
	}
	if cliapp.ShouldRunServeLocalClaudeCLIPreflight(opts) {
		preflight := cliapp.RunServeLocalClaudeCLIPreflight(ctx, repo, opts, cfg, resolvedPaths, workspaceBackendPreference, mountSources, providerPackLoad.Loaded, providerPackLoad.Catalog, channelPacks)
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
	if stores.SchemaBootstrapper != nil {
		summaries, err := initializeLoadedServeRuntimeStateStores(ctx, stores, loadedBundles)
		if err != nil {
			presenter.fail(4, "state_store_schema", err)
			return 1
		}
		stateStoreSummaries = summaries
	}
	for i := range loadedBundles {
		fact, err := prepareLoadedServeBundleSource(ctx, stores, loadedBundles[i], opts.Dev)
		if err != nil {
			presenter.fail(4, "bundle_source", err)
			return 1
		}
		loadedBundles[i].bundleSourceFact = fact
	}
	loadedBundle = loadedBundles[0]
	pinnedBundleHashes := servePinnedBundleHashes(loadedBundles)
	credentialStore, err := cliapp.BuildCredentialStore()
	if err != nil {
		presenter.fail(5, "credentials", err)
		return 1
	}
	apiListener, err := cliapp.ListenServeHTTPListener("api", opts.APIListenAddr)
	if err != nil {
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}
	defer apiListener.Close()
	mcpListener, err := cliapp.ListenServeHTTPListener("mcp", opts.MCPListenAddr)
	if err != nil {
		_ = apiListener.Close()
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}
	defer mcpListener.Close()
	toolGatewayBinding, err := createServeToolGatewayBinding(mcpListener.Addr())
	if err != nil {
		_ = mcpListener.Close()
		_ = apiListener.Close()
		presenter.fail(20, "http_listener_bind", err)
		return 3
	}

	runtimeContexts := make([]serveRuntimeBundleContext, 0, len(loadedBundles))
	workspaceLabels := serveLifecycleWorkspaceLabels(loadedBundles)
	for i, loaded := range loadedBundles {
		contextToolGatewayBinding := toolgateway.Binding{}
		if i == 0 {
			contextToolGatewayBinding = toolGatewayBinding
		}
		workspaceBackend, err := cliapp.DecideWorkspaceBackend(workspaceBackendPreference, cfg, loaded.source)
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
		contextDef, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
			Ctx:                    ctx,
			Stores:                 stores,
			Config:                 cfg,
			Loaded:                 loaded,
			StateStoreSummary:      serveStateStoreSummaryAt(stateStoreSummaries, i),
			Options:                opts,
			MountSources:           mountSources,
			WorkspaceBackend:       workspaceBackend,
			Credentials:            credentialStore,
			ManagedCredentials:     managedCredentialStore,
			ProviderCredentials:    providerCredentialStore,
			ProviderTriggerCatalog: providerPackLoad.Catalog,
			ChannelPlans:           channelPacks.Plans,
			ChannelBindings:        channelPacks.Bindings,
			BootStartedAt:          bootStartedAt,
			BootProgress:           bootProgress,
			EnableToolGateway:      i == 0,
			ToolGatewayBinding:     contextToolGatewayBinding,
			UseStartupOwnership:    i == 0,
			UseStartupRecovery:     len(loadedBundles) == 1,
			RequireBundleScopeName: len(loadedBundles) > 1,
			RuntimeInstanceID:      runtimeInstanceID,
		})
		if err != nil {
			presenter.failWithDiagnostic(5, "runtime_context", err, func(out io.Writer) bool {
				return cliapp.WriteWorkspacePrerequisiteFailure(out, "serve", err)
			})
			return 1
		}
		runtimeContexts = append(runtimeContexts, contextDef)
	}
	primaryContext := runtimeContexts[0]
	source = primaryContext.loaded.source
	bundle := primaryContext.loaded.bundle
	contractsRoot := primaryContext.loaded.contractsRoot
	bootBundleIdentity := primaryContext.bootIdentity
	workspaces := primaryContext.workspaces
	primaryWorkspaceBackend = primaryContext.workspaceBackend
	rt := primaryContext.runtime
	if err := rt.PrepareInitialStartupOwnership(ctx); err != nil {
		presenter.fail(5, "startup_ownership_lease", err)
		return 3
	}
	defer func() {
		if err := rt.ReleasePreparedStartupOwnership(context.Background()); err != nil {
			presenter.cleanupFailure("startup ownership release", err)
		}
	}()
	bootReport := primaryContext.validation.BootReport
	stateStoreSummary := serveRuntimeStateStoreSummary(runtimeContexts)
	preflightContexts, err := plannedServeRuntimeContexts(runtimeContexts)
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	installedTriggerSubjects, err := providerPackLoad.Catalog.InstalledCapabilitySubjects()
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	admissionState := runtime.ProcessAdmissionState{GenerationID: providerPackLoad.Catalog.GenerationID(), InstalledSubjects: installedTriggerSubjects}
	if err := runtime.ValidateRuntimeContextSetWithAdmission(admissionState, preflightContexts...); err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	if err := reconcileServeStandingServices(ctx, runtimeContexts); err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	if opts.AbandonActiveRuns {
		if stores.RunQuiescenceStore == nil {
			presenter.fail(5, "run_quiescence", errors.New("selected store active-run quiescence owner is required"))
			return 3
		}
		result, err := stores.RunQuiescenceStore.ApplyServeAbandonActiveRunQuiescence(ctx, time.Now().UTC())
		if err != nil {
			presenter.fail(5, "run_quiescence", err)
			return 3
		}
		presenter.recordAbandonedWork(len(result.Runs), len(result.Deliveries), result.PipelineReceiptCount)
	}
	if exitCode := runServeUnavailableBundleStartupRecovery(ctx, storeFacade, cfg, loadedBundle, source, mountSources, primaryWorkspaceBackend, presenter); exitCode != 0 {
		return exitCode
	}
	if err := enforceServeBundleMatchAdmissionForHashes(ctx, storeFacade.runBundleAvailabilityStore(), serveRuntimeBundleIdentitiesDetail(loadedBundles), opts.RequireBundleMatch, pinnedBundleHashes); err != nil {
		presenter.fail(5, "bundle_match_admission", err)
		return 3
	}
	runtimeContextManager, err := runtime.NewRuntimeContextManagerWithAdmission(runtimeContextAvailabilityReader(stores), admissionState)
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}

	effectStore, ok := stores.ManagerStore.(runtimeeffects.Store)
	if !ok || effectStore == nil {
		presenter.fail(5, "forkchat_sandbox", fmt.Errorf("selected runtime store does not implement completion execution authority"))
		return 1
	}
	forkChatLLM, err := buildForkChatSandboxLLMRuntime(cfg, workspaces, toolGatewayBinding, providerCredentialStore, effectStore, rt.Budget)
	if err != nil {
		presenter.fail(5, "forkchat_sandbox", err)
		return 1
	}

	var ready atomic.Bool
	supervisor := newRuntimeProjectSupervisor(repo, resolvedPlatformSpecPath, cfg, stores, &ready, mountSources, workspaceBackendPreference, credentialStore, providerCredentialStore, providerPackLoad.Catalog, contractsRoot, bundle, source, rt, opts.Dev)
	supervisor.loadProviderCatalog = func() (*providertriggers.CatalogSnapshot, error) {
		return providerPackLoad.Reload()
	}
	supervisor.SetChannelPackLoader(func(loadCtx context.Context, candidateSource semanticview.Source, candidateCatalog *providertriggers.CatalogSnapshot) (cliapp.ChannelPackLoad, error) {
		return cliapp.LoadConfiguredChannelPacks(loadCtx, repo, cfgResult, candidateSource.PlatformSpec(), candidateCatalog, providerCredentialStore, managedCredentialStore)
	})
	supervisor.replacementShutdown = runtime.ShutdownOptions{Grace: opts.ShutdownGrace}
	supervisor.runtimeLifetime = ctx
	supervisor.SetRuntimeContextManager(runtimeContextManager, primaryContext.bundleSourceFact, primaryContext.bootIdentity)
	if len(pinnedBundleHashes) > 0 {
		supervisor.DisableSourceReplacement("swarm serve --bundle-hash pins persisted bundle contexts for the process; dynamic project reload is not supported in this mode")
	}
	var apiServer, mcpServer *http.Server
	var storyFollower *serveAuthorActivityFollower
	defer func() {
		storyFollower.StopAndWait()
		shutdownErr := shutdownHTTPServer("api", apiServer)
		shutdownErr = errors.Join(shutdownErr, shutdownHTTPServer("mcp", mcpServer))
		shutdownErr = errors.Join(shutdownErr, closeAdditionalServeRuntimeContexts(context.Background(), runtimeContexts[1:], runtimeContextManager, opts))
		shutdownErr = errors.Join(shutdownErr, closeServeRuntime(context.Background(), supervisor, opts, workspaces))
		shutdownErr = errors.Join(shutdownErr, cleanupLoadedBundleSources())
		bundleSourcesCleaned = true
		shutdownErr = errors.Join(shutdownErr, storeFacade.closeWithError())
		storeClosed = true
		presenter.shutdown(shutdownErr)
	}()
	apiStoreCaps, err := storeFacade.apiCapabilities(selectedAPICapabilityRequest{
		RepoRoot:                repo,
		PlatformSpecPath:        resolvedPlatformSpecPath,
		RunningPlatformSpecPath: strings.TrimSpace(loadedBundle.runningSpecPath),
		LoadedBundle:            loadedBundle,
		RuntimeContexts:         runtimeContexts,
		Source:                  source,
		ContractsRoot:           contractsRoot,
		Config:                  cfg,
		Workspaces:              workspaces,
		Credentials:             credentialStore,
		ManagedCredentials:      managedCredentialStore,
		ProviderCredentials:     providerCredentialStore,
		RuntimeContextManager:   runtimeContextManager,
	})
	if err != nil {
		presenter.fail(5, "runtime_context", err)
		return 1
	}
	apiReadOptions := apiv1.OperatorReadOptions{
		RepoRoot:         repo,
		PlatformSpecPath: resolvedPlatformSpecPath,
		Ready: func() bool {
			return ready.Load()
		},
		Database:                  apiStoreCaps.Database,
		Runs:                      apiStoreCaps.Runs,
		Observability:             apiStoreCaps.Observability,
		Entities:                  apiStoreCaps.Entities,
		AgentConversations:        apiStoreCaps.AgentConversations,
		AgentDeliveryLifecycle:    stores.AgentDeliveryLifecycleStore,
		AgentUsage:                stores.AgentUsageStore,
		BundleCatalog:             apiStoreCaps.BundleCatalog,
		BundleDelete:              apiStoreCaps.BundleDelete,
		ConversationForks:         apiStoreCaps.ConversationForks,
		ConversationForkLifecycle: apiStoreCaps.ConversationForkLifecycle,
		ForkChatExecutor:          cliapp.NewWorkspaceAdmittedForkChatExecutor(apiv1.NewLLMForkChatExecutor(forkChatLLM), cfg, primaryWorkspaceBackend),
		RunBundleContext:          apiStoreCaps.RunBundleContext,
		TestSetup:                 apiStoreCaps.TestSetup,
		RunForkAvailability:       apiStoreCaps.RunForkAvailability,
		RunFork:                   apiStoreCaps.RunFork,
		AgentControl:              dashboardDynamicAgentControl{supervisor: supervisor},
		Mailbox:                   stores.MailboxAPIStore,
		DecisionCards:             stores.DecisionCards,
		DecisionAuthority:         stores.PipelineStore,
		Idempotency:               stores.IdempotencyStore,
		Events:                    rt.Bus,
		RunControl:                rt.RunControl,
		StandingServices:          &serveStandingServiceController{store: rt.Stores.PipelineStore, contexts: runtimeContexts, manager: runtimeContextManager},
		RuntimeIngress:            rt.RuntimeIngress,
		RuntimeContexts:           apiStoreCaps.RuntimeContexts,
		ResetCoordinator:          apiStoreCaps.ResetCoordinator,
		ResetQuiescer:             apiStoreCaps.ResetQuiescer,
		ResetCleaner:              apiStoreCaps.ResetCleaner,
		ResetContainers:           runtimedestructivereset.ManagedContainerStopper{Runtime: workspaces},
		Source:                    source,
		Bundle:                    bootBundleIdentity,
		RuntimeIdentity: apiv1.RuntimeIdentityResult{
			RuntimeInstanceID:   runtimeInstanceID,
			StartedAt:           bootStartedAt.Format(time.RFC3339Nano),
			APIVersion:          "v1",
			SupportedTransports: []string{"tcp"},
		},
	}
	apiV1Handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: resolvedPlatformSpecPath,
		AuthTokens:       apiAuth.Tokens,
		Handlers:         apiv1.OperatorReadHandlers(apiReadOptions),
		Subscriptions:    apiv1.OperatorSubscriptions(apiReadOptions),
	})
	if err != nil {
		presenter.fail(20, "api_initialization", err)
		return 1
	}
	var inboundHandler http.Handler
	if rt.InboundGateway != nil {
		inboundHandler = runtimeProcessInboundHandler{contexts: runtimeContextManager}
	}
	apiServer = newAPIServer(&ready, apiV1Handler, inboundHandler, ctx)
	mcpServer = newMCPServer(rt.ToolGateway)
	if err := projectContextRegistration.WriteFinal(runtimeInstanceID, apiListener.Addr(), apiAuth, resolvedPaths, storeSelection, mountSources); err != nil {
		presenter.fail(20, "context_registry", err)
		return 3
	}
	defer projectContextRegistration.Unregister()
	runtimeFailure := func(subject string, err error) {
		presenter.runtimeFailure(subject, err)
		cancelServe()
	}
	go serveHTTPServer("api", apiServer, apiListener, runtimeFailure)
	go serveHTTPServer("mcp", mcpServer, mcpListener, runtimeFailure)
	presenter.recordBootWarnings(bootReport)
	if err := startServeRuntimeContexts(ctx, runtimeContexts, runtimeContextManager); err != nil {
		presenter.fail(22, "ready", err)
		return 1
	}
	if err := reportServeStandingReadiness(ctx, rt.Stores.PipelineStore, opts.Output); err != nil {
		presenter.fail(22, "ready", err)
		return 1
	}
	if opts.TestRuntimeReadyHook != nil {
		opts.TestRuntimeReadyHook(rt)
	}
	if opts.TestRuntimeContextsReadyHook != nil {
		opts.TestRuntimeContextsReadyHook(runtimeContextManager)
	}
	startServeRunStalledEscalation(ctx, stores, runtimeContexts, rt.Bus)
	presenter.boot(20, "http_listener_bind", "ok", fmt.Sprintf("api_listener=%s api_routes=%s mcp_listener=%s mcp_routes=%s", apiListener.Addr(), serveAPIRoutes, mcpListener.Addr(), serveMCPRoutes))
	if err := waitForServeHealthEndpoints(ctx, apiListener.Addr()); err != nil {
		presenter.fail(21, "health_endpoints_respond", err)
		return 1
	}
	presenter.boot(21, "health_endpoints_respond", "ok", serveReadinessRoutes)
	standing, err := serveReadyStandingIngress(ctx, runtimeContextManager, providerCredentialStore, apiListener.Addr())
	if err != nil {
		presenter.fail(22, "ready", err)
		return 1
	}
	if opts.TestBeforeReadinessCommit != nil {
		if err := opts.TestBeforeReadinessCommit(); err != nil {
			presenter.fail(22, "ready", err)
			return 1
		}
	}
	readyAfter := time.Since(bootStartedAt)
	presenter.boot(22, "ready", "ok", fmt.Sprintf("total=%s state_stores=%s", readyAfter.Round(time.Millisecond), strings.TrimSpace(stateStoreSummary)))
	flowCount, agentCount, toolCount := serveLifecycleSourceCounts(runtimeContexts)
	feedEnabled := opts.Dev && !opts.NoFeed
	storyReader := serveAuthorActivityReaderFromStores(stores)
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
	if !presenter.commitReady(serveLifecycleReadyFacts{
		ProjectName: serveLifecycleProjectName(localState, loadedBundles),
		BundleCount: len(loadedBundles),
		FlowCount:   flowCount,
		AgentCount:  agentCount,
		ToolCount:   toolCount,
		APIListener: addrString(apiListener.Addr()),
		MCPListener: addrString(mcpListener.Addr()),
		ReadyAfter:  readyAfter,
		Standing:    standing,
	}, func() { ready.Store(true) }) {
		return 1
	}
	if feedEnabled {
		if err := presenter.writeFeedReady(); err != nil {
			presenter.storyWarning(err)
		} else {
			storyFollower = newServeAuthorActivityFollower(ctx, storyReader, presenter, runtimeInstanceID, runtimeContextManager, storyHead, runtimeauthoractivity.NewHumanRenderer(serveAuthorActivityRenderOptions(opts.Output, opts.NoColor)))
		}
	}

	<-ctx.Done()
	ready.Store(false)
	return 0
}

func reconcileServeStandingServices(ctx context.Context, contexts []serveRuntimeBundleContext) error {
	var owner *runtimepipeline.WorkflowInstanceStore
	var candidates []runtimepipeline.StandingServiceCandidate
	for _, contextDef := range contexts {
		if contextDef.runtime == nil || contextDef.runtime.Stores.PipelineStore == nil {
			continue
		}
		if owner == nil {
			owner = contextDef.runtime.Stores.PipelineStore
		} else if owner != contextDef.runtime.Stores.PipelineStore {
			return fmt.Errorf("standing service reconciliation requires one selected-store owner")
		}
		planned, err := contextDef.runtime.PlanStandingServiceCandidates()
		if err != nil {
			return err
		}
		candidates = append(candidates, planned...)
	}
	if owner == nil {
		return nil
	}
	_, err := owner.ReconcileStandingServiceSet(ctx, candidates)
	return err
}

type serveStandingServiceController struct {
	store    *runtimepipeline.WorkflowInstanceStore
	contexts []serveRuntimeBundleContext
	manager  *runtime.RuntimeContextManager
	mu       sync.Mutex
}

func (c *serveStandingServiceController) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	if c.store == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service owner is not configured")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	owner, err := c.closeAndDrain(ctx, operation.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	result, err := c.store.SuspendStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, errors.Join(err, c.restoreAdmission(owner, operation.ServiceID))
	}
	return result, nil
}

func (c *serveStandingServiceController) ResumeStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	if c.store == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service owner is not configured")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result, err := c.store.ResumeStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if err := c.publishActiveService(ctx, result.ServiceID); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	if owner, err := c.runtimeForStandingService(result.ServiceID); err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	} else if owner.InboundGateway != nil {
		if err := owner.InboundGateway.ReopenStandingServiceAdmission(result.ServiceID); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, c.failClosedAfterReopen(result.ServiceID, err)
		}
	}
	return result, nil
}

func (c *serveStandingServiceController) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	if c.store == nil {
		return runtimepipeline.StandingServiceReconciliation{}, fmt.Errorf("standing service owner is not configured")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	owner, err := c.closeAndDrain(ctx, operation.ServiceID)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, err
	}
	result, err := c.store.ResetStandingService(ctx, operation)
	if err != nil {
		return runtimepipeline.StandingServiceReconciliation{}, errors.Join(err, c.restoreAdmission(owner, operation.ServiceID))
	}
	if result.EffectiveState == "active" {
		if err := c.publishActiveService(ctx, result.ServiceID); err != nil {
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

func (c *serveStandingServiceController) closeAndDrain(ctx context.Context, serviceID string) (*runtime.Runtime, error) {
	owner, err := c.runtimeForStandingService(serviceID)
	if err != nil {
		return nil, err
	}
	if owner.InboundGateway != nil {
		if err := owner.InboundGateway.CloseStandingServiceAdmission(serviceID); err != nil {
			return nil, err
		}
	}
	if c.manager != nil {
		if err := c.manager.SuppressStandingServiceTargets(serviceID); err != nil {
			return nil, errors.Join(err, c.restoreAdmission(owner, serviceID))
		}
	}
	if owner.InboundGateway != nil {
		if err := owner.InboundGateway.WaitForStandingServiceAdmission(ctx, serviceID); err != nil {
			return nil, errors.Join(err, c.restoreAdmission(owner, serviceID))
		}
	}
	if err := owner.WaitForQuiescence(ctx); err != nil {
		return nil, errors.Join(err, c.restoreAdmission(owner, serviceID))
	}
	return owner, nil
}

func (c *serveStandingServiceController) restoreAdmission(owner *runtime.Runtime, serviceID string) error {
	if c.manager != nil {
		if err := c.manager.RestoreStandingServiceTargets(serviceID); err != nil {
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

func (c *serveStandingServiceController) runtimeForStandingService(serviceID string) (*runtime.Runtime, error) {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return nil, fmt.Errorf("standing service_id is required")
	}
	var owner *runtime.Runtime
	for _, contextDef := range c.contexts {
		if contextDef.runtime == nil {
			continue
		}
		candidates, err := contextDef.runtime.PlanStandingServiceCandidates()
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			if candidate.ServiceID != serviceID {
				continue
			}
			if owner != nil && owner != contextDef.runtime {
				return nil, fmt.Errorf("standing service %s has more than one loaded runtime owner", serviceID)
			}
			owner = contextDef.runtime
		}
	}
	if owner == nil {
		return nil, &runtimepipeline.StandingServiceError{ServiceID: serviceID, Err: runtimepipeline.ErrStandingServiceNotFound}
	}
	return owner, nil
}

func (c *serveStandingServiceController) publishActiveService(ctx context.Context, serviceID string) error {
	for _, contextDef := range c.contexts {
		if contextDef.runtime == nil {
			continue
		}
		candidates, err := contextDef.runtime.PlanStandingServiceCandidates()
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			if candidate.ServiceID != serviceID {
				continue
			}
			targets, _, err := contextDef.runtime.EnsureStandingServiceTargets(ctx, serviceID)
			if err != nil {
				return err
			}
			if c.manager != nil {
				return c.manager.PublishStandingServiceTargets(serviceID, targets)
			}
			return nil
		}
	}
	return fmt.Errorf("standing service %s is not declared by a loaded runtime context", serviceID)
}

func reportServeStandingReadiness(ctx context.Context, owner *runtimepipeline.WorkflowInstanceStore, out io.Writer) error {
	if owner == nil {
		return nil
	}
	statuses, err := owner.ListStandingServiceStatuses(ctx)
	if err != nil {
		return err
	}
	for _, status := range statuses {
		switch status.EffectiveState {
		case "active":
			if status.PublicationState != "published" {
				return fmt.Errorf("standing service %s is active but publication is %s", status.ServiceID, status.PublicationState)
			}
			if out != nil {
				fmt.Fprintf(out, "standing service %s %s run=%s generation=%d source=%s\n", status.ServiceID, status.Transition, status.RunID, status.Generation, status.BundleHash)
			}
		case "suspended":
			if out != nil {
				fmt.Fprintf(out, "standing service %s suspended by=%s at=%s reason=%s resume=`swarm standing resume %s`\n", status.ServiceID, status.OverrideActor, status.OverrideAt.Format(time.RFC3339), status.OverrideReason, status.ServiceID)
			}
		case "orphaned":
			if out != nil {
				fmt.Fprintf(out, "standing service %s orphaned declaration_removed=true run=%s generation=%d timers=quiesced\n", status.ServiceID, status.RunID, status.Generation)
			}
		default:
			return fmt.Errorf("standing service %s has unsupported effective state %q", status.ServiceID, status.EffectiveState)
		}
	}
	return nil
}

func runtimeContextAvailabilityReader(stores storeBundle) runtime.RunBundleAvailabilityReader {
	if stores.RunBundleAvailabilityStore != nil {
		return stores.RunBundleAvailabilityStore
	}
	if reader, ok := stores.RunBundleContextStore.(runtime.RunBundleAvailabilityReader); ok {
		return reader
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
			BundleHash:       contextDef.bundleSourceFact.BundleHash,
			BundleSourceFact: contextDef.bundleSourceFact,
			BundleIdentity:   contextDef.bootIdentity,
			Source:           contextDef.loaded.source,
			ContractsRoot:    contextDef.loaded.contractsRoot,
			PlatformSpecPath: contextDef.loaded.platformSpecPath,
			Runtime:          contextDef.runtime,
			StandingTargets:  targets,
		})
	}
	return planned, nil
}

func serveReadyStandingIngress(ctx context.Context, manager *runtime.RuntimeContextManager, credentials runtimecredentials.Store, apiAddr net.Addr) ([]serveLifecycleIngressFact, error) {
	if manager == nil || apiAddr == nil {
		return nil, nil
	}
	targets := map[string]runtime.StandingTarget{}
	for _, contextDef := range manager.LoadedContexts() {
		for _, target := range contextDef.StandingTargets {
			lookup := manager.LookupIngress(target.Alias, target.Provider)
			if !lookup.Loaded() || lookup.Target.ServiceID != target.ServiceID {
				continue
			}
			key := serveStandingIngressKey(target.BundleHash, target.Alias, target.Provider)
			if _, exists := targets[key]; exists {
				return nil, fmt.Errorf("standing ingress readiness has duplicate loaded target %s/%s", target.Alias, target.Provider)
			}
			targets[key] = target
		}
	}

	facts := []serveLifecycleIngressFact{}
	for _, subject := range manager.CapabilitySubjects() {
		if subject.Kind != packs.SubjectProviderTrigger || subject.Applicability != "effective" {
			continue
		}
		admission := subject.TriggerAdmission
		if admission == nil {
			return nil, fmt.Errorf("effective provider trigger %s has no compiled admission readback", subject.ID)
		}
		key := serveStandingIngressKey(admission.BundleHash, admission.Alias, subject.Provider)
		target, ok := targets[key]
		if !ok {
			return nil, fmt.Errorf("effective provider trigger %s has no loaded standing target", subject.ID)
		}
		bound := false
		signingSecret := strings.TrimSpace(target.SigningSecret)
		if signingSecret != "" {
			if credentials == nil {
				return nil, fmt.Errorf("standing ingress credential readback is unavailable for %s/%s", target.Alias, target.Provider)
			}
			value, found, err := credentials.Get(ctx, signingSecret)
			if err != nil {
				return nil, fmt.Errorf("read standing ingress credential %s: %w", signingSecret, err)
			}
			bound = found && strings.TrimSpace(value) != ""
		}
		facts = append(facts, serveLifecycleIngressFact{
			Provider:      strings.TrimSpace(target.Provider),
			URL:           fmt.Sprintf("http://%s/webhooks/%s/%s", apiAddr.String(), target.Alias, target.Provider),
			SigningSecret: signingSecret,
			SigningBound:  bound,
			BundleHash:    strings.TrimSpace(target.BundleHash),
			Subject:       subject,
		})
		delete(targets, key)
	}
	if len(targets) != 0 {
		return nil, fmt.Errorf("standing ingress readiness is missing %d effective capability subjects", len(targets))
	}
	return facts, nil
}

func serveStandingIngressKey(bundleHash, alias, provider string) string {
	return strings.TrimSpace(bundleHash) + "\x00" + strings.TrimSpace(alias) + "\x00" + strings.TrimSpace(provider)
}

func serveLifecycleSourceCounts(contexts []serveRuntimeBundleContext) (flows, agents, tools int) {
	for _, contextDef := range contexts {
		source := contextDef.loaded.source
		if source == nil {
			continue
		}
		flows += len(source.FlowSchemaEntries())
		agents += len(source.AgentEntries())
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

func closeServeRuntime(ctx context.Context, supervisor *runtimeProjectSupervisor, opts cliapp.ServeOptions, workspaces cliapp.ServeWorkspaceLifecycle) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var shutdownErr error
	if supervisor != nil {
		shutdownOpts := runtime.ShutdownOptions{Grace: opts.ShutdownGrace}
		_, shutdownErr = supervisor.CloseProjectWithShutdownOptions(ctx, shutdownOpts)
	}
	var cleanupErr error
	if opts.Dev && workspaces != nil {
		_, cleanupErr = workspaces.CleanupDevEntityContainers(ctx)
	}
	return errors.Join(shutdownErr, cleanupErr)
}

func runServeUnavailableBundleStartupRecovery(
	ctx context.Context,
	storeFacade selectedRuntimeStoreFacade,
	cfg *config.Config,
	loaded serveRuntimeBundle,
	source semanticview.Source,
	mountSources cliapp.WorkspaceMountSources,
	workspaceBackend cliapp.WorkspaceBackendSelection,
	presenter *serveLifecyclePresenter,
) int {
	recoveryStore := storeFacade.startupRecoveryStore()
	if recoveryStore == nil {
		return 0
	}
	recoveryWorkspaces, err := cliapp.ConfiguredWorkspaceLifecycleForServe(storeFacade.workspaceDB(), cfg, loaded.contractsRoot, source, mountSources, workspaceBackend)
	if err != nil {
		presenter.fail(5, "recovery_workspace", err)
		return 1
	}
	recoveryContainers := runtimestartuprecovery.ManagedContainerOwner(serveStartupRecoveryContainers{lifecycle: recoveryWorkspaces})
	if recoveryWorkspaces == nil {
		recoveryContainers = noWorkspaceStartupRecoveryContainers{}
	}
	recovery, err := runtimestartuprecovery.Recover(ctx, runtimestartuprecovery.Request{
		AvailabilityReader: recoveryStore,
		CleanupStore:       recoveryStore,
		Containers:         recoveryContainers,
		RequestedAt:        time.Now().UTC(),
	})
	if err != nil {
		presenter.fail(5, "startup_recovery", err)
		if runtimestartuprecovery.IsDataIntegrityError(err) {
			return serveExitDataIntegrity
		}
		return 3
	}
	if len(recovery.OrphanTargets) > 0 || len(recovery.StoppedContainers) > 0 {
		presenter.recordClosedUnavailableWork()
		log.Printf("unavailable bundle startup recovery complete: orphaned_runs=%d deliveries=%d sessions=%d timers=%d containers=%d pipeline_receipts=%d",
			len(recovery.Cleanup.Runs),
			len(recovery.Cleanup.Deliveries),
			len(recovery.Cleanup.Sessions),
			len(recovery.Cleanup.Timers),
			len(recovery.StoppedContainers),
			recovery.Cleanup.PipelineReceiptCount,
		)
	}
	return 0
}

func startServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager) error {
	prepared := make([]*runtime.Runtime, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.PrepareAuthorActivityCatalog(); err != nil {
			for _, rt := range prepared {
				_ = rt.Shutdown()
			}
			return fmt.Errorf("prepare author activity catalog: %w", err)
		}
		prepared = append(prepared, contextDef.runtime)
	}
	registered := make([]struct {
		hash    string
		runtime *runtime.Runtime
	}, 0, len(contexts))
	rollback := func() {
		closedByManager := make(map[*runtime.Runtime]struct{}, len(registered))
		for i := len(registered) - 1; i >= 0; i-- {
			entry := registered[i]
			if manager != nil {
				result := manager.DeactivateBundleHash(entry.hash, runtime.RuntimeContextCauseUnavailable)
				if result.Found && result.ShutdownErr == nil {
					closedByManager[entry.runtime] = struct{}{}
				}
			}
		}
		for i := len(prepared) - 1; i >= 0; i-- {
			rt := prepared[i]
			if _, closed := closedByManager[rt]; !closed {
				_ = rt.Shutdown()
			}
		}
	}
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.Start(ctx); err != nil {
			rollback()
			return err
		}
		targets, activations, err := contextDef.runtime.EnsureStandingTargets(ctx)
		if err != nil {
			rollback()
			return err
		}
		if manager != nil {
			for _, activation := range activations {
				if activation.EffectiveState == "active" {
					continue
				}
				if err := manager.SuppressStandingServiceTargets(activation.ServiceID); err != nil {
					rollback()
					return err
				}
			}
			if err := manager.Register(runtime.BundleContext{
				BundleHash:       contextDef.bundleSourceFact.BundleHash,
				BundleSourceFact: contextDef.bundleSourceFact,
				BundleIdentity:   contextDef.bootIdentity,
				Source:           contextDef.loaded.source,
				ContractsRoot:    contextDef.loaded.contractsRoot,
				PlatformSpecPath: contextDef.loaded.platformSpecPath,
				Runtime:          contextDef.runtime,
				StandingTargets:  targets,
			}); err != nil {
				rollback()
				return err
			}
			registered = append(registered, struct {
				hash    string
				runtime *runtime.Runtime
			}{
				hash:    strings.TrimSpace(contextDef.bundleSourceFact.BundleHash),
				runtime: contextDef.runtime,
			})
		}
	}
	return nil
}
func closeAdditionalServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager, opts cliapp.ServeOptions) error {
	var shutdownErr error
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if manager != nil {
			result := manager.DeactivateBundleHashWithOptions(contextDef.bundleSourceFact.BundleHash, runtime.RuntimeContextCauseUnloaded, runtime.ShutdownOptions{Grace: opts.ShutdownGrace})
			shutdownErr = errors.Join(shutdownErr, result.ShutdownErr)
			continue
		}
		if err := contextDef.runtime.ShutdownWithOptions(runtime.ShutdownOptions{Grace: opts.ShutdownGrace}); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}
	return shutdownErr
}

func buildStores(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
	if cfg == nil {
		return storeBundle{}, errors.New("runtime config is required")
	}
	switch selection.Backend {
	case storebackend.BackendPostgres:
		dsn, err := postgresDSNFromConfig(ctx, cfg.Database)
		if err != nil {
			return storeBundle{}, err
		}
		pg, err := store.NewPostgresStore(dsn)
		if err != nil {
			return storeBundle{}, err
		}
		if err := pg.Ping(ctx); err != nil {
			return storeBundle{}, err
		}
		if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		bundle := selectedPostgresStoreBundle(pg, cfg)
		if err := validateSelectedStoreBundleRoles(selection.Backend, bundle); err != nil {
			closeDB(pg.DB)
			return storeBundle{}, err
		}
		return bundle, nil
	case storebackend.BackendSQLite:
		sqliteStore, err := store.NewSQLiteRuntimeStore(selection.SQLitePath)
		if err != nil {
			return storeBundle{}, err
		}
		if err := sqliteStore.Ping(ctx); err != nil {
			_ = sqliteStore.Close()
			return storeBundle{}, err
		}
		if _, err := sqliteStore.BindSchemaCapabilities(ctx); err != nil {
			_ = sqliteStore.Close()
			return storeBundle{}, err
		}
		sqliteStore.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
		bundle := storeBundle{
			SQLDB:                       sqliteStore.DB,
			Database:                    sqlDBPinger{db: sqliteStore.DB},
			RuntimeLogStore:             sqliteStore,
			SchemaBootstrapper:          sqliteStore,
			EventStore:                  sqliteStore,
			PipelineStore:               runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore),
			SessionRegistry:             sqliteStore,
			ConversationStore:           sqliteStore,
			ManagerStore:                sqliteStore,
			ScheduleStore:               sqliteStore,
			MailboxMaterializer:         sqliteStore,
			MailboxStore:                sqliteStore,
			ToolEntityStore:             sqliteStore,
			HumanTaskStore:              sqliteStore,
			BudgetSpendStore:            sqliteStore,
			InboundStore:                sqliteStore,
			MailboxAPIStore:             sqliteStore,
			DecisionCards:               sqliteStore,
			ObservabilityStore:          sqliteStore,
			AgentUsageStore:             sqliteStore,
			AgentDeliveryLifecycleStore: sqliteStore,
			RuntimeIngressStore:         sqliteStore,
			IdempotencyStore:            sqliteStore,
			StartupOwnership:            sqliteStore,
			RunQuiescenceStore:          sqliteStore,
			RunReadStore:                sqliteStore,
			EntityReadStore:             sqliteStore,
			AgentConversationReadStore:  sqliteStore,
			RunBundleContextStore:       sqliteStore,
			RunStalledReader:            sqliteStore,
		}
		bundle.APIOptionalCapabilityBuilder = selectedSQLiteAPIOptionalCapabilityBuilder(sqliteStore)
		if err := validateSelectedStoreBundleRoles(selection.Backend, bundle); err != nil {
			_ = sqliteStore.Close()
			return storeBundle{}, err
		}
		return bundle, nil
	default:
		return storeBundle{}, fmt.Errorf("store backend selection is required; supported backends: %s, %s", storebackend.BackendPostgres, storebackend.BackendSQLite)
	}
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

func enforceServeBundleMatchAdmission(ctx context.Context, availability selectedRunBundleAvailabilityStore, bootIdentity string, requireMatch bool, pinnedBundleHash string) error {
	var pinned []string
	if hash := strings.TrimSpace(pinnedBundleHash); hash != "" {
		pinned = []string{hash}
	}
	return enforceServeBundleMatchAdmissionForHashes(ctx, availability, bootIdentity, requireMatch, pinned)
}

func enforceServeBundleMatchAdmissionForHashes(ctx context.Context, availability selectedRunBundleAvailabilityStore, bootIdentity string, requireMatch bool, pinnedBundleHashes []string) error {
	bootIdentity = strings.TrimSpace(bootIdentity)
	pinnedBundleHashes = uniqueTrimmedServeBundleHashes(pinnedBundleHashes)
	enforceActiveAvailability := requireMatch || len(pinnedBundleHashes) > 0
	if enforceActiveAvailability && bootIdentity == "" {
		return fmt.Errorf("boot bundle identity is required")
	}
	if availability == nil {
		return nil
	}
	if enforceActiveAvailability {
		conflicts, err := availability.ActiveRunBundleAvailabilityConflicts(ctx)
		if err != nil {
			return err
		}
		if len(conflicts) > 0 {
			details := make([]string, 0, len(conflicts))
			for _, conflict := range conflicts {
				details = append(details, conflict.DetailString())
			}
			return fmt.Errorf("active run bundle availability conflict: boot bundle %s cannot resume %d active run(s): %s", bootIdentity, len(conflicts), strings.Join(details, "; "))
		}
	}
	if len(pinnedBundleHashes) == 0 {
		return nil
	}
	mismatches, err := activeRunPinnedBundleHashesConflicts(ctx, availability, pinnedBundleHashes)
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
	return fmt.Errorf("active run pinned bundle_hash conflict: DB-loaded serve bundle_hash set %s cannot resume %d active run(s) with different bundle_hash: %s", strings.Join(pinnedBundleHashes, ","), len(mismatches), strings.Join(details, "; "))
}

func activeRunPinnedBundleHashConflicts(ctx context.Context, availability selectedRunBundleAvailabilityStore, pinnedBundleHash string) ([]runbundle.Availability, error) {
	pinnedBundleHash = strings.TrimSpace(pinnedBundleHash)
	if pinnedBundleHash == "" {
		return nil, nil
	}
	availabilities, err := availability.ActiveRunBundleAvailabilities(ctx)
	if err != nil {
		return nil, err
	}
	conflicts := make([]runbundle.Availability, 0, len(availabilities))
	for _, availability := range availabilities {
		if !availability.Available() {
			continue
		}
		if strings.TrimSpace(availability.BundleHash) != pinnedBundleHash {
			conflicts = append(conflicts, availability)
		}
	}
	return conflicts, nil
}

func activeRunPinnedBundleHashesConflicts(ctx context.Context, availability selectedRunBundleAvailabilityStore, pinnedBundleHashes []string) ([]runbundle.Availability, error) {
	allowed := map[string]struct{}{}
	for _, hash := range uniqueTrimmedServeBundleHashes(pinnedBundleHashes) {
		allowed[hash] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, nil
	}
	availabilities, err := availability.ActiveRunBundleAvailabilities(ctx)
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

func initializeStateStores(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
	if stores.SchemaBootstrapper == nil || bundle == nil {
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
	if err := ensureServeSchemaTables(ctx, stores, request); err != nil {
		return "", err
	}
	return cliapp.SummarizeServeSchemaPlans(plans.All()), nil
}

func initializeServePlatformStateStores(ctx context.Context, stores storeBundle, platformSpecPath string) (string, error) {
	if stores.SchemaBootstrapper == nil {
		return "store wiring ready", nil
	}
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
	if err := ensureServeSchemaTables(ctx, stores, request); err != nil {
		return "", err
	}
	return cliapp.SummarizeServeSchemaPlans(plans), nil
}

func initializeLoadedServeRuntimeStateStores(ctx context.Context, stores storeBundle, loaded []serveRuntimeBundle) ([]string, error) {
	summaries := make([]string, len(loaded))
	for i, bundle := range loaded {
		summary, err := initializeStateStores(ctx, stores, bundle.bundle)
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

func ensureServeSchemaTables(ctx context.Context, stores storeBundle, request store.SchemaBootstrapRequest) error {
	if stores.SchemaBootstrapper == nil {
		return nil
	}
	if err := stores.SchemaBootstrapper.BootstrapSchema(ctx, request); err != nil {
		return err
	}
	if err := rebindServePostgresSchemaCapabilities(ctx, stores); err != nil {
		return err
	}
	return nil
}

func rebindServePostgresSchemaCapabilities(ctx context.Context, stores storeBundle) error {
	binder := stores.facade().schemaCapabilityBinder()
	if binder == nil {
		return nil
	}
	if _, err := binder.BindSchemaCapabilities(ctx); err != nil {
		return fmt.Errorf("bind post-bootstrap schema capabilities: %w", err)
	}
	return nil
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
	return fmt.Sprintf("nodes=%d agents=%d events=%d tools=%d", len(source.NodeEntries()), len(source.AgentEntries()), len(source.ResolvedEventCatalog()), len(availableToolNames))
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

func systemWorkspaceContainers(lifecycle workspace.Lifecycle) []string {
	lister, ok := lifecycle.(systemWorkspaceContainerLister)
	if !ok || lister == nil {
		return nil
	}
	return lister.SystemWorkspaceContainers()
}

func newAPIServer(ready *atomic.Bool, apiV1Handler http.Handler, inboundHandler http.Handler, baseContexts ...context.Context) *http.Server {
	mux := http.NewServeMux()
	gateReady := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ready != nil && !ready.Load() {
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
		mux.Handle("/v1/rpc", gateReady(apiV1Handler))
		mux.Handle("/v1/ws", gateReady(apiV1Handler))
	}
	if inboundHandler != nil {
		mux.Handle("/webhooks/", gateReady(inboundHandler))
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

func newMCPServer(toolGateway *runtimemcp.Gateway) *http.Server {
	mux := http.NewServeMux()
	if toolGateway != nil {
		gatewayHandler := toolGateway.Handler()
		mux.Handle("/mcp", gatewayHandler)
		mux.Handle("/tools/", gatewayHandler)
	}
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
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

func shutdownHTTPServer(name string, server *http.Server) error {
	if server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s server shutdown: %w", name, err)
	}
	return nil
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

func closeDB(db *sql.DB) {
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		log.Printf("close db: %v", err)
	}
}
