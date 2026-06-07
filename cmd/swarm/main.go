package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
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
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"

	"gopkg.in/yaml.v3"
)

const (
	defaultPlatformSpecPath = platform.DefaultPlatformSpecPath
	defaultAPIListenAddr    = "127.0.0.1:8081"
	defaultMCPListenAddr    = "127.0.0.1:8082"
	serveAPIRoutes          = "/healthz /readyz /v1/rpc /v1/ws"
	serveMCPRoutes          = "/mcp /tools/"
	serveReadinessRoutes    = "/healthz /readyz"
	serveGatewayTokenBytes  = 32
	serveExitDataIntegrity  = 78
	runtimeStoreBackendHelp = "Runtime store backend: sqlite (local/dev default) or postgres (explicit opt-in production/external backend)"
)

var (
	buildStoresForServe                  = buildStores
	runtimeConfigExecutablePath          = os.Executable
	configuredWorkspaceLifecycleForServe = func(db *sql.DB, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources, backend workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
		return configuredWorkspaceLifecycleForBackend(db, contractsRoot, source, mountSources, backend)
	}
)

type serveWorkspaceLifecycle interface {
	workspace.Lifecycle
	workspace.DevEntityContainerCleaner
	runtimedestructivereset.ManagedContainerInventoryReader
	runtimedestructivereset.ManagedContainerRuntime
}

type serveStartupRecoveryContainers struct {
	lifecycle serveWorkspaceLifecycle
}

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

type previousEnv struct {
	value string
	set   bool
}

type storeBundle struct {
	Postgres            *store.PostgresStore
	SQLDB               *sql.DB
	RuntimeSQLDB        *sql.DB
	RuntimeLogStore     runtime.RuntimeLogPersistence
	RuntimeBlocker      string
	SchemaBootstrapper  store.SchemaBootstrapper
	EventStore          runtimebus.EventStore
	PipelineStore       *runtimepipeline.WorkflowInstanceStore
	SessionRegistry     sessions.Registry
	ConversationStore   runtimellm.ConversationPersistence
	ManagerStore        runtimemanager.ManagerPersistence
	ScheduleStore       runtimepipeline.SchedulePersistence
	MailboxMaterializer runtimepipeline.MailboxWriteMaterializationStore
	MailboxStore        runtimetools.MailboxPersistence
	ToolEntityStore     runtimetools.EntityPersistence
	HumanTaskStore      runtimetools.HumanTaskPersistence
	BudgetSpendStore    budgetspend.Store
	MailboxAPIStore     apiv1.MailboxAPIStore
	ObservabilityStore  apiv1.ObservabilityReadStore
	AgentUsageStore     apiv1.AgentUsageReadStore
	RuntimeIngressStore runtimeingress.Store
	IdempotencyStore    apiv1.APIIdempotencyStore
	TurnStore           runtimellm.TurnPersistence
	StartupOwnership    runtimestartupownership.Store
	RunQuiescenceStore  runtimerunquiescence.ServeAbandonStore
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

func (s storeBundle) runtimeStores() runtime.Stores {
	return runtime.Stores{
		SQLDB:               s.RuntimeSQLDB,
		ConstructionBlocker: s.RuntimeBlocker,
		EventStore:          s.EventStore,
		RuntimeLogStore:     s.RuntimeLogStore,
		PipelineStore:       s.PipelineStore,
		SessionRegistry:     s.SessionRegistry,
		ConversationStore:   s.ConversationStore,
		ManagerStore:        s.ManagerStore,
		ScheduleStore:       s.ScheduleStore,
		MailboxMaterializer: s.MailboxMaterializer,
		StartupOwnership:    s.StartupOwnership,
		MailboxStore:        s.MailboxStore,
		ToolEntityStore:     s.ToolEntityStore,
		HumanTaskStore:      s.HumanTaskStore,
		BudgetSpendStore:    s.BudgetSpendStore,
		RuntimeIngressStore: s.RuntimeIngressStore,
		TurnStore:           s.TurnStore,
	}
}

func selectedAPIRunBundleContextStore(stores storeBundle) apiv1.RunBundleContextStore {
	if stores.Postgres != nil {
		return stores.Postgres
	}
	runBundleContext, _ := stores.ObservabilityStore.(apiv1.RunBundleContextStore)
	return runBundleContext
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	os.Exit(executeRootCommand(ctx, "", os.Args[1:], os.Stdout, os.Stderr))
}

type serveOptions struct {
	ConfigPath                       string
	Backend                          string
	ContractsPath                    string
	DataSource                       string
	WorkspaceBackend                 string
	WorkspaceBackendSet              bool
	BundleHash                       string
	BundleHashes                     []string
	PlatformSpecPath                 string
	StoreMode                        string
	StoreModeSet                     bool
	APIListenAddr                    string
	MCPListenAddr                    string
	ShutdownGrace                    time.Duration
	Dev                              bool
	SelfCheck                        bool
	RequireBundleMatch               bool
	NoRequireBundleMatch             bool
	AbandonActiveRuns                bool
	Verbose                          bool
	Output                           io.Writer
	TestEntityStateHook              func(entityID, state string)
	TestWorkflowNodeHandlerStartHook runtimepipeline.WorkflowNodeHandlerStartHook
	TestLifecycleProbe               runtimelifecycleprobe.Observer
	TestOutboxSweeperConfig          runtimebus.OutboxSweeperConfig
	TestRuntimeReadyHook             func(*runtime.Runtime)
}

type serveRuntimeBundle struct {
	module           runtimepipeline.WorkflowModule
	bundle           *runtimecontracts.WorkflowContractBundle
	source           semanticview.Source
	contractsRoot    string
	platformSpecPath string
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
	workspaces        serveWorkspaceLifecycle
	validation        runtime.WorkflowContractValidationResult
	runtime           *runtime.Runtime
}

type serveRuntimeBundleContextRequest struct {
	Ctx                    context.Context
	Stores                 storeBundle
	Config                 *config.Config
	Loaded                 serveRuntimeBundle
	StateStoreSummary      string
	Options                serveOptions
	MountSources           workspaceMountSources
	WorkspaceBackend       workspaceBackendSelection
	Credentials            runtimecredentials.Store
	BootStartedAt          time.Time
	BootProgress           func(runtime.BootProgressEvent)
	EnableToolGateway      bool
	UseStartupOwnership    bool
	UseStartupRecovery     bool
	RequireBundleScopeName bool
}

func defaultServeOptions() serveOptions {
	return serveOptions{
		StoreMode:          storebackend.ActiveDefaultBackend().String(),
		WorkspaceBackend:   defaultWorkspaceBackend,
		APIListenAddr:      defaultAPIListenAddr,
		MCPListenAddr:      defaultMCPListenAddr,
		ShutdownGrace:      runtime.DefaultShutdownGrace,
		SelfCheck:          true,
		RequireBundleMatch: true,
	}
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

func serveConfigLoadDetail(configDetail string, resolvedPaths cliContractPlatformSpecPaths, opts serveOptions) string {
	parts := []string{"config=" + strings.TrimSpace(configDetail)}
	hashes, _ := serveBundleHashes(opts)
	if len(hashes) == 1 {
		parts = append(parts, "bundle_hash="+hashes[0])
	} else if len(hashes) > 1 {
		parts = append(parts, "bundle_hashes="+strings.Join(hashes, ","))
	} else {
		parts = append(parts, "contracts="+filepath.Clean(resolvedPaths.ContractsPath))
	}
	return strings.Join(parts, " ")
}

func serveBundleHashes(opts serveOptions) ([]string, error) {
	candidates := []string{}
	if hash := strings.TrimSpace(opts.BundleHash); hash != "" {
		candidates = append(candidates, hash)
	}
	candidates = append(candidates, opts.BundleHashes...)
	out := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		hash := strings.TrimSpace(candidate)
		if hash == "" {
			return nil, fmt.Errorf("--bundle-hash must be non-empty")
		}
		if err := runtimecontracts.ValidateBundleHash(hash); err != nil {
			return nil, fmt.Errorf("--bundle-hash must be bundle-v1:sha256:<64 lowercase hex>")
		}
		if _, ok := seen[hash]; ok {
			return nil, fmt.Errorf("--bundle-hash values must be unique")
		}
		seen[hash] = struct{}{}
		out = append(out, hash)
	}
	return out, nil
}

func servePreCatalogPlatformSpecPath(resolvedPaths cliContractPlatformSpecPaths, opts serveOptions) (string, error) {
	hashes, err := serveBundleHashes(opts)
	if err != nil {
		return "", err
	}
	if len(hashes) > 0 {
		return embeddedPlatformSpecPath()
	}
	return resolvedPaths.PlatformSpecPath, nil
}

func loadServeRuntimeBundles(ctx context.Context, repo string, stores storeBundle, resolvedPaths cliContractPlatformSpecPaths, opts serveOptions) ([]serveRuntimeBundle, error) {
	hashes, err := serveBundleHashes(opts)
	if err != nil {
		return nil, err
	}
	if len(hashes) > 0 {
		out := make([]serveRuntimeBundle, 0, len(hashes))
		for _, hash := range hashes {
			loaded, err := loadServeRuntimeBundleFromCatalog(ctx, repo, stores, hash)
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

func loadServeRuntimeBundle(ctx context.Context, repo string, stores storeBundle, resolvedPaths cliContractPlatformSpecPaths, opts serveOptions) (serveRuntimeBundle, error) {
	hashes, err := serveBundleHashes(opts)
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	if len(hashes) > 1 {
		return serveRuntimeBundle{}, fmt.Errorf("loadServeRuntimeBundle supports one bundle_hash; use loadServeRuntimeBundles for multi-context boot")
	}
	if len(hashes) == 1 {
		return loadServeRuntimeBundleFromCatalog(ctx, repo, stores, hashes[0])
	}
	contractsRoot, err := normalizeContractsRoot(resolvedPaths.ContractsPath)
	if err != nil {
		return serveRuntimeBundle{}, fmt.Errorf("resolve contracts: %w", err)
	}
	module, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPaths.PlatformSpecPath)
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
		bootIdentity:     bootIdentity,
	}, nil
}

func loadServeRuntimeBundleFromCatalog(ctx context.Context, repo string, stores storeBundle, bundleHash string) (serveRuntimeBundle, error) {
	if stores.Postgres == nil {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: swarm serve --bundle-hash requires postgres bundle catalog store")
	}
	if err := runtimecontracts.ValidateBundleHash(bundleHash); err != nil {
		return serveRuntimeBundle{}, err
	}
	record, err := stores.Postgres.LoadBundleCatalogRuntimeRecord(ctx, bundleHash)
	if errors.Is(err, store.ErrBundleNotFound) {
		return serveRuntimeBundle{}, fmt.Errorf("BUNDLE_UNAVAILABLE: bundle_hash %s is not present in bundles", bundleHash)
	}
	if err != nil {
		return serveRuntimeBundle{}, err
	}
	runtimeSource, err := runtimecontracts.LoadBundleCatalogRuntimeSource(repo, runtimecontracts.BundleCatalogRuntimeLoadRequest{
		BundleHash:  record.BundleHash,
		ContentYAML: record.ContentYAML,
		DataBlob:    record.DataBlob,
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
	module, source, err := newSwarmWorkflowModuleForBundle(runtimeSource.Bundle)
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
	return prepareServeBundleSource(ctx, stores, loaded.bundle, loaded.bootIdentity.Fingerprint, dev)
}

func buildServeRuntimeBundleContext(req serveRuntimeBundleContextRequest) (serveRuntimeBundleContext, error) {
	loaded := req.Loaded
	stateStoreSummary := strings.TrimSpace(req.StateStoreSummary)
	if stateStoreSummary == "" {
		var err error
		stateStoreSummary, err = initializeStateStores(req.Ctx, req.Stores, loaded.bundle, req.Options.Verbose)
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
	workspaces, err := configuredWorkspaceLifecycleForServe(req.Stores.SQLDB, loaded.contractsRoot, loaded.source, req.MountSources, req.WorkspaceBackend)
	if err != nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("configure workspaces: %w", err)
	}
	if workspaces == nil {
		return serveRuntimeBundleContext{}, fmt.Errorf("workspace lifecycle is not configured")
	}
	if req.RequireBundleScopeName {
		if scoper, ok := workspaces.(interface{ SetBundleScope(string) }); ok && scoper != nil {
			scoper.SetBundleScope(bundleSourceFact.BundleHash)
		}
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
	validation, err := runtime.ValidateWorkflowContractSurface(req.Ctx, loaded.source, runtime.DefaultWorkflowContractValidationOptions(req.Credentials))
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
			ToolGatewayToken:                 strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")),
			BundleFingerprint:                bootIdentity.Fingerprint,
			BundleSourceFact:                 bundleSourceFact,
			Credentials:                      req.Credentials,
			BootStartedAt:                    req.BootStartedAt,
			BootProgress:                     req.BootProgress,
			SystemContainers:                 systemWorkspaceContainers(workspaces),
			DisablePersistentStartupRecovery: !req.UseStartupRecovery,
			TestEntityStateHook:              req.Options.TestEntityStateHook,
			TestWorkflowNodeHandlerStartHook: req.Options.TestWorkflowNodeHandlerStartHook,
			TestLifecycleProbe:               req.Options.TestLifecycleProbe,
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
		workspaces:        workspaces,
		validation:        validation,
		runtime:           rt,
	}, nil
}

func buildForkChatSandboxLLMRuntime(cfg *config.Config, workspaces workspace.Resolver) (runtimellm.Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	return runtimellm.RuntimeFactory{
		Cfg:        cfg,
		Sessions:   sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL),
		LockOwner:  "forkchat-sandbox",
		Workspaces: workspaces,
	}.Build()
}

func runServeRuntime(ctx context.Context, repo string, opts serveOptions) int {
	bootStartedAt := time.Now().UTC()
	reporter := newServeBootReporter(opts.Verbose, opts.Output)
	reporter.emit(1, "process_start", "ok", "")
	if err := loadRepoDotEnv(repo); err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("load .env: %v", err)
		return 1
	}
	apiAuth := apiv1.ResolveAuthTokensFromEnvironment()
	if err := validateServeAPIAuthBinding(opts.APIListenAddr, apiAuth); err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("configure v1 api auth: %v", err)
		return 1
	}
	if apiAuth.UsesDefaultLoopbackToken() {
		log.Print(apiv1.DefaultLoopbackAPITokenWarning)
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.ContractsPath,
		PlatformSpecPath: opts.PlatformSpecPath,
	})
	if err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("resolve CLI path config: %v", err)
		return 1
	}

	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    opts.ConfigPath,
		BackendOverride: opts.Backend,
	})
	if err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("load config: %v", err)
		return 1
	}
	cfg := cfgResult.Config
	reporter.emit(2, "config_load", "ok", serveConfigLoadDetail(cfgResult.Detail(), resolvedPaths, opts))
	mountSources, err := resolveWorkspaceMountSources(repo, opts.DataSource, cfg)
	if err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("resolve workspace data source: %v", err)
		return 1
	}
	workspaceBackend, err := resolveWorkspaceBackend(opts.WorkspaceBackend, opts.WorkspaceBackendSet, cfg)
	if err != nil {
		reporter.emit(2, "config_load", "FAILED", err.Error())
		log.Printf("resolve workspace backend: %v", err)
		return 1
	}
	storeSelection, err := resolveRuntimeStoreSelection(repo, opts.StoreMode, opts.StoreModeSet, cfg)
	if err != nil {
		reporter.emit(3, "db_connection", "FAILED", err.Error())
		log.Printf("resolve store backend: %v", err)
		return 1
	}
	stores, err := buildStoresForServe(ctx, storeSelection, cfg)
	if err != nil {
		reporter.emit(3, "db_connection", "FAILED", err.Error())
		log.Printf("init stores: %v", err)
		return 1
	}
	reporter.emit(3, "db_connection", "ok", storeSelection.Backend.String())
	defer closeDB(stores.SQLDB)
	if stores.SchemaBootstrapper != nil {
		preCatalogPlatformSpecPath, err := servePreCatalogPlatformSpecPath(resolvedPaths, opts)
		if err != nil {
			reporter.emit(4, "bundle_load", "FAILED", err.Error())
			log.Printf("resolve pre-catalog platform spec: %v", err)
			return 1
		}
		if _, err := initializeServePlatformStateStores(ctx, stores, preCatalogPlatformSpecPath, opts.Verbose); err != nil {
			reporter.emit(4, "bundle_load", "FAILED", err.Error())
			log.Printf("initialize platform state stores: %v", err)
			return 1
		}
	}
	loadedBundles, err := loadServeRuntimeBundles(ctx, repo, stores, resolvedPaths, opts)
	if err != nil {
		reporter.emit(4, "bundle_load", "FAILED", err.Error())
		log.Printf("load Swarm contracts: %v", err)
		return 1
	}
	if len(loadedBundles) == 0 {
		reporter.emit(4, "bundle_load", "FAILED", "no bundle contexts loaded")
		return 1
	}
	defer func() {
		for _, loaded := range loadedBundles {
			if loaded.cleanup != nil {
				if err := loaded.cleanup(); err != nil {
					log.Printf("cleanup DB-loaded bundle runtime source failed: %v", err)
				}
			}
		}
	}()
	loadedBundle := loadedBundles[0]
	source := loadedBundle.source
	resolvedPlatformSpecPath := loadedBundle.platformSpecPath
	reporter.emit(4, "bundle_load", "ok", serveBootBundleLoadDetail(serveRuntimeBundleIdentitiesDetail(loadedBundles), source))
	stateStoreSummaries := make([]string, len(loadedBundles))
	if stores.SchemaBootstrapper != nil {
		summaries, err := initializeLoadedServeRuntimeStateStores(ctx, stores, loadedBundles, opts.Verbose)
		if err != nil {
			slog.Error("initialize state stores", "error", err)
			return 1
		}
		stateStoreSummaries = summaries
	}
	if opts.AbandonActiveRuns {
		if stores.RunQuiescenceStore == nil {
			slog.Error("abandon active runs failed", "error", "selected store active-run quiescence owner is required")
			return 3
		}
		result, err := stores.RunQuiescenceStore.ApplyServeAbandonActiveRunQuiescence(ctx, time.Now().UTC())
		if err != nil {
			slog.Error("abandon active runs failed", "error", err)
			return 3
		}
		log.Printf("serve abandon active runs complete: runs=%d deliveries=%d pipeline_receipts=%d", len(result.Runs), len(result.Deliveries), result.PipelineReceiptCount)
	}
	if stores.Postgres != nil {
		recoveryWorkspaces, err := configuredWorkspaceLifecycleForServe(stores.SQLDB, loadedBundle.contractsRoot, source, mountSources, workspaceBackend)
		if err != nil {
			slog.Error("configure recovery workspaces", "error", err)
			return 1
		}
		if recoveryWorkspaces == nil {
			slog.Error("workspace lifecycle is not configured")
			return 1
		}
		recovery, err := runtimestartuprecovery.Recover(ctx, runtimestartuprecovery.Request{
			AvailabilityReader: stores.Postgres,
			CleanupStore:       stores.Postgres,
			Containers:         serveStartupRecoveryContainers{lifecycle: recoveryWorkspaces},
			RequestedAt:        time.Now().UTC(),
		})
		if err != nil {
			slog.Error("unavailable bundle startup recovery failed", "error", err)
			if runtimestartuprecovery.IsDataIntegrityError(err) {
				return serveExitDataIntegrity
			}
			return 3
		}
		if len(recovery.OrphanTargets) > 0 || len(recovery.StoppedContainers) > 0 {
			log.Printf("unavailable bundle startup recovery complete: orphaned_runs=%d deliveries=%d sessions=%d timers=%d containers=%d pipeline_receipts=%d",
				len(recovery.Cleanup.Runs),
				len(recovery.Cleanup.Deliveries),
				len(recovery.Cleanup.Sessions),
				len(recovery.Cleanup.Timers),
				len(recovery.StoppedContainers),
				recovery.Cleanup.PipelineReceiptCount,
			)
		}
	}
	pinnedBundleHashes := servePinnedBundleHashes(loadedBundles)
	if err := enforceServeBundleMatchAdmissionForHashes(ctx, stores.Postgres, serveRuntimeBundleIdentitiesDetail(loadedBundles), opts.RequireBundleMatch, pinnedBundleHashes); err != nil {
		slog.Error("bundle match admission failed", "error", err)
		return 3
	}
	credentialStore, err := buildCredentialStore()
	if err != nil {
		slog.Error("configure credentials", "error", err)
		return 1
	}
	runtimeContexts := make([]serveRuntimeBundleContext, 0, len(loadedBundles))
	for i, loaded := range loadedBundles {
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
			BootStartedAt:          bootStartedAt,
			BootProgress:           reporter.runtimeSink(),
			EnableToolGateway:      i == 0,
			UseStartupOwnership:    len(loadedBundles) == 1 && i == 0,
			UseStartupRecovery:     len(loadedBundles) == 1,
			RequireBundleScopeName: len(loadedBundles) > 1,
		})
		if err != nil {
			reporter.emit(5, "runtime_context", "FAILED", err.Error())
			slog.Error("build runtime context", "bundle_hash", strings.TrimSpace(loaded.bundleSourceFact.BundleHash), "error", err)
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
	rt := primaryContext.runtime
	bootReport := primaryContext.validation.BootReport
	stateStoreSummary := serveRuntimeStateStoreSummary(runtimeContexts)

	apiListener, err := listenServeHTTPListener("api", opts.APIListenAddr)
	if err != nil {
		reporter.emit(20, "http_listener_bind", "FAILED", err.Error())
		log.Printf("bind api listener: %v", err)
		return 3
	}
	defer apiListener.Close()
	mcpListener, err := listenServeHTTPListener("mcp", opts.MCPListenAddr)
	if err != nil {
		_ = apiListener.Close()
		reporter.emit(20, "http_listener_bind", "FAILED", err.Error())
		log.Printf("bind mcp listener: %v", err)
		return 3
	}
	defer mcpListener.Close()
	restoreGatewayEnv, err := configureServeMCPGatewayEnv(mcpListener.Addr())
	if err != nil {
		_ = mcpListener.Close()
		_ = apiListener.Close()
		reporter.emit(20, "http_listener_bind", "FAILED", err.Error())
		log.Printf("configure mcp gateway env: %v", err)
		return 3
	}
	defer restoreGatewayEnv()

	forkChatLLM, err := buildForkChatSandboxLLMRuntime(cfg, workspaces)
	if err != nil {
		log.Printf("init forkchat sandbox runtime: %v", err)
		return 1
	}

	var ready atomic.Bool
	supervisor := newRuntimeProjectSupervisor(repo, resolvedPlatformSpecPath, cfg, stores, &ready, mountSources, workspaceBackend, contractsRoot, bundle, source, rt, opts.Dev)
	if len(pinnedBundleHashes) > 0 {
		supervisor.DisableSourceReplacement("swarm serve --bundle-hash pins persisted bundle contexts for the process; dynamic project reload is split to RuntimeContextManager Load/Unload work")
	}
	defer func() {
		if err := closeAdditionalServeRuntimeContexts(context.Background(), runtimeContexts[1:], opts); err != nil {
			log.Printf("additional runtime context shutdown failed: %v", err)
		}
		if err := closeServeRuntime(context.Background(), supervisor, opts, workspaces); err != nil {
			log.Printf("runtime shutdown failed: %v", err)
		}
	}()
	mailboxApprovalRoutes, err := apiv1.MailboxApprovalRoutesFromSpec(resolvedPlatformSpecPath)
	if err != nil {
		log.Printf("load v1 api mailbox approval routes: %v", err)
		return 1
	}
	var apiEntities apiv1.EntityReadStore
	var apiAgentConversations apiv1.AgentConversationReadStore
	var apiDatabase apiv1.Pinger
	var apiRuns apiv1.RunReadStore
	apiRunBundleContext := selectedAPIRunBundleContextStore(stores)
	apiObservability := stores.ObservabilityStore
	if stores.Postgres != nil {
		apiDatabase = stores.Postgres
		apiRuns = stores.Postgres
		apiEntities = stores.Postgres
		apiAgentConversations = stores.Postgres
		apiObservability = stores.Postgres
	} else if stores.SQLDB != nil {
		apiDatabase = sqlDBPinger{db: stores.SQLDB}
	}
	if stores.Postgres == nil {
		if runStore, ok := stores.ObservabilityStore.(apiv1.RunReadStore); ok {
			apiRuns = runStore
		}
		if entityStore, ok := stores.ObservabilityStore.(apiv1.EntityReadStore); ok {
			apiEntities = entityStore
		}
	}
	resetPlanner := runtimedestructivereset.InventoryPlanner{
		Reader: runtimedestructivereset.CompositeInventoryReader{
			Reader:     stores.Postgres,
			Containers: workspaces,
		},
	}
	runForkAvailability := apiv1.RunForkAvailabilityStore(stores.Postgres)
	runForkSourceLoader := runtimerunforkexecution.SelectedContractSourceLoader(runtimerunforkexecution.ContractBundleSourceLoader{
		RepoRoot:         repo,
		PlatformSpecPath: resolvedPlatformSpecPath,
	})
	if loadedBundle.dbLoaded {
		runForkSourceLoader = runtimerunforkexecution.BundleCatalogSelectedContractSourceLoader{
			RepoRoot: repo,
			Store:    stores.Postgres,
		}
	}
	apiRuntimeContexts, err := serveRuntimeContextManager(stores.Postgres, runtimeContexts)
	if err != nil {
		log.Printf("init runtime context manager: %v", err)
		return 1
	}
	apiReadOptions := apiv1.OperatorReadOptions{
		RepoRoot:         repo,
		PlatformSpecPath: resolvedPlatformSpecPath,
		Ready: func() bool {
			return ready.Load()
		},
		Database:           apiDatabase,
		Runs:               apiRuns,
		Observability:      apiObservability,
		Entities:           apiEntities,
		AgentConversations: apiAgentConversations,
		AgentUsage:         stores.AgentUsageStore,
		BundleCatalog:      stores.Postgres,
		BundleDelete: &runtimebundledelete.Coordinator{
			Planner:            stores.Postgres,
			Cleaner:            stores.Postgres,
			Finalizer:          stores.Postgres,
			Locks:              stores.Postgres,
			ContainerInventory: workspaces,
			Containers:         runtimedestructivereset.ManagedContainerStopper{Runtime: workspaces},
		},
		ConversationForks:   stores.Postgres,
		ForkChatExecutor:    apiv1.NewLLMForkChatExecutor(forkChatLLM),
		RunBundleContext:    apiRunBundleContext,
		RunForkAvailability: runForkAvailability,
		RunFork: apiv1.SelectedContractRunForkExecutor{
			Store:             stores.Postgres,
			SourceLoader:      runForkSourceLoader,
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
			AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
				Config:            cfg,
				EntityStore:       stores.ToolEntityStore,
				HumanTaskStore:    stores.HumanTaskStore,
				SessionRegistry:   stores.SessionRegistry,
				ConversationStore: stores.ConversationStore,
				TurnStore:         stores.TurnStore,
				ScheduleStore:     stores.ScheduleStore,
				MailboxStore:      stores.MailboxStore,
				Workspace:         workspaces,
				Credentials:       credentialStore,
			},
		},
		AgentControl:    dashboardDynamicAgentControl{supervisor: supervisor},
		Mailbox:         stores.MailboxAPIStore,
		Idempotency:     stores.IdempotencyStore,
		Events:          rt.Bus,
		RunControl:      rt.RunControl,
		RuntimeIngress:  rt.RuntimeIngress,
		RuntimeContexts: apiRuntimeContexts,
		ResetCoordinator: &runtimedestructivereset.Coordinator{
			Planner: resetPlanner,
			Locks:   stores.Postgres,
		},
		ResetQuiescer:         runtimedestructivereset.Quiescer{Store: stores.Postgres},
		ResetCleaner:          runtimedestructivereset.Cleaner{Store: stores.Postgres},
		ResetContainers:       runtimedestructivereset.ManagedContainerStopper{Runtime: workspaces},
		Source:                source,
		MailboxApprovalRoutes: mailboxApprovalRoutes,
		Bundle:                bootBundleIdentity,
	}
	apiV1Handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: resolvedPlatformSpecPath,
		AuthTokens:       apiAuth.Tokens,
		Handlers:         apiv1.OperatorReadHandlers(apiReadOptions),
		Subscriptions:    apiv1.OperatorSubscriptions(apiReadOptions),
	})
	if err != nil {
		log.Printf("init v1 api: %v", err)
		return 1
	}
	apiServer := newAPIServer(&ready, apiV1Handler)
	mcpServer := newMCPServer(rt.ToolGateway)
	go serveHTTPServer("api", apiServer, apiListener)
	go serveHTTPServer("mcp", mcpServer, mcpListener)
	defer shutdownHTTPServer("mcp", mcpServer)
	defer shutdownHTTPServer("api", apiServer)
	logBootWarnings(bootReport)
	if err := startServeRuntimeContexts(ctx, runtimeContexts); err != nil {
		reporter.emit(22, "ready", "FAILED", err.Error())
		log.Printf("start runtime: %v", err)
		return 1
	}
	if opts.TestRuntimeReadyHook != nil {
		opts.TestRuntimeReadyHook(rt)
	}
	startServeRunStalledEscalation(ctx, stores, runtimeContexts, rt.Bus)
	reporter.emit(20, "http_listener_bind", "ok", fmt.Sprintf("api_listener=%s api_routes=%s mcp_listener=%s mcp_routes=%s", apiListener.Addr(), serveAPIRoutes, mcpListener.Addr(), serveMCPRoutes))
	ready.Store(true)
	if err := waitForServeHealthEndpoints(ctx, apiListener.Addr()); err != nil {
		reporter.emit(21, "health_endpoints_respond", "FAILED", err.Error())
		log.Printf("health endpoint verification failed: %v", err)
		return 1
	}
	reporter.emit(21, "health_endpoints_respond", "ok", serveReadinessRoutes)
	reporter.emit(22, "ready", "ok", fmt.Sprintf("total=%s state_stores=%s", time.Since(bootStartedAt).Round(time.Millisecond), strings.TrimSpace(stateStoreSummary)))
	logReadySummary(source, contractsRoot, apiListener.Addr(), mcpListener.Addr())

	<-ctx.Done()
	ready.Store(false)
	return 0
}

func closeServeRuntime(ctx context.Context, supervisor *runtimeProjectSupervisor, opts serveOptions, workspaces serveWorkspaceLifecycle) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var shutdownErr error
	if supervisor != nil {
		shutdownOpts := runtime.ShutdownOptions{Grace: opts.ShutdownGrace}
		_, shutdownErr = supervisor.CloseProjectWithShutdownOptions(ctx, shutdownOpts)
	}
	var cleanupErr error
	if opts.Dev {
		if workspaces == nil {
			cleanupErr = fmt.Errorf("dev entity cleanup owner unavailable")
		} else {
			_, cleanupErr = workspaces.CleanupDevEntityContainers(ctx)
		}
	}
	return errors.Join(shutdownErr, cleanupErr)
}

func serveRuntimeContextManager(reader runtime.RunBundleAvailabilityReader, contexts []serveRuntimeBundleContext) (*runtime.RuntimeContextManager, error) {
	hasDBLoaded := false
	managerContexts := make([]runtime.BundleContext, 0, len(contexts))
	for _, contextDef := range contexts {
		if !contextDef.loaded.dbLoaded {
			continue
		}
		hasDBLoaded = true
		managerContexts = append(managerContexts, runtime.BundleContext{
			BundleHash:       contextDef.bundleSourceFact.BundleHash,
			BundleSourceFact: contextDef.bundleSourceFact,
			BundleIdentity:   contextDef.bootIdentity,
			Source:           contextDef.loaded.source,
			ContractsRoot:    contextDef.loaded.contractsRoot,
			PlatformSpecPath: contextDef.loaded.platformSpecPath,
			Runtime:          contextDef.runtime,
		})
	}
	if !hasDBLoaded {
		return nil, nil
	}
	return runtime.NewRuntimeContextManager(reader, managerContexts...)
}

func startServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext) error {
	started := make([]*runtime.Runtime, 0, len(contexts))
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.Start(ctx); err != nil {
			for i := len(started) - 1; i >= 0; i-- {
				_ = started[i].Shutdown()
			}
			return err
		}
		started = append(started, contextDef.runtime)
	}
	return nil
}

func closeAdditionalServeRuntimeContexts(ctx context.Context, contexts []serveRuntimeBundleContext, opts serveOptions) error {
	var shutdownErr error
	for _, contextDef := range contexts {
		if contextDef.runtime == nil {
			continue
		}
		if err := contextDef.runtime.ShutdownWithOptions(runtime.ShutdownOptions{Grace: opts.ShutdownGrace}); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}
	return shutdownErr
}

type verifyCommandResult struct {
	OK           bool                  `json:"ok"`
	Contracts    string                `json:"contracts"`
	Warnings     []verifyFindingOutput `json:"warnings"`
	LintEvidence []verifyFindingOutput `json:"lint_evidence"`
}

type verifyFindingOutput struct {
	CheckID  string `json:"check_id"`
	Severity string `json:"severity"`
	Location string `json:"location"`
	Message  string `json:"message"`
}

type verifyCommandOptions struct {
	contractsPath    string
	platformSpecPath string
	output           cliOutputOptions
	logging          cliLoggingOptions
}

func defaultVerifyCommandOptions() verifyCommandOptions {
	return verifyCommandOptions{
		logging: defaultCLILoggingOptions(),
	}
}

func runVerifyCommand(ctx context.Context, repo string, opts verifyCommandOptions, out io.Writer) int {
	return runVerifyCommandWithOutput(ctx, repo, opts, out, out)
}

func runVerifyCommandWithOutput(ctx context.Context, repo string, opts verifyCommandOptions, out, errOut io.Writer) int {
	if err := opts.logging.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: %v\n", err)
		}
		return 2
	}
	if err := opts.output.validate(); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: %v\n", err)
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: load .env: %v\n", err)
		}
		return 1
	}
	resolvedPaths, err := resolveCLIContractPlatformSpecPaths(repo, cliContractPlatformSpecPathOptions{
		ContractsPath:    opts.contractsPath,
		PlatformSpecPath: opts.platformSpecPath,
	})
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: resolve path config: %v\n", err)
		}
		return cliAPIErrorExitCode(err, cliAPIErrorClassifier{})
	}
	resolvedContractsPath := resolvedPaths.ContractsPath
	resolvedPlatformSpecPath := resolvedPaths.PlatformSpecPath
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: resolve contracts: %v\n", err)
		}
		return 1
	}
	validationOpts, err := verifyWorkflowContractValidationOptions(repo)
	if err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: configure validation: %v\n", err)
		}
		return 1
	}
	if _, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath); err != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "verify failed: load Swarm contracts: %v\n", err)
		}
		return 1
	} else {
		result, err := verifyBundleResultWithOptions(ctx, semanticview.Wrap(bundle), validationOpts)
		if err != nil {
			if errOut != nil {
				fmt.Fprintf(errOut, "verify failed: %v\n", err)
			}
			return 1
		}
		output := verifyCommandResult{
			OK:           true,
			Contracts:    contractsRoot,
			Warnings:     verifyFindingOutputs(result.BootReport.Warnings()),
			LintEvidence: verifyFindingOutputs(result.BootReport.LintEvidence()),
		}
		if err := renderCLIOutput(out, errOut, opts.output, output, func(_ io.Writer) {
			writeVerifyFindings(errOut, result.BootReport.Warnings(), false)
			writeVerifyFindings(errOut, result.BootReport.LintEvidence(), false)
			if out != nil {
				fmt.Fprintf(out, "verify ok: contracts=%s\n", contractsRoot)
			}
		}, func() ([]string, error) {
			return []string{"ok"}, nil
		}); err != nil {
			return 2
		}
	}
	return 0
}

func verifyFindingOutputs(findings []runtimebootverify.Finding) []verifyFindingOutput {
	out := make([]verifyFindingOutput, 0, len(findings))
	for _, finding := range findings {
		out = append(out, verifyFindingOutput{
			CheckID:  strings.TrimSpace(finding.CheckID),
			Severity: strings.TrimSpace(finding.Severity),
			Location: strings.TrimSpace(finding.Location),
			Message:  strings.TrimSpace(finding.Message),
		})
	}
	return out
}

type runStatusReport struct {
	RunID             string                    `json:"run_id"`
	RunTableStatus    string                    `json:"run_table_status,omitempty"`
	OperationalState  string                    `json:"operational_state,omitempty"`
	BlockingLayer     string                    `json:"blocking_layer,omitempty"`
	BlockingReason    string                    `json:"blocking_reason,omitempty"`
	RootEventID       string                    `json:"root_event_id,omitempty"`
	RootEventType     string                    `json:"root_event_type,omitempty"`
	StartedAt         time.Time                 `json:"started_at,omitempty"`
	LastEventAt       time.Time                 `json:"last_event_at,omitempty"`
	EndedAt           *time.Time                `json:"ended_at,omitempty"`
	EventCount        int                       `json:"event_count"`
	WarnErrorLogCount int                       `json:"warn_error_log_count"`
	EventCounts       []runStatusEventCount     `json:"event_counts"`
	Deliveries        []runStatusDeliveryCount  `json:"deliveries"`
	RecentEvents      []runStatusEvent          `json:"recent_events"`
	Mutations         []runStatusMutation       `json:"mutations,omitempty"`
	DeadLetters       []runStatusDeadLetter     `json:"dead_letters,omitempty"`
	AgentTurns        []runStatusAgentTurn      `json:"agent_turns,omitempty"`
	Heuristics        []string                  `json:"heuristics,omitempty"`
	RuntimeLogSummary []runStatusRuntimeSummary `json:"runtime_log_summary,omitempty"`
	RuntimeLogs       []runStatusRuntimeLog     `json:"runtime_logs,omitempty"`
}

type runStatusEventCount struct {
	EventName string `json:"event_name"`
	Count     int    `json:"count"`
}

type runStatusDeliveryCount struct {
	SubscriberID string `json:"subscriber_id"`
	Status       string `json:"status"`
	Count        int    `json:"count"`
}

type runStatusEvent struct {
	EventName string    `json:"event_name"`
	EntityID  string    `json:"entity_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type runStatusDeadLetter struct {
	OriginalEvent string    `json:"original_event"`
	EntityID      string    `json:"entity_id,omitempty"`
	FailureType   string    `json:"failure_type"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	HandlerNode   string    `json:"handler_node,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type runStatusAgentTurn struct {
	AgentID    string    `json:"agent_id"`
	Turns      int       `json:"turns"`
	ErrorCount int       `json:"error_count"`
	LastAt     time.Time `json:"last_at"`
}

type runStatusRuntimeLog struct {
	Level     string    `json:"level"`
	Component string    `json:"component"`
	Action    string    `json:"action"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type runStatusRuntimeSummary struct {
	Level     string `json:"level"`
	Component string `json:"component"`
	Action    string `json:"action"`
	Count     int    `json:"count"`
}

type runStatusMutation struct {
	MutationID    string          `json:"mutation_id,omitempty"`
	EntityID      string          `json:"entity_id,omitempty"`
	Field         string          `json:"field"`
	OldValue      json.RawMessage `json:"old_value,omitempty"`
	NewValue      json.RawMessage `json:"new_value,omitempty"`
	WriterType    string          `json:"writer_type,omitempty"`
	WriterID      string          `json:"writer_id,omitempty"`
	HandlerStep   string          `json:"handler_step,omitempty"`
	CausedByEvent string          `json:"caused_by_event,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type runStatusOptions struct {
	LogsOnly      bool
	LogsAllLevels bool
	Component     string
}

// runForkRuntimeOwnerHarness preserves internal runtime/store fork owner coverage for targeted tests.
// The public top-level `swarm fork <source-run-id> [--bundle-hash <bundle_hash>] [--at-event <event-id>] [--idempotency-key <key>]` command consumes /v1/rpc run.fork rather than this harness.
func runForkRuntimeOwnerHarness(ctx context.Context, repo string, args []string, out io.Writer) int {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "", "Optional path to Swarm runtime config")
	backend := fs.String("backend", "", "LLM backend profile for local runtime startup")
	contractsPath := fs.String("contracts", "", "Path to selected Swarm contract bundle root for fork planning or selected-contract execution")
	platformSpecPath := fs.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := fs.String("store", storebackend.ActiveDefaultBackend().String(), runtimeStoreBackendHelp)
	runID := fs.String("run", "", "Source run ID to plan from")
	at := fs.String("at", "", "Fork point event UUID or RFC3339 timestamp")
	dryRun := fs.Bool("dry-run", false, "Plan the fork without mutating runtime state")
	materializeOnly := fs.Bool("materialize-only", false, "Create fork run and materialize state snapshot without resuming execution")
	activate := fs.Bool("activate", false, "Activate an already materialized state-only fork")
	asJSON := fs.Bool("json", false, "Emit JSON")
	if err := fs.Parse(args); err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: %v\n", err)
		}
		return 2
	}
	storeModeSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "store" {
			storeModeSet = true
		}
	})
	modeCount := 0
	for _, enabled := range []bool{*dryRun, *materializeOnly, *activate} {
		if enabled {
			modeCount++
		}
	}
	if modeCount > 1 {
		if out != nil {
			fmt.Fprintln(out, "fork failed: --dry-run, --materialize-only, and --activate are mutually exclusive")
		}
		return 2
	}
	selectedContractsRequested := strings.TrimSpace(*contractsPath) != ""
	if modeCount == 0 && !selectedContractsRequested {
		if out != nil {
			fmt.Fprintln(out, "fork failed: mutating fork execution without --contracts is not implemented; use --dry-run, --materialize-only, --activate, or --contracts")
		}
		return 2
	}
	if *activate && strings.TrimSpace(*at) != "" {
		if out != nil {
			fmt.Fprintln(out, "fork failed: --activate targets --run <fork_run_id> and does not accept --at")
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: load .env: %v\n", err)
		}
		return 1
	}
	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{
		RepoRoot:        repo,
		ExplicitPath:    *configPath,
		BackendOverride: *backend,
	})
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: load config: %v\n", err)
		}
		return 1
	}
	cfg := cfgResult.Config
	storeSelection, err := resolveRuntimeStoreSelection(repo, *storeMode, storeModeSet, cfg)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: resolve store backend: %v\n", err)
		}
		return 1
	}
	stores, err := buildStores(ctx, storeSelection, cfg)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: init stores: %v\n", err)
		}
		return 1
	}
	defer closeDB(stores.SQLDB)
	if stores.Postgres == nil {
		if out != nil {
			fmt.Fprintln(out, "fork failed: postgres store required")
		}
		return 1
	}
	if *activate {
		result, err := runtimerunforkexecution.ActivateSelectedContractRunFork(ctx, runtimerunforkexecution.SelectedContractActivationGateRequest{
			ForkRunID: strings.TrimSpace(*runID),
			Store:     stores.Postgres,
			SourceLoader: runtimerunforkexecution.ContractBundleSourceLoader{
				RepoRoot:         repo,
				PlatformSpecPath: resolvePath(repo, *platformSpecPath),
			},
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printRunForkActivation(out, result.RunForkActivation)
		return 0
	}
	if *materializeOnly {
		var contractSelection *store.RunForkContractSelection
		if contracts := strings.TrimSpace(*contractsPath); contracts != "" {
			contractsRoot, err := normalizeContractsRoot(resolveContractsPath(repo, contracts))
			if err != nil {
				if out != nil {
					fmt.Fprintf(out, "fork failed: resolve contracts: %v\n", err)
				}
				return 1
			}
			_, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvePath(repo, *platformSpecPath))
			if err != nil {
				if out != nil {
					fmt.Fprintf(out, "fork failed: load selected contracts: %v\n", err)
				}
				return 1
			}
			source := semanticview.Wrap(bundle)
			selection := runtimerunforkadmission.SelectedContractSelection(source, contractsRoot)
			contractSelection = &selection
		}
		result, err := stores.Postgres.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{
			SourceRunID:       strings.TrimSpace(*runID),
			At:                strings.TrimSpace(*at),
			ContractSelection: contractSelection,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printRunForkMaterialization(out, result)
		return 0
	}
	if modeCount == 0 && selectedContractsRequested {
		contractsRoot, err := normalizeContractsRoot(resolveContractsPath(repo, *contractsPath))
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve contracts: %v\n", err)
			}
			return 1
		}
		_, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvePath(repo, *platformSpecPath))
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: load selected contracts: %v\n", err)
			}
			return 1
		}
		source := semanticview.Wrap(bundle)
		credentialStore, err := buildCredentialStore()
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure credentials: %v\n", err)
			}
			return 1
		}
		mountSources, err := resolveWorkspaceMountSources(repo, "", cfg)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve workspace data source: %v\n", err)
			}
			return 1
		}
		workspaceBackend, err := resolveWorkspaceBackend("", false, cfg)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve workspace backend: %v\n", err)
			}
			return 1
		}
		workspaces, err := configuredWorkspaceLifecycleForBackend(stores.SQLDB, contractsRoot, source, mountSources, workspaceBackend)
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: configure workspaces: %v\n", err)
			}
			return 1
		}
		result, err := runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
			SourceRunID: strings.TrimSpace(*runID),
			At:          strings.TrimSpace(*at),
			Store:       stores.Postgres,
			SourceLoader: runtimerunforkexecution.ContractBundleSourceLoader{
				RepoRoot:         repo,
				PlatformSpecPath: resolvePath(repo, *platformSpecPath),
			},
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
			AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
				Config:            cfg,
				EntityStore:       stores.ToolEntityStore,
				HumanTaskStore:    stores.HumanTaskStore,
				SessionRegistry:   stores.SessionRegistry,
				ConversationStore: stores.ConversationStore,
				TurnStore:         stores.TurnStore,
				ScheduleStore:     stores.ScheduleStore,
				MailboxStore:      stores.MailboxStore,
				Workspace:         workspaces,
				Credentials:       credentialStore,
			},
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: %v\n", err)
			}
			return 1
		}
		if *asJSON {
			enc := json.NewEncoder(out)
			enc.SetIndent("", "  ")
			if err := enc.Encode(result); err != nil {
				fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
				return 1
			}
			return 0
		}
		printSelectedContractExecution(out, result)
		return 0
	}
	plan, err := stores.Postgres.PlanRunFork(ctx, store.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(*runID),
		At:          strings.TrimSpace(*at),
	})
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "fork failed: %v\n", err)
		}
		return 1
	}
	if contracts := strings.TrimSpace(*contractsPath); contracts != "" {
		replayAdmission := store.RunForkSelectedContractReplayResumeAdmission(plan)
		if len(plan.ReplayResumeAdmission.Dispositions) > 0 || len(plan.ReplayResumeAdmission.UnsupportedBlockers) > 0 {
			plan.UnsupportedBlockers = replayAdmission.UnsupportedBlockers
			plan.UnsupportedBlockerCount = len(replayAdmission.UnsupportedBlockers)
		}
		plan.ReplayResumeAdmission = replayAdmission
		plan.ExecutionReady = replayAdmission.StateOnlyExecutionReady || replayAdmission.DeliveryEventReplayReady

		contractsRoot, err := normalizeContractsRoot(resolveContractsPath(repo, contracts))
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: resolve contracts: %v\n", err)
			}
			return 1
		}
		_, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvePath(repo, *platformSpecPath))
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: load selected contracts: %v\n", err)
			}
			return 1
		}
		source := semanticview.Wrap(bundle)
		admission, err := runtimerunforkadmission.AdmitContractFrontier(runtimerunforkadmission.ContractFrontierRequest{
			Plan:              plan,
			Source:            source,
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: admit selected contract frontier: %v\n", err)
			}
			return 1
		}
		routeAdmission, err := runtimerunforkadmission.AdmitSelectedContractRouteHistory(runtimerunforkadmission.SelectedContractRouteHistoryRequest{
			Plan:              plan,
			Source:            source,
			ContractSelection: runtimerunforkadmission.SelectedContractSelection(source, contractsRoot),
			FrontierAdmission: admission,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: admit selected contract routes: %v\n", err)
			}
			return 1
		}
		routeTopology, err := runtimerunforkexecution.BuildSelectedContractRouteTopology(runtimerunforkexecution.SelectedContractRouteTopologyRequest{
			Admission:      admission,
			RouteAdmission: routeAdmission,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: classify selected contract route topology: %v\n", err)
			}
			return 1
		}
		executionModel, err := runtimerunforkexecution.BuildSelectedContractExecutionModel(runtimerunforkexecution.SelectedContractExecutionModelRequest{
			Admission:      admission,
			RouteAdmission: routeAdmission,
			RouteTopology:  routeTopology,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: model selected contract execution: %v\n", err)
			}
			return 1
		}
		readiness, err := runtimerunforkexecution.BuildSelectedContractReadinessClassifier(runtimerunforkexecution.SelectedContractReadinessClassifierRequest{
			Plan:                      plan,
			ContractFrontierAdmission: admission,
			SelectedContractExecution: executionModel,
		})
		if err != nil {
			if out != nil {
				fmt.Fprintf(out, "fork failed: classify selected contract readiness: %v\n", err)
			}
			return 1
		}
		plan.ContractFrontierAdmission = &admission
		plan.SelectedContractExecution = &executionModel
		plan.SelectedContractReadiness = &readiness
	}
	if *asJSON {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(plan); err != nil {
			fmt.Fprintf(out, "fork failed: encode json: %v\n", err)
			return 1
		}
		return 0
	}
	printRunForkPlan(out, plan)
	return 0
}

func printRunForkActivation(w io.Writer, result store.RunForkActivation) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork activated for source run %s\n", result.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.ForkRunID, result.ForkRunStatus)
	fmt.Fprintf(w, "Source Run: %s status=%s\n", result.SourceRunID, result.SourceRunStatus)
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", result.ForkPoint.EventName, result.ForkPoint.EventID, result.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: activated=%t source_frozen=%t historical_replay_blocked=%t materialized_entities=%d\n",
		result.Activated,
		result.SourceFrozen,
		result.HistoricalReplayBlocked,
		result.MaterializedEntityCount,
	)
	if result.DeliveryEventReplay != nil {
		fmt.Fprintf(w, "Delivery/Event Replay: events=%d deliveries=%d owner=%s\n",
			result.DeliveryEventReplay.ReplayedEventCount,
			result.DeliveryEventReplay.ReplayedDeliveryCount,
			result.DeliveryEventReplay.Owner,
		)
	}
	if result.SelectedContractBinding != nil {
		fmt.Fprintf(w, "Selected Contract Binding: owner=%s contracts=%s workflow=%s@%s\n",
			result.SelectedContractBinding.Owner,
			result.SelectedContractBinding.ContractSelection.ContractsRoot,
			result.SelectedContractBinding.ContractSelection.WorkflowName,
			result.SelectedContractBinding.ContractSelection.WorkflowVersion,
		)
	}
}

func printSelectedContractExecution(w io.Writer, result runtimerunforkexecution.SelectedContractExecutionResult) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Selected-contract fork executed for source run %s\n", result.Materialization.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.Materialization.ForkRunID, result.Activation.ForkRunStatus)
	fmt.Fprintf(w, "Source Run: %s status=%s\n", result.Activation.SourceRunID, result.Activation.SourceRunStatus)
	fmt.Fprintf(w, "Owner: %s\n", result.Owner)
	fmt.Fprintf(w, "Summary: executed_events=%d materialized_entities=%d source_frozen=%t\n",
		result.ExecutedEventCount,
		result.Materialization.MaterializedEntityCount,
		result.Activation.SourceFrozen,
	)
	if result.Activation.BranchDivergence != nil {
		fmt.Fprintf(w, "Selected Contract Branch: owner=%s policy=%s source_frozen=%t source_status=%s facts=%s\n",
			result.Activation.BranchDivergence.Owner,
			result.Activation.BranchDivergence.Policy,
			result.Activation.BranchDivergence.SourceFrozen,
			result.Activation.BranchDivergence.SourceRunStatusAfterActivation,
			strings.Join(result.Activation.BranchDivergence.SourceAdvancedFacts, ","),
		)
	}
	if len(result.ForkEvents) > 0 {
		fmt.Fprintln(w, "Fork Events:")
		for _, event := range result.ForkEvents {
			fmt.Fprintf(w, "  %s -> %s %s\n", event.SourceEventID, event.ForkEventID, event.EventName)
		}
	}
}

func printRunForkMaterialization(w io.Writer, result store.RunForkMaterialization) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork materialized for run %s\n", result.SourceRunID)
	fmt.Fprintf(w, "Fork Run: %s status=%s\n", result.ForkRunID, result.ForkRunStatus)
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", result.ForkPoint.EventName, result.ForkPoint.EventID, result.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: materialized_entities=%d delivery_resume_blocked=%t source_status_unchanged=%t\n",
		result.MaterializedEntityCount,
		result.DeliveryResumeBlocked,
		result.SourceRunStatusUnchanged,
	)
	if result.SelectedContractBinding != nil {
		fmt.Fprintf(w, "Selected Contract Binding: owner=%s contracts=%s workflow=%s@%s\n",
			result.SelectedContractBinding.Owner,
			result.SelectedContractBinding.ContractSelection.ContractsRoot,
			result.SelectedContractBinding.ContractSelection.WorkflowName,
			result.SelectedContractBinding.ContractSelection.WorkflowVersion,
		)
	}
}

func printRunForkPlan(w io.Writer, plan store.RunForkPlan) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Fork plan for run %s\n", plan.SourceRunID)
	if strings.TrimSpace(plan.SourceRunStatus) != "" {
		fmt.Fprintf(w, "Source Status: %s\n", plan.SourceRunStatus)
	}
	fmt.Fprintf(w, "Fork Point: %s (%s) at %s\n", plan.ForkPoint.EventName, plan.ForkPoint.EventID, plan.ForkPoint.Timestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(w, "Summary: events_at_fork=%d reconstructed_entities=%d pending_work=%d unsupported_blockers=%d execution_ready=%t\n",
		plan.EventCountAtFork,
		plan.ReconstructedEntityCount,
		plan.PendingWorkCount,
		plan.UnsupportedBlockerCount,
		plan.ExecutionReady,
	)
	if len(plan.UnsupportedBlockers) > 0 {
		fmt.Fprintln(w, "Unsupported Blockers:")
		for _, blocker := range plan.UnsupportedBlockers {
			fmt.Fprintf(w, "  %s: %s\n", blocker.Code, blocker.Message)
		}
	}
	if len(plan.PendingWork) > 0 {
		fmt.Fprintln(w, "Pending Work:")
		for _, item := range plan.PendingWork {
			fmt.Fprintf(w, "  %s %s subscriber=%s/%s status=%s class=%s\n",
				item.EventID,
				item.EventName,
				item.SubscriberType,
				item.SubscriberID,
				item.Status,
				item.Classification,
			)
		}
	}
	if plan.ContractFrontierAdmission != nil {
		admission := plan.ContractFrontierAdmission
		fmt.Fprintf(w, "Contract Frontier Admission: owner=%s non_mutating=%t frontier_events=%d historical_execution_supported=%t\n",
			admission.Owner,
			admission.NonMutating,
			admission.FrontierEventCount,
			admission.HistoricalExecutionSupported,
		)
	}
	if plan.SelectedContractExecution != nil {
		model := plan.SelectedContractExecution
		fmt.Fprintf(w, "Selected Contract Execution Model: owner=%s non_mutating=%t execution_supported=%t future_owner=%s\n",
			model.Owner,
			model.NonMutating,
			model.ExecutionSupported,
			model.FutureExecutionOwner,
		)
	}
	if plan.SelectedContractReadiness != nil {
		readiness := plan.SelectedContractReadiness
		fmt.Fprintf(w, "Selected Contract Readiness: owner=%s non_mutating=%t facts=%d future_owner=%s\n",
			readiness.Owner,
			readiness.NonMutating,
			len(readiness.FactMatrix),
			readiness.FutureExecutionOwner,
		)
	}
}

func loadRunStatusReport(ctx context.Context, pg *store.PostgresStore, runID string, opts runStatusOptions) (runStatusReport, error) {
	if pg == nil || pg.DB == nil {
		return runStatusReport{}, errors.New("postgres store is required")
	}
	if strings.TrimSpace(runID) == "" {
		resolvedRunID, err := pg.ResolveLatestRunDebugRunID(ctx)
		if err != nil {
			return runStatusReport{}, err
		}
		runID = resolvedRunID
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return runStatusReport{}, errors.New("run_id is required")
	}
	detail, err := pg.LoadRunDebugReport(ctx, runID, store.RunDebugQueryOptions{
		LogsAllLevels:   opts.LogsAllLevels,
		Component:       opts.Component,
		EventLimit:      15,
		MutationLimit:   20,
		RuntimeLogLimit: logLimitForStatus(opts),
		DeadLetterLimit: 10,
	})
	if err != nil {
		return runStatusReport{}, err
	}
	report := runStatusReportFromStore(detail)
	operational := store.ProjectRunOperationalStatus(detail)
	report.OperationalState = operational.State
	report.BlockingLayer = operational.BlockingLayer
	report.BlockingReason = operational.BlockingReason
	report.Heuristics = append([]string(nil), operational.Heuristics...)

	return report, nil
}

func logLimitForStatus(opts runStatusOptions) int {
	if opts.LogsOnly || opts.LogsAllLevels || strings.TrimSpace(opts.Component) != "" {
		return 100
	}
	return 20
}

func printRunStatusReport(w io.Writer, report runStatusReport) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Run %s\n", report.RunID)
	if strings.TrimSpace(report.RootEventType) != "" {
		fmt.Fprintf(w, "Root: %s (%s)\n", report.RootEventType, report.RootEventID)
	}
	if strings.TrimSpace(report.RunTableStatus) != "" {
		fmt.Fprintf(w, "Run Table Status: %s\n", report.RunTableStatus)
	}
	if strings.TrimSpace(report.OperationalState) != "" {
		fmt.Fprintf(w, "Operational State: %s\n", report.OperationalState)
	}
	if strings.TrimSpace(report.BlockingLayer) != "" {
		fmt.Fprintf(w, "Blocking Layer: %s\n", report.BlockingLayer)
	}
	if strings.TrimSpace(report.BlockingReason) != "" {
		fmt.Fprintf(w, "Blocking Reason: %s\n", report.BlockingReason)
	}
	fmt.Fprintf(w, "Started: %s\n", report.StartedAt.Format(time.RFC3339))
	if !report.LastEventAt.IsZero() {
		fmt.Fprintf(w, "Last Event: %s\n", report.LastEventAt.Format(time.RFC3339))
	}
	if report.EndedAt != nil && !report.EndedAt.IsZero() {
		fmt.Fprintf(w, "Ended: %s\n", report.EndedAt.Format(time.RFC3339))
	}
	fmt.Fprintf(w, "Summary: events=%d deliveries=%d dead_letters=%d agent_turns=%d runtime_warn_errors=%d\n",
		report.EventCount,
		len(report.Deliveries),
		len(report.DeadLetters),
		len(report.AgentTurns),
		report.WarnErrorLogCount,
	)
	if len(report.Heuristics) > 0 {
		fmt.Fprintln(w, "\nHeuristics:")
		for _, item := range report.Heuristics {
			fmt.Fprintf(w, "  %s\n", item)
		}
	}
	if len(report.EventCounts) > 0 {
		fmt.Fprintln(w, "\nEvent Counts:")
		for _, item := range report.EventCounts {
			fmt.Fprintf(w, "  %s  %d\n", item.EventName, item.Count)
		}
	}
	if len(report.Deliveries) > 0 {
		fmt.Fprintln(w, "\nDeliveries:")
		for _, item := range report.Deliveries {
			fmt.Fprintf(w, "  %s  status=%s  count=%d\n", item.SubscriberID, item.Status, item.Count)
		}
	}
	if len(report.AgentTurns) > 0 {
		fmt.Fprintln(w, "\nAgent Turns:")
		for _, item := range report.AgentTurns {
			fmt.Fprintf(w, "  %s  turns=%d errors=%d last=%s\n", item.AgentID, item.Turns, item.ErrorCount, item.LastAt.Format(time.RFC3339))
		}
	}
	if len(report.Mutations) > 0 {
		fmt.Fprintln(w, "\nRecent Mutations:")
		for _, item := range report.Mutations {
			fmt.Fprintf(w, "  %s  entity=%s  writer=%s/%s  step=%s  at=%s\n",
				item.Field,
				item.EntityID,
				item.WriterType,
				item.WriterID,
				item.HandlerStep,
				item.CreatedAt.Format(time.RFC3339),
			)
		}
	}
	if len(report.DeadLetters) > 0 {
		fmt.Fprintln(w, "\nDead Letters:")
		for _, item := range report.DeadLetters {
			fmt.Fprintf(w, "  %s  entity=%s  type=%s  handler=%s  at=%s\n",
				item.OriginalEvent,
				item.EntityID,
				item.FailureType,
				item.HandlerNode,
				item.CreatedAt.Format(time.RFC3339),
			)
			if strings.TrimSpace(item.ErrorMessage) != "" {
				fmt.Fprintf(w, "    error=%s\n", item.ErrorMessage)
			}
		}
	}
	if len(report.RuntimeLogs) > 0 {
		if len(report.RuntimeLogSummary) > 0 {
			fmt.Fprintln(w, "\nRuntime Log Summary:")
			for _, item := range report.RuntimeLogSummary {
				fmt.Fprintf(w, "  %s  %s/%s  count=%d\n",
					strings.ToUpper(item.Level),
					item.Component,
					item.Action,
					item.Count,
				)
			}
		}
		fmt.Fprintln(w, "\nRuntime Warnings/Errors:")
		for _, item := range report.RuntimeLogs {
			fmt.Fprintf(w, "  %s  %s/%s  at=%s\n",
				strings.ToUpper(item.Level),
				item.Component,
				item.Action,
				item.CreatedAt.Format(time.RFC3339),
			)
			if strings.TrimSpace(item.Error) != "" {
				fmt.Fprintf(w, "    error=%s\n", item.Error)
			}
		}
	}
	if len(report.RecentEvents) > 0 {
		fmt.Fprintln(w, "\nRecent Events:")
		for _, item := range report.RecentEvents {
			fmt.Fprintf(w, "  %s  entity=%s  at=%s\n",
				item.EventName,
				item.EntityID,
				item.CreatedAt.Format(time.RFC3339),
			)
		}
	}
}

func runStatusReportFromStore(detail store.RunDebugReport) runStatusReport {
	report := runStatusReport{
		RunID:             detail.RunID,
		RunTableStatus:    detail.RunTableStatus,
		RootEventID:       detail.RootEventID,
		RootEventType:     detail.RootEventType,
		StartedAt:         detail.StartedAt,
		LastEventAt:       detail.LastEventAt,
		EndedAt:           detail.EndedAt,
		EventCount:        detail.EventCount,
		WarnErrorLogCount: detail.WarnErrorLogCount,
		RuntimeLogSummary: make([]runStatusRuntimeSummary, 0, len(detail.RuntimeLogSummary)),
		RuntimeLogs:       make([]runStatusRuntimeLog, 0, len(detail.RuntimeLogs)),
		EventCounts:       make([]runStatusEventCount, 0, len(detail.EventCounts)),
		Deliveries:        make([]runStatusDeliveryCount, 0, len(detail.Deliveries)),
		RecentEvents:      make([]runStatusEvent, 0, len(detail.Events)),
		Mutations:         make([]runStatusMutation, 0, len(detail.Mutations)),
		DeadLetters:       make([]runStatusDeadLetter, 0, len(detail.DeadLetters)),
		AgentTurns:        make([]runStatusAgentTurn, 0, len(detail.AgentTurns)),
	}
	for _, item := range detail.RuntimeLogSummary {
		report.RuntimeLogSummary = append(report.RuntimeLogSummary, runStatusRuntimeSummary(item))
	}
	for _, item := range detail.RuntimeLogs {
		report.RuntimeLogs = append(report.RuntimeLogs, runStatusRuntimeLog{
			Level:     item.Level,
			Component: item.Component,
			Action:    item.Action,
			Error:     item.Error,
			CreatedAt: item.CreatedAt,
		})
	}
	for _, item := range detail.EventCounts {
		report.EventCounts = append(report.EventCounts, runStatusEventCount(item))
	}
	for _, item := range detail.Deliveries {
		report.Deliveries = append(report.Deliveries, runStatusDeliveryCount(item))
	}
	for _, item := range detail.Events {
		report.RecentEvents = append(report.RecentEvents, runStatusEvent{
			EventName: item.EventName,
			EntityID:  item.EntityID,
			CreatedAt: item.CreatedAt,
		})
	}
	for _, item := range detail.Mutations {
		report.Mutations = append(report.Mutations, runStatusMutation(item))
	}
	for _, item := range detail.DeadLetters {
		report.DeadLetters = append(report.DeadLetters, runStatusDeadLetter(item))
	}
	for _, item := range detail.AgentTurns {
		report.AgentTurns = append(report.AgentTurns, runStatusAgentTurn(item))
	}
	return report
}

func verifyBundle(ctx context.Context, source semanticview.Source) error {
	_, err := verifyBundleResult(ctx, source)
	return err
}

func verifyBundleResult(ctx context.Context, source semanticview.Source) (runtime.WorkflowContractValidationResult, error) {
	credentialStore, err := buildCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationResult{}, fmt.Errorf("configure credentials: %w", err)
	}
	return verifyBundleResultWithOptions(ctx, source, runtime.DefaultWorkflowContractValidationOptions(credentialStore))
}

func verifyBundleResultWithOptions(ctx context.Context, source semanticview.Source, opts runtime.WorkflowContractValidationOptions) (runtime.WorkflowContractValidationResult, error) {
	if source == nil {
		return runtime.WorkflowContractValidationResult{}, errors.New("semantic source is required")
	}
	return runtime.ValidateWorkflowContractSurface(ctx, source, opts)
}

func verifyWorkflowContractValidationOptions(repo string) (runtime.WorkflowContractValidationOptions, error) {
	credentialStore, err := buildCredentialStore()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("configure credentials: %w", err)
	}
	opts := runtime.DefaultWorkflowContractValidationOptions(credentialStore)
	configResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("load runtime config: %w", err)
	}
	profile, err := configResult.Config.LLMBackendProfile()
	if err != nil {
		return runtime.WorkflowContractValidationOptions{}, fmt.Errorf("resolve llm backend profile: %w", err)
	}
	opts.ValidateLLMModelResolution = true
	opts.LLMProfile = profile
	opts.ModelAliases = configResult.Config.LLM.Models
	return opts, nil
}

func writeVerifyFindings(out io.Writer, findings []runtimebootverify.Finding, blocking bool) {
	if out == nil || len(findings) == 0 {
		return
	}
	for _, finding := range findings {
		fmt.Fprintln(out, runtimebootverify.FormatSurfaceFinding(finding, blocking))
	}
}

type runtimeConfigLoadOptions struct {
	RepoRoot        string
	ExplicitPath    string
	BackendOverride string
}

type runtimeConfigLoadResult struct {
	Config *config.Config
	Source string
	Path   string
}

func (r runtimeConfigLoadResult) Detail() string {
	source := strings.TrimSpace(r.Source)
	if source == "" {
		source = "unknown"
	}
	path := strings.TrimSpace(r.Path)
	if path == "" {
		return source
	}
	return fmt.Sprintf("%s:%s", source, filepath.Clean(path))
}

func loadRuntimeConfig(path string) (*config.Config, error) {
	result, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{ExplicitPath: path})
	if err != nil {
		return nil, err
	}
	return result.Config, nil
}

func loadRuntimeConfigWithOptions(opts runtimeConfigLoadOptions) (runtimeConfigLoadResult, error) {
	if err := rejectUnsupportedRuntimeControlEnv(); err != nil {
		return runtimeConfigLoadResult{}, err
	}
	if err := llmselection.RejectRetiredEnvBackend(os.LookupEnv); err != nil {
		return runtimeConfigLoadResult{}, err
	}
	if err := llmselection.RejectRetiredEnvRuntimeMode(os.LookupEnv); err != nil {
		return runtimeConfigLoadResult{}, err
	}
	if err := llmselection.RejectRetiredOpenAICompatibleBaseURLEnv(os.LookupEnv); err != nil {
		return runtimeConfigLoadResult{}, err
	}
	if err := llmselection.RejectRetiredModelEnv(os.LookupEnv); err != nil {
		return runtimeConfigLoadResult{}, err
	}

	explicitPath := strings.TrimSpace(opts.ExplicitPath)
	var result runtimeConfigLoadResult
	var cfg *config.Config
	backendOverride := strings.TrimSpace(opts.BackendOverride)
	if explicitPath != "" {
		path := resolvePath(opts.RepoRoot, explicitPath)
		loaded, err := config.LoadWithOptions(path, config.LoadOptions{BackendOverride: backendOverride})
		if err != nil {
			return runtimeConfigLoadResult{}, fmt.Errorf("load %s: %w", path, err)
		}
		cfg = loaded
		result.Source = "explicit"
		result.Path = path
	} else if path, ok, err := executableAdjacentRuntimeConfigPath(); err != nil {
		return runtimeConfigLoadResult{}, err
	} else if ok {
		loaded, err := config.LoadWithOptions(path, config.LoadOptions{BackendOverride: backendOverride})
		if err != nil {
			return runtimeConfigLoadResult{}, fmt.Errorf("load %s: %w", path, err)
		}
		cfg = loaded
		result.Source = "executable"
		result.Path = path
	} else {
		loaded, err := defaultRuntimeConfig()
		if err != nil {
			return runtimeConfigLoadResult{}, err
		}
		cfg = loaded
		result.Source = "built-in default"
	}
	if backendOverride != "" && result.Source == "built-in default" {
		cfg.LLM.Backend = backendOverride
		if err := cfg.Validate(); err != nil {
			return runtimeConfigLoadResult{}, err
		}
	}
	result.Config = cfg
	return result, nil
}

func executableAdjacentRuntimeConfigPath() (string, bool, error) {
	executable, err := runtimeConfigExecutablePath()
	if err != nil {
		return "", false, fmt.Errorf("resolve executable config path: %w", err)
	}
	executable = strings.TrimSpace(executable)
	if executable == "" {
		return "", false, nil
	}
	path := filepath.Join(filepath.Dir(executable), "config.yaml")
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return "", false, fmt.Errorf("executable-adjacent runtime config %s is a directory", path)
		}
		return path, true, nil
	}
	if os.IsNotExist(err) {
		return "", false, nil
	}
	return "", false, fmt.Errorf("inspect executable-adjacent runtime config %s: %w", path, err)
}

func defaultRuntimeConfig() (*config.Config, error) {
	if err := rejectUnsupportedRuntimeControlEnv(); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredEnvBackend(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredEnvRuntimeMode(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredOpenAICompatibleBaseURLEnv(os.LookupEnv); err != nil {
		return nil, err
	}
	if err := llmselection.RejectRetiredModelEnv(os.LookupEnv); err != nil {
		return nil, err
	}
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			RecoveryOnStartup: envBool("SWARM_RUNTIME_RECOVERY_ON_STARTUP", false),
		},
		Database: config.DatabaseConfig{
			Host:     envOrDefault("SWARM_DB_HOST", envOrDefault("PGHOST", "127.0.0.1")),
			Port:     envInt("SWARM_DB_PORT", envInt("PGPORT", 5432)),
			Name:     envOrDefault("SWARM_DB_NAME", envOrDefault("PGDATABASE", "swarm")),
			User:     envOrDefault("SWARM_DB_USER", envOrDefault("PGUSER", "postgres")),
			Password: envOrDefault("SWARM_DB_PASSWORD", envOrDefault("PGPASSWORD", "postgres")),
			SSLMode:  envOrDefault("SWARM_DB_SSLMODE", "disable"),
			PoolSize: envInt("SWARM_DB_POOL_SIZE", 5),
		},
		LLM: config.LLMConfig{
			Backend: llmselection.DefaultBackendID(),
			Session: config.LLMSessionConfig{
				LockTTL:               envDuration("SWARM_LLM_SESSION_LOCK_TTL", 10*time.Second),
				RotateAfterTurns:      envInt("SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", 40),
				RotateOnParseFailures: envInt("SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", 3),
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				MaxRetries:   envInt("SWARM_CLAUDE_API_MAX_RETRIES", 1),
				RetryBackoff: envDuration("SWARM_CLAUDE_API_RETRY_BACKOFF", 2*time.Second),
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              envOrDefault("SWARM_CLAUDE_CLI_COMMAND", "claude"),
				Timeout:              envDuration("SWARM_CLAUDE_CLI_TIMEOUT", time.Hour),
				OutputFormat:         envOrDefault("SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "stream-json"),
				Retries:              envInt("SWARM_CLAUDE_CLI_RETRIES", 1),
				NoSessionPersistence: envBool("SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", false),
				UseTMux:              envBool("SWARM_CLAUDE_CLI_USE_TMUX", false),
			},
			OpenAICompatible: config.OpenAICompatibleConfig{},
		},
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func resolveRuntimeStoreSelection(repo string, storeMode string, storeModeSet bool, cfg *config.Config) (storebackend.Selection, error) {
	if cfg == nil {
		return storebackend.Selection{}, errors.New("runtime config is required")
	}
	envBackend, envBackendSet := os.LookupEnv(storebackend.EnvStoreBackend)
	envSQLitePath, envSQLitePathSet := os.LookupEnv(storebackend.EnvSQLitePath)
	return storebackend.Resolve(storebackend.Input{
		RepoRoot:         repo,
		FlagBackend:      storeMode,
		FlagBackendSet:   storeModeSet,
		EnvBackend:       envBackend,
		EnvBackendSet:    envBackendSet,
		ConfigBackend:    cfg.Store.Backend,
		EnvSQLitePath:    envSQLitePath,
		EnvSQLitePathSet: envSQLitePathSet,
		ConfigSQLitePath: cfg.Store.SQLite.Path,
	})
}

func rejectUnsupportedRuntimeControlEnv() error {
	unsupported := make([]string, 0, 2)
	if _, ok := os.LookupEnv("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS"); ok {
		unsupported = append(unsupported, "SWARM_RUNTIME_MAX_CONCURRENT_AGENTS")
	}
	if _, ok := os.LookupEnv("SWARM_RUNTIME_EVENT_POLL_INTERVAL"); ok {
		unsupported = append(unsupported, "SWARM_RUNTIME_EVENT_POLL_INTERVAL")
	}
	if len(unsupported) == 0 {
		return nil
	}
	sort.Strings(unsupported)
	return fmt.Errorf("unsupported inert runtime controls configured: %s", strings.Join(unsupported, ", "))
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func loadRepoDotEnv(repo string) error {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return nil
	}
	path := filepath.Join(repo, ".env")
	return loadDotEnvFile(path)
}

func loadDotEnvFile(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("%s:%d: empty env key", path, lineNo)
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		} else if len(value) >= 2 {
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) || (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				value = value[1 : len(value)-1]
			}
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("%s:%d: set %s: %w", path, lineNo, key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}

func normalizeContractsRoot(path string) (string, error) {
	root := strings.TrimSpace(path)
	if root == "" {
		return "", errors.New("contracts path is required")
	}
	root = filepath.Clean(root)
	if regularFileExists(filepath.Join(root, "package.yaml")) {
		return root, nil
	}
	if filepath.Base(root) == "package.yaml" && regularFileExists(root) {
		return filepath.Dir(root), nil
	}
	return "", fmt.Errorf("no package.yaml found under %s", path)
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

func resolvePath(repoRoot, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(repoRoot, path)
}

func buildStores(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
	if cfg == nil {
		return storeBundle{}, errors.New("runtime config is required")
	}
	switch selection.Backend {
	case storebackend.BackendPostgres:
		dsn := store.DSNFromConfig(cfg.Database)
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
		return storeBundle{
			Postgres:            pg,
			SQLDB:               pg.DB,
			RuntimeSQLDB:        pg.DB,
			RuntimeLogStore:     pg,
			SchemaBootstrapper:  pg,
			EventStore:          pg,
			PipelineStore:       runtimepipeline.NewWorkflowInstanceStore(pg.DB),
			SessionRegistry:     sessions.NewPostgresRegistry(pg.DB, cfg.LLM.Session.LockTTL),
			ConversationStore:   pg,
			ManagerStore:        pg,
			ScheduleStore:       pg,
			MailboxMaterializer: pg,
			MailboxStore:        pg,
			ToolEntityStore:     pg,
			HumanTaskStore:      pg,
			BudgetSpendStore:    pg,
			MailboxAPIStore:     pg,
			ObservabilityStore:  pg,
			AgentUsageStore:     pg,
			RuntimeIngressStore: pg,
			IdempotencyStore:    pg,
			TurnStore:           pg,
			StartupOwnership:    pg,
			RunQuiescenceStore:  pg,
		}, nil
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
		return storeBundle{
			SQLDB:               sqliteStore.DB,
			RuntimeLogStore:     sqliteStore,
			SchemaBootstrapper:  sqliteStore,
			EventStore:          sqliteStore,
			PipelineStore:       runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore),
			SessionRegistry:     sqliteStore,
			ConversationStore:   sqliteStore,
			ManagerStore:        sqliteStore,
			ScheduleStore:       sqliteStore,
			MailboxMaterializer: sqliteStore,
			MailboxStore:        sqliteStore,
			ToolEntityStore:     sqliteStore,
			HumanTaskStore:      sqliteStore,
			BudgetSpendStore:    sqliteStore,
			MailboxAPIStore:     sqliteStore,
			ObservabilityStore:  sqliteStore,
			AgentUsageStore:     sqliteStore,
			RuntimeIngressStore: sqliteStore,
			IdempotencyStore:    sqliteStore,
			TurnStore:           sqliteStore,
			StartupOwnership:    sqliteStore,
			RunQuiescenceStore:  sqliteStore,
		}, nil
	default:
		return storeBundle{}, fmt.Errorf("store backend selection is required; supported backends: %s, %s", storebackend.BackendPostgres, storebackend.BackendSQLite)
	}
}

func enforceServeBundleMatchAdmission(ctx context.Context, pg *store.PostgresStore, bootIdentity string, requireMatch bool, pinnedBundleHash string) error {
	var pinned []string
	if hash := strings.TrimSpace(pinnedBundleHash); hash != "" {
		pinned = []string{hash}
	}
	return enforceServeBundleMatchAdmissionForHashes(ctx, pg, bootIdentity, requireMatch, pinned)
}

func enforceServeBundleMatchAdmissionForHashes(ctx context.Context, pg *store.PostgresStore, bootIdentity string, requireMatch bool, pinnedBundleHashes []string) error {
	bootIdentity = strings.TrimSpace(bootIdentity)
	pinnedBundleHashes = uniqueTrimmedServeBundleHashes(pinnedBundleHashes)
	if !requireMatch {
		log.Printf("legacy bundle match admission disabled by --no-require-bundle-match; startup recovery has already consumed active run bundle source state")
	}
	enforceActiveAvailability := requireMatch || len(pinnedBundleHashes) > 0
	if enforceActiveAvailability && bootIdentity == "" {
		return fmt.Errorf("boot bundle identity is required")
	}
	if pg == nil {
		return nil
	}
	if enforceActiveAvailability {
		conflicts, err := pg.ActiveRunBundleAvailabilityConflicts(ctx)
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
	mismatches, err := activeRunPinnedBundleHashesConflicts(ctx, pg, pinnedBundleHashes)
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

func activeRunPinnedBundleHashConflicts(ctx context.Context, pg *store.PostgresStore, pinnedBundleHash string) ([]runbundle.Availability, error) {
	pinnedBundleHash = strings.TrimSpace(pinnedBundleHash)
	if pinnedBundleHash == "" {
		return nil, nil
	}
	availabilities, err := pg.ActiveRunBundleAvailabilities(ctx)
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

func activeRunPinnedBundleHashesConflicts(ctx context.Context, pg *store.PostgresStore, pinnedBundleHashes []string) ([]runbundle.Availability, error) {
	allowed := map[string]struct{}{}
	for _, hash := range uniqueTrimmedServeBundleHashes(pinnedBundleHashes) {
		allowed[hash] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, nil
	}
	availabilities, err := pg.ActiveRunBundleAvailabilities(ctx)
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

func initializeStateStores(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle, verbose bool) (string, error) {
	if stores.SchemaBootstrapper == nil || bundle == nil {
		return "store wiring ready", nil
	}
	platformPlans, err := store.GeneratePlatformTableDDLs(bundle.Platform)
	if err != nil {
		return "", fmt.Errorf("platform-owned tables: %w", err)
	}
	statePlans, err := store.GenerateNodeStateTableDDLs(bundle.NodeEntries())
	if err != nil {
		return "", fmt.Errorf("state_schema tables: %w", err)
	}
	plans := append([]store.SchemaTableDDL{}, platformPlans...)
	plans = append(plans, statePlans...)
	if err := ensureServeSchemaTables(ctx, stores, plans); err != nil {
		return "", err
	}
	return summarizeServeSchemaPlans(plans, verbose), nil
}

func initializeServePlatformStateStores(ctx context.Context, stores storeBundle, platformSpecPath string, verbose bool) (string, error) {
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
	if err := ensureServeSchemaTables(ctx, stores, plans); err != nil {
		return "", err
	}
	return summarizeServeSchemaPlans(plans, verbose), nil
}

func initializeLoadedServeRuntimeStateStores(ctx context.Context, stores storeBundle, loaded []serveRuntimeBundle, verbose bool) ([]string, error) {
	summaries := make([]string, len(loaded))
	for i, bundle := range loaded {
		summary, err := initializeStateStores(ctx, stores, bundle.bundle, verbose)
		if err != nil {
			return nil, fmt.Errorf("bundle %s state stores: %w", bundle.serveIdentityDetail(), err)
		}
		summaries[i] = summary
	}
	return summaries, nil
}

func ensureServeSchemaTables(ctx context.Context, stores storeBundle, plans []store.SchemaTableDDL) error {
	if stores.SchemaBootstrapper == nil {
		return nil
	}
	if err := stores.SchemaBootstrapper.EnsureSchemaTables(ctx, plans); err != nil {
		return err
	}
	if err := rebindServePostgresSchemaCapabilities(ctx, stores); err != nil {
		return err
	}
	return nil
}

func rebindServePostgresSchemaCapabilities(ctx context.Context, stores storeBundle) error {
	if stores.Postgres == nil {
		return nil
	}
	if _, err := stores.Postgres.BindSchemaCapabilities(ctx); err != nil {
		return fmt.Errorf("bind post-bootstrap schema capabilities: %w", err)
	}
	return nil
}

func loadServePlatformSpecDocument(platformSpecPath string) (runtimecontracts.PlatformSpecDocument, error) {
	platformSpecPath = strings.TrimSpace(platformSpecPath)
	if platformSpecPath == "" {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("platform spec path is required")
	}
	data, err := os.ReadFile(platformSpecPath)
	if err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("read platform spec: %w", err)
	}
	var spec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return runtimecontracts.PlatformSpecDocument{}, fmt.Errorf("unmarshal platform spec: %w", err)
	}
	return spec, nil
}

type serveSchemaPlanSummary struct {
	tableCount  int
	columnCount int
	detail      string
}

func summarizeServeSchemaPlans(plans []store.SchemaTableDDL, verbose bool) string {
	summary := newServeSchemaPlanSummary(plans)
	slog.Info("swarm boot state stores", "tables", summary.tableCount, "columns", summary.columnCount)
	return summary.text(verbose)
}

func newServeSchemaPlanSummary(plans []store.SchemaTableDDL) serveSchemaPlanSummary {
	tableNames := make([]string, 0, len(plans))
	totalColumns := 0
	for _, plan := range plans {
		tableNames = append(tableNames, fmt.Sprintf("%s(%d)", strings.TrimSpace(plan.TableName), plan.ColumnCount))
		totalColumns += plan.ColumnCount
	}
	sort.Strings(tableNames)
	return serveSchemaPlanSummary{
		tableCount:  len(plans),
		columnCount: totalColumns,
		detail:      strings.Join(tableNames, ", "),
	}
}

func (summary serveSchemaPlanSummary) text(verbose bool) string {
	if summary.tableCount == 0 {
		return "verified 0 generated tables"
	}
	base := fmt.Sprintf("verified %d generated tables", summary.tableCount)
	if verbose && strings.TrimSpace(summary.detail) != "" {
		return fmt.Sprintf("%s (%s)", base, summary.detail)
	}
	return base
}

func serveStateStoreSummaryAt(summaries []string, index int) string {
	if index < 0 || index >= len(summaries) {
		return ""
	}
	return strings.TrimSpace(summaries[index])
}

type swarmWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func newSwarmWorkflowModule(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		return nil, nil, err
	}
	module, _, err := newSwarmWorkflowModuleForBundle(bundle)
	if err != nil {
		return nil, nil, err
	}
	return module, bundle, nil
}

func newSwarmWorkflowModuleForBundle(bundle *runtimecontracts.WorkflowContractBundle) (runtimepipeline.WorkflowModule, semanticview.Source, error) {
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return nil, nil, err
	}
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return nil, nil, err
	}
	return &swarmWorkflowModule{
		bundle:         bundle,
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, source, nil
}

func (m *swarmWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *swarmWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *swarmWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}
func (m *swarmWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry { return m.guardRegistry }
func (m *swarmWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}

type serveBootReporter struct {
	enabled bool
	out     io.Writer
}

func newServeBootReporter(enabled bool, out io.Writer) serveBootReporter {
	if out == nil {
		out = io.Discard
	}
	return serveBootReporter{enabled: enabled, out: out}
}

func (r serveBootReporter) runtimeSink() func(runtime.BootProgressEvent) {
	if !r.enabled {
		return nil
	}
	return func(evt runtime.BootProgressEvent) {
		r.emit(evt.Step, evt.Name, evt.Status, evt.Detail)
	}
}

func (r serveBootReporter) emit(step int, name, status, detail string) {
	if !r.enabled || r.out == nil {
		return
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = "ok"
	}
	detail = strings.TrimSpace(detail)
	if detail != "" {
		detail = "  (" + detail + ")"
	}
	fmt.Fprintf(r.out, "[%d/%d] %-34s %s%s\n", step, runtime.BootProgressTotalSteps, strings.TrimSpace(name), status, detail)
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

func logBootWarnings(report runtimebootverify.Report) {
	warningCounts := make(map[string]int, len(report.Findings))
	for _, finding := range report.Warnings() {
		warningCounts[strings.TrimSpace(finding.CheckID)]++
	}
	if len(warningCounts) > 0 {
		slog.Info("swarm boot validation warning summary",
			"event_wiring_warnings", warningCounts["event_chain_integrity"]+warningCounts["event_consumer_exists"]+warningCounts["event_producer_exists"],
			"tool_warnings", warningCounts["prompt_exists"]+warningCounts["tool_resolution"],
		)
	}
	for _, finding := range report.Warnings() {
		slog.Warn("swarm boot validation warning", "check_id", finding.CheckID, "location", finding.Location, "detail", finding.Message)
	}
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

func newAPIServer(ready *atomic.Bool, apiV1Handler http.Handler) *http.Server {
	mux := http.NewServeMux()
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
		mux.Handle("/v1/rpc", apiV1Handler)
		mux.Handle("/v1/ws", apiV1Handler)
	}
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
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

func validateServeListenAddr(flagName, addr string) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("%s must be a host:port listen address", flagName)
	}
	if strings.Contains(addr, "://") {
		return fmt.Errorf("%s must be a host:port listen address, not a URL", flagName)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be a host:port listen address: %w", flagName, err)
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	port = strings.TrimSpace(port)
	if host == "" {
		return fmt.Errorf("%s must include an explicit host", flagName)
	}
	if port == "" {
		return fmt.Errorf("%s must include an explicit port", flagName)
	}
	numericPort, err := strconv.Atoi(port)
	if err != nil || numericPort < 0 || numericPort > 65535 {
		return fmt.Errorf("%s port must be between 0 and 65535", flagName)
	}
	return nil
}

func validateServeAPIAuthBinding(apiListenAddr string, auth apiv1.AuthTokenResolution) error {
	if !auth.UsesDefaultLoopbackToken() {
		return nil
	}
	if err := validateServeListenAddr("--api-listen-addr", apiListenAddr); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(apiListenAddr))
	if err != nil {
		return fmt.Errorf("--api-listen-addr must be a host:port listen address: %w", err)
	}
	if apiv1.DefaultLoopbackAPITokenAllowedHost(host) {
		return nil
	}
	return fmt.Errorf("non-loopback API bind %s requires an explicit SWARM_API_TOKEN", strings.TrimSpace(apiListenAddr))
}

func listenServeHTTPListener(name, addr string) (net.Listener, error) {
	addr = strings.TrimSpace(addr)
	if err := validateServeListenAddr("--"+name+"-listen-addr", addr); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s listener bind failed: %w", name, err)
	}
	return listener, nil
}

func serveHTTPServer(name string, server *http.Server, listener net.Listener) {
	if server == nil || listener == nil {
		return
	}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("%s server stopped: %v", name, err)
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
			lastErr = probeServeHealthEndpoint(ctx, client, baseURL+"/readyz")
		}
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

func shutdownHTTPServer(name string, server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("%s server shutdown failed: %v", name, err)
	}
}

func logReadySummary(source semanticview.Source, contractsRoot string, apiAddr, mcpAddr net.Addr) {
	log.Printf(
		"swarm runtime ready contracts=%s flows=%d nodes=%d agents=%d events=%d api_listener=%s mcp_listener=%s",
		contractsRoot,
		len(source.FlowSchemaEntries()),
		len(source.NodeEntries()),
		len(source.AgentEntries()),
		len(source.ResolvedEventCatalog()),
		addrString(apiAddr),
		addrString(mcpAddr),
	)
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

func configureServeMCPGatewayEnv(mcpAddr net.Addr) (func(), error) {
	if mcpAddr == nil {
		return func() {}, errors.New("mcp listener address is unavailable")
	}
	mcpHostURL, err := serveListenerHTTPURL(mcpAddr, "127.0.0.1")
	if err != nil {
		return func() {}, err
	}
	mcpContainerURL, err := serveMCPContainerGatewayURL(mcpAddr)
	if err != nil {
		return func() {}, err
	}
	if err := validateExistingServeGatewayURL("SWARM_TOOL_GATEWAY_URL", os.Getenv("SWARM_TOOL_GATEWAY_URL"), mcpAddr); err != nil {
		return func() {}, err
	}
	if err := validateExistingServeGatewayURL("SWARM_TOOL_GATEWAY_CONTAINER_URL", os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"), mcpAddr); err != nil {
		return func() {}, err
	}
	gatewayToken := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"))
	if gatewayToken == "" {
		gatewayToken, err = generateServeMCPGatewayToken()
		if err != nil {
			return func() {}, err
		}
	}
	return setServeGatewayEnv(map[string]string{
		"SWARM_TOOL_GATEWAY_URL":           mcpHostURL,
		"SWARM_TOOL_GATEWAY_CONTAINER_URL": mcpContainerURL,
		"SWARM_TOOL_GATEWAY_TOKEN":         gatewayToken,
	})
}

func generateServeMCPGatewayToken() (string, error) {
	raw := make([]byte, serveGatewayTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate mcp gateway token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func validateExistingServeGatewayURL(name, raw string, mcpAddr net.Addr) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	_, mcpPort, err := splitListenerHostPort(mcpAddr)
	if err != nil {
		return err
	}
	parsed, err := httpURLHostPort(raw)
	if err != nil {
		return fmt.Errorf("%s must be a valid http(s) URL for the MCP listener: %w", name, err)
	}
	if parsed.port != mcpPort {
		return fmt.Errorf("%s must target the MCP listener port %s, got %s", name, mcpPort, parsed.port)
	}
	return nil
}

func setServeGatewayEnv(values map[string]string) (func(), error) {
	previous := make(map[string]previousEnv, len(values))
	for name, value := range values {
		prevValue, prevSet := os.LookupEnv(name)
		previous[name] = previousEnv{value: prevValue, set: prevSet}
		if err := os.Setenv(name, value); err != nil {
			restoreServeGatewayEnv(previous)
			return func() {}, err
		}
	}
	return func() {
		restoreServeGatewayEnv(previous)
	}, nil
}

func restoreServeGatewayEnv(previous map[string]previousEnv) {
	for name, prev := range previous {
		if prev.set {
			_ = os.Setenv(name, prev.value)
		} else {
			_ = os.Unsetenv(name)
		}
	}
}

type parsedHTTPHostPort struct {
	host string
	port string
}

func httpURLHostPort(raw string) (parsedHTTPHostPort, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return parsedHTTPHostPort{}, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return parsedHTTPHostPort{}, fmt.Errorf("scheme must be http or https")
	}
	host := strings.TrimSpace(parsed.Hostname())
	port := strings.TrimSpace(parsed.Port())
	if host == "" || port == "" {
		return parsedHTTPHostPort{}, fmt.Errorf("host and port are required")
	}
	return parsedHTTPHostPort{host: host, port: port}, nil
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

func buildCredentialStore() (runtimecredentials.Store, error) {
	path := strings.TrimSpace(os.Getenv("SWARM_CREDENTIALS_FILE"))
	if path == "" {
		var err error
		path, err = runtimecredentials.DefaultFilePath()
		if err != nil {
			return nil, err
		}
	}
	fileStore, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		return nil, err
	}
	return runtimecredentials.NewOverlayStore(runtimecredentials.NewEnvStore(), fileStore), nil
}

func configuredWorkspaceLifecycle(db *sql.DB, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources) (*workspace.DockerManager, error) {
	manager := workspace.NewDockerManager(db)
	cfg := workspace.DefaultDockerConfig()
	if dataSource := strings.TrimSpace(mountSources.DataSource); dataSource != "" {
		if volumesFrom := strings.TrimSpace(cfg.WorkspaceVolumesFrom); volumesFrom != "" {
			sourceLabel := strings.TrimSpace(mountSources.DataSourceSource)
			if sourceLabel == "" {
				sourceLabel = "explicit data source"
			}
			return nil, fmt.Errorf("workspace data source from %s cannot be combined with SWARM_WORKSPACE_VOLUMES_FROM=%s", sourceLabel, volumesFrom)
		}
		cfg.SharedDataSource = dataSource
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		cfg.ContractsSource = contractsDir
	}
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	return manager, nil
}

func configuredWorkspaceLifecycleForBackend(db *sql.DB, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources, backend workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
	selected := strings.TrimSpace(backend.Backend)
	if selected == "" {
		selected = defaultWorkspaceBackend
	}
	switch selected {
	case workspace.BackendDocker:
		return configuredWorkspaceLifecycle(db, contractsRoot, source, mountSources)
	case workspace.BackendHost:
		return configuredHostWorkspaceLifecycle(db, contractsRoot, source, mountSources)
	default:
		sourceLabel := strings.TrimSpace(backend.Source)
		if sourceLabel == "" {
			sourceLabel = "workspace backend"
		}
		return nil, fmt.Errorf("workspace backend from %s must be docker or host, got %q", sourceLabel, selected)
	}
}

func configuredHostWorkspaceLifecycle(db *sql.DB, contractsRoot string, source semanticview.Source, mountSources workspaceMountSources) (*workspace.HostManager, error) {
	if volumesFrom := strings.TrimSpace(os.Getenv("SWARM_WORKSPACE_VOLUMES_FROM")); volumesFrom != "" {
		return nil, fmt.Errorf("host workspace backend cannot consume SWARM_WORKSPACE_VOLUMES_FROM=%s", volumesFrom)
	}
	manager := workspace.NewHostManager(db)
	cfg := workspace.DefaultHostConfig()
	if dataSource := strings.TrimSpace(mountSources.DataSource); dataSource != "" {
		cfg.SharedDataSource = dataSource
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		cfg.ContractsSource = contractsDir
	}
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	return manager, nil
}

func repoRoot() string {
	root := discoverRepoRoot()
	if root != "" {
		return root
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve cwd: %v", err)
	}
	log.Fatalf("locate repo root from %s", dir)
	return ""
}

func discoverRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func assetCommandRepoRoot(repo string) string {
	if repo = strings.TrimSpace(repo); repo != "" {
		return repo
	}
	return discoverRepoRoot()
}

func embeddedPlatformSpecPath() (string, error) {
	return platform.MaterializePlatformSpecFile()
}

func closeDB(db *sql.DB) {
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		log.Printf("close db: %v", err)
	}
}
