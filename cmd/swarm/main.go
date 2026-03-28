package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	builderpkg "swarm/internal/builder"
	"swarm/internal/config"
	dashboardserver "swarm/internal/dashboard/server"
	"swarm/internal/runtime"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimellm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
	"swarm/internal/store"
)

const (
	defaultPlatformSpecPath = "docs/specs/swarm-platform/platform/contracts/platform-spec.yaml"
	defaultHealthAddr       = ":8081"
)

type storeBundle struct {
	Postgres          *store.PostgresStore
	SQLDB             *sql.DB
	EventStore        runtimebus.EventStore
	SessionRegistry   sessions.Registry
	ConversationStore runtimellm.ConversationPersistence
	ManagerStore      runtimemanager.ManagerPersistence
	ScheduleStore     runtimepipeline.SchedulePersistence
	TurnStore         runtimellm.TurnPersistence
}

func (s storeBundle) runtimeStores() runtime.Stores {
	return runtime.Stores{
		SQLDB:             s.SQLDB,
		EventStore:        s.EventStore,
		SessionRegistry:   s.SessionRegistry,
		ConversationStore: s.ConversationStore,
		ManagerStore:      s.ManagerStore,
		ScheduleStore:     s.ScheduleStore,
		TurnStore:         s.TurnStore,
	}
}

func main() {
	repo := repoRoot()
	configPath := flag.String("config", "", "Optional path to Swarm runtime config")
	contractsPath := flag.String("contracts", "", "Path to Swarm contract bundle root")
	platformSpecPath := flag.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := flag.String("store", "postgres", "Store mode: postgres")
	healthAddr := flag.String("health-addr", defaultHealthAddr, "HTTP bind address for health checks")
	selfCheck := flag.Bool("self-check", true, "Run runtime self-check during boot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolvedConfigPath := resolvePath(repo, *configPath)
	resolvedContractsPath := resolveContractsPath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)

	cfg, err := loadRuntimeConfig(resolvedConfigPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		log.Fatalf("resolve contracts: %v", err)
	}
	module, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath)
	if err != nil {
		log.Fatalf("load Swarm contracts: %v", err)
	}
	if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		slog.Error("validate prompt schema guards", "error", err)
		os.Exit(1)
	}
	source := semanticview.Wrap(bundle)
	stores, err := buildStores(ctx, *storeMode, cfg)
	if err != nil {
		log.Fatalf("init stores: %v", err)
	}
	defer closeDB(stores.SQLDB)
	stateStoreSummary, err := initializeStateStores(ctx, stores, bundle)
	if err != nil {
		slog.Error("initialize state stores", "error", err)
		os.Exit(1)
	}
	workspaces := configuredWorkspaceLifecycle(stores.SQLDB, repo, contractsRoot, source)
	if err := workspaces.ValidateSource(ctx, source); err != nil {
		slog.Error("validate workspaces", "error", err)
		os.Exit(1)
	}
	credentialStore, err := buildCredentialStore()
	if err != nil {
		slog.Error("configure credentials", "error", err)
		os.Exit(1)
	}
	bootReport := runtimebootverify.Run(ctx, source, runtimebootverify.Options{
		Credentials:       credentialStore,
		CheckMCPReachable: true,
	})
	if bootReport.HasErrors() {
		for _, finding := range bootReport.Errors() {
			slog.Error("swarm boot verification failed", "check_id", finding.CheckID, "location", finding.Location, "detail", finding.Message)
		}
		os.Exit(1)
	}

	rt, err := runtime.NewRuntime(ctx, cfg, stores.runtimeStores(), runtime.RuntimeOptions{
		SelfCheck:          *selfCheck,
		WorkflowModule:     module,
		WorkspaceLifecycle: workspaces,
		Credentials:        credentialStore,
	})
	if err != nil {
		log.Fatalf("init runtime: %v", err)
	}

	var ready atomic.Bool
	supervisor := newRuntimeProjectSupervisor(repo, resolvedPlatformSpecPath, cfg, stores, &ready, contractsRoot, bundle, source, rt)
	defer func() {
		if _, err := supervisor.CloseProject(context.Background()); err != nil {
			log.Printf("runtime shutdown failed: %v", err)
		}
	}()
	healthServer := newHealthServer(*healthAddr, &ready, dashboardServerOptions(supervisor, stores, &ready, cfg.LLM.Session.RotateAfterTurns, credentialStore))
	go serveHealth(healthServer)
	defer shutdownHealthServer(healthServer)

	logBootSkeleton(source, contractsRoot, resolvedPlatformSpecPath, bootReport, stateStoreSummary)
	if err := rt.Start(ctx); err != nil {
		log.Fatalf("start runtime: %v", err)
	}
	ready.Store(true)
	logReadySummary(source, contractsRoot, *healthAddr)

	<-ctx.Done()
	ready.Store(false)
}

func loadRuntimeConfig(path string) (*config.Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return defaultRuntimeConfig()
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	return cfg, nil
}

func defaultRuntimeConfig() (*config.Config, error) {
	mode := strings.TrimSpace(os.Getenv("SWARM_LLM_RUNTIME_MODE"))
	if mode == "" {
		if strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")) != "" && strings.TrimSpace(os.Getenv("SWARM_CLAUDE_DEFAULT_MODEL")) != "" {
			mode = "api"
		} else {
			mode = "cli_test"
		}
	}
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			MaxConcurrentAgents: envInt("SWARM_RUNTIME_MAX_CONCURRENT_AGENTS", 10),
			EventPollInterval:   envDuration("SWARM_RUNTIME_EVENT_POLL_INTERVAL", time.Second),
			RecoveryOnStartup:   envBool("SWARM_RUNTIME_RECOVERY_ON_STARTUP", false),
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
			RuntimeMode: mode,
			Session: config.LLMSessionConfig{
				LockTTL:               envDuration("SWARM_LLM_SESSION_LOCK_TTL", 10*time.Second),
				RotateAfterTurns:      envInt("SWARM_LLM_SESSION_ROTATE_AFTER_TURNS", 40),
				RotateOnParseFailures: envInt("SWARM_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", 3),
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: envOrDefault("SWARM_CLAUDE_DEFAULT_MODEL", ""),
				HaikuModel:   envOrDefault("SWARM_CLAUDE_HAIKU_MODEL", ""),
				MaxRetries:   envInt("SWARM_CLAUDE_API_MAX_RETRIES", 1),
				RetryBackoff: envDuration("SWARM_CLAUDE_API_RETRY_BACKOFF", 2*time.Second),
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              envOrDefault("SWARM_CLAUDE_CLI_COMMAND", "claude"),
				Timeout:              envDuration("SWARM_CLAUDE_CLI_TIMEOUT", 15*time.Minute),
				OutputFormat:         envOrDefault("SWARM_CLAUDE_CLI_OUTPUT_FORMAT", "stream-json"),
				Retries:              envInt("SWARM_CLAUDE_CLI_RETRIES", 1),
				NoSessionPersistence: envBool("SWARM_CLAUDE_CLI_NO_SESSION_PERSISTENCE", false),
				UseTMux:              envBool("SWARM_CLAUDE_CLI_USE_TMUX", false),
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
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
	if _, err := os.Stat(filepath.Join(root, "package.yaml")); err == nil {
		return root, nil
	}
	parent := filepath.Dir(root)
	if parent != root {
		if _, err := os.Stat(filepath.Join(parent, "package.yaml")); err == nil {
			return parent, nil
		}
	}
	return "", fmt.Errorf("no package.yaml found under %s", path)
}

func resolveContractsPath(repoRoot, raw string) string {
	if resolved := resolvePath(repoRoot, raw); strings.TrimSpace(resolved) != "" {
		return resolved
	}
	if discovered := strings.TrimSpace(runtimecontracts.DefaultWorkflowContractsDir(repoRoot)); discovered != "" {
		return discovered
	}
	return ""
}

func resolvePath(repoRoot, path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(repoRoot, path)
}

func buildStores(ctx context.Context, storeMode string, cfg *config.Config) (storeBundle, error) {
	switch strings.ToLower(strings.TrimSpace(storeMode)) {
	case "postgres":
		dsn := store.DSNFromConfig(cfg.Database)
		pg, err := store.NewPostgresStore(dsn)
		if err != nil {
			return storeBundle{}, err
		}
		if err := pg.Ping(ctx); err != nil {
			return storeBundle{}, err
		}
		return storeBundle{
			Postgres:          pg,
			SQLDB:             pg.DB,
			EventStore:        pg,
			SessionRegistry:   sessions.NewPostgresRegistry(pg.DB, cfg.LLM.Session.LockTTL),
			ConversationStore: pg,
			ManagerStore:      pg,
			ScheduleStore:     pg,
			TurnStore:         pg,
		}, nil
	default:
		return storeBundle{}, fmt.Errorf("store mode %q is unsupported; postgres is required", strings.TrimSpace(storeMode))
	}
}

func initializeStateStores(ctx context.Context, stores storeBundle, bundle *runtimecontracts.WorkflowContractBundle) (string, error) {
	if stores.Postgres == nil || bundle == nil {
		return "store wiring ready", nil
	}
	platformPlans, err := store.GeneratePlatformTableDDLs(bundle.Platform)
	if err != nil {
		return "", fmt.Errorf("platform-owned tables: %w", err)
	}
	entityPlans, err := store.GenerateEntityTableDDLs(bundle.WorkflowEntitySchema())
	if err != nil {
		return "", fmt.Errorf("entity_schema tables: %w", err)
	}
	statePlans, err := store.GenerateNodeStateTableDDLs(bundle.NodeEntries())
	if err != nil {
		return "", fmt.Errorf("state_schema tables: %w", err)
	}
	plans := append(platformPlans, entityPlans...)
	plans = append(plans, statePlans...)
	if err := stores.Postgres.EnsureSchemaTables(ctx, plans); err != nil {
		return "", err
	}
	tableNames := make([]string, 0, len(plans))
	totalColumns := 0
	for _, plan := range plans {
		tableNames = append(tableNames, fmt.Sprintf("%s(%d)", strings.TrimSpace(plan.TableName), plan.ColumnCount))
		totalColumns += plan.ColumnCount
	}
	sort.Strings(tableNames)
	slog.Info("swarm boot state stores", "tables", len(plans), "columns", totalColumns, "detail", strings.Join(tableNames, ", "))
	if len(tableNames) == 0 {
		return "verified 0 generated tables", nil
	}
	return fmt.Sprintf("verified %d generated tables (%s)", len(plans), strings.Join(tableNames, ", ")), nil
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
	}, bundle, nil
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

func logBootSkeleton(source semanticview.Source, contractsRoot, platformSpecPath string, report runtimebootverify.Report, stateStoreSummary string) {
	warningCounts := make(map[string]int, len(report.Findings))
	for _, finding := range report.Warnings() {
		warningCounts[strings.TrimSpace(finding.CheckID)]++
	}
	steps := []struct {
		index int
		name  string
		note  string
	}{
		{1, "load_platform_spec", fmt.Sprintf("loaded %s", filepath.Clean(platformSpecPath))},
		{2, "walk_flow_tree", fmt.Sprintf("discovered %d flow(s)", len(source.FlowSchemaEntries()))},
		{3, "construct_paths", "constructed hierarchical contract paths from package tree"},
		{4, "register_templates", fmt.Sprintf("registered %d template flow(s)", templateFlowCount(source))},
		{5, "build_registries", fmt.Sprintf("nodes=%d agents=%d events=%d tools=%d", len(source.NodeEntries()), len(source.AgentEntries()), len(source.ResolvedEventCatalog()), len(source.ToolEntries()))},
		{6, "resolve_subscriptions", "subscription resolution skeleton in place; full validation lands in CP4"},
		{7, "validate_pins", fmt.Sprintf("validated flow pins and event wiring; warnings=%d", warningCounts["event_chain_integrity"]+warningCounts["event_consumer_exists"]+warningCounts["event_producer_exists"])},
		{8, "validate_required_agents", "validated required_agents coverage and state-machine targets"},
		{9, "validate_tools", fmt.Sprintf("validated tools and prompt coverage; warnings=%d", warningCounts["prompt_exists"]+warningCounts["tool_resolution"])},
		{10, "validate_permissions", "validated platform permission requirements during boot verification"},
		{11, "validate_platform_version", fmt.Sprintf("loaded platform spec version %s for workflow %s", strings.TrimSpace(source.PlatformSpec().Platform.Version), strings.TrimSpace(source.WorkflowVersion()))},
		{12, "initialize_state_stores", fmt.Sprintf("%s (contracts=%s)", strings.TrimSpace(stateStoreSummary), contractsRoot)},
		{13, "start_system_nodes", "delegated to runtime.Start()"},
		{14, "start_agents", "delegated to runtime.Start()"},
		{15, "ready", "health server transitions to ready after runtime start"},
	}
	for _, step := range steps {
		slog.Info("swarm boot", "step", fmt.Sprintf("%02d", step.index), "name", step.name, "detail", step.note)
	}
	for _, finding := range report.Warnings() {
		slog.Warn("swarm boot validation warning", "check_id", finding.CheckID, "location", finding.Location, "detail", finding.Message)
	}
}

func templateFlowCount(source semanticview.Source) int {
	count := 0
	for _, flow := range source.FlowSchemaEntries() {
		if strings.EqualFold(strings.TrimSpace(flow.Mode), "template") {
			count++
		}
	}
	return count
}

func newHealthServer(addr string, ready *atomic.Bool, dashboardOpts dashboardserver.Options) *http.Server {
	mux := http.NewServeMux()
	dashboardHandler := dashboardserver.NewHandler(dashboardOpts)
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
	mux.Handle("/api/", dashboardHandler)
	mux.Handle("/api", dashboardHandler)
	return &http.Server{
		Addr:              strings.TrimSpace(addr),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func serveHealth(server *http.Server) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("health server stopped: %v", err)
	}
}

func shutdownHealthServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("health server shutdown failed: %v", err)
	}
}

func logReadySummary(source semanticview.Source, contractsRoot, healthAddr string) {
	log.Printf(
		"swarm runtime ready contracts=%s flows=%d nodes=%d agents=%d events=%d health=%s",
		contractsRoot,
		len(source.FlowSchemaEntries()),
		len(source.NodeEntries()),
		len(source.AgentEntries()),
		len(source.ResolvedEventCatalog()),
		healthAddr,
	)
}

func dashboardServerOptions(supervisor *runtimeProjectSupervisor, stores storeBundle, ready *atomic.Bool, rotateAfterTurns int, credentialStore runtimecredentials.Store) dashboardserver.Options {
	var (
		agents        dashboardserver.AgentReader
		mailbox       dashboardserver.MailboxReader
		conversations dashboardserver.ConversationReader
		observability dashboardserver.ObservabilityReader
		agentControl  dashboardserver.AgentController
		runtimeCtl    dashboardserver.RuntimeController
	)
	if stores.Postgres != nil {
		agents = dashboardserver.NewSQLAgentReader(stores.Postgres.DB, stores.Postgres, rotateAfterTurns)
		mailbox = stores.Postgres
		conversations = dashboardserver.NewSQLConversationReader(stores.Postgres.DB)
		observability = dashboardserver.NewSQLObservabilityReader(stores.Postgres.DB)
	}
	if supervisor != nil {
		agentControl = dashboardDynamicAgentControl{supervisor: supervisor}
		runtimeCtl = dashboardDynamicRuntimeControl{supervisor: supervisor}
	}
	healthFn := func(ctx context.Context) (map[string]any, error) {
		source := semanticview.Source(nil)
		if supervisor != nil {
			source = supervisor.CurrentSource()
		}
		checks := map[string]any{
			"runtime": map[string]any{
				"ready": ready != nil && ready.Load(),
			},
		}
		if source != nil {
			checks["runtime"] = map[string]any{
				"ready":  ready != nil && ready.Load(),
				"flows":  len(source.FlowSchemaEntries()),
				"nodes":  len(source.NodeEntries()),
				"agents": len(source.AgentEntries()),
				"events": len(source.ResolvedEventCatalog()),
			}
		}
		if stores.Postgres != nil {
			dbErr := stores.Postgres.Ping(ctx)
			checks["database"] = map[string]any{
				"ok": dbErr == nil,
			}
			if dbErr != nil {
				checks["database_error"] = dbErr.Error()
			}
		}
		return checks, nil
	}
	var sourceProvider builderpkg.SourceProvider
	var runtimeProvider builderpkg.RuntimeProvider
	var projectControl builderpkg.ProjectController
	if supervisor != nil {
		sourceProvider = supervisor.CurrentSource
		runtimeProvider = supervisor.CurrentRuntime
		projectControl = supervisor
	}
	builderHandler := builderpkg.NewHandler(builderpkg.Options{
		Health:         healthFn,
		Instances:      runtimepipeline.NewWorkflowInstanceStore(stores.SQLDB),
		Runtime:        runtimeCtl,
		Credentials:    credentialStore,
		Version:        "swarm-dev",
		CurrentSource:  sourceProvider,
		CurrentRuntime: runtimeProvider,
		ProjectControl: projectControl,
	})
	return dashboardserver.Options{
		Health:        healthFn,
		Agents:        agents,
		AgentControl:  agentControl,
		Mailbox:       mailbox,
		Instances:     runtimepipeline.NewWorkflowInstanceStore(stores.SQLDB),
		Conversations: conversations,
		Observability: observability,
		Runtime:       runtimeCtl,
		Version:       "swarm-dev",
		Builder:       builderHandler,
	}
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

func configuredWorkspaceLifecycle(db *sql.DB, repoRoot, contractsRoot string, source semanticview.Source) *workspace.DockerManager {
	manager := workspace.NewDockerManager(db)
	cfg := workspace.DefaultDockerConfig()
	if dataDir := strings.TrimSpace(filepath.Join(repoRoot, "data")); dataDir != "" {
		cfg.SharedDataSource = dataDir
	}
	if contractsDir := strings.TrimSpace(contractsRoot); contractsDir != "" {
		cfg.ContractsSource = contractsDir
	}
	manager.SetConfig(cfg)
	manager.SetSemanticSource(source)
	return manager
}

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("resolve cwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			log.Fatalf("locate repo root from %s", dir)
		}
		dir = parent
	}
}

func closeDB(db *sql.DB) {
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		log.Printf("close db: %v", err)
	}
}
