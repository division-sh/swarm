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

	"empireai/internal/config"
	dashboardserver "empireai/internal/dashboard/server"
	"empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimellm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
)

const (
	defaultContractsPath    = "docs/specs/mas-platform/tests/test-guard-pass"
	defaultPlatformSpecPath = "docs/specs/mas-platform/platform/contracts/platform-spec.yaml"
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
	configPath := flag.String("config", "", "Optional path to MAS runtime config")
	contractsPath := flag.String("contracts", defaultContractsPath, "Path to MAS contract bundle root")
	platformSpecPath := flag.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := flag.String("store", "inmemory", "Store mode: inmemory|postgres")
	healthAddr := flag.String("health-addr", defaultHealthAddr, "HTTP bind address for health checks")
	selfCheck := flag.Bool("self-check", true, "Run runtime self-check during boot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolvedConfigPath := resolvePath(repo, *configPath)
	resolvedContractsPath := resolvePath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)

	cfg, err := loadRuntimeConfig(resolvedConfigPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		log.Fatalf("resolve contracts: %v", err)
	}
	module, bundle, err := newMASWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath)
	if err != nil {
		log.Fatalf("load MAS contracts: %v", err)
	}
	if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
		slog.Error("validate prompt schema guards", "error", err)
		os.Exit(1)
	}
	source := semanticview.Wrap(bundle)
	bootWarnings, err := runtimepipeline.ValidateWorkflowContractsDetailed(source)
	if err != nil {
		slog.Error("validate MAS contracts", "error", err)
		os.Exit(1)
	}
	agentCount, permissionErrors := runtimetools.ValidateAgentPermissions(source)
	if len(permissionErrors) > 0 {
		slog.Error("validate agent permissions", "count", len(permissionErrors), "error", permissionErrors[0])
		for _, validationErr := range permissionErrors[1:] {
			slog.Error("validate agent permissions", "error", validationErr)
		}
		os.Exit(1)
	}
	permissionSummary := fmt.Sprintf("validated %d agents, all permission requirements satisfied", agentCount)
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

	rt, err := runtime.NewRuntime(ctx, cfg, stores.runtimeStores(), runtime.RuntimeOptions{
		SelfCheck:      *selfCheck,
		WorkflowModule: module,
	})
	if err != nil {
		log.Fatalf("init runtime: %v", err)
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			log.Printf("runtime shutdown failed: %v", err)
		}
	}()

	var ready atomic.Bool
	healthServer := newHealthServer(*healthAddr, &ready, dashboardServerOptions(source, stores, &ready, cfg.LLM.Session.RotateAfterTurns))
	go serveHealth(healthServer)
	defer shutdownHealthServer(healthServer)

	logBootSkeleton(source, contractsRoot, resolvedPlatformSpecPath, bootWarnings, permissionSummary, stateStoreSummary)
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
	mode := strings.TrimSpace(os.Getenv("MAS_LLM_RUNTIME_MODE"))
	if mode == "" {
		if strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")) != "" && strings.TrimSpace(os.Getenv("MAS_CLAUDE_DEFAULT_MODEL")) != "" {
			mode = "api"
		} else {
			mode = "cli_test"
		}
	}
	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			MaxConcurrentAgents: envInt("MAS_RUNTIME_MAX_CONCURRENT_AGENTS", 10),
			EventPollInterval:   envDuration("MAS_RUNTIME_EVENT_POLL_INTERVAL", time.Second),
			RecoveryOnStartup:   envBool("MAS_RUNTIME_RECOVERY_ON_STARTUP", false),
		},
		Database: config.DatabaseConfig{
			Host:     envOrDefault("MAS_DB_HOST", envOrDefault("PGHOST", "127.0.0.1")),
			Port:     envInt("MAS_DB_PORT", envInt("PGPORT", 5432)),
			Name:     envOrDefault("MAS_DB_NAME", envOrDefault("PGDATABASE", "empireai")),
			User:     envOrDefault("MAS_DB_USER", envOrDefault("PGUSER", "postgres")),
			Password: envOrDefault("MAS_DB_PASSWORD", envOrDefault("PGPASSWORD", "postgres")),
			SSLMode:  envOrDefault("MAS_DB_SSLMODE", "disable"),
			PoolSize: envInt("MAS_DB_POOL_SIZE", 5),
		},
		LLM: config.LLMConfig{
			RuntimeMode: mode,
			Session: config.LLMSessionConfig{
				LockTTL:               envDuration("MAS_LLM_SESSION_LOCK_TTL", 10*time.Second),
				RotateAfterTurns:      envInt("MAS_LLM_SESSION_ROTATE_AFTER_TURNS", 40),
				RotateOnParseFailures: envInt("MAS_LLM_SESSION_ROTATE_ON_PARSE_FAILURES", 3),
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: envOrDefault("MAS_CLAUDE_DEFAULT_MODEL", ""),
				HaikuModel:   envOrDefault("MAS_CLAUDE_HAIKU_MODEL", ""),
				MaxRetries:   envInt("MAS_CLAUDE_API_MAX_RETRIES", 1),
				RetryBackoff: envDuration("MAS_CLAUDE_API_RETRY_BACKOFF", 2*time.Second),
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              envOrDefault("MAS_CLAUDE_CLI_COMMAND", "claude"),
				Timeout:              envDuration("MAS_CLAUDE_CLI_TIMEOUT", 15*time.Minute),
				OutputFormat:         envOrDefault("MAS_CLAUDE_CLI_OUTPUT_FORMAT", "stream-json"),
				Retries:              envInt("MAS_CLAUDE_CLI_RETRIES", 1),
				NoSessionPersistence: envBool("MAS_CLAUDE_CLI_NO_SESSION_PERSISTENCE", false),
				UseTMux:              envBool("MAS_CLAUDE_CLI_USE_TMUX", false),
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
		return storeBundle{
			EventStore:      runtimebus.InMemoryEventStore{},
			SessionRegistry: sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL),
		}, nil
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
	slog.Info("mas boot state stores", "tables", len(plans), "columns", totalColumns, "detail", strings.Join(tableNames, ", "))
	if len(tableNames) == 0 {
		return "verified 0 generated tables", nil
	}
	return fmt.Sprintf("verified %d generated tables (%s)", len(plans), strings.Join(tableNames, ", ")), nil
}

type masWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func newMASWorkflowModule(repoRoot, contractsRoot, platformSpecPath string) (runtimepipeline.WorkflowModule, *runtimecontracts.WorkflowContractBundle, error) {
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
	return &masWorkflowModule{
		bundle:         bundle,
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, bundle, nil
}

func (m *masWorkflowModule) SemanticSource() semanticview.Source { return m.source }
func (m *masWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}
func (m *masWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	return append([]runtimepipeline.WorkflowNode(nil), m.nodes...)
}
func (m *masWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry   { return m.guardRegistry }
func (m *masWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry { return m.actionRegistry }

func logBootSkeleton(source semanticview.Source, contractsRoot, platformSpecPath string, warnings []runtimepipeline.WorkflowContractWarning, permissionSummary, stateStoreSummary string) {
	warningCounts := make(map[string]int, len(warnings))
	for _, warning := range warnings {
		warningCounts[strings.TrimSpace(warning.Category)]++
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
		{7, "validate_pins", fmt.Sprintf("validated flow pins and event wiring; warnings=%d", warningCounts["EVENT-NO-SCHEMA"]+warningCounts["EVENT-NO-CONSUMER"]+warningCounts["EVENT-NO-PRODUCER"])},
		{8, "validate_required_agents", "validated required_agents coverage and state-machine targets"},
		{9, "validate_tools", fmt.Sprintf("validated tools and prompt coverage; warnings=%d", warningCounts["PROMPT-MISSING"]+warningCounts["PROMPT-STUB"])},
		{10, "validate_permissions", permissionSummary},
		{11, "validate_platform_version", fmt.Sprintf("loaded platform spec version %s for workflow %s", strings.TrimSpace(source.PlatformSpec().Platform.Version), strings.TrimSpace(source.WorkflowVersion()))},
		{12, "initialize_state_stores", fmt.Sprintf("%s (contracts=%s)", strings.TrimSpace(stateStoreSummary), contractsRoot)},
		{13, "start_system_nodes", "delegated to runtime.Start()"},
		{14, "start_agents", "delegated to runtime.Start()"},
		{15, "ready", "health server transitions to ready after runtime start"},
	}
	for _, step := range steps {
		slog.Info("mas boot", "step", fmt.Sprintf("%02d", step.index), "name", step.name, "detail", step.note)
	}
	for _, warning := range warnings {
		slog.Warn("mas boot validation warning", "category", warning.Category, "detail", warning.Message)
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
	mux.Handle("/api/", dashboardserver.NewHandler(dashboardOpts))
	mux.Handle("/api", dashboardserver.NewHandler(dashboardOpts))
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
		"mas runtime ready contracts=%s flows=%d nodes=%d agents=%d events=%d health=%s",
		contractsRoot,
		len(source.FlowSchemaEntries()),
		len(source.NodeEntries()),
		len(source.AgentEntries()),
		len(source.ResolvedEventCatalog()),
		healthAddr,
	)
}

func dashboardServerOptions(source semanticview.Source, stores storeBundle, ready *atomic.Bool, rotateAfterTurns int) dashboardserver.Options {
	var (
		agents        dashboardserver.AgentReader
		mailbox       dashboardserver.MailboxReader
		conversations dashboardserver.ConversationReader
	)
	if stores.Postgres != nil {
		agents = dashboardserver.NewSQLAgentReader(stores.Postgres.DB, stores.Postgres, rotateAfterTurns)
		mailbox = stores.Postgres
		conversations = dashboardserver.NewSQLConversationReader(stores.Postgres.DB)
	}
	return dashboardserver.Options{
		Health: func(ctx context.Context) (map[string]any, error) {
			checks := map[string]any{
				"runtime": map[string]any{
					"ready":  ready != nil && ready.Load(),
					"flows":  len(source.FlowSchemaEntries()),
					"nodes":  len(source.NodeEntries()),
					"agents": len(source.AgentEntries()),
					"events": len(source.ResolvedEventCatalog()),
				},
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
		},
		Agents:        agents,
		Mailbox:       mailbox,
		Instances:     runtimepipeline.NewWorkflowInstanceStore(stores.SQLDB),
		Conversations: conversations,
	}
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
