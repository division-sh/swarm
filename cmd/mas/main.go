package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"empireai/internal/config"
	"empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimellm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
	"empireai/internal/runtime/sessions"
	"empireai/internal/store"
	"gopkg.in/yaml.v3"
)

const (
	defaultConfigPath       = "configs/mas.yaml"
	defaultContractsPath    = "docs/specs/mas-platform/tests/generic-runtime/contracts"
	defaultPlatformSpecPath = "docs/specs/mas-platform/platform/contracts/platform-spec.yaml"
	defaultHealthAddr       = ":8081"
)

type runtimeConfigFile struct {
	Runtime  config.RuntimeConfig  `yaml:"runtime"`
	Database config.DatabaseConfig `yaml:"database"`
	LLM      config.LLMConfig      `yaml:"llm"`
}

type storeBundle struct {
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
	configPath := flag.String("config", defaultConfigPath, "Path to MAS runtime config")
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
	source := semanticview.Wrap(bundle)
	stores, err := buildStores(ctx, *storeMode, cfg)
	if err != nil {
		log.Fatalf("init stores: %v", err)
	}
	defer closeDB(stores.SQLDB)

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
	healthServer := newHealthServer(*healthAddr, &ready)
	go serveHealth(healthServer)
	defer shutdownHealthServer(healthServer)

	logBootSkeleton(source, contractsRoot, resolvedPlatformSpecPath)
	if err := rt.Start(ctx); err != nil {
		log.Fatalf("start runtime: %v", err)
	}
	ready.Store(true)
	logReadySummary(source, contractsRoot, *healthAddr)

	<-ctx.Done()
	ready.Store(false)
}

func loadRuntimeConfig(path string) (*config.Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var parsed runtimeConfigFile
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg := &config.Config{
		Runtime:  parsed.Runtime,
		Database: parsed.Database,
		LLM:      parsed.LLM,
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
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

type masWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	nodes          []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
	policies       interface {
		runtimepipeline.WorkflowModule
		ScanPolicy() runtimepipeline.ScanPolicy
		DiscoveryPolicy() runtimepipeline.DiscoveryPolicy
		ScoringPolicy() runtimepipeline.ScoringPolicy
		PayloadFactory() runtimepipeline.PayloadFactory
	}
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
	policies := runtimepipeline.NewGenericTestWorkflowModule()
	return &masWorkflowModule{
		bundle:         bundle,
		source:         source,
		workflow:       workflow,
		nodes:          nodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
		policies: policies.(interface {
			runtimepipeline.WorkflowModule
			ScanPolicy() runtimepipeline.ScanPolicy
			DiscoveryPolicy() runtimepipeline.DiscoveryPolicy
			ScoringPolicy() runtimepipeline.ScoringPolicy
			PayloadFactory() runtimepipeline.PayloadFactory
		}),
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
func (m *masWorkflowModule) ScanPolicy() runtimepipeline.ScanPolicy         { return m.policies.ScanPolicy() }
func (m *masWorkflowModule) DiscoveryPolicy() runtimepipeline.DiscoveryPolicy {
	return m.policies.DiscoveryPolicy()
}
func (m *masWorkflowModule) ScoringPolicy() runtimepipeline.ScoringPolicy {
	return m.policies.ScoringPolicy()
}
func (m *masWorkflowModule) PayloadFactory() runtimepipeline.PayloadFactory {
	return m.policies.PayloadFactory()
}

func logBootSkeleton(source semanticview.Source, contractsRoot, platformSpecPath string) {
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
		{7, "validate_pins", "placeholder only; boot validation deferred to CP4"},
		{8, "validate_required_agents", "placeholder only; boot validation deferred to CP4"},
		{9, "validate_tools", "placeholder only; boot validation deferred to CP4"},
		{10, "validate_permissions", "placeholder only; boot validation deferred to CP4"},
		{11, "validate_platform_version", "placeholder only; boot validation deferred to CP4"},
		{12, "initialize_state_stores", fmt.Sprintf("store wiring ready (contracts=%s)", contractsRoot)},
		{13, "start_system_nodes", "delegated to runtime.Start()"},
		{14, "start_agents", "delegated to runtime.Start()"},
		{15, "ready", "health server transitions to ready after runtime start"},
	}
	for _, step := range steps {
		log.Printf("mas boot step=%02d name=%s detail=%s", step.index, step.name, step.note)
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

func newHealthServer(addr string, ready *atomic.Bool) *http.Server {
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
