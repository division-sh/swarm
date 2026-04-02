package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
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
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
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
	if len(os.Args) > 1 && strings.TrimSpace(os.Args[1]) == "verify" {
		os.Exit(runVerifyCommand(context.Background(), repo, os.Args[2:], os.Stdout))
	}
	configPath := flag.String("config", "", "Optional path to Swarm runtime config")
	contractsPath := flag.String("contracts", "", "Path to Swarm contract bundle root")
	platformSpecPath := flag.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	storeMode := flag.String("store", "postgres", "Store mode: postgres")
	healthAddr := flag.String("health-addr", defaultHealthAddr, "HTTP bind address for health checks")
	selfCheck := flag.Bool("self-check", true, "Run runtime self-check during boot")
	traceID := flag.String("trace-id", "", "Print a lifecycle trace for the given trace ID and exit")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	resolvedConfigPath := resolvePath(repo, *configPath)
	resolvedContractsPath := resolveContractsPath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)
	if err := loadRepoDotEnv(repo); err != nil {
		log.Fatalf("load .env: %v", err)
	}

	cfg, err := loadRuntimeConfig(resolvedConfigPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	stores, err := buildStores(ctx, *storeMode, cfg)
	if err != nil {
		log.Fatalf("init stores: %v", err)
	}
	defer closeDB(stores.SQLDB)
	if strings.TrimSpace(*traceID) != "" {
		if stores.Postgres == nil {
			log.Fatal("trace reporting requires postgres store")
		}
		report, err := stores.Postgres.TraceReport(ctx, *traceID)
		if err != nil {
			log.Fatalf("trace report: %v", err)
		}
		printTraceReport(os.Stdout, report)
		return
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
	if err := workspaces.EnsurePrereqs(ctx); err != nil {
		slog.Error("prepare workspaces", "error", err)
		os.Exit(1)
	}
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		slog.Error("ensure system workspaces", "error", err)
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
		EnableToolGateway:  true,
		ToolGatewayToken:   strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")),
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
	healthServer := newHealthServer(*healthAddr, &ready, rt.ToolGateway, dashboardServerOptions(supervisor, stores, &ready, cfg.LLM.Session.RotateAfterTurns, credentialStore))
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

func runVerifyCommand(ctx context.Context, repo string, args []string, out io.Writer) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	contractsPath := fs.String("contracts", "", "Path to Swarm contract bundle root")
	platformSpecPath := fs.String("platform-spec", defaultPlatformSpecPath, "Path to platform spec yaml")
	if err := fs.Parse(args); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: %v\n", err)
		}
		return 2
	}
	if err := loadRepoDotEnv(repo); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: load .env: %v\n", err)
		}
		return 1
	}
	resolvedContractsPath := resolveContractsPath(repo, *contractsPath)
	resolvedPlatformSpecPath := resolvePath(repo, *platformSpecPath)
	contractsRoot, err := normalizeContractsRoot(resolvedContractsPath)
	if err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: resolve contracts: %v\n", err)
		}
		return 1
	}
	if _, bundle, err := newSwarmWorkflowModule(repo, contractsRoot, resolvedPlatformSpecPath); err != nil {
		if out != nil {
			fmt.Fprintf(out, "verify failed: load Swarm contracts: %v\n", err)
		}
		return 1
	} else {
		if err := verifyBundle(ctx, semanticview.Wrap(bundle)); err != nil {
			if out != nil {
				fmt.Fprintf(out, "verify failed: %v\n", err)
			}
			return 1
		}
		if out != nil {
			fmt.Fprintf(out, "verify ok: contracts=%s\n", contractsRoot)
		}
	}
	return 0
}

func verifyBundle(ctx context.Context, source semanticview.Source) error {
	if source == nil {
		return errors.New("semantic source is required")
	}
	if bundle, ok := semanticview.Bundle(source); ok {
		if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
			return fmt.Errorf("validate prompt schema guards: %w", err)
		}
	}
	credentialStore, err := buildCredentialStore()
	if err != nil {
		return fmt.Errorf("configure credentials: %w", err)
	}
	report := runtimebootverify.Run(ctx, source, runtimebootverify.Options{
		Credentials:       credentialStore,
		CheckMCPReachable: true,
	})
	if report.HasErrors() {
		lines := make([]string, 0, len(report.Errors()))
		for _, finding := range report.Errors() {
			lines = append(lines, fmt.Sprintf("%s [%s] %s", strings.TrimSpace(finding.CheckID), strings.TrimSpace(finding.Location), strings.TrimSpace(finding.Message)))
		}
		return fmt.Errorf("boot verification failed:\n%s", strings.Join(lines, "\n"))
	}
	return verifyEmitSchemaCoverage(source)
}

func verifyEmitSchemaCoverage(source semanticview.Source) error {
	if source == nil {
		return errors.New("semantic source is required")
	}
	runtimetools.InitEventSchemaRegistry(source)
	if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 && envBool("SWARM_EMIT_SCHEMA_STRICT", true) {
		sample := generated
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries (sample: %s)", len(generated), strings.Join(sample, ", "))
	}
	return nil
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

func printTraceReport(w io.Writer, report store.TraceReport) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "Trace %s\n", strings.TrimSpace(report.TraceID))
	fmt.Fprintf(w, "Events: %d  Deliveries: %d  Receipts: %d  DeadLetters: %d\n\n", len(report.Events), len(report.Deliveries), len(report.Receipts), len(report.DeadLetters))
	if summary := traceFirstStuckSummary(report); summary != "" {
		fmt.Fprintf(w, "Summary: %s\n\n", summary)
	}

	deliveriesByEvent := make(map[string][]store.TraceDelivery, len(report.Deliveries))
	for _, delivery := range report.Deliveries {
		deliveriesByEvent[delivery.EventID] = append(deliveriesByEvent[delivery.EventID], delivery)
	}
	receiptsByEvent := make(map[string][]store.TraceReceipt, len(report.Receipts))
	for _, receipt := range report.Receipts {
		receiptsByEvent[receipt.EventID] = append(receiptsByEvent[receipt.EventID], receipt)
	}
	deadLettersByEvent := make(map[string][]store.TraceDeadLetter, len(report.DeadLetters))
	for _, deadLetter := range report.DeadLetters {
		deadLettersByEvent[deadLetter.OriginalEventID] = append(deadLettersByEvent[deadLetter.OriginalEventID], deadLetter)
	}
	turnsByEvent := make(map[string][]store.TraceTurn, len(report.Turns))
	turnsWithoutEvent := make([]store.TraceTurn, 0)
	for _, turn := range report.Turns {
		if strings.TrimSpace(turn.TriggerEventID) == "" {
			turnsWithoutEvent = append(turnsWithoutEvent, turn)
			continue
		}
		turnsByEvent[turn.TriggerEventID] = append(turnsByEvent[turn.TriggerEventID], turn)
	}

	for _, evt := range report.Events {
		fmt.Fprintf(w, "%s  %s  id=%s", evt.CreatedAt.Format(time.RFC3339), evt.EventName, evt.EventID)
		if evt.SourceEventID != "" {
			fmt.Fprintf(w, " parent=%s", evt.SourceEventID)
		}
		if evt.ProducedBy != "" {
			fmt.Fprintf(w, " by=%s", evt.ProducedBy)
		}
		if evt.FlowInstance != "" {
			fmt.Fprintf(w, " flow=%s", evt.FlowInstance)
		}
		if evt.EntityID != "" {
			fmt.Fprintf(w, " entity=%s", evt.EntityID)
		}
		fmt.Fprintln(w)

		for _, delivery := range deliveriesByEvent[evt.EventID] {
			fmt.Fprintf(w, "  delivery  %s/%s  status=%s", delivery.SubscriberType, delivery.SubscriberID, delivery.Status)
			if delivery.ReasonCode != "" {
				fmt.Fprintf(w, " reason=%s", delivery.ReasonCode)
			}
			if delivery.ActiveSessionID != "" {
				fmt.Fprintf(w, " session=%s", delivery.ActiveSessionID)
			}
			if delivery.StartedAt.Valid {
				fmt.Fprintf(w, " started=%s", delivery.StartedAt.Time.Format(time.RFC3339))
			}
			if delivery.RetryCount > 0 {
				fmt.Fprintf(w, " retries=%d", delivery.RetryCount)
			}
			if delivery.LastError != "" {
				fmt.Fprintf(w, " error=%q", delivery.LastError)
			}
			fmt.Fprintln(w)
		}
		for _, receipt := range receiptsByEvent[evt.EventID] {
			fmt.Fprintf(w, "  receipt   %s/%s  outcome=%s", receipt.SubscriberType, receipt.SubscriberID, receipt.Outcome)
			if receipt.ReasonCode != "" {
				fmt.Fprintf(w, " reason=%s", receipt.ReasonCode)
			}
			if errText := strings.TrimSpace(asString(receipt.SideEffects["error"])); errText != "" {
				fmt.Fprintf(w, " error=%q", errText)
			}
			fmt.Fprintln(w)
		}
		for _, deadLetter := range deadLettersByEvent[evt.EventID] {
			fmt.Fprintf(w, "  dead      type=%s retries=%d depth=%d", deadLetter.FailureType, deadLetter.RetryCount, deadLetter.ChainDepth)
			if deadLetter.HandlerNode != "" {
				fmt.Fprintf(w, " handler=%s", deadLetter.HandlerNode)
			}
			if deadLetter.ErrorMessage != "" {
				fmt.Fprintf(w, " error=%q", deadLetter.ErrorMessage)
			}
			fmt.Fprintln(w)
		}
		for _, turn := range turnsByEvent[evt.EventID] {
			printTraceTurn(w, turn)
		}
		fmt.Fprintln(w)
	}
	for _, turn := range turnsWithoutEvent {
		printTraceTurn(w, turn)
		fmt.Fprintln(w)
	}
}

func printTraceTurn(w io.Writer, turn store.TraceTurn) {
	fmt.Fprintf(w, "  turn      agent=%s session=%s mode=%s parse_ok=%t", turn.AgentID, turn.SessionID, turn.RuntimeMode, turn.ParseOK)
	if turn.ScopeKey != "" {
		fmt.Fprintf(w, " scope=%s", turn.ScopeKey)
	}
	if turn.LatencyMS > 0 {
		fmt.Fprintf(w, " latency_ms=%d", turn.LatencyMS)
	}
	if turn.RetryCount > 0 {
		fmt.Fprintf(w, " retries=%d", turn.RetryCount)
	}
	if turn.Error != "" {
		fmt.Fprintf(w, " error=%q", turn.Error)
	}
	fmt.Fprintln(w)
	if len(turn.AvailableTools) > 0 {
		fmt.Fprintf(w, "    tools     available=%s\n", strings.Join(turn.AvailableTools, ","))
	}
	if len(turn.ToolCalls) > 0 {
		fmt.Fprintf(w, "    tools     called=%s\n", strings.Join(turn.ToolCalls, ","))
	}
	if len(turn.EmittedEvents) > 0 {
		fmt.Fprintf(w, "    events    emitted=%s\n", strings.Join(turn.EmittedEvents, ","))
	}
	if len(turn.MCPServers) > 0 || len(turn.MCPToolsListed) > 0 || len(turn.MCPToolsVisible) > 0 {
		if len(turn.MCPServers) > 0 {
			serverStates := make([]string, 0, len(turn.MCPServers))
			for name, status := range turn.MCPServers {
				serverStates = append(serverStates, name+":"+status)
			}
			sort.Strings(serverStates)
			fmt.Fprintf(w, "    mcp       servers=%s\n", strings.Join(serverStates, ","))
		}
		if len(turn.MCPToolsListed) > 0 {
			fmt.Fprintf(w, "    mcp       listed=%s\n", strings.Join(turn.MCPToolsListed, ","))
		}
		if len(turn.MCPToolsVisible) > 0 {
			fmt.Fprintf(w, "    mcp       visible=%s\n", strings.Join(turn.MCPToolsVisible, ","))
		}
	}
}

func traceFirstStuckSummary(report store.TraceReport) string {
	deliveriesByEvent := make(map[string][]store.TraceDelivery, len(report.Deliveries))
	for _, delivery := range report.Deliveries {
		deliveriesByEvent[delivery.EventID] = append(deliveriesByEvent[delivery.EventID], delivery)
	}
	receiptsByEvent := make(map[string][]store.TraceReceipt, len(report.Receipts))
	for _, receipt := range report.Receipts {
		receiptsByEvent[receipt.EventID] = append(receiptsByEvent[receipt.EventID], receipt)
	}
	deadLettersByEvent := make(map[string][]store.TraceDeadLetter, len(report.DeadLetters))
	for _, deadLetter := range report.DeadLetters {
		deadLettersByEvent[deadLetter.OriginalEventID] = append(deadLettersByEvent[deadLetter.OriginalEventID], deadLetter)
	}

	for _, evt := range report.Events {
		deliveries := deliveriesByEvent[evt.EventID]
		receipts := receiptsByEvent[evt.EventID]
		if len(deliveries) == 0 && len(receipts) == 0 && len(deadLettersByEvent[evt.EventID]) == 0 {
			return fmt.Sprintf("unrouted event %s id=%s", evt.EventName, evt.EventID)
		}
		receiptKeys := map[string]struct{}{}
		for _, receipt := range receipts {
			receiptKeys[receipt.SubscriberType+"/"+receipt.SubscriberID] = struct{}{}
			switch strings.TrimSpace(strings.ToLower(receipt.Outcome)) {
			case "dead_letter", "discard", "reject", "kill", "escalate":
				if receipt.ReasonCode != "" {
					return fmt.Sprintf("receipt %s/%s outcome=%s reason=%s for %s", receipt.SubscriberType, receipt.SubscriberID, receipt.Outcome, receipt.ReasonCode, evt.EventName)
				}
				return fmt.Sprintf("receipt %s/%s outcome=%s for %s", receipt.SubscriberType, receipt.SubscriberID, receipt.Outcome, evt.EventName)
			}
		}
		for _, delivery := range deliveries {
			key := delivery.SubscriberType + "/" + delivery.SubscriberID
			if _, ok := receiptKeys[key]; !ok && strings.TrimSpace(strings.ToLower(delivery.Status)) == "in_progress" {
				if delivery.ActiveSessionID != "" {
					return fmt.Sprintf("in-progress delivery %s session=%s for %s", key, delivery.ActiveSessionID, evt.EventName)
				}
				if delivery.ReasonCode != "" {
					return fmt.Sprintf("in-progress delivery %s reason=%s for %s", key, delivery.ReasonCode, evt.EventName)
				}
				return fmt.Sprintf("in-progress delivery %s for %s", key, evt.EventName)
			}
			if _, ok := receiptKeys[key]; !ok && strings.TrimSpace(strings.ToLower(delivery.Status)) == "pending" {
				if delivery.ReasonCode != "" {
					return fmt.Sprintf("pending delivery %s reason=%s for %s", key, delivery.ReasonCode, evt.EventName)
				}
				return fmt.Sprintf("pending delivery %s for %s", key, evt.EventName)
			}
			switch strings.TrimSpace(strings.ToLower(delivery.Status)) {
			case "failed", "dead_letter":
				if delivery.ReasonCode != "" {
					return fmt.Sprintf("delivery %s status=%s reason=%s for %s", key, delivery.Status, delivery.ReasonCode, evt.EventName)
				}
				return fmt.Sprintf("delivery %s status=%s for %s", key, delivery.Status, evt.EventName)
			}
		}
	}
	return ""
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

func newHealthServer(addr string, ready *atomic.Bool, toolGateway *runtimemcp.Gateway, dashboardOpts dashboardserver.Options) *http.Server {
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
	if toolGateway != nil {
		gatewayHandler := toolGateway.Handler()
		mux.Handle("/mcp", gatewayHandler)
		mux.Handle("/tools/", gatewayHandler)
	}
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
