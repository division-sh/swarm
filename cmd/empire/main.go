package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"empireai/internal/config"
	"empireai/internal/dashboard"
	"empireai/internal/events"
	"empireai/internal/factory"
	"empireai/internal/mailbox"
	"empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	llm "empireai/internal/runtime/llm"
	runtimellm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	workspace "empireai/internal/runtime/workspace"
	"empireai/internal/store"
	"github.com/google/uuid"
)

const defaultMigrationFilePath = "contracts/ddl-canonical.sql"

func main() {
	if handled, err := tryRunSubcommand(); handled {
		if err != nil {
			log.Fatalf("command failed: %v", err)
		}
		return
	}

	cfgPath := flag.String("config", "configs/empire.yaml", "Path to empire config")
	selfCheck := flag.Bool("self-check", true, "Run bootstrap self check")
	storeMode := flag.String("store", "inmemory", "Event/session storage mode: inmemory|postgres")
	applyMigrations := flag.Bool("migrate", false, "Apply SQL migrations on startup (postgres mode)")
	migrationFile := flag.String("migration-file", defaultMigrationFilePath, "Migration file path")
	mailboxStatus := flag.Bool("mailbox-status", false, "Print mailbox pending/critical counts and exit")
	mailboxList := flag.Bool("mailbox-list", false, "List pending mailbox items and exit")
	mailboxListCritical := flag.Bool("mailbox-list-critical", false, "With -mailbox-list, only show critical pending items")
	mailboxListReviews := flag.Bool("mailbox-list-reviews", false, "With -mailbox-list, only show founder review gate items")
	mailboxLimit := flag.Int("mailbox-limit", 20, "Mailbox list limit")
	mailboxViewID := flag.String("mailbox-view-id", "", "Mailbox item ID to view and exit")
	mailboxDecideID := flag.String("mailbox-decide-id", "", "Mailbox item ID to decide and exit")
	mailboxDecision := flag.String("mailbox-decision", "", "Mailbox decision action (approve|reject|more-data|kill|revise|skip)")
	mailboxNotes := flag.String("mailbox-notes", "", "Mailbox decision notes")
	digestGenerate := flag.Bool("digest", false, "Generate portfolio digest and exit")
	digestTopN := flag.Int("digest-top", 10, "Top verticals to include in digest")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stores := buildStores(ctx, *storeMode, cfg, *applyMigrations, *migrationFile)
	if hasOperatorAction(
		*mailboxStatus,
		*mailboxList,
		*mailboxListCritical,
		*mailboxListReviews,
		strings.TrimSpace(*mailboxViewID) != "",
		strings.TrimSpace(*mailboxDecideID) != "",
		*digestGenerate,
	) {
		if err := runOperatorActions(ctx, stores, operatorOptions{
			mailboxStatus:       *mailboxStatus,
			mailboxList:         *mailboxList,
			mailboxListCritical: *mailboxListCritical,
			mailboxListReviews:  *mailboxListReviews,
			mailboxLimit:        *mailboxLimit,
			mailboxViewID:       strings.TrimSpace(*mailboxViewID),
			mailboxDecideID:     strings.TrimSpace(*mailboxDecideID),
			mailboxDecision:     strings.TrimSpace(*mailboxDecision),
			mailboxNotes:        *mailboxNotes,
			digestGenerate:      *digestGenerate,
			digestTopN:          *digestTopN,
		}); err != nil {
			log.Fatalf("operator command failed: %v", err)
		}
		return
	}

	if err := runRuntime(ctx, cfg, stores, *selfCheck); err != nil {
		log.Fatalf("runtime failed: %v", err)
	}
}

func runRuntime(ctx context.Context, cfg *config.Config, stores storeBundle, selfCheck bool) error {
	workspaceLifecycle := buildWorkspaceLifecycle(ctx, stores.SQLDB)
	toolGatewayAddr := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_ADDR"))
	if toolGatewayAddr == "" && workspaceLifecycle != nil {
		toolGatewayAddr = ":8090"
	}
	rt, err := runtime.NewRuntime(ctx, cfg, stores.toRuntimeStores(), runtime.RuntimeOptions{
		SelfCheck:          selfCheck,
		WorkspaceLifecycle: workspaceLifecycle,
		EnableToolGateway:  toolGatewayAddr != "",
		ToolGatewayToken:   strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN")),
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := rt.Shutdown(); err != nil {
			log.Printf("runtime shutdown failed: %v", err)
		}
	}()
	bus := rt.Bus
	if stores.SQLDB != nil && envBool("EMPIREAI_ENABLE_DETERMINISTIC_SCAN_RUNNER", false) {
		runner := factory.NewScanRequestedRunner(stores.SQLDB, stores.EventStore, stores.MailboxStore, bus)
		go runner.Run(ctx)
	}
	inboundAddr := os.Getenv("EMPIREAI_INBOUND_ADDR")
	if inboundAddr != "" {
		gateway := rt.InboundGateway
		goServeHTTP("inbound gateway", func() error { return http.ListenAndServe(inboundAddr, gateway.Handler()) })
		if stores.InboundStore != nil {
			go inboundCleanupLoop(ctx, stores.InboundStore)
		}
	}
	if stores.MailboxStore != nil {
		go mailboxTimeoutLoop(ctx, stores.MailboxStore)
		if notifier := buildCriticalNotifierFromEnv(); notifier != nil {
			go mailboxCriticalNotifyLoop(ctx, stores.MailboxStore, notifier, bus)
		}
	}
	if stores.SQLDB != nil {
		go humanTaskExpiryLoop(ctx, stores.SQLDB, cfg, bus)
		go marginalMaintenanceLoop(ctx, stores.SQLDB, bus)
	}
	if err := syncRuntimeGlobalAgents(ctx, stores.ManagerStore); err != nil {
		log.Printf("global agents sync failed (continuing): %v", err)
	}
	if err := rotateGlobalAgentSessions(ctx, stores.ManagerStore, stores.SessionRegistry, cfg.LLM.RuntimeMode); err != nil {
		log.Printf("global session rotate after sync failed (continuing): %v", err)
	}
	if rt.Budget != nil {
		go budgetHeartbeatLoop(ctx, rt.Budget)
	}

	// Spec v2.0: bidirectional Telegram bot for human tasks (Phase 1 uses long polling).
	startTelegramHumanTaskBot(ctx, stores, cfg, bus)
	if toolGatewayAddr != "" && rt.ToolGateway != nil {
		goServeHTTP("tool gateway", func() error { return http.ListenAndServe(toolGatewayAddr, rt.ToolGateway.Handler()) })
	}
	dashboardAddr := strings.TrimSpace(os.Getenv("EMPIREAI_DASHBOARD_ADDR"))
	if dashboardAddr != "" && stores.SQLDB != nil {
		dashboardServer := dashboard.NewServer(stores.SQLDB, cfg, stores.EventStore, stores.MailboxStore, rt.Manager)
		goServeHTTP("dashboard server", func() error { return http.ListenAndServe(dashboardAddr, dashboardServer.Handler()) })
	}
	if err := rt.Start(ctx); err != nil {
		return err
	}

	// Deterministic holding-side managers (spec v2.0): digest compilation + health monitoring.
	if stores.DigestStore != nil && stores.MailboxStore != nil {
		go portfolioDigestLoop(ctx, bus, stores.DigestStore, stores.MailboxStore)
	}
	if stores.SQLDB != nil && stores.MailboxStore != nil {
		go verticalHealthMonitorLoop(ctx, bus, stores.SQLDB, stores.MailboxStore)
	}

	// Emit system.started after agents are subscribed so the coordinator receives it immediately.
	if stores.SQLDB != nil {
		if err := emitSystemStarted(ctx, stores, bus); err != nil {
			log.Printf("emit system.started failed: %v", err)
		}
	}

	fmt.Println("empire runtime bootstrap ready")
	rt.Wait(ctx)
	return nil
}

func goServeHTTP(name string, serve func() error) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("%s panic: %v", name, r)
			}
		}()
		if err := serve(); err != nil {
			log.Printf("%s stopped: %v", name, err)
		}
	}()
}

func tryRunSubcommand() (bool, error) {
	if len(os.Args) < 2 {
		return false, nil
	}
	switch strings.TrimSpace(os.Args[1]) {
	case "init":
		return true, runInitSubcommand(os.Args[2:])
	case "mailbox":
		return true, runMailboxSubcommand(os.Args[2:])
	case "tasks":
		return true, runTasksSubcommand(os.Args[2:])
	case "digest":
		return true, runDigestSubcommand(os.Args[2:])
	case "status":
		return true, runStatusSubcommand(os.Args[2:])
	case "budget":
		return true, runBudgetSubcommand(os.Args[2:])
	case "agents":
		return true, runAgentsSubcommand(os.Args[2:])
	case "verticals":
		return true, runVerticalsSubcommand(os.Args[2:])
	case "vertical":
		return true, runVerticalSubcommand(os.Args[2:])
	case "deployments":
		return true, runDeploymentsSubcommand(os.Args[2:])
	case "secrets":
		return true, runSecretsSubcommand(os.Args[2:])
	case "config":
		return true, runConfigSubcommand(os.Args[2:])
	case "scan":
		return true, runScanSubcommand(os.Args[2:])
	case "factory":
		return true, runFactorySubcommand(os.Args[2:])
	case "spec-audit":
		return true, runSpecAuditSubcommand(os.Args[2:])
	case "template":
		return true, runTemplateSubcommand(os.Args[2:])
	case "ops":
		return true, runOpsSubcommand(os.Args[2:])
	case "pipeline":
		return true, runPipelineSubcommand(os.Args[2:])
	case "directive":
		return true, runDirectiveSubcommand(os.Args[2:])
	case "chat":
		return true, runChatSubcommand(os.Args[2:])
	case "monitor":
		return true, runMonitorSubcommand(os.Args[2:])
	case "agent":
		return true, runAgentSubcommand(os.Args[2:])
	default:
		return false, nil
	}
}

func truncateString(v string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(v) <= max {
		return v
	}
	if max <= 3 {
		return v[:max]
	}
	return v[:max-3] + "..."
}

func readOptionalJSONFile(path string, fallback []byte) ([]byte, error) {
	if strings.TrimSpace(path) == "" {
		return fallback, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read json file %s: %w", path, err)
	}
	return b, nil
}

type templateAgentDiffRow struct {
	Role string `json:"role"`
}

type templateRouteDiffRow struct {
	EventPattern string `json:"event_pattern"`
	SubscriberID string `json:"subscriber_id"`
}

func renderTemplateDiff(ctx context.Context, db *sql.DB, fromVersion, toVersion string) error {
	if db == nil {
		return fmt.Errorf("template diff requires postgres db")
	}
	fromAgents, fromBootstrap, fromSeeded, err := loadTemplateEnvelope(ctx, db, fromVersion)
	if err != nil {
		return err
	}
	toAgents, toBootstrap, toSeeded, err := loadTemplateEnvelope(ctx, db, toVersion)
	if err != nil {
		return err
	}

	fromAgentMap := templateAgentRoleMap(fromAgents)
	toAgentMap := templateAgentRoleMap(toAgents)
	addedAgents, removedAgents, changedAgents := diffTemplateMaps(fromAgentMap, toAgentMap)

	fromBootstrapMap := templateRouteKeyMap(fromBootstrap)
	toBootstrapMap := templateRouteKeyMap(toBootstrap)
	addedBootstrap, removedBootstrap, _ := diffTemplateMaps(fromBootstrapMap, toBootstrapMap)

	fromSeededMap := templateRouteKeyMap(fromSeeded)
	toSeededMap := templateRouteKeyMap(toSeeded)
	addedSeeded, removedSeeded, _ := diffTemplateMaps(fromSeededMap, toSeededMap)

	fmt.Printf("template diff from=%s to=%s\n", fromVersion, toVersion)
	fmt.Printf("agents: +%d -%d ~%d\n", len(addedAgents), len(removedAgents), len(changedAgents))
	if len(addedAgents) > 0 {
		fmt.Printf("  added: %s\n", strings.Join(addedAgents, ", "))
	}
	if len(removedAgents) > 0 {
		fmt.Printf("  removed: %s\n", strings.Join(removedAgents, ", "))
	}
	if len(changedAgents) > 0 {
		fmt.Printf("  changed: %s\n", strings.Join(changedAgents, ", "))
	}
	fmt.Printf("routes.bootstrap: +%d -%d\n", len(addedBootstrap), len(removedBootstrap))
	fmt.Printf("routes.seeded: +%d -%d\n", len(addedSeeded), len(removedSeeded))
	return nil
}

func loadTemplateEnvelope(ctx context.Context, db *sql.DB, version string) ([]byte, []byte, []byte, error) {
	var agentsRaw, bootstrapRaw, seededRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT agents, bootstrap_routes, seeded_routes
		FROM org_templates
		WHERE version = $1
	`, strings.TrimSpace(version)).Scan(&agentsRaw, &bootstrapRaw, &seededRaw); err != nil {
		return nil, nil, nil, fmt.Errorf("load template %s: %w", strings.TrimSpace(version), err)
	}
	return agentsRaw, bootstrapRaw, seededRaw, nil
}

func templateAgentRoleMap(raw []byte) map[string]string {
	out := map[string]string{}
	var rows []templateAgentDiffRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return out
	}
	for _, r := range rows {
		role := strings.TrimSpace(r.Role)
		if role == "" {
			continue
		}
		out[role] = canonicalJSONRole(raw, role)
	}
	return out
}

func canonicalJSONRole(raw []byte, role string) string {
	role = strings.TrimSpace(role)
	if role == "" || !json.Valid(raw) {
		return ""
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return ""
	}
	for _, r := range rows {
		if strings.TrimSpace(asString(r["role"])) != role {
			continue
		}
		b, _ := json.Marshal(r)
		return string(b)
	}
	return ""
}

func templateRouteKeyMap(raw []byte) map[string]string {
	out := map[string]string{}
	var rows []templateRouteDiffRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return out
	}
	for _, r := range rows {
		pattern := strings.TrimSpace(r.EventPattern)
		sub := strings.TrimSpace(r.SubscriberID)
		if pattern == "" || sub == "" {
			continue
		}
		k := pattern + " -> " + sub
		out[k] = k
	}
	return out
}

func diffTemplateMaps(fromMap, toMap map[string]string) (added, removed, changed []string) {
	added = make([]string, 0)
	removed = make([]string, 0)
	changed = make([]string, 0)
	for k, toVal := range toMap {
		fromVal, exists := fromMap[k]
		if !exists {
			added = append(added, k)
			continue
		}
		if fromVal != toVal {
			changed = append(changed, k)
		}
	}
	for k := range fromMap {
		if _, exists := toMap[k]; !exists {
			removed = append(removed, k)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(changed)
	return added, removed, changed
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func nullable(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func dispatchSystemDirectiveViaDashboard(ctx context.Context, target targetAgent, message string) (string, bool, error) {
	endpoint := strings.TrimSpace(os.Getenv("EMPIREAI_DIRECTIVE_ENDPOINT"))
	if endpoint == "" {
		endpoint = "http://localhost:8070/dashboard/api/control/directive"
	}
	if !strings.HasPrefix(strings.ToLower(endpoint), "http://") && !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
		endpoint = "http://" + strings.TrimPrefix(endpoint, "//")
	}
	apiKey := strings.TrimSpace(os.Getenv("EMPIREAI_API_KEY"))
	if apiKey == "" {
		apiKey = "local-dev-key"
	}

	reqBody, _ := json.Marshal(map[string]any{
		"agent_id": strings.TrimSpace(target.ID),
		"message":  strings.TrimSpace(message),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(reqBody)))
	if err != nil {
		return "", true, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Empire-Key", apiKey)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", true, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		return "", true, fmt.Errorf("dashboard directive endpoint status %d: %s", resp.StatusCode, msg)
	}
	var out struct {
		EventID string `json:"event_id"`
		OK      bool   `json:"ok"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", true, fmt.Errorf("decode dashboard directive response: %w", err)
	}
	if strings.TrimSpace(out.EventID) == "" {
		return "", true, fmt.Errorf("dashboard directive response missing event_id")
	}
	return strings.TrimSpace(out.EventID), true, nil
}

type storeBundle struct {
	SQLDB             *sql.DB
	EventStore        runtimebus.EventStore
	SessionRegistry   sessions.Registry
	ConversationStore runtimellm.ConversationPersistence
	ManagerStore      runtimemanager.ManagerPersistence
	ScheduleStore     runtimepipeline.SchedulePersistence
	MailboxStore      runtimetools.MailboxPersistence
	InboundStore      runtime.InboundPersistence
	DigestStore       runtime.DigestPersistence
	TurnStore         runtimellm.TurnPersistence
	ScanCampaignStore runtimepipeline.ScanCampaignPersistence
}

func (s storeBundle) toRuntimeStores() runtime.Stores {
	return runtime.Stores{
		SQLDB:             s.SQLDB,
		EventStore:        s.EventStore,
		SessionRegistry:   s.SessionRegistry,
		ConversationStore: s.ConversationStore,
		ManagerStore:      s.ManagerStore,
		ScheduleStore:     s.ScheduleStore,
		MailboxStore:      s.MailboxStore,
		InboundStore:      s.InboundStore,
		DigestStore:       s.DigestStore,
		TurnStore:         s.TurnStore,
		ScanCampaignStore: s.ScanCampaignStore,
	}
}

func buildStores(
	ctx context.Context,
	storeMode string,
	cfg *config.Config,
	applyMigrations bool,
	migrationFile string,
) storeBundle {
	switch storeMode {
	case "postgres":
		dsn := store.DSNFromConfig(cfg.Database)
		pg, err := store.NewPostgresStore(dsn)
		if err != nil {
			log.Fatalf("postgres init failed: %v", err)
		}
		configurePostgresPool(pg.DB, cfg.Database.PoolSize)
		if err := pg.Ping(ctx); err != nil {
			log.Fatalf("postgres ping failed: %v", err)
		}
		if applyMigrations {
			specs, err := discoverManagedMigrationSpecs(migrationFile)
			if err != nil {
				log.Fatalf("discover migrations failed: %v", err)
			}
			if err := applyManagedMigrations(ctx, pg, specs); err != nil {
				log.Fatalf("migration failed: %v", err)
			}
		}
		sr := sessions.NewPostgresRegistry(pg.DB, cfg.LLM.Session.LockTTL)
		return storeBundle{
			SQLDB:             pg.DB,
			EventStore:        pg,
			SessionRegistry:   sr,
			ConversationStore: pg,
			ManagerStore:      pg,
			ScheduleStore:     pg,
			MailboxStore:      pg,
			InboundStore:      pg,
			DigestStore:       pg,
			TurnStore:         pg,
			ScanCampaignStore: pg,
		}
	case "inmemory":
		fallthrough
	default:
		return storeBundle{
			EventStore:      runtimebus.InMemoryEventStore{},
			SessionRegistry: sessions.NewInMemoryRegistry(cfg.LLM.Session.LockTTL),
		}
	}
}

func configurePostgresPool(db *sql.DB, configuredSize int) {
	if db == nil {
		return
	}
	maxOpen := configuredSize
	if maxOpen <= 0 {
		maxOpen = 24
	}
	if maxOpen < 4 {
		maxOpen = 4
	}
	maxIdle := maxOpen / 2
	if maxIdle < 2 {
		maxIdle = 2
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(45 * time.Minute)
}

type migrationSpec struct {
	Version int
	Name    string
	Path    string
}

var migrationFilePattern = regexp.MustCompile(`^(\d{3})_(.+)\.sql$`)

func discoverManagedMigrationSpecs(migrationFile string) ([]migrationSpec, error) {
	root := strings.TrimSpace(migrationFile)
	if root == "" {
		return nil, fmt.Errorf("migration file path is required")
	}
	root = filepath.Clean(root)
	base := filepath.Base(root)
	if match := migrationFilePattern.FindStringSubmatch(base); len(match) != 3 {
		if _, err := os.Stat(root); err != nil {
			return nil, fmt.Errorf("stat migration file %s: %w", root, err)
		}
		name := strings.TrimSuffix(base, filepath.Ext(base))
		if strings.TrimSpace(name) == "" {
			name = "migration"
		}
		return []migrationSpec{{
			Version: 1,
			Name:    name,
			Path:    root,
		}}, nil
	}

	dir := filepath.Dir(root)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations directory %s: %w", dir, err)
	}
	specs := make([]migrationSpec, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		match := migrationFilePattern.FindStringSubmatch(name)
		if len(match) != 3 {
			continue
		}
		version, convErr := strconv.Atoi(match[1])
		if convErr != nil {
			return nil, fmt.Errorf("parse migration version for %s: %w", name, convErr)
		}
		specs = append(specs, migrationSpec{
			Version: version,
			Name:    strings.TrimSuffix(name, ".sql"),
			Path:    filepath.Join(dir, name),
		})
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("no migrations discovered in %s", dir)
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Version < specs[j].Version
	})
	return specs, nil
}

func applyManagedMigrations(ctx context.Context, pg *store.PostgresStore, migrations []migrationSpec) error {
	specs := make([]store.MigrationSpec, 0, len(migrations))
	for _, m := range migrations {
		specs = append(specs, store.MigrationSpec{
			Version: m.Version,
			Name:    m.Name,
			Path:    m.Path,
		})
	}
	return pg.ApplyManagedMigrations(ctx, specs)
}

func buildWorkspaceLifecycle(ctx context.Context, db *sql.DB) workspace.Lifecycle {
	if db == nil {
		return nil
	}
	if !envBool("EMPIREAI_ENABLE_DOCKER_WORKSPACES", true) {
		return nil
	}
	workspaces := workspace.NewDockerManager(db)
	if err := workspaces.EnsureSystemWorkspaces(ctx); err != nil {
		if envBool("EMPIREAI_REQUIRE_DOCKER_WORKSPACES", false) {
			log.Fatalf("workspace bootstrap failed: %v", err)
		}
		log.Printf("workspace bootstrap warning (falling back to host execution): %v", err)
		return nil
	}
	return workspaces
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func appendTargetedEvent(ctx context.Context, stores storeBundle, evt events.Event, recipients []string) error {
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if len(evt.Payload) == 0 {
		evt.Payload = []byte("{}")
	}
	if err := stores.EventStore.AppendEvent(ctx, evt); err != nil {
		return err
	}
	if len(recipients) > 0 {
		recipients = filterExistingRecipients(ctx, stores.SQLDB, recipients)
		if len(recipients) == 0 {
			return nil
		}
		if err := stores.EventStore.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
			return err
		}
	}
	return nil
}

func filterExistingRecipients(ctx context.Context, db *sql.DB, recipients []string) []string {
	if db == nil || len(recipients) == 0 {
		return recipients
	}
	exists := make(map[string]struct{}, len(recipients))
	rows, err := db.QueryContext(ctx, `SELECT id FROM agents`)
	if err != nil {
		return recipients
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			exists[id] = struct{}{}
		}
	}
	filtered := make([]string, 0, len(recipients))
	for _, id := range recipients {
		if _, ok := exists[id]; ok {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func mailboxTimeoutLoop(ctx context.Context, store runtimetools.MailboxPersistence) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	expire := func() {
		expired, err := store.ExpireMailboxItems(ctx, 200)
		if err != nil {
			log.Printf("mailbox timeout transition failed: %v", err)
			return
		}
		if len(expired) > 0 {
			log.Printf("mailbox timeout transition applied count=%d", len(expired))
		}
	}
	expire()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expire()
		}
	}
}

func mailboxCriticalNotifyLoop(ctx context.Context, store runtimetools.MailboxPersistence, notifier mailbox.CriticalNotifier, bus *runtime.EventBus) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	dispatch := func() {
		items, err := store.ListUnnotifiedCriticalMailboxItems(ctx, 50)
		if err != nil {
			log.Printf("critical mailbox fetch failed: %v", err)
			return
		}
		for _, item := range items {
			sendCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
			err := notifier.NotifyCritical(sendCtx, item)
			cancel()
			if err != nil {
				log.Printf("critical mailbox notify failed id=%s err=%v", item.ID, err)
				continue
			}
			if err := store.MarkMailboxItemNotified(ctx, item.ID); err != nil {
				log.Printf("mark mailbox notified failed id=%s err=%v", item.ID, err)
				continue
			}
			log.Printf("critical mailbox notified id=%s type=%s vertical=%s", item.ID, item.Type, item.VerticalID)

			// Spec v2.0 digest trigger: critical mailbox items prompt an immediate digest compilation/push.
			if bus != nil {
				payload := mustJSON(map[string]any{
					"mailbox_id":  item.ID,
					"type":        item.Type,
					"vertical_id": item.VerticalID,
					"from_agent":  item.FromAgent,
					"summary":     item.Summary,
					"notified_at": time.Now().UTC().Format(time.RFC3339),
				})
				publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := bus.Publish(publishCtx, events.Event{
					ID:          uuid.NewString(),
					Type:        events.EventType("mailbox.critical_notified"),
					SourceAgent: "mailbox-notifier",
					Payload:     payload,
					CreatedAt:   time.Now(),
				}); err != nil {
					log.Printf("mailbox.critical_notified publish failed mailbox=%s err=%v", item.ID, err)
				}
				cancel()
			}
		}
	}

	dispatch()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dispatch()
		}
	}
}

func inboundCleanupLoop(ctx context.Context, store runtime.InboundPersistence) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	cleanup := func() {
		cutoff := inboundRetentionCutoff(time.Now())
		total := 0
		for {
			n, err := store.PurgeInboundEventsBefore(ctx, cutoff, 1000)
			if err != nil {
				log.Printf("inbound cleanup failed: %v", err)
				return
			}
			total += n
			if n < 1000 {
				break
			}
		}
		if total > 0 {
			log.Printf("inbound cleanup purged rows=%d cutoff=%s", total, cutoff.UTC().Format(time.RFC3339))
		}
	}

	cleanup()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanup()
		}
	}
}

func inboundRetentionCutoff(now time.Time) time.Time {
	return now.Add(-7 * 24 * time.Hour)
}

func buildCriticalNotifierFromEnv() mailbox.CriticalNotifier {
	var notifiers []mailbox.CriticalNotifier

	if webhookURL := strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_WEBHOOK_URL")); webhookURL != "" {
		notifiers = append(notifiers, &mailbox.WebhookNotifier{URL: webhookURL})
	}

	tgToken := telegramBotTokenFromEnv()
	tgChat := telegramChatIDFromEnv()
	if tgToken != "" && tgChat != "" {
		notifiers = append(notifiers, &mailbox.TelegramNotifier{
			BotToken: tgToken,
			ChatID:   tgChat,
			BaseURL:  telegramBaseURLFromEnv(),
		})
	}

	smtpAddr := strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_SMTP_ADDR"))
	smtpFrom := strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_EMAIL_FROM"))
	smtpToRaw := strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_EMAIL_TO"))
	if smtpAddr != "" && smtpFrom != "" && smtpToRaw != "" {
		recipients := splitCSV(smtpToRaw)
		if len(recipients) > 0 {
			timeout := 10 * time.Second
			if raw := strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_SMTP_TIMEOUT")); raw != "" {
				if d, err := time.ParseDuration(raw); err == nil && d > 0 {
					timeout = d
				}
			}
			notifiers = append(notifiers, &mailbox.EmailNotifier{
				SMTPAddr: smtpAddr,
				Username: strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_SMTP_USERNAME")),
				Password: strings.TrimSpace(os.Getenv("EMPIREAI_NOTIFY_SMTP_PASSWORD")),
				From:     smtpFrom,
				To:       recipients,
				Timeout:  timeout,
			})
		}
	}

	return mailbox.NewMultiCriticalNotifier(notifiers...)
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func runSelfCheck(modelRuntime llm.Runtime, bus *runtime.EventBus) error {
	ctx := context.Background()

	// Minimal event path check.
	t := events.EventType("runtime.boot")
	ch := bus.Subscribe("bootstrap-self-check", t)
	payload := mustJSON(map[string]string{"status": "ok"})
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        t,
		SourceAgent: "bootstrap",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(ctx, evt); err != nil {
		return err
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		return fmt.Errorf("eventbus publish/subscribe timeout")
	}

	// Runtime wiring check intentionally avoids provider calls.
	// Session start requires a real agent record in postgres-backed mode.
	_ = modelRuntime
	return nil
}
