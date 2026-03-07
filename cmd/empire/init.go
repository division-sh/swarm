package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/specaudit"
	"empireai/internal/templateops"
	"github.com/google/uuid"
)

type initOptions struct {
	ConfigPath        string
	StoreMode         string
	MigrationFile     string
	SelfCheck         bool
	AgentsDir         string
	TemplateAgentsDir string
	TemplateRoutesYML string
	TemplateVersion   string
}

func parseInitOptions(args []string) (initOptions, error) {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode (postgres only)")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path (managed migrations applied)")
	selfCheck := fs.Bool("self-check", true, "Run bootstrap self check after init")
	agentsDir := fs.String("agents-dir", "configs/agents", "Global agents YAML directory (holding + factory)")
	templateAgentsDir := fs.String("template-agents-dir", "configs/agents/templates", "Template agents YAML directory (OpCo)")
	templateRoutesYAML := fs.String("template-routes-yaml", "configs/agents/templates/routes.yaml", "Routing template YAML file")
	templateVersion := fs.String("template-version", "2.0.50", "Initial org template version to publish if none exist")
	if err := fs.Parse(args); err != nil {
		return initOptions{}, err
	}
	return initOptions{
		ConfigPath:        strings.TrimSpace(*cfgPath),
		StoreMode:         strings.TrimSpace(*storeMode),
		MigrationFile:     strings.TrimSpace(*migrationFile),
		SelfCheck:         *selfCheck,
		AgentsDir:         strings.TrimSpace(*agentsDir),
		TemplateAgentsDir: strings.TrimSpace(*templateAgentsDir),
		TemplateRoutesYML: strings.TrimSpace(*templateRoutesYAML),
		TemplateVersion:   strings.TrimSpace(*templateVersion),
	}, nil
}

func runInitSubcommand(args []string) error {
	opts, err := parseInitOptions(args)
	if err != nil {
		return err
	}
	if opts.StoreMode != "postgres" {
		return fmt.Errorf("init requires --store postgres")
	}

	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stores := buildStores(ctx, "postgres", cfg, true, opts.MigrationFile)
	if stores.SQLDB == nil || stores.ManagerStore == nil {
		return fmt.Errorf("postgres store unavailable")
	}

	if err := ensureInitialTemplateCLI(ctx, stores.SQLDB, stores.MailboxStore, opts.TemplateVersion, opts.TemplateAgentsDir, opts.TemplateRoutesYML); err != nil {
		return err
	}
	if err := seedGlobalAgentsFromYAML(ctx, stores.ManagerStore, opts.AgentsDir); err != nil {
		return err
	}
	if err := persistRuntimeConfig(ctx, stores.SQLDB, opts.ConfigPath); err != nil {
		return err
	}

	log.Printf("init complete: migrations applied, global agents seeded, template ensured")
	return runRuntime(ctx, cfg, stores, opts.SelfCheck)
}

func ensureInitialTemplateCLI(ctx context.Context, db *sql.DB, mailbox runtimetools.MailboxPersistence, version, agentsDir, routesYAML string) error {
	if db == nil {
		return fmt.Errorf("db unavailable")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM org_templates)`).Scan(&exists); err != nil {
		return fmt.Errorf("check org_templates: %w", err)
	}
	if exists {
		return nil
	}

	agents, bootstrap, seeded, err := templateops.CompileTemplateFromYAML(strings.TrimSpace(agentsDir), strings.TrimSpace(routesYAML))
	if err != nil {
		return err
	}
	env := mustJSON(map[string]any{
		"version":          strings.TrimSpace(version),
		"agents":           json.RawMessage(agents),
		"bootstrap_routes": json.RawMessage(bootstrap),
		"seeded_routes":    json.RawMessage(seeded),
	})
	if res := specaudit.Validate("template", env); !res.Passed {
		return fmt.Errorf("initial template failed spec audit issues=%d", len(res.Issues))
	}
	svc := templateops.NewService(db, mailbox)
	if err := svc.PublishTemplate(ctx, strings.TrimSpace(version), agents, bootstrap, seeded, "init", "initial template (init)"); err != nil {
		return err
	}
	return nil
}

type globalAgentSyncStore interface {
	runtimemanager.AgentPersistence
}

func seedGlobalAgentsFromYAML(ctx context.Context, store globalAgentSyncStore, agentsDir string) error {
	agents, err := templateops.LoadGlobalAgentsFromYAML(strings.TrimSpace(agentsDir))
	if err != nil {
		return err
	}
	desired := make(map[string]struct{}, len(agents))
	for _, cfg := range agents {
		if id := strings.TrimSpace(cfg.ID); id != "" {
			desired[id] = struct{}{}
		}
	}
	existingByID := make(map[string]runtimemanager.PersistedAgent, len(agents))
	existingAll := make([]runtimemanager.PersistedAgent, 0, len(agents))
	if existing, loadErr := store.LoadAgents(ctx); loadErr == nil {
		existingAll = existing
		for _, rec := range existing {
			id := strings.TrimSpace(rec.Config.ID)
			if id == "" {
				continue
			}
			existingByID[id] = rec
		}
	}
	for _, cfg := range agents {
		if cfg.ID == "" {
			continue
		}
		rec := runtimemanager.PersistedAgent{
			Config:    cfg,
			Status:    "active",
			HiredBy:   "runtime-sync",
			StartedAt: time.Now(),
		}
		if prev, ok := existingByID[cfg.ID]; ok {
			if strings.TrimSpace(prev.Status) != "" {
				rec.Status = prev.Status
			}
			if strings.TrimSpace(prev.HiredBy) != "" {
				rec.HiredBy = prev.HiredBy
			}
			if strings.TrimSpace(prev.TemplateVersion) != "" {
				rec.TemplateVersion = prev.TemplateVersion
			}
			if !prev.StartedAt.IsZero() {
				rec.StartedAt = prev.StartedAt
			}
		}
		if err := store.UpsertAgent(ctx, rec); err != nil {
			return err
		}
	}
	for _, rec := range existingAll {
		id := strings.TrimSpace(rec.Config.ID)
		if id == "" {
			continue
		}
		if _, ok := desired[id]; ok {
			continue
		}
		// Keep OpCo-scoped agents untouched; this sync is for holding/factory roster only.
		if strings.TrimSpace(rec.Config.VerticalID) != "" {
			continue
		}
		if err := store.MarkAgentTerminated(ctx, id); err != nil {
			return fmt.Errorf("terminate stale global agent %s: %w", id, err)
		}
	}
	return nil
}

func persistRuntimeConfig(ctx context.Context, db *sql.DB, configPath string) error {
	if db == nil {
		return nil
	}
	configPath = strings.TrimSpace(configPath)
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read config for runtime_config: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runtime_config (config_yaml, config_path, applied_at, created_at)
		VALUES ($1, $2, now(), now())
	`, string(raw), configPath); err != nil {
		return fmt.Errorf("insert runtime_config: %w", err)
	}
	return nil
}

func emitSystemStarted(ctx context.Context, stores storeBundle, bus *runtime.EventBus) error {
	if stores.SQLDB == nil || bus == nil {
		return nil
	}
	agentCount := 0
	verticalCount := 0
	geoCount := 0
	previousStarts := 0
	_ = stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agents WHERE COALESCE(status,'') <> 'terminated'`).Scan(&agentCount)
	_ = stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals`).Scan(&verticalCount)
	_ = stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM geographies`).Scan(&geoCount)
	_ = stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type = 'system.started'`).Scan(&previousStarts)

	templateVersion := ""
	_ = stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(version,'')
		FROM org_templates
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&templateVersion)

	payload := map[string]any{
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"agent_count":      agentCount,
		"template_version": strings.TrimSpace(templateVersion),
		"is_cold_start":    previousStarts == 0 && verticalCount == 0 && geoCount == 0,
		"startup_count":    previousStarts + 1,
	}
	b := mustJSON(payload)
	return bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.started"),
		SourceAgent: "runtime",
		Payload:     b,
		CreatedAt:   time.Now(),
	})
}

// Optional: allow init to bring up the dashboard/inbound gateway even before any directives.
// Kept here to avoid sprinkling init-specific imports in main.go.
func initHTTPServer(addr string, handler http.Handler, name string) {
	if strings.TrimSpace(addr) == "" || handler == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("%s panic: %v", name, r)
			}
		}()
		if err := http.ListenAndServe(addr, handler); err != nil {
			log.Printf("%s stopped: %v", name, err)
		}
	}()
}
