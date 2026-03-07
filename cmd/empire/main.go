package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
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
	"empireai/internal/digest"
	"empireai/internal/events"
	"empireai/internal/factory"
	"empireai/internal/mailbox"
	"empireai/internal/models"
	"empireai/internal/ops"
	"empireai/internal/runtime"
	runtimeagents "empireai/internal/runtime/agents"
	runtimebus "empireai/internal/runtime/bus"
	llm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimemcp "empireai/internal/runtime/mcp"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	workspace "empireai/internal/runtime/workspace"
	"empireai/internal/specaudit"
	"empireai/internal/store"
	"empireai/internal/templateops"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
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
	bus := runtime.NewEventBus(stores.EventStore)
	var runtimeLogger *runtime.RuntimeLogger
	if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
		if envBool("EMPIREAI_EMIT_SCHEMA_STRICT", true) {
			return fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries", len(generated))
		}
		sample := generated
		if len(sample) > 10 {
			sample = sample[:10]
		}
		log.Printf("emit schema hardening warning: %d agent-emitted event schemas are missing explicit definitions; add explicit schemas (sample: %s)", len(generated), strings.Join(sample, ", "))
	}
	var pipelineCoordinator *runtimepipeline.FactoryPipelineCoordinator
	var scoringNode *runtimepipeline.ScoringNode
	if stores.SQLDB != nil {
		runtimeLogger = runtime.NewRuntimeLogger(stores.SQLDB)
		bus.SetRuntimeLogger(runtimeLogger)
		bus.SetCycleTracker(runtimebus.NewOpCoCycleTracker(stores.SQLDB))
		pipelineCoordinator = runtimepipeline.NewFactoryPipelineCoordinator(bus, stores.SQLDB)
		if pipelineCoordinator != nil {
			pipelineCoordinator.SetShardPlanner(runtimepipeline.NewShardPlanner(cfg.Sharding))
			bus.SetInterceptors(pipelineCoordinator)
			go pipelineCoordinator.RunMaintenance(ctx)
			scoringNode = runtimepipeline.NewScoringNode(bus, pipelineCoordinator, stores.SQLDB)
			if scoringNode != nil {
				go scoringNode.Run(ctx)
			}
		}
	}
	if stores.SQLDB != nil && envBool("EMPIREAI_ENABLE_DETERMINISTIC_SCAN_RUNNER", false) {
		runner := factory.NewScanRequestedRunner(stores.SQLDB, stores.EventStore, stores.MailboxStore, bus)
		go runner.Run(ctx)
	}
	if stores.ScanCampaignStore != nil {
		hooks := runtimepipeline.ScanCampaignHooks{
			Warnf: func(component, format string, args ...any) {
				log.Printf("runtime.warn component=%s message=%s", strings.TrimSpace(component), fmt.Sprintf(format, args...))
			},
			RecordTransition: func(ctx context.Context, db *sql.DB, in runtimepipeline.ScanCampaignTransitionInput) error {
				return runtime.RecordPipelineTransition(ctx, db, runtime.PipelineTransitionInput{
					EventID:       in.EventID,
					EventType:     in.EventType,
					Handler:       in.Handler,
					PipelineType:  in.PipelineType,
					PipelineID:    in.PipelineID,
					Action:        in.Action,
					StateBefore:   in.StateBefore,
					StateAfter:    in.StateAfter,
					EventsEmitted: in.EventsEmitted,
					DropReason:    in.DropReason,
					Error:         in.Error,
					Duration:      in.Duration,
				})
			},
			EnsureDirectiveGeography: runtimepipeline.EnsureDirectiveGeography,
		}
		manager := runtimepipeline.NewScanCampaignManager(bus, stores.ScanCampaignStore, hooks, stores.SQLDB)
		go manager.Run(ctx)
	}
	inboundAddr := os.Getenv("EMPIREAI_INBOUND_ADDR")
	if inboundAddr != "" {
		gateway := runtime.NewInboundGateway(bus, stores.InboundStore)
		go func() {
			if err := http.ListenAndServe(inboundAddr, gateway.Handler()); err != nil {
				log.Printf("inbound gateway stopped: %v", err)
			}
		}()
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

	scheduler := runtime.NewScheduler(func(sc runtime.Schedule) {
		callbackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		payload := sc.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		if err := bus.Publish(callbackCtx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType(sc.EventType),
			SourceAgent: sc.AgentID,
			TaskID:      sc.TaskID,
			VerticalID:  sc.VerticalID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		}); err != nil {
			log.Printf("schedule publish failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
		}
		if stores.ScheduleStore != nil {
			if err := stores.ScheduleStore.MarkScheduleFired(callbackCtx, sc); err != nil {
				log.Printf("mark schedule fired failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
			}
		}
	})
	defer scheduler.Stop()
	if stores.ScheduleStore != nil {
		schedules, err := stores.ScheduleStore.LoadActiveSchedules(ctx)
		if err != nil {
			return fmt.Errorf("load schedules failed: %w", err)
		}
		for _, sc := range schedules {
			if err := scheduler.Register(sc); err != nil {
				log.Printf("restore schedule failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
			}
		}

		// Spec v2.0: daily portfolio digest heartbeat at 09:00 (local time per spec; UTC by default).
		if err := ensurePortfolioDigestSchedule(ctx, stores.ScheduleStore); err != nil {
			log.Printf("digest schedule ensure failed: %v", err)
		}
		if err := ensureMarginalReviewSchedule(ctx, stores.ScheduleStore); err != nil {
			log.Printf("marginal review schedule ensure failed: %v", err)
		}
		if err := ensureInfraHealthCheckSchedule(ctx, stores.ScheduleStore); err != nil {
			log.Printf("infra health schedule ensure failed: %v", err)
		}
	}

	workspaceLifecycle := buildWorkspaceLifecycle(ctx, stores.SQLDB)

	var budgetTracker *runtime.BudgetTracker
	if stores.SQLDB != nil {
		budgetTracker = runtime.NewBudgetTracker(stores.SQLDB, bus, cfg, stores.MailboxStore)
		go budgetHeartbeatLoop(ctx, budgetTracker)
	}

	llm, err := runtime.RuntimeFactory{
		Cfg:           cfg,
		Sessions:      stores.SessionRegistry,
		Turns:         stores.TurnStore,
		Conversations: stores.ConversationStore,
		Budget:        budgetTracker,
		Workspaces:    workspaceLifecycle,
	}.Build()
	if err != nil {
		return fmt.Errorf("build runtime: %w", err)
	}

	toolExecutor := runtimetools.NewExecutor(bus, scheduler, nil, stores.ScheduleStore)
	toolExecutor.SetConfig(cfg)
	toolExecutor.SetMailboxStore(stores.MailboxStore)
	toolExecutor.SetSQLDB(stores.SQLDB)
	factory := runtimeagents.NewLLMAgentFactory(llm, toolExecutor, toolExecutor.ToolDefinitions())
	manager := runtimemanager.NewAgentManager(bus, factory, stores.ManagerStore)
	manager.SetWorkspaceLifecycle(workspaceLifecycle)
	manager.SetSessionRegistry(stores.SessionRegistry, cfg.LLM.RuntimeMode)
	manager.SetBudgetTracker(budgetTracker)
	toolExecutor.SetManager(manager)
	if err := rotateGlobalAgentSessions(ctx, stores.ManagerStore, stores.SessionRegistry, cfg.LLM.RuntimeMode); err != nil {
		log.Printf("global session rotate after sync failed (continuing): %v", err)
	}

	// Spec v2.0: bidirectional Telegram bot for human tasks (Phase 1 uses long polling).
	startTelegramHumanTaskBot(ctx, stores, cfg, bus)
	toolGatewayAddr := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_ADDR"))
	if toolGatewayAddr == "" && workspaceLifecycle != nil {
		toolGatewayAddr = ":8090"
	}
	if toolGatewayAddr != "" {
		token := strings.TrimSpace(os.Getenv("EMPIREAI_TOOL_GATEWAY_TOKEN"))
		toolGateway := runtimemcp.NewGateway(toolExecutor, token, runtime.RuntimeMCPGatewayHooks(runtimeLogger))
		go func() {
			if err := http.ListenAndServe(toolGatewayAddr, toolGateway.Handler()); err != nil {
				log.Printf("tool gateway stopped: %v", err)
			}
		}()
	}
	dashboardAddr := strings.TrimSpace(os.Getenv("EMPIREAI_DASHBOARD_ADDR"))
	if dashboardAddr != "" && stores.SQLDB != nil {
		dashboardServer := dashboard.NewServer(stores.SQLDB, cfg, stores.EventStore, stores.MailboxStore, manager)
		go func() {
			if err := http.ListenAndServe(dashboardAddr, dashboardServer.Handler()); err != nil {
				log.Printf("dashboard server stopped: %v", err)
			}
		}()
	}
	if cfg.Runtime.RecoveryOnStartup {
		if err := manager.Recover(ctx); err != nil {
			// Spec intent: enforcement should be strict, but the runtime should remain operable
			// (dashboard/control plane available) even if legacy/persisted state cannot be hydrated.
			// Otherwise, operators cannot reset/seed/migrate out of a bad state and docker will crash-loop.
			log.Printf("runtime recovery failed (continuing without recovery): %v", err)

			// Clear any partially hydrated in-memory state so we're deterministic after a failed Recover.
			if resetErr := manager.ResetRuntimeState(); resetErr != nil {
				log.Printf("runtime state reset after recovery failure also failed: %v", resetErr)
			}

			// Escalate to mailbox so the operator sees it immediately in the dashboard.
			if stores.MailboxStore != nil {
				ctxPayload, _ := json.Marshal(map[string]any{
					"error":        err.Error(),
					"instruction":  "Runtime recovery failed. Use dashboard control actions (reset_db + seed-org) to reinitialize, or fix persisted config and restart.",
					"spec_version": "v2.0.15",
				})
				if len(ctxPayload) == 0 {
					ctxPayload = []byte("{}")
				}
				if _, mailboxErr := stores.MailboxStore.InsertMailboxItem(ctx, runtime.MailboxItem{
					FromAgent: "runtime",
					Type:      "runtime.recovery_failed",
					Priority:  "critical",
					Status:    "pending",
					Context:   ctxPayload,
					Summary:   truncateString("Runtime recovery failed: "+err.Error(), 200),
				}); mailboxErr != nil {
					log.Printf("runtime recovery mailbox insert failed: %v", mailboxErr)
				}
			}

			// Also emit an event for the live event stream / traces.
			payload, _ := json.Marshal(map[string]any{
				"error":        err.Error(),
				"spec_version": "v2.0.15",
			})
			if len(payload) == 0 {
				payload = []byte("{}")
			}
			if publishErr := bus.Publish(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("runtime.recovery_failed"),
				SourceAgent: "runtime",
				Payload:     payload,
				CreatedAt:   time.Now(),
			}); publishErr != nil {
				log.Printf("runtime recovery_failed publish failed: %v", publishErr)
			}
		}
	}
	manager.Run(ctx)
	if stores.SQLDB != nil && runtimeLogger != nil {
		go runtime.StartMCPStallDiagnosticLoop(ctx, stores.SQLDB, runtimeLogger, runtime.DefaultMCPStallDiagnosticConfig())
	}
	if stores.SQLDB != nil {
		if shardDispatcher := runtime.NewShardDispatcher(stores.SQLDB, bus, manager, cfg.Sharding); shardDispatcher != nil {
			go shardDispatcher.Run(ctx)
		}
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

	if selfCheck {
		if err := runSelfCheck(llm, bus); err != nil {
			return fmt.Errorf("self-check failed: %w", err)
		}
	}

	fmt.Println("empire runtime bootstrap ready")
	<-ctx.Done()
	return nil
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

func runMailboxSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire mailbox <list|view|decide|approve-spend|reject-spend|review|respond> [flags]")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("mailbox list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		critical := fs.Bool("critical", false, "Show only critical pending items")
		reviews := fs.Bool("reviews", false, "Show only founder review gate items")
		limit := fs.Int("limit", 20, "Max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxList:         true,
			mailboxListCritical: *critical,
			mailboxListReviews:  *reviews,
			mailboxLimit:        *limit,
		})
	case "view":
		fs := flag.NewFlagSet("mailbox view", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire mailbox view <id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxViewID: strings.TrimSpace(fs.Args()[0]),
		})
	case "decide":
		fs := flag.NewFlagSet("mailbox decide", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		action := fs.String("action", "", "Decision action (approve|reject|more-data|kill|revise|skip)")
		notes := fs.String("notes", "", "Decision notes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire mailbox decide <id> --action <action> [--notes ...]")
		}
		if strings.TrimSpace(*action) == "" {
			return fmt.Errorf("--action is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		return runOperatorActions(ctx, stores, operatorOptions{
			mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
			mailboxDecision: strings.TrimSpace(*action),
			mailboxNotes:    *notes,
		})
	case "approve-spend":
		return runMailboxDecisionAlias(args[1:], "approve")
	case "reject-spend":
		return runMailboxDecisionAlias(args[1:], "reject")
	case "review":
		return runMailboxReviewAlias(args[1:])
	case "respond":
		return runMailboxResponseAlias(args[1:])
	default:
		return fmt.Errorf("unknown mailbox command: %s", args[0])
	}
}

func runMailboxDecisionAlias(args []string, forcedAction string) error {
	fs := flag.NewFlagSet("mailbox decision alias", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Decision notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: forcedAction,
		mailboxNotes:    *notes,
	})
}

func runMailboxReviewAlias(args []string) error {
	fs := flag.NewFlagSet("mailbox review", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	action := fs.String("action", "", "Review action (approve|revise|skip)")
	notes := fs.String("notes", "", "Review notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	if strings.TrimSpace(*action) == "" {
		return fmt.Errorf("--action is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: strings.TrimSpace(*action),
		mailboxNotes:    *notes,
	})
}

func runMailboxResponseAlias(args []string) error {
	fs := flag.NewFlagSet("mailbox respond", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Response notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("mailbox item id is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		mailboxDecideID: strings.TrimSpace(fs.Args()[0]),
		mailboxDecision: "respond",
		mailboxNotes:    *notes,
	})
}

func runTasksSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire tasks <list|view|claim|complete|reject|stats> [flags]")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("tasks list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		status := fs.String("status", "open", "Task status filter (open|all|pending_review|approved|assigned|completed|rejected|deferred|expired)")
		category := fs.String("category", "", "Filter by category (optional)")
		outcome := fs.String("outcome", "", "Filter by outcome (optional; completed tasks only)")
		limit := fs.Int("limit", 50, "Max rows")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks commands require postgres store")
		}
		return printHumanTasks(ctx, stores.SQLDB, strings.TrimSpace(*status), strings.TrimSpace(*category), strings.TrimSpace(*outcome), *limit)
	case "view":
		fs := flag.NewFlagSet("tasks view", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks view <id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks commands require postgres store")
		}
		return printHumanTask(ctx, stores.SQLDB, strings.TrimSpace(fs.Args()[0]))
	case "claim":
		fs := flag.NewFlagSet("tasks claim", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		assignedTo := fs.String("assigned-to", "founder", "Human identifier")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks claim <id> [--assigned-to ...]")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks claim requires postgres store")
		}
		return claimHumanTask(ctx, stores, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*assignedTo))
	case "complete":
		fs := flag.NewFlagSet("tasks complete", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		result := fs.String("result", "", "Result text")
		outcome := fs.String("outcome", "success", "Outcome (success|partial|failed)")
		followUp := fs.Bool("follow-up", false, "Whether follow-up is needed")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks complete <id> --result \"...\" [--outcome success|partial|failed] [--follow-up]")
		}
		if strings.TrimSpace(*result) == "" {
			return fmt.Errorf("--result is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks complete requires postgres store")
		}
		return completeHumanTask(ctx, stores, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*result), strings.TrimSpace(*outcome), *followUp)
	case "reject":
		fs := flag.NewFlagSet("tasks reject", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		reason := fs.String("reason", "", "Human pushback reason")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire tasks reject <id> [--reason \"...\"]")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil || stores.EventStore == nil {
			return fmt.Errorf("tasks reject requires postgres store")
		}
		return rejectHumanTask(ctx, stores, cfg, strings.TrimSpace(fs.Args()[0]), strings.TrimSpace(*reason))
	case "stats":
		fs := flag.NewFlagSet("tasks stats", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("tasks stats requires postgres store")
		}
		return printHumanTaskStats(ctx, stores.SQLDB, cfg)
	default:
		return fmt.Errorf("unknown tasks command: %s", args[0])
	}
}

func printHumanTasks(ctx context.Context, db *sql.DB, status string, category string, outcome string, limit int) error {
	if db == nil {
		return fmt.Errorf("db unavailable")
	}
	status = strings.TrimSpace(status)
	category = strings.TrimSpace(category)
	outcome = strings.TrimSpace(outcome)
	if status == "" {
		status = "open"
	}
	if limit <= 0 {
		limit = 50
	}

	where := []string{"1=1"}
	args := []any{}
	if status != "all" && status != "open" {
		args = append(args, status)
		where = append(where, fmt.Sprintf("t.status = $%d", len(args)))
	} else if status == "open" {
		where = append(where, "t.status IN ('pending_review', 'approved', 'assigned')")
	}
	if category != "" {
		args = append(args, category)
		where = append(where, fmt.Sprintf("t.category = $%d", len(args)))
	}
	if outcome != "" {
		args = append(args, outcome)
		where = append(where, fmt.Sprintf("COALESCE(t.outcome,'') = $%d", len(args)))
	}
	args = append(args, limit)
	q := fmt.Sprintf(`
		SELECT
			t.id::text,
			t.status,
			COALESCE(t.priority, 'medium'),
			t.category,
			COALESCE(v.slug, ''),
			COALESCE(t.vertical_id::text, ''),
			t.requesting_agent,
			COALESCE(t.assigned_to, ''),
			t.created_at,
			t.deadline
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE %s
		ORDER BY t.created_at DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Printf("tasks: status=%s\n", status)
	fmt.Printf("%-10s  %-14s  %-6s  %-18s  %-16s  %-16s  %-20s  %-12s  %-20s  %-20s\n",
		"id", "status", "prio", "category", "vertical", "vertical_id", "requesting_agent", "assigned_to", "created_at", "deadline")
	for rows.Next() {
		var id, st, prio, cat, slug, vid, reqAgent, assigned string
		var created time.Time
		var deadline sql.NullTime
		if err := rows.Scan(&id, &st, &prio, &cat, &slug, &vid, &reqAgent, &assigned, &created, &deadline); err != nil {
			return err
		}
		vert := slug
		if strings.TrimSpace(vert) == "" {
			vert = "-"
		}
		deadlineText := "-"
		if deadline.Valid {
			deadlineText = deadline.Time.UTC().Format(time.RFC3339)
		}
		fmt.Printf("%-10s  %-14s  %-6s  %-18s  %-16s  %-16s  %-20s  %-12s  %-20s  %-20s\n",
			id[:min(10, len(id))],
			st,
			prio,
			cat,
			vert,
			trunc(vid, 16),
			trunc(reqAgent, 20),
			trunc(assigned, 12),
			created.UTC().Format(time.RFC3339),
			deadlineText,
		)
	}
	return rows.Err()
}

func printHumanTask(ctx context.Context, db *sql.DB, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	var (
		id, requesting, verticalID, slug, category, description, priority, status, assignedTo, result, outcome string
		created                                                                                                time.Time
		deadline, reviewed, completed                                                                          sql.NullTime
		followUp                                                                                               bool
	)
	err := db.QueryRowContext(ctx, `
		SELECT
			t.id::text,
			t.requesting_agent,
			COALESCE(t.vertical_id::text, ''),
			COALESCE(v.slug, ''),
			t.category,
			t.description,
			COALESCE(t.priority, 'medium'),
			t.status,
			COALESCE(t.assigned_to, ''),
			COALESCE(t.result, ''),
			COALESCE(t.outcome, ''),
			COALESCE(t.follow_up_needed, false),
			t.created_at,
			t.deadline,
			t.reviewed_at,
			t.completed_at
		FROM human_tasks t
		LEFT JOIN verticals v ON v.id = t.vertical_id
		WHERE t.id = $1::uuid
	`, taskID).Scan(
		&id, &requesting, &verticalID, &slug, &category, &description, &priority, &status,
		&assignedTo, &result, &outcome, &followUp, &created, &deadline, &reviewed, &completed,
	)
	if err != nil {
		return err
	}
	fmt.Printf("id: %s\n", id)
	fmt.Printf("status: %s\n", status)
	fmt.Printf("priority: %s\n", priority)
	fmt.Printf("category: %s\n", category)
	fmt.Printf("vertical: %s (%s)\n", slug, verticalID)
	fmt.Printf("requesting_agent: %s\n", requesting)
	fmt.Printf("assigned_to: %s\n", assignedTo)
	fmt.Printf("created_at: %s\n", created.UTC().Format(time.RFC3339))
	if deadline.Valid {
		fmt.Printf("deadline: %s\n", deadline.Time.UTC().Format(time.RFC3339))
	}
	if reviewed.Valid {
		fmt.Printf("reviewed_at: %s\n", reviewed.Time.UTC().Format(time.RFC3339))
	}
	if completed.Valid {
		fmt.Printf("completed_at: %s\n", completed.Time.UTC().Format(time.RFC3339))
	}
	fmt.Printf("follow_up_needed: %v\n", followUp)
	fmt.Printf("\nDESCRIPTION:\n%s\n", description)
	if result != "" {
		fmt.Printf("\nRESULT:\n%s\n", result)
		fmt.Printf("\nOUTCOME:\n%s\n", outcome)
	}
	return nil
}

func claimHumanTask(ctx context.Context, stores storeBundle, taskID, assignedTo string) error {
	taskID = strings.TrimSpace(taskID)
	assignedTo = strings.TrimSpace(assignedTo)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	if assignedTo == "" {
		assignedTo = "founder"
	}

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'assigned',
		    assigned_to = $2
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, assignedTo).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"assigned_to":      assignedTo,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.assigned"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(requestingAgent)}); err != nil {
		return err
	}

	fmt.Printf("task claimed: id=%s assigned_to=%s\n", taskID, assignedTo)
	return nil
}

func completeHumanTask(ctx context.Context, stores storeBundle, taskID, result, outcome string, followUp bool) error {
	taskID = strings.TrimSpace(taskID)
	result = strings.TrimSpace(result)
	outcome = strings.TrimSpace(outcome)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	if result == "" {
		return fmt.Errorf("result is required")
	}
	if outcome == "" {
		outcome = "success"
	}

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'completed',
		    result = $2,
		    outcome = $3,
		    follow_up_needed = $4,
		    completed_at = now()
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, result, outcome, followUp).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"result_text":      result,
		"outcome":          outcome,
		"follow_up_needed": followUp,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.completed"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(requestingAgent)}); err != nil {
		return err
	}

	fmt.Printf("task completed: id=%s outcome=%s follow_up_needed=%v\n", taskID, outcome, followUp)
	return nil
}

func rejectHumanTask(ctx context.Context, stores storeBundle, cfg *config.Config, taskID, reason string) error {
	taskID = strings.TrimSpace(taskID)
	reason = strings.TrimSpace(reason)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}

	resetDay := "monday"
	if cfg != nil && strings.TrimSpace(cfg.Budget.HumanTasks.BudgetReset) != "" {
		resetDay = strings.TrimSpace(cfg.Budget.HumanTasks.BudgetReset)
	}
	requeueAt := runtime.NextWeekResetUTC(time.Now(), resetDay).UTC().Format(time.RFC3339)

	decisionObj := map[string]any{
		"decision":     "deferred",
		"defer_reason": "human_pushback",
		"human_reason": reason,
		"requeue_date": requeueAt,
		"decided_by":   "human",
		"decided_at":   time.Now().UTC().Format(time.RFC3339),
	}
	decisionJSON, _ := json.Marshal(decisionObj)

	var requestingAgent string
	var verticalID string
	if err := stores.SQLDB.QueryRowContext(ctx, `
		UPDATE human_tasks
		SET status = 'deferred',
		    reviewed_at = now(),
		    review_decision = $2::jsonb,
		    requeue_count = COALESCE(requeue_count, 0) + 1,
		    assigned_to = NULL
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`, taskID, string(decisionJSON)).Scan(&requestingAgent, &verticalID); err != nil {
		return err
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
		"defer_reason":     "human_pushback",
		"human_reason":     reason,
		"requeue_date":     requeueAt,
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.deferred"),
		SourceAgent: "cli",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(requestingAgent), "empire-coordinator"}); err != nil {
		return err
	}

	fmt.Printf("task rejected (pushback): id=%s requeue_date=%s\n", taskID, requeueAt)
	return nil
}

func printHumanTaskStats(ctx context.Context, db *sql.DB, cfg *config.Config) error {
	if db == nil {
		return fmt.Errorf("db unavailable")
	}
	resetDay := "monday"
	maxPerWeek := 0
	if cfg != nil {
		if strings.TrimSpace(cfg.Budget.HumanTasks.BudgetReset) != "" {
			resetDay = strings.TrimSpace(cfg.Budget.HumanTasks.BudgetReset)
		}
		maxPerWeek = cfg.Budget.HumanTasks.MaxTasksPerWeek
	}
	weekStart := runtime.WeekStartUTC(time.Now(), resetDay)
	var approvedThisWeek int
	_ = db.QueryRowContext(ctx, `
		SELECT COALESCE(count(*), 0)
		FROM human_tasks
		WHERE reviewed_at >= $1
		  AND status IN ('approved', 'assigned', 'completed')
	`, weekStart).Scan(&approvedThisWeek)

	rows, err := db.QueryContext(ctx, `
		SELECT category, COALESCE(status,''), COALESCE(outcome,''), count(*)
		FROM human_tasks
		WHERE created_at >= now() - interval '30 days'
		GROUP BY category, COALESCE(status,''), COALESCE(outcome,'')
		ORDER BY category ASC
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type stat struct {
		Category string
		Status   string
		Outcome  string
		Count    int
	}
	stats := make([]stat, 0, 64)
	for rows.Next() {
		var s stat
		if err := rows.Scan(&s.Category, &s.Status, &s.Outcome, &s.Count); err != nil {
			return err
		}
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Printf("tasks stats (30d)\n")
	fmt.Printf("weekly budget: %d/%d (week_start=%s reset=%s)\n", approvedThisWeek, maxPerWeek, weekStart.UTC().Format(time.RFC3339), resetDay)
	fmt.Printf("%-18s  %-12s  %-10s  %-6s\n", "category", "status", "outcome", "count")
	for _, s := range stats {
		fmt.Printf("%-18s  %-12s  %-10s  %-6d\n", s.Category, s.Status, s.Outcome, s.Count)
	}
	return nil
}

func trunc(v string, n int) string {
	v = strings.TrimSpace(v)
	if n <= 0 {
		return v
	}
	if len(v) <= n {
		return v
	}
	return v[:n]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func runDigestSubcommand(args []string) error {
	fs := flag.NewFlagSet("digest", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	topN := fs.Int("top", 10, "Top verticals in digest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	return runOperatorActions(ctx, stores, operatorOptions{
		digestGenerate: true,
		digestTopN:     *topN,
	})
}

func runStatusSubcommand(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	vertical := fs.String("vertical", "", "Optional vertical id or slug")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("status requires persistent store mode (use -store postgres)")
	}

	if strings.TrimSpace(*vertical) != "" {
		verticalID, err := resolveVerticalID(ctx, stores.SQLDB, strings.TrimSpace(*vertical))
		if err != nil {
			return err
		}
		var name, slug, stage, mode, templateVersion string
		if err := stores.SQLDB.QueryRowContext(ctx, `
			SELECT name, COALESCE(slug,''), stage, mode, COALESCE(template_version,'')
			FROM verticals
			WHERE id = $1::uuid
		`, verticalID).Scan(&name, &slug, &stage, &mode, &templateVersion); err != nil {
			return fmt.Errorf("load vertical status: %w", err)
		}
		fmt.Printf("vertical status\nid: %s\nslug: %s\nname: %s\nstage: %s\nmode: %s\ntemplate_version: %s\n",
			verticalID, slug, name, stage, mode, templateVersion)
		return nil
	}

	var total, operating, factory int
	if err := stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals`).Scan(&total); err != nil {
		return err
	}
	if err := stores.SQLDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM verticals WHERE mode = 'operating'`).Scan(&operating); err != nil {
		return err
	}
	factory = total - operating
	fmt.Printf("status\nverticals_total: %d\nverticals_factory: %d\nverticals_operating: %d\n", total, factory, operating)
	if stores.MailboxStore != nil {
		st, err := mailbox.GetStatus(ctx, stores.MailboxStore)
		if err != nil {
			return err
		}
		fmt.Printf("mailbox_pending: %d\nmailbox_critical: %d\n", st.Pending, st.Critical)
	}
	return nil
}

func runBudgetSubcommand(args []string) error {
	fs := flag.NewFlagSet("budget", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("budget requires persistent store mode (use -store postgres)")
	}
	monthStart := time.Now().UTC()
	monthStart = time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, time.UTC)

	var spent int64
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(sum(amount_cents),0)
		FROM spend_ledger
		WHERE created_at >= $1
	`, monthStart).Scan(&spent); err != nil {
		return fmt.Errorf("load budget spend: %w", err)
	}

	portfolioCap := cfg.Budget.PortfolioMonthlyCap
	perVerticalCap := cfg.Budget.PerVerticalMonthlyCap
	factoryCap := cfg.Budget.FactoryMonthlyCap
	portfolioPct := 0.0
	if portfolioCap > 0 {
		portfolioPct = (float64(spent) / float64(portfolioCap)) * 100
	}

	fmt.Printf("budget\nmonth_start: %s\nspent_cents: %d\n", monthStart.Format(time.RFC3339), spent)
	fmt.Printf("portfolio_monthly_cap_cents: %d\n", portfolioCap)
	fmt.Printf("per_vertical_monthly_cap_cents: %d\n", perVerticalCap)
	fmt.Printf("factory_monthly_cap_cents: %d\n", factoryCap)
	if portfolioCap > 0 {
		fmt.Printf("portfolio_used_pct: %.2f\n", portfolioPct)
	} else {
		fmt.Printf("portfolio_used_pct: n/a\n")
	}
	return nil
}

func runAgentsSubcommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: empire agents <vertical-id|slug> [flags]")
	}
	target := strings.TrimSpace(args[0])
	if target == "" {
		return fmt.Errorf("usage: empire agents <vertical-id|slug> [flags]")
	}
	return runVerticalTeamSubcommand(target, args[1:])
}

func runVerticalsSubcommand(args []string) error {
	if len(args) == 0 {
		args = []string{"list"}
	}
	switch args[0] {
	case "list":
		return runVerticalsListSubcommand(args[1:], false)
	case "operating":
		return runVerticalsListSubcommand(args[1:], true)
	default:
		return fmt.Errorf("usage: empire verticals <list|operating> [flags]")
	}
}

func runVerticalsListSubcommand(args []string, operatingOnly bool) error {
	fs := flag.NewFlagSet("verticals list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	limit := fs.Int("limit", 100, "Row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("verticals command requires persistent store mode (use -store postgres)")
	}

	query := `
		SELECT id::text, name, COALESCE(slug,''), stage, mode
		FROM verticals
	`
	if operatingOnly {
		query += ` WHERE mode = 'operating'`
	}
	query += ` ORDER BY created_at DESC LIMIT $1`
	rows, err := stores.SQLDB.QueryContext(ctx, query, *limit)
	if err != nil {
		return fmt.Errorf("list verticals: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, slug, stage, mode string
		if err := rows.Scan(&id, &name, &slug, &stage, &mode); err != nil {
			return fmt.Errorf("scan vertical row: %w", err)
		}
		fmt.Printf("- id=%s slug=%s mode=%s stage=%s name=%s\n", id, slug, mode, stage, name)
	}
	return rows.Err()
}

func runVerticalSubcommand(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: empire vertical <id|slug> <metrics|team|logs|kill> [flags]")
	}
	target := strings.TrimSpace(args[0])
	sub := strings.TrimSpace(args[1])
	switch sub {
	case "metrics":
		return runVerticalMetricsSubcommand(target, args[2:])
	case "team":
		return runVerticalTeamSubcommand(target, args[2:])
	case "logs":
		return runVerticalLogsSubcommand(target, args[2:])
	case "kill":
		return runVerticalKillSubcommand(target, args[2:])
	default:
		return fmt.Errorf("unknown vertical subcommand: %s", sub)
	}
}

func runVerticalMetricsSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical metrics", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical metrics requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	var users, mrr, apiCost, infraCost int
	err = stores.SQLDB.QueryRowContext(ctx, `
		SELECT users_total, mrr_cents, api_cost_cents, infra_cost_cents
		FROM vertical_metrics
		WHERE vertical_id = $1::uuid
		ORDER BY period_end DESC
		LIMIT 1
	`, verticalID).Scan(&users, &mrr, &apiCost, &infraCost)
	if err == sql.ErrNoRows {
		fmt.Printf("no metrics found for vertical %s\n", verticalID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load vertical metrics: %w", err)
	}
	fmt.Printf("vertical metrics\nid: %s\nusers_total: %d\nmrr_cents: %d\napi_cost_cents: %d\ninfra_cost_cents: %d\n",
		verticalID, users, mrr, apiCost, infraCost)
	return nil
}

func runVerticalTeamSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical team", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical team requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT id, role, status, COALESCE(parent_agent_id, '')
		FROM agents
		WHERE vertical_id = $1::uuid
		ORDER BY started_at ASC
	`, verticalID)
	if err != nil {
		return fmt.Errorf("list team agents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, role, status, parent string
		if err := rows.Scan(&id, &role, &status, &parent); err != nil {
			return fmt.Errorf("scan team row: %w", err)
		}
		fmt.Printf("- id=%s role=%s status=%s parent=%s\n", id, role, status, parent)
	}
	return rows.Err()
}

func runVerticalLogsSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical logs", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	agent := fs.String("agent", "", "Filter by source agent")
	limit := fs.Int("limit", 20, "Max events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical logs requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}

	query := `
		SELECT id::text, type, source_agent, created_at
		FROM events
		WHERE vertical_id = $1::uuid
	`
	argsQ := []any{verticalID}
	if strings.TrimSpace(*agent) != "" {
		query += ` AND source_agent = $2`
		argsQ = append(argsQ, strings.TrimSpace(*agent))
		query += ` ORDER BY created_at DESC LIMIT $3`
		argsQ = append(argsQ, *limit)
	} else {
		query += ` ORDER BY created_at DESC LIMIT $2`
		argsQ = append(argsQ, *limit)
	}
	rows, err := stores.SQLDB.QueryContext(ctx, query, argsQ...)
	if err != nil {
		return fmt.Errorf("list vertical logs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, typ, source string
		var created time.Time
		if err := rows.Scan(&id, &typ, &source, &created); err != nil {
			return fmt.Errorf("scan event row: %w", err)
		}
		fmt.Printf("- id=%s type=%s source=%s at=%s\n", id, typ, source, created.UTC().Format(time.RFC3339))
	}
	return rows.Err()
}

func runVerticalKillSubcommand(target string, args []string) error {
	fs := flag.NewFlagSet("vertical kill", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	notes := fs.String("notes", "", "Kill notes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("vertical kill requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}
	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE verticals
		SET stage = 'winding_down',
		    kill_reason = NULLIF($2,''),
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, strings.TrimSpace(*notes)); err != nil {
		return fmt.Errorf("kill vertical: %w", err)
	}
	if envBool("EMPIREAI_ENABLE_DOCKER_WORKSPACES", true) {
		workspaces := workspace.NewDockerManager(stores.SQLDB)
		if err := workspaces.StopVerticalWorkspace(ctx, verticalID); err != nil && envBool("EMPIREAI_REQUIRE_DOCKER_WORKSPACES", false) {
			return fmt.Errorf("stop vertical workspace: %w", err)
		}
	}
	fmt.Printf("vertical marked winding_down id=%s\n", verticalID)
	return nil
}

func runDeploymentsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire deployments <list|health> [flags]")
	}
	switch args[0] {
	case "list":
		return runDeploymentsListSubcommand(args[1:])
	case "health":
		return runDeploymentsHealthSubcommand(args[1:])
	default:
		return fmt.Errorf("unknown deployments subcommand: %s", args[0])
	}
}

func runDeploymentsListSubcommand(args []string) error {
	fs := flag.NewFlagSet("deployments list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	limit := fs.Int("limit", 50, "Row limit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("deployments list requires persistent store mode (use -store postgres)")
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT id::text, COALESCE(vertical_id::text,''), status, COALESCE(url,''), COALESCE(environment,'production'), COALESCE(version,1), COALESCE(health_status,'unknown')
		FROM deployments
		ORDER BY created_at DESC
		LIMIT $1
	`, *limit)
	if err != nil {
		return fmt.Errorf("list deployments: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, verticalID, status, url, env, health string
		var version int
		if err := rows.Scan(&id, &verticalID, &status, &url, &env, &version, &health); err != nil {
			return fmt.Errorf("scan deployment row: %w", err)
		}
		fmt.Printf("- id=%s vertical=%s env=%s version=%d status=%s health=%s url=%s\n", id, verticalID, env, version, status, health, url)
	}
	return rows.Err()
}

func runDeploymentsHealthSubcommand(args []string) error {
	fs := flag.NewFlagSet("deployments health", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("deployments health requires persistent store mode (use -store postgres)")
	}
	rows, err := stores.SQLDB.QueryContext(ctx, `
		SELECT COALESCE(health_status, 'unknown') AS health, COUNT(*)
		FROM deployments
		GROUP BY health
		ORDER BY health
	`)
	if err != nil {
		return fmt.Errorf("deployment health aggregation: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var health string
		var count int
		if err := rows.Scan(&health, &count); err != nil {
			return fmt.Errorf("scan health row: %w", err)
		}
		fmt.Printf("- health=%s count=%d\n", health, count)
	}
	return rows.Err()
}

func runSecretsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire secrets <set|list|rotate> ...")
	}
	switch args[0] {
	case "set":
		return runSecretsSetSubcommand(args[1:])
	case "list":
		return runSecretsListSubcommand(args[1:])
	case "rotate":
		return runSecretsRotateSubcommand(args[1:])
	default:
		return fmt.Errorf("unknown secrets subcommand: %s", args[0])
	}
}

func runSecretsSetSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets set", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 3 {
		return fmt.Errorf("usage: empire secrets set <vertical-id|slug> <key.path> <value>")
	}
	target := strings.TrimSpace(fs.Args()[0])
	keyPath := strings.TrimSpace(fs.Args()[1])
	value := strings.TrimSpace(strings.Join(fs.Args()[2:], " "))
	if keyPath == "" || value == "" {
		return fmt.Errorf("key and value are required")
	}
	path := splitCredentialPath(keyPath)
	if len(path) == 0 {
		return fmt.Errorf("invalid key path")
	}

	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("secrets set requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, target)
	if err != nil {
		return err
	}

	var currentRaw []byte
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&currentRaw); err != nil {
		return fmt.Errorf("load existing credentials: %w", err)
	}
	current := map[string]any{}
	_ = json.Unmarshal(currentRaw, &current)
	storedValue, err := maybeEncryptCredentialValue(ctx, stores.SQLDB, value)
	if err != nil {
		return err
	}
	setNestedYAML(current, path, storedValue)
	nextRaw, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("encode updated credentials: %w", err)
	}

	if _, err := stores.SQLDB.ExecContext(ctx, `
		UPDATE verticals
		SET credentials = $2::jsonb,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, string(nextRaw)); err != nil {
		return fmt.Errorf("set credential value: %w", err)
	}
	fmt.Printf("secret set vertical=%s key=%s\n", verticalID, keyPath)
	return nil
}

func maybeEncryptCredentialValue(ctx context.Context, db *sql.DB, plain string) (string, error) {
	plain = strings.TrimSpace(plain)
	if plain == "" || db == nil {
		return plain, nil
	}
	key := strings.TrimSpace(os.Getenv("EMPIREAI_CREDENTIALS_KEY"))
	if key == "" {
		return plain, nil
	}
	var encoded string
	if err := db.QueryRowContext(ctx, `
		SELECT encode(pgp_sym_encrypt($1::text, $2::text), 'base64')
	`, plain, key).Scan(&encoded); err != nil {
		return "", fmt.Errorf("encrypt credential value: %w", err)
	}
	return "enc::" + strings.TrimSpace(encoded), nil
}

func runSecretsListSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets list", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire secrets list <vertical-id|slug>")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("secrets list requires persistent store mode (use -store postgres)")
	}
	verticalID, err := resolveVerticalID(ctx, stores.SQLDB, strings.TrimSpace(fs.Args()[0]))
	if err != nil {
		return err
	}
	var raw []byte
	if err := stores.SQLDB.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&raw); err != nil {
		return fmt.Errorf("load credentials: %w", err)
	}
	keys := flattenCredentialKeys(raw)
	if len(keys) == 0 {
		fmt.Println("no secret keys configured")
		return nil
	}
	for _, k := range keys {
		fmt.Printf("- %s\n", k)
	}
	return nil
}

func runSecretsRotateSubcommand(args []string) error {
	fs := flag.NewFlagSet("secrets rotate", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	value := fs.String("value", "", "New secret value")
	key := fs.String("key", "", "Optional key path override")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 2 {
		return fmt.Errorf("usage: empire secrets rotate <vertical-id|slug> <provider> --value <new-value> [--key provider.token]")
	}
	if strings.TrimSpace(*value) == "" {
		return fmt.Errorf("--value is required")
	}
	target := strings.TrimSpace(fs.Args()[0])
	provider := strings.TrimSpace(fs.Args()[1])
	keyPath := strings.TrimSpace(*key)
	if keyPath == "" {
		keyPath = provider + ".token"
	}
	return runSecretsSetSubcommand([]string{
		"-config", *cfgPath,
		"-store", *storeMode,
		fmt.Sprintf("-migrate=%v", *migrate),
		"-migration-file", *migrationFile,
		target, keyPath, *value,
	})
}

func resolveVerticalID(ctx context.Context, db *sql.DB, target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("vertical id or slug is required")
	}
	var id string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM verticals
		WHERE id::text = $1 OR slug = $1
		ORDER BY CASE WHEN id::text = $1 THEN 0 ELSE 1 END
		LIMIT 1
	`, target).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("vertical not found: %s", target)
		}
		return "", fmt.Errorf("resolve vertical id: %w", err)
	}
	return id, nil
}

func splitCredentialPath(path string) []string {
	parts := strings.Split(path, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func flattenCredentialKeys(raw []byte) []string {
	if len(raw) == 0 {
		return nil
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	keys := make([]string, 0, 16)
	var walk func(prefix string, v any)
	walk = func(prefix string, v any) {
		switch node := v.(type) {
		case map[string]any:
			for k, vv := range node {
				next := k
				if prefix != "" {
					next = prefix + "." + k
				}
				walk(next, vv)
			}
		default:
			if prefix != "" {
				keys = append(keys, prefix)
			}
		}
	}
	walk("", root)
	return keys
}

func runConfigSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire config <set|get> ...")
	}
	switch args[0] {
	case "set":
		fs := flag.NewFlagSet("config set", flag.ContinueOnError)
		file := fs.String("file", "configs/empire.yaml", "Config file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 2 {
			return fmt.Errorf("usage: empire config set <key.path> <value>")
		}
		keyPath := strings.TrimSpace(fs.Args()[0])
		value := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
		doc, err := readYAMLDocument(*file)
		if err != nil {
			return err
		}
		setNestedYAML(doc, splitCredentialPath(keyPath), parseConfigValue(value))
		return writeYAMLDocument(*file, doc)
	case "get":
		fs := flag.NewFlagSet("config get", flag.ContinueOnError)
		file := fs.String("file", "configs/empire.yaml", "Config file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire config get <key.path>")
		}
		keyPath := strings.TrimSpace(fs.Args()[0])
		doc, err := readYAMLDocument(*file)
		if err != nil {
			return err
		}
		val, ok := getNestedYAML(doc, splitCredentialPath(keyPath))
		if !ok {
			return fmt.Errorf("config key not found: %s", keyPath)
		}
		out, _ := json.MarshalIndent(val, "", "  ")
		fmt.Println(string(out))
		return nil
	default:
		return fmt.Errorf("unknown config subcommand: %s", args[0])
	}
}

func readYAMLDocument(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	return doc, nil
}

func writeYAMLDocument(path string, doc map[string]any) error {
	b, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	fmt.Printf("config updated %s\n", path)
	return nil
}

func setNestedYAML(root map[string]any, path []string, value any) {
	if len(path) == 0 {
		return
	}
	cur := root
	for i := 0; i < len(path)-1; i++ {
		k := path[i]
		next, ok := cur[k]
		if !ok {
			n := map[string]any{}
			cur[k] = n
			cur = n
			continue
		}
		m, ok := next.(map[string]any)
		if !ok {
			m = map[string]any{}
			cur[k] = m
		}
		cur = m
	}
	cur[path[len(path)-1]] = value
}

func getNestedYAML(root map[string]any, path []string) (any, bool) {
	if len(path) == 0 {
		return root, true
	}
	var cur any = root
	for _, k := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := m[k]
		if !ok {
			return nil, false
		}
		cur = next
	}
	return cur, true
}

func parseConfigValue(raw string) any {
	v := strings.TrimSpace(raw)
	switch strings.ToLower(v) {
	case "true", "enabled", "on":
		return true
	case "false", "disabled", "off":
		return false
	}
	if i, err := strconv.Atoi(v); err == nil {
		return i
	}
	return v
}

func runScanSubcommand(args []string) error {
	if len(args) > 0 {
		switch strings.TrimSpace(args[0]) {
		case "shards":
			return runScanShardsSubcommand(args[1:])
		case "shard":
			return runScanShardSubcommand(args[1:])
		}
	}

	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	geography := fs.String("geography", "", "Geography to scan")
	depth := fs.String("depth", "full", "Scan depth: discovery|score|full")
	count := fs.Int("count", 3, "How many candidate verticals to generate")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*geography) == "" {
		return fmt.Errorf("--geography is required")
	}
	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.SQLDB == nil {
		return fmt.Errorf("scan requires persistent store mode (use -store postgres)")
	}
	p := factory.NewPipeline(stores.SQLDB, stores.EventStore, stores.MailboxStore)
	sum, err := p.RunScan(ctx, *geography, *depth, *count)
	if err != nil {
		return err
	}
	fmt.Printf(
		"scan completed geography=%s depth=%s discovered=%d scored=%d ready_for_review=%d killed=%d\n",
		*geography, *depth, sum.Discovered, sum.Scored, sum.ReadyForReview, sum.Killed,
	)
	for _, id := range sum.VerticalIDs {
		fmt.Printf("- vertical_id=%s\n", id)
	}
	return nil
}

func runFactorySubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire factory run [flags]")
	}
	switch args[0] {
	case "run":
		fs := flag.NewFlagSet("factory run", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		limit := fs.Int("limit", 20, "Max pending verticals to process")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("factory run requires persistent store mode (use -store postgres)")
		}
		p := factory.NewPipeline(stores.SQLDB, stores.EventStore, stores.MailboxStore)
		sum, err := p.RunPending(ctx, *limit)
		if err != nil {
			return err
		}
		fmt.Printf(
			"factory run completed processed=%d scored=%d ready_for_review=%d killed=%d\n",
			len(sum.VerticalIDs), sum.Scored, sum.ReadyForReview, sum.Killed,
		)
		return nil
	default:
		return fmt.Errorf("unknown factory command: %s", args[0])
	}
}

func runSpecAuditSubcommand(args []string) error {
	fs := flag.NewFlagSet("spec-audit", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	specType := fs.String("spec-type", "vertical_spec", "Spec type: vertical_spec|template|technical_spec")
	verticalID := fs.String("vertical-id", "", "Vertical ID for vertical spec audits")
	specFile := fs.String("spec-file", "", "Path to JSON spec file")
	requestedBy := fs.String("requested-by", "factory-cto", "Requesting agent id")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if stores.EventStore == nil {
		return fmt.Errorf("spec-audit requires persistent store mode (use -store postgres)")
	}

	specRaw, err := loadAuditSpecInput(ctx, stores.SQLDB, strings.TrimSpace(*specType), strings.TrimSpace(*verticalID), strings.TrimSpace(*specFile))
	if err != nil {
		return err
	}
	requestPayload := mustJSON(map[string]any{
		"spec_type":    *specType,
		"vertical_id":  *verticalID,
		"requested_by": *requestedBy,
		"spec":         json.RawMessage(specRaw),
	})
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_requested"),
		SourceAgent: *requestedBy,
		VerticalID:  strings.TrimSpace(*verticalID),
		Payload:     requestPayload,
		CreatedAt:   time.Now(),
	}, []string{"spec-auditor"}); err != nil {
		return err
	}

	result := specaudit.Validate(strings.TrimSpace(*specType), specRaw)
	resultPayload := mustJSON(map[string]any{
		"spec_type":   result.SpecType,
		"vertical_id": strings.TrimSpace(*verticalID),
		"passed":      result.Passed,
		"issues":      result.Issues,
	})
	eventType := events.EventType("spec.validation_failed")
	if result.Passed {
		eventType = events.EventType("spec.validation_passed")
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "spec-auditor",
		VerticalID:  strings.TrimSpace(*verticalID),
		Payload:     resultPayload,
		CreatedAt:   time.Now(),
	}, []string{strings.TrimSpace(*requestedBy)}); err != nil {
		return err
	}
	if result.Passed {
		fmt.Printf("spec-audit passed spec_type=%s vertical_id=%s\n", result.SpecType, strings.TrimSpace(*verticalID))
		return nil
	}
	fmt.Printf("spec-audit failed spec_type=%s vertical_id=%s issues=%d\n", result.SpecType, strings.TrimSpace(*verticalID), len(result.Issues))
	for _, issue := range result.Issues {
		fmt.Printf("- [%s] %s at %s: %s\n", issue.Severity, issue.Code, issue.Location, issue.Message)
	}
	return nil
}

func loadAuditSpecInput(ctx context.Context, db *sql.DB, specType, verticalID, specFile string) ([]byte, error) {
	specType = strings.ToLower(strings.TrimSpace(specType))
	specType = strings.ReplaceAll(specType, "-", "_")
	if specFile != "" {
		b, err := os.ReadFile(specFile)
		if err != nil {
			return nil, fmt.Errorf("read spec file: %w", err)
		}
		return b, nil
	}
	if specType == "template" {
		return nil, fmt.Errorf("--spec-file is required for template audits")
	}
	if db == nil {
		return nil, fmt.Errorf("spec lookup requires postgres db")
	}
	if verticalID == "" {
		return nil, fmt.Errorf("--vertical-id is required when --spec-file is not provided")
	}
	if specType == "technical_spec" {
		var raw []byte
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(full_spec, '{}'::jsonb) FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&raw); err != nil {
			return nil, fmt.Errorf("load technical spec: %w", err)
		}
		return raw, nil
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(mvp_spec, '{}'::jsonb) FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load vertical spec: %w", err)
	}
	return raw, nil
}

func runTemplateSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire template <publish|list|current|diff|plan|apply> [flags]")
	}
	switch args[0] {
	case "publish":
		fs := flag.NewFlagSet("template publish", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		version := fs.String("version", "", "Template version")
		agentsFile := fs.String("agents-file", "", "Path to template agents json (legacy)")
		bootstrapFile := fs.String("bootstrap-routes-file", "", "Path to bootstrap routes json (legacy)")
		seededFile := fs.String("seeded-routes-file", "", "Path to seeded routes json (legacy)")
		agentsDir := fs.String("agents-dir", "configs/agents/templates", "Path to YAML agent templates directory")
		routesYAML := fs.String("routes-yaml", "configs/agents/templates/routes.yaml", "Path to YAML routing template")
		createdBy := fs.String("created-by", "factory-cto", "Publisher agent")
		description := fs.String("description", "", "Template description")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*version) == "" {
			return fmt.Errorf("--version is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := templateops.NewService(stores.SQLDB, stores.MailboxStore)

		// Default path: publish from YAML authoring surface.
		// Legacy JSON flags remain supported for compatibility.
		var agents, bootstrapRoutes, seededRoutes []byte
		if strings.TrimSpace(*agentsFile) != "" || strings.TrimSpace(*bootstrapFile) != "" || strings.TrimSpace(*seededFile) != "" {
			agents, err = readOptionalJSONFile(*agentsFile, []byte("[]"))
			if err != nil {
				return err
			}
			bootstrapRoutes, err = readOptionalJSONFile(*bootstrapFile, []byte("[]"))
			if err != nil {
				return err
			}
			seededRoutes, err = readOptionalJSONFile(*seededFile, []byte("[]"))
			if err != nil {
				return err
			}
		} else {
			agents, bootstrapRoutes, seededRoutes, err = templateops.CompileTemplateFromYAML(*agentsDir, *routesYAML)
			if err != nil {
				return err
			}

			// Validate the envelope shape expected by the Spec Auditor.
			env := mustJSON(map[string]any{
				"version":          strings.TrimSpace(*version),
				"agents":           json.RawMessage(agents),
				"bootstrap_routes": json.RawMessage(bootstrapRoutes),
				"seeded_routes":    json.RawMessage(seededRoutes),
				"notes":            strings.TrimSpace(*description),
			})
			res := specaudit.Validate("template", env)
			if !res.Passed {
				fmt.Printf("template publish blocked by spec audit issues=%d\n", len(res.Issues))
				for _, issue := range res.Issues {
					fmt.Printf("- [%s] %s at %s: %s\n", issue.Severity, issue.Code, issue.Location, issue.Message)
				}
				return fmt.Errorf("template publish failed spec audit")
			}
		}

		if stores.EventStore != nil {
			reqPayload := map[string]any{
				"version":      strings.TrimSpace(*version),
				"created_by":   strings.TrimSpace(*createdBy),
				"description":  strings.TrimSpace(*description),
				"agents_dir":   strings.TrimSpace(*agentsDir),
				"routes_yaml":  strings.TrimSpace(*routesYAML),
				"requested_at": time.Now().UTC().Format(time.RFC3339),
			}
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("template.publish_requested"),
				SourceAgent: "human",
				Payload:     mustJSON(reqPayload),
				CreatedAt:   time.Now(),
			}, []string{"factory-cto"}); err != nil {
				log.Printf("template publish_requested append failed: %v", err)
			}
		}

		if err := svc.PublishTemplate(ctx, *version, agents, bootstrapRoutes, seededRoutes, *createdBy, *description); err != nil {
			return err
		}
		fmt.Printf("template published version=%s\n", *version)
		return nil
	case "list":
		fs := flag.NewFlagSet("template list", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		limit := fs.Int("limit", 20, "Max templates to list")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("template list requires persistent store mode (use -store postgres)")
		}
		if *limit <= 0 {
			*limit = 20
		}
		rows, err := stores.SQLDB.QueryContext(ctx, `
			SELECT version, COALESCE(created_by,''), COALESCE(description,''), created_at
			FROM org_templates
			ORDER BY created_at DESC
			LIMIT $1
		`, *limit)
		if err != nil {
			return fmt.Errorf("list templates: %w", err)
		}
		defer rows.Close()
		fmt.Println("template versions")
		n := 0
		for rows.Next() {
			var version, createdBy, desc string
			var createdAt time.Time
			if err := rows.Scan(&version, &createdBy, &desc, &createdAt); err != nil {
				return fmt.Errorf("scan template row: %w", err)
			}
			n++
			fmt.Printf("- version=%s created_by=%s created_at=%s description=%q\n",
				version, nullable(createdBy, "-"), createdAt.UTC().Format(time.RFC3339), desc)
		}
		if n == 0 {
			fmt.Println("- (none)")
		}
		return nil
	case "current":
		fs := flag.NewFlagSet("template current", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		verticalID := fs.String("vertical-id", "", "Optional vertical id to resolve effective template")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("template current requires persistent store mode (use -store postgres)")
		}
		if strings.TrimSpace(*verticalID) != "" {
			var version string
			if err := stores.SQLDB.QueryRowContext(ctx, `
				SELECT COALESCE(template_version, '')
				FROM verticals
				WHERE id = $1::uuid
			`, strings.TrimSpace(*verticalID)).Scan(&version); err != nil {
				return fmt.Errorf("load vertical template version: %w", err)
			}
			if strings.TrimSpace(version) == "" {
				fmt.Printf("vertical template\nvertical_id: %s\ntemplate_version: (none)\n", strings.TrimSpace(*verticalID))
				return nil
			}
			fmt.Printf("vertical template\nvertical_id: %s\ntemplate_version: %s\n", strings.TrimSpace(*verticalID), version)
			return nil
		}
		var version, createdBy string
		var createdAt time.Time
		if err := stores.SQLDB.QueryRowContext(ctx, `
			SELECT version, COALESCE(created_by,''), created_at
			FROM org_templates
			ORDER BY created_at DESC
			LIMIT 1
		`).Scan(&version, &createdBy, &createdAt); err != nil {
			return fmt.Errorf("load current template: %w", err)
		}
		fmt.Printf("current template\nversion: %s\ncreated_by: %s\ncreated_at: %s\n",
			version, nullable(createdBy, "-"), createdAt.UTC().Format(time.RFC3339))
		return nil
	case "diff":
		fs := flag.NewFlagSet("template diff", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		fromVersion := fs.String("from-version", "", "Source template version")
		toVersion := fs.String("to-version", "", "Target template version")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*fromVersion) == "" || strings.TrimSpace(*toVersion) == "" {
			return fmt.Errorf("--from-version and --to-version are required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("template diff requires persistent store mode (use -store postgres)")
		}
		if err := renderTemplateDiff(ctx, stores.SQLDB, strings.TrimSpace(*fromVersion), strings.TrimSpace(*toVersion)); err != nil {
			return err
		}
		return nil
	case "plan":
		fs := flag.NewFlagSet("template plan", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		toVersion := fs.String("to-version", "", "Target template version")
		requestedBy := fs.String("requested-by", "factory-cto", "Planner agent")
		limit := fs.Int("limit", 50, "Max verticals to plan")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*toVersion) == "" {
			return fmt.Errorf("--to-version is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := templateops.NewService(stores.SQLDB, stores.MailboxStore)
		n, err := svc.PlanMigrations(ctx, *toVersion, *requestedBy, *limit)
		if err != nil {
			return err
		}
		fmt.Printf("template migration plans created=%d to_version=%s\n", n, *toVersion)
		return nil
	case "apply":
		fs := flag.NewFlagSet("template apply", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		executedBy := fs.String("executed-by", "empire-coordinator", "Executor agent")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire template apply <migration-id>")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if err := applyTemplateMigrationWithPrimitives(ctx, cfg.LLM.RuntimeMode, stores, strings.TrimSpace(fs.Args()[0]), *executedBy); err != nil {
			return err
		}
		fmt.Printf("template migration applied id=%s\n", strings.TrimSpace(fs.Args()[0]))
		return nil
	default:
		return fmt.Errorf("unknown template command: %s", args[0])
	}
}

func runAgentSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire agent prompt <agent_id> [--edit|--set-from <file>|--revert|--diff]")
	}
	switch strings.TrimSpace(args[0]) {
	case "prompt":
		fs := flag.NewFlagSet("agent prompt", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		edit := fs.Bool("edit", false, "Open $EDITOR with current prompt content")
		revert := fs.Bool("revert", false, "Delete prompt override for this agent")
		diff := fs.Bool("diff", false, "Show override vs template prompt")
		setFrom := fs.String("set-from", "", "Set override prompt from file")
		source := fs.String("source", "cli", "Override source label")
		notes := fs.String("notes", "", "Optional override notes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if len(fs.Args()) < 1 {
			return fmt.Errorf("usage: empire agent prompt <agent_id> [--edit|--set-from <file>|--revert|--diff]")
		}
		agentID := strings.TrimSpace(fs.Args()[0])
		if agentID == "" {
			return fmt.Errorf("agent_id is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		if stores.SQLDB == nil {
			return fmt.Errorf("agent prompt requires postgres db")
		}
		templatePrompt, overridePrompt, hasOverride, err := loadAgentPromptState(ctx, stores.SQLDB, agentID)
		if err != nil {
			return err
		}
		if *revert {
			if _, err := stores.SQLDB.ExecContext(ctx, `DELETE FROM prompt_overrides WHERE agent_id = $1`, agentID); err != nil {
				return fmt.Errorf("revert prompt override: %w", err)
			}
			fmt.Printf("prompt override reverted agent_id=%s\n", agentID)
			fmt.Println("note: restart/reconfigure the agent for immediate effect")
			return nil
		}
		if *diff {
			for _, line := range renderPromptDiffCLI(templatePrompt, overridePrompt) {
				fmt.Println(line)
			}
			return nil
		}

		newPrompt := ""
		switch {
		case strings.TrimSpace(*setFrom) != "":
			b, err := os.ReadFile(strings.TrimSpace(*setFrom))
			if err != nil {
				return fmt.Errorf("read --set-from file: %w", err)
			}
			newPrompt = strings.TrimSpace(string(b))
		case *edit:
			seed := templatePrompt
			if hasOverride {
				seed = overridePrompt
			}
			p, err := editPromptInEditor(seed)
			if err != nil {
				return err
			}
			newPrompt = strings.TrimSpace(p)
		}
		if newPrompt != "" {
			prev := strings.TrimSpace(templatePrompt)
			if hasOverride {
				prev = strings.TrimSpace(overridePrompt)
			}
			if _, err := stores.SQLDB.ExecContext(ctx, `
				INSERT INTO prompt_overrides (agent_id, prompt, previous_prompt, source, notes, created_at, updated_at)
				VALUES ($1, $2, NULLIF($3,''), $4, NULLIF($5,''), now(), now())
				ON CONFLICT (agent_id) DO UPDATE SET
					prompt = EXCLUDED.prompt,
					previous_prompt = EXCLUDED.previous_prompt,
					source = EXCLUDED.source,
					notes = EXCLUDED.notes,
					updated_at = now()
			`, agentID, newPrompt, prev, strings.TrimSpace(*source), strings.TrimSpace(*notes)); err != nil {
				return fmt.Errorf("set prompt override: %w", err)
			}
			fmt.Printf("prompt override set agent_id=%s\n", agentID)
			fmt.Println("note: restart/reconfigure the agent for immediate effect")
			return nil
		}

		fmt.Printf("agent prompt\nagent_id: %s\nhas_override: %t\n", agentID, hasOverride)
		fmt.Println("effective_prompt:")
		if hasOverride {
			fmt.Println(overridePrompt)
		} else {
			fmt.Println(templatePrompt)
		}
		return nil
	default:
		return fmt.Errorf("unknown agent subcommand: %s", args[0])
	}
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

func runOpsSubcommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: empire ops <tick|record-metrics> [flags]")
	}
	switch args[0] {
	case "tick":
		fs := flag.NewFlagSet("ops tick", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := ops.NewService(stores.SQLDB, stores.MailboxStore)
		sum, err := svc.Tick(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("ops tick complete kill_candidates=%d budget_alerts=%d routing_proposals=%d\n",
			sum.KillCandidates, sum.BudgetAlerts, sum.RoutingProposals)
		return nil
	case "record-metrics":
		fs := flag.NewFlagSet("ops record-metrics", flag.ContinueOnError)
		cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
		storeMode := fs.String("store", "postgres", "Storage mode")
		migrate := fs.Bool("migrate", false, "Apply migrations")
		migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
		verticalID := fs.String("vertical-id", "", "Vertical UUID")
		usersTotal := fs.Int("users-total", 0, "Total users")
		usersNew := fs.Int("users-new", 0, "New users")
		usersChurned := fs.Int("users-churned", 0, "Churned users")
		mrr := fs.Int("mrr-cents", 0, "MRR in cents")
		apiCost := fs.Int("api-cost-cents", 0, "API cost in cents")
		infraCost := fs.Int("infra-cost-cents", 0, "Infra cost in cents")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*verticalID) == "" {
			return fmt.Errorf("--vertical-id is required")
		}
		ctx := context.Background()
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
		svc := ops.NewService(stores.SQLDB, stores.MailboxStore)
		now := time.Now().UTC()
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		err = svc.RecordMetrics(ctx, ops.MetricInput{
			VerticalID:     strings.TrimSpace(*verticalID),
			PeriodStart:    start,
			PeriodEnd:      start.Add(24 * time.Hour),
			UsersTotal:     *usersTotal,
			UsersNew:       *usersNew,
			UsersChurned:   *usersChurned,
			MRRCents:       *mrr,
			APICostCents:   *apiCost,
			InfraCostCents: *infraCost,
		})
		if err != nil {
			return err
		}
		fmt.Printf("metrics recorded vertical_id=%s\n", strings.TrimSpace(*verticalID))
		return nil
	default:
		return fmt.Errorf("unknown ops command: %s", args[0])
	}
}

func runDirectiveSubcommand(args []string) error {
	fs := flag.NewFlagSet("directive", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	if err := syncRuntimeGlobalAgents(ctx, stores.ManagerStore); err != nil {
		log.Printf("directive command global agents sync failed (continuing): %v", err)
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire directive \"message\"  (or legacy: empire directive <target> \"message\")")
	}

	// Spec v2.0 default: directives always go to Empire Coordinator as system.directive.
	targetRaw := "empire-coordinator"
	msgArgs := fs.Args()
	if len(msgArgs) >= 2 {
		// Legacy: empire directive <target> "message"
		targetRaw = msgArgs[0]
		msgArgs = msgArgs[1:]
	}
	msg := strings.TrimSpace(strings.Join(msgArgs, " "))
	if msg == "" {
		return fmt.Errorf("directive message is required")
	}

	target, err := resolveTargetAgent(ctx, stores, targetRaw)
	if err != nil {
		return err
	}
	if err := ensureTargetAgentRegistered(ctx, stores, target); err != nil {
		return err
	}
	if err := requireSystemStarted(ctx, stores.SQLDB); err != nil {
		return err
	}

	eventID, err := dispatchSystemDirective(ctx, stores, target, msg)
	if err != nil {
		return err
	}
	fmt.Printf("directive queued event=%s target=%s\n", eventID, target.ID)
	return nil
}

func runChatSubcommand(args []string) error {
	fs := flag.NewFlagSet("chat", flag.ContinueOnError)
	cfgPath := fs.String("config", "configs/empire.yaml", "Path to empire config")
	storeMode := fs.String("store", "postgres", "Storage mode")
	migrate := fs.Bool("migrate", false, "Apply migrations")
	migrationFile := fs.String("migration-file", defaultMigrationFilePath, "Migration file path")
	async := fs.Bool("async", false, "Queue messages as events instead of live chat response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) < 1 {
		return fmt.Errorf("usage: empire chat <vertical/agent|agent-id> [initial message]")
	}

	ctx := context.Background()
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	stores := buildStores(ctx, *storeMode, cfg, *migrate, *migrationFile)
	target, err := resolveTargetAgent(ctx, stores, fs.Args()[0])
	if err != nil {
		return err
	}
	if err := ensureChatTargetAgentRegistered(ctx, stores, target); err != nil {
		return err
	}

	if *async {
		// One-shot async mode: empire chat <target> "message" --async
		if len(fs.Args()) > 1 {
			msg := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
			eventID, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), msg)
			if err != nil {
				return err
			}
			fmt.Printf("chat message queued event=%s target=%s\n", eventID, target.ID)
			return nil
		}
		fmt.Printf("chat session target=%s (async queue mode). Type /exit to finish.\n", target.ID)
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				break
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if line == "/exit" || line == "/quit" {
				break
			}
			eventID, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), line)
			if err != nil {
				return err
			}
			fmt.Printf("queued event=%s\n", eventID)
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return nil
	}

	workspaceLifecycle := buildWorkspaceLifecycle(ctx, stores.SQLDB)
	llm, err := runtime.RuntimeFactory{
		Cfg:           cfg,
		Sessions:      stores.SessionRegistry,
		Turns:         stores.TurnStore,
		Conversations: stores.ConversationStore,
		Workspaces:    workspaceLifecycle,
	}.Build()
	if err != nil {
		return err
	}
	bus := runtime.NewEventBus(stores.EventStore)
	var budgetTracker *runtime.BudgetTracker
	if stores.SQLDB != nil {
		budgetTracker = runtime.NewBudgetTracker(stores.SQLDB, bus, cfg, stores.MailboxStore)
	}
	scheduler := runtime.NewScheduler(func(runtime.Schedule) {})
	defer scheduler.Stop()
	toolExecutor := runtimetools.NewExecutor(bus, scheduler, nil, stores.ScheduleStore)
	toolExecutor.SetConfig(cfg)
	toolExecutor.SetMailboxStore(stores.MailboxStore)
	toolExecutor.SetSQLDB(stores.SQLDB)
	factory := runtimeagents.NewLLMAgentFactory(llm, toolExecutor, toolExecutor.ToolDefinitions())
	manager := runtimemanager.NewAgentManager(bus, factory, stores.ManagerStore)
	manager.SetWorkspaceLifecycle(workspaceLifecycle)
	manager.SetSessionRegistry(stores.SessionRegistry, cfg.LLM.RuntimeMode)
	manager.SetBudgetTracker(budgetTracker)
	toolExecutor.SetManager(manager)
	if err := syncRuntimeGlobalAgents(ctx, stores.ManagerStore); err != nil {
		log.Printf("chat command global agents sync failed (continuing): %v", err)
	}
	if err := manager.Recover(ctx); err != nil {
		return fmt.Errorf("recover manager for chat: %w", err)
	}

	// One-shot live mode: empire chat <target> "message"
	if len(fs.Args()) > 1 {
		msg := strings.TrimSpace(strings.Join(fs.Args()[1:], " "))
		if _, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), msg); err != nil {
			return err
		}
		resp, err := manager.ChatWithAgent(ctx, target.ID, msg)
		if err != nil {
			return err
		}
		fmt.Println(strings.TrimSpace(resp))
		return nil
	}

	fmt.Printf("chat session target=%s (live). Type /exit to finish.\n", target.ID)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "/exit" || line == "/quit" {
			break
		}
		if _, err := dispatchBoardMessage(ctx, stores, target, events.EventType("board.chat"), line); err != nil {
			return err
		}
		resp, err := manager.ChatWithAgent(ctx, target.ID, line)
		if err != nil {
			return err
		}
		fmt.Println(strings.TrimSpace(resp))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

type targetAgent struct {
	ID          string
	Role        string
	VerticalID  string
	VerticalKey string
	Config      models.AgentConfig
}

func resolveTargetAgent(ctx context.Context, stores storeBundle, raw string) (targetAgent, error) {
	if stores.ManagerStore == nil {
		return targetAgent{}, fmt.Errorf("target resolution requires persistent store mode (use -store postgres)")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return targetAgent{}, fmt.Errorf("target is required")
	}
	agents, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return targetAgent{}, fmt.Errorf("load agents: %w", err)
	}
	if len(agents) == 0 {
		return resolveTargetFallback(raw)
	}

	if strings.Contains(raw, "/") {
		parts := strings.SplitN(raw, "/", 2)
		verticalID := strings.TrimSpace(parts[0])
		role := normalizeAgentAlias(parts[1])
		if verticalID == "" || role == "" {
			return targetAgent{}, fmt.Errorf("invalid target: %s", raw)
		}
		candidateID := fmt.Sprintf("%s-%s", role, verticalID)
		for _, rec := range agents {
			if rec.Config.ID == candidateID {
				return targetAgent{
					ID:          rec.Config.ID,
					Role:        rec.Config.Role,
					VerticalID:  rec.Config.VerticalID,
					VerticalKey: rec.Config.VerticalID,
					Config:      rec.Config,
				}, nil
			}
		}
		for _, rec := range agents {
			if rec.Config.VerticalID == verticalID && rec.Config.Role == role {
				return targetAgent{
					ID:          rec.Config.ID,
					Role:        rec.Config.Role,
					VerticalID:  rec.Config.VerticalID,
					VerticalKey: rec.Config.VerticalID,
					Config:      rec.Config,
				}, nil
			}
		}
		return targetAgent{}, fmt.Errorf("agent target not found: %s", raw)
	}

	alias := normalizeAgentAlias(raw)
	for _, rec := range agents {
		if rec.Config.ID == raw {
			return targetAgent{
				ID:          rec.Config.ID,
				Role:        rec.Config.Role,
				VerticalID:  rec.Config.VerticalID,
				VerticalKey: rec.Config.VerticalID,
				Config:      rec.Config,
			}, nil
		}
	}
	matches := make([]targetAgent, 0)
	for _, rec := range agents {
		if rec.Config.Role == alias {
			matches = append(matches, targetAgent{
				ID:          rec.Config.ID,
				Role:        rec.Config.Role,
				VerticalID:  rec.Config.VerticalID,
				VerticalKey: rec.Config.VerticalID,
				Config:      rec.Config,
			})
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return targetAgent{}, fmt.Errorf("ambiguous target %q: use <vertical>/<agent> or full agent id", raw)
	}
	if t, err := resolveTargetFallback(raw); err == nil {
		return t, nil
	}
	return targetAgent{}, fmt.Errorf("agent target not found: %s", raw)
}

func resolveTargetFallback(raw string) (targetAgent, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return targetAgent{}, fmt.Errorf("target is required")
	}
	if strings.Contains(raw, "/") {
		parts := strings.SplitN(raw, "/", 2)
		verticalKey := strings.TrimSpace(parts[0])
		role := normalizeAgentAlias(parts[1])
		if verticalKey == "" || role == "" {
			return targetAgent{}, fmt.Errorf("invalid target: %s", raw)
		}
		verticalID := ""
		if isUUID(verticalKey) {
			verticalID = verticalKey
		}
		id := fmt.Sprintf("%s-%s", role, verticalKey)
		cfg := models.AgentConfig{
			ID:         id,
			Role:       role,
			Mode:       "operating",
			VerticalID: verticalID,
		}
		return targetAgent{
			ID:          id,
			Role:        role,
			VerticalID:  verticalID,
			VerticalKey: verticalKey,
			Config:      cfg,
		}, nil
	}
	role := normalizeAgentAlias(raw)
	cfg := models.AgentConfig{
		ID:   raw,
		Role: role,
		Mode: "factory",
	}
	return targetAgent{ID: raw, Role: role, VerticalID: "", VerticalKey: "", Config: cfg}, nil
}

func ensureTargetAgentRegistered(ctx context.Context, stores storeBundle, target targetAgent) error {
	if stores.ManagerStore == nil {
		return nil
	}
	loaded, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	for _, rec := range loaded {
		if strings.TrimSpace(rec.Config.ID) == strings.TrimSpace(target.ID) {
			return nil
		}
	}
	if !hasSystemPrompt(target.Config) {
		return fmt.Errorf("target agent %q is not registered with a valid system_prompt; run `empire init` or seed org before sending directives", target.ID)
	}
	return stores.ManagerStore.UpsertAgent(ctx, runtime.PersistedAgent{
		Config:          target.Config,
		Status:          "active",
		HiredBy:         "human-interface",
		TemplateVersion: "2.0.15",
	})
}

func ensureChatTargetAgentRegistered(ctx context.Context, stores storeBundle, target targetAgent) error {
	if stores.ManagerStore == nil {
		return nil
	}
	loaded, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	for _, rec := range loaded {
		if strings.TrimSpace(rec.Config.ID) == strings.TrimSpace(target.ID) {
			return nil
		}
	}
	cfg := target.Config
	if !hasSystemPrompt(cfg) {
		cfg.ID = strings.TrimSpace(target.ID)
		cfg.Role = strings.TrimSpace(coalesce(cfg.Role, target.Role))
		if strings.TrimSpace(cfg.Mode) == "" {
			cfg.Mode = "operating"
		}
		if strings.TrimSpace(cfg.Type) == "" {
			cfg.Type = "sonnet"
		}
		cfg.Config = mustJSON(map[string]any{
			"system_prompt": "You are a temporary chat endpoint for a not-yet-bootstrapped agent. Acknowledge board input briefly and request full bootstrap context before acting.",
			"tools":         []string{},
			"subscriptions": []string{"board.chat", "board.directive"},
		})
		cfg.Subscriptions = []string{"board.chat", "board.directive"}
	}
	return stores.ManagerStore.UpsertAgent(ctx, runtime.PersistedAgent{
		Config:          cfg,
		Status:          "active",
		HiredBy:         "human-interface",
		TemplateVersion: "2.0.15",
	})
}

func hasSystemPrompt(cfg models.AgentConfig) bool {
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return false
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v) != ""
}

func requireSystemStarted(ctx context.Context, db *sql.DB) error {
	ok, err := hasSystemStarted(ctx, db)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("system is not initialized yet (missing system.started): run `empire init` first")
	}
	return nil
}

func hasSystemStarted(ctx context.Context, db *sql.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("directive requires postgres store mode")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE type = 'system.started')`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check system.started: %w", err)
	}
	return exists, nil
}

func syncRuntimeGlobalAgents(ctx context.Context, managerStore runtime.ManagerPersistence) error {
	if managerStore == nil {
		return nil
	}
	agentsDir := strings.TrimSpace(os.Getenv("EMPIREAI_GLOBAL_AGENTS_DIR"))
	if agentsDir == "" {
		agentsDir = "configs/agents"
	}
	rosterPath := filepath.Join(agentsDir, "roster.yaml")
	if _, err := os.Stat(rosterPath); err != nil {
		// Best effort; test harnesses and stripped runtime images may not ship YAML authoring files.
		return nil
	}
	return seedGlobalAgentsFromYAML(ctx, managerStore, agentsDir)
}

func rotateGlobalAgentSessions(ctx context.Context, managerStore runtime.ManagerPersistence, sessions sessions.Registry, runtimeMode string) error {
	if managerStore == nil || sessions == nil {
		return nil
	}
	runtimeMode = strings.TrimSpace(runtimeMode)
	if runtimeMode == "" {
		return nil
	}
	agents, err := managerStore.LoadAgents(ctx)
	if err != nil {
		return err
	}
	for _, rec := range agents {
		agentID := strings.TrimSpace(rec.Config.ID)
		if agentID == "" || strings.TrimSpace(rec.Config.VerticalID) != "" {
			continue
		}
		if _, err := sessions.Rotate(ctx, agentID, runtimeMode, "runtime-sync", "global config sync", ""); err != nil {
			errText := strings.ToLower(strings.TrimSpace(err.Error()))
			if strings.Contains(errText, "no active session to rotate") {
				continue
			}
			return fmt.Errorf("rotate global session agent=%s: %w", agentID, err)
		}
	}
	return nil
}

func isUUID(v string) bool {
	_, err := uuid.Parse(strings.TrimSpace(v))
	return err == nil
}

func normalizeAgentAlias(v string) string {
	a := strings.ToLower(strings.TrimSpace(v))
	switch a {
	case "ceo":
		return "opco-ceo"
	case "hop", "head-of-product":
		return "vp-product"
	case "hog", "head-of-growth":
		return "vp-growth"
	case "cto":
		return "cto-agent"
	case "pm":
		return "pm-agent"
	case "support":
		return "support-agent"
	case "marketing":
		return "marketing-agent"
	case "backend":
		return "backend-agent"
	case "frontend":
		return "frontend-agent"
	case "qa":
		return "qa-agent"
	case "devops":
		return "devops-agent"
	case "cos", "chief-of-staff":
		return "chief-of-staff"
	default:
		return a
	}
}

func dispatchBoardMessage(ctx context.Context, stores storeBundle, target targetAgent, eventType events.EventType, message string) (string, error) {
	if stores.EventStore == nil {
		return "", fmt.Errorf("directive/chat requires persistent store mode (use -store postgres)")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("message is required")
	}

	payload, _ := json.Marshal(map[string]any{
		"target_agent_id": target.ID,
		"role":            target.Role,
		"vertical_id":     target.VerticalID,
		"vertical_key":    target.VerticalKey,
		"message":         msg,
		"sent_by":         "human-board",
		"sent_at":         time.Now().UTC().Format(time.RFC3339),
	})
	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        eventType,
		SourceAgent: "human-board",
		VerticalID:  target.VerticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := stores.EventStore.AppendEvent(ctx, evt); err != nil {
		return "", err
	}
	if err := stores.EventStore.InsertEventDeliveries(ctx, eventID, []string{target.ID}); err != nil {
		return "", err
	}
	return eventID, nil
}

func dispatchSystemDirective(ctx context.Context, stores storeBundle, target targetAgent, message string) (string, error) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("message is required")
	}
	eventID, attempted, err := dispatchSystemDirectiveViaDashboard(ctx, target, msg)
	if err == nil {
		return eventID, nil
	}

	// Fallback path: when dashboard/runtime control plane is unavailable,
	// enqueue system.directive directly in the event store so runtime recovery/
	// replay can process it once the orchestrator is up.
	fallbackEventID, fallbackErr := dispatchSystemDirectiveDirect(ctx, stores, target, msg)
	if fallbackErr == nil {
		return fallbackEventID, nil
	}

	if !attempted {
		return "", fmt.Errorf("directive dispatch unavailable: dashboard control endpoint not attempted")
	}
	return "", fmt.Errorf("directive dispatch requires runtime interceptor (/api/directive): %w", err)
}

func dispatchSystemDirectiveDirect(ctx context.Context, stores storeBundle, target targetAgent, message string) (string, error) {
	if stores.EventStore == nil {
		return "", fmt.Errorf("directive dispatch unavailable: event store is not configured")
	}
	payload, _ := json.Marshal(map[string]any{
		"directive_text": strings.TrimSpace(message),
		"sent_by":        "cli",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	})
	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human-board",
		VerticalID:  strings.TrimSpace(target.VerticalID),
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := stores.EventStore.AppendEvent(ctx, evt); err != nil {
		return "", fmt.Errorf("append system.directive event: %w", err)
	}
	if err := stores.EventStore.InsertEventDeliveries(ctx, eventID, []string{strings.TrimSpace(target.ID)}); err != nil {
		return "", fmt.Errorf("insert directive delivery: %w", err)
	}
	return eventID, nil
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
	EventStore        runtime.EventStore
	SessionRegistry   sessions.Registry
	ConversationStore runtime.ConversationPersistence
	ManagerStore      runtime.ManagerPersistence
	ScheduleStore     runtime.SchedulePersistence
	MailboxStore      runtime.MailboxPersistence
	InboundStore      runtime.InboundPersistence
	DigestStore       runtime.DigestPersistence
	TurnStore         runtime.TurnPersistence
	ScanCampaignStore runtime.ScanCampaignPersistence
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
			EventStore:      runtime.InMemoryEventStore{},
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

func loadAgentPromptState(ctx context.Context, db *sql.DB, agentID string) (templatePrompt string, overridePrompt string, hasOverride bool, err error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", "", false, fmt.Errorf("agent_id is required")
	}
	var cfgRaw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT config
		FROM agents
		WHERE id = $1
		  AND status <> 'terminated'
	`, agentID).Scan(&cfgRaw); err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, fmt.Errorf("agent not found: %s", agentID)
		}
		return "", "", false, fmt.Errorf("load agent config: %w", err)
	}
	templatePrompt = extractSystemPromptFromConfigCLI(cfgRaw)
	err = db.QueryRowContext(ctx, `
		SELECT prompt
		FROM prompt_overrides
		WHERE agent_id = $1
	`, agentID).Scan(&overridePrompt)
	if err != nil {
		if err == sql.ErrNoRows {
			return templatePrompt, "", false, nil
		}
		return "", "", false, fmt.Errorf("load prompt override: %w", err)
	}
	return templatePrompt, strings.TrimSpace(overridePrompt), true, nil
}

func extractSystemPromptFromConfigCLI(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v)
}

func renderPromptDiffCLI(templatePrompt, overridePrompt string) []string {
	templatePrompt = strings.TrimSpace(templatePrompt)
	overridePrompt = strings.TrimSpace(overridePrompt)
	if templatePrompt == overridePrompt {
		return []string{"(no diff)"}
	}
	left := strings.Split(templatePrompt, "\n")
	right := strings.Split(overridePrompt, "\n")
	if len(left) == 1 && left[0] == "" {
		left = nil
	}
	if len(right) == 1 && right[0] == "" {
		right = nil
	}
	max := len(left)
	if len(right) > max {
		max = len(right)
	}
	out := make([]string, 0, max*2)
	for i := 0; i < max; i++ {
		lv := ""
		rv := ""
		if i < len(left) {
			lv = left[i]
		}
		if i < len(right) {
			rv = right[i]
		}
		if lv == rv {
			continue
		}
		if lv != "" {
			out = append(out, "- "+lv)
		}
		if rv != "" {
			out = append(out, "+ "+rv)
		}
	}
	if len(out) == 0 {
		return []string{"(no diff)"}
	}
	return out
}

func editPromptInEditor(initial string) (string, error) {
	editor := strings.TrimSpace(os.Getenv("EDITOR"))
	if editor == "" {
		editor = "vi"
	}
	f, err := os.CreateTemp("", "empire-prompt-*.txt")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	path := f.Name()
	defer os.Remove(path)
	if _, err := f.WriteString(initial); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write temp file: %w", err)
	}
	_ = f.Close()
	cmd := exec.Command(editor, path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("open editor %q: %w", editor, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read edited prompt: %w", err)
	}
	return string(b), nil
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

type operatorOptions struct {
	mailboxStatus       bool
	mailboxList         bool
	mailboxListCritical bool
	mailboxListReviews  bool
	mailboxLimit        int
	mailboxViewID       string
	mailboxDecideID     string
	mailboxDecision     string
	mailboxNotes        string
	digestGenerate      bool
	digestTopN          int
}

func hasOperatorAction(flags ...bool) bool {
	for _, f := range flags {
		if f {
			return true
		}
	}
	return false
}

func runOperatorActions(ctx context.Context, stores storeBundle, opts operatorOptions) error {
	if opts.mailboxStatus || opts.mailboxList || opts.mailboxViewID != "" || opts.mailboxDecideID != "" {
		if stores.MailboxStore == nil {
			return fmt.Errorf("mailbox commands require persistent store mode (use -store postgres)")
		}
	}
	if opts.digestGenerate {
		if stores.MailboxStore == nil || stores.DigestStore == nil {
			return fmt.Errorf("digest command requires persistent store mode (use -store postgres)")
		}
	}

	if (opts.mailboxListCritical || opts.mailboxListReviews) && !opts.mailboxList {
		return fmt.Errorf("-mailbox-list-critical and -mailbox-list-reviews require -mailbox-list")
	}

	if opts.mailboxStatus {
		if err := mailbox.PrintStatus(ctx, stores.MailboxStore, os.Stdout); err != nil {
			return err
		}
	}
	if opts.mailboxList {
		if err := mailbox.PrintPendingWithOptions(ctx, stores.MailboxStore, os.Stdout, mailbox.ListOptions{
			Limit:        opts.mailboxLimit,
			CriticalOnly: opts.mailboxListCritical,
			ReviewsOnly:  opts.mailboxListReviews,
		}); err != nil {
			return err
		}
	}
	if opts.mailboxViewID != "" {
		if err := mailbox.PrintItem(ctx, stores.MailboxStore, os.Stdout, opts.mailboxViewID); err != nil {
			return err
		}
	}
	if opts.mailboxDecideID != "" {
		if opts.mailboxDecision == "" {
			return fmt.Errorf("-mailbox-decision is required with -mailbox-decide-id")
		}
		item, err := stores.MailboxStore.GetMailboxItem(ctx, opts.mailboxDecideID)
		if err != nil {
			return err
		}
		outcome, err := mailbox.Decide(ctx, stores.MailboxStore, opts.mailboxDecideID, opts.mailboxDecision, opts.mailboxNotes)
		if err != nil {
			return err
		}
		if err := emitMailboxDecisionSideEffects(ctx, stores, item, outcome, opts.mailboxNotes); err != nil {
			return err
		}
		fmt.Printf("mailbox: decided id=%s status=%s decision=%s\n", opts.mailboxDecideID, outcome.Status, outcome.Decision)
	}
	if opts.digestGenerate {
		snap, err := digest.BuildSnapshot(ctx, stores.DigestStore, stores.MailboxStore, opts.digestTopN)
		if err != nil {
			return err
		}
		fmt.Println(digest.RenderText(snap))
	}
	return nil
}

func emitMailboxDecisionSideEffects(
	ctx context.Context,
	stores storeBundle,
	item runtime.MailboxItem,
	outcome mailbox.DecisionOutcome,
	notes string,
) error {
	if stores.EventStore == nil {
		return nil
	}
	basePayload := map[string]any{
		"mailbox_id": item.ID,
		"type":       item.Type,
		"status":     outcome.Status,
		"decision":   outcome.Decision,
		"notes":      notes,
		"context":    json.RawMessage(item.Context),
	}

	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.decision"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   time.Now(),
	}, nil); err != nil {
		return err
	}
	if err := appendTargetedEvent(ctx, stores, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("mailbox.item_decided"),
		SourceAgent: "mailbox",
		VerticalID:  item.VerticalID,
		Payload:     mustJSON(basePayload),
		CreatedAt:   time.Now(),
	}, nil); err != nil {
		return err
	}

	if outcome.Status == "more_data" && item.VerticalID != "" {
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.needs_more_data"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   time.Now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}

	if item.Type == "vertical_approval" && item.VerticalID != "" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("vertical.approved")
		case "rejected":
			evtType = events.EventType("vertical.killed")
		}
		if evtType != "" {
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   time.Now(),
			}, []string{"empire-coordinator"}); err != nil {
				return err
			}
		}
	}

	if item.Type == "template_migration" {
		if outcome.Status == "approved" {
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("template.migration_approved"),
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   time.Now(),
			}, []string{"empire-coordinator"}); err != nil {
				return err
			}
		}
	}

	if item.Type == "spend_request" || item.Type == "budget_increase" || item.Type == "devops.capacity_warning" {
		var evtType events.EventType
		switch outcome.Status {
		case "approved":
			evtType = events.EventType("spend.approved")
		case "rejected":
			evtType = events.EventType("spend.rejected")
		}
		if evtType != "" {
			recipients := []string{}
			if item.VerticalID != "" {
				recipients = []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}
			} else if strings.TrimSpace(item.FromAgent) != "" {
				// Holding-side spend decisions (e.g., capacity warnings) route back to the requester.
				recipients = []string{strings.TrimSpace(item.FromAgent)}
			}
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload:     mustJSON(basePayload),
				CreatedAt:   time.Now(),
			}, recipients); err != nil {
				return err
			}
		}
	}

	if isFounderInputMailbox(item) && item.VerticalID != "" {
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("founder_input.response"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload:     mustJSON(basePayload),
			CreatedAt:   time.Now(),
		}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
			return err
		}
	}

	// Spec v2.0 GAP 1: escalation responses are open-ended directives back to the OpCo CEO.
	if item.VerticalID != "" && strings.Contains(strings.ToLower(item.Type), "escalation") && outcome.Status == "approved" {
		directive := strings.TrimSpace(notes)
		if directive != "" {
			if err := appendTargetedEvent(ctx, stores, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("opco.escalation_response"),
				SourceAgent: "mailbox",
				VerticalID:  item.VerticalID,
				Payload: mustJSON(map[string]any{
					"mailbox_id":     item.ID,
					"directive_text": directive,
					"action_items":   []any{},
					"context":        json.RawMessage(item.Context),
				}),
				CreatedAt: time.Now(),
			}, []string{fmt.Sprintf("opco-ceo-%s", item.VerticalID)}); err != nil {
				return err
			}
		}
	}

	// Spec v2.0 §7.6: approved geography expansion recommendations must trigger
	// lightweight validation for the new geography.
	if outcome.Status == "approved" && isGeographyExpansionMailbox(item) {
		geoID, req, campaignID, err := queueGeographyExpansionValidation(ctx, stores.SQLDB, stores.ScanCampaignStore, item)
		if err != nil {
			return err
		}
		if err := appendTargetedEvent(ctx, stores, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("geography.expansion_queued"),
			SourceAgent: "mailbox",
			VerticalID:  item.VerticalID,
			Payload: mustJSON(map[string]any{
				"mailbox_id":   item.ID,
				"vertical_id":  item.VerticalID,
				"geography_id": geoID,
				"geography":    req.Geography,
				"country":      req.Country,
				"region":       req.Region,
				"mode":         req.Mode,
				"categories":   req.Categories,
				"priority":     req.Priority,
				"campaign_id":  campaignID,
				"context":      json.RawMessage(item.Context),
			}),
			CreatedAt: time.Now(),
		}, []string{"empire-coordinator"}); err != nil {
			return err
		}
	}

	return nil
}

type geographyExpansionRequest struct {
	Geography  string
	Country    string
	Region     string
	Mode       string
	Categories []string
	Priority   string
}

func mailboxReviewType(raw json.RawMessage) string {
	var obj map[string]any
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	for _, key := range []string{"review_type", "kind", "mailbox_type", "subtype"} {
		val := strings.ToLower(strings.TrimSpace(asString(obj[key])))
		if val != "" {
			return val
		}
	}
	return ""
}

func isGeographyExpansionMailbox(item runtime.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "" {
		return false
	}
	switch t {
	case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
		return true
	}
	if strings.Contains(t, "geography") && strings.Contains(t, "expansion") {
		return true
	}
	if t == "review" {
		rt := mailboxReviewType(item.Context)
		switch rt {
		case "domain_approval", "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
			return true
		}
	}
	return false
}

func isFounderInputMailbox(item runtime.MailboxItem) bool {
	t := strings.ToLower(strings.TrimSpace(item.Type))
	if t == "founder_input" {
		return true
	}
	return t == "review" && mailboxReviewType(item.Context) == "founder_input"
}

func queueGeographyExpansionValidation(
	ctx context.Context,
	db *sql.DB,
	scanStore runtime.ScanCampaignPersistence,
	item runtime.MailboxItem,
) (string, geographyExpansionRequest, string, error) {
	req := parseGeographyExpansionRequest(item.Context)
	if strings.TrimSpace(req.Geography) == "" {
		return "", req, "", fmt.Errorf("geography expansion requires context.geography")
	}
	if db == nil {
		return "", req, "", fmt.Errorf("geography expansion requires postgres db")
	}
	geoID, err := ensureGeographyRecord(ctx, db, req)
	if err != nil {
		return "", req, "", err
	}
	if scanStore == nil {
		return geoID, req, "", fmt.Errorf("scan campaign store is unavailable")
	}
	campaign, err := scanStore.CreateScanCampaign(ctx, runtime.CreateScanCampaignInput{
		GeographyID: geoID,
		Mode:        req.Mode,
		Categories:  req.Categories,
		Priority:    req.Priority,
		Status:      "queued",
	})
	if err != nil {
		return "", req, "", fmt.Errorf("queue geography expansion scan campaign: %w", err)
	}
	return geoID, req, campaign.ID, nil
}

func parseGeographyExpansionRequest(raw json.RawMessage) geographyExpansionRequest {
	out := geographyExpansionRequest{
		Mode:     "local_services",
		Priority: "normal",
	}
	var obj map[string]any
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	lookup := func(keys ...string) string {
		for _, k := range keys {
			if obj == nil {
				continue
			}
			if v := strings.TrimSpace(asString(obj[k])); v != "" && v != "null" {
				return v
			}
		}
		return ""
	}
	out.Geography = lookup("geography", "target_geography", "geography_name")
	out.Country = lookup("country", "country_code")
	out.Region = lookup("region")
	if mode := strings.ToLower(lookup("mode")); mode != "" {
		out.Mode = mode
	}
	if priority := strings.ToLower(lookup("priority")); priority != "" {
		out.Priority = priority
	}
	if cats := parseStringList(anyFrom(obj, "categories", "taxonomy_categories")); len(cats) > 0 {
		out.Categories = cats
	}
	if out.Country == "" && strings.Contains(out.Geography, ",") {
		parts := strings.Split(out.Geography, ",")
		out.Country = strings.TrimSpace(parts[len(parts)-1])
	}
	if out.Country == "" {
		out.Country = "unspecified"
	}
	return out
}

func ensureGeographyRecord(ctx context.Context, db *sql.DB, req geographyExpansionRequest) (string, error) {
	if db == nil {
		return "", fmt.Errorf("postgres db is required")
	}
	name := strings.TrimSpace(req.Geography)
	country := strings.TrimSpace(req.Country)
	region := strings.TrimSpace(req.Region)

	var id string
	err := db.QueryRowContext(ctx, `
		SELECT id::text
		FROM geographies
		WHERE lower(name) = lower($1)
		  AND ($2 = '' OR lower(country) = lower($2))
		ORDER BY created_at DESC
		LIMIT 1
	`, name, country).Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("lookup geography %q: %w", name, err)
	}

	id = uuid.NewString()
	scanCfg := mustJSON(map[string]any{
		"source":      "mailbox.geography_expansion",
		"mode":        req.Mode,
		"categories":  req.Categories,
		"priority":    req.Priority,
		"geography":   name,
		"country":     country,
		"region":      region,
		"recorded_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,''), $5::jsonb, now())
	`, id, name, country, region, string(scanCfg)); err != nil {
		return "", fmt.Errorf("insert geography %q: %w", name, err)
	}
	return id, nil
}

func anyFrom(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if m == nil {
			continue
		}
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

func parseStringList(v any) []string {
	normalize := func(in []string) []string {
		seen := make(map[string]struct{}, len(in))
		out := make([]string, 0, len(in))
		for _, raw := range in {
			s := strings.TrimSpace(raw)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
		return out
	}
	switch t := v.(type) {
	case []string:
		return normalize(t)
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			s := strings.TrimSpace(asString(x))
			if s != "" && s != "null" {
				out = append(out, s)
			}
		}
		return normalize(out)
	case string:
		parts := strings.Split(t, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				out = append(out, s)
			}
		}
		return normalize(out)
	default:
		return nil
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

func mailboxTimeoutLoop(ctx context.Context, store runtime.MailboxPersistence) {
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

func mailboxCriticalNotifyLoop(ctx context.Context, store runtime.MailboxPersistence, notifier mailbox.CriticalNotifier, bus *runtime.EventBus) {
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
				payload, _ := json.Marshal(map[string]any{
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
	payload, _ := json.Marshal(map[string]string{"status": "ok"})
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
