package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	runtimeagents "empireai/internal/runtime/agents"
	runtimebus "empireai/internal/runtime/bus"
	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimemcp "empireai/internal/runtime/mcp"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	workspace "empireai/internal/runtime/workspace"
	"github.com/google/uuid"
)

type Stores struct {
	SQLDB             *sql.DB
	EventStore        runtimebus.EventStore
	SessionRegistry   sessions.Registry
	ConversationStore llm.ConversationPersistence
	ManagerStore      runtimemanager.ManagerPersistence
	ScheduleStore     runtimepipeline.SchedulePersistence
	MailboxStore      runtimetools.MailboxPersistence
	InboundStore      InboundPersistence
	DigestStore       DigestPersistence
	TurnStore         llm.TurnPersistence
	ScanCampaignStore runtimepipeline.ScanCampaignPersistence
}

type RuntimeOptions struct {
	SelfCheck          bool
	WorkspaceLifecycle workspace.Lifecycle
	EnableToolGateway  bool
	ToolGatewayToken   string
	WorkflowModule     runtimepipeline.WorkflowModule
	ProductPolicy      func() runtimeproductpolicy.Policy
}

type Runtime struct {
	Config          *config.Config
	Stores          Stores
	Options         RuntimeOptions
	Bus             *EventBus
	Logger          *RuntimeLogger
	Pipeline        *runtimepipeline.FactoryPipelineCoordinator
	SystemNodes     []runtimepipeline.BackgroundNode
	ScanCampaign    *runtimepipeline.ScanCampaignManager
	Scheduler       *runtimepipeline.Scheduler
	Workspace       workspace.Lifecycle
	Budget          *BudgetTracker
	LLM             llm.Runtime
	ToolExecutor    *runtimetools.Executor
	Manager         *runtimemanager.AgentManager
	InboundGateway  *InboundGateway
	ToolGateway     *runtimemcp.Gateway
	ShardDispatcher *runtimepipeline.ShardDispatcher
}

func cycleTrackerForDB(db *sql.DB) *runtimebus.OpCoCycleTracker {
	if db == nil {
		return nil
	}
	return runtimebus.NewOpCoCycleTracker(db)
}

func ensureWorkflowBootWiring(opts RuntimeOptions) error {
	if opts.WorkflowModule == nil {
		return fmt.Errorf("workflow module is required: configure RuntimeOptions.WorkflowModule")
	}
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return opts.WorkflowModule
	})
	return runtimepipeline.ValidateWorkflowContracts(opts.WorkflowModule.ContractBundle())
}

func ensureProductPolicyBootWiring(opts RuntimeOptions) error {
	if opts.ProductPolicy == nil {
		return fmt.Errorf("product policy is required: configure RuntimeOptions.ProductPolicy")
	}
	runtimeproductpolicy.SetDefaultFactory(opts.ProductPolicy)
	if runtimeproductpolicy.DefaultOrNil() == nil {
		return fmt.Errorf("product policy factory returned nil")
	}
	return nil
}

func NewRuntime(ctx context.Context, cfg *config.Config, stores Stores, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
		if runtimeEnvBool("EMPIREAI_EMIT_SCHEMA_STRICT", true) {
			return nil, fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries", len(generated))
		}
		sample := generated
		if len(sample) > 10 {
			sample = sample[:10]
		}
		log.Printf("emit schema hardening warning: %d agent-emitted event schemas are missing explicit definitions; add explicit schemas (sample: %s)", len(generated), strings.Join(sample, ", "))
	}
	if err := ensureWorkflowBootWiring(opts); err != nil {
		return nil, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	if err := ensureProductPolicyBootWiring(opts); err != nil {
		return nil, err
	}

	rt := &Runtime{
		Config:    cfg,
		Stores:    stores,
		Options:   opts,
		Workspace: opts.WorkspaceLifecycle,
	}

	if stores.SQLDB != nil {
		rt.Logger = NewRuntimeLogger(stores.SQLDB)
	}
	rt.Bus = NewEventBusWithOptions(stores.EventStore, rt.Logger, cycleTrackerForDB(stores.SQLDB), func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	})
	if stores.SQLDB != nil {
		rt.Pipeline = runtimepipeline.NewFactoryPipelineCoordinatorWithOptions(rt.Bus, stores.SQLDB, runtimepipeline.FactoryPipelineCoordinatorOptions{
			ShardPlanner: runtimepipeline.NewShardPlanner(cfg.Sharding()),
			Module:       opts.WorkflowModule,
		})
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)...)
		}
	}

	if stores.ScanCampaignStore != nil {
		hooks := runtimepipeline.ScanCampaignHooks{
			Warnf: func(component, format string, args ...any) {
				log.Printf("runtime.warn component=%s message=%s", strings.TrimSpace(component), fmt.Sprintf(format, args...))
			},
			RecordTransition: func(ctx context.Context, db *sql.DB, in runtimepipeline.ScanCampaignTransitionInput) error {
				return RecordPipelineTransition(ctx, db, PipelineTransitionInput{
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
			ControlPlaneRecipient: func() string {
				return strings.TrimSpace(runtimeproductpolicy.ControlPlaneAgentID())
			},
		}
		rt.ScanCampaign = runtimepipeline.NewScanCampaignManager(rt.Bus, stores.ScanCampaignStore, hooks, stores.SQLDB)
	}

	rt.Scheduler = runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		callbackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		payload := sc.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		if err := rt.Bus.Publish(callbackCtx, events.Event{
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
			if exactStore, ok := stores.ScheduleStore.(runtimepipeline.ExactSchedulePersistence); ok {
				if err := exactStore.MarkScheduleFiredExact(callbackCtx, sc); err != nil {
					log.Printf("mark schedule fired failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
				}
			} else if err := stores.ScheduleStore.MarkScheduleFired(callbackCtx, sc); err != nil {
				log.Printf("mark schedule fired failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
			}
		}
	})

	if stores.SQLDB != nil {
		rt.Budget = NewBudgetTracker(stores.SQLDB, rt.Bus, cfg, stores.MailboxStore)
	}

	modelRuntime, err := llm.RuntimeFactory{
		Cfg:           cfg,
		Sessions:      stores.SessionRegistry,
		Turns:         stores.TurnStore,
		Conversations: stores.ConversationStore,
		Budget:        rt.Budget,
		Workspaces:    rt.Workspace,
	}.Build()
	if err != nil {
		return nil, fmt.Errorf("build runtime: %w", err)
	}
	rt.LLM = modelRuntime

	var managerRef *runtimemanager.AgentManager
	rt.ToolExecutor = runtimetools.NewExecutorWithOptions(rt.Bus, rt.Scheduler, runtimetools.ExecutorOptions{
		Config:       cfg,
		MailboxStore: stores.MailboxStore,
		SQLDB:        stores.SQLDB,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, stores.ScheduleStore)

	factory := runtimeagents.NewLLMAgentFactory(rt.LLM, rt.ToolExecutor, rt.ToolExecutor.ToolDefinitions())
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, factory, runtimemanager.AgentManagerOptions{
		Workspaces:           rt.Workspace,
		Sessions:             stores.SessionRegistry,
		RuntimeMode:          cfg.LLM.RuntimeMode,
		Budget:               rt.Budget,
		DisableSpinupControl: true,
	}, stores.ManagerStore)
	managerRef = rt.Manager
	if rt.Pipeline != nil && rt.Manager != nil {
		rt.Pipeline.SetInstanceActivator(rt.Manager.ActivateFlowInstance)
	}
	if rt.Pipeline != nil {
		rt.Pipeline.SetTimerScheduling(rt.Scheduler, stores.ScheduleStore)
	}

	if stores.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, stores.InboundStore)
	}
	if opts.EnableToolGateway {
		rt.ToolGateway = runtimemcp.NewGateway(rt.ToolExecutor, strings.TrimSpace(opts.ToolGatewayToken), RuntimeMCPGatewayHooks(rt.Logger))
	}
	if stores.SQLDB != nil {
		rt.ShardDispatcher = runtimepipeline.NewShardDispatcher(stores.SQLDB, rt.Bus, rt.Manager, cfg.Sharding())
	}

	return rt, nil
}

func (rt *Runtime) Start(ctx context.Context) error {
	if rt == nil {
		return fmt.Errorf("runtime is nil")
	}
	if rt.Pipeline != nil {
		if err := rt.Pipeline.ValidateWorkflowContracts(); err != nil {
			return fmt.Errorf("workflow contract validation failed: %w", err)
		}
	}
	if rt.Pipeline != nil {
		go rt.Pipeline.RunMaintenance(ctx)
	}
	for _, node := range rt.SystemNodes {
		if node != nil {
			go node.Run(ctx)
		}
	}
	if rt.ScanCampaign != nil {
		go rt.ScanCampaign.Run(ctx)
	}
	if rt.Scheduler != nil && rt.Stores.ScheduleStore != nil {
		schedules, err := rt.Stores.ScheduleStore.LoadActiveSchedules(ctx)
		if err != nil {
			return fmt.Errorf("load schedules failed: %w", err)
		}
		for _, sc := range schedules {
			if err := rt.Scheduler.Register(sc); err != nil {
				log.Printf("restore schedule failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
			}
		}
		if err := ensureLifecycleWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			log.Printf("workflow lifecycle schedule ensure failed: %v", err)
		}
		if err := ensureRecurringWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Pipeline); err != nil {
			log.Printf("workflow recurring schedule ensure failed: %v", err)
		}
		if err := ensureInfraHealthCheckSchedule(ctx, rt.Stores.ScheduleStore); err != nil {
			log.Printf("infra health schedule ensure failed: %v", err)
		}
	}
	if rt.Config.Runtime.RecoveryOnStartup && rt.Manager != nil {
		if err := rt.Manager.Recover(ctx); err != nil {
			log.Printf("runtime recovery failed (continuing without recovery): %v", err)
			if resetErr := rt.Manager.ResetRuntimeState(); resetErr != nil {
				log.Printf("runtime state reset after recovery failure also failed: %v", resetErr)
			}
			if rt.Stores.MailboxStore != nil {
				ctxPayload := mustJSON(map[string]any{
					"error":        err.Error(),
					"instruction":  "Runtime recovery failed. Use dashboard control actions (reset_db + seed-org) to reinitialize, or fix persisted config and restart.",
					"spec_version": "v2.0.15",
				})
				if _, mailboxErr := rt.Stores.MailboxStore.InsertMailboxItem(ctx, runtimetools.MailboxItem{
					FromAgent: "runtime",
					Type:      "runtime.recovery_failed",
					Priority:  "critical",
					Status:    "pending",
					Context:   ctxPayload,
					Summary:   runtimeTruncateString("Runtime recovery failed: "+err.Error(), 200),
				}); mailboxErr != nil {
					log.Printf("runtime recovery mailbox insert failed: %v", mailboxErr)
				}
			}
			payload := mustJSON(map[string]any{
				"error":        err.Error(),
				"spec_version": "v2.0.15",
			})
			if publishErr := rt.Bus.Publish(ctx, events.Event{
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
	if rt.Manager != nil {
		rt.Manager.Run(ctx)
	}
	if rt.Stores.SQLDB != nil && rt.Logger != nil {
		go StartMCPStallDiagnosticLoop(ctx, rt.Stores.SQLDB, rt.Logger, runtimemcp.DefaultStallDiagnosticConfig())
	}
	if rt.ShardDispatcher != nil {
		go rt.ShardDispatcher.Run(ctx)
	}
	if rt.Options.SelfCheck {
		if err := rt.selfCheck(); err != nil {
			return fmt.Errorf("self-check failed: %w", err)
		}
	}
	return nil
}

func (rt *Runtime) Shutdown() error {
	if rt == nil {
		return nil
	}
	var shutdownErr error
	if rt.Manager != nil {
		if err := rt.Manager.Shutdown(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	if rt.Scheduler != nil {
		rt.Scheduler.Stop()
	}
	return shutdownErr
}

func (rt *Runtime) Wait(ctx context.Context) {
	<-ctx.Done()
}

func (rt *Runtime) selfCheck() error {
	ctx := context.Background()
	t := events.EventType("runtime.boot")
	ch := rt.Bus.Subscribe("bootstrap-self-check", t)
	payload := mustJSON(map[string]string{"status": "ok"})
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        t,
		SourceAgent: "bootstrap",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := rt.Bus.Publish(ctx, evt); err != nil {
		return err
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		return fmt.Errorf("eventbus publish/subscribe timeout")
	}
	_ = rt.LLM
	return nil
}

func ensureRecurringWorkflowSchedules(ctx context.Context, store runtimepipeline.SchedulePersistence, workflow runtimepipeline.WorkflowRuntime) error {
	if store == nil || workflow == nil || workflow.ContractBundle() == nil {
		return nil
	}
	for _, timer := range workflow.ContractBundle().WorkflowTimers() {
		if !timer.Recurring {
			continue
		}
		owner := strings.TrimSpace(timer.Owner)
		eventType := strings.TrimSpace(timer.Event)
		if owner == "" || eventType == "" {
			continue
		}
		cron, ok := recurringWorkflowTimerSpec(timer)
		if !ok {
			continue
		}
		if err := store.UpsertSchedule(ctx, runtimepipeline.Schedule{
			AgentID:   owner,
			EventType: eventType,
			Mode:      "cron",
			Cron:      cron,
			Payload:   recurringWorkflowTimerPayload(timer),
		}); err != nil {
			return err
		}
	}
	return nil
}

func ensureLifecycleWorkflowSchedules(ctx context.Context, store runtimepipeline.SchedulePersistence, scheduler *runtimepipeline.Scheduler, workflow runtimepipeline.WorkflowRuntime) error {
	if store == nil || workflow == nil || workflow.ContractBundle() == nil {
		return nil
	}
	instanceStore := workflow.WorkflowInstanceStore()
	if instanceStore == nil || !instanceStore.Enabled() {
		return nil
	}
	instances, err := instanceStore.List(ctx)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		verticalID := strings.TrimSpace(instance.InstanceID)
		for _, timerState := range instance.TimerState {
			if timerState.Cancelled {
				continue
			}
			timerID := strings.TrimSpace(timerState.TimerID)
			if timerID == "" {
				continue
			}
			timer, ok := workflow.ContractBundle().WorkflowTimerByID(timerID)
			if !ok || timer.Recurring {
				continue
			}
			owner := strings.TrimSpace(timer.Owner)
			eventType := strings.TrimSpace(timer.Event)
			if owner == "" || eventType == "" {
				continue
			}
			sc := runtimepipeline.Schedule{
				AgentID:    owner,
				EventType:  eventType,
				Mode:       "once",
				At:         timerState.FiresAt,
				VerticalID: verticalID,
				TaskID:     timerID,
				Payload:    mustJSON(map[string]any{"timer_id": timerID, "trigger_reason": timerID}),
			}
			if err := store.UpsertSchedule(ctx, sc); err != nil {
				return err
			}
			if scheduler != nil {
				if err := scheduler.Register(sc); err != nil {
					log.Printf("rehydrate lifecycle schedule failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
				}
			}
		}
	}
	return nil
}

func recurringWorkflowTimerSpec(timer runtimecontracts.WorkflowTimerContract) (string, bool) {
	var interval time.Duration
	if delay := strings.TrimSpace(timer.Delay); delay != "" && !strings.Contains(delay, "{") {
		if parsed, err := time.ParseDuration(delay); err == nil && parsed > 0 {
			interval += parsed
		}
	}
	interval += time.Duration(timer.DelaySeconds) * time.Second
	interval += time.Duration(timer.DelayMinutes) * time.Minute
	interval += time.Duration(timer.DelayHours) * time.Hour
	interval += time.Duration(timer.DelayDays) * 24 * time.Hour
	if interval <= 0 {
		return "", false
	}
	return "@every " + interval.String(), true
}

func recurringWorkflowTimerPayload(timer runtimecontracts.WorkflowTimerContract) []byte {
	switch strings.TrimSpace(timer.Event) {
	case "timer.portfolio_digest":
		return mustJSON(map[string]any{"trigger_reason": strings.TrimSpace(timer.ID)})
	default:
		return mustJSON(map[string]any{})
	}
}

func ensureInfraHealthCheckSchedule(ctx context.Context, store runtimepipeline.SchedulePersistence) error {
	if store == nil {
		return nil
	}
	cron := strings.TrimSpace(os.Getenv("EMPIREAI_INFRA_HEALTH_CRON"))
	if cron == "" {
		cron = "0 * * * *"
	}
	return store.UpsertSchedule(ctx, runtimepipeline.Schedule{
		AgentID:   "holding-devops",
		EventType: "timer.infra_health_check",
		Mode:      "cron",
		Cron:      cron,
		Payload:   []byte(`{"trigger":"infra_health_check"}`),
	})
}

func runtimeEnvBool(key string, fallback bool) bool {
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

func runtimeTruncateString(v string, max int) string {
	v = strings.TrimSpace(v)
	if max <= 0 {
		return ""
	}
	if len(v) <= max {
		return v
	}
	return v[:max]
}
