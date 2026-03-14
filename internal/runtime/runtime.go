package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
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
	"empireai/internal/runtime/semanticview"
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
}

type RuntimeOptions struct {
	SelfCheck          bool
	WorkspaceLifecycle workspace.Lifecycle
	EnableToolGateway  bool
	ToolGatewayToken   string
	WorkflowModule     runtimepipeline.WorkflowModule
}

type Runtime struct {
	Config         *config.Config
	Stores         Stores
	Options        RuntimeOptions
	Bus            *runtimebus.EventBus
	Logger         *RuntimeLogger
	Pipeline       *runtimepipeline.PipelineCoordinator
	SystemNodes    []runtimepipeline.BackgroundNode
	Scheduler      *runtimepipeline.Scheduler
	Workspace      workspace.Lifecycle
	Budget         *BudgetTracker
	LLM            llm.Runtime
	ToolExecutor   *runtimetools.Executor
	Manager        *runtimemanager.AgentManager
	InboundGateway *InboundGateway
	ToolGateway    *runtimemcp.Gateway
}

var (
	defaultControlPlaneRecipientOnce  sync.Once
	defaultControlPlaneRecipientValue string
)

func runtimeControlPlaneRecipient() string {
	if recipient := controlPlaneRecipientFromSource(runtimepipeline.DefaultWorkflowSemanticSourceOrNil()); recipient != "" {
		return recipient
	}
	defaultControlPlaneRecipientOnce.Do(func() {
		repoRoot := runtimepipeline.WorkflowRepoRoot()
		bundle, err := runtimecontracts.LoadWorkflowContractBundle(repoRoot)
		if err == nil {
			defaultControlPlaneRecipientValue = controlPlaneRecipientFromSource(semanticview.Wrap(bundle))
		}
	})
	if strings.TrimSpace(defaultControlPlaneRecipientValue) != "" {
		return strings.TrimSpace(defaultControlPlaneRecipientValue)
	}
	return "control-plane"
}

func controlPlaneRecipientFromSource(source semanticview.Source) string {
	if source == nil {
		return ""
	}
	for _, key := range []string{"control_plane_agent_id", "manager_fallback_agent_id"} {
		if value, ok := semanticview.PolicyValueForFlow(source, "", key); ok {
			if agentID := strings.TrimSpace(asString(value.Value)); agentID != "" {
				return agentID
			}
		}
	}
	return ""
}

func DefaultControlPlaneRecipient() string {
	if recipient := strings.TrimSpace(runtimeControlPlaneRecipient()); recipient != "" {
		return recipient
	}
	return "control-plane"
}

func ensureWorkflowBootWiring(opts RuntimeOptions) error {
	if opts.WorkflowModule == nil {
		return fmt.Errorf("workflow module is required: configure RuntimeOptions.WorkflowModule")
	}
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return opts.WorkflowModule
	})
	return runtimepipeline.ValidateWorkflowContracts(opts.WorkflowModule.SemanticSource())
}

func NewRuntime(ctx context.Context, cfg *config.Config, stores Stores, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
		if runtimeEnvBool("MAS_EMIT_SCHEMA_STRICT", true) {
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

	rt := &Runtime{
		Config:    cfg,
		Stores:    stores,
		Options:   opts,
		Workspace: opts.WorkspaceLifecycle,
	}

	if stores.SQLDB != nil {
		rt.Logger = NewRuntimeLogger(stores.SQLDB)
	}
	rt.Bus = newRuntimeEventBus(stores.EventStore, rt.Logger, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	})
	if stores.SQLDB != nil {
		rt.Pipeline = runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, stores.SQLDB, runtimepipeline.PipelineCoordinatorOptions{
			Module: opts.WorkflowModule,
		})
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)...)
		}
	}

	rt.Scheduler = runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		callbackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		payload := sc.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		if err := rt.Bus.Publish(callbackCtx, (events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType(sc.EventType),
			SourceAgent: sc.AgentID,
			TaskID:      sc.TaskID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		}).WithEntityID(sc.EffectiveEntityID())); err != nil {
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
		Config:         cfg,
		MailboxStore:   stores.MailboxStore,
		SQLDB:          stores.SQLDB,
		WorkflowSource: opts.WorkflowModule.SemanticSource(),
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
		rt.Pipeline.SetInstanceDeactivator(func(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
			return rt.Manager.DeactivateFlowInstance(ctx, req.TemplateID, req.InstanceID, req.EntityID)
		})
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
	}
	if rt.Config.Runtime.RecoveryOnStartup && rt.Manager != nil {
		if err := rt.Manager.Recover(ctx); err != nil {
			log.Printf("runtime recovery failed (continuing without recovery): %v", err)
			if resetErr := rt.Manager.ResetRuntimeState(); resetErr != nil {
				log.Printf("runtime state reset after recovery failure also failed: %v", resetErr)
			}
			if rt.Stores.MailboxStore != nil {
				ctxPayload := mustJSON(map[string]any{
					"error":       err.Error(),
					"instruction": "Runtime recovery failed. Reinitialize or repair persisted runtime state before restart.",
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
				"error": err.Error(),
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
	if rt.Bus != nil {
		rt.Bus.StartOutboxSweeper(ctx, runtimebus.DefaultOutboxSweeperConfig())
	}
	if rt.Manager != nil {
		rt.Manager.Run(ctx)
	}
	if rt.Stores.SQLDB != nil && rt.Logger != nil {
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
	if store == nil || workflow == nil {
		return nil
	}
	source := workflow.SemanticSource()
	if source == nil {
		return nil
	}
	for _, timer := range source.WorkflowTimers() {
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
	if store == nil || workflow == nil {
		return nil
	}
	source := workflow.SemanticSource()
	if source == nil {
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
		entityID := strings.TrimSpace(instance.InstanceID)
		for _, timerState := range instance.TimerState {
			if timerState.Cancelled {
				continue
			}
			timerID := strings.TrimSpace(timerState.TimerID)
			if timerID == "" {
				continue
			}
			timer, ok := source.WorkflowTimerByID(timerID)
			if !ok || timer.Recurring {
				continue
			}
			owner := strings.TrimSpace(timer.Owner)
			eventType := strings.TrimSpace(timer.Event)
			if owner == "" || eventType == "" {
				continue
			}
			sc := runtimepipeline.Schedule{
				AgentID:   owner,
				EventType: eventType,
				Mode:      "once",
				At:        timerState.FiresAt,
				EntityID:  entityID,
				TaskID:    timerID,
				Payload:   mustJSON(map[string]any{"timer_id": timerID, "trigger_reason": timerID}),
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
	timerID := strings.TrimSpace(timer.ID)
	if timerID == "" {
		return mustJSON(map[string]any{})
	}
	return mustJSON(map[string]any{"trigger_reason": timerID})
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
