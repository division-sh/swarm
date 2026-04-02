package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/config"
	"swarm/internal/events"
	runtimeagents "swarm/internal/runtime/agents"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecredentials "swarm/internal/runtime/credentials"
	llm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimetools "swarm/internal/runtime/tools"
	workspace "swarm/internal/runtime/workspace"
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
	LLMRuntime         llm.Runtime
	Credentials        runtimecredentials.Store
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
	Credentials    runtimecredentials.Store
	LLM            llm.Runtime
	ToolExecutor   *runtimetools.Executor
	Manager        *runtimemanager.AgentManager
	InboundGateway *InboundGateway
	ToolGateway    *runtimemcp.Gateway
}

var ()

func runtimeControlPlaneRecipient() string {
	if recipient := controlPlaneRecipientFromSource(runtimepipeline.DefaultWorkflowSemanticSourceOrNil()); recipient != "" {
		return recipient
	}
	return ""
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
	return strings.TrimSpace(runtimeControlPlaneRecipient())
}

func runtimeThrottleSuppressPrefixes(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	value, ok := semanticview.PolicyValueForFlow(source, "", "throttle_suppress_prefixes")
	if !ok {
		return nil
	}
	switch typed := value.Value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := strings.TrimSpace(asString(item)); text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func ensureWorkflowBootWiring(opts RuntimeOptions) error {
	if opts.WorkflowModule == nil {
		return fmt.Errorf("workflow module is required: configure RuntimeOptions.WorkflowModule")
	}
	runtimepipeline.SetDefaultWorkflowModuleFactory(func() runtimepipeline.WorkflowModule {
		return opts.WorkflowModule
	})
	source := opts.WorkflowModule.SemanticSource()
	if opts.WorkspaceLifecycle != nil {
		if err := opts.WorkspaceLifecycle.ValidateSource(context.Background(), source); err != nil {
			return fmt.Errorf("workspace validation failed: %w", err)
		}
	}
	report := runtimebootverify.Run(context.Background(), opts.WorkflowModule.SemanticSource(), runtimebootverify.Options{
		Credentials:       opts.Credentials,
		CheckMCPReachable: true,
	})
	if !report.HasErrors() {
		return nil
	}
	lines := make([]string, 0, len(report.Errors()))
	for _, finding := range report.Errors() {
		lines = append(lines, strings.TrimSpace(finding.Message))
	}
	return fmt.Errorf(strings.Join(lines, "\n"))
}

func NewRuntime(ctx context.Context, cfg *config.Config, stores Stores, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	if err := ensureWorkflowBootWiring(opts); err != nil {
		return nil, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	source := opts.WorkflowModule.SemanticSource()
	if bundle, ok := semanticview.Bundle(source); ok {
		runtimecontracts.SetActivePromptBundle(bundle)
	} else {
		runtimecontracts.SetActivePromptBundle(nil)
	}
	runtimeauthority.SetProvider(runtimeauthority.NewSourceProvider(source))
	if warnings, err := runtimetools.ValidateToolImplementations(source); err != nil {
		return nil, fmt.Errorf("tool implementation validation failed: %w", err)
	} else {
		for _, warning := range warnings {
			slog.Warn("tool implementation validation warning", "warning", warning.Error())
		}
	}

	rt := &Runtime{
		Config:    cfg,
		Stores:    stores,
		Options:   opts,
		Workspace: opts.WorkspaceLifecycle,
	}
	if opts.Credentials != nil {
		rt.Credentials = opts.Credentials
	} else {
		rt.Credentials = runtimecredentials.NewEnvStore()
	}

	if stores.SQLDB != nil {
		rt.Logger = NewRuntimeLogger(stores.SQLDB)
	}
	payloadValidator := newRuntimePayloadValidator(runtimeEnvBool("SWARM_STRICT_PAYLOAD_VALIDATION", false), rt.Logger)
	bus, err := newRuntimeEventBus(stores.EventStore, rt.Logger, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	}, payloadValidator)
	if err != nil {
		return nil, fmt.Errorf("build event bus: %w", err)
	}
	rt.Bus = bus
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
		payload := scheduleEventPayload(sc)
		if err := rt.Bus.Publish(callbackCtx, (events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType(sc.EventType),
			SourceAgent: sc.AgentID,
			TaskID:      sc.TaskID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		}).WithEntityID(sc.EffectiveEntityID())); err != nil {
			log.Printf("schedule publish failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
			if rt.Logger != nil {
				rt.Logger.Error(callbackCtx, "scheduler", "publish_failed", map[string]any{
					"agent_id":   sc.AgentID,
					"event_type": sc.EventType,
					"entity_id":  sc.EffectiveEntityID(),
				}, err)
			}
		}
		if stores.ScheduleStore != nil {
			if exactStore, ok := stores.ScheduleStore.(runtimepipeline.ExactSchedulePersistence); ok {
				if err := exactStore.MarkScheduleFiredExact(callbackCtx, sc); err != nil {
					log.Printf("mark schedule fired failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
					if rt.Logger != nil {
						rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
							"agent_id":   sc.AgentID,
							"event_type": sc.EventType,
							"entity_id":  sc.EffectiveEntityID(),
						}, err)
					}
				}
			} else if err := stores.ScheduleStore.MarkScheduleFired(callbackCtx, sc); err != nil {
				log.Printf("mark schedule fired failed agent=%s event=%s err=%v", sc.AgentID, sc.EventType, err)
				if rt.Logger != nil {
					rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"entity_id":  sc.EffectiveEntityID(),
					}, err)
				}
			}
		}
	})

	if stores.SQLDB != nil {
		rt.Budget = NewBudgetTracker(stores.SQLDB, rt.Bus, cfg, stores.MailboxStore, rt.Logger, source)
	}

	modelRuntime := opts.LLMRuntime
	if modelRuntime == nil {
		modelRuntime, err = llm.RuntimeFactory{
			Cfg:           cfg,
			Sessions:      stores.SessionRegistry,
			Turns:         stores.TurnStore,
			Conversations: stores.ConversationStore,
			Budget:        rt.Budget,
			Workspaces:    rt.Workspace,
			Events:        rt.Bus,
		}.Build()
		if err != nil {
			return nil, fmt.Errorf("build runtime: %w", err)
		}
	}
	rt.LLM = modelRuntime
	if warnings, err := runtimetools.ValidateNativeToolBootConfig(ctx, source, rt.Credentials, modelRuntime); err != nil {
		return nil, fmt.Errorf("native tool validation failed: %w", err)
	} else {
		for _, warning := range warnings {
			slog.Warn("native tool validation warning", "warning", warning.Error())
		}
	}

	var managerRef *runtimemanager.AgentManager
	rt.ToolExecutor = runtimetools.NewExecutorWithOptions(rt.Bus, rt.Scheduler, runtimetools.ExecutorOptions{
		Config:            cfg,
		Credentials:       rt.Credentials,
		MailboxStore:      stores.MailboxStore,
		SQLDB:             stores.SQLDB,
		WorkflowSource:    source,
		FlowActivator:     nil,
		WorkspaceResolver: rt.Workspace,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, stores.ScheduleStore)
	if missing, err := runtimecredentials.MissingRequired(ctx, rt.Credentials, source); err != nil {
		return nil, fmt.Errorf("credential validation failed: %w", err)
	} else {
		for _, item := range missing {
			requiredBy := make([]string, 0, len(item.RequiredBy))
			for _, ref := range item.RequiredBy {
				requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
			}
			slog.Warn("credential requirement warning", "key", item.Key, "required_by", strings.Join(requiredBy, ", "))
		}
	}
	runtimetools.InitEventSchemaRegistry(source)
	if generated := runtimetools.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
		if runtimeEnvBool("SWARM_EMIT_SCHEMA_STRICT", true) {
			return nil, fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries", len(generated))
		}
		sample := generated
		if len(sample) > 10 {
			sample = sample[:10]
		}
		slog.Warn("emit schema hardening: agent-emitted event schemas missing explicit definitions",
			"count", len(generated),
			"sample", strings.Join(sample, ", "),
		)
	}

	factory := runtimeagents.NewLLMAgentFactory(rt.LLM, rt.ToolExecutor, rt.ToolExecutor.ToolDefinitions())
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, factory, runtimemanager.AgentManagerOptions{
		Workspaces:               rt.Workspace,
		Sessions:                 stores.SessionRegistry,
		RuntimeMode:              cfg.LLM.RuntimeMode,
		Budget:                   rt.Budget,
		ThrottleSuppressPrefixes: runtimeThrottleSuppressPrefixes(source),
		DisableSpinupControl:     true,
	}, stores.ManagerStore)
	managerRef = rt.Manager
	if rt.ToolExecutor != nil && rt.Manager != nil {
		rt.ToolExecutor.SetFlowActivator(rt.Manager.ActivateFlowInstance)
	}
	if rt.Pipeline != nil && rt.Manager != nil {
		rt.Manager.SetWorkflowInstanceStore(rt.Pipeline.WorkflowInstanceStore())
		rt.Pipeline.SetInstanceActivator(rt.Manager.ActivateFlowInstance)
		rt.Pipeline.SetInstanceDeactivator(func(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
			return rt.Manager.DeactivateFlowInstance(ctx, req.TemplateID, req.InstanceID, req.EntityID)
		})
	}
	if rt.Pipeline != nil {
		rt.Pipeline.SetTimerScheduling(rt.Scheduler, stores.ScheduleStore)
	}

	if stores.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, rt.Logger, stores.InboundStore)
	}
	if opts.EnableToolGateway {
		rt.ToolGateway = runtimemcp.NewGateway(rt.ToolExecutor, strings.TrimSpace(opts.ToolGatewayToken), RuntimeMCPGatewayHooks(rt.Logger, func(agentID string) (runtimeactors.AgentConfig, bool) {
			if rt.Manager == nil {
				return runtimeactors.AgentConfig{}, false
			}
			return rt.Manager.GetAgentConfig(strings.TrimSpace(agentID))
		}))
	}

	return rt, nil
}

func scheduleEventPayload(sc runtimepipeline.Schedule) []byte {
	payload := sc.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded == nil {
		return payload
	}
	delete(decoded, "__schedule_task_id")
	delete(decoded, "timer_id")
	delete(decoded, "trigger_reason")
	entityID := strings.TrimSpace(sc.EffectiveEntityID())
	if _, ok := decoded["entity_id"]; !ok {
		if entityID != "" {
			decoded["entity_id"] = entityID
		}
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return payload
	}
	return encoded
}

func (rt *Runtime) Start(ctx context.Context) error {
	if rt == nil {
		return fmt.Errorf("runtime is nil")
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
				if rt.Logger != nil {
					rt.Logger.Error(ctx, "scheduler", "restore_schedule_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"entity_id":  sc.EffectiveEntityID(),
					}, err)
				}
			}
		}
		if err := ensureLifecycleWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			log.Printf("workflow lifecycle schedule ensure failed: %v", err)
			if rt.Logger != nil {
				rt.Logger.Error(ctx, "scheduler", "ensure_lifecycle_failed", nil, err)
			}
		}
		if err := ensureRecurringWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Pipeline); err != nil {
			log.Printf("workflow recurring schedule ensure failed: %v", err)
			if rt.Logger != nil {
				rt.Logger.Error(ctx, "scheduler", "ensure_recurring_failed", nil, err)
			}
		}
	}
	if rt.Config.Runtime.RecoveryOnStartup && rt.Manager != nil {
		if err := rt.Manager.Recover(ctx); err != nil {
			log.Printf("runtime recovery failed (continuing without recovery): %v", err)
			if rt.Logger != nil {
				rt.Logger.Error(ctx, "runtime", "recovery_failed", nil, err)
			}
			if resetErr := rt.Manager.ResetRuntimeState(); resetErr != nil {
				log.Printf("runtime state reset after recovery failure also failed: %v", resetErr)
				if rt.Logger != nil {
					rt.Logger.Error(ctx, "runtime", "recovery_reset_failed", nil, resetErr)
				}
			}
			if rt.Stores.MailboxStore != nil {
				ctxPayload := mustJSON(map[string]any{
					"error":       err.Error(),
					"instruction": "Runtime recovery failed. Reinitialize or repair persisted runtime state before restart.",
				})
				if _, mailboxErr := rt.Stores.MailboxStore.InsertMailboxItem(ctx, runtimetools.MailboxItem{
					FromAgent: "runtime",
					Type:      "alert",
					Priority:  "critical",
					Status:    "pending",
					Context:   ctxPayload,
					Summary:   runtimeTruncateString("Runtime recovery failed: "+err.Error(), 200),
				}); mailboxErr != nil {
					log.Printf("runtime recovery mailbox insert failed: %v", mailboxErr)
					if rt.Logger != nil {
						rt.Logger.Error(ctx, "runtime", "recovery_mailbox_insert_failed", nil, mailboxErr)
					}
				}
			}
			payload := mustJSON(map[string]any{
				"error":           err.Error(),
				"failed_event_id": nil,
				"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
			})
			if publishErr := rt.Bus.Publish(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("platform.recovery_failed"),
				SourceAgent: "runtime",
				Payload:     payload,
				CreatedAt:   time.Now(),
			}); publishErr != nil {
				log.Printf("runtime recovery_failed publish failed: %v", publishErr)
				if rt.Logger != nil {
					rt.Logger.Error(ctx, "runtime", "recovery_failed_publish_failed", nil, publishErr)
				}
			}
		}
	}
	if rt.Bus != nil {
		rt.Bus.StartOutboxSweeper(ctx, runtimebus.DefaultOutboxSweeperConfig())
	}
	if rt.Manager != nil {
		if err := rt.Manager.EnsureStaticAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
	}
	if rt.Manager != nil {
		if err := rt.Manager.EnsureStaticFlowRequiredAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
	}
	if rt.Manager != nil {
		rt.Manager.Run(ctx)
	}
	if rt.Stores.SQLDB != nil && rt.Logger != nil {
	}
	var bootCheck <-chan events.Event
	if rt.Options.SelfCheck && rt.Bus != nil {
		bootCheck = rt.Bus.Subscribe("bootstrap-self-check", events.EventType("platform.boot"))
	}
	if err := rt.publishBootCompleted(context.Background()); err != nil {
		return fmt.Errorf("publish platform.boot: %w", err)
	}
	if rt.Options.SelfCheck {
		if err := rt.verifyBootPublished(bootCheck); err != nil {
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

func (rt *Runtime) publishBootCompleted(ctx context.Context) error {
	if rt == nil || rt.Bus == nil {
		return nil
	}
	t := events.EventType("platform.boot")
	var flowCount, nodeCount, agentCount, eventCount int
	if rt != nil && rt.Options.WorkflowModule != nil {
		if source := rt.Options.WorkflowModule.SemanticSource(); source != nil {
			flowCount = len(source.FlowSchemaEntries())
			nodeCount = len(source.NodeEntries())
			agentCount = len(source.AgentEntries())
			eventCount = len(source.ResolvedEventCatalog())
		}
	}
	payload := mustJSON(map[string]any{
		"flow_count":  flowCount,
		"node_count":  nodeCount,
		"agent_count": agentCount,
		"event_count": eventCount,
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
	})
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        t,
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	return rt.Bus.Publish(ctx, evt)
}

func (rt *Runtime) verifyBootPublished(ch <-chan events.Event) error {
	if rt == nil || !rt.Options.SelfCheck {
		return nil
	}
	if ch == nil {
		return fmt.Errorf("platform.boot subscription is not configured")
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
		if strings.TrimSpace(timer.StartOn) != "" || strings.TrimSpace(timer.CancelOn) != "" {
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
