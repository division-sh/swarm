package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sort"
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
	"swarm/internal/runtime/core/timeridentity"
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
	MCPTurns       *runtimemcp.TurnContextRegistry
	Authority      runtimeauthority.Provider
	EmitRegistry   *runtimetools.EmitRegistry
	PromptResolver runtimecontracts.PromptResolver
}

type terminalInstanceStateSetter interface {
	SetTerminalInstanceStates(states []string)
}

const runtimeQuiescenceStableChecks = 3

func (rt *Runtime) WaitForQuiescence(ctx context.Context) error {
	if rt == nil {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	for {
		if rt.Bus != nil {
			if err := rt.Bus.WaitForQuiescence(ctx); err != nil {
				return err
			}
		}
		if rt.Manager != nil {
			if err := rt.Manager.WaitForQuiescence(ctx); err != nil {
				return err
			}
		}
		pendingDeliveries := 0
		if rt.Bus != nil {
			pendingDeliveries = rt.Bus.PendingAgentDeliveries()
		}
		if pendingDeliveries == 0 {
			stable++
			if stable >= runtimeQuiescenceStableChecks {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
	if !report.HasErrors() && !(bootWarningsFatal() && len(report.Warnings()) > 0) {
		return nil
	}
	lines := make([]string, 0, len(report.Errors())+len(report.Warnings()))
	for _, finding := range report.Errors() {
		lines = append(lines, strings.TrimSpace(finding.Message))
	}
	if bootWarningsFatal() {
		for _, finding := range report.Warnings() {
			lines = append(lines, strings.TrimSpace(finding.Message))
		}
	}
	return fmt.Errorf(strings.Join(lines, "\n"))
}

func bootWarningsFatal() bool {
	return runtimeEnvBool("SWARM_BOOT_WARNINGS_FATAL", true)
}

func bindRuntimeTerminalInstanceStates(stores Stores, source semanticview.Source) {
	if source == nil {
		return
	}
	states := source.WorkflowTerminalStages()
	candidates := []any{
		stores.EventStore,
		stores.ConversationStore,
		stores.ManagerStore,
		stores.ScheduleStore,
		stores.MailboxStore,
		stores.InboundStore,
		stores.DigestStore,
		stores.TurnStore,
	}
	for _, candidate := range candidates {
		setter, ok := candidate.(terminalInstanceStateSetter)
		if !ok || setter == nil {
			continue
		}
		setter.SetTerminalInstanceStates(states)
	}
}

func newRuntimePromptResolver(source semanticview.Source) (runtimecontracts.PromptResolver, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required")
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		return nil, fmt.Errorf("bundle-backed semantic source is required for contract prompt resolution")
	}
	return runtimecontracts.NewBundlePromptResolver(bundle), nil
}

func NewRuntime(ctx context.Context, cfg *config.Config, stores Stores, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("runtime config is required")
	}
	if err := validateClaudeStartupConfig(cfg, opts); err != nil {
		return nil, fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	if err := ensureWorkflowBootWiring(opts); err != nil {
		return nil, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	source := opts.WorkflowModule.SemanticSource()
	bindRuntimeTerminalInstanceStates(stores, source)
	promptResolver, err := newRuntimePromptResolver(source)
	if err != nil {
		return nil, fmt.Errorf("build prompt resolver: %w", err)
	}
	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	authorityProvider := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authorityProvider)
	if warnings, err := runtimetools.ValidateToolImplementations(source); err != nil {
		return nil, fmt.Errorf("tool implementation validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(warnings) > 0 {
			parts := make([]string, 0, len(warnings))
			for _, warning := range warnings {
				parts = append(parts, strings.TrimSpace(warning.Error()))
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("tool implementation validation warnings are fatal: %s", strings.Join(parts, "; "))
		}
		for _, warning := range warnings {
			slog.Warn("tool implementation validation warning", "warning", warning.Error())
		}
	}

	rt := &Runtime{
		Config:         cfg,
		Stores:         stores,
		Options:        opts,
		Workspace:      opts.WorkspaceLifecycle,
		MCPTurns:       mcpTurns,
		Authority:      authorityProvider,
		EmitRegistry:   emitRegistry,
		PromptResolver: promptResolver,
	}
	if opts.Credentials != nil {
		rt.Credentials = opts.Credentials
	} else {
		rt.Credentials = runtimecredentials.NewEnvStore()
	}

	if stores.SQLDB != nil {
		rt.Logger = NewRuntimeLogger(stores.SQLDB)
	}
	payloadValidator := newRuntimePayloadValidator(runtimeEnvBool("SWARM_STRICT_PAYLOAD_VALIDATION", false), rt.Logger, emitRegistry.EventSchemaSnapshot())
	bus, err := newRuntimeEventBus(stores.EventStore, rt.Logger, source, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	}, payloadValidator)
	if err != nil {
		return nil, fmt.Errorf("build event bus: %w", err)
	}
	rt.Bus = bus

	var managerRef *runtimemanager.AgentManager
	rt.Scheduler = runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		callbackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := rt.Bus.Publish(callbackCtx, scheduledEvent(sc)); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "publish_failed", rt.Logger.Error(callbackCtx, "scheduler", "publish_failed", map[string]any{
					"agent_id":   sc.AgentID,
					"event_type": sc.EventType,
					"entity_id":  sc.EffectiveEntityID(),
				}, err))
			}
		}
		if stores.ScheduleStore != nil {
			if exactStore, ok := stores.ScheduleStore.(runtimepipeline.ExactSchedulePersistence); ok {
				if err := exactStore.MarkScheduleFiredExact(callbackCtx, sc); err != nil {
					if rt.Logger != nil {
						handleRuntimeLogPersistenceError("scheduler", "mark_fired_failed", rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
							"agent_id":   sc.AgentID,
							"event_type": sc.EventType,
							"entity_id":  sc.EffectiveEntityID(),
						}, err))
					}
				}
			} else if err := stores.ScheduleStore.MarkScheduleFired(callbackCtx, sc); err != nil {
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("scheduler", "mark_fired_failed", rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"entity_id":  sc.EffectiveEntityID(),
					}, err))
				}
			}
		}
	})
	if stores.SQLDB != nil {
		rt.Pipeline = runtimepipeline.NewPipelineCoordinatorWithOptions(rt.Bus, stores.SQLDB, runtimepipeline.PipelineCoordinatorOptions{
			Module: opts.WorkflowModule,
			InstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
				if managerRef == nil {
					return fmt.Errorf("flow instance activator is required")
				}
				return managerRef.ActivateFlowInstance(ctx, req)
			},
			InstanceDeactivator: func(ctx context.Context, req runtimepipeline.FlowInstanceDeactivationRequest) error {
				if managerRef == nil {
					return fmt.Errorf("flow instance deactivator is required")
				}
				return managerRef.DeactivateFlowInstanceModel(ctx, req)
			},
			TimerScheduler:     rt.Scheduler,
			TimerScheduleStore: stores.ScheduleStore,
		})
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)...)
		}
	}

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
			MCPTurns:      rt.MCPTurns,
		}.Build()
		if err != nil {
			return nil, fmt.Errorf("build runtime: %w", err)
		}
	}
	rt.LLM = modelRuntime
	if warnings, err := runtimetools.ValidateNativeToolBootConfig(ctx, source, rt.Credentials, modelRuntime); err != nil {
		return nil, fmt.Errorf("native tool validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(warnings) > 0 {
			parts := make([]string, 0, len(warnings))
			for _, warning := range warnings {
				parts = append(parts, strings.TrimSpace(warning.Error()))
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("native tool validation warnings are fatal: %s", strings.Join(parts, "; "))
		}
		for _, warning := range warnings {
			slog.Warn("native tool validation warning", "warning", warning.Error())
		}
	}

	rt.ToolExecutor = runtimetools.NewExecutorWithOptions(rt.Bus, rt.Scheduler, runtimetools.ExecutorOptions{
		Config:            cfg,
		Credentials:       rt.Credentials,
		MailboxStore:      stores.MailboxStore,
		SQLDB:             stores.SQLDB,
		WorkflowSource:    source,
		WorkspaceResolver: rt.Workspace,
		AuthorityProvider: rt.Authority,
		EmitRegistry:      rt.EmitRegistry,
		ManagerProvider: func() runtimetools.Manager {
			return managerRef
		},
	}, stores.ScheduleStore)
	if missing, err := runtimecredentials.MissingRequired(ctx, rt.Credentials, source); err != nil {
		return nil, fmt.Errorf("credential validation failed: %w", err)
	} else {
		if bootWarningsFatal() && len(missing) > 0 {
			parts := make([]string, 0, len(missing))
			for _, item := range missing {
				requiredBy := make([]string, 0, len(item.RequiredBy))
				for _, ref := range item.RequiredBy {
					requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
				}
				sort.Strings(requiredBy)
				parts = append(parts, fmt.Sprintf("%s required by %s", strings.TrimSpace(item.Key), strings.Join(requiredBy, ", ")))
			}
			sort.Strings(parts)
			return nil, fmt.Errorf("missing required credentials: %s", strings.Join(parts, "; "))
		}
		for _, item := range missing {
			requiredBy := make([]string, 0, len(item.RequiredBy))
			for _, ref := range item.RequiredBy {
				requiredBy = append(requiredBy, strings.TrimSpace(ref.Kind)+":"+strings.TrimSpace(ref.Name))
			}
			slog.Warn("credential requirement warning", "key", item.Key, "required_by", strings.Join(requiredBy, ", "))
		}
	}
	if generated := rt.EmitRegistry.GeneratedEmitSchemasForAgentRoles(); len(generated) > 0 {
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

	factory := runtimeagents.NewLLMAgentFactory(rt.LLM, rt.ToolExecutor, rt.ToolExecutor.ToolDefinitions(), runtimeagents.LLMAgentOptions{
		PromptResolver:    rt.PromptResolver,
		AuthorityProvider: rt.Authority,
		EmitRegistry:      rt.EmitRegistry,
	})
	var workflowInstances runtimepipeline.WorkflowInstancePersistence
	if rt.Pipeline != nil {
		workflowInstances = rt.Pipeline.WorkflowInstanceStore()
	}
	rt.Manager = runtimemanager.NewAgentManagerWithOptions(rt.Bus, factory, runtimemanager.AgentManagerOptions{
		Workspaces:        rt.Workspace,
		Sessions:          stores.SessionRegistry,
		SemanticSource:    source,
		PromptResolver:    rt.PromptResolver,
		WorkflowInstances: workflowInstances,
		RuntimeMode:       cfg.LLM.RuntimeMode,
		Budget:            rt.Budget,
		ResetRuntimeOwnedState: func() {
			if rt.MCPTurns != nil {
				rt.MCPTurns.Reset()
			}
		},
		ThrottleSuppressPrefixes: runtimeThrottleSuppressPrefixes(source),
		DisableSpinupControl:     true,
	}, stores.ManagerStore)
	managerRef = rt.Manager

	if stores.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, rt.Logger, stores.InboundStore)
	}
	if opts.EnableToolGateway {
		rt.ToolGateway = runtimemcp.NewGateway(rt.ToolExecutor, strings.TrimSpace(opts.ToolGatewayToken), RuntimeMCPGatewayHooks(rt.Logger, func(agentID string) (runtimeactors.AgentConfig, bool) {
			if rt.Manager == nil {
				return runtimeactors.AgentConfig{}, false
			}
			return rt.Manager.GetAgentConfig(strings.TrimSpace(agentID))
		}, rt.EmitRegistry, rt.MCPTurns))
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
	if handle, ok := timeridentity.ParseTimerHandle(decoded); ok && handle.Kind == timeridentity.TimerHandleWorkflowTimer {
		delete(decoded, "timer_handle")
	}
	entityID := strings.TrimSpace(sc.EffectiveEntityID())
	if _, ok := decoded["entity_id"]; !ok {
		if entityID != "" {
			decoded["entity_id"] = entityID
		}
	}
	flowInstance := strings.TrimSpace(sc.EffectiveFlowInstance())
	if _, ok := decoded["flow_instance"]; !ok {
		if flowInstance != "" {
			decoded["flow_instance"] = flowInstance
		}
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return payload
	}
	return encoded
}

func scheduledEvent(sc runtimepipeline.Schedule) events.Event {
	return (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(sc.EventType),
		SourceAgent: sc.AgentID,
		TaskID:      sc.TaskID,
		Payload:     scheduleEventPayload(sc),
		CreatedAt:   time.Now(),
	}).WithEnvelope(events.EventEnvelope{
		EntityID:     sc.EffectiveEntityID(),
		FlowInstance: sc.EffectiveFlowInstance(),
	})
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
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("scheduler", "restore_schedule_failed", rt.Logger.Error(ctx, "scheduler", "restore_schedule_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"entity_id":  sc.EffectiveEntityID(),
					}, err))
				}
			}
		}
		if err := ensureLifecycleWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "ensure_lifecycle_failed", rt.Logger.Error(ctx, "scheduler", "ensure_lifecycle_failed", nil, err))
			}
		}
		if err := ensureRecurringWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Pipeline); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "ensure_recurring_failed", rt.Logger.Error(ctx, "scheduler", "ensure_recurring_failed", nil, err))
			}
		}
	}
	if rt.Config.Runtime.RecoveryOnStartup && rt.Manager != nil {
		if err := rt.Manager.Recover(ctx); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("runtime", "recovery_failed", rt.Logger.Error(ctx, "runtime", "recovery_failed", nil, err))
			}
			if resetErr := rt.Manager.ResetRuntimeState(); resetErr != nil {
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("runtime", "recovery_reset_failed", rt.Logger.Error(ctx, "runtime", "recovery_reset_failed", nil, resetErr))
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
					if rt.Logger != nil {
						handleRuntimeLogPersistenceError("runtime", "recovery_mailbox_insert_failed", rt.Logger.Error(ctx, "runtime", "recovery_mailbox_insert_failed", nil, mailboxErr))
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
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("runtime", "recovery_failed_publish_failed", rt.Logger.Error(ctx, "runtime", "recovery_failed_publish_failed", nil, publishErr))
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
	if err := validateClaudeManagedAgentWorkspaces(ctx, rt.Config, rt.Workspace, rt.Manager); err != nil {
		return fmt.Errorf("claude runtime workspace validation failed: %w", err)
	}
	if err := validateClaudeMCPToolsForManagedAgents(rt.Config, rt.ToolGateway, rt.Options.ToolGatewayToken, rt.Manager); err != nil {
		return fmt.Errorf("claude runtime mcp validation failed: %w", err)
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
		startTrigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
		if err != nil || !startTrigger.IsBoot() {
			continue
		}
		cancelTrigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
		if err != nil || cancelTrigger.Valid() {
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
		instanceIdentity := runtimepipeline.StoredFlowInstance(source, instance)
		entityID := strings.TrimSpace(instanceIdentity.EntityID)
		if entityID == "" {
			continue
		}
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
				AgentID:      owner,
				EventType:    eventType,
				Mode:         "once",
				At:           timerState.FiresAt,
				EntityID:     entityID,
				FlowInstance: strings.TrimSpace(instance.StorageRef),
				TaskID:       timeridentity.WorkflowTimerHandle(timerID).TaskID(),
				Payload:      recurringWorkflowTimerPayload(timer),
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
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	if !handle.Valid() {
		return mustJSON(map[string]any{})
	}
	return mustJSON(handle.PayloadMetadata())
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
