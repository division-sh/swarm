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
	"sync"
	"time"

	"github.com/google/uuid"
	"swarm/internal/config"
	"swarm/internal/events"
	runtimeagents "swarm/internal/runtime/agents"
	runtimeauthority "swarm/internal/runtime/authority"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimecredentials "swarm/internal/runtime/credentials"
	runtimeingress "swarm/internal/runtime/ingress"
	llm "swarm/internal/runtime/llm"
	runtimemanager "swarm/internal/runtime/manager"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	runtimestartupownership "swarm/internal/runtime/startupownership"
	runtimetools "swarm/internal/runtime/tools"
	workspace "swarm/internal/runtime/workspace"
)

type Stores struct {
	SQLDB               *sql.DB
	EventStore          runtimebus.EventStore
	SessionRegistry     sessions.Registry
	ConversationStore   llm.ConversationPersistence
	ManagerStore        runtimemanager.ManagerPersistence
	ScheduleStore       runtimepipeline.SchedulePersistence
	StartupOwnership    runtimestartupownership.Store
	MailboxStore        runtimetools.MailboxPersistence
	InboundStore        InboundPersistence
	RuntimeIngressStore runtimeingress.Store
	TurnStore           llm.TurnPersistence
}

type runtimeLogSchemaCapabilityProvider interface {
	CanonicalRuntimeLogCapability(context.Context) (bool, bool, error)
}

type eventReceiptSchemaCapabilityProvider interface {
	CanonicalEventReceiptsCapability(context.Context) (bool, error)
}

type RuntimeOptions struct {
	SelfCheck          bool
	WorkspaceLifecycle workspace.Lifecycle
	EnableToolGateway  bool
	ToolGatewayToken   string
	BundleFingerprint  string
	WorkflowModule     runtimepipeline.WorkflowModule
	LLMRuntime         llm.Runtime
	Credentials        runtimecredentials.Store
	BootStartedAt      time.Time
	BootProgress       func(BootProgressEvent)
	SystemContainers   []string
}

// RuntimeDeps is the canonical dependency graph for NewRuntime boot wiring.
type RuntimeDeps struct {
	Config  *config.Config
	Stores  Stores
	Options RuntimeOptions
}

type validatedRuntimeDeps struct {
	Config                   *config.Config
	Stores                   Stores
	Options                  RuntimeOptions
	Source                   semanticview.Source
	PromptResolver           runtimecontracts.PromptResolver
	Credentials              runtimecredentials.Store
	Authority                runtimeauthority.Provider
	EmitRegistry             *runtimetools.EmitRegistry
	RuntimeLogCapabilities   runtimeLogCapabilityResolver
	EventReceiptCapability   func(context.Context) (bool, error)
	TrimmedBundleFingerprint string
}

const BootProgressTotalSteps = 22
const DefaultShutdownGrace = runtimemanager.DefaultShutdownGrace

type ShutdownOptions struct {
	Grace time.Duration
}

func DefaultShutdownOptions() ShutdownOptions {
	return ShutdownOptions{Grace: DefaultShutdownGrace}
}

type BootProgressEvent struct {
	Step   int
	Total  int
	Name   string
	Status string
	Detail string
	At     time.Time
}

type Runtime struct {
	lifecycleMu    sync.Mutex
	startCtx       context.Context
	cancelStart    context.CancelFunc
	ownershipLease runtimestartupownership.Lease
	ownerID        string
	shutdownGate   shutdownAdmission

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
	RuntimeIngress *runtimeingress.Controller
	RunControl     *runtimeruncontrol.Controller
	InboundGateway *InboundGateway
	ToolGateway    *runtimemcp.Gateway
	MCPTurns       *runtimemcp.TurnContextRegistry
	Authority      runtimeauthority.Provider
	EmitRegistry   *runtimetools.EmitRegistry
	PromptResolver runtimecontracts.PromptResolver
}

func (rt *Runtime) emitBootProgress(step int, name, status, detail string) {
	if rt == nil || rt.Options.BootProgress == nil {
		return
	}
	rt.Options.BootProgress(BootProgressEvent{
		Step:   step,
		Total:  BootProgressTotalSteps,
		Name:   strings.TrimSpace(name),
		Status: strings.TrimSpace(status),
		Detail: strings.TrimSpace(detail),
		At:     time.Now().UTC(),
	})
}

func (rt *Runtime) shutdownAdmissionClosed() bool {
	if rt == nil {
		return false
	}
	return rt.shutdownGate.Closed()
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
	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(opts.Credentials))
	if err != nil {
		return err
	}
	_ = result
	return nil
}

func bootWarningsFatal() bool {
	return runtimeEnvBool("SWARM_BOOT_WARNINGS_FATAL", true)
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

// Validate checks the NewRuntime boot dependency graph without constructing a runtime.
func (deps RuntimeDeps) Validate() error {
	_, err := deps.validated()
	return err
}

func (deps RuntimeDeps) validated() (validatedRuntimeDeps, error) {
	cfg := deps.Config
	stores := deps.Stores
	opts := deps.Options
	if cfg == nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config is required")
	}
	if err := cfg.ValidateExtensions(); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	if _, err := cfg.LLMBackendProfile(); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("runtime config validation failed: %w", err)
	}
	if err := ensureWorkflowBootWiring(opts); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("workflow contract validation failed: %w", err)
	}
	source := opts.WorkflowModule.SemanticSource()
	if err := validateClaudeStartupConfig(cfg, opts, source); err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	promptResolver, err := newRuntimePromptResolver(source)
	if err != nil {
		return validatedRuntimeDeps{}, fmt.Errorf("build prompt resolver: %w", err)
	}
	authorityProvider := runtimeauthority.NewSourceProvider(source)
	emitRegistry := runtimetools.NewEmitRegistry(source, authorityProvider)
	credentials := opts.Credentials
	if credentials == nil {
		credentials = runtimecredentials.NewEnvStore()
	}
	return validatedRuntimeDeps{
		Config:                   cfg,
		Stores:                   stores,
		Options:                  opts,
		Source:                   source,
		PromptResolver:           promptResolver,
		Credentials:              credentials,
		Authority:                authorityProvider,
		EmitRegistry:             emitRegistry,
		RuntimeLogCapabilities:   runtimeLogSchemaCapabilities(stores),
		EventReceiptCapability:   canonicalEventReceiptCapabilities(stores),
		TrimmedBundleFingerprint: strings.TrimSpace(opts.BundleFingerprint),
	}, nil
}

func (deps validatedRuntimeDeps) payloadValidator(logger *RuntimeLogger) runtimebus.PayloadValidator {
	return newRuntimePayloadValidator(logger, deps.EmitRegistry.EventSchemaSnapshot())
}

func (deps validatedRuntimeDeps) bindPayloadValidator(payloadValidator runtimebus.PayloadValidator) {
	type eventPayloadValidationBinder interface {
		SetEventPayloadValidator(func(eventType string, payload []byte) error)
	}
	if binder, ok := deps.Stores.EventStore.(eventPayloadValidationBinder); ok && binder != nil {
		binder.SetEventPayloadValidator(payloadValidator)
	}
	if binder, ok := deps.Stores.InboundStore.(eventPayloadValidationBinder); ok && binder != nil {
		binder.SetEventPayloadValidator(payloadValidator)
	}
}

func NewRuntime(ctx context.Context, deps RuntimeDeps) (*Runtime, error) {
	boot, err := deps.validated()
	if err != nil {
		return nil, err
	}
	cfg := boot.Config
	stores := boot.Stores
	opts := boot.Options
	source := boot.Source
	mcpTurns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	rt := &Runtime{
		ownerID:        newRuntimeOwnerID(),
		Config:         cfg,
		Stores:         stores,
		Options:        opts,
		Workspace:      opts.WorkspaceLifecycle,
		MCPTurns:       mcpTurns,
		Authority:      boot.Authority,
		EmitRegistry:   boot.EmitRegistry,
		PromptResolver: boot.PromptResolver,
		Credentials:    boot.Credentials,
	}

	if stores.SQLDB != nil {
		rt.Logger = NewRuntimeLogger(stores.SQLDB, boot.RuntimeLogCapabilities)
	}
	payloadValidator := boot.payloadValidator(rt.Logger)
	boot.bindPayloadValidator(payloadValidator)
	bus, err := newRuntimeEventBus(stores.EventStore, rt.Logger, source, boot.TrimmedBundleFingerprint, func() []runtimebus.EventInterceptor {
		if rt.Pipeline == nil {
			return nil
		}
		return []runtimebus.EventInterceptor{rt.Pipeline}
	}, payloadValidator)
	if err != nil {
		return nil, fmt.Errorf("build event bus: %w", err)
	}
	rt.Bus = bus
	rt.RuntimeIngress = runtimeingress.NewController(stores.RuntimeIngressStore, rt.Bus, runtimeingress.Options{})
	rt.Bus.SetRuntimeIngressDispatchGate(rt.RuntimeIngress)
	if err := rt.RuntimeIngress.SyncState(ctx); err != nil {
		return nil, fmt.Errorf("sync runtime ingress state: %w", err)
	}
	if runControlStore, ok := stores.EventStore.(runtimeruncontrol.Store); ok && runControlStore != nil {
		rt.RunControl = runtimeruncontrol.NewController(runControlStore, rt.Bus, runtimeruncontrol.Options{})
		rt.Bus.SetRunDispatchGate(rt.RunControl)
	}

	var managerRef *runtimemanager.AgentManager
	rt.Scheduler = runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
		callbackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := rt.Bus.Publish(callbackCtx, scheduledEvent(sc)); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "publish_failed", rt.Logger.Error(callbackCtx, "scheduler", "publish_failed", map[string]any{
					"agent_id":   sc.AgentID,
					"event_type": sc.EventType,
					"run_id":     sc.EffectiveRunID(),
					"entity_id":  sc.EffectiveEntityID(),
				}, err))
			}
		}
		if stores.ScheduleStore != nil {
			if err := stores.ScheduleStore.CompleteScheduleFireExact(callbackCtx, sc); err != nil {
				if rt.Logger != nil {
					handleRuntimeLogPersistenceError("scheduler", "mark_fired_failed", rt.Logger.Error(callbackCtx, "scheduler", "mark_fired_failed", map[string]any{
						"agent_id":   sc.AgentID,
						"event_type": sc.EventType,
						"run_id":     sc.EffectiveRunID(),
						"entity_id":  sc.EffectiveEntityID(),
					}, err))
				}
			}
		}
	})
	if stores.SQLDB != nil {
		artifactRoot, err := runtimepipeline.ResolveArtifactRepoRoot("")
		if err != nil {
			return nil, fmt.Errorf("artifact repo root validation failed: %w", err)
		}
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
			TimerScheduler:          rt.Scheduler,
			TimerScheduleStore:      stores.ScheduleStore,
			EventReceiptsCapability: boot.EventReceiptCapability,
			ArtifactRoot:            artifactRoot,
			BundleFingerprint:       opts.BundleFingerprint,
		})
		if rt.Pipeline != nil {
			rt.SystemNodes = append(rt.SystemNodes, rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)...)
		}
	}

	if stores.SQLDB != nil {
		rt.Budget = NewBudgetTracker(stores.SQLDB, rt.Bus, cfg, stores.MailboxStore, rt.Logger, source)
	}

	backendProfile, err := cfg.LLMBackendProfile()
	if err != nil {
		return nil, err
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
		LLMBackend:        backendProfile.ID,
		Budget:            rt.Budget,
		ResetRuntimeOwnedState: func() {
			if rt.MCPTurns != nil {
				rt.MCPTurns.Reset()
			}
		},
		RuntimeShutdownAdmissionClosed: rt.shutdownAdmissionClosed,
		RuntimeIngressSafetyPause: func(ctx context.Context, reason string) error {
			_, err := rt.RuntimeIngress.SafetyPause(ctx, runtimeingress.TransitionRequest{
				Reason:       reason,
				ControlledBy: "runtime",
			})
			return err
		},
		ThrottleSuppressPrefixes: runtimeThrottleSuppressPrefixes(source),
		DisableSpinupControl:     true,
	}, stores.ManagerStore)
	managerRef = rt.Manager

	if stores.InboundStore != nil {
		rt.InboundGateway = NewInboundGateway(rt.Bus, rt.Logger, rt.shutdownAdmissionClosed, stores.InboundStore)
		rt.InboundGateway.SetRuntimeIngress(rt.RuntimeIngress)
	}
	if opts.EnableToolGateway {
		rt.ToolGateway = runtimemcp.NewGateway(rt.ToolExecutor, strings.TrimSpace(opts.ToolGatewayToken), RuntimeMCPGatewayHooks(rt.Logger, rt.RuntimeIngress, func(agentID string) (runtimeactors.AgentConfig, bool) {
			if rt.Manager == nil {
				return runtimeactors.AgentConfig{}, false
			}
			return rt.Manager.GetAgentConfig(strings.TrimSpace(agentID))
		}, rt.shutdownAdmissionClosed, rt.EmitRegistry, rt.MCPTurns))
	}

	return rt, nil
}

func runtimeLogSchemaCapabilities(stores Stores) runtimeLogCapabilityResolver {
	candidates := []any{
		stores.EventStore,
		stores.ManagerStore,
		stores.ScheduleStore,
		stores.MailboxStore,
		stores.InboundStore,
	}
	for _, candidate := range candidates {
		if provider, ok := candidate.(runtimeLogSchemaCapabilityProvider); ok && provider != nil {
			return provider
		}
	}
	return nil
}

func canonicalEventReceiptCapabilities(stores Stores) func(context.Context) (bool, error) {
	candidates := []any{
		stores.EventStore,
		stores.ManagerStore,
		stores.ScheduleStore,
		stores.MailboxStore,
		stores.InboundStore,
	}
	for _, candidate := range candidates {
		if provider, ok := candidate.(eventReceiptSchemaCapabilityProvider); ok && provider != nil {
			return provider.CanonicalEventReceiptsCapability
		}
	}
	return nil
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
		RunID:       sc.EffectiveRunID(),
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
	bootStartedAt := rt.Options.BootStartedAt
	if bootStartedAt.IsZero() {
		bootStartedAt = time.Now().UTC()
	}
	if rt.shutdownAdmissionClosed() {
		return fmt.Errorf("runtime shutdown already started")
	}
	rt.lifecycleMu.Lock()
	if rt.cancelStart != nil || rt.ownershipLease != nil {
		rt.lifecycleMu.Unlock()
		return fmt.Errorf("runtime already started")
	}
	startCtx, cancelStart := context.WithCancel(ctx)
	var lease runtimestartupownership.Lease
	if rt.Stores.StartupOwnership != nil {
		var err error
		lease, err = rt.Stores.StartupOwnership.AcquireRuntimeStartupOwnership(ctx, rt.ownerID)
		if err != nil {
			rt.emitBootProgress(5, "startup_ownership_lease", "FAILED", err.Error())
			cancelStart()
			rt.lifecycleMu.Unlock()
			return err
		}
		rt.emitBootProgress(5, "startup_ownership_lease", "ok", "owner="+rt.ownerID)
	} else {
		rt.emitBootProgress(5, "startup_ownership_lease", "skipped", "startup ownership store unavailable")
	}
	rt.startCtx = startCtx
	rt.cancelStart = cancelStart
	rt.ownershipLease = lease
	rt.lifecycleMu.Unlock()

	started := false
	defer func() {
		if started {
			return
		}
		rt.cleanupStartFailure()
	}()

	startupRecoverySnapshot, err := rt.inspectStartupRecoverySnapshot(ctx)
	startupRecoveryDecision := newStartupRecoveryDecisionReport(startupRecoverySnapshot)
	if err != nil {
		rt.emitBootProgress(6, "recovery_snapshot_inspection", "FAILED", err.Error())
	} else {
		rt.emitBootProgress(6, "recovery_snapshot_inspection", "ok", startupRecoverySnapshot.summary())
	}
	if err != nil && !startupRecoverySnapshot.RecoveryOnStartup {
		return err
	}
	if err != nil {
		startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
		startupRecoveryDecision.ReasonCode = startupRecoveryReasonInspectFailed
		startupRecoveryDecision.ErrorText = err.Error()
		startupRecoveryDecision.InspectionError = err.Error()
	}
	if denyErr := startupRecoveryDecision.denialError(); denyErr != nil {
		startupRecoveryDecision.ErrorText = denyErr.Error()
		rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
		rt.emitBootProgress(7, "recovery_decision", "FAILED", denyErr.Error())
		return denyErr
	}
	rt.emitBootProgress(7, "recovery_decision", string(startupRecoveryDecision.Outcome), string(startupRecoveryDecision.ReasonCode))

	if rt.Pipeline != nil {
		if _, err := rt.Pipeline.RepairContractEntityTypes(ctx); err != nil {
			rt.emitBootProgress(8, "entity_type_contract_repair", "FAILED", err.Error())
			return fmt.Errorf("repair contract entity types: %w", err)
		}
		go rt.Pipeline.RunMaintenance(startCtx)
		rt.emitBootProgress(8, "pipeline_maintenance", "started", "")
	} else {
		rt.emitBootProgress(8, "pipeline_maintenance", "skipped", "pipeline unavailable")
	}
	systemNodeCount := 0
	for _, node := range rt.SystemNodes {
		if node != nil {
			go node.Run(startCtx)
			systemNodeCount++
		}
	}
	rt.emitBootProgress(9, "system_nodes_start", "ok", fmt.Sprintf("%d nodes started", systemNodeCount))
	if rt.Scheduler != nil && rt.Stores.ScheduleStore != nil {
		schedules, err := rt.Stores.ScheduleStore.LoadActiveSchedules(ctx)
		if err != nil {
			rt.emitBootProgress(10, "schedule_restoration", "FAILED", err.Error())
			return fmt.Errorf("load schedules failed: %w", err)
		}
		startupRecoveryDecision.ScheduleRestoreAttempted = len(schedules) > 0
		results := restoreStartupTimerSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Logger, schedules)
		timerReplayCount, timerSkipCount, timerDropCount, timerRecoveryErrText := summarizeStartupTimerRecovery(results)
		startupRecoveryDecision.ScheduleReplayCount = timerReplayCount
		startupRecoveryDecision.ScheduleSkipCount = timerSkipCount
		startupRecoveryDecision.ScheduleDropCount = timerDropCount
		if err := ensureLifecycleWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "ensure_lifecycle_failed", rt.Logger.Error(ctx, "scheduler", "ensure_lifecycle_failed", nil, err))
			}
		}
		if err := ensureRecurringWorkflowSchedules(ctx, rt.Stores.ScheduleStore, rt.Scheduler, rt.Pipeline); err != nil {
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("scheduler", "ensure_recurring_failed", rt.Logger.Error(ctx, "scheduler", "ensure_recurring_failed", nil, err))
			}
		}
		if startupRecoveryDecision.Outcome != startupRecoveryOutcomeDegraded && startupRecoveryDecision.ScheduleDropCount > 0 {
			startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
			startupRecoveryDecision.ReasonCode = startupRecoveryReasonScheduleRestore
			if strings.TrimSpace(timerRecoveryErrText) != "" {
				startupRecoveryDecision.ErrorText = timerRecoveryErrText
			} else {
				startupRecoveryDecision.ErrorText = fmt.Sprintf("failed to restore %d active schedule(s)", startupRecoveryDecision.ScheduleDropCount)
			}
		}
		rt.emitBootProgress(10, "schedule_restoration", "ok", fmt.Sprintf("%d schedules restored, %d skipped, %d dropped", startupRecoveryDecision.ScheduleReplayCount, startupRecoveryDecision.ScheduleSkipCount, startupRecoveryDecision.ScheduleDropCount))
	} else {
		rt.emitBootProgress(10, "schedule_restoration", "skipped", "scheduler or schedule store unavailable")
	}
	if rt.Config.Runtime.RecoveryOnStartup && rt.Manager != nil {
		startupRecoveryDecision.ManagerRecoveryAttempted = true
		managerReplaySummary, err := rt.Manager.RecoverWithStartupReplayDiagnostics(ctx)
		startupRecoveryDecision.ManagerReplayCount = managerReplaySummary.ReplayedCount
		startupRecoveryDecision.ManagerSkipCount = managerReplaySummary.SkippedCount
		startupRecoveryDecision.ManagerDropCount = managerReplaySummary.DroppedCount
		if err != nil {
			startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
			startupRecoveryDecision.ReasonCode = startupRecoveryReasonRecoverFailed
			startupRecoveryDecision.ErrorText = err.Error()
			if rt.Logger != nil {
				handleRuntimeLogPersistenceError("runtime", "recovery_failed", rt.Logger.Error(ctx, "runtime", "recovery_failed", nil, err))
			}
			startupRecoveryDecision.ManagerResetAttempted = true
			if resetErr := rt.Manager.ResetRuntimeStateWithSource("startup_recovery_failed"); resetErr != nil {
				startupRecoveryDecision.ManagerResetError = resetErr.Error()
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
		} else if startupRecoveryDecision.Outcome != startupRecoveryOutcomeDegraded && startupRecoveryDecision.ManagerDropCount > 0 {
			startupRecoveryDecision.Outcome = startupRecoveryOutcomeDegraded
			startupRecoveryDecision.ReasonCode = startupRecoveryReasonRecoverFailed
			if strings.TrimSpace(managerReplaySummary.FirstDroppedError) != "" {
				startupRecoveryDecision.ErrorText = strings.TrimSpace(managerReplaySummary.FirstDroppedError)
			} else {
				startupRecoveryDecision.ErrorText = fmt.Sprintf("failed to replay %d pending event(s) during startup recovery", startupRecoveryDecision.ManagerDropCount)
			}
		}
		status := "ok"
		if startupRecoveryDecision.Outcome == startupRecoveryOutcomeDegraded {
			status = string(startupRecoveryDecision.Outcome)
		}
		rt.emitBootProgress(11, "manager_recovery_if_enabled", status, fmt.Sprintf("%d replayed, %d skipped, %d dropped", startupRecoveryDecision.ManagerReplayCount, startupRecoveryDecision.ManagerSkipCount, startupRecoveryDecision.ManagerDropCount))
	} else {
		rt.emitBootProgress(11, "manager_recovery_if_enabled", "skipped", "recovery_on_startup disabled or manager unavailable")
	}
	rt.logStartupRecoveryDecision(ctx, startupRecoveryDecision)
	if rt.Bus != nil {
		rt.Bus.StartOutboxSweeper(startCtx, runtimebus.DefaultOutboxSweeperConfig())
		rt.emitBootProgress(12, "outbox_sweeper", "started", "")
	} else {
		rt.emitBootProgress(12, "outbox_sweeper", "skipped", "event bus unavailable")
	}
	staticAgentIDs := []string{}
	if rt.Manager != nil {
		staticAgentIDs, err = staticBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(13, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		if err := rt.Manager.EnsureStaticAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(13, "static_agents_bootstrap", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static agents: %w", err)
		}
		rt.emitBootProgress(13, "static_agents_bootstrap", "ok", fmt.Sprintf("%d static agents", len(staticAgentIDs)))
	} else {
		rt.emitBootProgress(13, "static_agents_bootstrap", "skipped", "manager unavailable")
	}
	flowRequiredAgentIDs := []string{}
	if rt.Manager != nil {
		flowRequiredAgentIDs, err = staticFlowRequiredBootAgentIDs(rt.Options.WorkflowModule.SemanticSource())
		if err != nil {
			rt.emitBootProgress(14, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		if err := rt.Manager.EnsureStaticFlowRequiredAgents(ctx, rt.Options.WorkflowModule.SemanticSource()); err != nil {
			rt.emitBootProgress(14, "flow_required_agents", "FAILED", err.Error())
			return fmt.Errorf("bootstrap static flow required agents: %w", err)
		}
		rt.emitBootProgress(14, "flow_required_agents", "ok", fmt.Sprintf("%d flow-required agents", len(flowRequiredAgentIDs)))
	} else {
		rt.emitBootProgress(14, "flow_required_agents", "skipped", "manager unavailable")
	}
	source := rt.Options.WorkflowModule.SemanticSource()
	if err := validateClaudeStartupConfigForActiveAgents(rt.Config, rt.Options, source, rt.Manager); err != nil {
		rt.emitBootProgress(15, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime startup validation failed: %w", err)
	}
	if err := validateClaudeManagedAgentWorkspaces(ctx, rt.Config, source, rt.Workspace, rt.Manager); err != nil {
		rt.emitBootProgress(15, "workspace_validation_and_system_containers", "FAILED", err.Error())
		return fmt.Errorf("claude runtime workspace validation failed: %w", err)
	}
	rt.emitBootProgress(15, "workspace_validation_and_system_containers", "ok", fmt.Sprintf("%d system containers", len(rt.Options.SystemContainers)))
	startupProbe, _ := llm.StartupVisibleToolSurfaceProberForRuntime(rt.LLM)
	if err := validateClaudeMCPToolsForManagedAgents(ctx, rt.Config, source, startupProbe, rt.MCPTurns, rt.ToolExecutor, rt.Manager); err != nil {
		rt.emitBootProgress(16, "mcp_tool_validation", "FAILED", err.Error())
		return fmt.Errorf("claude runtime mcp validation failed: %w", err)
	}
	rt.emitBootProgress(16, "mcp_tool_validation", "ok", "")
	if rt.Manager != nil {
		rt.Manager.Run(startCtx)
		rt.emitBootProgress(17, "manager_event_loop_start", "ok", "")
	} else {
		rt.emitBootProgress(17, "manager_event_loop_start", "skipped", "manager unavailable")
	}
	if rt.Stores.SQLDB != nil && rt.Logger != nil {
	}
	var bootCheck <-chan events.Event
	if rt.Options.SelfCheck && rt.Bus != nil {
		bootCheck = rt.Bus.SubscribeInternal("bootstrap-self-check", events.EventType("platform.boot"))
		rt.emitBootProgress(18, "boot_self_check_optional", "ok", "platform.boot self-check subscribed")
	} else {
		rt.emitBootProgress(18, "boot_self_check_optional", "skipped", "self-check disabled or event bus unavailable")
	}
	bootEventID, err := rt.publishBootCompleted(context.Background(), bootCompletedReport{
		StartedAt:                 bootStartedAt,
		RecoveryDecision:          startupRecoveryDecision,
		StaticAgentsStarted:       staticAgentIDs,
		FlowRequiredAgentsStarted: flowRequiredAgentIDs,
		SystemContainersStarted:   rt.Options.SystemContainers,
		SelfCheckRequired:         rt.Options.SelfCheck,
	})
	if err != nil {
		rt.emitBootProgress(19, "platform_boot_event_published", "FAILED", err.Error())
		return fmt.Errorf("publish platform.boot: %w", err)
	}
	rt.emitBootProgress(19, "platform_boot_event_published", "ok", bootEventID)
	if rt.Options.SelfCheck {
		if err := rt.verifyBootPublished(bootCheck); err != nil {
			rt.emitBootProgress(18, "boot_self_check_optional", "FAILED", err.Error())
			return fmt.Errorf("self-check failed: %w", err)
		}
	}
	started = true
	return nil
}

func (rt *Runtime) Shutdown() error {
	return rt.ShutdownWithOptions(DefaultShutdownOptions())
}

func (rt *Runtime) ShutdownWithOptions(opts ShutdownOptions) error {
	if rt == nil {
		return nil
	}
	grace, err := runtimemanager.ResolveShutdownGrace(opts.Grace)
	if err != nil {
		return err
	}
	rt.shutdownGate.Close()
	var shutdownErr error
	if rt.Manager != nil {
		managerOpts := runtimemanager.ShutdownOptions{Grace: grace}
		if err := rt.Manager.ShutdownWithOptions(managerOpts); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	if rt.Scheduler != nil {
		rt.Scheduler.Stop()
	}
	if rt.Stores.ScheduleStore != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := rt.Stores.ScheduleStore.ReleaseScheduleClaims(releaseCtx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
		cancel()
	}
	rt.lifecycleMu.Lock()
	cancelStart := rt.cancelStart
	lease := rt.ownershipLease
	rt.cancelStart = nil
	rt.startCtx = nil
	rt.ownershipLease = nil
	rt.lifecycleMu.Unlock()
	if cancelStart != nil {
		cancelStart()
	}
	if lease != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := lease.Release(releaseCtx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
		cancel()
	}
	return shutdownErr
}

func (rt *Runtime) cleanupStartFailure() {
	if rt == nil {
		return
	}
	if rt.Manager != nil {
		_ = rt.Manager.Shutdown()
	}
	if rt.Scheduler != nil {
		rt.Scheduler.Stop()
	}
	if rt.Stores.ScheduleStore != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = rt.Stores.ScheduleStore.ReleaseScheduleClaims(releaseCtx)
		cancel()
	}
	rt.lifecycleMu.Lock()
	cancelStart := rt.cancelStart
	lease := rt.ownershipLease
	rt.cancelStart = nil
	rt.startCtx = nil
	rt.ownershipLease = nil
	rt.lifecycleMu.Unlock()
	if cancelStart != nil {
		cancelStart()
	}
	if lease != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = lease.Release(releaseCtx)
		cancel()
	}
}

func (rt *Runtime) Wait(ctx context.Context) {
	<-ctx.Done()
}

type bootCompletedReport struct {
	StartedAt                 time.Time
	RecoveryDecision          startupRecoveryDecisionReport
	StaticAgentsStarted       []string
	FlowRequiredAgentsStarted []string
	SystemContainersStarted   []string
	SelfCheckRequired         bool
}

func staticBootAgentIDs(source semanticview.Source) ([]string, error) {
	records, err := runtimemanager.StaticAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	return persistedBootAgentIDs(records), nil
}

func staticFlowRequiredBootAgentIDs(source semanticview.Source) ([]string, error) {
	records, err := runtimemanager.StaticFlowRequiredAgentMaterializationRecords(source)
	if err != nil {
		return nil, err
	}
	return persistedBootAgentIDs(records), nil
}

func persistedBootAgentIDs(records []runtimemanager.PersistedAgent) []string {
	out := make([]string, 0, len(records))
	for _, rec := range records {
		if id := strings.TrimSpace(rec.Config.ID); id != "" {
			out = append(out, id)
		}
	}
	return sortedNonEmptyStrings(out)
}

func sortedNonEmptyStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

func (rt *Runtime) publishBootCompleted(ctx context.Context, report bootCompletedReport) (string, error) {
	if rt == nil || rt.Bus == nil {
		return "", nil
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
	completedAt := time.Now().UTC()
	startedAt := report.StartedAt
	if startedAt.IsZero() {
		startedAt = completedAt
	}
	durationMS := completedAt.Sub(startedAt).Milliseconds()
	if durationMS < 0 {
		durationMS = 0
	}
	payload := mustJSON(map[string]any{
		"flow_count":                   flowCount,
		"node_count":                   nodeCount,
		"agent_count":                  agentCount,
		"event_count":                  eventCount,
		"timestamp":                    completedAt.Format(time.RFC3339Nano),
		"boot_started_at":              startedAt.Format(time.RFC3339Nano),
		"boot_completed_at":            completedAt.Format(time.RFC3339Nano),
		"duration_ms":                  durationMS,
		"bundle_fingerprint":           strings.TrimSpace(rt.Options.BundleFingerprint),
		"recovery_decision":            report.RecoveryDecision.bootPayload(),
		"static_agents_started":        sortedNonEmptyStrings(report.StaticAgentsStarted),
		"flow_required_agents_started": sortedNonEmptyStrings(report.FlowRequiredAgentsStarted),
		"system_containers_started":    sortedNonEmptyStrings(report.SystemContainersStarted),
		"self_check_required":          report.SelfCheckRequired,
		"self_check_passed":            nil,
	})
	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        t,
		SourceAgent: "runtime",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	return eventID, rt.Bus.Publish(ctx, evt)
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

func ensureRecurringWorkflowSchedules(ctx context.Context, store runtimepipeline.SchedulePersistence, scheduler *runtimepipeline.Scheduler, workflow runtimepipeline.WorkflowRuntime) error {
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
		sc := runtimepipeline.Schedule{
			RunID:     runtimecorrelation.RunIDFromContext(ctx),
			AgentID:   owner,
			EventType: eventType,
			Mode:      "cron",
			Cron:      cron,
			Payload:   recurringWorkflowTimerPayload(timer),
		}
		if err := store.UpsertSchedule(ctx, sc); err != nil {
			return err
		}
		if _, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, scheduler, sc); err != nil {
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
				RunID:        runtimecorrelation.RunIDFromContext(ctx),
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
				if _, err := runtimepipeline.ClaimAndRegisterSchedule(ctx, store, scheduler, sc); err != nil {
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
