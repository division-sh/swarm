package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"swarm/internal/config"
	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/core/toolcapabilities"
	runtimecredentials "swarm/internal/runtime/credentials"
	llm "swarm/internal/runtime/llm"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

type Executor struct {
	mu              sync.RWMutex
	manager         Manager
	managerProvider ManagerProvider
	sqlDB           *sql.DB
	bus             EventPublisher
	scheduler       Scheduler
	scheduleStore   SchedulePersistence
	mailboxStore    MailboxPersistence
	cfg             *config.Config
	credentials     runtimecredentials.Store
	httpClient      *http.Client
	mcpClient       *runtimemcp.Client
	workflowSource  semanticview.Source
	flowActivator   runtimepipeline.FlowInstanceActivator
	workspaces      workspace.Resolver
	authorizer      *ToolAuthorizer
	validator       *ToolInputValidator
	dispatcher      *ToolDispatcher
	oneShotMu       sync.Mutex
	oneShotEmits    map[string]struct{}
}

type runtimeToolLogSink interface {
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry)
}

func NewExecutor(bus EventPublisher, scheduler Scheduler, manager Manager, stores ...SchedulePersistence) *Executor {
	return NewExecutorWithOptions(bus, scheduler, ExecutorOptions{Manager: manager}, stores...)
}

func NewExecutorWithOptions(bus EventPublisher, scheduler Scheduler, opts ExecutorOptions, stores ...SchedulePersistence) *Executor {
	var scheduleStore SchedulePersistence
	if len(stores) > 0 {
		scheduleStore = stores[0]
	}
	exec := &Executor{
		manager:         opts.Manager,
		managerProvider: opts.ManagerProvider,
		bus:             bus,
		scheduler:       scheduler,
		scheduleStore:   scheduleStore,
		mailboxStore:    opts.MailboxStore,
		sqlDB:           opts.SQLDB,
		cfg:             opts.Config,
		credentials:     opts.Credentials,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		mcpClient:       opts.MCPClient,
		workflowSource:  opts.WorkflowSource,
		flowActivator:   opts.FlowActivator,
		workspaces:      opts.WorkspaceResolver,
		oneShotEmits:    make(map[string]struct{}),
	}
	if exec.credentials == nil {
		exec.credentials = runtimecredentials.NewEnvStore()
	}
	if exec.mcpClient == nil {
		exec.mcpClient = runtimemcp.NewClient(exec.credentials)
	}
	if exec.mcpClient != nil {
		for _, err := range exec.mcpClient.Refresh(context.Background(), exec.workflowSource) {
			processWarn("tool-executor", "mcp discovery warning: %v", err)
		}
	}
	exec.authorizer = NewToolAuthorizer(bus)
	exec.validator = NewToolInputValidator(exec.contractDefinitions)
	exec.dispatcher = NewToolDispatcher(
		func(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
			return exec.handleEmitTool(ctx, actor, name, input)
		},
		func(actor models.AgentConfig, name string) (RegisteredTool, bool, error) {
			return exec.resolveRegisteredTool(actor, name)
		},
		func(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error) {
			return exec.execHTTPTool(ctx, actor, tool, input)
		},
		func(ctx context.Context, actor models.AgentConfig, tool RegisteredTool, input any) (any, error) {
			return exec.execMCPTool(ctx, actor, tool, input)
		},
		exec.buildToolHandlers(),
	)
	return exec
}

func (e *Executor) contractDefinitions() ([]llm.ToolDefinition, error) {
	e.mu.RLock()
	source := e.workflowSource
	client := e.mcpClient
	e.mu.RUnlock()
	var discovered map[string]runtimemcp.DiscoveredTool
	if client != nil {
		discovered = client.DiscoveredTools()
	}
	return toolDefinitionsForRuntime(source, discovered)
}

func (e *Executor) SetWorkflowSource(source semanticview.Source) {
	e.mu.Lock()
	e.workflowSource = source
	client := e.mcpClient
	e.mu.Unlock()
	if client != nil {
		for _, err := range client.Refresh(context.Background(), source) {
			processWarn("tool-executor", "mcp discovery warning: %v", err)
		}
	}
}

func (e *Executor) SetManager(manager Manager) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if manager == nil {
		e.manager = nil
		return
	}
	e.manager = manager
	e.managerProvider = nil
}

func (e *Executor) SetFlowActivator(activator runtimepipeline.FlowInstanceActivator) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flowActivator = activator
}

func (e *Executor) SetConfig(cfg *config.Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
}

func (e *Executor) resolveRegisteredTool(actor models.AgentConfig, name string) (RegisteredTool, bool, error) {
	name = normalizeNativeToolName(name)
	e.mu.RLock()
	source := e.workflowSource
	client := e.mcpClient
	e.mu.RUnlock()
	var discovered map[string]runtimemcp.DiscoveredTool
	if client != nil {
		discovered = client.DiscoveredTools()
	}
	return resolveRegisteredToolForActor(source, actor, name, discovered)
}

func (e *Executor) ToolDefinitions() []llm.ToolDefinition {
	defs, err := e.contractDefinitions()
	if err != nil {
		processWarn("tool-executor", "failed to load contract tool definitions: %v", err)
		return nil
	}
	return defs
}

func (e *Executor) ToolDefinitionsForActor(actor models.AgentConfig) []llm.ToolDefinition {
	e.mu.RLock()
	source := e.workflowSource
	client := e.mcpClient
	e.mu.RUnlock()
	var discovered map[string]runtimemcp.DiscoveredTool
	if client != nil {
		discovered = client.DiscoveredTools()
	}
	entries, err := registeredToolsForActor(source, actor, discovered)
	if err != nil {
		processWarn("tool-executor", "failed to load actor-scoped contract tool definitions: %v", err)
		return nil
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	filtered := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		entry := entries[name]
		if !classifyToolAuthorization(actor, name).allowed {
			continue
		}
		filtered = append(filtered, llm.ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	filtered = append(filtered, GenerateEmitToolsForActor(actor, func(_ string, component string, format string, args ...any) {
		processWarn(component, format, args...)
	})...)
	return filtered
}

func (e *Executor) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	return capabilitySetForActor(actor, names, requestAllowed)
}

func (e *Executor) Execute(ctx context.Context, name string, input any) (any, error) {
	actor, ok := ActorFromContext(ctx)
	if !ok {
		err := errors.New("missing actor context for tool execution")
		e.emitToolExecutionEvent(ctx, models.AgentConfig{}, name, input, nil, err, 0, "context")
		return nil, err
	}
	name = normalizeNativeToolName(strings.TrimSpace(name))
	if err := e.authorizeToolUsage(ctx, actor, name); err != nil {
		e.emitToolExecutionEvent(ctx, actor, name, input, nil, err, 0, "authorize")
		return nil, err
	}
	if err := e.validateRuntimeToolInput(name, input); err != nil {
		wrapped := WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"execute.validate_runtime_tool_input",
			false,
			err,
			"runtime tool input validation failed",
		)
		e.emitToolExecutionEvent(ctx, actor, name, input, nil, wrapped, 0, "validate")
		return nil, wrapped
	}
	input = normalizeRuntimeToolInput(name, input)
	start := time.Now()
	out, err := e.dispatchTool(ctx, actor, name, input)
	e.emitToolExecutionEvent(ctx, actor, name, input, out, err, time.Since(start), "dispatch")
	return out, err
}

func (e *Executor) validateRuntimeToolInput(name string, input any) error {
	if e.validator == nil {
		return nil
	}
	return e.validator.Validate(name, input)
}

func runtimeToolSchemaForName(defs []llm.ToolDefinition, name string) (map[string]any, bool) {
	name = strings.TrimSpace(name)
	for _, def := range defs {
		if strings.TrimSpace(def.Name) != name {
			continue
		}
		schema, ok := def.Schema.(map[string]any)
		return schema, ok
	}
	return nil, false
}

func pruneSchemaUnknownKeys(payload map[string]any, schema map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	props := schemaProperties(schema["properties"])
	if len(props) == 0 {
		return payload
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		if _, ok := props[key]; ok {
			out[key] = value
		}
	}
	return out
}

func payloadTouchesSchemaProps(payload map[string]any, schema map[string]any) bool {
	if len(payload) == 0 || schema == nil {
		return false
	}
	props := schemaProperties(schema["properties"])
	for key := range payload {
		if _, ok := props[key]; ok {
			return true
		}
	}
	return false
}

func payloadHasLegacyOnlyProps(payload map[string]any, schema map[string]any) bool {
	if len(payload) == 0 || schema == nil {
		return false
	}
	props := schemaProperties(schema["properties"])
	for key := range payload {
		if _, ok := props[key]; !ok {
			return true
		}
	}
	return false
}

func toolAllowsLegacySubsetFallback(name string) bool {
	switch strings.TrimSpace(name) {
	case "agent_message",
		"agent_fire",
		"mailbox_send",
		"human_task_request",
		"human_task_decide":
		return true
	default:
		return false
	}
}

func normalizeRuntimeToolInput(name string, input any) any {
	return canonicalRuntimeToolInput(name, input)
}

func (e *Executor) emitToolExecutionEvent(
	ctx context.Context,
	actor models.AgentConfig,
	toolName string,
	input any,
	result any,
	execErr error,
	latency time.Duration,
	phase string,
) {
	if e == nil || e.bus == nil {
		return
	}
	logger, ok := e.bus.(runtimeToolLogSink)
	if !ok || logger == nil {
		return
	}
	toolName = normalizeNativeToolName(toolName)
	level := "info"
	action := "tool_execution_succeeded"
	if execErr != nil {
		level = "warn"
		if errors.Is(execErr, ErrToolNotAllowed) {
			action = "tool_execution_denied"
		} else {
			action = "tool_execution_failed"
		}
	}
	detail := toolExecutionDiagnosticDetail(ctx, actor, toolName, input, result, execErr, phase)
	if strings.TrimSpace(actor.Role) != "" {
		detail["actor_role"] = strings.TrimSpace(actor.Role)
	}
	logger.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:      level,
		Message:    toolExecutionMessage(toolName, execErr),
		Component:  "tool-executor",
		Action:     action,
		AgentID:    strings.TrimSpace(actor.ID),
		EntityID:   strings.TrimSpace(actor.EffectiveEntityID()),
		Detail:     detail,
		Error:      toolExecErrorText(execErr),
		DurationUS: int(latency / time.Microsecond),
	})
}

func toolExecutionMessage(toolName string, execErr error) string {
	toolName = strings.TrimSpace(toolName)
	switch {
	case execErr == nil:
		if toolName == "" {
			return "Tool execution succeeded"
		}
		return fmt.Sprintf("Tool %s executed successfully", toolName)
	case errors.Is(execErr, ErrToolNotAllowed):
		if toolName == "" {
			return "Tool execution was denied"
		}
		return fmt.Sprintf("Tool %s execution was denied", toolName)
	default:
		if toolName == "" {
			return "Tool execution failed"
		}
		return fmt.Sprintf("Tool %s execution failed", toolName)
	}
}

func toolExecutionDiagnosticDetail(ctx context.Context, actor models.AgentConfig, toolName string, input any, result any, execErr error, phase string) map[string]any {
	detail := map[string]any{
		"tool_name":     toolName,
		"phase":         strings.TrimSpace(phase),
		"ok":            execErr == nil,
		"input_summary": SafeTelemetryText(input),
	}
	if result != nil {
		detail["result_summary"] = SafeTelemetryText(result)
	}
	if set, ok := toolcapabilities.FromContext(ctx); ok {
		if cap, ok := set.Capability(toolName); ok {
			if v := string(cap.Kind); v != "" {
				detail["tool_kind"] = v
			}
			detail["visible"] = cap.Visible
			detail["callable"] = cap.Callable
			if v := string(cap.ContextRequirement); v != "" {
				detail["context_requirement"] = v
			}
			if v := strings.TrimSpace(cap.AuthorizationClass); v != "" {
				detail["authorization_class"] = v
			}
			if v := strings.TrimSpace(cap.DenialReason); v != "" {
				detail["denial_reason"] = v
			}
			if execErr != nil && !cap.Callable {
				detail["denial_layer"] = "executor"
			}
		} else {
			decision := classifyToolAuthorization(actor, toolName)
			detail["tool_kind"] = string(toolKindPolicy(toolName))
			detail["context_requirement"] = string(toolContextRequirementPolicy(toolName))
			if v := strings.TrimSpace(string(decision.class)); v != "" {
				detail["authorization_class"] = v
			}
			if execErr != nil && !decision.allowed {
				detail["denial_reason"] = "tool_not_allowed"
				detail["denial_layer"] = "authorizer"
			}
		}
	} else {
		decision := classifyToolAuthorization(actor, toolName)
		detail["tool_kind"] = string(toolKindPolicy(toolName))
		detail["context_requirement"] = string(toolContextRequirementPolicy(toolName))
		if v := strings.TrimSpace(string(decision.class)); v != "" {
			detail["authorization_class"] = v
		}
		if execErr != nil && !decision.allowed {
			detail["denial_reason"] = "tool_not_allowed"
			detail["denial_layer"] = "authorizer"
		}
	}
	if runtimeErr, ok := AsRuntimeError(execErr); ok {
		if v := strings.TrimSpace(runtimeErr.Code); v != "" {
			detail["runtime_error_code"] = v
		}
		if v := strings.TrimSpace(runtimeErr.Operation); v != "" {
			detail["runtime_error_operation"] = v
		}
		if v := strings.TrimSpace(runtimeErr.Component); v != "" {
			detail["runtime_error_component"] = v
		}
		detail["retryable"] = runtimeErr.Retryable
	}
	return detail
}

func toolExecErrorText(err error) string {
	if err == nil {
		return ""
	}
	return SafeTelemetryText(FormatRuntimeError(err))
}

func (e *Executor) authorizeToolUsage(ctx context.Context, actor models.AgentConfig, toolName string) error {
	if set, ok := toolcapabilities.FromContext(ctx); ok {
		if cap, ok := set.Capability(toolName); ok {
			if cap.Callable {
				return nil
			}
			return fmt.Errorf("%w: tool %s is not allowed for agent %s", ErrToolNotAllowed, toolName, actor.ID)
		}
		return fmt.Errorf("%w: tool %s is not offered for agent %s", ErrToolNotAllowed, toolName, actor.ID)
	}
	if e.authorizer == nil {
		return nil
	}
	return e.authorizer.Authorize(ctx, actor, toolName)
}

func (e *Executor) dispatchTool(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
	if e.dispatcher == nil {
		return nil, fmt.Errorf("tool dispatcher is not configured")
	}
	return e.dispatcher.Dispatch(ctx, actor, name, input)
}

func (e *Executor) runtimeLogSink() runtimeToolLogSink {
	if e == nil || e.bus == nil {
		return nil
	}
	logger, ok := e.bus.(runtimeToolLogSink)
	if !ok || logger == nil {
		return nil
	}
	return logger
}

func (e *Executor) getManager() Manager {
	e.mu.RLock()
	manager := e.manager
	provider := e.managerProvider
	e.mu.RUnlock()
	if manager != nil {
		return manager
	}
	if provider != nil {
		return provider()
	}
	return nil
}

func decodeToolInput(input any, out any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	return nil
}

func (e *Executor) SetMailboxStore(store MailboxPersistence) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mailboxStore = store
}

func (e *Executor) SetSQLDB(db *sql.DB) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sqlDB = db
}

func (e *Executor) ValidateRuntimeToolInputForTest(name string, input any) error {
	return e.validateRuntimeToolInput(name, input)
}

func (e *Executor) ExecAgentMessageDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execAgentMessage(ctx, actor, input)
}

func (e *Executor) ExecScheduleDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execSchedule(context.Background(), actor, input)
}

func (e *Executor) ExecConfigureRoutingDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execConfigureRouting(context.Background(), actor, input)
}

func (e *Executor) ExecAgentHireDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execAgentHire(actor, input)
}

func (e *Executor) ExecAgentFireDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execAgentFire(actor, input)
}

func (e *Executor) ExecAgentReconfigureDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execAgentReconfigure(actor, input)
}

func (e *Executor) ExecMailboxSendDirect(actor models.AgentConfig, input any) (any, error) {
	return e.execMailboxSend(context.Background(), actor, input)
}

func (e *Executor) ExecHumanTaskRequestDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execHumanTaskRequest(ctx, actor, input)
}

func (e *Executor) ExecHumanTaskDecideDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execHumanTaskDecide(ctx, actor, input)
}

func authorizeRouting(actor, target models.AgentConfig, status string) error {
	return runtimeauthority.Active().AuthorizeRouting(actor, target, status)
}

func authorizeManage(actor, target models.AgentConfig, manager Manager) error {
	_ = manager
	if !runtimeauthority.SameFlowInstance(actor, target) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	return runtimeauthority.Active().AuthorizeManagement(actor, target)
}

func authorizeMailboxSend(actor models.AgentConfig) error {
	return runtimeauthority.Active().AuthorizeMailboxSend(actor)
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
