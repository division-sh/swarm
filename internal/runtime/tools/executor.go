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
	"swarm/internal/runtime/diaglog"
	llm "swarm/internal/runtime/llm"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	workspace "swarm/internal/runtime/workspace"
)

type Executor struct {
	mu                             sync.RWMutex
	manager                        Manager
	managerProvider                ManagerProvider
	sqlDB                          *sql.DB
	bus                            EventPublisher
	scheduler                      Scheduler
	scheduleStore                  SchedulePersistence
	mailboxStore                   MailboxPersistence
	cfg                            *config.Config
	credentials                    runtimecredentials.Store
	httpClient                     *http.Client
	mcpClient                      *runtimemcp.Client
	workflowSource                 semanticview.Source
	workspaces                     workspace.Resolver
	authority                      runtimeauthority.Provider
	emitRegistry                   *EmitRegistry
	authorizer                     *ToolAuthorizer
	validator                      *ToolInputValidator
	dispatcher                     *ToolDispatcher
	allowInternalLegacyEntityTools bool
	oneShotMu                      sync.Mutex
	oneShotEmits                   map[string]struct{}
}

type runtimeToolLogSink interface {
	LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error
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
		manager:                        opts.Manager,
		managerProvider:                opts.ManagerProvider,
		bus:                            bus,
		scheduler:                      scheduler,
		scheduleStore:                  scheduleStore,
		mailboxStore:                   opts.MailboxStore,
		sqlDB:                          opts.SQLDB,
		cfg:                            opts.Config,
		credentials:                    opts.Credentials,
		httpClient:                     &http.Client{Timeout: 30 * time.Second},
		mcpClient:                      opts.MCPClient,
		workflowSource:                 opts.WorkflowSource,
		workspaces:                     opts.WorkspaceResolver,
		authority:                      runtimeauthority.ProviderOrNoop(opts.AuthorityProvider),
		allowInternalLegacyEntityTools: opts.AllowInternalLegacyEntityTools,
		oneShotEmits:                   make(map[string]struct{}),
	}
	if opts.EmitRegistry != nil {
		exec.emitRegistry = opts.EmitRegistry
	} else {
		exec.emitRegistry = NewEmitRegistry(exec.workflowSource, exec.authority)
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
	exec.authorizer = NewToolAuthorizer(bus, exec.toolAuthorizationDecision)
	exec.validator = NewToolInputValidator(exec.contractDefinitionsForActor)
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
		exec.dispatchRoleScopedEntityTool,
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

func (e *Executor) contractDefinitionsForActor(actor *models.AgentConfig) ([]llm.ToolDefinition, error) {
	if actor == nil {
		return e.contractDefinitions()
	}
	e.mu.RLock()
	source := e.workflowSource
	client := e.mcpClient
	e.mu.RUnlock()
	var discovered map[string]runtimemcp.DiscoveredTool
	if client != nil {
		discovered = client.DiscoveredTools()
	}
	return toolDefinitionsForActor(source, *actor, discovered)
}

func (e *Executor) resolveRegisteredTool(actor models.AgentConfig, name string) (RegisteredTool, bool, error) {
	name = normalizeNativeToolName(name)
	e.mu.RLock()
	source := e.workflowSource
	client := e.mcpClient
	allowInternalLegacy := e.allowInternalLegacyEntityTools
	e.mu.RUnlock()
	var discovered map[string]runtimemcp.DiscoveredTool
	if client != nil {
		discovered = client.DiscoveredTools()
	}
	if allowInternalLegacy && IsLegacyEntityToolSurfaceName(name) {
		entries, err := registeredToolsForRuntime(source, discovered)
		if err != nil {
			return RegisteredTool{}, false, err
		}
		tool, ok := entries[name]
		return tool, ok, nil
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
		if !e.toolAuthorizationDecision(actor, name).allowed {
			continue
		}
		filtered = append(filtered, llm.ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Usage:       strings.TrimSpace(entry.Usage),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	if e.emitRegistry != nil {
		filtered = append(filtered, e.emitRegistry.GenerateEmitToolsForActor(actor, func(_ string, component string, format string, args ...any) {
			processWarn(component, format, args...)
		})...)
	}
	return filtered
}

func (e *Executor) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	seen := map[string]struct{}{}
	for _, raw := range names {
		name := normalizeNativeToolName(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		decision := e.toolAuthorizationDecision(actor, name)
		cap := toolcapabilities.Capability{
			Name:               name,
			Kind:               toolKindPolicy(name),
			Visible:            decision.allowed,
			Callable:           decision.allowed,
			ContextRequirement: toolContextRequirementPolicy(name),
			AuthorizationClass: string(decision.class),
		}
		if len(requestAllowed) > 0 {
			if _, ok := requestAllowed[name]; !ok {
				cap.Visible = false
				cap.Callable = false
				cap.DenialReason = "request_not_allowed"
				caps = append(caps, cap)
				continue
			}
		}
		if !decision.allowed {
			cap.DenialReason = "tool_not_allowed"
		}
		caps = append(caps, cap)
	}
	return toolcapabilities.NewSet(caps)
}

func (e *Executor) toolAuthorizationDecision(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
	toolName = normalizeNativeToolName(toolName)
	e.mu.RLock()
	source := e.workflowSource
	allowInternalLegacy := e.allowInternalLegacyEntityTools
	e.mu.RUnlock()
	if _, legacy := legacyEntityToolSurfaceNames[toolName]; legacy && !allowInternalLegacy {
		return toolAuthorizationDecision{
			ownership: toolOwnershipPlatformBuiltin,
			class:     toolAuthorizationDenied,
			allowed:   false,
		}
	}
	if roleScopedEntityToolsEnabledForActor(source, actor) {
		if _, _, ok := roleScopedEntityToolSpecForActor(source, actor, toolName); ok {
			return toolAuthorizationDecision{
				ownership: toolOwnershipPlatformBuiltin,
				class:     toolAuthorizationRoleScoped,
				allowed:   true,
			}
		}
	}
	decision := classifyToolAuthorization(actor, toolName, e.authority, e.emitRegistry)
	if decision.allowed {
		return decision
	}
	if _, _, ok := roleScopedEntityToolSpecForActor(source, actor, toolName); ok {
		return toolAuthorizationDecision{
			ownership: toolOwnershipPlatformBuiltin,
			class:     toolAuthorizationRoleScoped,
			allowed:   true,
		}
	}
	return decision
}

func (e *Executor) RoleScopedEntityToolsEnabledForActor(actor models.AgentConfig) bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	return roleScopedEntityToolsEnabledForActor(source, actor)
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
	if err := e.validateRuntimeToolInput(actor, name, input); err != nil {
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
	if isEmitToolName(name) {
		return out, err
	}
	e.emitToolExecutionEvent(ctx, actor, name, input, out, err, time.Since(start), "dispatch")
	return out, err
}

func (e *Executor) validateRuntimeToolInput(actor models.AgentConfig, name string, input any) error {
	if e.validator == nil {
		return nil
	}
	return e.validator.Validate(&actor, name, input)
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
	detail := toolExecutionDiagnosticDetail(ctx, actor, toolName, input, result, execErr, phase, e.authority, e.emitRegistry)
	if strings.TrimSpace(actor.Role) != "" {
		detail["actor_role"] = strings.TrimSpace(actor.Role)
	}
	logger.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
		Level:      diaglog.NormalizeLevel(level),
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

func toolExecutionDiagnosticDetail(ctx context.Context, actor models.AgentConfig, toolName string, input any, result any, execErr error, phase string, provider runtimeauthority.Provider, emitRegistry *EmitRegistry) map[string]any {
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
			decision := classifyToolAuthorization(actor, toolName, provider, emitRegistry)
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
		decision := classifyToolAuthorization(actor, toolName, provider, emitRegistry)
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

func diagnosticPayloadMap(input any) map[string]any {
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil
	}
	if payload == nil {
		return map[string]any{}
	}
	return payload
}

func (e *Executor) mailboxStoreDependency() (MailboxPersistence, error) {
	e.mu.RLock()
	store := e.mailboxStore
	e.mu.RUnlock()
	if store == nil {
		return nil, errors.New("mailbox store is not configured")
	}
	return store, nil
}

func (e *Executor) sqlDBDependency() (*sql.DB, error) {
	e.mu.RLock()
	db := e.sqlDB
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	return db, nil
}

func (e *Executor) ValidateRuntimeToolInputForTest(name string, input any) error {
	return e.validateRuntimeToolInput(models.AgentConfig{}, name, input)
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

func authorizeRouting(provider runtimeauthority.Provider, actor, target models.AgentConfig, status string) error {
	return runtimeauthority.ProviderOrNoop(provider).AuthorizeRouting(actor, target, status)
}

func authorizeManage(provider runtimeauthority.Provider, actor, target models.AgentConfig, manager Manager) error {
	_ = manager
	if !runtimeauthority.SameFlowInstance(actor, target) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	return runtimeauthority.ProviderOrNoop(provider).AuthorizeManagement(actor, target)
}

func authorizeMailboxSend(provider runtimeauthority.Provider, actor models.AgentConfig) error {
	return runtimeauthority.ProviderOrNoop(provider).AuthorizeMailboxSend(actor)
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
