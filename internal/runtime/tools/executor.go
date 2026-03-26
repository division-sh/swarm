package tools

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeauthority "swarm/internal/runtime/authority"
	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
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
	workflowSource  semanticview.Source
	flowActivator   runtimepipeline.FlowInstanceActivator
	authorizer      *ToolAuthorizer
	validator       *ToolInputValidator
	dispatcher      *ToolDispatcher
	oneShotMu       sync.Mutex
	oneShotEmits    map[string]struct{}
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
		workflowSource:  opts.WorkflowSource,
		flowActivator:   opts.FlowActivator,
		oneShotEmits:    make(map[string]struct{}),
	}
	exec.authorizer = NewToolAuthorizer(bus)
	exec.validator = NewToolInputValidator(exec.contractDefinitions)
	exec.dispatcher = NewToolDispatcher(
		func(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
			return exec.handleEmitTool(ctx, actor, name, input)
		},
		exec.buildToolHandlers(),
	)
	return exec
}

func (e *Executor) contractDefinitions() ([]llm.ToolDefinition, error) {
	e.mu.RLock()
	source := e.workflowSource
	e.mu.RUnlock()
	return ContractDefinitionsForSource(source)
}

func (e *Executor) SetWorkflowSource(source semanticview.Source) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.workflowSource = source
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

func (e *Executor) ToolDefinitions() []llm.ToolDefinition {
	defs, err := e.contractDefinitions()
	if err != nil {
		runtimeWarn("tool-executor", "failed to load contract tool definitions: %v", err)
		return nil
	}
	return defs
}

func (e *Executor) Execute(ctx context.Context, name string, input any) (any, error) {
	actor, ok := ActorFromContext(ctx)
	if !ok {
		return nil, errors.New("missing actor context for tool execution")
	}
	name = strings.TrimSpace(name)
	if err := e.authorizeToolUsage(ctx, actor, name); err != nil {
		return nil, err
	}
	if err := e.validateRuntimeToolInput(name, input); err != nil {
		return nil, WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"execute.validate_runtime_tool_input",
			false,
			err,
			"runtime tool input validation failed",
		)
	}
	input = normalizeRuntimeToolInput(name, input)
	start := time.Now()
	out, err := e.dispatchTool(ctx, actor, name, input)
	e.emitToolExecutionEvent(ctx, actor, name, input, out, err, time.Since(start))
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
		"configure_routing",
		"agent_fire",
		"mailbox_send",
		"human_task_request",
		"human_task_decide",
		"systemd_control":
		return true
	default:
		return false
	}
}

func normalizeRuntimeToolInput(name string, input any) any {
	if strings.TrimSpace(name) == "" || strings.HasPrefix(strings.TrimSpace(name), "emit_") {
		return input
	}
	var payload map[string]any
	if err := decodeToolInput(input, &payload); err != nil || payload == nil {
		return input
	}

	switch name {
	case "agent_message":
		if strings.TrimSpace(asString(payload["to"])) == "" {
			if target := strings.TrimSpace(asString(payload["target_agent_id"])); target != "" {
				payload["to"] = target
			}
		}
		if strings.TrimSpace(asString(payload["target_agent_id"])) == "" {
			if to := strings.TrimSpace(asString(payload["to"])); to != "" {
				payload["target_agent_id"] = to
			}
		}
		if strings.TrimSpace(asString(payload["message"])) == "" {
			if data, ok := payload["payload"].(map[string]any); ok {
				if msg := strings.TrimSpace(asString(data["message"])); msg != "" {
					payload["message"] = msg
				}
			}
			if strings.TrimSpace(asString(payload["message"])) == "" {
				payload["message"] = "runtime_tool"
			}
		}
	case "schedule":
		if strings.TrimSpace(asString(payload["action"])) == "" {
			if eventType := strings.TrimSpace(asString(payload["event_type"])); eventType != "" {
				payload["action"] = eventType
			}
		}
		if strings.TrimSpace(asString(payload["event_type"])) == "" {
			if action := strings.TrimSpace(asString(payload["action"])); action != "" {
				payload["event_type"] = action
			}
		}
		if asInt(payload["delay_seconds"]) <= 0 {
			if at := strings.TrimSpace(asString(payload["at"])); at != "" {
				if parsed, err := time.Parse(time.RFC3339, at); err == nil {
					delay := int(time.Until(parsed).Seconds())
					if delay < 0 {
						delay = 0
					}
					payload["delay_seconds"] = delay
				}
			}
		}
		if payload["payload"] == nil && payload["context"] != nil {
			payload["payload"] = payload["context"]
		}
		if strings.TrimSpace(asString(payload["at"])) == "" {
			if rawDelay, ok := payload["delay_seconds"]; ok {
				delaySeconds := asInt(rawDelay)
				if delaySeconds < 0 {
					delaySeconds = 0
				}
				payload["mode"] = "once"
				payload["at"] = time.Now().Add(time.Duration(delaySeconds) * time.Second).UTC().Format(time.RFC3339)
			}
		}
	case "configure_routing":
		if strings.TrimSpace(asString(payload["operation"])) == "" {
			switch strings.ToLower(strings.TrimSpace(asString(payload["status"]))) {
			case "deactivated":
				payload["operation"] = "remove"
			default:
				payload["operation"] = "add"
			}
		}
		if strings.TrimSpace(asString(payload["event_type"])) == "" {
			if pattern := strings.TrimSpace(asString(payload["event_pattern"])); pattern != "" {
				payload["event_type"] = pattern
			}
		}
		if strings.TrimSpace(asString(payload["event_pattern"])) == "" {
			if eventType := strings.TrimSpace(asString(payload["event_type"])); eventType != "" {
				payload["event_pattern"] = eventType
			}
		}
		if strings.TrimSpace(asString(payload["status"])) == "" {
			switch strings.ToLower(strings.TrimSpace(asString(payload["operation"]))) {
			case "remove":
				payload["status"] = "deactivated"
			case "add", "modify":
				payload["status"] = "active"
			}
		}
	case "agent_hire":
		if strings.TrimSpace(asString(payload["agent_id"])) == "" {
			if config, ok := payload["config"].(map[string]any); ok {
				payload["agent_id"] = strings.TrimSpace(asString(config["id"]))
			}
		}
		if strings.TrimSpace(asString(payload["role"])) == "" {
			if config, ok := payload["config"].(map[string]any); ok {
				payload["role"] = strings.TrimSpace(asString(config["role"]))
			}
		}
		if payload["config"] == nil {
			config := map[string]any{
				"id":   strings.TrimSpace(asString(payload["agent_id"])),
				"role": strings.TrimSpace(asString(payload["role"])),
			}
			if mode := strings.TrimSpace(asString(payload["mode"])); mode != "" {
				config["mode"] = mode
			}
			if entityID := strings.TrimSpace(asString(payload["entity_id"])); entityID != "" {
				config["entity_id"] = entityID
			}
			rawConfig := map[string]any{}
			if modelTier := strings.TrimSpace(asString(payload["model_tier"])); modelTier != "" {
				rawConfig["model_tier"] = modelTier
			}
			if systemPrompt := strings.TrimSpace(asString(payload["system_prompt"])); systemPrompt != "" {
				rawConfig["system_prompt"] = systemPrompt
			}
			if len(rawConfig) > 0 {
				config["config"] = rawConfig
			}
			payload["config"] = config
		}
	case "agent_fire":
		if strings.TrimSpace(asString(payload["reason"])) == "" {
			payload["reason"] = "runtime_tool"
		}
	case "agent_reconfigure":
		if payload["config"] == nil {
			config := map[string]any{}
			if modelTier := strings.TrimSpace(asString(payload["model_tier"])); modelTier != "" {
				config["model_tier"] = modelTier
			}
			if systemPrompt := strings.TrimSpace(asString(payload["system_prompt"])); systemPrompt != "" {
				config["system_prompt"] = systemPrompt
			}
			if maxTurns := asInt(payload["max_turns_per_task"]); maxTurns > 0 {
				config["max_turns_per_task"] = maxTurns
			}
			payload["config"] = config
		}
	case "mailbox_send":
		if mailboxType, err := NormalizeMailboxType(asString(payload["type"])); err == nil && mailboxType != "" {
			payload["type"] = mailboxType
		}
		if priority, err := NormalizeMailboxPriority(asString(payload["priority"])); err == nil && priority != "" {
			payload["priority"] = priority
		}
		if strings.TrimSpace(asString(payload["subject"])) == "" {
			if summary := strings.TrimSpace(asString(payload["summary"])); summary != "" {
				payload["subject"] = summary
			}
		}
		if payload["payload"] == nil && payload["context"] != nil {
			payload["payload"] = payload["context"]
		}
		if strings.TrimSpace(asString(payload["summary"])) == "" {
			if subject := strings.TrimSpace(asString(payload["subject"])); subject != "" {
				payload["summary"] = subject
			}
		}
		if payload["context"] == nil && payload["payload"] != nil {
			payload["context"] = payload["payload"]
		}
	case "human_task_request":
		if entityID := strings.TrimSpace(asString(payload["entity_id"])); entityID != "" {
			payload["entity_id"] = entityID
		}
		if strings.TrimSpace(asString(payload["deadline"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_at"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_rfc3339"])) == "" {
			if hours := asInt(payload["deadline_hours"]); hours > 0 {
				payload["deadline_at"] = time.Now().Add(time.Duration(hours) * time.Hour).UTC().Format(time.RFC3339)
			}
		}
	case "human_task_decide":
		switch strings.ToLower(strings.TrimSpace(asString(payload["decision"]))) {
		case "approve":
			payload["decision"] = "approved"
		case "reject":
			payload["decision"] = "rejected"
		case "defer":
			payload["decision"] = "deferred"
		}
	case "systemd_control":
		if strings.TrimSpace(asString(payload["service"])) == "" {
			if unit := strings.TrimSpace(asString(payload["unit"])); unit != "" {
				payload["service"] = unit
			}
		}
		if strings.TrimSpace(asString(payload["unit"])) == "" {
			if service := strings.TrimSpace(asString(payload["service"])); service != "" {
				payload["unit"] = service
			}
		}
	}
	return payload
}

func (e *Executor) emitToolExecutionEvent(
	ctx context.Context,
	actor models.AgentConfig,
	toolName string,
	input any,
	result any,
	execErr error,
	latency time.Duration,
) {
	if e.bus == nil || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(toolName) == "" {
		return
	}
	payload, _ := json.Marshal(map[string]any{
		"agent_id":     actor.ID,
		"agent_role":   actor.Role,
		"entity_id":    actor.EffectiveEntityID(),
		"tool_name":    toolName,
		"ok":           execErr == nil,
		"error":        toolExecErrorText(execErr),
		"duration_ms":  int(latency / time.Millisecond),
		"input":        SafeTelemetryText(input),
		"result":       SafeTelemetryText(result),
		"runtime_tool": true,
	})
	if err := e.bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("agent.tool_execution"),
		SourceAgent: actor.ID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}).WithEntityID(actor.EffectiveEntityID())); err != nil {
		runtimeWarn(
			"tool-executor",
			"failed to publish agent.tool_execution actor=%s tool=%s: %v",
			strings.TrimSpace(actor.ID),
			strings.TrimSpace(toolName),
			err,
		)
	}
}

func toolExecErrorText(err error) string {
	if err == nil {
		return ""
	}
	return SafeTelemetryText(FormatRuntimeError(err))
}

func (e *Executor) authorizeToolUsage(ctx context.Context, actor models.AgentConfig, toolName string) error {
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

func extractAllowedTools(actor models.AgentConfig) (map[string]struct{}, bool) {
	allowed := make(map[string]struct{})
	if len(actor.Config) == 0 || !json.Valid(actor.Config) {
		return allowed, false
	}
	var parsed map[string]any
	if err := json.Unmarshal(actor.Config, &parsed); err != nil {
		return allowed, false
	}
	found := false
	for _, key := range []string{"tools", "allowed_tools"} {
		raw, ok := parsed[key]
		if !ok {
			continue
		}
		arr, ok := raw.([]any)
		if !ok {
			continue
		}
		for _, item := range arr {
			name := strings.TrimSpace(asString(item))
			if name == "" {
				continue
			}
			found = true
			allowed[name] = struct{}{}
		}
	}
	return allowed, found
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

func (e *Executor) ExecNginxReloadDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execNginxReload(ctx, actor, input)
}

func (e *Executor) ExecSystemdControlDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execSystemdControl(ctx, actor, input)
}

func (e *Executor) ExecCertbotExecuteDirect(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	return e.execCertbotExecute(ctx, actor, input)
}

func authorizeRouting(actor, target models.AgentConfig, status string) error {
	return runtimeauthority.Active().AuthorizeRouting(actor, target, status)
}

func authorizeManage(actor, target models.AgentConfig, manager Manager) error {
	if !runtimeauthority.SameFlowInstance(actor, target) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
	if manager != nil {
		if isManagerAncestor(manager, strings.TrimSpace(target.ID), strings.TrimSpace(actor.ID)) {
			return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
		}
		if isManagerAncestor(manager, strings.TrimSpace(actor.ID), strings.TrimSpace(target.ID)) {
			return nil
		}
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
