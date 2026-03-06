package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	llm "empireai/internal/runtime/llm"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

type RuntimeToolExecutor struct {
	mu            sync.RWMutex
	manager       runtimetools.Manager
	sqlDB         *sql.DB
	bus           runtimetools.EventPublisher
	scheduler     runtimetools.Scheduler
	scheduleStore SchedulePersistence
	mailboxStore  MailboxPersistence
	cfg           *config.Config
	oneShotMu     sync.Mutex
	oneShotEmits  map[string]struct{}
}

func NewRuntimeToolExecutor(bus *EventBus, scheduler *Scheduler, manager *AgentManager, stores ...SchedulePersistence) *RuntimeToolExecutor {
	var scheduleStore SchedulePersistence
	if len(stores) > 0 {
		scheduleStore = stores[0]
	}
	var publisher runtimetools.EventPublisher
	if bus != nil {
		publisher = bus
	}
	var sched runtimetools.Scheduler
	if scheduler != nil {
		sched = scheduler
	}
	var mgr runtimetools.Manager
	if manager != nil {
		mgr = manager
	}
	return &RuntimeToolExecutor{
		manager:       mgr,
		bus:           publisher,
		scheduler:     sched,
		scheduleStore: scheduleStore,
		oneShotEmits:  make(map[string]struct{}),
	}
}

func (e *RuntimeToolExecutor) SetManager(manager runtimetools.Manager) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if manager == nil {
		e.manager = nil
		return
	}
	e.manager = manager
}

func (e *RuntimeToolExecutor) SetConfig(cfg *config.Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
}

func (e *RuntimeToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	if defs, err := runtimetools.ContractDefinitions(); err == nil && len(defs) > 0 {
		return defs
	}
	return e.legacyToolDefinitions()
}

func (e *RuntimeToolExecutor) legacyToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{
			Name:        "agent_message",
			Description: "Direct message to another agent (requires target_agent_id)",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"target_agent_id":  map[string]any{"type": "string"},
					"target_agent_ids": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"to_agent_id":      map[string]any{"type": "string"},
					"to_agent_ids":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
					"event_type":       map[string]any{"type": "string"},
					"source_agent":     map[string]any{"type": "string"},
					"vertical_id":      map[string]any{"type": "string"},
					"task_id":          map[string]any{"type": "string"},
					"message":          map[string]any{"type": "string"},
					"payload":          map[string]any{},
				},
				"additionalProperties": false,
			},
		},
		{
			Name:        "schedule",
			Description: "Register timer-based wake-up events",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"agent_id":    map[string]any{"type": "string"},
					"event_type":  map[string]any{"type": "string"},
					"mode":        map[string]any{"type": "string", "enum": []string{"once", "cron"}},
					"cron":        map[string]any{"type": "string"},
					"at":          map[string]any{"type": "string"},
					"vertical_id": map[string]any{"type": "string"},
					"task_id":     map[string]any{"type": "string"},
					"payload":     map[string]any{},
				},
				"required":             []string{"event_type"},
				"additionalProperties": false,
			},
		},
		{
			Name:        "configure_routing",
			Description: "Install or update routing rule for a vertical",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"vertical_id":        map[string]any{"type": "string"},
					"event_pattern":      map[string]any{"type": "string"},
					"subscriber_id":      map[string]any{"type": "string"},
					"installed_by":       map[string]any{"type": "string"},
					"reason":             map[string]any{"type": "string"},
					"status":             map[string]any{"type": "string"},
					"source":             map[string]any{"type": "string"},
					"bootstrap_version":  map[string]any{"type": "integer"},
					"runtime_tool_event": map[string]any{"type": "boolean"},
				},
				"event_pattern",
				"subscriber_id",
			),
		},
		{
			Name:        "agent_hire",
			Description: "Hire/spawn an agent with given config",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"vertical_id": map[string]any{"type": "string"},
					"config": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"id":          map[string]any{"type": "string"},
							"role":        map[string]any{"type": "string"},
							"mode":        map[string]any{"type": "string"},
							"vertical_id": map[string]any{"type": "string"},
						},
						"required":             []string{"id"},
						"additionalProperties": true,
					},
				},
				"config",
			),
		},
		{
			Name:        "agent_fire",
			Description: "Terminate an agent",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"agent_id": map[string]any{"type": "string"},
				},
				"agent_id",
			),
		},
		{
			Name:        "agent_reconfigure",
			Description: "Reconfigure an existing agent",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"agent_id": map[string]any{"type": "string"},
					"config": map[string]any{
						"type":                 "object",
						"additionalProperties": true,
					},
				},
				"agent_id",
			),
		},
		{
			Name:        "mailbox_send",
			Description: "Create a mailbox item for human review/approval",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"event_id":    map[string]any{"type": "string"},
					"vertical_id": map[string]any{"type": "string"},
					"type":        map[string]any{"type": "string"},
					"priority":    map[string]any{"type": "string"},
					"summary":     map[string]any{"type": "string"},
					"context":     map[string]any{"type": "object"},
					"timeout_at":  map[string]any{"type": "string"},
				},
				"type",
			),
		},
		{
			Name:        "human_task_request",
			Description: "Request human execution for a physical-world task (creates human_tasks row and emits human_task.requested)",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"vertical_id":      map[string]any{"type": "string"},
					"category":         map[string]any{"type": "string"},
					"description":      map[string]any{"type": "string"},
					"talking_points":   map[string]any{},
					"expected_value":   map[string]any{"type": "string"},
					"priority":         map[string]any{"type": "string"},
					"deadline":         map[string]any{"type": "string"},
					"deadline_at":      map[string]any{"type": "string"},
					"deadline_rfc3339": map[string]any{"type": "string"},
				},
				"category",
				"description",
			),
		},
		{
			Name:        "human_task_decide",
			Description: "Empire Coordinator only: approve/reject/defer a human task request (updates human_tasks + emits human_task.{approved,rejected,deferred})",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"task_id":       map[string]any{"type": "string"},
					"decision":      map[string]any{"type": "string", "enum": []string{"approve", "approved", "reject", "rejected", "defer", "deferred"}},
					"reason":        map[string]any{"type": "string"},
					"priority_rank": map[string]any{"type": "integer"},
					"requeue_date":  map[string]any{"type": "string"},
				},
				"task_id",
				"decision",
			),
		},
		{
			Name:        "sql_execute",
			Description: "Execute read-only SQL (SELECT/CTE) in the actor's scoped vertical schema",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"query",
			),
		},
		{
			Name:        "nginx_reload",
			Description: "Reload nginx after config validation (holding-devops only)",
			Schema:      runtimetools.ObjectSchema(map[string]any{}),
		},
		{
			Name:        "systemd_control",
			Description: "Control empireai-* systemd units (holding-devops only)",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"action": map[string]any{"type": "string", "enum": []string{"start", "stop", "restart", "enable", "disable"}},
					"unit":   map[string]any{"type": "string"},
				},
				"action",
				"unit",
			),
		},
		{
			Name:        "certbot_execute",
			Description: "Issue/renew certbot cert for a domain (holding-devops only)",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"domain": map[string]any{"type": "string"},
				},
				"domain",
			),
		},
		{
			Name:        "whatsapp_business_api",
			Description: "Call WhatsApp Business API with per-vertical credentials",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
		{
			Name:        "email_api",
			Description: "Send email via per-vertical credentials",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"to":      map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "minItems": 1},
					"subject": map[string]any{"type": "string"},
					"body":    map[string]any{"type": "string"},
				},
				"to",
			),
		},
		{
			Name:        "instagram_api",
			Description: "Call Instagram Graph API with per-vertical credentials",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
		{
			Name:        "domain_purchase",
			Description: "Submit domain purchase via registrar integration",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
		{
			Name:        "domain_availability_check",
			Description: "Check domain availability via registrar integration",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
		{
			Name:        "dns_configure",
			Description: "Configure DNS via provider integration",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
		{
			Name:        "instagram_handle_check",
			Description: "Check if an Instagram handle appears available",
			Schema: runtimetools.ObjectSchema(
				map[string]any{
					"handle": map[string]any{"type": "string"},
				},
				"handle",
			),
		},
		{
			Name:        "whatsapp_name_check",
			Description: "Check WhatsApp display name via provider integration",
			Schema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"method":          map[string]any{"type": "string"},
					"url":             map[string]any{"type": "string"},
					"path":            map[string]any{"type": "string"},
					"query":           map[string]any{"type": "object"},
					"headers":         map[string]any{"type": "object"},
					"body":            map[string]any{},
					"timeout_seconds": map[string]any{"type": "integer"},
				},
				"additionalProperties": true,
			},
		},
	}
}

func (e *RuntimeToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
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
	out, err := e.executeTool(ctx, actor, name, input)
	e.emitToolExecutionEvent(ctx, actor, name, input, out, err, time.Since(start))
	return out, err
}

func (e *RuntimeToolExecutor) validateRuntimeToolInput(name string, input any) error {
	name = strings.TrimSpace(name)
	if name == "" || strings.HasPrefix(name, "emit_") {
		return nil
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return err
	}
	if payload == nil {
		payload = map[string]any{}
	}

	contractSchema, foundContract := runtimeToolSchemaForName(e.ToolDefinitions(), name)
	if foundContract && contractSchema != nil {
		if err := runtimetools.ValidatePayloadAgainstSchema(contractSchema, payload); err == nil {
			return nil
		} else if payloadTouchesSchemaProps(payload, contractSchema) &&
			!payloadHasLegacyOnlyProps(payload, contractSchema) &&
			!toolAllowsLegacySubsetFallback(name) {
			return err
		}
	}

	err, found := validateToolInputAgainstToolDefinitions(name, payload, e.legacyToolDefinitions())
	if !found || err == nil {
		return nil
	}
	legacyErr, legacyFound := validateToolInputAgainstToolDefinitions(name, input, e.legacyToolDefinitions())
	if legacyFound && legacyErr == nil {
		return nil
	}
	return err
}

func validateToolInputAgainstToolDefinitions(name string, input any, defs []llm.ToolDefinition) (error, bool) {
	schema, ok := runtimeToolSchemaForName(defs, name)
	if !ok || schema == nil {
		return nil, false
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return err, true
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return runtimetools.ValidatePayloadAgainstSchema(schema, payload), true
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
		if strings.TrimSpace(asString(payload["target_agent_id"])) == "" {
			if to := strings.TrimSpace(asString(payload["to"])); to != "" {
				payload["target_agent_id"] = to
			}
		}
	case "schedule":
		if strings.TrimSpace(asString(payload["event_type"])) == "" {
			if action := strings.TrimSpace(asString(payload["action"])); action != "" {
				payload["event_type"] = action
			}
		}
		if payload["payload"] == nil && payload["context"] != nil {
			payload["payload"] = payload["context"]
		}
		if strings.TrimSpace(asString(payload["at"])) == "" && asInt(payload["delay_seconds"]) > 0 {
			payload["mode"] = "once"
			payload["at"] = time.Now().Add(time.Duration(asInt(payload["delay_seconds"])) * time.Second).UTC().Format(time.RFC3339)
		}
	case "configure_routing":
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
		if payload["config"] == nil {
			config := map[string]any{
				"id":   strings.TrimSpace(asString(payload["agent_id"])),
				"role": strings.TrimSpace(asString(payload["role"])),
			}
			if mode := strings.TrimSpace(asString(payload["mode"])); mode != "" {
				config["mode"] = mode
			}
			if verticalID := strings.TrimSpace(asString(payload["vertical_id"])); verticalID != "" {
				config["vertical_id"] = verticalID
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
		if strings.TrimSpace(asString(payload["summary"])) == "" {
			if subject := strings.TrimSpace(asString(payload["subject"])); subject != "" {
				payload["summary"] = subject
			}
		}
		if payload["context"] == nil && payload["payload"] != nil {
			payload["context"] = payload["payload"]
		}
	case "human_task_request":
		if strings.TrimSpace(asString(payload["deadline"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_at"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_rfc3339"])) == "" {
			if hours := asInt(payload["deadline_hours"]); hours > 0 {
				payload["deadline_at"] = time.Now().Add(time.Duration(hours) * time.Hour).UTC().Format(time.RFC3339)
			}
		}
	case "systemd_control":
		if strings.TrimSpace(asString(payload["unit"])) == "" {
			if service := strings.TrimSpace(asString(payload["service"])); service != "" {
				payload["unit"] = service
			}
		}
	case "email_api":
		if to := strings.TrimSpace(asString(payload["to"])); to != "" {
			payload["to"] = []string{to}
		}
	case "whatsapp_business_api":
		runtimetools.NormalizeExternalContractPayload(payload, http.MethodPost)
	case "instagram_api":
		runtimetools.NormalizeExternalContractPayload(payload, http.MethodPost)
	case "domain_purchase":
		runtimetools.NormalizeExternalContractPayload(payload, http.MethodPost)
	case "domain_availability_check":
		if strings.TrimSpace(asString(payload["method"])) == "" {
			payload["method"] = http.MethodGet
		}
		if payload["query"] == nil && strings.TrimSpace(asString(payload["domain"])) != "" {
			payload["query"] = map[string]any{"domain": strings.TrimSpace(asString(payload["domain"]))}
		}
	case "dns_configure":
		runtimetools.NormalizeExternalContractPayload(payload, http.MethodPost)
	case "whatsapp_name_check":
		runtimetools.NormalizeExternalContractPayload(payload, http.MethodPost)
	}
	return payload
}

func (e *RuntimeToolExecutor) executeTool(ctx context.Context, actor models.AgentConfig, name string, input any) (any, error) {
	if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
		return e.handleEmitTool(ctx, actor, name, input)
	}
	switch name {
	case "agent_message":
		return e.execAgentMessage(ctx, actor, input)
	case "schedule":
		return e.execSchedule(actor, input)
	case "configure_routing":
		return e.execConfigureRouting(actor, input)
	case "agent_hire":
		return e.execAgentHire(actor, input)
	case "agent_fire":
		return e.execAgentFire(actor, input)
	case "agent_reconfigure":
		return e.execAgentReconfigure(actor, input)
	case "mailbox_send":
		return e.execMailboxSend(actor, input)
	case "human_task_request":
		return e.execHumanTaskRequest(ctx, actor, input)
	case "human_task_decide":
		return e.execHumanTaskDecide(ctx, actor, input)
	case "sql_execute":
		return e.execSQLExecute(ctx, actor, input)
	case "nginx_reload":
		return e.execNginxReload(ctx, actor, input)
	case "systemd_control":
		return e.execSystemdControl(ctx, actor, input)
	case "certbot_execute":
		return e.execCertbotExecute(ctx, actor, input)
	case "whatsapp_business_api":
		return e.execExternalProxy(ctx, actor, "whatsapp_business_api", input)
	case "email_api":
		return e.execEmailAPI(ctx, actor, input)
	case "instagram_api":
		return e.execExternalProxy(ctx, actor, "instagram_api", input)
	case "domain_purchase":
		return e.execExternalProxy(ctx, actor, "domain_purchase", input)
	case "domain_availability_check":
		return e.execExternalProxy(ctx, actor, "domain_availability_check", input)
	case "dns_configure":
		return e.execExternalProxy(ctx, actor, "dns_configure", input)
	case "instagram_handle_check":
		return e.execInstagramHandleCheck(ctx, actor, input)
	case "whatsapp_name_check":
		return e.execExternalProxy(ctx, actor, "whatsapp_name_check", input)
	default:
		return nil, fmt.Errorf("unsupported runtime tool: %s", name)
	}
}

func (e *RuntimeToolExecutor) emitToolExecutionEvent(
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
		"vertical_id":  actor.VerticalID,
		"tool_name":    toolName,
		"ok":           execErr == nil,
		"error":        toolExecErrorText(execErr),
		"duration_ms":  int(latency / time.Millisecond),
		"input":        runtimetools.SafeTelemetryText(input),
		"result":       runtimetools.SafeTelemetryText(result),
		"runtime_tool": true,
	})
	if err := e.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("agent.tool_execution"),
		SourceAgent: actor.ID,
		VerticalID:  actor.VerticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
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
	return runtimetools.SafeTelemetryText(FormatRuntimeError(err))
}

func (e *RuntimeToolExecutor) authorizeToolUsage(ctx context.Context, actor models.AgentConfig, toolName string) error {
	if runtimetools.IsUniversal(toolName) {
		return nil
	}
	if IsEmitToolAllowedForRole(actor.Role, toolName) {
		return nil
	}
	allowed, constrained := extractAllowedTools(actor)
	if !constrained {
		return nil
	}
	if _, ok := allowed[toolName]; ok {
		return nil
	}
	err := fmt.Errorf("tool %s is not allowed for agent %s", toolName, actor.ID)
	if e.bus != nil {
		payload, _ := json.Marshal(map[string]any{
			"reason":       "tool_not_allowed",
			"agent_id":     actor.ID,
			"agent_role":   actor.Role,
			"tool_name":    toolName,
			"vertical_id":  actor.VerticalID,
			"runtime_tool": true,
		})
		if pubErr := e.bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.contradiction_detected"),
			SourceAgent: "runtime",
			VerticalID:  actor.VerticalID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		}); pubErr != nil {
			runtimeWarn(
				"tool-executor",
				"failed to publish spec.contradiction_detected actor=%s tool=%s: %v",
				strings.TrimSpace(actor.ID),
				strings.TrimSpace(toolName),
				pubErr,
			)
		}
	}
	return err
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

func (e *RuntimeToolExecutor) getManager() runtimetools.Manager {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.manager
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

func (e *RuntimeToolExecutor) SetMailboxStore(store MailboxPersistence) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mailboxStore = store
}

func (e *RuntimeToolExecutor) SetSQLDB(db *sql.DB) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sqlDB = db
}

var (
	productRoles = []string{"vp-product", "cto-agent", "pm-agent", "support-agent", "tech-writer", "backend-agent", "frontend-agent", "qa-agent", "devops-agent"}
	growthRoles  = []string{"vp-growth", "marketing-agent"}
	engRoles     = []string{"tech-writer", "backend-agent", "frontend-agent", "qa-agent", "devops-agent"}
)

func authorizeRouting(actor, target models.AgentConfig, status string) error {
	switch actor.Role {
	case "opco-ceo", "empire-coordinator":
		return nil
	case "chief-of-staff":
		if status != "proposed" {
			return errors.New("chief-of-staff can only propose routing (status=proposed)")
		}
		return nil
	case "vp-product":
		if target.Role == "" || !slices.Contains(productRoles, target.Role) {
			return errors.New("vp-product can only configure product-side routing")
		}
		return nil
	case "vp-growth":
		if target.Role == "" || !slices.Contains(growthRoles, target.Role) {
			return errors.New("vp-growth can only configure growth-side routing")
		}
		return nil
	case "cto-agent":
		if target.Role == "" || !slices.Contains(engRoles, target.Role) {
			return errors.New("cto-agent can only configure engineering-side routing")
		}
		return nil
	default:
		return fmt.Errorf("role %s is not authorized to configure routing", actor.Role)
	}
}

func authorizeManage(actor models.AgentConfig, targetRole, targetVerticalID string) error {
	if actor.Role == "empire-coordinator" {
		return nil
	}
	if actor.VerticalID != "" && targetVerticalID != "" && actor.VerticalID != targetVerticalID {
		return errors.New("cross-vertical management is not allowed")
	}
	switch actor.Role {
	case "opco-ceo":
		return nil
	case "vp-product":
		if slices.Contains(productRoles, targetRole) {
			return nil
		}
		return errors.New("vp-product can only manage product domain agents")
	case "vp-growth":
		if slices.Contains(growthRoles, targetRole) {
			return nil
		}
		return errors.New("vp-growth can only manage growth domain agents")
	case "cto-agent":
		if slices.Contains(engRoles, targetRole) {
			return nil
		}
		return errors.New("cto-agent can only manage engineering agents")
	default:
		return fmt.Errorf("role %s is not authorized to manage agents", actor.Role)
	}
}

func authorizeMailboxSend(actor models.AgentConfig) error {
	switch actor.Role {
	case "opco-ceo",
		"vp-product",
		"vp-growth",
		"support-agent",
		"marketing-agent",
		"validation-coordinator",
		"empire-coordinator",
		"factory-cto",
		"holding-devops",
		"operations-analyst":
		return nil
	default:
		return fmt.Errorf("role %s is not authorized to send mailbox items", actor.Role)
	}
}

func coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
