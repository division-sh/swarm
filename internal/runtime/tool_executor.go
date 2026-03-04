package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"empireai/internal/commgraph"
	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

type RuntimeToolExecutor struct {
	mu            sync.RWMutex
	manager       *AgentManager
	sqlDB         *sql.DB
	bus           *EventBus
	scheduler     *Scheduler
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
	return &RuntimeToolExecutor{
		manager:       manager,
		bus:           bus,
		scheduler:     scheduler,
		scheduleStore: scheduleStore,
		oneShotEmits:  make(map[string]struct{}),
	}
}

func (e *RuntimeToolExecutor) SetManager(manager *AgentManager) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.manager = manager
}

func (e *RuntimeToolExecutor) SetConfig(cfg *config.Config) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cfg = cfg
}

func toolObjectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func (e *RuntimeToolExecutor) ToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
				map[string]any{
					"agent_id": map[string]any{"type": "string"},
				},
				"agent_id",
			),
		},
		{
			Name:        "agent_reconfigure",
			Description: "Reconfigure an existing agent",
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
				map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"query",
			),
		},
		{
			Name:        "nginx_reload",
			Description: "Reload nginx after config validation (holding-devops only)",
			Schema:      toolObjectSchema(map[string]any{}),
		},
		{
			Name:        "systemd_control",
			Description: "Control empireai-* systemd units (holding-devops only)",
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
			Schema: toolObjectSchema(
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
	start := time.Now()
	out, err := e.executeTool(ctx, actor, name, input)
	e.emitToolExecutionEvent(ctx, actor, name, input, out, err, time.Since(start))
	return out, err
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
		"input":        safeTelemetryText(input),
		"result":       safeTelemetryText(result),
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
	return safeTelemetryText(FormatRuntimeError(err))
}

func (e *RuntimeToolExecutor) authorizeToolUsage(ctx context.Context, actor models.AgentConfig, toolName string) error {
	if IsUniversalRuntimeTool(toolName) {
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

func (e *RuntimeToolExecutor) execAgentMessage(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
	}
	var in struct {
		TargetAgentID  string   `json:"target_agent_id"`
		TargetAgentIDs []string `json:"target_agent_ids"`
		ToAgentID      string   `json:"to_agent_id"`
		ToAgentIDs     []string `json:"to_agent_ids"`
		EventType      string   `json:"event_type"`
		SourceAgent    string   `json:"source_agent"`
		VerticalID     string   `json:"vertical_id"`
		TaskID         string   `json:"task_id"`
		Message        string   `json:"message"`
		Payload        any      `json:"payload"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.EventType) == "" {
		in.EventType = "agent.message"
	}
	if strings.TrimSpace(in.SourceAgent) == "" {
		in.SourceAgent = actor.ID
	}

	targets := make([]string, 0, 4)
	for _, v := range []string{in.TargetAgentID, in.ToAgentID} {
		if tv := strings.TrimSpace(v); tv != "" {
			targets = append(targets, tv)
		}
	}
	for _, v := range append(in.TargetAgentIDs, in.ToAgentIDs...) {
		if tv := strings.TrimSpace(v); tv != "" {
			targets = append(targets, tv)
		}
	}
	targets = uniqueNonEmptyStrings(targets)
	if len(targets) == 0 {
		return nil, errors.New("target_agent_id is required")
	}

	// Validate recipient existence and enforce operating cross-vertical constraints.
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	targetVertical := strings.TrimSpace(in.VerticalID)
	for _, t := range targets {
		cfg, ok := manager.GetAgentConfig(t)
		if !ok {
			return nil, fmt.Errorf("target agent not found: %s", t)
		}
		if actor.Mode == "operating" && strings.TrimSpace(actor.VerticalID) != "" {
			if strings.TrimSpace(cfg.VerticalID) != strings.TrimSpace(actor.VerticalID) {
				return nil, errors.New("cross-vertical agent_message is not allowed in operating mode")
			}
		}
		if targetVertical == "" {
			targetVertical = strings.TrimSpace(cfg.VerticalID)
		}
		if strings.TrimSpace(in.VerticalID) != "" && strings.TrimSpace(cfg.VerticalID) != strings.TrimSpace(in.VerticalID) {
			return nil, errors.New("vertical_id does not match target agent vertical")
		}
		if err := authorizeAgentMessage(actor, cfg, manager); err != nil {
			return nil, fmt.Errorf("agent_message target %s: %w", t, err)
		}
	}

	wirePayload, err := json.Marshal(map[string]any{
		"from_agent_id": actor.ID,
		"from_role":     actor.Role,
		"to_agent_ids":  targets,
		"message":       strings.TrimSpace(in.Message),
		"data":          in.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	if len(wirePayload) == 0 || string(wirePayload) == "null" {
		wirePayload = []byte("{}")
	}
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(in.EventType),
		SourceAgent: in.SourceAgent,
		TaskID:      in.TaskID,
		VerticalID:  targetVertical,
		Payload:     wirePayload,
		CreatedAt:   time.Now(),
	}
	if err := e.bus.PublishDirect(ctx, evt, targets); err != nil {
		return nil, err
	}
	return map[string]any{"event_id": evt.ID, "status": "sent", "targets": targets}, nil
}

func authorizeAgentMessage(actor, target models.AgentConfig, manager *AgentManager) error {
	if strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(target.ID) == "" {
		return errors.New("agent ids are required for message authorization")
	}
	if strings.TrimSpace(actor.ID) == strings.TrimSpace(target.ID) {
		return nil
	}
	if hasRoleMessageAuthority(actor, target) {
		return nil
	}
	if manager != nil && isManagerAncestor(manager, actor.ID, target.ID) {
		return nil
	}
	// Workers can always escalate upward to their manager chain.
	if manager != nil && isManagerAncestor(manager, target.ID, actor.ID) {
		return nil
	}
	return fmt.Errorf("role %s cannot message role %s", actor.Role, target.Role)
}

func hasRoleMessageAuthority(actor, target models.AgentConfig) bool {
	sender := normalizeCommRole(actor.Role)
	recipient := normalizeCommRole(target.Role)
	if sender == "" || recipient == "" {
		return false
	}
	for _, rule := range commgraph.MessageAuthorities() {
		if normalizeCommRole(rule.SenderRole) != sender {
			continue
		}
		if !messageScopeAllowed(actor, target, rule.Scope) {
			continue
		}
		for _, candidate := range rule.RecipientRoles {
			if normalizeCommRole(candidate) == recipient {
				return true
			}
		}
	}
	return false
}

func messageScopeAllowed(actor, target models.AgentConfig, scope string) bool {
	scope = strings.TrimSpace(strings.ToLower(scope))
	switch scope {
	case "", "any":
		return true
	case "holding":
		return strings.TrimSpace(actor.VerticalID) == "" && strings.TrimSpace(target.VerticalID) == ""
	case "opco":
		actorVertical := strings.TrimSpace(actor.VerticalID)
		targetVertical := strings.TrimSpace(target.VerticalID)
		return actorVertical != "" && actorVertical == targetVertical
	default:
		return false
	}
}

func normalizeCommRole(role string) string {
	role = strings.TrimSpace(strings.ToLower(role))
	role = strings.ReplaceAll(role, "_", "-")
	role = strings.Join(strings.Fields(role), "-")
	switch role {
	case "head-of-product":
		return "vp-product"
	case "head-of-growth":
		return "vp-growth"
	case "cto":
		return "cto-agent"
	case "opco-devops":
		return "devops-agent"
	default:
		return role
	}
}

func isManagerAncestor(manager *AgentManager, managerID, targetID string) bool {
	managerID = strings.TrimSpace(managerID)
	targetID = strings.TrimSpace(targetID)
	if manager == nil || managerID == "" || targetID == "" || managerID == targetID {
		return false
	}
	currentID := targetID
	visited := map[string]struct{}{currentID: {}}
	for {
		cfg, ok := manager.GetAgentConfig(currentID)
		if !ok {
			return false
		}
		parent := strings.TrimSpace(cfg.ParentAgent)
		if parent == "" {
			return false
		}
		if parent == managerID {
			return true
		}
		if _, seen := visited[parent]; seen {
			return false
		}
		visited[parent] = struct{}{}
		currentID = parent
	}
}

func uniqueNonEmptyStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (e *RuntimeToolExecutor) execSchedule(actor models.AgentConfig, input any) (any, error) {
	if e.scheduler == nil {
		return nil, errors.New("scheduler is not configured")
	}
	var in struct {
		AgentID    string `json:"agent_id"`
		EventType  string `json:"event_type"`
		Mode       string `json:"mode"`
		Cron       string `json:"cron"`
		At         string `json:"at"`
		VerticalID string `json:"vertical_id"`
		TaskID     string `json:"task_id"`
		Payload    any    `json:"payload"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Mode) == "" {
		in.Mode = "once"
	}
	if strings.TrimSpace(in.AgentID) == "" {
		in.AgentID = actor.ID
	}
	if in.AgentID != actor.ID {
		return nil, errors.New("agents can only schedule for themselves")
	}
	if strings.TrimSpace(in.VerticalID) == "" {
		in.VerticalID = actor.VerticalID
	}
	if strings.TrimSpace(in.VerticalID) != "" && strings.TrimSpace(actor.VerticalID) != "" && in.VerticalID != actor.VerticalID {
		return nil, errors.New("cross-vertical schedule is not allowed")
	}

	var at time.Time
	if strings.TrimSpace(in.At) != "" {
		parsed, err := time.Parse(time.RFC3339, in.At)
		if err != nil {
			return nil, fmt.Errorf("invalid at value: %w", err)
		}
		at = parsed
	}

	payload, err := json.Marshal(in.Payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	if len(payload) == 0 || string(payload) == "null" {
		payload = []byte("{}")
	}

	if err := e.scheduler.Register(Schedule{
		AgentID:    in.AgentID,
		EventType:  in.EventType,
		Mode:       in.Mode,
		Cron:       in.Cron,
		At:         at,
		VerticalID: in.VerticalID,
		TaskID:     in.TaskID,
		Payload:    payload,
	}); err != nil {
		return nil, err
	}
	if e.scheduleStore != nil {
		if err := e.scheduleStore.UpsertSchedule(context.Background(), Schedule{
			AgentID:    in.AgentID,
			EventType:  in.EventType,
			Mode:       in.Mode,
			Cron:       in.Cron,
			At:         at,
			VerticalID: in.VerticalID,
			TaskID:     in.TaskID,
			Payload:    payload,
		}); err != nil {
			return nil, err
		}
	}

	return map[string]any{"status": "scheduled"}, nil
}

func (e *RuntimeToolExecutor) execConfigureRouting(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		VerticalID       string `json:"vertical_id"`
		EventPattern     string `json:"event_pattern"`
		SubscriberID     string `json:"subscriber_id"`
		InstalledBy      string `json:"installed_by"`
		Reason           string `json:"reason"`
		Status           string `json:"status"`
		Source           string `json:"source"`
		BootstrapVersion int    `json:"bootstrap_version"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if in.Status == "" {
		in.Status = "active"
	}
	// Agent-initiated routing changes are always "discovered" and attributed to the actor.
	// Template migrations use a separate privileged path.
	in.InstalledBy = actor.ID
	in.Source = "discovered"
	in.BootstrapVersion = 0
	if strings.TrimSpace(in.VerticalID) == "" {
		in.VerticalID = actor.VerticalID
	}
	if strings.TrimSpace(in.VerticalID) != "" && strings.TrimSpace(actor.VerticalID) != "" && in.VerticalID != actor.VerticalID {
		return nil, errors.New("cross-vertical routing change is not allowed")
	}
	targetCfg, _ := manager.GetAgentConfig(in.SubscriberID)
	if err := authorizeRouting(actor, targetCfg, in.Status); err != nil {
		return nil, err
	}

	rule := PersistedRoutingRule{
		VerticalID:       in.VerticalID,
		EventPattern:     in.EventPattern,
		SubscriberID:     in.SubscriberID,
		InstalledBy:      in.InstalledBy,
		Reason:           in.Reason,
		Status:           in.Status,
		Source:           in.Source,
		BootstrapVersion: in.BootstrapVersion,
	}
	if err := manager.ConfigureRouting(rule); err != nil {
		return nil, err
	}
	if e.bus != nil {
		if err := e.bus.Publish(context.Background(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.routing_updated"),
			SourceAgent: actor.ID,
			VerticalID:  strings.TrimSpace(in.VerticalID),
			Payload: mustJSON(map[string]any{
				"vertical_id":        in.VerticalID,
				"event_pattern":      in.EventPattern,
				"subscriber_id":      in.SubscriberID,
				"installed_by":       actor.ID,
				"reason":             in.Reason,
				"status":             in.Status,
				"source":             "discovered",
				"bootstrap_version":  0,
				"runtime_tool_event": true,
			}),
			CreatedAt: time.Now(),
		}); err != nil {
			runtimeWarn(
				"tool-executor",
				"failed to publish opco.routing_updated actor=%s vertical_id=%s pattern=%s: %v",
				strings.TrimSpace(actor.ID),
				strings.TrimSpace(in.VerticalID),
				strings.TrimSpace(in.EventPattern),
				err,
			)
		}
	}
	return map[string]any{"status": "updated"}, nil
}

func (e *RuntimeToolExecutor) execAgentHire(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		VerticalID string             `json:"vertical_id"`
		Config     models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if in.Config.ID == "" {
		return nil, errors.New("config.id is required")
	}
	if in.Config.VerticalID == "" {
		in.Config.VerticalID = coalesce(in.VerticalID, actor.VerticalID)
	}
	if in.Config.Mode == "" {
		in.Config.Mode = coalesce(actor.Mode, "operating")
	}
	if err := authorizeManage(actor, in.Config.Role, in.Config.VerticalID); err != nil {
		return nil, err
	}
	if err := manager.SpawnAgentFor(in.VerticalID, in.Config); err != nil {
		return nil, err
	}
	return map[string]any{"status": "hired", "agent_id": in.Config.ID}, nil
}

func (e *RuntimeToolExecutor) execAgentFire(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		AgentID string `json:"agent_id"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	targetCfg, ok := manager.GetAgentConfig(in.AgentID)
	if !ok {
		return nil, fmt.Errorf("target agent not found: %s", in.AgentID)
	}
	if err := authorizeManage(actor, targetCfg.Role, targetCfg.VerticalID); err != nil {
		return nil, err
	}
	if err := manager.TeardownAgent(in.AgentID); err != nil {
		return nil, err
	}
	return map[string]any{"status": "fired", "agent_id": in.AgentID}, nil
}

func (e *RuntimeToolExecutor) execAgentReconfigure(actor models.AgentConfig, input any) (any, error) {
	manager := e.getManager()
	if manager == nil {
		return nil, errors.New("agent manager is not configured")
	}
	var in struct {
		AgentID string             `json:"agent_id"`
		Config  models.AgentConfig `json:"config"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.AgentID) == "" {
		return nil, errors.New("agent_id is required")
	}
	targetCfg, ok := manager.GetAgentConfig(in.AgentID)
	if !ok {
		return nil, fmt.Errorf("target agent not found: %s", in.AgentID)
	}
	if err := authorizeManage(actor, targetCfg.Role, targetCfg.VerticalID); err != nil {
		return nil, err
	}
	if err := manager.ReconfigureAgent(in.AgentID, in.Config); err != nil {
		return nil, err
	}
	return map[string]any{"status": "reconfigured", "agent_id": in.AgentID}, nil
}

func (e *RuntimeToolExecutor) execMailboxSend(actor models.AgentConfig, input any) (any, error) {
	if e.mailboxStore == nil {
		return nil, errors.New("mailbox store is not configured")
	}
	if err := authorizeMailboxSend(actor); err != nil {
		return nil, err
	}
	var in struct {
		EventID    string `json:"event_id"`
		VerticalID string `json:"vertical_id"`
		Type       string `json:"type"`
		Priority   string `json:"priority"`
		Summary    string `json:"summary"`
		Context    any    `json:"context"`
		TimeoutAt  string `json:"timeout_at"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.VerticalID) == "" {
		in.VerticalID = actor.VerticalID
	}
	if strings.TrimSpace(in.VerticalID) != "" && strings.TrimSpace(actor.VerticalID) != "" && in.VerticalID != actor.VerticalID {
		return nil, errors.New("cross-vertical mailbox item is not allowed")
	}
	if strings.TrimSpace(in.Type) == "" {
		return nil, errors.New("mailbox type is required")
	}
	normalizedType, err := normalizeMailboxType(in.Type)
	if err != nil {
		return nil, err
	}
	in.Type = normalizedType
	if strings.TrimSpace(in.Priority) == "" {
		in.Priority = "normal"
	}
	normalizedPriority, err := normalizeMailboxPriority(in.Priority)
	if err != nil {
		return nil, err
	}
	in.Priority = normalizedPriority
	ctxJSON, err := json.Marshal(in.Context)
	if err != nil {
		return nil, fmt.Errorf("marshal mailbox context: %w", err)
	}
	if len(ctxJSON) == 0 || string(ctxJSON) == "null" {
		ctxJSON = []byte("{}")
	}
	var timeout time.Time
	if strings.TrimSpace(in.TimeoutAt) != "" {
		parsed, err := time.Parse(time.RFC3339, in.TimeoutAt)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout_at: %w", err)
		}
		timeout = parsed
	}

	id, err := e.mailboxStore.InsertMailboxItem(context.Background(), MailboxItem{
		EventID:    in.EventID,
		VerticalID: in.VerticalID,
		FromAgent:  actor.ID,
		Type:       in.Type,
		Priority:   in.Priority,
		Status:     "pending",
		Context:    ctxJSON,
		Summary:    in.Summary,
		TimeoutAt:  timeout,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "queued", "mailbox_id": id}, nil
}

func normalizeMailboxType(raw string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(raw))
	switch t {
	case "vertical_decision":
		t = "vertical_approval"
	case "template_migration_review", "template_migration":
		t = "migration_approval"
	case "escalation_request", "customer_escalation", "health_warning":
		t = "escalation"
	case "geography_expansion", "vertical_geography_expansion", "expansion_recommendation":
		t = "domain_approval"
	case "product_spec_review", "deploy_review", "founder_input", "human_task":
		t = "review"
	case "capacity_warning":
		t = "budget_increase"
	}
	switch t {
	case "review", "escalation", "spend_request", "budget_increase", "digest", "vertical_approval", "migration_approval", "domain_approval":
		return t, nil
	default:
		return "", fmt.Errorf("invalid mailbox type %q", raw)
	}
}

func normalizeMailboxPriority(raw string) (string, error) {
	p := strings.ToLower(strings.TrimSpace(raw))
	switch p {
	case "", "normal":
		return "normal", nil
	case "medium":
		return "normal", nil
	case "urgent":
		return "high", nil
	case "low", "high", "critical":
		return p, nil
	default:
		return "", fmt.Errorf("invalid mailbox priority %q", raw)
	}
}

func (e *RuntimeToolExecutor) execHumanTaskRequest(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	cfg := e.cfg
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
	}

	var in struct {
		VerticalID      string `json:"vertical_id"`
		Category        string `json:"category"`
		Description     string `json:"description"`
		TalkingPoints   any    `json:"talking_points"`
		ExpectedValue   string `json:"expected_value"`
		Priority        string `json:"priority"`
		Deadline        string `json:"deadline"`
		DeadlineAt      string `json:"deadline_at"`
		DeadlineRFC3339 string `json:"deadline_rfc3339"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}

	in.VerticalID = strings.TrimSpace(coalesce(in.VerticalID, actor.VerticalID))
	in.Category = strings.TrimSpace(in.Category)
	in.Description = strings.TrimSpace(in.Description)
	in.ExpectedValue = strings.TrimSpace(in.ExpectedValue)
	in.Priority = strings.TrimSpace(in.Priority)
	deadlineStr := strings.TrimSpace(coalesce(in.Deadline, in.DeadlineAt, in.DeadlineRFC3339))

	if strings.TrimSpace(actor.ID) == "" {
		return nil, errors.New("actor id is required")
	}
	if in.Category == "" {
		return nil, errors.New("category is required")
	}
	if cfg != nil && len(cfg.Budget.HumanTasks.CategoriesEnabled) > 0 {
		enabled := false
		for _, c := range cfg.Budget.HumanTasks.CategoriesEnabled {
			if strings.EqualFold(strings.TrimSpace(c), in.Category) {
				enabled = true
				break
			}
		}
		if !enabled {
			return nil, fmt.Errorf("category %q is not enabled for human tasks", in.Category)
		}
	}
	if in.Description == "" {
		return nil, errors.New("description is required")
	}
	if in.Priority == "" {
		in.Priority = "medium"
	}

	var deadline sql.NullTime
	if deadlineStr != "" {
		t, err := time.Parse(time.RFC3339, deadlineStr)
		if err != nil {
			return nil, fmt.Errorf("invalid deadline (expected RFC3339): %w", err)
		}
		deadline = sql.NullTime{Time: t, Valid: true}
	}

	talkingJSON := []byte("null")
	if in.TalkingPoints != nil {
		if b, err := json.Marshal(in.TalkingPoints); err == nil && len(b) > 0 {
			talkingJSON = b
		}
	}

	var taskID string
	const q = `
		INSERT INTO human_tasks (
			requesting_agent, vertical_id, category, description,
			talking_points, expected_value, priority, deadline, status
		) VALUES (
			$1, NULLIF($2,'')::uuid, $3, $4,
			$5::jsonb, NULLIF($6,''), $7, $8, 'pending_review'
		)
		RETURNING id::text
	`
	if err := db.QueryRowContext(ctx, q,
		actor.ID,
		in.VerticalID,
		in.Category,
		in.Description,
		talkingJSON,
		in.ExpectedValue,
		in.Priority,
		deadline,
	).Scan(&taskID); err != nil {
		return nil, fmt.Errorf("insert human task: %w", err)
	}

	payload := map[string]any{
		"task_id":          taskID,
		"requesting_agent": actor.ID,
		"vertical_id":      in.VerticalID,
		"category":         in.Category,
		"description":      in.Description,
		"talking_points":   json.RawMessage(talkingJSON),
		"expected_value":   in.ExpectedValue,
		"priority":         in.Priority,
	}
	if deadline.Valid {
		payload["deadline"] = deadline.Time.UTC().Format(time.RFC3339)
	}

	if err := e.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("human_task.requested"),
		SourceAgent: actor.ID,
		VerticalID:  in.VerticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"tool-executor",
			"failed to publish human_task.requested task_id=%s actor=%s: %v",
			strings.TrimSpace(taskID),
			strings.TrimSpace(actor.ID),
			err,
		)
	}

	return map[string]any{
		"task_id": taskID,
		"status":  "pending_review",
	}, nil
}

func (e *RuntimeToolExecutor) execHumanTaskDecide(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	cfg := e.cfg
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if e.bus == nil {
		return nil, errors.New("event bus is not configured")
	}
	if actor.Role != "empire-coordinator" {
		return nil, fmt.Errorf("role %s is not authorized to decide human tasks", actor.Role)
	}

	var in struct {
		TaskID       string `json:"task_id"`
		Decision     string `json:"decision"`
		Reason       string `json:"reason"`
		PriorityRank int    `json:"priority_rank"`
		RequeueDate  string `json:"requeue_date"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	in.TaskID = strings.TrimSpace(in.TaskID)
	in.Decision = strings.ToLower(strings.TrimSpace(in.Decision))
	in.Reason = strings.TrimSpace(in.Reason)
	in.RequeueDate = strings.TrimSpace(in.RequeueDate)
	if in.TaskID == "" {
		return nil, errors.New("task_id is required")
	}
	if in.Decision == "" {
		return nil, errors.New("decision is required (approve|reject|defer)")
	}
	var newStatus string
	var evtType events.EventType
	switch in.Decision {
	case "approve", "approved":
		newStatus = "approved"
		evtType = events.EventType("human_task.approved")
	case "reject", "rejected":
		newStatus = "rejected"
		evtType = events.EventType("human_task.rejected")
	case "defer", "deferred":
		newStatus = "deferred"
		evtType = events.EventType("human_task.deferred")
	default:
		return nil, fmt.Errorf("unknown decision: %s", in.Decision)
	}

	// Spec v2.0: weekly human task budget is enforced by the Empire Coordinator.
	// We enforce it at decision time to prevent accidental approvals beyond the cap.
	if newStatus == "approved" && cfg != nil && cfg.Budget.HumanTasks.MaxTasksPerWeek > 0 {
		// Requeued tasks do not count against the current week's budget (spec v2.0):
		// they already counted when first approved.
		var requeueCount int
		_ = db.QueryRowContext(ctx, `SELECT COALESCE(requeue_count, 0) FROM human_tasks WHERE id = $1::uuid`, in.TaskID).Scan(&requeueCount)
		if requeueCount > 0 {
			goto skipBudget
		}
		weekStart := WeekStartUTC(time.Now(), cfg.Budget.HumanTasks.BudgetReset)
		var approvedThisWeek int
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(count(*), 0)
			FROM human_tasks
			WHERE reviewed_at >= $1
			  AND status IN ('approved', 'assigned', 'completed')
		`, weekStart).Scan(&approvedThisWeek); err == nil {
			if approvedThisWeek >= cfg.Budget.HumanTasks.MaxTasksPerWeek {
				// Force deferral to keep the request in the pipeline without reaching the human.
				newStatus = "deferred"
				evtType = events.EventType("human_task.deferred")
				if in.Reason == "" {
					in.Reason = "weekly human task budget exhausted"
				} else {
					in.Reason = "weekly human task budget exhausted: " + in.Reason
				}
				if in.RequeueDate == "" {
					in.RequeueDate = NextWeekResetUTC(time.Now(), cfg.Budget.HumanTasks.BudgetReset).Format(time.RFC3339)
				}
			}
		}
	}
skipBudget:

	decisionObj := map[string]any{
		"decision":      newStatus,
		"reason":        in.Reason,
		"decided_by":    actor.ID,
		"decided_at":    time.Now().UTC().Format(time.RFC3339),
		"priority_rank": in.PriorityRank,
	}
	if in.RequeueDate != "" {
		decisionObj["requeue_date"] = in.RequeueDate
	}
	decisionJSON, _ := json.Marshal(decisionObj)

	var requestingAgent string
	var verticalID string
	const q = `
		UPDATE human_tasks
		SET status = $2,
		    reviewed_at = now(),
		    review_decision = $3::jsonb
		WHERE id = $1::uuid
		RETURNING requesting_agent, COALESCE(vertical_id::text, '')
	`
	if err := db.QueryRowContext(ctx, q, in.TaskID, newStatus, decisionJSON).Scan(&requestingAgent, &verticalID); err != nil {
		return nil, fmt.Errorf("update human task decision: %w", err)
	}

	outPayload := map[string]any{
		"task_id":          in.TaskID,
		"requesting_agent": strings.TrimSpace(requestingAgent),
		"vertical_id":      strings.TrimSpace(verticalID),
	}
	switch string(evtType) {
	case "human_task.approved":
		outPayload["approved_reason"] = in.Reason
		outPayload["priority_rank"] = in.PriorityRank
	case "human_task.rejected":
		outPayload["rejection_reason"] = in.Reason
	case "human_task.deferred":
		outPayload["defer_reason"] = in.Reason
		if in.RequeueDate != "" {
			outPayload["requeue_date"] = in.RequeueDate
		}
	}

	if err := e.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        evtType,
		SourceAgent: actor.ID,
		VerticalID:  verticalID,
		Payload:     mustJSON(outPayload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"tool-executor",
			"failed to publish human task decision event=%s task_id=%s actor=%s: %v",
			strings.TrimSpace(string(evtType)),
			strings.TrimSpace(in.TaskID),
			strings.TrimSpace(actor.ID),
			err,
		)
	}

	return map[string]any{
		"task_id": in.TaskID,
		"status":  newStatus,
	}, nil
}

func (e *RuntimeToolExecutor) getManager() *AgentManager {
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

func (e *RuntimeToolExecutor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return nil, NewRuntimeError(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			false,
			"invalid emit tool name: %s",
			toolName,
		)
	}
	if !IsEmitToolAllowedForRole(actor.Role, toolName) {
		return nil, NewRuntimeError(
			"emit_tool_not_allowed",
			"tool-executor",
			"handle_emit_tool.authorize",
			false,
			"event type %q is not allowed for role %q",
			eventType,
			canonicalRuntimeRole(actor.Role),
		)
	}
	if e.bus == nil {
		return nil, NewRuntimeError(
			"dependency_unavailable",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			"event bus is not configured",
		)
	}

	payloadMap := map[string]any{}
	if err := decodeToolInput(input, &payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.decode_input",
			false,
			err,
			"invalid emit tool input",
		)
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}

	inbound, _ := InboundEventFromContext(ctx)
	if err := e.trackTransitionPrerequisites(actor, inbound); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_prerequisite_failed",
			"tool-executor",
			"handle_emit_tool.track_prerequisites",
			false,
			err,
			"emit transition prerequisites failed",
		)
	}

	payloadMap = e.enrichEmitPayloadContext(actor, inbound, eventType, payloadMap)
	payloadMap = e.preNormalizeEmitPayload(actor, inbound, eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_pre_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}
	payloadMap = e.normalizeEmitPayload(actor, inbound, eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_post_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}
	if err := e.enforceMigrationGuardrail(ctx, actor, eventType, payloadMap); err != nil {
		return nil, err
	}

	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: actor.ID,
		TaskID:      strings.TrimSpace(asString(payloadMap["task_id"])),
		VerticalID:  strings.TrimSpace(asString(payloadMap["vertical_id"])),
		Payload:     mustJSON(payloadMap),
		CreatedAt:   time.Now(),
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(actor.VerticalID)
	}
	if emitted.TaskID == "" {
		emitted.TaskID = strings.TrimSpace(inbound.TaskID)
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(inbound.VerticalID)
	}

	if err := e.validateEmitTransition(actor, inbound, emitted); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_guardrail_violation",
			"tool-executor",
			"handle_emit_tool.validate_transition",
			false,
			err,
			"emit transition rejected by guardrail",
		)
	}
	if err := e.bus.Publish(ctx, emitted); err != nil {
		return nil, WrapRuntimeError(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			err,
			"failed to publish emitted event type=%s event_id=%s",
			eventType,
			emitted.ID,
		)
	}

	if rec, ok := EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
		rec.Append(emitted)
	}
	return map[string]any{
		"status":     "published",
		"event_id":   emitted.ID,
		"event_type": eventType,
	}, nil
}

func (e *RuntimeToolExecutor) preNormalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if eventType == "source.scraped" {
		return preNormalizeSourceScrapedPayload(inbound, payload)
	}
	role := canonicalRuntimeRole(actor.Role)
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return preNormalizeCoordinatorScanRequestedPayload(inbound, payload)
	}
	if shouldFlattenLegacyNestedEmitPayload(eventType) {
		return preNormalizeLegacyNestedEmitPayload(payload)
	}
	return payload
}

func (e *RuntimeToolExecutor) enrichEmitPayloadContext(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	out := make(map[string]any, len(payload)+2)
	for k, v := range payload {
		out[k] = v
	}
	if emitSchemaAllowsProperty(eventType, "task_id") && strings.TrimSpace(asString(out["task_id"])) == "" {
		out["task_id"] = strings.TrimSpace(inbound.TaskID)
	}
	if emitSchemaAllowsProperty(eventType, "vertical_id") && strings.TrimSpace(asString(out["vertical_id"])) == "" {
		verticalID := strings.TrimSpace(actor.VerticalID)
		if verticalID == "" {
			verticalID = strings.TrimSpace(inbound.VerticalID)
		}
		out["vertical_id"] = verticalID
	}
	return out
}

func (e *RuntimeToolExecutor) normalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	role := canonicalRuntimeRole(actor.Role)
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return normalizeCoordinatorScanRequestedPayload(inbound, payload)
	}
	if role == "empire-coordinator" && strings.HasPrefix(eventType, "budget.") && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed" {
		payload["event_type"] = eventType
		if _, ok := payload["threshold_event_id"]; !ok {
			payload["threshold_event_id"] = strings.TrimSpace(inbound.ID)
		}
	}
	if eventType == "portfolio.digest_compiled" {
		msg := strings.TrimSpace(asString(payload["message"]))
		legacy := strings.TrimSpace(asString(payload["digest_text"]))
		switch {
		case msg == "" && legacy != "":
			payload["message"] = legacy
		case msg != "" && legacy == "":
			payload["digest_text"] = msg
		}
	}
	return payload
}

func normalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}

	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}

	mode := normalizeScanModeCompat(asString(out["mode"]))
	if mode == "" {
		mode = inferDiscoveryMode(directiveText)
	}
	if mode == "" {
		mode = "saas_gap"
	}
	out["mode"] = mode

	priority := normalizeScanPriorityCompat(asString(out["priority"]))
	if priority == "" {
		priority = "normal"
	}
	out["priority"] = priority

	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["taxonomy_categories"]; !ok {
		if categories := extractCategoryList(out); len(categories) > 0 {
			out["taxonomy_categories"] = categories
		} else {
			out["taxonomy_categories"] = []string{}
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		modes := []string{strings.TrimSpace(asString(out["mode"]))}
		if modes[0] == "" {
			modes[0] = "saas_gap"
		}
		strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
		if strategicContext == "" {
			strategicContext = directiveText
		}
		directiveID := strings.TrimSpace(asString(out["directive_id"]))
		if directiveID == "" {
			directiveID = strings.TrimSpace(inbound.ID)
		}
		out["campaign_context"] = map[string]any{
			"modes":             modes,
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func emitSchemaAllowsProperty(eventType, property string) bool {
	eventType = strings.TrimSpace(eventType)
	property = strings.TrimSpace(property)
	if eventType == "" || property == "" {
		return false
	}
	schema := schemaForEventType(eventType).Schema
	props, ok := schema["properties"].(map[string]any)
	if !ok || len(props) == 0 {
		return false
	}
	_, ok = props[property]
	return ok
}

func (e *RuntimeToolExecutor) enforceMigrationGuardrail(ctx context.Context, actor models.AgentConfig, eventType string, payload map[string]any) error {
	eventType = strings.TrimSpace(eventType)
	if eventType != "devops.deploy_requested" && eventType != "devops.rollback_requested" {
		return nil
	}
	migrationSQL := strings.TrimSpace(extractMigrationSQL(eventType, payload))
	if migrationSQL == "" {
		return nil
	}
	classification := ClassifyMigration(migrationSQL)
	if !classification.RequiresApproval {
		return nil
	}
	if e.mailboxStore != nil {
		contextPayload := map[string]any{
			"event_type":          eventType,
			"vertical_id":         strings.TrimSpace(asString(payload["vertical_id"])),
			"requesting_agent":    strings.TrimSpace(asString(payload["requesting_agent"])),
			"destructive_ops":     classification.DestructiveOps,
			"requires_approval":   classification.RequiresApproval,
			"migration_statement": migrationSQL,
		}
		if _, err := e.mailboxStore.InsertMailboxItem(ctx, MailboxItem{
			VerticalID: strings.TrimSpace(asString(payload["vertical_id"])),
			FromAgent:  actor.ID,
			Type:       "migration_approval",
			Priority:   "critical",
			Status:     "pending",
			Context:    mustJSON(contextPayload),
			Summary:    "Destructive migration requires human approval before deploy",
		}); err != nil {
			runtimeWarn("tool-executor", "failed to insert migration_approval mailbox item: %v", err)
		}
	}
	return NewRuntimeError(
		"migration_requires_approval",
		"tool-executor",
		"handle_emit_tool.migration_guardrail",
		false,
		"migration contains destructive operations and requires approval: %s",
		strings.Join(classification.DestructiveOps, "; "),
	)
}

func extractMigrationSQL(eventType string, payload map[string]any) string {
	if payload == nil {
		return ""
	}
	switch strings.TrimSpace(eventType) {
	case "devops.deploy_requested":
		if raw := strings.TrimSpace(asString(payload["migration_sql"])); raw != "" {
			return raw
		}
		if manifest, ok := payload["manifest"].(map[string]any); ok {
			return strings.TrimSpace(asString(manifest["migration_sql"]))
		}
	case "devops.rollback_requested":
		if raw := strings.TrimSpace(asString(payload["rollback_migration"])); raw != "" {
			return raw
		}
		if manifest, ok := payload["manifest"].(map[string]any); ok {
			return strings.TrimSpace(asString(manifest["rollback_migration"]))
		}
	}
	return ""
}

func preNormalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}
	originalMode := strings.TrimSpace(asString(out["mode"]))
	originalPriority := strings.TrimSpace(asString(out["priority"]))

	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn(
			"emit-normalization",
			"flattening coordinator scan.requested nested payload event_id=%s source=%s keys=%d",
			strings.TrimSpace(inbound.ID),
			strings.TrimSpace(inbound.SourceAgent),
			len(nested),
		)
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}

	modeRaw := asString(out["mode"])
	if mode := normalizeScanModeCompat(modeRaw); mode != "" {
		out["mode"] = mode
	} else if strings.TrimSpace(modeRaw) != "" {
		directiveText := ""
		if len(inbound.Payload) > 0 {
			var payload map[string]any
			if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
				directiveText = strings.TrimSpace(asString(payload["directive_text"]))
			}
		}
		inferred := inferDiscoveryMode(directiveText)
		if inferred != "" {
			runtimeWarn(
				"emit-normalization",
				"coercing invalid coordinator mode raw=%q inferred=%q event_id=%s",
				strings.TrimSpace(modeRaw),
				inferred,
				strings.TrimSpace(inbound.ID),
			)
		}
		out["mode"] = inferred
	}
	if priority := normalizeScanPriorityCompat(asString(out["priority"])); priority != "" {
		out["priority"] = priority
	}
	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		modes := []string{strings.TrimSpace(asString(out["mode"]))}
		if modes[0] == "" {
			modes[0] = "saas_gap"
		}
		strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
		if strategicContext == "" {
			strategicContext = directiveText
		}
		directiveID := strings.TrimSpace(asString(out["directive_id"]))
		if directiveID == "" {
			directiveID = strings.TrimSpace(inbound.ID)
		}
		out["campaign_context"] = map[string]any{
			"modes":             modes,
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	if coercedMode := strings.TrimSpace(asString(out["mode"])); originalMode != "" && coercedMode != "" && !strings.EqualFold(originalMode, coercedMode) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested mode normalized raw=%q normalized=%q event_id=%s",
			originalMode,
			coercedMode,
			strings.TrimSpace(inbound.ID),
		)
	}
	if coercedPriority := strings.TrimSpace(asString(out["priority"])); originalPriority != "" && coercedPriority != "" && !strings.EqualFold(originalPriority, coercedPriority) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested priority normalized raw=%q normalized=%q event_id=%s",
			originalPriority,
			coercedPriority,
			strings.TrimSpace(inbound.ID),
		)
	}

	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func shouldFlattenLegacyNestedEmitPayload(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		return true
	default:
		return false
	}
}

func preNormalizeLegacyNestedEmitPayload(current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn(
			"emit-normalization",
			"flattening legacy nested emit payload keys=%d",
			len(nested),
		)
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}
	delete(out, "payload")
	return out
}

func preNormalizeSourceScrapedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := preNormalizeLegacyNestedEmitPayload(current)
	currentGeo := strings.TrimSpace(asString(out["geography"]))
	if !isPlaceholderGeography(currentGeo) {
		return out
	}
	if inferred := extractAssignedGeography(inbound); inferred != "" {
		out["geography"] = inferred
	}
	return out
}

func extractAssignedGeography(inbound events.Event) string {
	payload := parsePayloadMap(inbound.Payload)
	if len(payload) == 0 {
		return ""
	}
	for _, key := range []string{"geography", "geography_label"} {
		if value := strings.TrimSpace(asString(payload[key])); !isPlaceholderGeography(value) {
			return value
		}
	}
	if shard, ok := asObject(payload["shard"]); ok {
		if scope, ok := asObject(shard["scope"]); ok {
			for _, key := range []string{"geography", "geography_label"} {
				if value := strings.TrimSpace(asString(scope[key])); !isPlaceholderGeography(value) {
					return value
				}
			}
			if geoID := strings.TrimSpace(asString(scope["geography_id"])); geoID != "" {
				return geoID
			}
		}
	}
	if geoID := strings.TrimSpace(asString(payload["geography_id"])); geoID != "" {
		return geoID
	}
	return ""
}

func isPlaceholderGeography(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return true
	}
	tokens := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', '/', '|', ';':
			return true
		default:
			return false
		}
	})
	if len(tokens) == 0 {
		tokens = []string{value}
	}
	placeholder := map[string]struct{}{
		"unspecified": {},
		"unknown":     {},
		"n/a":         {},
		"na":          {},
		"none":        {},
		"null":        {},
		"-":           {},
	}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if _, ok := placeholder[token]; !ok {
			return false
		}
	}
	return true
}

func normalizeScanModeCompat(raw string) string {
	if mode := normalizeScanMode(raw); mode != "" {
		return mode
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "automation_micro":
		return "saas_gap"
	case "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus", "corpus":
		return "corpus"
	case "derived":
		return "derived"
	default:
		return ""
	}
}

func normalizeScanPriorityCompat(raw string) string {
	if priority := normalizeScanPriority(raw); priority != "" {
		return priority
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func asObject(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	default:
		return nil, false
	}
}

func (e *RuntimeToolExecutor) trackTransitionPrerequisites(actor models.AgentConfig, inbound events.Event) error {
	role := canonicalRuntimeRole(actor.Role)
	inboundType := strings.TrimSpace(string(inbound.Type))
	if inboundType == "" {
		return nil
	}
	switch role {
	case "validation-coordinator":
		if inboundType == "vertical.needs_more_data" {
			e.clearOneShot(actor.ID, "vertical.ready_for_review", transitionContextKey(inbound, inbound))
		}
	case "business-research-agent":
		if inboundType == "validation.started" || inboundType == "spec.revision_requested" {
			e.clearOneShot(actor.ID, "spec.approved", transitionContextKey(inbound, inbound))
		}
	}
	return nil
}

func (e *RuntimeToolExecutor) validateEmitTransition(actor models.AgentConfig, inbound events.Event, emitted events.Event) error {
	role := canonicalRuntimeRole(actor.Role)
	inboundType := strings.TrimSpace(string(inbound.Type))
	emittedType := strings.TrimSpace(string(emitted.Type))
	switch {
	case role == "empire-coordinator" && emittedType == "opco.spinup_requested":
		if inboundType != "vertical.approved" {
			return fmt.Errorf("guardrail_violation transition_violation: opco.spinup_requested requires inbound vertical.approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && emittedType == "template.migration_completed":
		if inboundType != "template.migration_approved" {
			return fmt.Errorf("guardrail_violation transition_violation: template.migration_completed requires inbound template.migration_approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && strings.HasPrefix(emittedType, "budget.") && inboundType == "budget.threshold_crossed":
		expected := strings.TrimSpace(string(budgetEventTypeFromThresholdPayload(inbound.Payload)))
		if expected != "" && expected != emittedType {
			return fmt.Errorf("guardrail_violation transition_violation: expected %s for inbound budget.threshold_crossed, got %s", expected, emittedType)
		}
	case role == "factory-cto" && emittedType == "template.version_published":
		if inboundType != "spec.validation_passed" {
			return fmt.Errorf("guardrail_violation transition_violation: template.version_published requires inbound spec.validation_passed, got %s", inboundType)
		}
	case role == "validation-coordinator" && emittedType == "vertical.ready_for_review":
		if inboundType != "validation.package_ready" {
			return fmt.Errorf("guardrail_violation transition_violation: vertical.ready_for_review requires inbound validation.package_ready, got %s", inboundType)
		}
		key := transitionContextKey(emitted, inbound)
		if e.isOneShotEmitted(actor.ID, emittedType, key) {
			return fmt.Errorf("guardrail_violation duplicate_emission: vertical.ready_for_review already emitted for context=%s", key)
		}
		e.markOneShotEmitted(actor.ID, emittedType, key)
	case role == "business-research-agent" && emittedType == "spec.requested":
		if inboundType != "validation.started" {
			return fmt.Errorf("guardrail_violation transition_violation: spec.requested requires inbound validation.started, got %s", inboundType)
		}
	case role == "business-research-agent" && emittedType == "spec_review.requested":
		if inboundType != "spec.draft_ready" {
			return fmt.Errorf("guardrail_violation transition_violation: spec_review.requested requires inbound spec.draft_ready, got %s", inboundType)
		}
	case role == "business-research-agent" && emittedType == "spec.approved":
		if inboundType != "spec_review.passed" {
			return fmt.Errorf("guardrail_violation transition_violation: spec.approved requires inbound spec_review.passed, got %s", inboundType)
		}
		key := transitionContextKey(emitted, inbound)
		if e.isOneShotEmitted(actor.ID, emittedType, key) {
			return fmt.Errorf("guardrail_violation duplicate_emission: spec.approved already emitted for context=%s", key)
		}
		e.markOneShotEmitted(actor.ID, emittedType, key)
	}
	return nil
}

func (e *RuntimeToolExecutor) oneShotKey(agentID, eventType, contextKey string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventType) + "|" + strings.TrimSpace(contextKey)
}

func (e *RuntimeToolExecutor) isOneShotEmitted(agentID, eventType, contextKey string) bool {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return false
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	defer e.oneShotMu.Unlock()
	_, ok := e.oneShotEmits[key]
	return ok
}

func (e *RuntimeToolExecutor) markOneShotEmitted(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	e.oneShotEmits[key] = struct{}{}
	e.oneShotMu.Unlock()
}

func (e *RuntimeToolExecutor) clearOneShot(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	delete(e.oneShotEmits, key)
	e.oneShotMu.Unlock()
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

func (e *RuntimeToolExecutor) execSQLExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if strings.TrimSpace(actor.VerticalID) == "" {
		return nil, errors.New("sql_execute requires actor vertical_id")
	}

	var in struct {
		Query string `json:"query"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, errors.New("query is required")
	}
	normalizedQuery, err := sanitizeSQLReadQuery(query)
	if err != nil {
		return nil, err
	}

	schema := actor.VerticalID
	if slug, err := lookupVerticalSlug(ctx, db, actor.VerticalID); err == nil && strings.TrimSpace(slug) != "" {
		schema = slug + "_schema"
	}
	schema = sanitizeIdentifier(schema)
	if schema == "" {
		return nil, errors.New("failed to derive sql schema for actor")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin sql_execute tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path = "+quoteIdent(schema)); err != nil {
		return nil, fmt.Errorf("set search_path: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return nil, fmt.Errorf("set read only: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '15s'"); err != nil {
		return nil, fmt.Errorf("set statement timeout: %w", err)
	}

	rows, err := tx.QueryContext(ctx, normalizedQuery)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		rowObj := make(map[string]any, len(cols))
		for i, c := range cols {
			rowObj[c] = normalizeSQLValue(values[i])
		}
		out = append(out, rowObj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit read tx: %w", err)
	}
	return map[string]any{
		"rows":      out,
		"schema":    schema,
		"query":     normalizedQuery,
		"read_only": true,
	}, nil
}

func lookupVerticalSlug(ctx context.Context, db *sql.DB, verticalID string) (string, error) {
	var slug string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug); err != nil {
		return "", err
	}
	return strings.TrimSpace(slug), nil
}

func isSelectQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(query))
	return strings.HasPrefix(q, "select ") || strings.HasPrefix(q, "with ")
}

const (
	maxSQLQueryLength = 8000
	maxSQLResultRows  = 200
)

var (
	sqlForbiddenTokenPattern        = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|set|reset|call|do|copy|vacuum|analyze|comment)\b`)
	sqlCommentPattern               = regexp.MustCompile(`--|/\*|\*/`)
	sqlSchemaQualifiedFromJoinRegex = regexp.MustCompile(`(?is)\b(from|join)\s+((\"[^\"]+\"|[a-z_][a-z0-9_]*)\s*\.)`)
	sqlRestrictedSchemaPattern      = regexp.MustCompile(`(?is)(\"?(pg_catalog|information_schema|public)\"?\s*\.)`)
	sqlLimitPattern                 = regexp.MustCompile(`(?i)\blimit\s+([0-9]+)\b`)
)

func sanitizeSQLReadQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", errors.New("query is required")
	}
	if len(query) > maxSQLQueryLength {
		return "", fmt.Errorf("query too long (max %d chars)", maxSQLQueryLength)
	}
	if strings.Contains(query, ";") {
		return "", errors.New("multi-statement SQL is not allowed")
	}
	if sqlCommentPattern.MatchString(query) {
		return "", errors.New("SQL comments are not allowed")
	}
	if !isSelectQuery(query) {
		return "", errors.New("only read-only SELECT queries are allowed")
	}
	if sqlForbiddenTokenPattern.MatchString(query) {
		return "", errors.New("query contains non-read-only SQL")
	}
	if sqlRestrictedSchemaPattern.MatchString(query) {
		return "", errors.New("access to system/shared schemas is not allowed")
	}
	if sqlSchemaQualifiedFromJoinRegex.MatchString(query) {
		return "", errors.New("schema-qualified table references are not allowed")
	}
	if matches := sqlLimitPattern.FindStringSubmatch(query); len(matches) == 2 {
		limitVal := strings.TrimSpace(matches[1])
		if limitVal != "" {
			if n, convErr := strconv.Atoi(limitVal); convErr == nil && n > maxSQLResultRows {
				return "", fmt.Errorf("LIMIT exceeds maximum of %d", maxSQLResultRows)
			}
		}
		return query, nil
	}
	return query + fmt.Sprintf(" LIMIT %d", maxSQLResultRows), nil
}

func sanitizeIdentifier(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func normalizeSQLValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}

func (e *RuntimeToolExecutor) execNginxReload(ctx context.Context, actor models.AgentConfig, _ any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("nginx_reload is restricted to holding-devops")
	}
	if out, err := exec.CommandContext(ctx, "nginx", "-t").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("nginx config test failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.CommandContext(ctx, "systemctl", "reload", "nginx").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("nginx reload failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "reloaded"}, nil
}

func (e *RuntimeToolExecutor) execSystemdControl(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("systemd_control is restricted to holding-devops")
	}
	var in struct {
		Action string `json:"action"`
		Unit   string `json:"unit"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	action := strings.TrimSpace(strings.ToLower(in.Action))
	unit := strings.TrimSpace(in.Unit)
	switch action {
	case "start", "stop", "restart", "enable", "disable":
	default:
		return nil, fmt.Errorf("unsupported systemd action: %s", action)
	}
	if !strings.HasPrefix(unit, "empireai-") {
		return nil, errors.New("systemd unit must start with empireai-")
	}
	out, err := exec.CommandContext(ctx, "systemctl", action, unit).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("systemctl %s %s failed: %w: %s", action, unit, err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "ok", "action": action, "unit": unit}, nil
}

func (e *RuntimeToolExecutor) execCertbotExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if actor.Role != "holding-devops" {
		return nil, errors.New("certbot_execute is restricted to holding-devops")
	}
	var in struct {
		Domain string `json:"domain"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	domain := strings.TrimSpace(in.Domain)
	if domain == "" {
		return nil, errors.New("domain is required")
	}
	out, err := exec.CommandContext(ctx, "certbot", "--nginx", "-d", domain, "--non-interactive", "--agree-tos").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("certbot failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return map[string]any{"status": "ok", "domain": domain}, nil
}

func (e *RuntimeToolExecutor) execInstagramHandleCheck(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	_ = actor
	var in struct {
		Handle string `json:"handle"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	handle := strings.TrimSpace(strings.TrimPrefix(in.Handle, "@"))
	if handle == "" {
		return nil, errors.New("handle is required")
	}
	valid := regexp.MustCompile(`^[a-zA-Z0-9._]{1,30}$`)
	if !valid.MatchString(handle) {
		return nil, errors.New("invalid instagram handle format")
	}
	url := "https://www.instagram.com/" + handle + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	available := resp.StatusCode == http.StatusNotFound
	return map[string]any{
		"handle":    handle,
		"available": available,
		"status":    resp.StatusCode,
	}, nil
}

func (e *RuntimeToolExecutor) execEmailAPI(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	creds, err := e.loadVerticalCredentials(ctx, actor.VerticalID)
	if err != nil {
		return nil, err
	}
	emailCfg, _ := creds["email"].(map[string]any)
	smtpAddr, _ := emailCfg["smtp_addr"].(string)
	username, _ := emailCfg["username"].(string)
	password, _ := emailCfg["password"].(string)
	from, _ := emailCfg["from"].(string)
	if strings.TrimSpace(smtpAddr) == "" || strings.TrimSpace(from) == "" {
		return nil, errors.New("email credentials not configured (email.smtp_addr/email.from)")
	}

	var in struct {
		To      []string `json:"to"`
		Subject string   `json:"subject"`
		Body    string   `json:"body"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	if len(in.To) == 0 {
		return nil, errors.New("email_api requires at least one recipient")
	}
	msg := []byte(
		"To: " + strings.Join(in.To, ",") + "\r\n" +
			"Subject: " + in.Subject + "\r\n" +
			"MIME-Version: 1.0\r\n" +
			"Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
			in.Body,
	)
	host := strings.Split(strings.TrimSpace(smtpAddr), ":")[0]
	var auth smtp.Auth
	if strings.TrimSpace(username) != "" || strings.TrimSpace(password) != "" {
		auth = smtp.PlainAuth("", username, password, host)
	}
	if err := smtp.SendMail(smtpAddr, auth, from, in.To, msg); err != nil {
		return nil, fmt.Errorf("send email failed: %w", err)
	}
	return map[string]any{"status": "sent", "to": in.To}, nil
}

func (e *RuntimeToolExecutor) execExternalProxy(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	creds, err := e.loadExternalCredentials(ctx, actor.VerticalID, toolName)
	if err != nil {
		return nil, err
	}
	for k, v := range defaultExternalCredentialEnv(toolName) {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if _, exists := creds[k]; !exists {
			creds[k] = v
		}
	}

	var in struct {
		Method         string         `json:"method"`
		URL            string         `json:"url"`
		Path           string         `json:"path"`
		Query          map[string]any `json:"query"`
		Headers        map[string]any `json:"headers"`
		Body           any            `json:"body"`
		TimeoutSeconds int            `json:"timeout_seconds"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}

	reqURL := strings.TrimSpace(in.URL)
	if reqURL == "" {
		reqURL = strings.TrimSpace(asString(creds["endpoint"]))
	}
	if reqURL == "" {
		return nil, fmt.Errorf("%s endpoint not configured", toolName)
	}
	if strings.TrimSpace(in.Path) != "" {
		reqURL = strings.TrimRight(reqURL, "/") + "/" + strings.TrimLeft(strings.TrimSpace(in.Path), "/")
	}
	parsedURL, err := url.Parse(reqURL)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}
	q := parsedURL.Query()
	for k, v := range in.Query {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(asString(v))
		if key == "" || val == "" {
			continue
		}
		q.Set(key, val)
	}
	parsedURL.RawQuery = q.Encode()

	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = defaultExternalMethod(toolName)
	}

	var bodyReader io.Reader
	if method != http.MethodGet && method != http.MethodHead {
		payload := in.Body
		if payload == nil {
			payload = input
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	timeout := 30 * time.Second
	if in.TimeoutSeconds > 0 {
		timeout = time.Duration(in.TimeoutSeconds) * time.Second
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, parsedURL.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	applyExternalHeaders(req, in.Headers)
	applyExternalCredentialHeaders(req, creds, toolName)
	if req.Body != nil && req.Header.Get("content-type") == "" {
		req.Header.Set("content-type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("external request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	respBody := parseExternalResponseBody(respBytes)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s returned status=%d body=%v", toolName, resp.StatusCode, respBody)
	}
	return map[string]any{
		"status":      "ok",
		"tool":        toolName,
		"status_code": resp.StatusCode,
		"body":        respBody,
	}, nil
}

func (e *RuntimeToolExecutor) loadVerticalCredentials(ctx context.Context, verticalID string) (map[string]any, error) {
	e.mu.RLock()
	db := e.sqlDB
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if strings.TrimSpace(verticalID) == "" {
		return nil, errors.New("vertical_id is required for credentialed tool")
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(credentials, '{}'::jsonb)
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load vertical credentials: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode vertical credentials: %w", err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return e.decryptCredentialMap(ctx, out), nil
}

func (e *RuntimeToolExecutor) loadExternalCredentials(ctx context.Context, verticalID, toolName string) (map[string]any, error) {
	creds := map[string]any{}
	if strings.TrimSpace(verticalID) != "" {
		verticalCreds, err := e.loadVerticalCredentials(ctx, verticalID)
		if err != nil {
			return nil, err
		}
		switch toolName {
		case "whatsapp_business_api":
			mergeCredMap(creds, asMap(verticalCreds["whatsapp"]))
		case "instagram_api":
			mergeCredMap(creds, asMap(verticalCreds["instagram"]))
		case "domain_purchase", "domain_availability_check":
			mergeCredMap(creds, asMap(verticalCreds["registrar"]))
		case "dns_configure":
			mergeCredMap(creds, asMap(verticalCreds["dns"]))
		case "whatsapp_name_check":
			mergeCredMap(creds, asMap(verticalCreds["whatsapp_name_check"]))
		}
	}
	return creds, nil
}

func mergeCredMap(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func safeTelemetryText(v any) string {
	redacted := redactTelemetryValue(v)
	raw, err := json.Marshal(redacted)
	if err != nil {
		return truncateTelemetry(fmt.Sprintf("%v", redacted), maxToolTelemetryChars)
	}
	return truncateTelemetry(string(raw), maxToolTelemetryChars)
}

var telemetryPaymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)

const maxToolTelemetryChars = 1000

func redactTelemetryValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			if isSensitiveKey(k) {
				out[k] = "[REDACTED]"
				continue
			}
			out[k] = redactTelemetryValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = redactTelemetryValue(t[i])
		}
		return out
	case string:
		t = telemetryPaymentRefRegex.ReplaceAllString(t, "[PAYMENT_REF]")
		return truncateTelemetry(t, 220)
	default:
		return t
	}
}

func isSensitiveKey(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	for _, needle := range []string{
		"secret", "token", "password", "api_key", "apikey", "authorization", "auth",
		"payment_ref", "payment_reference", "transaction_id", "charge_id",
	} {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}

func truncateTelemetry(v string, max int) string {
	if max <= 0 || len(v) <= max {
		return v
	}
	return v[:max] + "..."
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

func defaultExternalMethod(toolName string) string {
	switch toolName {
	case "domain_availability_check", "whatsapp_name_check":
		return http.MethodGet
	default:
		return http.MethodPost
	}
}

func applyExternalHeaders(req *http.Request, headers map[string]any) {
	for k, v := range headers {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(asString(v))
		if key == "" || val == "" {
			continue
		}
		req.Header.Set(key, val)
	}
}

func applyExternalCredentialHeaders(req *http.Request, creds map[string]any, toolName string) {
	defaults := defaultExternalCredentialEnv(toolName)
	for k, v := range defaults {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if _, exists := creds[k]; !exists {
			creds[k] = v
		}
	}
	if hdrs := asMap(creds["headers"]); len(hdrs) > 0 {
		for k, v := range hdrs {
			req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(asString(v)))
		}
	}
	headerName := strings.TrimSpace(asString(creds["auth_header"]))
	if headerName == "" {
		headerName = "Authorization"
	}
	token := strings.TrimSpace(asString(creds["bearer_token"]))
	if token == "" {
		token = strings.TrimSpace(asString(creds["token"]))
	}
	if token == "" {
		token = strings.TrimSpace(asString(creds["api_key"]))
	}
	if token == "" {
		return
	}
	if strings.EqualFold(headerName, "Authorization") && !strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = "Bearer " + token
	}
	req.Header.Set(headerName, token)
}

func defaultExternalCredentialEnv(toolName string) map[string]string {
	switch toolName {
	case "domain_purchase", "domain_availability_check":
		return map[string]string{
			"endpoint": os.Getenv("REGISTRAR_API_ENDPOINT"),
			"api_key":  os.Getenv("REGISTRAR_API_KEY"),
		}
	case "dns_configure":
		endpoint := strings.TrimSpace(os.Getenv("CLOUDFLARE_API_ENDPOINT"))
		if endpoint == "" {
			endpoint = "https://api.cloudflare.com/client/v4"
		}
		return map[string]string{
			"endpoint": endpoint,
			"api_key":  os.Getenv("CLOUDFLARE_API_TOKEN"),
		}
	case "whatsapp_name_check":
		return map[string]string{
			"endpoint": os.Getenv("WHATSAPP_NAME_CHECK_API_ENDPOINT"),
			"api_key":  os.Getenv("WHATSAPP_NAME_CHECK_API_KEY"),
		}
	case "whatsapp_business_api":
		return map[string]string{
			"endpoint": os.Getenv("WHATSAPP_API_ENDPOINT"),
			"api_key":  os.Getenv("WHATSAPP_API_KEY"),
		}
	case "instagram_api":
		return map[string]string{
			"endpoint": os.Getenv("INSTAGRAM_API_ENDPOINT"),
			"api_key":  os.Getenv("INSTAGRAM_API_KEY"),
		}
	default:
		return map[string]string{}
	}
}

func parseExternalResponseBody(raw []byte) any {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return map[string]any{}
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err == nil {
		return parsed
	}
	return trimmed
}

func (e *RuntimeToolExecutor) decryptCredentialMap(ctx context.Context, in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = e.decryptCredentialValue(ctx, v)
	}
	return out
}

func (e *RuntimeToolExecutor) decryptCredentialValue(ctx context.Context, v any) any {
	switch t := v.(type) {
	case map[string]any:
		return e.decryptCredentialMap(ctx, t)
	case []any:
		arr := make([]any, len(t))
		for i := range t {
			arr[i] = e.decryptCredentialValue(ctx, t[i])
		}
		return arr
	case string:
		const prefix = "enc::"
		if !strings.HasPrefix(t, prefix) {
			return t
		}
		key := strings.TrimSpace(os.Getenv("EMPIREAI_CREDENTIALS_KEY"))
		if key == "" {
			return t
		}
		e.mu.RLock()
		db := e.sqlDB
		e.mu.RUnlock()
		if db == nil {
			return t
		}
		encoded := strings.TrimSpace(strings.TrimPrefix(t, prefix))
		if encoded == "" {
			return ""
		}
		var plain string
		if err := db.QueryRowContext(ctx, `
			SELECT pgp_sym_decrypt(decode($1, 'base64'), $2::text)
		`, encoded, key).Scan(&plain); err != nil {
			return t
		}
		return plain
	default:
		return v
	}
}
