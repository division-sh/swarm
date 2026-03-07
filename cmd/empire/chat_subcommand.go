package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/runtime/sessions"
	"github.com/google/uuid"
)

type targetAgent struct {
	ID          string
	Role        string
	VerticalID  string
	VerticalKey string
	Config      models.AgentConfig
}

func resolveTargetAgent(ctx context.Context, stores storeBundle, raw string) (targetAgent, error) {
	if stores.ManagerStore == nil {
		return targetAgent{}, fmt.Errorf("target resolution requires persistent store mode (use -store postgres)")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return targetAgent{}, fmt.Errorf("target is required")
	}
	agents, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return targetAgent{}, fmt.Errorf("load agents: %w", err)
	}
	if len(agents) == 0 {
		return resolveTargetFallback(raw)
	}
	if strings.Contains(raw, "/") {
		parts := strings.SplitN(raw, "/", 2)
		verticalID := strings.TrimSpace(parts[0])
		role := normalizeAgentAlias(parts[1])
		if verticalID == "" || role == "" {
			return targetAgent{}, fmt.Errorf("invalid target: %s", raw)
		}
		candidateID := fmt.Sprintf("%s-%s", role, verticalID)
		for _, rec := range agents {
			if rec.Config.ID == candidateID {
				return targetAgent{ID: rec.Config.ID, Role: rec.Config.Role, VerticalID: rec.Config.VerticalID, VerticalKey: rec.Config.VerticalID, Config: rec.Config}, nil
			}
		}
		for _, rec := range agents {
			if rec.Config.VerticalID == verticalID && rec.Config.Role == role {
				return targetAgent{ID: rec.Config.ID, Role: rec.Config.Role, VerticalID: rec.Config.VerticalID, VerticalKey: rec.Config.VerticalID, Config: rec.Config}, nil
			}
		}
		return targetAgent{}, fmt.Errorf("agent target not found: %s", raw)
	}
	alias := normalizeAgentAlias(raw)
	for _, rec := range agents {
		if rec.Config.ID == raw {
			return targetAgent{ID: rec.Config.ID, Role: rec.Config.Role, VerticalID: rec.Config.VerticalID, VerticalKey: rec.Config.VerticalID, Config: rec.Config}, nil
		}
	}
	matches := make([]targetAgent, 0)
	for _, rec := range agents {
		if rec.Config.Role == alias {
			matches = append(matches, targetAgent{ID: rec.Config.ID, Role: rec.Config.Role, VerticalID: rec.Config.VerticalID, VerticalKey: rec.Config.VerticalID, Config: rec.Config})
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return targetAgent{}, fmt.Errorf("ambiguous target %q: use <vertical>/<agent> or full agent id", raw)
	}
	if t, err := resolveTargetFallback(raw); err == nil {
		return t, nil
	}
	return targetAgent{}, fmt.Errorf("agent target not found: %s", raw)
}

func resolveTargetFallback(raw string) (targetAgent, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return targetAgent{}, fmt.Errorf("target is required")
	}
	if strings.Contains(raw, "/") {
		parts := strings.SplitN(raw, "/", 2)
		verticalKey := strings.TrimSpace(parts[0])
		role := normalizeAgentAlias(parts[1])
		if verticalKey == "" || role == "" {
			return targetAgent{}, fmt.Errorf("invalid target: %s", raw)
		}
		verticalID := ""
		if isUUID(verticalKey) {
			verticalID = verticalKey
		}
		id := fmt.Sprintf("%s-%s", role, verticalKey)
		cfg := models.AgentConfig{ID: id, Role: role, Mode: "operating", VerticalID: verticalID}
		return targetAgent{ID: id, Role: role, VerticalID: verticalID, VerticalKey: verticalKey, Config: cfg}, nil
	}
	role := normalizeAgentAlias(raw)
	cfg := models.AgentConfig{ID: raw, Role: role, Mode: "factory"}
	return targetAgent{ID: raw, Role: role, Config: cfg}, nil
}

func ensureTargetAgentRegistered(ctx context.Context, stores storeBundle, target targetAgent) error {
	if stores.ManagerStore == nil {
		return nil
	}
	loaded, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	for _, rec := range loaded {
		if strings.TrimSpace(rec.Config.ID) == strings.TrimSpace(target.ID) {
			return nil
		}
	}
	if !hasSystemPrompt(target.Config) {
		return fmt.Errorf("target agent %q is not registered with a valid system_prompt; run `empire init` or seed org before sending directives", target.ID)
	}
	return stores.ManagerStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config:          target.Config,
		Status:          "active",
		HiredBy:         "human-interface",
		TemplateVersion: "2.0.15",
	})
}

func ensureChatTargetAgentRegistered(ctx context.Context, stores storeBundle, target targetAgent) error {
	if stores.ManagerStore == nil {
		return nil
	}
	loaded, err := stores.ManagerStore.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	for _, rec := range loaded {
		if strings.TrimSpace(rec.Config.ID) == strings.TrimSpace(target.ID) {
			return nil
		}
	}
	cfg := target.Config
	if !hasSystemPrompt(cfg) {
		cfg.ID = strings.TrimSpace(target.ID)
		cfg.Role = strings.TrimSpace(nullable(cfg.Role, target.Role))
		if strings.TrimSpace(cfg.Mode) == "" {
			cfg.Mode = "operating"
		}
		if strings.TrimSpace(cfg.Type) == "" {
			cfg.Type = "sonnet"
		}
		cfg.Config = mustJSON(map[string]any{
			"system_prompt": "You are a temporary chat endpoint for a not-yet-bootstrapped agent. Acknowledge board input briefly and request full bootstrap context before acting.",
			"tools":         []string{},
			"subscriptions": []string{"board.chat", "board.directive"},
		})
		cfg.Subscriptions = []string{"board.chat", "board.directive"}
	}
	return stores.ManagerStore.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config:          cfg,
		Status:          "active",
		HiredBy:         "human-interface",
		TemplateVersion: "2.0.15",
	})
}

func hasSystemPrompt(cfg models.AgentConfig) bool {
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return false
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v) != ""
}

func requireSystemStarted(ctx context.Context, db *sql.DB) error {
	ok, err := hasSystemStarted(ctx, db)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("system is not initialized yet (missing system.started): run `empire init` first")
	}
	return nil
}

func hasSystemStarted(ctx context.Context, db *sql.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("directive requires postgres store mode")
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE type = 'system.started')`).Scan(&exists); err != nil {
		return false, fmt.Errorf("check system.started: %w", err)
	}
	return exists, nil
}

type globalAgentStore interface {
	runtimemanager.AgentPersistence
}

func syncRuntimeGlobalAgents(ctx context.Context, managerStore globalAgentStore) error {
	if managerStore == nil {
		return nil
	}
	agentsDir := strings.TrimSpace(os.Getenv("EMPIREAI_GLOBAL_AGENTS_DIR"))
	if agentsDir == "" {
		agentsDir = "configs/agents"
	}
	rosterPath := filepath.Join(agentsDir, "roster.yaml")
	if _, err := os.Stat(rosterPath); err != nil {
		return nil
	}
	return seedGlobalAgentsFromYAML(ctx, managerStore, agentsDir)
}

type loadedAgentStore interface {
	LoadAgents(ctx context.Context) ([]runtimemanager.PersistedAgent, error)
}

func rotateGlobalAgentSessions(ctx context.Context, managerStore loadedAgentStore, sessionRegistry sessions.Registry, runtimeMode string) error {
	if managerStore == nil || sessionRegistry == nil {
		return nil
	}
	runtimeMode = strings.TrimSpace(runtimeMode)
	if runtimeMode == "" {
		return nil
	}
	agents, err := managerStore.LoadAgents(ctx)
	if err != nil {
		return err
	}
	for _, rec := range agents {
		agentID := strings.TrimSpace(rec.Config.ID)
		if agentID == "" || strings.TrimSpace(rec.Config.VerticalID) != "" {
			continue
		}
		if _, err := sessionRegistry.Rotate(ctx, agentID, runtimeMode, "runtime-sync", "global config sync", ""); err != nil {
			errText := strings.ToLower(strings.TrimSpace(err.Error()))
			if strings.Contains(errText, "no active session to rotate") {
				continue
			}
			return fmt.Errorf("rotate global session agent=%s: %w", agentID, err)
		}
	}
	return nil
}

func isUUID(v string) bool {
	_, err := uuid.Parse(strings.TrimSpace(v))
	return err == nil
}

func normalizeAgentAlias(v string) string {
	a := strings.ToLower(strings.TrimSpace(v))
	switch a {
	case "ceo":
		return "opco-ceo"
	case "hop", "head-of-product":
		return "vp-product"
	case "hog", "head-of-growth":
		return "vp-growth"
	case "cto":
		return "cto-agent"
	case "pm":
		return "pm-agent"
	case "support":
		return "support-agent"
	case "marketing":
		return "marketing-agent"
	case "backend":
		return "backend-agent"
	case "frontend":
		return "frontend-agent"
	case "qa":
		return "qa-agent"
	case "devops":
		return "devops-agent"
	case "cos", "chief-of-staff":
		return "chief-of-staff"
	default:
		return a
	}
}

func dispatchBoardMessage(ctx context.Context, stores storeBundle, target targetAgent, eventType events.EventType, message string) (string, error) {
	if stores.EventStore == nil {
		return "", fmt.Errorf("directive/chat requires persistent store mode (use -store postgres)")
	}
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("message is required")
	}
	payload := mustJSON(map[string]any{
		"target_agent_id": target.ID,
		"role":            target.Role,
		"vertical_id":     target.VerticalID,
		"vertical_key":    target.VerticalKey,
		"message":         msg,
		"sent_by":         "human-board",
		"sent_at":         time.Now().UTC().Format(time.RFC3339),
	})
	eventID := uuid.NewString()
	evt := events.Event{ID: eventID, Type: eventType, SourceAgent: "human-board", VerticalID: target.VerticalID, Payload: payload, CreatedAt: time.Now()}
	if err := stores.EventStore.AppendEvent(ctx, evt); err != nil {
		return "", err
	}
	if err := stores.EventStore.InsertEventDeliveries(ctx, eventID, []string{target.ID}); err != nil {
		return "", err
	}
	return eventID, nil
}

func dispatchSystemDirective(ctx context.Context, stores storeBundle, target targetAgent, message string) (string, error) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		return "", fmt.Errorf("message is required")
	}
	eventID, attempted, err := dispatchSystemDirectiveViaDashboard(ctx, target, msg)
	if err == nil {
		return eventID, nil
	}
	fallbackEventID, fallbackErr := dispatchSystemDirectiveDirect(ctx, stores, target, msg)
	if fallbackErr == nil {
		return fallbackEventID, nil
	}
	if !attempted {
		return "", fmt.Errorf("directive dispatch unavailable: dashboard control endpoint not attempted")
	}
	return "", fmt.Errorf("directive dispatch requires runtime interceptor (/api/directive): %w", err)
}

func dispatchSystemDirectiveDirect(ctx context.Context, stores storeBundle, target targetAgent, message string) (string, error) {
	if stores.EventStore == nil {
		return "", fmt.Errorf("directive dispatch unavailable: event store is not configured")
	}
	payload := mustJSON(map[string]any{
		"directive_text": strings.TrimSpace(message),
		"sent_by":        "cli",
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	})
	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType("system.directive"),
		SourceAgent: "human-board",
		VerticalID:  strings.TrimSpace(target.VerticalID),
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := stores.EventStore.AppendEvent(ctx, evt); err != nil {
		return "", fmt.Errorf("append system.directive event: %w", err)
	}
	if err := stores.EventStore.InsertEventDeliveries(ctx, eventID, []string{strings.TrimSpace(target.ID)}); err != nil {
		return "", fmt.Errorf("insert directive delivery: %w", err)
	}
	return eventID, nil
}
