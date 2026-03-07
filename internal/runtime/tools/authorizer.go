package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

type ToolAuthorizer struct {
	bus EventPublisher
}

func NewToolAuthorizer(bus EventPublisher) *ToolAuthorizer {
	return &ToolAuthorizer{bus: bus}
}

func (a *ToolAuthorizer) Authorize(ctx context.Context, actor models.AgentConfig, toolName string) error {
	if IsUniversal(toolName) {
		return nil
	}
	if IsEmitToolAllowedForRole(actor.Role, toolName) {
		return nil
	}
	allowed, constrained := extractAllowedToolsFromConfig(actor)
	if !constrained {
		return nil
	}
	if _, ok := allowed[toolName]; ok {
		return nil
	}
	err := fmt.Errorf("tool %s is not allowed for agent %s", toolName, actor.ID)
	if a.bus != nil {
		payload, marshalErr := json.Marshal(map[string]any{
			"reason":       "tool_not_allowed",
			"agent_id":     actor.ID,
			"agent_role":   actor.Role,
			"tool_name":    toolName,
			"vertical_id":  actor.VerticalID,
			"runtime_tool": true,
		})
		if marshalErr == nil {
			if pubErr := a.bus.Publish(ctx, events.Event{
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
	}
	return err
}

func extractAllowedToolsFromConfig(actor models.AgentConfig) (map[string]struct{}, bool) {
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
