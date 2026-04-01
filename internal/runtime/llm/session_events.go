package llm

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
)

func publishAgentStarted(ctx context.Context, publisher EventPublisher, session *Session, eventType events.EventType) {
	if publisher == nil || session == nil || strings.TrimSpace(session.AgentID) == "" {
		return
	}
	if err := publisher.MarkDeliveryInProgress(ctx, session.AgentID, session.ID); err != nil {
		log.Printf("mark delivery in progress failed agent=%s session=%s err=%v", strings.TrimSpace(session.AgentID), strings.TrimSpace(session.ID), err)
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	payload := map[string]any{
		"agent_id":          strings.TrimSpace(session.AgentID),
		"flow_instance":     nil,
		"conversation_mode": strings.TrimSpace(session.ConversationMode),
		"model_tier":        sessionModelTier(actor),
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
	}
	if flowInstance := strings.TrimSpace(session.ScopeKey); flowInstance != "" && strings.TrimSpace(session.RuntimeMode) == "session" {
		payload["flow_instance"] = flowInstance
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		log.Printf("marshal %s payload failed: %v", strings.TrimSpace(string(eventType)), err)
		return
	}
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        eventType,
		SourceAgent: "runtime",
		Payload:     raw,
		CreatedAt:   time.Now(),
	}
	if entityID := actor.EffectiveEntityID(); entityID != "" {
		evt = evt.WithEntityID(entityID)
	}
	if err := publisher.Publish(ctx, evt); err != nil {
		log.Printf("publish %s failed agent=%s err=%v", strings.TrimSpace(string(eventType)), strings.TrimSpace(session.AgentID), err)
	}
}

func sessionModelTier(actor runtimeactors.AgentConfig) string {
	if modelTier := strings.TrimSpace(actor.Type); modelTier != "" {
		return modelTier
	}
	if len(actor.Config) > 0 {
		var parsed map[string]any
		if json.Unmarshal(actor.Config, &parsed) == nil {
			if modelTier, _ := parsed["model_tier"].(string); strings.TrimSpace(modelTier) != "" {
				return strings.TrimSpace(modelTier)
			}
		}
	}
	return ""
}
