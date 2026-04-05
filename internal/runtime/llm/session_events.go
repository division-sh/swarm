package llm

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
)

func publishAgentStarted(ctx context.Context, publisher EventPublisher, session *Session, eventType events.EventType) {
	if publisher == nil || session == nil || strings.TrimSpace(session.AgentID) == "" {
		return
	}
	if err := publisher.MarkDeliveryInProgress(ctx, session.AgentID, session.ID); err != nil {
		logPublisherRuntime(ctx, publisher, "error", "mark_delivery_in_progress_failed", "Marking the agent delivery in progress failed", session.AgentID, session.ID, "", nil, err)
	}
	actor, _ := runtimeactors.ActorFromContext(ctx)
	payload := map[string]any{
		"agent_id":          strings.TrimSpace(session.AgentID),
		"flow_instance":     nil,
		"conversation_mode": strings.TrimSpace(session.ConversationMode),
		"session_scope":     strings.TrimSpace(session.SessionScope),
		"model_tier":        sessionModelTier(actor),
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
	}
	if flowInstance := strings.TrimSpace(actor.CanonicalFlowPath()); flowInstance != "" && strings.TrimSpace(session.SessionScope) == sessions.SessionScopeEntity.String() {
		payload["flow_instance"] = flowInstance
	}
	if flowInstance := strings.TrimSpace(session.ScopeKey); flowInstance != "" && strings.TrimSpace(session.SessionScope) == sessions.SessionScopeFlow.String() {
		payload["flow_instance"] = flowInstance
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		logPublisherRuntime(ctx, publisher, "error", "marshal_agent_started_payload_failed", "Marshalling the agent-started payload failed", session.AgentID, session.ID, "", map[string]any{
			"event_type": strings.TrimSpace(string(eventType)),
		}, err)
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
	if flowInstance := strings.TrimSpace(asString(payload["flow_instance"])); flowInstance != "" {
		evt = evt.WithFlowInstance(flowInstance)
	}
	if err := publisher.Publish(ctx, evt); err != nil {
		logPublisherRuntime(ctx, publisher, "error", "publish_agent_started_failed", "Publishing the agent-started event failed", session.AgentID, session.ID, evt.EntityID(), map[string]any{
			"event_type": strings.TrimSpace(string(eventType)),
		}, err)
	}
}

func sessionModelTier(actor runtimeactors.AgentConfig) string {
	if modelTier := strings.TrimSpace(actor.ModelTier); modelTier != "" {
		return modelTier
	}
	if modelTier := strings.TrimSpace(actor.Type); modelTier != "" {
		return modelTier
	}
	return ""
}
