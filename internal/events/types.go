package events

import (
	"encoding/json"
	"strings"
	"time"
)

type EventType string

type EventScope string

const (
	EventScopeGlobal EventScope = "global"
	EventScopeFlow   EventScope = "flow"
	EventScopeEntity EventScope = "entity"
)

type EventEnvelope struct {
	EntityID     string     `json:"-"`
	FlowInstance string     `json:"-"`
	Scope        EventScope `json:"-"`
}

type Event struct {
	ID            string          `json:"id"`
	Type          EventType       `json:"type"`
	SourceAgent   string          `json:"source_agent"`
	TaskID        string          `json:"task_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	ChainDepth    int             `json:"-"`
	RunID         string          `json:"-"`
	ParentEventID string          `json:"-"`
	Envelope      EventEnvelope   `json:"-"`
	CreatedAt     time.Time       `json:"created_at"`
}

type PersistedReplayEvent struct {
	Event       Event
	ReplayError string
}

func (e Event) WithEnvelope(envelope EventEnvelope) Event {
	e.Envelope = envelope.Normalized()
	return e
}

func (e Event) WithEntityID(entityID string) Event {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.EntityID = entityID
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope
	return e
}

func (e Event) WithFlowInstance(flowInstance string) Event {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return e
	}
	envelope := e.Envelope.Normalized()
	envelope.FlowInstance = flowInstance
	envelope.Scope = inferEventScope(envelope.EntityID, envelope.FlowInstance)
	e.Envelope = envelope
	return e
}

func (e Event) ContextMap(currentState string) map[string]any {
	out := map[string]any{}
	if id := strings.TrimSpace(e.ID); id != "" {
		out["id"] = id
	}
	if eventType := strings.TrimSpace(string(e.Type)); eventType != "" {
		out["type"] = eventType
		out["trigger_event_type"] = eventType
	}
	if sourceAgent := strings.TrimSpace(e.SourceAgent); sourceAgent != "" {
		out["source_agent"] = sourceAgent
	}
	if taskID := strings.TrimSpace(e.TaskID); taskID != "" {
		out["task_id"] = taskID
	}
	envelope := e.NormalizedEnvelope()
	if envelope.EntityID != "" {
		out["entity_id"] = envelope.EntityID
	}
	if envelope.FlowInstance != "" {
		out["flow_instance"] = envelope.FlowInstance
	}
	if envelope.Scope != "" {
		out["scope"] = string(envelope.Scope)
	}
	if currentState = strings.TrimSpace(currentState); currentState != "" {
		out["current_state"] = currentState
	}
	if parentEventID := strings.TrimSpace(e.ParentEventID); parentEventID != "" {
		out["source_event_id"] = parentEventID
	}
	if !e.CreatedAt.IsZero() {
		out["emitted_at"] = e.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func (e Event) EntityID() string {
	return strings.TrimSpace(e.Envelope.Normalized().EntityID)
}

func (e Event) FlowInstance() string {
	return strings.TrimSpace(e.Envelope.Normalized().FlowInstance)
}

func (e Event) Scope() EventScope {
	return e.Envelope.Normalized().Scope
}

func (e Event) NormalizedEnvelope() EventEnvelope {
	return e.Envelope.Normalized()
}

func (e EventEnvelope) Normalized() EventEnvelope {
	e.EntityID = strings.TrimSpace(e.EntityID)
	e.FlowInstance = strings.Trim(strings.TrimSpace(e.FlowInstance), "/")
	e.Scope = normalizeEventScope(e.Scope)
	if e.Scope == "" {
		e.Scope = inferEventScope(e.EntityID, e.FlowInstance)
	}
	return e
}

func inferEventScope(entityID, flowInstance string) EventScope {
	entityID = strings.TrimSpace(entityID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	switch {
	case entityID != "":
		return EventScopeEntity
	case flowInstance != "":
		return EventScopeFlow
	default:
		return EventScopeGlobal
	}
}

func normalizeEventScope(scope EventScope) EventScope {
	switch strings.ToLower(strings.TrimSpace(string(scope))) {
	case "":
		return ""
	case string(EventScopeEntity):
		return EventScopeEntity
	case string(EventScopeFlow):
		return EventScopeFlow
	case string(EventScopeGlobal):
		return EventScopeGlobal
	default:
		return ""
	}
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
