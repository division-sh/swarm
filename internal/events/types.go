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
	e.Payload = withEntityIDPayload(e.Payload, entityID)
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
	e.Payload = withPayloadString(e.Payload, "flow_instance", flowInstance)
	return e
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

func withEntityIDPayload(raw json.RawMessage, entityID string) json.RawMessage {
	return withPayloadString(raw, "entity_id", entityID)
}

func withPayloadString(raw json.RawMessage, key, value string) json.RawMessage {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return raw
	}
	payload := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
			return raw
		}
	}
	payload[key] = value
	encoded, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return encoded
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
