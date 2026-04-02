package events

import (
	"encoding/json"
	"strings"
	"time"
)

type EventType string

type Event struct {
	ID            string          `json:"id"`
	Type          EventType       `json:"type"`
	SourceAgent   string          `json:"source_agent"`
	TaskID        string          `json:"task_id,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	ChainDepth    int             `json:"-"`
	RunID         string          `json:"-"`
	TraceID       string          `json:"-"`
	ParentEventID string          `json:"-"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (e Event) WithEntityID(entityID string) Event {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return e
	}
	e.Payload = withEntityIDPayload(e.Payload, entityID)
	return e
}

func (e Event) EntityID() string {
	if len(e.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(e.Payload, &payload); err == nil && payload != nil {
			if value := strings.TrimSpace(asString(payload["entity_id"])); value != "" {
				return value
			}
		}
	}
	return ""
}

func withEntityIDPayload(raw json.RawMessage, entityID string) json.RawMessage {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return raw
	}
	payload := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
			return raw
		}
	}
	payload["entity_id"] = entityID
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
