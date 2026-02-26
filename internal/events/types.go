package events

import (
	"encoding/json"
	"time"
)

type EventType string

type Event struct {
	ID          string          `json:"id"`
	Type        EventType       `json:"type"`
	SourceAgent string          `json:"source_agent"`
	TaskID      string          `json:"task_id,omitempty"`
	VerticalID  string          `json:"vertical_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}
