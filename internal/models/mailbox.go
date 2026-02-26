package models

import (
	"encoding/json"
	"time"
)

type MailboxItem struct {
	ID            string          `json:"id"`
	EventID       string          `json:"event_id,omitempty"`
	VerticalID    string          `json:"vertical_id,omitempty"`
	FromAgent     string          `json:"from_agent,omitempty"`
	Type          string          `json:"type"`
	Priority      string          `json:"priority"`
	Status        string          `json:"status"`
	Summary       string          `json:"summary,omitempty"`
	Context       json.RawMessage `json:"context,omitempty"`
	TimeoutAt     time.Time       `json:"timeout_at,omitempty"`
	Decision      string          `json:"decision,omitempty"`
	DecisionNotes string          `json:"decision_notes,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}
