package models

import (
	"encoding/json"
	"time"
)

type SpendEntry struct {
	ID          string          `json:"id"`
	VerticalID  string          `json:"vertical_id,omitempty"`
	AgentID     string          `json:"agent_id,omitempty"`
	Category    string          `json:"category"`
	AmountCents int             `json:"amount_cents"`
	Currency    string          `json:"currency"`
	Description string          `json:"description,omitempty"`
	Source      string          `json:"source"`
	ApprovedBy  string          `json:"approved_by,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}
