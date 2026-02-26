package models

import (
	"encoding/json"
	"time"
)

type FounderDirective struct {
	ID          string    `json:"id"`
	VerticalID  string    `json:"vertical_id,omitempty"`
	Directive   string    `json:"directive"`
	ProvidedBy  string    `json:"provided_by,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	EffectiveAt time.Time `json:"effective_at,omitempty"`
}

type FounderDecision struct {
	ID        string          `json:"id"`
	MailboxID string          `json:"mailbox_id"`
	Decision  string          `json:"decision"`
	Notes     string          `json:"notes,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	DecidedBy string          `json:"decided_by,omitempty"`
	DecidedAt time.Time       `json:"decided_at"`
}
