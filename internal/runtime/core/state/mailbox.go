package state

import (
	"encoding/json"
	"strings"
	"time"
)

// MailboxItem is a platform-owned durable human-task/escalation record.
type MailboxItem struct {
	ID             string          `json:"id"`
	EventID        string          `json:"event_id,omitempty"`
	EntityID       string          `json:"entity_id,omitempty"`
	FlowInstance   string          `json:"flow_instance,omitempty"`
	FromAgent      string          `json:"from_agent,omitempty"`
	Type           string          `json:"type"`
	Priority       string          `json:"priority"`
	Status         string          `json:"status"`
	Notified       bool            `json:"notified,omitempty"`
	Summary        string          `json:"summary,omitempty"`
	Context        json.RawMessage `json:"context,omitempty"`
	TimeoutAt      time.Time       `json:"timeout_at,omitempty"`
	Decision       string          `json:"decision,omitempty"`
	DecisionNotes  string          `json:"decision_notes,omitempty"`
	ReplyContextID string          `json:"-"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func (m MailboxItem) EffectiveEntityID() string {
	return strings.TrimSpace(m.EntityID)
}

func (m *MailboxItem) NormalizeEntityID() {
	if m == nil {
		return
	}
	entityID := m.EffectiveEntityID()
	m.EntityID = entityID
}
