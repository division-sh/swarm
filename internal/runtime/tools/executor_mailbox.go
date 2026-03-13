package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	models "empireai/internal/runtime/actors"
)

func (e *Executor) execMailboxSend(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	if e.mailboxStore == nil {
		return nil, errors.New("mailbox store is not configured")
	}
	if err := authorizeMailboxSend(actor); err != nil {
		return nil, err
	}
	var in struct {
		EventID    string `json:"event_id"`
		EntityID   string `json:"entity_id"`
		VerticalID string `json:"vertical_id"`
		Type       string `json:"type"`
		Priority   string `json:"priority"`
		Summary    string `json:"summary"`
		Context    any    `json:"context"`
		TimeoutAt  string `json:"timeout_at"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	entityID := strings.TrimSpace(coalesce(in.EntityID, in.VerticalID, actor.VerticalID))
	if strings.TrimSpace(in.VerticalID) == "" {
		in.VerticalID = entityID
	}
	if entityID != "" && strings.TrimSpace(actor.VerticalID) != "" && entityID != actor.VerticalID {
		return nil, errors.New("cross-entity mailbox item is not allowed")
	}
	if strings.TrimSpace(in.Type) == "" {
		return nil, errors.New("mailbox type is required")
	}
	normalizedType, err := NormalizeMailboxType(in.Type)
	if err != nil {
		return nil, err
	}
	in.Type = normalizedType
	if strings.TrimSpace(in.Priority) == "" {
		in.Priority = "normal"
	}
	normalizedPriority, err := NormalizeMailboxPriority(in.Priority)
	if err != nil {
		return nil, err
	}
	in.Priority = normalizedPriority
	ctxJSON, err := json.Marshal(in.Context)
	if err != nil {
		return nil, fmt.Errorf("marshal mailbox context: %w", err)
	}
	if len(ctxJSON) == 0 || string(ctxJSON) == "null" {
		ctxJSON = []byte("{}")
	}
	var timeout time.Time
	if strings.TrimSpace(in.TimeoutAt) != "" {
		parsed, err := time.Parse(time.RFC3339, in.TimeoutAt)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout_at: %w", err)
		}
		timeout = parsed
	}

	id, err := e.mailboxStore.InsertMailboxItem(ctx, MailboxItem{
		EventID:    in.EventID,
		VerticalID: entityID,
		FromAgent:  actor.ID,
		Type:       in.Type,
		Priority:   in.Priority,
		Status:     "pending",
		Context:    ctxJSON,
		Summary:    in.Summary,
		TimeoutAt:  timeout,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": "queued", "mailbox_id": id}, nil
}
