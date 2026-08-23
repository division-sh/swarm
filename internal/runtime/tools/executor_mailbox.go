package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func (e *Executor) execNotifyHuman(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	store, err := e.mailboxStoreDependency()
	if err != nil {
		return nil, err
	}
	if err := authorizeNotifyHuman(e.authority, actor); err != nil {
		return nil, err
	}
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return nil, err
	}
	if err := ValidatePayloadAgainstSchema(notifyHumanContractSchema().InputSchema, payload); err != nil {
		return nil, fmt.Errorf("validate notify_human input: %w", err)
	}
	var in struct {
		Summary string `json:"summary"`
		Context any    `json:"context"`
	}
	if err := decodeToolInput(payload, &in); err != nil {
		return nil, err
	}
	in.Summary = strings.TrimSpace(in.Summary)
	if in.Summary == "" {
		return nil, fmt.Errorf("notify_human summary is required")
	}
	ctxJSON, err := json.Marshal(in.Context)
	if err != nil {
		return nil, fmt.Errorf("marshal mailbox context: %w", err)
	}
	if len(ctxJSON) == 0 || string(ctxJSON) == "null" {
		ctxJSON = []byte("{}")
	}
	eventID := ""
	if inbound, ok := runtimebus.InboundEventFromContext(ctx); ok {
		eventID = inbound.ID()
	}

	id, err := store.InsertMailboxItem(ctx, MailboxItem{
		EventID:        eventID,
		EntityID:       actor.EffectiveEntityID(),
		FlowInstance:   actor.CanonicalFlowPath(),
		FromAgent:      actor.ID,
		Type:           NotifyHumanMailboxItemType,
		Priority:       "normal",
		Status:         "pending",
		Context:        ctxJSON,
		Summary:        in.Summary,
		ReplyContextID: events.DeliveryContextFromContext(ctx).ReplyContextID(),
	})
	if err != nil {
		return nil, err
	}
	if e.noticePresentation != nil {
		unread, countErr := store.CountUnreadInformationalNotices(ctx)
		if countErr != nil {
			processWarn("tool-executor", "count committed informational notices: %v", countErr)
		} else {
			e.noticePresentation.PresentCommittedInformationalNotice(CommittedInformationalNotice{
				MailboxID:   id,
				UnreadCount: unread,
			})
		}
	}
	return map[string]any{"status": "queued", "mailbox_id": id}, nil
}
