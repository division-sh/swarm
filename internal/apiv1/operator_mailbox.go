package apiv1

import (
	"context"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/mailbox"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type MailboxAPIStore interface {
	ListV1MailboxItems(context.Context, mailbox.V1ListOptions) ([]mailbox.V1Item, string, error)
	GetV1MailboxItem(context.Context, string) (mailbox.V1ItemDetail, error)
	CountUnreadInformationalNotices(context.Context) (int, error)
}

type APIIdempotencyStore interface {
	WithAPIIdempotency(
		context.Context,
		apiidempotency.Request,
		func(context.Context) (apiidempotency.Completion, error),
	) (apiidempotency.Completion, bool, error)
}

type EventPublisher interface {
	Publish(context.Context, events.Event) error
}

func mailboxListOptionsFromParams(params map[string]any) (mailbox.V1ListOptions, error) {
	out := mailbox.V1ListOptions{}
	var err error
	if out.Status, _, err = optionalStringParam(params, "status"); err != nil {
		return out, err
	}
	out.Status = strings.TrimSpace(strings.ToLower(out.Status))
	if out.Status != "" && out.Status != "pending" && out.Status != "decided" && out.Status != decisioncard.StatusSuperseded && out.Status != "expired" && out.Status != "deferred" {
		return out, NewInvalidParamsError(map[string]any{"field": "status", "reason": "must be a valid MailboxStatus"})
	}
	if out.RunID, _, err = optionalStringParam(params, "run_id"); err != nil {
		return out, err
	}
	if out.EntityID, _, err = optionalStringParam(params, "entity_id"); err != nil {
		return out, err
	}
	if out.Type, _, err = optionalStringParam(params, "type"); err != nil {
		return out, err
	}
	if out.Priority, _, err = optionalStringParam(params, "priority"); err != nil {
		return out, err
	}
	if out.AnchorKind, _, err = optionalStringParam(params, "anchor_kind"); err != nil {
		return out, err
	}
	out.AnchorKind = strings.TrimSpace(out.AnchorKind)
	if out.AnchorKind != "" && !decisioncard.IsRegisteredAnchorKind(out.AnchorKind) {
		return out, NewInvalidParamsError(map[string]any{"field": "anchor_kind", "reason": "must be a registered decision-card anchor kind"})
	}
	out.Priority = strings.TrimSpace(strings.ToLower(out.Priority))
	if out.Priority != "" && out.Priority != "normal" && out.Priority != "high" && out.Priority != "critical" {
		return out, NewInvalidParamsError(map[string]any{"field": "priority", "reason": "must be a valid MailboxPriority"})
	}
	if out.Cursor, _, err = optionalStringParam(params, "cursor"); err != nil {
		return out, err
	}
	if raw, ok := params["limit"]; ok && !isEmptyParam(raw) {
		limit, ok := integerParam(raw)
		if !ok || limit < 1 || limit > 200 {
			return out, NewInvalidParamsError(map[string]any{"field": "limit", "reason": "must be an integer from 1 to 200"})
		}
		out.Limit = limit
	}
	return out, nil
}

func requiredTimestampParam(params map[string]any, name string) (time.Time, error) {
	value, present, err := optionalStringParam(params, name)
	if err != nil {
		return time.Time{}, err
	}
	if !present || strings.TrimSpace(value) == "" {
		return time.Time{}, NewInvalidParamsError(map[string]any{"field": name, "reason": "required parameter is missing"})
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be RFC3339 timestamp"})
	}
	return parsed.UTC(), nil
}
