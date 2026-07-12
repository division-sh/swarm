package apiv1

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store"
)

type MailboxAPIStore interface {
	ListV1MailboxItems(context.Context, store.MailboxV1ListOptions) ([]store.MailboxV1Item, string, error)
	GetV1MailboxItem(context.Context, string) (store.MailboxV1ItemDetail, error)
}

type APIIdempotencyStore interface {
	WithAPIIdempotency(
		context.Context,
		store.APIIdempotencyRequest,
		func(context.Context) (store.APIIdempotencyCompletion, error),
	) (store.APIIdempotencyCompletion, bool, error)
}

type EventPublisher interface {
	Publish(context.Context, events.Event) error
}

type EventMutationPublisher interface {
	PublishInMutation(context.Context, events.Event) error
}

type mailboxListResult struct {
	Items      []store.MailboxV1Item `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

func OperatorMailboxHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.Mailbox == nil {
		return nil
	}
	handlers := map[string]MethodHandler{
		"mailbox.list": func(ctx context.Context, req Request) (any, error) {
			listOpts, err := mailboxListOptionsFromParams(req.Params)
			if err != nil {
				return nil, err
			}
			items, nextCursor, err := opts.Mailbox.ListV1MailboxItems(ctx, listOpts)
			if errors.Is(err, store.ErrMailboxV1InvalidCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid mailbox cursor"})
			}
			if err != nil {
				return nil, err
			}
			if items == nil {
				items = []store.MailboxV1Item{}
			}
			return mailboxListResult{Items: items, NextCursor: nextCursor}, nil
		},
		"mailbox.get": func(ctx context.Context, req Request) (any, error) {
			detail, err := opts.Mailbox.GetV1MailboxItem(ctx, stringParam(req.Params, "mailbox_id"))
			if errors.Is(err, store.ErrMailboxV1NotFound) {
				return nil, NewApplicationError(MailboxNotFoundCode, false, map[string]any{"mailbox_id": stringParam(req.Params, "mailbox_id")})
			}
			if err != nil {
				return nil, err
			}
			return detail, nil
		},
	}
	for method, handler := range decisionCardHandlers(opts) {
		handlers[method] = handler
	}
	return handlers
}

func mailboxListOptionsFromParams(params map[string]any) (store.MailboxV1ListOptions, error) {
	out := store.MailboxV1ListOptions{}
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
