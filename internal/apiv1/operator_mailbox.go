package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"swarm/internal/events"
	"swarm/internal/store"
)

type MailboxAPIStore interface {
	ListV1MailboxItems(context.Context, store.MailboxV1ListOptions) ([]store.MailboxV1Item, string, error)
	GetV1MailboxItem(context.Context, string) (store.MailboxV1ItemDetail, error)
	DecideV1MailboxItem(context.Context, store.MailboxV1DecisionRequest) (store.MailboxV1DecisionOutcome, error)
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

type TransactionalEventPublisher interface {
	PublishTx(context.Context, *sql.Tx, events.Event) error
}

type mailboxListResult struct {
	Items      []store.MailboxV1Item `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

func OperatorMailboxHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.Mailbox == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
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
			detail.DecisionSheet = mailboxDecisionSheet(ctx, detail.Item, opts)
			return detail, nil
		},
		"mailbox.approve": func(ctx context.Context, req Request) (any, error) {
			payload, err := optionalObjectRaw(req.Params, "decision_payload")
			if err != nil {
				return nil, err
			}
			eventName, subscribers, subscriberSource := mailboxApprovalEventEffect(opts.MailboxApprovalRoutes, stringParam(req.Params, "mailbox_id"), opts.Mailbox, ctx, opts)
			return executeMailboxDecision(ctx, req, opts, store.MailboxV1DecisionRequest{
				MailboxID:                     stringParam(req.Params, "mailbox_id"),
				Action:                        "approved",
				ActorTokenID:                  req.ActorTokenID,
				DecisionPayload:               payload,
				Now:                           now().UTC(),
				ApprovalEventType:             eventName,
				ApprovalEventSubscribers:      subscribers,
				ApprovalEventSubscriberSource: subscriberSource,
			})
		},
		"mailbox.reject": func(ctx context.Context, req Request) (any, error) {
			return executeMailboxDecision(ctx, req, opts, store.MailboxV1DecisionRequest{
				MailboxID:    stringParam(req.Params, "mailbox_id"),
				Action:       "rejected",
				ActorTokenID: req.ActorTokenID,
				Reason:       stringParam(req.Params, "reason"),
				Now:          now().UTC(),
			})
		},
		"mailbox.defer": func(ctx context.Context, req Request) (any, error) {
			until, err := requiredTimestampParam(req.Params, "until")
			if err != nil {
				return nil, err
			}
			return executeMailboxDecision(ctx, req, opts, store.MailboxV1DecisionRequest{
				MailboxID:    stringParam(req.Params, "mailbox_id"),
				Action:       "deferred",
				ActorTokenID: req.ActorTokenID,
				DeferUntil:   until,
				Now:          now().UTC(),
			})
		},
	}
}

func mailboxDecisionSheet(ctx context.Context, item store.MailboxV1Item, opts OperatorReadOptions) *store.MailboxV1DecisionSheet {
	entityContext := mailboxEntityContext(ctx, item, opts.Entities)
	return &store.MailboxV1DecisionSheet{
		EntityContext:     entityContext,
		DownstreamPreview: mailboxDownstreamPreview(item, opts),
	}
}

func mailboxEntityContext(ctx context.Context, item store.MailboxV1Item, entities EntityReadStore) store.MailboxV1EntityContext {
	entityID := strings.TrimSpace(item.SourceEntityID)
	if entityID == "" {
		return store.MailboxV1EntityContext{Available: false, Reason: "no_source_entity"}
	}
	if entities == nil {
		return store.MailboxV1EntityContext{Available: false, Reason: "entity_reader_unavailable"}
	}
	entity, err := entities.LoadOperatorEntity(ctx, entityID, strings.TrimSpace(item.SourceRunID))
	if err == nil {
		return store.MailboxV1EntityContext{Available: true, Entity: &entity}
	}
	switch {
	case errors.Is(err, store.ErrEntityNotFound):
		return store.MailboxV1EntityContext{Available: false, Reason: "entity_not_found"}
	case errors.Is(err, store.ErrAmbiguousEntityRunID):
		return store.MailboxV1EntityContext{Available: false, Reason: "ambiguous_run_id"}
	}
	if paramErr := entityReadParamError(err); paramErr != nil {
		return store.MailboxV1EntityContext{Available: false, Reason: "invalid_entity_ref"}
	}
	return store.MailboxV1EntityContext{Available: false, Reason: "entity_reader_unavailable"}
}

func mailboxDownstreamPreview(item store.MailboxV1Item, opts OperatorReadOptions) store.MailboxV1DownstreamPreview {
	eventName := strings.TrimSpace(opts.MailboxApprovalRoutes[strings.TrimSpace(item.Type)])
	if eventName == "" {
		return store.MailboxV1DownstreamPreview{
			Available:        false,
			Reason:           "no_approval_route",
			Subscribers:      []string{},
			SubscriberSource: "none",
		}
	}
	subscribers, subscriberSource := mailboxDownstreamSubscribers(eventName, opts)
	return store.MailboxV1DownstreamPreview{
		Available:        true,
		EventName:        eventName,
		Subscribers:      subscribers,
		SubscriberSource: subscriberSource,
	}
}

func mailboxDownstreamSubscribers(eventName string, opts OperatorReadOptions) ([]string, string) {
	if opts.Source == nil {
		return []string{}, "unavailable"
	}
	entry, ok := opts.Source.EventEntry(eventName)
	if !ok {
		return []string{}, "unavailable"
	}
	subscribers := append([]string(nil), entry.SwarmConsumer()...)
	subscribers = append(subscribers, entry.Consumer...)
	subscribers = uniqueNonEmptyStrings(subscribers)
	sort.Strings(subscribers)
	return subscribers, "event_catalog"
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func executeMailboxDecision(ctx context.Context, req Request, opts OperatorReadOptions, decision store.MailboxV1DecisionRequest) (any, error) {
	idempotencyKey := stringParam(req.Params, "idempotency_key")
	action := strings.ToLower(strings.TrimSpace(decision.Action))
	if action == "approved" || action == "approve" {
		if multiRuntimeContextMode(opts) {
			return nil, runtimeContextRequiredError(req.Method, "mailbox approval event publishing is ambiguous in multi-context DB-loaded mode; per-context mailbox approval routing is split to #1176")
		}
		txPublisher, ok := opts.Events.(TransactionalEventPublisher)
		if !ok || txPublisher == nil {
			return nil, errors.New("transactional event publisher is required for mailbox approval")
		}
		decision.ApprovalEventTx = txPublisher.PublishTx
	}
	if strings.TrimSpace(idempotencyKey) != "" {
		decision.Idempotency = &store.APIIdempotencyRequest{
			Method:         req.Method,
			ActorTokenID:   req.ActorTokenID,
			IdempotencyKey: idempotencyKey,
			RequestHash:    req.RequestHash,
			ResourceID:     decision.MailboxID,
			TTL:            24 * time.Hour,
			Now:            decision.Now,
		}
	}
	outcome, err := opts.Mailbox.DecideV1MailboxItem(ctx, decision)
	if err != nil {
		var conflict *store.APIIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, NewApplicationError(IdempotencyConflictCode, false, map[string]any{
				"original_request_hash":    conflict.OriginalRequestHash,
				"conflicting_request_hash": conflict.ConflictingRequestHash,
				"original_response_ref": map[string]any{
					"method":      conflict.Method,
					"resource_id": conflict.ResourceID,
				},
			})
		}
		return nil, mailboxDecisionError(decision.MailboxID, err)
	}
	outcome.Result.IdempotencyReplayed = outcome.Replayed
	return outcome.Result, nil
}

func mailboxDecisionError(mailboxID string, err error) error {
	if errors.Is(err, store.ErrMailboxV1NotFound) {
		return NewApplicationError(MailboxNotFoundCode, false, map[string]any{"mailbox_id": mailboxID})
	}
	var already *store.MailboxV1AlreadyDecidedError
	if errors.As(err, &already) {
		return NewApplicationError(MailboxAlreadyDecidedCode, false, map[string]any{
			"mailbox_id":        already.MailboxID,
			"existing_decision": already.ExistingDecision,
			"decided_at":        already.DecidedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	var invalidDefer *store.MailboxV1InvalidDeferUntilError
	if errors.As(err, &invalidDefer) {
		details := map[string]any{"reason": invalidDefer.Reason}
		if invalidDefer.MaxUntil != nil {
			details["max_until"] = invalidDefer.MaxUntil.UTC().Format(time.RFC3339Nano)
		}
		return NewApplicationError(InvalidDeferUntilCode, false, details)
	}
	var route *store.MailboxV1ApprovalRouteError
	if errors.As(err, &route) {
		return NewApplicationError(MailboxApprovalEventUnconfiguredCode, false, map[string]any{
			"mailbox_id": route.MailboxID,
			"item_type":  route.ItemType,
		})
	}
	return err
}

func mailboxListOptionsFromParams(params map[string]any) (store.MailboxV1ListOptions, error) {
	out := store.MailboxV1ListOptions{}
	var err error
	if out.Status, _, err = optionalStringParam(params, "status"); err != nil {
		return out, err
	}
	out.Status = strings.TrimSpace(strings.ToLower(out.Status))
	if out.Status != "" && out.Status != "pending" && out.Status != "decided" && out.Status != "expired" && out.Status != "deferred" {
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

func optionalObjectRaw(params map[string]any, name string) (json.RawMessage, error) {
	if params == nil {
		return nil, nil
	}
	value, ok := params[name]
	if !ok || isEmptyParam(value) {
		return nil, nil
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be an object"})
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return raw, nil
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

func mailboxApprovalEventEffect(routes map[string]string, mailboxID string, mailbox MailboxAPIStore, ctx context.Context, opts OperatorReadOptions) (string, []string, string) {
	if len(routes) == 0 || mailbox == nil {
		return "", nil, ""
	}
	detail, err := mailbox.GetV1MailboxItem(ctx, mailboxID)
	if err != nil {
		return "", nil, ""
	}
	eventName := strings.TrimSpace(routes[detail.Item.Type])
	if eventName == "" {
		return "", nil, ""
	}
	subscribers, subscriberSource := mailboxDownstreamSubscribers(eventName, opts)
	return eventName, subscribers, subscriberSource
}
