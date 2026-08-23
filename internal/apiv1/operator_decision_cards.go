package apiv1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/mailbox"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

const decisionCardEventName = "mailbox.card_decided"

type MailboxNoticeAcknowledgmentStore interface {
	MarkMailboxItemNotified(context.Context, string) error
}

type mailboxProjectionListResult struct {
	Items                      []any  `json:"items"`
	NextCursor                 string `json:"next_cursor,omitempty"`
	UnreadInformationalNotices int    `json:"unread_informational_notices"`
}

type mailboxProjectionCursor struct {
	Notice string `json:"notice,omitempty"`
	Card   string `json:"card,omitempty"`
}

type mailboxProjectionEntry struct {
	Value     any
	Kind      string
	ID        string
	CreatedAt time.Time
}

func OperatorDecisionCardHandlers(opts DecisionCardHandlerOptions) map[string]MethodHandler {
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	handlers := map[string]MethodHandler{}
	if opts.Cards != nil && opts.ProposedEffects != nil && opts.Mailbox != nil {
		handlers["mailbox.list"] = func(ctx context.Context, req Request) (any, error) {
			return listMailboxProjection(ctx, req, opts)
		}
		handlers["mailbox.get"] = func(ctx context.Context, req Request) (any, error) {
			id := strings.TrimSpace(firstStringParam(req.Params, "mailbox_id", "card_id"))
			card, err := opts.Cards.GetDecisionCard(ctx, id)
			if err == nil {
				return decisionCardProjection(ctx, opts.ProposedEffects, card, card.Anchor.Kind())
			}
			if !errors.Is(err, decisioncard.ErrNotFound) {
				return nil, err
			}
			detail, err := opts.Mailbox.GetV1MailboxItem(ctx, id)
			if errors.Is(err, mailbox.ErrV1NotFound) {
				return nil, NewApplicationError(MailboxNotFoundCode, false, map[string]any{"mailbox_id": id})
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"kind": decisioncard.KindNotice, "notice": detail}, nil
		}
	}
	if opts.Cards != nil && opts.Authority != nil && opts.BundleSource != nil && opts.Idempotency != nil {
		for name, handler := range map[string]MethodHandler{
			"mailbox.decide": func(ctx context.Context, req Request) (any, error) {
				fields, err := optionalSemanticObject(req.SemanticParams, "fields")
				if err != nil {
					return nil, err
				}
				cardID := strings.TrimSpace(stringParam(req.Params, "card_id"))
				verdict := strings.TrimSpace(stringParam(req.Params, "verdict"))
				observedHash := strings.TrimSpace(stringParam(req.Params, "observed_content_hash"))
				if cardID == "" || verdict == "" || observedHash == "" {
					return nil, NewInvalidParamsError(map[string]any{"reason": "card_id, verdict, and observed_content_hash are required"})
				}
				eventID := uuid.NewString()
				return executeIdempotentDecisionCardMutation(ctx, req, opts, cardID, runtimepipeline.NewDecisionCardDecision(decisioncard.DecideRequest{
					CardID: cardID, Verdict: verdict, Fields: fields, ActorTokenID: req.ActorTokenID,
					ObservedContentHash: observedHash, DeliveryReceiptID: stringParam(req.Params, "delivery_receipt_id"),
					DeliveryRenderHash: stringParam(req.Params, "delivery_render_hash"), InputDraftID: stringParam(req.Params, "input_draft_id"),
					DecisionEventID: eventID, Now: now().UTC(),
				}))
			},
			"mailbox.defer": func(ctx context.Context, req Request) (any, error) {
				cardID := strings.TrimSpace(stringParam(req.Params, "card_id"))
				until, err := requiredTimestampParam(req.Params, "until")
				if err != nil {
					return nil, err
				}
				return executeIdempotentDecisionCardMutation(ctx, req, opts, cardID, runtimepipeline.NewDecisionCardDeferral(decisioncard.DeferRequest{
					CardID: cardID, ActorTokenID: req.ActorTokenID, Until: until, Now: now().UTC(),
				}))
			},
			"mailbox.begin_input": func(ctx context.Context, req Request) (any, error) {
				cardID := strings.TrimSpace(stringParam(req.Params, "card_id"))
				verdict := strings.TrimSpace(stringParam(req.Params, "verdict"))
				observedHash := strings.TrimSpace(stringParam(req.Params, "observed_content_hash"))
				return executeIdempotentDecisionCardMutation(ctx, req, opts, cardID, runtimepipeline.NewDecisionCardInputBegin(decisioncard.BeginInputRequest{
					CardID: cardID, Verdict: verdict, ActorTokenID: req.ActorTokenID,
					DeliveryReceiptID: stringParam(req.Params, "delivery_receipt_id"), Now: now().UTC(),
				}, observedHash))
			},
			"mailbox.cancel_input": func(ctx context.Context, req Request) (any, error) {
				cardID := strings.TrimSpace(stringParam(req.Params, "card_id"))
				return executeIdempotentDecisionCardMutation(ctx, req, opts, cardID, runtimepipeline.NewDecisionCardInputCancellation(decisioncard.CancelInputRequest{
					CardID: cardID, InputDraftID: stringParam(req.Params, "input_draft_id"), ActorTokenID: req.ActorTokenID, Now: now().UTC(),
				}))
			},
		} {
			handlers[name] = handler
		}
	}
	if opts.Cards != nil && opts.NoticeAcknowledgment != nil && opts.BundleSource != nil && opts.Idempotency != nil {
		handlers["mailbox.acknowledge"] = func(ctx context.Context, req Request) (any, error) {
			id := strings.TrimSpace(stringParam(req.Params, "mailbox_id"))
			return executeIdempotentMailboxNoticeAcknowledgment(ctx, req, opts, id, opts.NoticeAcknowledgment)
		}
	}
	return handlers
}

func listMailboxProjection(ctx context.Context, req Request, opts DecisionCardHandlerOptions) (any, error) {
	listOpts, err := mailboxListOptionsFromParams(req.Params)
	if err != nil {
		return nil, err
	}
	cursor, err := decodeMailboxProjectionCursor(listOpts.Cursor)
	if err != nil {
		return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid tagged mailbox cursor"})
	}
	limit := listOpts.Limit
	if limit <= 0 {
		limit = 50
	}
	noticeOpts := listOpts
	noticeOpts.Cursor = cursor.Notice
	noticeOpts.Limit = limit
	var notices []mailbox.V1Item
	var noticeNext string
	if listOpts.AnchorKind == "" {
		notices, noticeNext, err = opts.Mailbox.ListV1MailboxItems(ctx, noticeOpts)
		if err != nil {
			if errors.Is(err, mailbox.ErrV1InvalidCursor) {
				return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid notice cursor"})
			}
			return nil, err
		}
	}
	cards, cardNext, err := opts.Cards.ListDecisionCards(ctx, decisioncard.ListOptions{
		Status: listOpts.Status, RunID: listOpts.RunID, EntityID: listOpts.EntityID, AnchorKind: listOpts.AnchorKind, Limit: limit, Cursor: cursor.Card,
	})
	if errors.Is(err, decisioncard.ErrInvalidCursor) {
		return nil, NewInvalidParamsError(map[string]any{"field": "cursor", "reason": "invalid decision-card cursor"})
	}
	if err != nil {
		return nil, err
	}
	entries := make([]mailboxProjectionEntry, 0, len(notices)+len(cards))
	for _, notice := range notices {
		createdAt, err := time.Parse(time.RFC3339Nano, notice.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("decode notice creation time: %w", err)
		}
		entries = append(entries, mailboxProjectionEntry{Value: map[string]any{"kind": decisioncard.KindNotice, "notice": notice}, Kind: decisioncard.KindNotice, ID: notice.MailboxID, CreatedAt: createdAt})
	}
	for _, card := range cards {
		projection, err := decisionCardProjection(ctx, opts.ProposedEffects, card, card.Anchor.Kind())
		if err != nil {
			return nil, err
		}
		entries = append(entries, mailboxProjectionEntry{Value: projection, Kind: decisioncard.KindDecisionCard, ID: card.CardID, CreatedAt: card.CreatedAt})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].CreatedAt.Before(entries[j].CreatedAt)
		}
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind < entries[j].Kind
		}
		return entries[i].ID < entries[j].ID
	})
	selected := entries
	if len(selected) > limit {
		selected = selected[:limit]
	}
	items := make([]any, 0, len(selected))
	nextState := cursor
	for _, entry := range selected {
		items = append(items, entry.Value)
		ownerCursor := mailbox.EncodeV1Cursor(entry.CreatedAt, entry.ID)
		if entry.Kind == decisioncard.KindNotice {
			nextState.Notice = ownerCursor
		} else {
			nextState.Card = ownerCursor
		}
	}
	next := ""
	if len(entries) > limit || noticeNext != "" || cardNext != "" {
		next = encodeMailboxProjectionCursor(nextState)
	}
	unread, err := opts.Mailbox.CountUnreadInformationalNotices(ctx)
	if err != nil {
		return nil, err
	}
	return mailboxProjectionListResult{Items: items, NextCursor: next, UnreadInformationalNotices: unread}, nil
}

func decisionCardProjection(ctx context.Context, proposedEffects decisioncard.ProposedEffectStore, card any, kind decisioncard.AnchorKind) (map[string]any, error) {
	out := map[string]any{"kind": decisioncard.KindDecisionCard, "decision_card": card}
	if kind != decisioncard.AnchorKindProposedEffect {
		return out, nil
	}
	if proposedEffects == nil {
		return nil, fmt.Errorf("proposed-effect readback store is not configured")
	}
	var cardID string
	switch typed := card.(type) {
	case decisioncard.Card:
		cardID = typed.CardID
	case decisioncard.ListItem:
		cardID = typed.CardID
	default:
		return nil, fmt.Errorf("unsupported decision-card projection %T", card)
	}
	readback, err := proposedEffects.ProposedEffectReadback(ctx, cardID)
	if err != nil {
		return nil, err
	}
	out["effect"] = readback
	return out, nil
}

func decodeMailboxProjectionCursor(raw string) (mailboxProjectionCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return mailboxProjectionCursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return mailboxProjectionCursor{}, err
	}
	var cursor mailboxProjectionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return mailboxProjectionCursor{}, err
	}
	if strings.TrimSpace(cursor.Notice) == "" && strings.TrimSpace(cursor.Card) == "" {
		return mailboxProjectionCursor{}, fmt.Errorf("empty tagged mailbox cursor")
	}
	return cursor, nil
}

func encodeMailboxProjectionCursor(cursor mailboxProjectionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func executeIdempotentDecisionCardMutation(ctx context.Context, req Request, opts DecisionCardHandlerOptions, cardID string, mutation runtimepipeline.DecisionCardMutation) (any, error) {
	if opts.Idempotency == nil {
		return nil, fmt.Errorf("decision card mutation idempotency owner is required")
	}
	card, err := opts.Cards.GetDecisionCard(ctx, cardID)
	if err != nil && !errors.Is(err, decisioncard.ErrNotFound) {
		return nil, decisionCardAPIError(cardID, err)
	}
	authority := opts.Authority
	bundleSource := opts.BundleSource
	if err == nil && runtimeContextManager(opts.RuntimeContexts) != nil {
		var selected selectedRuntimeContext
		ctx, selected, err = runtimeBundleContextByHash(ctx, opts.RuntimeContexts, card.BundleHash, card.RunID)
		if err != nil {
			return nil, err
		}
		if selected.Runtime == nil || selected.Runtime.Pipeline == nil || selected.Runtime.Bus == nil {
			return nil, fmt.Errorf("decision card owners are required for the selected runtime")
		}
		authority = selected.Runtime.Pipeline
		bundleSource = selected.Runtime.Bus
	}
	if authority == nil {
		return nil, fmt.Errorf("decision card workflow mutation owner is required for the selected runtime")
	}
	if bundleSource == nil {
		return nil, fmt.Errorf("decision card bundle source owner is required for the selected runtime")
	}
	ctx, err = bundleSource.AdmitBundleSourceFact(ctx)
	if err != nil {
		return nil, err
	}
	idempotencyKey := strings.TrimSpace(stringParam(req.Params, "idempotency_key"))
	result := semanticvalue.EmptyObject()
	var replayed bool
	now := time.Now().UTC()
	if opts.Now != nil {
		now = opts.Now().UTC()
	}
	response, replayed, err := authority.CommitDecisionCardMutation(ctx, decisionCardMutationIdempotencyAdapter{owner: opts.Idempotency}, runtimepipeline.DecisionCardMutationIdempotencyRequest{
		Method: req.Method, ActorTokenID: req.ActorTokenID, IdempotencyKey: idempotencyKey,
		RequestHash: req.RequestHash, ResourceID: cardID, TTL: 24 * time.Hour, Now: now,
	}, mutation)
	if err != nil {
		return nil, decisionCardAPIError(cardID, err)
	}
	result, err = canonicaljson.Decode(response)
	if err != nil {
		return nil, decisionCardAPIError(cardID, err)
	}
	result, err = result.With("idempotency_replayed", semanticvalue.Bool(replayed))
	if err != nil {
		return nil, decisionCardAPIError(cardID, fmt.Errorf("decision card mutation result is not an object: %w", err))
	}
	return result, nil
}

type decisionCardMutationIdempotencyAdapter struct {
	owner APIIdempotencyStore
}

func (a decisionCardMutationIdempotencyAdapter) WithDecisionCardMutationIdempotency(
	ctx context.Context,
	req runtimepipeline.DecisionCardMutationIdempotencyRequest,
	execute func(context.Context) (runtimepipeline.DecisionCardMutationIdempotencyCompletion, error),
) (runtimepipeline.DecisionCardMutationIdempotencyCompletion, bool, error) {
	completion, replayed, err := a.owner.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method: req.Method, ActorTokenID: req.ActorTokenID, IdempotencyKey: req.IdempotencyKey,
		RequestHash: req.RequestHash, ResourceID: req.ResourceID, TTL: req.TTL, Now: req.Now,
	}, func(callbackCtx context.Context) (apiidempotency.Completion, error) {
		result, err := execute(callbackCtx)
		return apiidempotency.Completion{ResourceID: result.ResourceID, Response: result.Response}, err
	})
	return runtimepipeline.DecisionCardMutationIdempotencyCompletion{
		ResourceID: completion.ResourceID, Response: completion.Response,
	}, replayed, err
}

func executeIdempotentMailboxNoticeAcknowledgment(
	ctx context.Context,
	req Request,
	opts DecisionCardHandlerOptions,
	mailboxID string,
	writer MailboxNoticeAcknowledgmentStore,
) (any, error) {
	if opts.Idempotency == nil {
		return nil, fmt.Errorf("mailbox notice idempotency owner is required")
	}
	if opts.BundleSource == nil {
		return nil, fmt.Errorf("mailbox notice bundle source owner is required")
	}
	var err error
	ctx, err = opts.BundleSource.AdmitBundleSourceFact(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if opts.Now != nil {
		now = opts.Now().UTC()
	}
	completion, replayed, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method: req.Method, ActorTokenID: req.ActorTokenID,
		IdempotencyKey: strings.TrimSpace(stringParam(req.Params, "idempotency_key")),
		RequestHash:    req.RequestHash, ResourceID: mailboxID, TTL: 24 * time.Hour, Now: now,
	}, func(callbackCtx context.Context) (apiidempotency.Completion, error) {
		if _, err := opts.Cards.GetDecisionCard(callbackCtx, mailboxID); err == nil {
			return apiidempotency.Completion{}, NewInvalidParamsError(map[string]any{"mailbox_id": mailboxID, "reason": "decision cards use decide or defer; acknowledge is notice-only"})
		} else if !errors.Is(err, decisioncard.ErrNotFound) {
			return apiidempotency.Completion{}, err
		}
		if err := writer.MarkMailboxItemNotified(callbackCtx, mailboxID); err != nil {
			if errors.Is(err, mailbox.ErrV1NotFound) {
				return apiidempotency.Completion{}, decisioncard.ErrNotFound
			}
			return apiidempotency.Completion{}, err
		}
		raw, err := canonicaljson.Bytes(map[string]any{"ok": true, "mailbox_id": mailboxID, "kind": decisioncard.KindNotice})
		return apiidempotency.Completion{ResourceID: mailboxID, Response: raw}, err
	})
	if err != nil {
		return nil, decisionCardAPIError(mailboxID, err)
	}
	result, err := canonicaljson.Decode(completion.Response)
	if err != nil {
		return nil, decisionCardAPIError(mailboxID, err)
	}
	result, err = result.With("idempotency_replayed", semanticvalue.Bool(replayed))
	if err != nil {
		return nil, decisionCardAPIError(mailboxID, err)
	}
	return result, nil
}

func decisionCardAPIError(cardID string, err error) error {
	switch {
	case errors.Is(err, decisioncard.ErrNotFound):
		return NewApplicationError(MailboxNotFoundCode, false, map[string]any{"card_id": cardID})
	case errors.Is(err, decisioncard.ErrAlreadyTerminal):
		return NewApplicationError(MailboxAlreadyDecidedCode, false, map[string]any{"card_id": cardID})
	case errors.Is(err, decisioncard.ErrStaleContent):
		return NewApplicationError("MAILBOX_STALE_CARD", false, map[string]any{"card_id": cardID, "remediation": "refresh mailbox.get and retry against the current content hash"})
	case errors.Is(err, decisioncard.ErrInvalidVerdict), errors.Is(err, decisioncard.ErrInvalidFields):
		return NewApplicationError("MAILBOX_INVALID_VERDICT", false, map[string]any{"card_id": cardID, "reason": err.Error()})
	case errors.Is(err, decisioncard.ErrDraftNotFound), errors.Is(err, decisioncard.ErrDraftNotAuthority):
		return NewApplicationError("MAILBOX_INPUT_DRAFT_NOT_AUTHORITY", false, map[string]any{"card_id": cardID})
	case errors.Is(err, decisioncard.ErrInvalidDeferUntil):
		return NewApplicationError(InvalidDeferUntilCode, false, map[string]any{"reason": err.Error()})
	}
	var conflict *apiidempotency.ConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{"original_request_hash": conflict.OriginalRequestHash, "conflicting_request_hash": conflict.ConflictingRequestHash})
	}
	if strings.Contains(err.Error(), "superseded") || strings.Contains(err.Error(), "no longer current") {
		return NewApplicationError("MAILBOX_CARD_SUPERSEDED", false, map[string]any{"card_id": cardID})
	}
	return err
}

func optionalSemanticObject(params semanticvalue.Value, name string) (semanticvalue.Value, error) {
	if params.Kind() != semanticvalue.KindObject {
		return semanticvalue.Value{}, NewInvalidParamsError(map[string]any{"field": name, "reason": "params must be an object"})
	}
	value, ok := params.Lookup(name)
	if !ok || value.Kind() == semanticvalue.KindNull {
		return semanticvalue.EmptyObject(), nil
	}
	if value.Kind() != semanticvalue.KindObject {
		return semanticvalue.Value{}, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be an object"})
	}
	return value, nil
}

func firstStringParam(params map[string]any, names ...string) string {
	for _, name := range names {
		if value := stringParam(params, name); strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
