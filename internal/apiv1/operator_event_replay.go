package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	"swarm/internal/store"
)

const (
	eventReplayIdempotencyTTL      = 24 * time.Hour
	eventReplayScopeSubscriberID   = "__runtime_replay_scope__"
	eventReplaySyntheticEventName  = "event.replayed"
	eventReplayDefaultActorSource  = "swarm-cli:anonymous"
	eventReplaySubscriberTypeAgent = "agent"
	eventReplayStatusDelivered     = "delivered"
	eventReplayStatusFailed        = "failed"
	eventReplayStatusDeadLetter    = "dead_letter"
	eventReplayStatusPending       = "pending"
	eventReplayStatusInProgress    = "in_progress"
)

type eventReplayPublisher interface {
	EventPublisher
	PublishDirect(context.Context, events.Event, []string) error
	CheckDirectRecipients(context.Context, events.Event, []string) (runtimebus.DirectRecipientStatus, error)
}

type eventReplayResult struct {
	EventID             string                `json:"event_id"`
	ReplayEventID       string                `json:"replay_event_id"`
	AuditEventID        string                `json:"audit_event_id"`
	SubscribersReplayed []string              `json:"subscribers_replayed"`
	OriginalDeliveries  []eventReplayDelivery `json:"original_deliveries"`
	NewDeliveries       []eventReplayDelivery `json:"new_deliveries"`
}

type eventReplayDelivery struct {
	DeliveryID       string `json:"delivery_id"`
	SubscriberID     string `json:"subscriber_id"`
	SessionID        string `json:"session_id,omitempty"`
	Status           string `json:"status"`
	Attempt          int    `json:"attempt"`
	SourceDeliveryID string `json:"source_delivery_id,omitempty"`
}

type eventReplayStoredResult struct {
	EventID             string   `json:"event_id"`
	ReplayEventID       string   `json:"replay_event_id"`
	AuditEventID        string   `json:"audit_event_id"`
	SubscribersReplayed []string `json:"subscribers_replayed"`
}

func OperatorEventReplayHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if !eventReplayConfigured(opts) {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"event.replay": func(ctx context.Context, req Request) (any, error) {
			return executeEventReplay(ctx, req, opts, now().UTC())
		},
	}
}

func eventReplayConfigured(opts OperatorReadOptions) bool {
	if opts.Observability == nil || opts.Idempotency == nil || opts.Events == nil {
		return false
	}
	_, ok := opts.Events.(eventReplayPublisher)
	return ok
}

func executeEventReplay(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	eventID, err := requiredStringParam(req.Params, "event_id")
	if err != nil {
		return nil, err
	}
	requestedSubscribers, _, err := optionalStringListParam(req.Params, "subscribers")
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	publisher, ok := opts.Events.(eventReplayPublisher)
	if !ok || publisher == nil {
		return nil, errors.New("event replay publisher is required")
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     eventID,
		TTL:            eventReplayIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		stored, err := performEventReplay(ctx, req, opts, publisher, eventID, requestedSubscribers, now)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		response, err := json.Marshal(stored)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: stored.ReplayEventID,
			Response:   response,
		}, nil
	})
	if err != nil {
		return nil, eventReplayError(err)
	}
	var stored eventReplayStoredResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return nil, fmt.Errorf("decode event.replay idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode event.replay response: %w", err)
	}
	result, err := eventReplayResultFromStore(ctx, opts, stored)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func performEventReplay(
	ctx context.Context,
	req Request,
	opts OperatorReadOptions,
	publisher eventReplayPublisher,
	eventID string,
	requestedSubscribers []string,
	now time.Time,
) (eventReplayStoredResult, error) {
	original, err := opts.Observability.LoadOperatorEvent(ctx, eventID)
	if errors.Is(err, store.ErrEventNotFound) {
		return eventReplayStoredResult{}, NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": eventID})
	}
	if err != nil {
		return eventReplayStoredResult{}, err
	}
	originalDeliveries, selectedSubscribers, err := eventReplayTargets(original, requestedSubscribers)
	if err != nil {
		return eventReplayStoredResult{}, err
	}
	replayEventID := uuid.NewString()
	replayEvent, err := replayEventFromOriginal(original, replayEventID, now)
	if err != nil {
		return eventReplayStoredResult{}, err
	}
	status, err := publisher.CheckDirectRecipients(ctx, replayEvent, selectedSubscribers)
	if err != nil {
		return eventReplayStoredResult{}, eventReplayPublishError(original.EventName, err)
	}
	if len(status.Missing) > 0 {
		return eventReplayStoredResult{}, NewApplicationError(EventReplaySubscriberUnavailableCode, true, map[string]any{
			"event_id":     original.EventID,
			"subscribers":  status.Missing,
			"requested":    status.Requested,
			"deliverable":  status.Recipients,
			"replay_event": replayEventID,
		})
	}
	if err := publisher.PublishDirect(ctx, replayEvent, selectedSubscribers); err != nil {
		return eventReplayStoredResult{}, eventReplayPublishError(original.EventName, err)
	}
	auditEventID := uuid.NewString()
	auditPayload, err := eventReplayAuditPayload(req, original, replayEventID, auditEventID, selectedSubscribers, originalDeliveries)
	if err != nil {
		return eventReplayStoredResult{}, err
	}
	if err := publisher.Publish(ctx, events.Event{
		ID:            auditEventID,
		RunID:         original.RunID,
		Type:          events.EventType(eventReplaySyntheticEventName),
		SourceAgent:   eventReplayActorSource(req),
		Payload:       auditPayload,
		ParentEventID: original.EventID,
		CreatedAt:     now,
	}.WithEntityID(original.EntityID)); err != nil {
		return eventReplayStoredResult{}, eventReplayPublishError(eventReplaySyntheticEventName, err)
	}
	return eventReplayStoredResult{
		EventID:             original.EventID,
		ReplayEventID:       replayEventID,
		AuditEventID:        auditEventID,
		SubscribersReplayed: selectedSubscribers,
	}, nil
}

func eventReplayTargets(original store.OperatorEventFull, requested []string) ([]eventReplayDelivery, []string, error) {
	originalBySubscriber := map[string]store.OperatorEventDelivery{}
	orderedSubscribers := []string{}
	for _, delivery := range original.Deliveries {
		subscriberType := strings.TrimSpace(delivery.SubscriberType)
		subscriberID := strings.TrimSpace(delivery.SubscriberID)
		if subscriberType != eventReplaySubscriberTypeAgent || subscriberID == "" || subscriberID == eventReplayScopeSubscriberID {
			continue
		}
		if _, seen := originalBySubscriber[subscriberID]; seen {
			continue
		}
		originalBySubscriber[subscriberID] = delivery
		orderedSubscribers = append(orderedSubscribers, subscriberID)
	}
	if len(orderedSubscribers) == 0 {
		return nil, nil, NewApplicationError(EventReplayNoDeliveryHistoryCode, false, map[string]any{"event_id": original.EventID})
	}
	requested = uniqueTrimmedStrings(requested)
	if len(requested) == 0 {
		deliveries, err := deliveriesForSubscribers(original.EventID, originalBySubscriber, orderedSubscribers)
		return deliveries, orderedSubscribers, err
	}
	selected := make([]string, 0, len(requested))
	for _, subscriber := range requested {
		if _, ok := originalBySubscriber[subscriber]; !ok {
			return nil, nil, NewApplicationError(EventReplaySubscriberNotOriginalCode, false, map[string]any{
				"event_id":              original.EventID,
				"subscriber_id":         subscriber,
				"original_subscribers":  orderedSubscribers,
				"requested_subscribers": requested,
			})
		}
		selected = append(selected, subscriber)
	}
	deliveries, err := deliveriesForSubscribers(original.EventID, originalBySubscriber, selected)
	return deliveries, selected, err
}

func validateReplayEligibleDelivery(eventID string, delivery store.OperatorEventDelivery) error {
	switch strings.TrimSpace(delivery.Status) {
	case eventReplayStatusDelivered, eventReplayStatusFailed, eventReplayStatusDeadLetter:
		return nil
	case eventReplayStatusPending, eventReplayStatusInProgress, "":
		return NewApplicationError(EventReplayNotEligibleCode, false, map[string]any{
			"event_id":      eventID,
			"delivery_id":   strings.TrimSpace(delivery.DeliveryID),
			"subscriber_id": strings.TrimSpace(delivery.SubscriberID),
			"status":        strings.TrimSpace(delivery.Status),
			"reason":        "original delivery is not terminal",
		})
	default:
		return NewApplicationError(EventReplayNotEligibleCode, false, map[string]any{
			"event_id":      eventID,
			"delivery_id":   strings.TrimSpace(delivery.DeliveryID),
			"subscriber_id": strings.TrimSpace(delivery.SubscriberID),
			"status":        strings.TrimSpace(delivery.Status),
			"reason":        "unsupported delivery status",
		})
	}
}

func deliveriesForSubscribers(eventID string, index map[string]store.OperatorEventDelivery, subscribers []string) ([]eventReplayDelivery, error) {
	out := make([]eventReplayDelivery, 0, len(subscribers))
	for _, subscriber := range subscribers {
		if delivery, ok := index[subscriber]; ok {
			if err := validateReplayEligibleDelivery(eventID, delivery); err != nil {
				return nil, err
			}
			out = append(out, eventReplayDelivery{
				DeliveryID:   strings.TrimSpace(delivery.DeliveryID),
				SubscriberID: subscriber,
				SessionID:    strings.TrimSpace(delivery.SessionID),
				Status:       strings.TrimSpace(delivery.Status),
				Attempt:      delivery.RetryCount + 1,
			})
		}
	}
	return out, nil
}

func replayEventFromOriginal(original store.OperatorEventFull, replayEventID string, now time.Time) (events.Event, error) {
	payload, err := json.Marshal(original.Payload)
	if err != nil {
		return events.Event{}, err
	}
	source := strings.TrimSpace(original.Source)
	if source == "" || source == "unknown" {
		source = "event.replay"
	}
	evt := events.Event{
		ID:            replayEventID,
		RunID:         original.RunID,
		Type:          events.EventType(original.EventName),
		SourceAgent:   source,
		Payload:       payload,
		ParentEventID: original.EventID,
		CreatedAt:     now,
	}
	return evt.WithEntityID(original.EntityID), nil
}

func eventReplayAuditPayload(
	req Request,
	original store.OperatorEventFull,
	replayEventID string,
	auditEventID string,
	selectedSubscribers []string,
	originalDeliveries []eventReplayDelivery,
) (json.RawMessage, error) {
	payload := map[string]any{
		"original_event_id":   original.EventID,
		"original_event_name": original.EventName,
		"replay_event_id":     replayEventID,
		"audit_event_id":      auditEventID,
		"run_id":              original.RunID,
		"entity_id":           original.EntityID,
		"subscribers":         append([]string(nil), selectedSubscribers...),
		"triggered_by":        eventReplayActorSource(req),
		"actor_token_id":      strings.TrimSpace(req.ActorTokenID),
		"original_deliveries": originalDeliveries,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func eventReplayResultFromStore(ctx context.Context, opts OperatorReadOptions, stored eventReplayStoredResult) (eventReplayResult, error) {
	original, err := opts.Observability.LoadOperatorEvent(ctx, stored.EventID)
	if err != nil {
		return eventReplayResult{}, err
	}
	replay, err := opts.Observability.LoadOperatorEvent(ctx, stored.ReplayEventID)
	if err != nil {
		return eventReplayResult{}, err
	}
	if _, err := opts.Observability.LoadOperatorEvent(ctx, stored.AuditEventID); err != nil {
		return eventReplayResult{}, err
	}
	originalDeliveries, _, err := eventReplayTargets(original, stored.SubscribersReplayed)
	if err != nil {
		return eventReplayResult{}, err
	}
	newDeliveries := eventReplayNewDeliveries(replay.Deliveries, originalDeliveries)
	return eventReplayResult{
		EventID:             strings.TrimSpace(stored.EventID),
		ReplayEventID:       strings.TrimSpace(stored.ReplayEventID),
		AuditEventID:        strings.TrimSpace(stored.AuditEventID),
		SubscribersReplayed: append([]string(nil), stored.SubscribersReplayed...),
		OriginalDeliveries:  originalDeliveries,
		NewDeliveries:       newDeliveries,
	}, nil
}

func eventReplayNewDeliveries(deliveries []store.OperatorEventDelivery, originals []eventReplayDelivery) []eventReplayDelivery {
	sourceBySubscriber := map[string]string{}
	for _, original := range originals {
		sourceBySubscriber[original.SubscriberID] = original.DeliveryID
	}
	out := make([]eventReplayDelivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		subscriberID := strings.TrimSpace(delivery.SubscriberID)
		if strings.TrimSpace(delivery.SubscriberType) != eventReplaySubscriberTypeAgent || subscriberID == "" || subscriberID == eventReplayScopeSubscriberID {
			continue
		}
		out = append(out, eventReplayDelivery{
			DeliveryID:       strings.TrimSpace(delivery.DeliveryID),
			SubscriberID:     subscriberID,
			SessionID:        strings.TrimSpace(delivery.SessionID),
			Status:           strings.TrimSpace(delivery.Status),
			Attempt:          delivery.RetryCount + 1,
			SourceDeliveryID: sourceBySubscriber[subscriberID],
		})
	}
	return out
}

func eventReplayActorSource(req Request) string {
	actor := strings.TrimSpace(req.ActorTokenID)
	if actor == "" {
		return eventReplayDefaultActorSource
	}
	return "swarm-cli:" + actor
}

func optionalStringListParam(params map[string]any, name string) ([]string, bool, error) {
	if params == nil {
		return nil, false, nil
	}
	value, ok := params[name]
	if !ok || isEmptyParam(value) {
		return nil, ok, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, true, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be an array of strings"})
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return nil, true, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be an array of non-empty strings"})
		}
		out = append(out, text)
	}
	return uniqueTrimmedStrings(out), true, nil
}

func uniqueTrimmedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, value := range in {
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

func eventReplayError(err error) error {
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    conflict.OriginalRequestHash,
			"conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      conflict.Method,
				"resource_id": conflict.ResourceID,
			},
		})
	}
	return err
}

func eventReplayPublishError(eventName string, err error) error {
	return runStartPublishError(eventName, err)
}
