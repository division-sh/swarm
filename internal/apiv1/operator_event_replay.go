package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/google/uuid"
)

const (
	eventReplayIdempotencyTTL      = 24 * time.Hour
	eventReplaySyntheticEventName  = "event.replayed"
	eventReplayDefaultActorSource  = "swarm-cli:anonymous"
	eventReplaySubscriberTypeAgent = "agent"
)

type eventReplayPublisher interface {
	EventPublisher
	PublishDirectRoutes(context.Context, events.Event, []events.DeliveryRoute) error
	CheckDirectRoutes(context.Context, events.Event, []events.DeliveryRoute) (runtimebus.ExactDirectRouteStatus, error)
}

type EventReplayOwner interface {
	eventReplayPublisher
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
	DeliveryID       string                                  `json:"delivery_id"`
	SubscriberID     string                                  `json:"subscriber_id"`
	SessionID        string                                  `json:"session_id,omitempty"`
	Status           string                                  `json:"status"`
	ReasonCode       string                                  `json:"reason_code,omitempty"`
	Failure          *runtimefailures.Envelope               `json:"failure,omitempty"`
	Attempt          int                                     `json:"attempt"`
	RetryCount       int                                     `json:"retry_count"`
	RetryScheduled   bool                                    `json:"retry_scheduled"`
	Terminal         bool                                    `json:"terminal"`
	CreatedAt        *time.Time                              `json:"created_at,omitempty"`
	StartedAt        *time.Time                              `json:"started_at,omitempty"`
	FinishedAt       *time.Time                              `json:"finished_at,omitempty"`
	DeadLetters      []operatorread.OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
	SourceDeliveryID string                                  `json:"source_delivery_id,omitempty"`
	route            events.DeliveryRoute
}

type agentReplayResult struct {
	EventID            string                `json:"event_id"`
	AgentID            string                `json:"agent_id"`
	ReplayEventID      string                `json:"replay_event_id"`
	AuditEventID       string                `json:"audit_event_id"`
	OriginalDeliveries []eventReplayDelivery `json:"original_deliveries"`
	NewDeliveries      []eventReplayDelivery `json:"new_deliveries"`
}

type eventReplayStoredResult struct {
	EventID             string                 `json:"event_id"`
	ReplayEventID       string                 `json:"replay_event_id"`
	AuditEventID        string                 `json:"audit_event_id"`
	SubscribersReplayed []string               `json:"subscribers_replayed"`
	AgentIdentity       agentidentity.Identity `json:"agent_identity,omitempty"`
}

type operatorEventReplayRequest struct {
	EventID              string
	RequestedSubscribers []string
	AgentIdentity        agentidentity.Identity
}

type eventReplayPerformed struct {
	Stored           eventReplayStoredResult
	ReplayPublishErr error
}

func OperatorEventReplayHandlers(opts EventReplayHandlerOptions) map[string]MethodHandler {
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
		"agent.replay": func(ctx context.Context, req Request) (any, error) {
			return executeAgentReplay(ctx, req, opts, now().UTC())
		},
	}
}

func eventReplayConfigured(opts EventReplayHandlerOptions) bool {
	if opts.Observability == nil || opts.Idempotency == nil {
		return false
	}
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		return true
	}
	if opts.Events == nil {
		return false
	}
	return opts.Events != nil
}

func executeEventReplay(ctx context.Context, req Request, opts EventReplayHandlerOptions, now time.Time) (any, error) {
	eventID, err := requiredStringParam(req.Params, "event_id")
	if err != nil {
		return nil, err
	}
	requestedSubscribers, _, err := optionalStringListParam(req.Params, "subscribers")
	if err != nil {
		return nil, err
	}
	return executeOperatorEventReplay(ctx, req, opts, now, operatorEventReplayRequest{
		EventID:              eventID,
		RequestedSubscribers: requestedSubscribers,
	})
}

func executeAgentReplay(ctx context.Context, req Request, opts EventReplayHandlerOptions, now time.Time) (any, error) {
	agentID, err := requiredStringParam(req.Params, "agent_id")
	if err != nil {
		return nil, err
	}
	eventID, err := requiredStringParam(req.Params, "event_id")
	if err != nil {
		return nil, err
	}
	if opts.AgentIdentities == nil {
		return nil, errors.New("agent identity resolver is required for agent replay")
	}
	identity, err := resolveOperatorAgentIdentityParam(ctx, opts.AgentIdentities, req.Params, agentID)
	if err != nil {
		return nil, err
	}
	result, err := executeOperatorEventReplay(ctx, req, opts, now, operatorEventReplayRequest{
		EventID:              eventID,
		RequestedSubscribers: []string{agentID},
		AgentIdentity:        identity,
	})
	if err != nil {
		return nil, err
	}
	return agentReplayResultFromEventReplay(identity, result)
}

func executeOperatorEventReplay(
	ctx context.Context,
	req Request,
	opts EventReplayHandlerOptions,
	now time.Time,
	replayReq operatorEventReplayRequest,
) (eventReplayResult, error) {
	publisher := opts.Events
	if publisher == nil {
		return eventReplayResult{}, errors.New("event replay publisher is required")
	}
	eventID := strings.TrimSpace(replayReq.EventID)
	if eventID == "" {
		return eventReplayResult{}, NewInvalidParamsError(map[string]any{"field": "event_id", "reason": "required parameter is missing"})
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return eventReplayResult{}, err
	}
	var replayPublishErr error
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		ResourceID:     eventID,
		TTL:            eventReplayIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		performed, err := performEventReplay(ctx, req, opts, publisher, eventID, replayReq.RequestedSubscribers, replayReq.AgentIdentity, now)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		replayPublishErr = performed.ReplayPublishErr
		response, err := json.Marshal(performed.Stored)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{
			ResourceID: performed.Stored.ReplayEventID,
			Response:   response,
		}, nil
	})
	if err != nil {
		return eventReplayResult{}, eventReplayError(err)
	}
	var stored eventReplayStoredResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		if replay {
			return eventReplayResult{}, fmt.Errorf("decode %s idempotency response: %w", req.Method, err)
		}
		return eventReplayResult{}, fmt.Errorf("decode %s response: %w", req.Method, err)
	}
	if err := ensureEventReplayAudit(ctx, req, opts, publisher, stored, now); err != nil {
		return eventReplayResult{}, err
	}
	if replayPublishErr != nil {
		return eventReplayResult{}, replayPublishErr
	}
	result, err := eventReplayResultFromStore(ctx, opts, stored)
	if err != nil {
		return eventReplayResult{}, err
	}
	return result, nil
}

func performEventReplay(
	ctx context.Context,
	req Request,
	opts EventReplayHandlerOptions,
	publisher eventReplayPublisher,
	eventID string,
	requestedSubscribers []string,
	requestedIdentity agentidentity.Identity,
	now time.Time,
) (eventReplayPerformed, error) {
	original, err := opts.Observability.LoadOperatorEvent(ctx, eventID)
	if errors.Is(err, operatorread.ErrEventNotFound) {
		return eventReplayPerformed{}, NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": eventID})
	}
	if err != nil {
		return eventReplayPerformed{}, err
	}
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		var availability runbundle.Availability
		var selected selectedRuntimeContext
		ctx, selected, availability, err = runtimeBundleContextByRun(ctx, opts.RuntimeContexts, original.RunID)
		if err != nil {
			return eventReplayPerformed{}, err
		}
		_ = availability
		if selected.Runtime == nil || selected.Runtime.Bus == nil {
			return eventReplayPerformed{}, errors.New("event replay publisher is required for selected runtime context")
		}
		publisher = selected.Runtime.Bus
	}
	if err := opts.ExecutionPosture.Admit(original.ExecutionMode, req.Method+" direct-route replay"); err != nil {
		return eventReplayPerformed{}, err
	}
	originalDeliveries, selectedSubscribers, err := eventReplayTargetsForRequest(original, requestedSubscribers, requestedIdentity)
	if err != nil {
		return eventReplayPerformed{}, err
	}
	selectedRoutes, err := eventReplayRoutes(originalDeliveries)
	if err != nil {
		return eventReplayPerformed{}, err
	}
	replayEventID := uuid.NewString()
	replayEvent, err := replayEventFromOriginal(original, replayEventID, now)
	if err != nil {
		return eventReplayPerformed{}, err
	}
	status, err := publisher.CheckDirectRoutes(ctx, replayEvent, selectedRoutes)
	if err != nil {
		return eventReplayPerformed{}, eventReplayPublishError(original.EventName, err)
	}
	if len(status.Missing) > 0 {
		return eventReplayPerformed{}, NewApplicationError(EventReplaySubscriberUnavailableCode, true, map[string]any{
			"event_id":     original.EventID,
			"subscribers":  eventReplayRouteSubscriberIDs(status.Missing),
			"requested":    eventReplayRouteSubscriberIDs(status.Requested),
			"deliverable":  eventReplayRouteSubscriberIDs(status.Deliverable),
			"replay_event": replayEventID,
		})
	}
	var replayPublishErr error
	if err := publisher.PublishDirectRoutes(ctx, replayEvent, selectedRoutes); err != nil {
		persisted, loadErr := eventReplayEvidencePersisted(ctx, opts, replayEventID)
		if loadErr != nil {
			return eventReplayPerformed{}, loadErr
		}
		if !persisted {
			return eventReplayPerformed{}, eventReplayPublishError(original.EventName, err)
		}
		replayPublishErr = eventReplayPublishError(original.EventName, err)
	}
	auditEventID := uuid.NewString()
	return eventReplayPerformed{
		Stored: eventReplayStoredResult{
			EventID:             original.EventID,
			ReplayEventID:       replayEventID,
			AuditEventID:        auditEventID,
			SubscribersReplayed: selectedSubscribers,
			AgentIdentity:       requestedIdentity.Normalize(),
		},
		ReplayPublishErr: replayPublishErr,
	}, nil
}

func eventReplayRouteSubscriberIDs(routes []events.DeliveryRoute) []string {
	ids := make([]string, 0, len(routes))
	for _, route := range routes {
		ids = append(ids, route.Recipient.ID())
	}
	return uniqueTrimmedStrings(ids)
}

func eventReplayEvidencePersisted(ctx context.Context, opts EventReplayHandlerOptions, replayEventID string) (bool, error) {
	if _, err := opts.Observability.LoadOperatorEvent(ctx, replayEventID); err == nil {
		return true, nil
	} else if errors.Is(err, operatorread.ErrEventNotFound) {
		return false, nil
	} else {
		return false, err
	}
}

func ensureEventReplayAudit(
	ctx context.Context,
	req Request,
	opts EventReplayHandlerOptions,
	publisher eventReplayPublisher,
	stored eventReplayStoredResult,
	now time.Time,
) error {
	if strings.TrimSpace(stored.AuditEventID) == "" {
		return fmt.Errorf("%s idempotency response missing audit_event_id", req.Method)
	}
	if _, err := opts.Observability.LoadOperatorEvent(ctx, stored.AuditEventID); err == nil {
		return nil
	} else if !errors.Is(err, operatorread.ErrEventNotFound) {
		return err
	}
	original, err := opts.Observability.LoadOperatorEvent(ctx, stored.EventID)
	if errors.Is(err, operatorread.ErrEventNotFound) {
		return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": stored.EventID})
	}
	if err != nil {
		return err
	}
	if runtimeContextManager(opts.RuntimeContexts) != nil {
		var availability runbundle.Availability
		var selected selectedRuntimeContext
		ctx, selected, availability, err = runtimeBundleContextByRun(ctx, opts.RuntimeContexts, original.RunID)
		if err != nil {
			return err
		}
		_ = availability
		if selected.Runtime == nil || selected.Runtime.Bus == nil {
			return errors.New("event replay publisher is required for selected runtime context")
		}
		publisher = selected.Runtime.Bus
	}
	if err := opts.ExecutionPosture.Admit(original.ExecutionMode, req.Method+" audit publication"); err != nil {
		return err
	}
	originalDeliveries, _, err := eventReplayTargetsForRequest(original, stored.SubscribersReplayed, stored.AgentIdentity)
	if err != nil {
		return err
	}
	auditPayload, err := eventReplayAuditPayload(req, original, stored.ReplayEventID, stored.AuditEventID, stored.SubscribersReplayed, originalDeliveries)
	if err != nil {
		return err
	}
	auditEvent, err := events.NewReplayEvent(events.ReplayEventInput{
		Facts: events.EventFacts{
			ID: stored.AuditEventID, Type: events.EventType(eventReplaySyntheticEventName),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: eventReplayActorSource(req)},
			Payload:  auditPayload, Envelope: events.EventEnvelope{EntityID: original.EntityID},
			CreatedAt: now, ExecutionMode: original.ExecutionMode,
		},
		Lineage: events.EventLineage{RunID: original.RunID, ParentEventID: original.EventID, ExecutionMode: original.ExecutionMode},
	})
	if err != nil {
		return err
	}
	if err := publisher.Publish(ctx, auditEvent); err != nil {
		return eventReplayPublishError(eventReplaySyntheticEventName, err)
	}
	return nil
}

func eventReplayTargets(original operatorread.OperatorEventFull, requested []string) ([]eventReplayDelivery, []string, error) {
	originalBySubscriber := map[string][]operatorread.OperatorEventDelivery{}
	orderedSubscribers := []string{}
	for _, delivery := range original.Deliveries {
		subscriberType := strings.TrimSpace(delivery.SubscriberType)
		subscriberID := strings.TrimSpace(delivery.SubscriberID)
		if subscriberType != eventReplaySubscriberTypeAgent || subscriberID == "" {
			continue
		}
		if _, seen := originalBySubscriber[subscriberID]; !seen {
			orderedSubscribers = append(orderedSubscribers, subscriberID)
		}
		originalBySubscriber[subscriberID] = append(originalBySubscriber[subscriberID], delivery)
	}
	if len(orderedSubscribers) == 0 {
		return nil, nil, NewApplicationError(EventReplayNoDeliveryHistoryCode, false, map[string]any{"event_id": original.EventID})
	}
	requested = uniqueTrimmedStrings(requested)
	if len(requested) == 0 {
		deliveries, err := deliveriesForSubscribers(original.EventID, originalBySubscriber, orderedSubscribers)
		if err != nil {
			return nil, nil, err
		}
		return deliveries, orderedSubscribers, nil
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
	if err != nil {
		return nil, nil, err
	}
	return deliveries, selected, nil
}

func eventReplayTargetsForRequest(original operatorread.OperatorEventFull, requested []string, identity agentidentity.Identity) ([]eventReplayDelivery, []string, error) {
	if identity.IsZero() {
		return eventReplayTargets(original, requested)
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return nil, nil, fmt.Errorf("agent replay target identity: %w", err)
	}
	hasAgentDelivery := false
	for _, delivery := range original.Deliveries {
		if delivery.Route.Normalized().Recipient.IsAgent() {
			hasAgentDelivery = true
			break
		}
	}
	if !hasAgentDelivery {
		return nil, nil, NewApplicationError(EventReplayNoDeliveryHistoryCode, false, map[string]any{"event_id": original.EventID})
	}
	deliveries := make([]eventReplayDelivery, 0)
	for _, delivery := range original.Deliveries {
		route := delivery.Route.Normalized()
		if !route.Recipient.IsAgent() || route.AgentIdentity.Normalize() != identity {
			continue
		}
		if err := validateReplayEligibleDelivery(original.EventID, delivery); err != nil {
			return nil, nil, err
		}
		deliveries = append(deliveries, eventReplayDeliveryFromStore(delivery, ""))
	}
	if len(deliveries) == 0 {
		return nil, nil, NewApplicationError(EventReplaySubscriberNotOriginalCode, false, map[string]any{
			"event_id":      original.EventID,
			"subscriber_id": identity.AgentID(),
			"flow_instance": identity.FlowInstance(),
		})
	}
	return deliveries, []string{identity.AgentID()}, nil
}

func validateReplayEligibleDelivery(eventID string, delivery operatorread.OperatorEventDelivery) error {
	status, err := runtimedelivery.ParseStatus(delivery.Status)
	if err == nil && (status == runtimedelivery.StatusDelivered || status == runtimedelivery.StatusFailed || status == runtimedelivery.StatusDeadLetter) {
		return nil
	}
	data := eventReplayDeliveryFailureEvidence(eventID, delivery)
	data["reason"] = "original delivery status is not replayable"
	return NewApplicationError(EventReplayNotEligibleCode, false, data)
}

func deliveriesForSubscribers(eventID string, index map[string][]operatorread.OperatorEventDelivery, subscribers []string) ([]eventReplayDelivery, error) {
	out := []eventReplayDelivery{}
	for _, subscriber := range subscribers {
		for _, delivery := range index[subscriber] {
			if err := validateReplayEligibleDelivery(eventID, delivery); err != nil {
				return nil, err
			}
			out = append(out, eventReplayDeliveryFromStore(delivery, ""))
		}
	}
	return out, nil
}

func eventReplayRoutes(deliveries []eventReplayDelivery) ([]events.DeliveryRoute, error) {
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		route := delivery.route.Normalized()
		if !route.Recipient.IsAgent() || route.Recipient.ID() != strings.TrimSpace(delivery.SubscriberID) {
			return nil, fmt.Errorf("replay source delivery %s is missing its exact agent route", strings.TrimSpace(delivery.DeliveryID))
		}
		routes = append(routes, route)
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return nil, fmt.Errorf("validate replay source routes: %w", err)
	}
	if len(routes) != len(deliveries) {
		return nil, fmt.Errorf("replay source routes are not one-to-one with original deliveries")
	}
	return routes, nil
}

func replayEventFromOriginal(original operatorread.OperatorEventFull, replayEventID string, now time.Time) (events.Event, error) {
	snapshot, err := original.EventSnapshot()
	if err != nil {
		var event events.Event
		return event, err
	}
	return events.NewReplayEvent(events.ReplayEventInput{
		Facts: events.EventFacts{
			ID: replayEventID, Type: snapshot.Type(),
			Producer: events.ProducerClaim{Type: snapshot.ProducerType(), ID: snapshot.SourceAgent()},
			TaskID:   snapshot.TaskID(), Payload: snapshot.Payload(), ChainDepth: snapshot.ChainDepth() + 1,
			Envelope: snapshot.Envelope(), RoutingSource: snapshot.RoutingSource(), CreatedAt: now,
			ExecutionMode: snapshot.ExecutionMode(),
		},
		Lineage: events.EventLineage{RunID: snapshot.RunID(), ParentEventID: snapshot.ID(), TaskID: snapshot.TaskID(), ExecutionMode: snapshot.ExecutionMode()},
	})
}

func eventReplayAuditPayload(
	req Request,
	original operatorread.OperatorEventFull,
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
		"subscribers":         append([]string(nil), selectedSubscribers...),
		"triggered_by":        eventReplayActorSource(req),
		"actor_token_id":      strings.TrimSpace(req.ActorTokenID),
		"original_deliveries": originalDeliveries,
	}
	if entityID := strings.TrimSpace(original.EntityID); entityID != "" {
		payload["entity_id"] = entityID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func eventReplayResultFromStore(ctx context.Context, opts EventReplayHandlerOptions, stored eventReplayStoredResult) (eventReplayResult, error) {
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
	originalDeliveries, _, err := eventReplayTargetsForRequest(original, stored.SubscribersReplayed, stored.AgentIdentity)
	if err != nil {
		return eventReplayResult{}, err
	}
	newDeliveries, err := eventReplayNewDeliveries(replay.Deliveries, originalDeliveries)
	if err != nil {
		return eventReplayResult{}, err
	}
	return eventReplayResult{
		EventID:             strings.TrimSpace(stored.EventID),
		ReplayEventID:       strings.TrimSpace(stored.ReplayEventID),
		AuditEventID:        strings.TrimSpace(stored.AuditEventID),
		SubscribersReplayed: append([]string(nil), stored.SubscribersReplayed...),
		OriginalDeliveries:  originalDeliveries,
		NewDeliveries:       newDeliveries,
	}, nil
}

func eventReplayNewDeliveries(deliveries []operatorread.OperatorEventDelivery, originals []eventReplayDelivery) ([]eventReplayDelivery, error) {
	sourceByRoute := map[string]string{}
	for _, original := range originals {
		identity, err := original.route.Identity()
		if err != nil {
			return nil, fmt.Errorf("original replay delivery %s route: %w", original.DeliveryID, err)
		}
		encoded := events.EncodeDeliveryRouteIdentity(identity)
		if _, duplicate := sourceByRoute[encoded]; duplicate {
			return nil, fmt.Errorf("duplicate original replay route identity %s", encoded)
		}
		sourceByRoute[encoded] = original.DeliveryID
	}
	out := make([]eventReplayDelivery, 0, len(deliveries))
	matched := map[string]struct{}{}
	for _, delivery := range deliveries {
		subscriberID := strings.TrimSpace(delivery.SubscriberID)
		if strings.TrimSpace(delivery.SubscriberType) != eventReplaySubscriberTypeAgent || subscriberID == "" {
			continue
		}
		identity, err := delivery.Route.Identity()
		if err != nil {
			return nil, fmt.Errorf("new replay delivery %s route: %w", delivery.DeliveryID, err)
		}
		sourceDeliveryID := sourceByRoute[events.EncodeDeliveryRouteIdentity(identity)]
		if sourceDeliveryID == "" {
			return nil, fmt.Errorf("new replay delivery %s has no exact original route", delivery.DeliveryID)
		}
		if _, duplicate := matched[sourceDeliveryID]; duplicate {
			return nil, fmt.Errorf("original replay delivery %s matched multiple new deliveries", sourceDeliveryID)
		}
		matched[sourceDeliveryID] = struct{}{}
		out = append(out, eventReplayDeliveryFromStore(delivery, sourceDeliveryID))
	}
	if len(out) != len(originals) {
		return nil, fmt.Errorf("replay persisted %d exact agent deliveries, want %d", len(out), len(originals))
	}
	return out, nil
}

func eventReplayDeliveryFromStore(delivery operatorread.OperatorEventDelivery, sourceDeliveryID string) eventReplayDelivery {
	published := eventPublishDeliveryFromStore(delivery)
	return eventReplayDelivery{
		DeliveryID:       published.DeliveryID,
		SubscriberID:     published.SubscriberID,
		SessionID:        published.SessionID,
		Status:           published.Status,
		ReasonCode:       published.ReasonCode,
		Failure:          runtimefailures.CloneEnvelope(published.Failure),
		Attempt:          published.Attempt,
		RetryCount:       published.RetryCount,
		RetryScheduled:   published.RetryScheduled,
		Terminal:         published.Terminal,
		CreatedAt:        published.CreatedAt,
		StartedAt:        published.StartedAt,
		FinishedAt:       published.FinishedAt,
		DeadLetters:      append([]operatorread.OperatorDeadLetterRecord(nil), published.DeadLetters...),
		SourceDeliveryID: strings.TrimSpace(sourceDeliveryID),
		route:            delivery.Route.Normalized(),
	}
}

func eventReplayDeliveryFailureEvidence(eventID string, delivery operatorread.OperatorEventDelivery) map[string]any {
	data := map[string]any{
		"event_id":        strings.TrimSpace(eventID),
		"delivery_id":     strings.TrimSpace(delivery.DeliveryID),
		"subscriber_id":   strings.TrimSpace(delivery.SubscriberID),
		"status":          strings.TrimSpace(delivery.Status),
		"retry_count":     delivery.RetryCount,
		"retry_scheduled": delivery.RetryScheduled,
		"terminal":        delivery.Terminal,
		"dead_letters":    append([]operatorread.OperatorDeadLetterRecord(nil), delivery.DeadLetters...),
	}
	if reason := strings.TrimSpace(delivery.ReasonCode); reason != "" {
		data["reason_code"] = reason
	}
	if delivery.Failure != nil {
		data["failure"] = *delivery.Failure
	}
	return data
}

func agentReplayResultFromEventReplay(identity agentidentity.Identity, replay eventReplayResult) (agentReplayResult, error) {
	identity = identity.Normalize()
	agentID := identity.AgentID()
	originals := deliveriesForAgentIdentity(replay.OriginalDeliveries, identity)
	if len(originals) == 0 {
		return agentReplayResult{}, fmt.Errorf("agent.replay canonical replay result missing original delivery for agent %s", agentID)
	}
	newDeliveries := deliveriesForAgentIdentity(replay.NewDeliveries, identity)
	if len(newDeliveries) != len(originals) {
		return agentReplayResult{}, fmt.Errorf("agent.replay canonical replay result missing new delivery for agent %s", agentID)
	}
	return agentReplayResult{
		EventID: strings.TrimSpace(replay.EventID), AgentID: agentID,
		ReplayEventID: strings.TrimSpace(replay.ReplayEventID), AuditEventID: strings.TrimSpace(replay.AuditEventID),
		OriginalDeliveries: originals, NewDeliveries: newDeliveries,
	}, nil
}

func deliveriesForAgentIdentity(deliveries []eventReplayDelivery, identity agentidentity.Identity) []eventReplayDelivery {
	identity = identity.Normalize()
	out := make([]eventReplayDelivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.route.Normalized().AgentIdentity.Normalize() == identity {
			out = append(out, delivery)
		}
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
	var conflict *apiidempotency.ConflictError
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
	return eventCatalogPublishError(eventName, err)
}
