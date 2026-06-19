package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimerunstart "github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

type eventPublishResult struct {
	EventID       string                 `json:"event_id"`
	RunID         string                 `json:"run_id"`
	SourceEventID string                 `json:"source_event_id,omitempty"`
	NewRunCreated bool                   `json:"new_run_created"`
	Deliveries    []eventPublishDelivery `json:"deliveries"`
}

type eventPublishDelivery struct {
	DeliveryID     string                           `json:"delivery_id"`
	SubscriberType string                           `json:"subscriber_type"`
	SubscriberID   string                           `json:"subscriber_id"`
	SessionID      string                           `json:"session_id,omitempty"`
	Status         string                           `json:"status"`
	ReasonCode     string                           `json:"reason_code,omitempty"`
	LastError      string                           `json:"last_error,omitempty"`
	Attempt        int                              `json:"attempt"`
	RetryCount     int                              `json:"retry_count"`
	RetryEligible  bool                             `json:"retry_eligible"`
	Terminal       bool                             `json:"terminal"`
	CreatedAt      *time.Time                       `json:"created_at,omitempty"`
	StartedAt      *time.Time                       `json:"started_at,omitempty"`
	FinishedAt     *time.Time                       `json:"finished_at,omitempty"`
	DeadLetters    []store.OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
}

type eventPublicationParams struct {
	BundleHash        string
	BundleSource      string
	BundleFingerprint string
	EventID           string
	EventName         string
	Payload           json.RawMessage
	EntityID          string
	EntityIDProvided  bool
	FlowInstance      string
	TargetRoute       events.RouteIdentity
	TargetRouteSet    bool
	RunID             string
	SourceEventID     string
	IdempotencyKey    string
	Emitter           string
	NewRunCreated     bool
	RunIDProvided     bool
}

type eventPublicationConfig struct {
	sourceAgent                    func(Request) string
	allowEmitterParam              bool
	allowExplicitTargetRoute       bool
	rootInputOnly                  bool
	requireExistingExplicitRun     bool
	injectRunIDEntityIDWhenMissing bool
	injectRunIDEntityIDOnlyNewRun  bool
	durablePublishAck              bool
	publishError                   func(eventPublicationParams, error) error
	buildCompletion                func(context.Context, OperatorReadOptions, eventPublicationParams) (any, string, error)
}

type eventAcknowledgedPublisher interface {
	PublishAcknowledged(context.Context, events.Event) error
}

func OperatorEventPublishHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if !eventPublishConfigured(opts) {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"event.publish": func(ctx context.Context, req Request) (any, error) {
			return executeEventPublish(ctx, req, opts, now().UTC())
		},
	}
}

func eventPublishConfigured(opts OperatorReadOptions) bool {
	return runStartConfigured(opts) && opts.Runs != nil && opts.Observability != nil
}

func executeEventPublish(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	cfg := eventPublicationConfig{
		sourceAgent:                    eventPublishSourceAgent,
		allowEmitterParam:              true,
		allowExplicitTargetRoute:       true,
		requireExistingExplicitRun:     true,
		injectRunIDEntityIDWhenMissing: true,
		injectRunIDEntityIDOnlyNewRun:  true,
		durablePublishAck:              true,
		publishError:                   eventPublishPublishError,
		buildCompletion: func(_ context.Context, _ OperatorReadOptions, params eventPublicationParams) (any, string, error) {
			return eventPublishResult{
				EventID:       params.EventID,
				RunID:         params.RunID,
				SourceEventID: params.SourceEventID,
				NewRunCreated: params.NewRunCreated,
				Deliveries:    []eventPublishDelivery{},
			}, params.EventID, nil
		},
	}
	completion, replay, err := executeOperatorEventPublication(ctx, req, opts, now, cfg)
	if err != nil {
		return nil, runStartIdempotencyError(err)
	}
	stored, err := eventPublishStoredResult(completion)
	if err != nil {
		if replay {
			return nil, fmt.Errorf("decode event.publish idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode event.publish response: %w", err)
	}
	result, err := eventPublishResultFromStore(ctx, opts, completion, stored)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func eventPublishStoredResult(completion store.APIIdempotencyCompletion) (eventPublishResult, error) {
	var stored eventPublishResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		return eventPublishResult{}, err
	}
	return stored, nil
}

func eventPublishResultFromStore(ctx context.Context, opts OperatorReadOptions, completion store.APIIdempotencyCompletion, stored eventPublishResult) (eventPublishResult, error) {
	eventID := strings.TrimSpace(completion.ResourceID)
	if eventID == "" {
		eventID = strings.TrimSpace(stored.EventID)
	}
	event, err := opts.Observability.LoadOperatorEvent(ctx, eventID)
	if errors.Is(err, store.ErrEventNotFound) {
		return eventPublishResult{}, fmt.Errorf("load published event %s: %w", eventID, err)
	}
	if err != nil {
		return eventPublishResult{}, err
	}
	runID := strings.TrimSpace(event.RunID)
	if runID == "" {
		runID = strings.TrimSpace(stored.RunID)
	}
	return eventPublishResult{
		EventID:       strings.TrimSpace(event.EventID),
		RunID:         runID,
		SourceEventID: strings.TrimSpace(event.SourceEventID),
		NewRunCreated: stored.NewRunCreated,
		Deliveries:    eventPublishDeliveries(event.Deliveries),
	}, nil
}

func executeOperatorEventPublication(
	ctx context.Context,
	req Request,
	opts OperatorReadOptions,
	now time.Time,
	cfg eventPublicationConfig,
) (store.APIIdempotencyCompletion, bool, error) {
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return store.APIIdempotencyCompletion{}, false, err
	}
	return opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runStartIDempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		params, bundleIdentity, err := eventPublicationParamsFromRequest(req, cfg)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		selectedOpts := opts
		ctx, selectedOpts, params, err = resolveEventPublicationBundleScope(ctx, opts, params, bundleIdentity, cfg)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if !cfg.rootInputOnly {
			resolvedEventName, err := resolveEventPublicationEventName(selectedOpts.Source, params.EventName)
			if err != nil {
				return store.APIIdempotencyCompletion{}, err
			}
			params.EventName = resolvedEventName
		}
		params, err = validateEventPublication(ctx, selectedOpts, params, cfg)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if err := publishEventPublication(ctx, selectedOpts.Events, eventPublicationEvent(params, now), cfg); err != nil {
			if cfg.publishError != nil {
				return store.APIIdempotencyCompletion{}, cfg.publishError(params, err)
			}
			return store.APIIdempotencyCompletion{}, runStartPublishError(params.EventName, err)
		}
		result, resourceID, err := cfg.buildCompletion(ctx, selectedOpts, params)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: resourceID,
			Response:   response,
		}, nil
	})
}

func publishEventPublication(ctx context.Context, publisher EventPublisher, evt events.Event, cfg eventPublicationConfig) error {
	if cfg.durablePublishAck {
		acknowledged, ok := publisher.(eventAcknowledgedPublisher)
		if !ok || acknowledged == nil {
			return errors.New("durable event.publish acknowledgment requires acknowledged publisher")
		}
		return acknowledged.PublishAcknowledged(ctx, evt)
	}
	return publisher.Publish(ctx, evt)
}

func eventPublicationParamsFromRequest(req Request, cfg eventPublicationConfig) (eventPublicationParams, bundleIdentityParam, error) {
	eventName := stringParam(req.Params, "event_name")
	if eventName == "" {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "event_name", "reason": "required parameter is missing"})
	}
	bundleIdentity, err := bundleIdentityInputParam(req.Params)
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	runID, runIDProvided, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	sourceEventID, sourceEventIDSet, err := optionalStringParam(req.Params, "source_event_id")
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	targetRoute, targetRouteSet, err := eventPublicationTargetRouteParam(req.Params)
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	if targetRouteSet && !cfg.allowExplicitTargetRoute {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "target", "reason": "is not supported for this method"})
	}
	if sourceEventIDSet {
		if sourceEventID == "" {
			return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "source_event_id", "reason": "must be a UUID"})
		}
		parsed, err := uuid.Parse(sourceEventID)
		if err != nil {
			return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "source_event_id", "reason": "must be a UUID"})
		}
		sourceEventID = parsed.String()
	}
	if sourceEventID != "" && runID == "" {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "is required when source_event_id is provided"})
	}
	if targetRouteSet && runID == "" {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "is required when target is provided"})
	}
	newRun := false
	if runID == "" {
		runID = uuid.NewString()
		newRun = true
	} else if parsed, err := uuid.Parse(runID); err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must be a UUID"})
	} else {
		runID = parsed.String()
	}
	injectEntityID := cfg.injectRunIDEntityIDWhenMissing && (!cfg.injectRunIDEntityIDOnlyNewRun || newRun)
	payload, entityID, entityIDProvided, err := eventPublicationPayload(req.Params, runID, injectEntityID)
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	emitter := ""
	if cfg.allowEmitterParam {
		emitter, _, err = optionalStringParam(req.Params, "emitter")
		if err != nil {
			return eventPublicationParams{}, bundleIdentityParam{}, err
		}
	}
	if emitter == "" && cfg.sourceAgent != nil {
		emitter = cfg.sourceAgent(req)
	}
	return eventPublicationParams{
		BundleFingerprint: bundleIdentity.LegacyFingerprint,
		EventID:           uuid.NewString(),
		EventName:         eventName,
		Payload:           payload,
		EntityID:          entityID,
		EntityIDProvided:  entityIDProvided,
		TargetRoute:       targetRoute,
		TargetRouteSet:    targetRouteSet,
		RunID:             runID,
		SourceEventID:     sourceEventID,
		IdempotencyKey:    idempotencyKey,
		Emitter:           emitter,
		NewRunCreated:     newRun,
		RunIDProvided:     runIDProvided,
	}, bundleIdentity, nil
}

func eventPublicationTargetRouteParam(params map[string]any) (events.RouteIdentity, bool, error) {
	if params == nil {
		return events.RouteIdentity{}, false, nil
	}
	raw, ok := params["target"]
	if !ok {
		return events.RouteIdentity{}, false, nil
	}
	if isEmptyParam(raw) {
		return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target", "reason": "must be an object"})
	}
	target, ok := raw.(map[string]any)
	if !ok {
		return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target", "reason": "must be an object"})
	}
	for key := range target {
		switch key {
		case "flow_instance", "entity_id":
		default:
			return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target." + key, "reason": "unknown field"})
		}
	}
	flowInstance, err := requiredTargetStringParam(target, "target.flow_instance", "flow_instance")
	if err != nil {
		return events.RouteIdentity{}, true, err
	}
	entityID, err := requiredTargetStringParam(target, "target.entity_id", "entity_id")
	if err != nil {
		return events.RouteIdentity{}, true, err
	}
	parsedEntityID, err := uuid.Parse(entityID)
	if err != nil {
		return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target.entity_id", "reason": "must be a UUID"})
	}
	route := events.RouteIdentity{
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		EntityID:     parsedEntityID.String(),
	}.Normalized()
	if route.FlowInstance == "" {
		return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target.flow_instance", "reason": "is required"})
	}
	return route, true, nil
}

func requiredTargetStringParam(params map[string]any, field, key string) (string, error) {
	value, ok := params[key]
	if !ok || isEmptyParam(value) {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "is required"})
	}
	text, ok := value.(string)
	if !ok {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "must be a string"})
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "is required"})
	}
	return text, nil
}

func eventPublicationPayload(params map[string]any, runID string, injectRunIDEntityID bool) (json.RawMessage, string, bool, error) {
	if params == nil {
		return nil, "", false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	raw, ok := params["payload"]
	if !ok || isEmptyParam(raw) {
		return nil, "", false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, "", false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "must be an object"})
	}
	cloned := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		cloned[key] = value
	}
	entityID, supplied, err := runStartPayloadEntityID(cloned["entity_id"])
	if err != nil {
		return nil, "", false, err
	}
	switch {
	case supplied:
		cloned["entity_id"] = entityID
	case injectRunIDEntityID:
		entityID = strings.TrimSpace(runID)
		cloned["entity_id"] = entityID
	}
	encoded, err := json.Marshal(cloned)
	if err != nil {
		return nil, "", false, err
	}
	return encoded, entityID, supplied, nil
}

func eventPublicationEvent(params eventPublicationParams, createdAt time.Time) events.Event {
	envelope := events.EventEnvelope{EntityID: params.EntityID}
	if flowInstance := strings.Trim(strings.TrimSpace(params.FlowInstance), "/"); flowInstance != "" {
		envelope.FlowInstance = flowInstance
	}
	if params.TargetRouteSet {
		envelope = events.EnvelopeForTargetRoute(envelope, params.TargetRoute)
	}
	return events.NewRootIngressEvent(params.EventID, events.EventType(params.EventName), params.Emitter, "", params.Payload, 0, params.RunID, params.SourceEventID, envelope, createdAt)
}

func validateEventPublication(ctx context.Context, opts OperatorReadOptions, params eventPublicationParams, cfg eventPublicationConfig) (eventPublicationParams, error) {
	if cfg.rootInputOnly {
		set, err := runtimerunstart.ValidateInputEvents(opts.Source, []string{params.EventName})
		if err != nil {
			return params, runStartRootInputError(params.EventName, set, err)
		}
		return params, nil
	}
	if !eventDeclared(opts.Source, params.EventName) {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"declared_events": declaredEventNames(opts.Source),
		})
	}
	if params.EntityIDProvided && eventPublicationHasCreateEntityHandler(opts.Source, params.EventName) {
		return params, NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$.entity_id",
				"rule":       "create_entity_mints_entity_id",
				"message":    "caller-supplied entity_id is not allowed for create-entity event.publish",
			}},
			"event_name": params.EventName,
		})
	}
	if params.TargetRouteSet && eventPublicationHasCreateEntityHandler(opts.Source, params.EventName) {
		return params, NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$.target.entity_id",
				"rule":       "create_entity_mints_entity_id",
				"message":    "caller-supplied target entity_id is not allowed for create-entity event.publish",
			}},
			"event_name": params.EventName,
		})
	}
	if cfg.requireExistingExplicitRun && !params.NewRunCreated {
		runs, err := requireRunReadStore(opts.Runs)
		if err != nil {
			return params, err
		}
		header, err := runs.LoadRunHeader(ctx, params.RunID)
		if errors.Is(err, store.ErrRunNotFound) {
			return params, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": params.RunID})
		}
		if err != nil {
			return params, err
		}
		status := strings.TrimSpace(strings.ToLower(header.Status))
		if status != "running" && status != "paused" {
			return params, NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{
				"run_id":         params.RunID,
				"current_status": status,
			})
		}
	}
	if params.SourceEventID != "" {
		sourceEvent, err := opts.Observability.LoadOperatorEvent(ctx, params.SourceEventID)
		if errors.Is(err, store.ErrEventNotFound) {
			return params, NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": params.SourceEventID})
		}
		if err != nil {
			return params, err
		}
		if strings.TrimSpace(sourceEvent.RunID) != params.RunID {
			return params, NewInvalidParamsError(map[string]any{
				"field":           "source_event_id",
				"reason":          "must belong to run_id",
				"source_event_id": params.SourceEventID,
				"run_id":          params.RunID,
				"source_run_id":   strings.TrimSpace(sourceEvent.RunID),
			})
		}
	}
	if cfg.requireExistingExplicitRun && !params.NewRunCreated {
		enriched, err := enrichExistingRunEventPublicationRoute(ctx, opts, params)
		if err != nil {
			return params, err
		}
		params = enriched
		if err := validateExistingRunEventPublicationRecipientPlan(ctx, opts, params, cfg); err != nil {
			return params, err
		}
	}
	return params, nil
}

func enrichExistingRunEventPublicationRoute(ctx context.Context, opts OperatorReadOptions, params eventPublicationParams) (eventPublicationParams, error) {
	if params.TargetRouteSet {
		return enrichExistingRunEventPublicationTargetRoute(ctx, opts, params)
	}
	if params.EntityIDProvided && eventPublicationRequiresExplicitTargetRoute(opts.Source, params.EventName) {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"run_id":          params.RunID,
			"entity_id":       params.EntityID,
			"declared_events": declaredEventNames(opts.Source),
			"reason":          "selected_run_target_required",
		})
	}
	if strings.TrimSpace(params.EntityID) == "" {
		return params, nil
	}
	entities, err := requireEntityReadStore(opts.Entities)
	if err != nil {
		return params, err
	}
	entity, err := entities.LoadOperatorEntity(ctx, params.EntityID, params.RunID)
	if errors.Is(err, store.ErrEntityNotFound) {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"run_id":          params.RunID,
			"entity_id":       params.EntityID,
			"declared_events": declaredEventNames(opts.Source),
			"reason":          "selected_run_entity_not_found",
		})
	}
	if err != nil {
		return params, err
	}
	params.FlowInstance = strings.Trim(strings.TrimSpace(entity.Entity.FlowInstance), "/")
	return params, nil
}

func eventPublicationRequiresExplicitTargetRoute(source semanticview.Source, eventName string) bool {
	if source == nil {
		return false
	}
	eventName = runtimeeventidentity.Normalize(eventName)
	if eventName == "" {
		return false
	}
	for _, scope := range source.FlowScopes() {
		if !strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
			continue
		}
		if source.FlowHasInputEvent(scope.ID, eventName) {
			return true
		}
	}
	return false
}

func enrichExistingRunEventPublicationTargetRoute(ctx context.Context, opts OperatorReadOptions, params eventPublicationParams) (eventPublicationParams, error) {
	target := params.TargetRoute.Normalized()
	if target.EntityID == "" || target.FlowInstance == "" {
		return params, NewInvalidParamsError(map[string]any{"field": "target", "reason": "flow_instance and entity_id are required"})
	}
	entities, err := requireEntityReadStore(opts.Entities)
	if err != nil {
		return params, err
	}
	entity, err := entities.LoadOperatorEntity(ctx, target.EntityID, params.RunID)
	if errors.Is(err, store.ErrEntityNotFound) {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"run_id":          params.RunID,
			"entity_id":       target.EntityID,
			"flow_instance":   target.FlowInstance,
			"declared_events": declaredEventNames(opts.Source),
			"reason":          "selected_target_entity_not_found",
		})
	}
	if err != nil {
		return params, err
	}
	storedFlowInstance := strings.Trim(strings.TrimSpace(entity.Entity.FlowInstance), "/")
	if storedFlowInstance != target.FlowInstance {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":           params.EventName,
			"run_id":               params.RunID,
			"entity_id":            target.EntityID,
			"flow_instance":        target.FlowInstance,
			"stored_flow_instance": storedFlowInstance,
			"declared_events":      declaredEventNames(opts.Source),
			"reason":               "selected_target_flow_instance_mismatch",
		})
	}
	params.TargetRoute = target
	params.EntityID = target.EntityID
	params.FlowInstance = target.FlowInstance
	return params, nil
}

type eventPublishRecipientPlanChecker interface {
	CheckPublishRecipientPlan(context.Context, events.Event) (runtimebus.PublishRecipientPlan, error)
}

func validateExistingRunEventPublicationRecipientPlan(ctx context.Context, opts OperatorReadOptions, params eventPublicationParams, cfg eventPublicationConfig) error {
	checker, ok := opts.Events.(eventPublishRecipientPlanChecker)
	if !ok || checker == nil {
		return NewApplicationError(EventPublishFailedCode, true, map[string]any{
			"event_name": params.EventName,
			"event_id":   params.EventID,
			"run_id":     params.RunID,
			"phase":      "publish",
			"reason":     "recipient planning unavailable: event publisher does not expose subscribed recipient planning",
		})
	}
	plan, err := checker.CheckPublishRecipientPlan(ctx, eventPublicationEvent(params, time.Time{}))
	if err != nil {
		if cfg.publishError != nil {
			return cfg.publishError(params, err)
		}
		return runStartPublishError(params.EventName, err)
	}
	if strings.TrimSpace(plan.TargetFailure) != "" {
		return NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"run_id":          params.RunID,
			"declared_events": declaredEventNames(opts.Source),
			"reason":          "selected_run_target_not_routable",
			"target_failure":  plan.TargetFailure,
		})
	}
	if len(plan.PersistedRecipients) == 0 && len(plan.DeliveryRoutes) == 0 {
		return NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":              params.EventName,
			"run_id":                  params.RunID,
			"entity_id":               params.EntityID,
			"flow_instance":           params.FlowInstance,
			"declared_events":         declaredEventNames(opts.Source),
			"reason":                  "declared_event_has_no_selected_run_recipient",
			"routed_recipients":       plan.RoutedRecipients,
			"subscription_recipients": plan.SubscriptionRecipients,
		})
	}
	return nil
}

func eventPublicationHasCreateEntityHandler(source semanticview.Source, eventName string) bool {
	if source == nil {
		return false
	}
	eventName = runtimeeventidentity.Normalize(eventName)
	for _, nodeID := range source.RuntimeEventOwners(eventName) {
		handler, ok := source.NodeEventHandler(nodeID, eventName)
		if ok && handler.CreateEntity {
			return true
		}
	}
	for nodeID := range source.NodeEntries() {
		for authoredEventName, handler := range source.NodeEventHandlers(nodeID) {
			if !handler.CreateEntity {
				continue
			}
			canonical := runtimeeventidentity.Normalize(source.ResolveNodeEventReference(nodeID, authoredEventName))
			authored := runtimeeventidentity.Normalize(authoredEventName)
			if canonical == eventName || authored == eventName {
				return true
			}
		}
	}
	return false
}

func runStartRootInputError(eventName string, set runtimerunstart.RootInputSet, err error) error {
	declared := append([]string{}, set.Declared...)
	routable := append([]string{}, set.Routable...)
	details := map[string]any{
		"event_name":      eventName,
		"declared_events": declared,
		"routable_events": routable,
		"reason":          strings.TrimSpace(err.Error()),
	}
	if !stringSliceContains(declared, eventName) {
		details["reason"] = "not_declared_root_input"
	} else if !stringSliceContains(routable, eventName) {
		details["reason"] = "declared_root_input_not_routable"
	}
	return NewApplicationError(EventNotDeclaredCode, false, details)
}

func stringSliceContains(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func eventPublishSourceAgent(req Request) string {
	actor := strings.TrimSpace(req.ActorTokenID)
	if actor == "" {
		actor = "anonymous"
	}
	return "cli-publish:" + actor
}

func eventPublishPublishError(params eventPublicationParams, err error) error {
	mapped := runStartPublishError(params.EventName, err)
	var appErr *ApplicationError
	if errors.As(mapped, &appErr) {
		return mapped
	}
	return NewApplicationError(EventPublishFailedCode, true, map[string]any{
		"event_name": params.EventName,
		"event_id":   params.EventID,
		"run_id":     params.RunID,
		"phase":      "publish",
		"reason":     strings.TrimSpace(err.Error()),
	})
}

func eventPublishDeliveries(in []store.OperatorEventDelivery) []eventPublishDelivery {
	out := make([]eventPublishDelivery, 0, len(in))
	seen := map[string]struct{}{}
	for _, delivery := range in {
		if strings.TrimSpace(delivery.SubscriberID) == "__runtime_replay_scope__" {
			continue
		}
		item := eventPublishDeliveryFromStore(delivery)
		key := strings.Join([]string{item.DeliveryID, item.SubscriberType, item.SubscriberID, item.Status}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}

func eventPublishDeliveryFromStore(delivery store.OperatorEventDelivery) eventPublishDelivery {
	attempt := delivery.RetryCount + 1
	if attempt < 1 {
		attempt = 1
	}
	status := strings.TrimSpace(delivery.Status)
	return eventPublishDelivery{
		DeliveryID:     strings.TrimSpace(delivery.DeliveryID),
		SubscriberType: strings.TrimSpace(delivery.SubscriberType),
		SubscriberID:   strings.TrimSpace(delivery.SubscriberID),
		SessionID:      strings.TrimSpace(delivery.SessionID),
		Status:         status,
		ReasonCode:     strings.TrimSpace(delivery.ReasonCode),
		LastError:      strings.TrimSpace(delivery.LastError),
		Attempt:        attempt,
		RetryCount:     delivery.RetryCount,
		RetryEligible:  delivery.RetryEligible || store.OperatorDeliveryRetryEligible(status),
		Terminal:       delivery.Terminal || store.OperatorDeliveryTerminal(status),
		CreatedAt:      cloneTimePtr(delivery.CreatedAt),
		StartedAt:      cloneTimePtr(delivery.StartedAt),
		FinishedAt:     cloneTimePtr(delivery.FinishedAt),
		DeadLetters:    append([]store.OperatorDeadLetterRecord(nil), delivery.DeadLetters...),
	}
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func eventDeclared(source semanticview.Source, eventName string) bool {
	eventName = runtimeeventidentity.Normalize(eventName)
	if source == nil || eventName == "" {
		return false
	}
	if _, ok := source.EventEntry(eventName); ok {
		return true
	}
	for name := range source.ResolvedEventCatalog() {
		if runtimeeventidentity.Normalize(name) == eventName {
			return true
		}
	}
	for _, candidate := range eventPublicationEventNameCandidates(source, eventName) {
		if candidate == eventName {
			return true
		}
	}
	return false
}

func declaredEventNames(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for name := range source.EventEntries() {
		name = runtimeeventidentity.Normalize(name)
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	for name := range source.ResolvedEventCatalog() {
		name = runtimeeventidentity.Normalize(name)
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	for _, scope := range source.FlowScopes() {
		for eventName := range scope.Events {
			canonical := canonicalFlowEventName(source, scope, eventName)
			if canonical != "" {
				seen[canonical] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func resolveEventPublicationEventName(source semanticview.Source, eventName string) (string, error) {
	eventName = runtimeeventidentity.Normalize(eventName)
	candidates := eventPublicationEventNameCandidates(source, eventName)
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	reason := "unknown_event"
	if strings.Contains(eventName, "/") {
		reason = "unknown_flow_scoped_event"
	}
	if len(candidates) > 1 {
		reason = "ambiguous_event_name"
	}
	return "", NewApplicationError(EventNotDeclaredCode, false, map[string]any{
		"event_name":      eventName,
		"declared_events": declaredEventNames(source),
		"reason":          reason,
	})
}

func eventPublicationEventNameCandidates(source semanticview.Source, eventName string) []string {
	eventName = runtimeeventidentity.Normalize(eventName)
	if source == nil || eventName == "" {
		return nil
	}
	scoped := strings.Contains(eventName, "/")
	if !scoped {
		if _, ok := source.EventEntry(eventName); ok {
			return []string{eventName}
		}
	}
	flowCandidates := make(map[string]struct{})
	for _, scope := range source.FlowScopes() {
		for localEventName := range scope.Events {
			localEventName = runtimeeventidentity.Normalize(localEventName)
			if localEventName == "" {
				continue
			}
			canonical := canonicalFlowEventName(source, scope, localEventName)
			if canonical == "" {
				continue
			}
			if !scoped && localEventName == eventName {
				flowCandidates[canonical] = struct{}{}
				continue
			}
			if scoped && flowScopedEventNameMatches(eventName, scope, localEventName, canonical) {
				flowCandidates[canonical] = struct{}{}
			}
		}
	}
	if len(flowCandidates) > 0 {
		return sortedEventNameCandidates(flowCandidates)
	}
	if scoped {
		return nil
	}
	for name := range source.ResolvedEventCatalog() {
		if runtimeeventidentity.Normalize(name) == eventName {
			return []string{eventName}
		}
	}
	return nil
}

func canonicalFlowEventName(source semanticview.Source, scope semanticview.FlowScope, eventName string) string {
	eventName = runtimeeventidentity.Normalize(eventName)
	if source == nil || eventName == "" {
		return ""
	}
	flowID := strings.TrimSpace(scope.ID)
	if _, _, ok := source.ResolveFlowEventCatalogEntry(flowID, eventName); !ok {
		return ""
	}
	canonical := runtimeeventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventName))
	if canonical == "" {
		return eventName
	}
	return canonical
}

func flowScopedEventNameMatches(requested string, scope semanticview.FlowScope, localEventName, canonical string) bool {
	requested = runtimeeventidentity.Normalize(requested)
	localEventName = runtimeeventidentity.Normalize(localEventName)
	canonical = runtimeeventidentity.Normalize(canonical)
	if requested == "" || localEventName == "" {
		return false
	}
	if requested == canonical {
		return true
	}
	for _, prefix := range []string{scope.ID, scope.Path} {
		prefix = runtimeeventidentity.Normalize(prefix)
		if prefix == "" {
			continue
		}
		if requested == prefix+"/"+localEventName {
			return true
		}
	}
	return false
}

func sortedEventNameCandidates(candidates map[string]struct{}) []string {
	out := make([]string, 0, len(candidates))
	for candidate := range candidates {
		candidate = runtimeeventidentity.Normalize(candidate)
		if candidate != "" {
			out = append(out, candidate)
		}
	}
	sort.Strings(out)
	return out
}
