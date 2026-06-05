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
	DeliveryID     string `json:"delivery_id"`
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	SessionID      string `json:"session_id,omitempty"`
	Status         string `json:"status"`
	Attempt        int    `json:"attempt"`
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
	rootInputOnly                  bool
	requireExistingExplicitRun     bool
	injectRunIDEntityIDWhenMissing bool
	injectRunIDEntityIDOnlyNewRun  bool
	publishError                   func(eventPublicationParams, error) error
	buildCompletion                func(context.Context, OperatorReadOptions, eventPublicationParams) (any, string, error)
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
		requireExistingExplicitRun:     true,
		injectRunIDEntityIDWhenMissing: true,
		injectRunIDEntityIDOnlyNewRun:  true,
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
		if err := selectedOpts.Events.Publish(ctx, eventPublicationEvent(params, now)); err != nil {
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
		RunID:             runID,
		SourceEventID:     sourceEventID,
		IdempotencyKey:    idempotencyKey,
		Emitter:           emitter,
		NewRunCreated:     newRun,
		RunIDProvided:     runIDProvided,
	}, bundleIdentity, nil
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
	evt := (events.Event{
		ID:            params.EventID,
		RunID:         params.RunID,
		ParentEventID: params.SourceEventID,
		Type:          events.EventType(params.EventName),
		SourceAgent:   params.Emitter,
		Payload:       params.Payload,
		CreatedAt:     createdAt,
	}).WithEntityID(params.EntityID)
	if flowInstance := strings.Trim(strings.TrimSpace(params.FlowInstance), "/"); flowInstance != "" {
		evt = evt.WithFlowInstance(flowInstance)
	}
	return evt
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
	seen := map[eventPublishDelivery]struct{}{}
	for _, delivery := range in {
		if strings.TrimSpace(delivery.SubscriberID) == "__runtime_replay_scope__" {
			continue
		}
		attempt := delivery.RetryCount + 1
		if attempt < 1 {
			attempt = 1
		}
		item := eventPublishDelivery{
			DeliveryID:     strings.TrimSpace(delivery.DeliveryID),
			SubscriberType: strings.TrimSpace(delivery.SubscriberType),
			SubscriberID:   strings.TrimSpace(delivery.SubscriberID),
			SessionID:      strings.TrimSpace(delivery.SessionID),
			Status:         strings.TrimSpace(delivery.Status),
			Attempt:        attempt,
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
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
