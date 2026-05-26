package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimerunstart "swarm/internal/runtime/runstart"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store"
)

type eventPublishResult struct {
	EventID       string                 `json:"event_id"`
	RunID         string                 `json:"run_id"`
	SourceEventID string                 `json:"source_event_id,omitempty"`
	NewRunCreated bool                   `json:"new_run_created"`
	Deliveries    []eventPublishDelivery `json:"deliveries"`
}

type eventPublishDelivery struct {
	SubscriberID string `json:"subscriber_id"`
	SessionID    string `json:"session_id,omitempty"`
	Status       string `json:"status"`
	Attempt      int    `json:"attempt"`
}

type eventPublicationParams struct {
	BundleFingerprint string
	EventID           string
	EventName         string
	Payload           json.RawMessage
	EntityID          string
	RunID             string
	SourceEventID     string
	IdempotencyKey    string
	Emitter           string
	NewRunCreated     bool
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
	bootFingerprint := strings.TrimSpace(opts.Bundle.Fingerprint)
	ctx = runtimecorrelation.WithBundleFingerprint(ctx, bootFingerprint)
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
		params, err := eventPublicationParamsFromRequest(req, bootFingerprint, cfg)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if err := validateEventPublication(ctx, opts, params, cfg); err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if err := opts.Events.Publish(ctx, events.Event{
			ID:            params.EventID,
			RunID:         params.RunID,
			ParentEventID: params.SourceEventID,
			Type:          events.EventType(params.EventName),
			SourceAgent:   params.Emitter,
			Payload:       params.Payload,
			CreatedAt:     now,
		}.WithEntityID(params.EntityID)); err != nil {
			if cfg.publishError != nil {
				return store.APIIdempotencyCompletion{}, cfg.publishError(params, err)
			}
			return store.APIIdempotencyCompletion{}, runStartPublishError(params.EventName, err)
		}
		result, resourceID, err := cfg.buildCompletion(ctx, opts, params)
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

func eventPublicationParamsFromRequest(req Request, bootFingerprint string, cfg eventPublicationConfig) (eventPublicationParams, error) {
	eventName := stringParam(req.Params, "event_name")
	if eventName == "" {
		return eventPublicationParams{}, NewInvalidParamsError(map[string]any{"field": "event_name", "reason": "required parameter is missing"})
	}
	bundleIdentity, err := bundleIdentityInputParam(req.Params)
	if err != nil {
		return eventPublicationParams{}, err
	}
	if bundleIdentity.BundleHash != "" {
		return eventPublicationParams{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{
			"reason": "bundle_hash runtime assertions require canonical bundle source facts; use legacy bundle_ref.fingerprint during the #1001 transition",
		})
	}
	fingerprint := bundleIdentity.LegacyFingerprint
	if fingerprint != "" && fingerprint != strings.TrimSpace(bootFingerprint) {
		return eventPublicationParams{}, NewApplicationError(BundleMismatchCode, false, bundleIdentity.mismatchDetails(bootFingerprint))
	}
	runID, _, err := optionalStringParam(req.Params, "run_id")
	if err != nil {
		return eventPublicationParams{}, err
	}
	sourceEventID, sourceEventIDSet, err := optionalStringParam(req.Params, "source_event_id")
	if err != nil {
		return eventPublicationParams{}, err
	}
	if sourceEventIDSet {
		if sourceEventID == "" {
			return eventPublicationParams{}, NewInvalidParamsError(map[string]any{"field": "source_event_id", "reason": "must be a UUID"})
		}
		parsed, err := uuid.Parse(sourceEventID)
		if err != nil {
			return eventPublicationParams{}, NewInvalidParamsError(map[string]any{"field": "source_event_id", "reason": "must be a UUID"})
		}
		sourceEventID = parsed.String()
	}
	if sourceEventID != "" && runID == "" {
		return eventPublicationParams{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "is required when source_event_id is provided"})
	}
	newRun := false
	if runID == "" {
		runID = uuid.NewString()
		newRun = true
	} else if parsed, err := uuid.Parse(runID); err != nil {
		return eventPublicationParams{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "must be a UUID"})
	} else {
		runID = parsed.String()
	}
	injectEntityID := cfg.injectRunIDEntityIDWhenMissing && (!cfg.injectRunIDEntityIDOnlyNewRun || newRun)
	payload, entityID, err := eventPublicationPayload(req.Params, runID, injectEntityID)
	if err != nil {
		return eventPublicationParams{}, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return eventPublicationParams{}, err
	}
	emitter := ""
	if cfg.allowEmitterParam {
		emitter, _, err = optionalStringParam(req.Params, "emitter")
		if err != nil {
			return eventPublicationParams{}, err
		}
	}
	if emitter == "" && cfg.sourceAgent != nil {
		emitter = cfg.sourceAgent(req)
	}
	return eventPublicationParams{
		BundleFingerprint: fingerprint,
		EventID:           uuid.NewString(),
		EventName:         eventName,
		Payload:           payload,
		EntityID:          entityID,
		RunID:             runID,
		SourceEventID:     sourceEventID,
		IdempotencyKey:    idempotencyKey,
		Emitter:           emitter,
		NewRunCreated:     newRun,
	}, nil
}

func eventPublicationPayload(params map[string]any, runID string, injectRunIDEntityID bool) (json.RawMessage, string, error) {
	if params == nil {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	raw, ok := params["payload"]
	if !ok || isEmptyParam(raw) {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, "", NewInvalidParamsError(map[string]any{"field": "payload", "reason": "must be an object"})
	}
	cloned := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		cloned[key] = value
	}
	entityID, supplied, err := runStartPayloadEntityID(cloned["entity_id"])
	if err != nil {
		return nil, "", err
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
		return nil, "", err
	}
	return encoded, entityID, nil
}

func validateEventPublication(ctx context.Context, opts OperatorReadOptions, params eventPublicationParams, cfg eventPublicationConfig) error {
	if cfg.rootInputOnly {
		set, err := runtimerunstart.ValidateInputEvents(opts.Source, []string{params.EventName})
		if err != nil {
			return runStartRootInputError(params.EventName, set, err)
		}
		return nil
	}
	if !eventDeclared(opts.Source, params.EventName) {
		return NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"declared_events": declaredEventNames(opts.Source),
		})
	}
	if cfg.requireExistingExplicitRun && !params.NewRunCreated {
		runs, err := requireRunReadStore(opts.Runs)
		if err != nil {
			return err
		}
		header, err := runs.LoadRunHeader(ctx, params.RunID)
		if errors.Is(err, store.ErrRunNotFound) {
			return NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": params.RunID})
		}
		if err != nil {
			return err
		}
		status := strings.TrimSpace(strings.ToLower(header.Status))
		if status != "running" && status != "paused" {
			return NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{
				"run_id":         params.RunID,
				"current_status": status,
			})
		}
	}
	if params.SourceEventID != "" {
		sourceEvent, err := opts.Observability.LoadOperatorEvent(ctx, params.SourceEventID)
		if errors.Is(err, store.ErrEventNotFound) {
			return NewApplicationError(EventNotFoundCode, false, map[string]any{"event_id": params.SourceEventID})
		}
		if err != nil {
			return err
		}
		if strings.TrimSpace(sourceEvent.RunID) != params.RunID {
			return NewInvalidParamsError(map[string]any{
				"field":           "source_event_id",
				"reason":          "must belong to run_id",
				"source_event_id": params.SourceEventID,
				"run_id":          params.RunID,
				"source_run_id":   strings.TrimSpace(sourceEvent.RunID),
			})
		}
	}
	return nil
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
	for _, delivery := range in {
		if strings.TrimSpace(delivery.SubscriberID) == "__runtime_replay_scope__" {
			continue
		}
		attempt := delivery.RetryCount + 1
		if attempt < 1 {
			attempt = 1
		}
		out = append(out, eventPublishDelivery{
			SubscriberID: strings.TrimSpace(delivery.SubscriberID),
			SessionID:    strings.TrimSpace(delivery.SessionID),
			Status:       strings.TrimSpace(delivery.Status),
			Attempt:      attempt,
		})
	}
	return out
}

func eventDeclared(source semanticview.Source, eventName string) bool {
	eventName = strings.TrimSpace(eventName)
	if source == nil || eventName == "" {
		return false
	}
	if _, ok := source.EventEntry(eventName); ok {
		return true
	}
	for name := range source.ResolvedEventCatalog() {
		if strings.TrimSpace(name) == eventName {
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
		name = strings.TrimSpace(name)
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	for name := range source.ResolvedEventCatalog() {
		name = strings.TrimSpace(name)
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
