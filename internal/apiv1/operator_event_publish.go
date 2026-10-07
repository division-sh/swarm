package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimerunstart "github.com/division-sh/swarm/internal/runtime/runstart"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
)

type eventPublishResult struct {
	EventID                  string                  `json:"event_id"`
	RunID                    string                  `json:"run_id"`
	OperatorReferenceEventID string                  `json:"operator_reference_event_id,omitempty"`
	NewRunCreated            bool                    `json:"new_run_created"`
	Deliveries               []eventPublishDelivery  `json:"deliveries"`
	DataBinding              durabledata.DataBinding `json:"data_binding"`
}

type eventPublishDelivery struct {
	DeliveryID     string                                  `json:"delivery_id"`
	SubscriberType string                                  `json:"subscriber_type"`
	SubscriberID   string                                  `json:"subscriber_id"`
	SessionID      string                                  `json:"session_id,omitempty"`
	Status         string                                  `json:"status"`
	ReasonCode     string                                  `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope               `json:"failure,omitempty"`
	Attempt        int                                     `json:"attempt"`
	RetryCount     int                                     `json:"retry_count"`
	RetryScheduled bool                                    `json:"retry_scheduled"`
	Terminal       bool                                    `json:"terminal"`
	CreatedAt      *time.Time                              `json:"created_at,omitempty"`
	StartedAt      *time.Time                              `json:"started_at,omitempty"`
	FinishedAt     *time.Time                              `json:"finished_at,omitempty"`
	DeadLetters    []operatorread.OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
}

type eventPublicationParams struct {
	SourceArtifactFact     runtimecorrelation.SourceArtifactFact
	EventID                string
	EventName              string
	Payload                json.RawMessage
	EntityID               string
	PayloadEntityIDPresent bool
	FlowInstance           string
	TargetRoute            events.RouteIdentity
	TargetRouteSet         bool
	APIEventEndpoint       *runtimebus.APIEventPublicationEndpoint
	RunID                  string
	SourceEventID          string
	IdempotencyKey         string
	Emitter                string
	NewRunCreated          bool
	RunIDProvided          bool
	ScenarioExecution      *scenarioexecution.Selector
	Data                   durabledata.RunCreationDataEnvelope
	DataPresent            bool
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
	atomicAPICompletion            bool
	publishError                   func(eventPublicationParams, error) error
	buildCompletion                func(context.Context, EventPublicationOptions, eventPublicationParams) (any, string, error)
}

type AcknowledgedEventPublisher interface {
	PublishAcknowledged(context.Context, events.Event) error
}

type apiEventAcknowledgedPublisher interface {
	LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error)
	PublishAPIEventAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion) (apiidempotency.Completion, bool, error)
	PublishAPIEventWithRunCreationAcknowledged(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint, apiidempotency.Request, apiidempotency.Completion, *durabledata.RunCreationCommand) (apiidempotency.Completion, bool, error)
}

func OperatorEventPublishHandlers(opts EventPublishHandlerOptions) map[string]MethodHandler {
	if !eventPublishConfigured(opts.Publication) {
		return nil
	}
	now := opts.Publication.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"event.publish": func(ctx context.Context, req Request) (any, error) {
			return executeEventPublish(ctx, req, opts.Publication, now().UTC())
		},
	}
}

func eventPublishConfigured(opts EventPublicationOptions) bool {
	atomicPublisher, _ := opts.Acknowledged.(apiEventAcknowledgedPublisher)
	return runStartConfigured(RunStartHandlerOptions{Publication: opts}) &&
		opts.Acknowledged != nil && atomicPublisher != nil && opts.RecipientPlans != nil && opts.Runs != nil && opts.Observability != nil
}

func executeEventPublish(ctx context.Context, req Request, opts EventPublicationOptions, now time.Time) (any, error) {
	cfg := eventPublicationConfig{
		sourceAgent:                    eventPublishSourceAgent,
		allowEmitterParam:              true,
		allowExplicitTargetRoute:       true,
		requireExistingExplicitRun:     true,
		injectRunIDEntityIDWhenMissing: true,
		injectRunIDEntityIDOnlyNewRun:  true,
		durablePublishAck:              true,
		atomicAPICompletion:            true,
		publishError:                   eventPublishPublishError,
		buildCompletion: func(_ context.Context, _ EventPublicationOptions, params eventPublicationParams) (any, string, error) {
			return eventPublishResult{
				EventID:                  params.EventID,
				RunID:                    params.RunID,
				OperatorReferenceEventID: params.SourceEventID,
				NewRunCreated:            params.NewRunCreated,
				Deliveries:               []eventPublishDelivery{},
				DataBinding:              durabledata.DataBinding{State: "none"},
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
	return stored, nil
}

func eventPublishStoredResult(completion apiidempotency.Completion) (eventPublishResult, error) {
	var stored eventPublishResult
	if err := json.Unmarshal(completion.Response, &stored); err != nil {
		return eventPublishResult{}, err
	}
	return stored, nil
}

func executeOperatorEventPublication(
	ctx context.Context,
	req Request,
	opts EventPublicationOptions,
	now time.Time,
	cfg eventPublicationConfig,
) (apiidempotency.Completion, bool, error) {
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return apiidempotency.Completion{}, false, err
	}
	idempotency := apiidempotency.Request{
		Method:         req.Method,
		Actor:          apiidempotency.BearerActor(req.ActorTokenID),
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runStartIDempotencyTTL,
		Now:            now,
	}
	if cfg.atomicAPICompletion {
		publisher, ok := opts.Acknowledged.(apiEventAcknowledgedPublisher)
		if !ok || publisher == nil {
			return apiidempotency.Completion{}, false, errors.New("event.publish requires atomic selected-store publication and API completion")
		}
		completion, replay, err := publisher.LookupAPIEventPublication(ctx, idempotency)
		if err != nil || replay {
			return completion, replay, err
		}
	}
	atomicReplay := false
	execute := func(ctx context.Context) (apiidempotency.Completion, error) {
		params, bundleIdentity, err := eventPublicationParamsFromRequest(req, cfg)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		selectedOpts := opts
		ctx, selectedOpts, params, err = resolveEventPublicationBundleScope(ctx, opts, params, bundleIdentity, cfg)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if params.DataPresent && !params.NewRunCreated {
			return apiidempotency.Completion{}, NewApplicationError(string(durabledata.CodeRunDataImmutable), false, map[string]any{"run_id": params.RunID})
		}
		var selectedFeeds []runtimepinrouting.SourceEvent
		if req.Method == "run.start" && params.DataPresent {
			selectedFeeds, err = admitRunStartDeploymentFeeds(selectedOpts.Source, params.Data)
			if err != nil {
				return apiidempotency.Completion{}, err
			}
		}
		ctx, err = admitScenarioExecutionSelector(ctx, selectedOpts, params.RunID, params.NewRunCreated, params.ScenarioExecution)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if cfg.rootInputOnly {
			if _, err := runtimerunstart.ValidateInputEvents(selectedOpts.Source, []string{params.EventName}); err != nil {
				return apiidempotency.Completion{}, rootInputApplicationError(err)
			}
		}
		resolvedEventName, err := resolveEventPublicationEventName(selectedOpts.Source, params.EventName)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		params.EventName = resolvedEventName
		endpoint, err := runtimebus.NewRootInputAPIEventPublicationEndpoint(selectedOpts.Source, resolvedEventName)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		params.APIEventEndpoint = &endpoint
		params, err = validateEventPublication(ctx, selectedOpts, params, cfg)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if cfg.rootInputOnly && params.NewRunCreated {
			if _, err := checkEventPublicationRecipientPlan(ctx, selectedOpts, params, cfg); err != nil {
				return apiidempotency.Completion{}, err
			}
			if err := runtimerunstart.ValidateFinite(selectedOpts.Source, selectedFeeds); err != nil {
				return apiidempotency.Completion{}, finiteRunStartApplicationError(err)
			}
		}
		publication, err := eventPublicationEvent(params, now, selectedOpts.ExecutionPosture)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if cfg.atomicAPICompletion {
			completion, err := eventPublicationCompletion(ctx, selectedOpts, params, cfg)
			if err != nil {
				return apiidempotency.Completion{}, err
			}
			publisher, ok := selectedOpts.Acknowledged.(apiEventAcknowledgedPublisher)
			if !ok || publisher == nil {
				return apiidempotency.Completion{}, errors.New("event.publish requires atomic selected-store publication and API completion")
			}
			var runCreation *durabledata.RunCreationCommand
			if params.NewRunCreated {
				semanticRequest, semanticErr := runCreationInitialEvent(params)
				if semanticErr != nil {
					return apiidempotency.Completion{}, semanticErr
				}
				command := durabledata.RunCreationCommand{
					RunID: params.RunID, Actor: req.ActorTokenID, BundleHash: params.SourceArtifactFact.BundleHash(),
					EventID: params.EventID, InitialEvent: semanticRequest, Data: params.Data,
				}
				runCreation = &command
			}
			committed, replay, err := publisher.PublishAPIEventWithRunCreationAcknowledged(ctx, publication, params.APIEventEndpoint, idempotency, completion, runCreation)
			if err != nil {
				if errors.Is(err, apiidempotency.ErrConflict) {
					return apiidempotency.Completion{}, err
				}
				if cfg.publishError != nil {
					return apiidempotency.Completion{}, cfg.publishError(params, err)
				}
				return apiidempotency.Completion{}, eventCatalogPublishError(params.EventName, err)
			}
			atomicReplay = replay
			return committed, nil
		}
		if err := publishEventPublication(ctx, selectedOpts, publication, cfg); err != nil {
			if cfg.publishError != nil {
				return apiidempotency.Completion{}, cfg.publishError(params, err)
			}
			return apiidempotency.Completion{}, eventCatalogPublishError(params.EventName, err)
		}
		return eventPublicationCompletion(ctx, selectedOpts, params, cfg)
	}
	if cfg.atomicAPICompletion {
		completion, err := execute(ctx)
		return completion, atomicReplay, err
	}
	return opts.Idempotency.WithAPIIdempotency(ctx, idempotency, execute)
}

func eventPublicationCompletion(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, cfg eventPublicationConfig) (apiidempotency.Completion, error) {
	result, resourceID, err := cfg.buildCompletion(ctx, opts, params)
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	response, err := json.Marshal(result)
	if err != nil {
		return apiidempotency.Completion{}, err
	}
	return apiidempotency.Completion{ResourceID: resourceID, Response: response}, nil
}

func publishEventPublication(ctx context.Context, opts EventPublicationOptions, evt events.Event, cfg eventPublicationConfig) error {
	if cfg.durablePublishAck {
		return opts.Acknowledged.PublishAcknowledged(ctx, evt)
	}
	return opts.Events.Publish(ctx, evt)
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
	scenarioSelector, err := scenarioExecutionSelectorParam(req.Params)
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	data, dataPresent, err := runCreationDataEnvelopeParam(req.Method, req.Params)
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
	if runIDProvided && (cfg.rootInputOnly || dataPresent) {
		newRun = true
	}
	if dataPresent && !runIDProvided {
		return eventPublicationParams{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "run_id", "reason": "is required for data-bearing create-new publication"})
	}
	payload, payloadEntityIDPresent, err := eventPublicationPayload(req.Params)
	if err != nil {
		return eventPublicationParams{}, bundleIdentityParam{}, err
	}
	entityID := ""
	if cfg.injectRunIDEntityIDWhenMissing && (!cfg.injectRunIDEntityIDOnlyNewRun || newRun) {
		entityID = strings.TrimSpace(runID)
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
	eventID := uuid.NewString()
	if newRun {
		eventID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.run.initial-event.v1\x00"+runID)).String()
	}
	return eventPublicationParams{
		EventID:                eventID,
		EventName:              eventName,
		Payload:                payload,
		EntityID:               entityID,
		PayloadEntityIDPresent: payloadEntityIDPresent,
		TargetRoute:            targetRoute,
		TargetRouteSet:         targetRouteSet,
		RunID:                  runID,
		SourceEventID:          sourceEventID,
		IdempotencyKey:         idempotencyKey,
		Emitter:                emitter,
		NewRunCreated:          newRun,
		RunIDProvided:          runIDProvided,
		ScenarioExecution:      scenarioSelector,
		Data:                   data,
		DataPresent:            dataPresent,
	}, bundleIdentity, nil
}

func runCreationDataEnvelopeParam(method string, params map[string]any) (durabledata.RunCreationDataEnvelope, bool, error) {
	raw, present := params["data"]
	if !present {
		return durabledata.RunCreationDataEnvelope{}, false, nil
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": "data", "reason": "must be an object"})
	}
	if err := exactDataParams(object, "imports", "pins"); err != nil {
		return durabledata.RunCreationDataEnvelope{}, true, err
	}
	importsRaw, ok := object["imports"].([]any)
	if !ok {
		return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": "data.imports", "reason": "must be an array"})
	}
	pinsRaw, ok := object["pins"].([]any)
	if !ok {
		return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": "data.pins", "reason": "must be an array"})
	}
	if len(importsRaw) > durabledata.MaxDataDeclarationsPerBundle || len(pinsRaw) > durabledata.MaxDataDeclarationsPerBundle {
		return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": "data", "reason": fmt.Sprintf("imports and pins are capped at %d items each", durabledata.MaxDataDeclarationsPerBundle)})
	}
	envelope := durabledata.RunCreationDataEnvelope{Imports: make([]durabledata.FusedImport, 0, len(importsRaw)), Pins: make([]durabledata.ExplicitPin, 0, len(pinsRaw))}
	totalBytes := 0
	for index, rawItem := range importsRaw {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("data.imports[%d]", index), "reason": "must be an object"})
		}
		if err := exactDataParams(item, "source_invocation_id", "declaration", "expected_head", "input"); err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		id, err := canonicalDataUUID(item["source_invocation_id"], fmt.Sprintf("data.imports[%d].source_invocation_id", index))
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		declaration, err := dataDeclarationRef(item["declaration"])
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		expected, err := dataExpectedHead(item["expected_head"])
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		input, err := dataInput(item["input"])
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		totalBytes += len(input)
		envelope.Imports = append(envelope.Imports, durabledata.FusedImport{SourceInvocationID: id, Declaration: declaration, ExpectedHead: expected, InputFormat: "jsonl", Input: input})
	}
	if totalBytes > durabledata.MaxDecodedImportBytes {
		return durabledata.RunCreationDataEnvelope{}, true, NewApplicationError("MESSAGE_BUDGET_EXCEEDED", false, map[string]any{"boundary": "aggregate_import", "method": method, "limit_bytes": durabledata.MaxDecodedImportBytes, "observed_bytes": totalBytes, "receipt_created": false})
	}
	for index, rawItem := range pinsRaw {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("data.pins[%d]", index), "reason": "must be an object"})
		}
		if err := exactDataParams(item, "declaration", "version_id"); err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		declaration, err := dataDeclarationRef(item["declaration"])
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		versionID, err := dataVersionID(item["version_id"], fmt.Sprintf("data.pins[%d].version_id", index))
		if err != nil {
			return durabledata.RunCreationDataEnvelope{}, true, err
		}
		envelope.Pins = append(envelope.Pins, durabledata.ExplicitPin{Declaration: declaration, VersionID: versionID})
	}
	canonical, err := envelope.Canonical()
	if err != nil {
		return durabledata.RunCreationDataEnvelope{}, true, NewInvalidParamsError(map[string]any{"field": "data", "reason": err.Error()})
	}
	return canonical, true, nil
}

func runCreationInitialEvent(params eventPublicationParams) (json.RawMessage, error) {
	request := map[string]any{
		"event_name": params.EventName, "payload": json.RawMessage(params.Payload), "emitter": params.Emitter,
		"entity_id": params.EntityID, "flow_instance": params.FlowInstance, "source_event_id": params.SourceEventID,
	}
	if params.TargetRouteSet {
		request["target"] = params.TargetRoute
	}
	if params.ScenarioExecution != nil {
		request["scenario_execution"] = *params.ScenarioExecution
	}
	return canonicaljson.Bytes(request)
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
	route.FlowID = runtimeflowidentity.SemanticScopeFromFlowInstanceRef(route.FlowInstance)
	if route.FlowID == "" {
		return events.RouteIdentity{}, true, NewInvalidParamsError(map[string]any{"field": "target.flow_instance", "reason": "must be a canonical instance path"})
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

func eventPublicationPayload(params map[string]any) (json.RawMessage, bool, error) {
	if params == nil {
		return nil, false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	raw, ok := params["payload"]
	if !ok || isEmptyParam(raw) {
		return nil, false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "required parameter is missing"})
	}
	payload, ok := raw.(map[string]any)
	if !ok {
		return nil, false, NewInvalidParamsError(map[string]any{"field": "payload", "reason": "must be an object"})
	}
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	entityID, supplied := cloned["entity_id"]
	payloadEntityIDPresent := supplied && !isEmptyParam(entityID)
	admitted, err := canonicaljson.FromGo(cloned)
	if err != nil {
		return nil, false, err
	}
	projected, err := workflowexpr.ProjectSemanticValue(admitted)
	if err != nil {
		return nil, false, err
	}
	encoded, err := canonicaljson.MarshalPreservingNumberKinds(projected)
	if err != nil {
		return nil, false, err
	}
	return encoded, payloadEntityIDPresent, nil
}

func eventPublicationEvent(params eventPublicationParams, createdAt time.Time, posture executionposture.Posture) (events.Event, error) {
	if !posture.Valid() {
		var event events.Event
		return event, fmt.Errorf("runtime execution posture is required")
	}
	envelope := events.EventEnvelope{EntityID: params.EntityID}
	if flowInstance := strings.Trim(strings.TrimSpace(params.FlowInstance), "/"); flowInstance != "" {
		envelope.FlowInstance = flowInstance
	}
	if params.TargetRouteSet {
		envelope = events.EnvelopeForTargetRoute(envelope, params.TargetRoute)
	}
	routingSource, err := events.NewRootRoutingSource(params.RunID)
	if err != nil {
		var event events.Event
		return event, fmt.Errorf("public input routing source: %w", err)
	}
	facts := events.EventFacts{
		ID: params.EventID, Type: events.EventType(params.EventName),
		Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: params.Emitter},
		Payload:  params.Payload, Envelope: envelope, CreatedAt: createdAt, ExecutionMode: posture.RootMode(),
		RoutingSource: routingSource,
	}
	if params.NewRunCreated {
		return events.NewRunCreatingRootIngressEvent(events.RunCreatingRootIngressEventInput{Facts: facts, RunID: params.RunID})
	}
	var provenance *events.OperatorReferenceProvenance
	if sourceEventID := strings.TrimSpace(params.SourceEventID); sourceEventID != "" {
		provenance, err := events.NewOperatorReferenceProvenance(sourceEventID)
		if err != nil {
			var event events.Event
			return event, err
		}
		return events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{Facts: facts, RunID: params.RunID, Provenance: &provenance})
	}
	return events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{Facts: facts, RunID: params.RunID, Provenance: provenance})
}

func validateEventPublication(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, cfg eventPublicationConfig) (eventPublicationParams, error) {
	if cfg.rootInputOnly {
		_, err := runtimerunstart.ValidateInputEvents(opts.Source, []string{params.EventName})
		if err != nil {
			return params, rootInputApplicationError(err)
		}
	} else if !eventDeclared(opts.Source, params.EventName) {
		return params, NewApplicationError(EventNotDeclaredCode, false, map[string]any{
			"event_name":      params.EventName,
			"declared_events": declaredEventNames(opts.Source),
		})
	}
	constructorOwnsIdentity := (params.PayloadEntityIDPresent || params.TargetRouteSet) &&
		eventPublicationHasKeyedConstructor(opts.Source, params.EventName, params.TargetRoute, params.RunID)
	if params.PayloadEntityIDPresent && constructorOwnsIdentity {
		return params, NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$.entity_id",
				"rule":       "constructor_owns_entity_id",
				"message":    "caller-supplied entity_id is not allowed for a creating input",
			}},
			"event_name": params.EventName,
		})
	}
	if params.TargetRouteSet && constructorOwnsIdentity {
		return params, NewApplicationError(PayloadValidationFailedCode, false, map[string]any{
			"violations": []map[string]any{{
				"field_path": "$.target.entity_id",
				"rule":       "constructor_owns_entity_id",
				"message":    "caller-supplied target entity_id is not allowed for a creating input",
			}},
			"event_name": params.EventName,
		})
	}
	if cfg.rootInputOnly {
		return params, nil
	}
	if cfg.requireExistingExplicitRun && !params.NewRunCreated {
		runs, err := requireRunReadStore(opts.Runs)
		if err != nil {
			return params, err
		}
		header, err := runs.LoadRunHeader(ctx, params.RunID)
		if errors.Is(err, operatorread.ErrRunNotFound) {
			return params, NewApplicationError(RunNotFoundCode, false, map[string]any{"run_id": params.RunID})
		}
		if err != nil {
			return params, err
		}
		status := strings.TrimSpace(header.Status)
		state, stateErr := runtimerunlifecycle.ParseState(status)
		if stateErr != nil || !state.Active() {
			return params, NewApplicationError(RunAlreadyTerminalCode, false, map[string]any{
				"run_id":         params.RunID,
				"current_status": status,
			})
		}
	}
	if params.SourceEventID != "" {
		sourceEvent, err := opts.Observability.LoadOperatorEvent(ctx, params.SourceEventID)
		if errors.Is(err, operatorread.ErrEventNotFound) {
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

func enrichExistingRunEventPublicationRoute(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams) (eventPublicationParams, error) {
	if params.TargetRouteSet {
		return enrichExistingRunEventPublicationTargetRoute(ctx, opts, params)
	}
	if strings.TrimSpace(params.EntityID) == "" {
		return enrichExistingRunEventPublicationPrimaryEntity(ctx, opts, params)
	}
	entities, err := requireEntityReadStore(opts.Entities)
	if err != nil {
		return params, err
	}
	entity, err := entities.LoadOperatorEntity(ctx, params.EntityID, params.RunID)
	if errors.Is(err, operatorread.ErrEntityNotFound) {
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

func enrichExistingRunEventPublicationPrimaryEntity(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams) (eventPublicationParams, error) {
	entities, err := requireEntityReadStore(opts.Entities)
	if err != nil {
		return params, nil
	}
	entity, err := entities.LoadOperatorEntity(ctx, params.RunID, params.RunID)
	if errors.Is(err, operatorread.ErrEntityNotFound) {
		return params, nil
	}
	if err != nil {
		return params, err
	}
	params.EntityID = strings.TrimSpace(entity.Entity.EntityID)
	params.FlowInstance = strings.Trim(strings.TrimSpace(entity.Entity.FlowInstance), "/")
	return params, nil
}

func enrichExistingRunEventPublicationTargetRoute(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams) (eventPublicationParams, error) {
	target := params.TargetRoute.Normalized()
	if target.EntityID == "" || target.FlowInstance == "" {
		return params, NewInvalidParamsError(map[string]any{"field": "target", "reason": "flow_instance and entity_id are required"})
	}
	entities, err := requireEntityReadStore(opts.Entities)
	if err != nil {
		return params, err
	}
	entity, err := entities.LoadOperatorEntity(ctx, target.EntityID, params.RunID)
	if errors.Is(err, operatorread.ErrEntityNotFound) {
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

type EventRecipientPlanChecker interface {
	CheckPublishRecipientPlan(context.Context, events.Event) (runtimebus.PublishRecipientPlan, error)
}

type apiEventRecipientPlanChecker interface {
	CheckAPIEventPublishRecipientPlan(context.Context, events.Event, *runtimebus.APIEventPublicationEndpoint) (runtimebus.PublishRecipientPlan, error)
}

func checkEventPublicationRecipientPlan(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, cfg eventPublicationConfig) (runtimebus.PublishRecipientPlan, error) {
	checker := opts.RecipientPlans
	if checker == nil {
		return runtimebus.PublishRecipientPlan{}, NewApplicationError(EventPublishFailedCode, true, map[string]any{
			"event_name": params.EventName,
			"event_id":   params.EventID,
			"run_id":     params.RunID,
			"phase":      "publish",
			"reason":     "recipient planning unavailable: event publisher does not expose subscribed recipient planning",
		})
	}
	publication, err := eventPublicationEvent(params, time.Time{}, opts.ExecutionPosture)
	if err != nil {
		return runtimebus.PublishRecipientPlan{}, err
	}
	var plan runtimebus.PublishRecipientPlan
	if params.APIEventEndpoint != nil {
		apiChecker, ok := checker.(apiEventRecipientPlanChecker)
		if !ok {
			return runtimebus.PublishRecipientPlan{}, NewApplicationError(EventPublishFailedCode, true, map[string]any{
				"event_name": params.EventName,
				"event_id":   params.EventID,
				"run_id":     params.RunID,
				"phase":      "publish",
				"reason":     "recipient planning unavailable: event publisher does not expose typed API endpoint planning",
			})
		}
		plan, err = apiChecker.CheckAPIEventPublishRecipientPlan(ctx, publication, params.APIEventEndpoint)
	} else {
		plan, err = checker.CheckPublishRecipientPlan(ctx, publication)
	}
	if err != nil {
		if cfg.publishError != nil {
			return runtimebus.PublishRecipientPlan{}, cfg.publishError(params, err)
		}
		return runtimebus.PublishRecipientPlan{}, eventCatalogPublishError(params.EventName, err)
	}
	return plan, nil
}

func validateExistingRunEventPublicationRecipientPlan(ctx context.Context, opts EventPublicationOptions, params eventPublicationParams, cfg eventPublicationConfig) error {
	plan, err := checkEventPublicationRecipientPlan(ctx, opts, params, cfg)
	if err != nil {
		return err
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

func eventPublicationHasKeyedConstructor(source semanticview.Source, eventName string, target events.RouteIdentity, runID string) bool {
	if source == nil {
		return false
	}
	eventName = runtimeeventidentity.Normalize(eventName)
	target = target.Normalized()
	if target.Empty() || target.FlowID == "." && target.FlowInstance == runID {
		if pin, found := semanticview.SelectedRootInputPin(source, eventName); found {
			constructor, err := pipeline.CompileFlowConstructor(source, ".", pin.EventType())
			if err == nil && constructor.Eligible() {
				return true
			}
		}
	}
	outputPin, ok := semanticview.SelectedRootOutputPin(source, eventName)
	if !ok {
		return false
	}
	for _, plan := range runtimepinrouting.CompileConnectGraph(source).PlansFromOutputPin(".", outputPin) {
		if !plan.AcceptsReceiverTarget(target, runID) {
			continue
		}
		key := plan.InstanceKey()
		if key == nil || key.Mode() != runtimecontracts.FlowInputResolutionModeCreate {
			continue
		}
		constructor, err := pipeline.CompileFlowConstructor(source, plan.ReceiverRoute("", "").FlowID, string(plan.ReceiverLocalEvent()))
		if err == nil && constructor.Eligible() {
			return true
		}
	}
	return false
}

func rootInputApplicationError(err error) error {
	diagnostic, ok := runtimerunstart.AsRootInputValidationError(err)
	if !ok {
		return err
	}
	return NewApplicationError(EventNotDeclaredCode, false, map[string]any{
		"event_name":      diagnostic.EventName,
		"declared_events": append([]string{}, diagnostic.Inputs.Declared...),
		"routable_events": append([]string{}, diagnostic.Inputs.Routable...),
		"reason":          string(diagnostic.Reason),
	})
}

func eventPublishSourceAgent(req Request) string {
	actor := strings.TrimSpace(req.ActorTokenID)
	if actor == "" {
		actor = "anonymous"
	}
	return "cli-publish:" + actor
}

func eventPublishPublishError(params eventPublicationParams, err error) error {
	mapped := eventCatalogPublishError(params.EventName, err)
	var appErr *ApplicationError
	if errors.As(mapped, &appErr) {
		return mapped
	}
	return eventPublishFailureError(params, err, true)
}

func runStartEventPublishError(params eventPublicationParams, err error) error {
	mapped := publicationApplicationError(params.EventName, err)
	var appErr *ApplicationError
	if errors.As(mapped, &appErr) {
		return mapped
	}
	return eventPublishFailureError(params, err, !errors.Is(err, runtimebus.ErrInvalidEventType))
}

func eventPublishFailureError(params eventPublicationParams, err error, retryable bool) error {
	var terminal *pipeline.TerminalReceiverError
	if errors.As(err, &terminal) {
		return NewApplicationError(EventPublishFailedCode, false, map[string]any{
			"event_name": params.EventName,
			"event_id":   params.EventID,
			"run_id":     params.RunID,
			"phase":      "publish",
			"reason":     "receiver_entity_terminal",
			"flow_id":    terminal.FlowID,
			"stage":      terminal.Stage,
		})
	}
	return NewApplicationError(EventPublishFailedCode, retryable, map[string]any{
		"event_name": params.EventName,
		"event_id":   params.EventID,
		"run_id":     params.RunID,
		"phase":      "publish",
		"reason":     strings.TrimSpace(err.Error()),
	})
}

func eventPublishDeliveries(in []operatorread.OperatorEventDelivery) []eventPublishDelivery {
	out := make([]eventPublishDelivery, 0, len(in))
	seen := map[string]struct{}{}
	for _, delivery := range in {
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

func eventPublishDeliveryFromStore(delivery operatorread.OperatorEventDelivery) eventPublishDelivery {
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
		Failure:        runtimefailures.CloneEnvelope(delivery.Failure),
		Attempt:        attempt,
		RetryCount:     delivery.RetryCount,
		RetryScheduled: delivery.RetryScheduled,
		Terminal:       delivery.Terminal,
		CreatedAt:      cloneTimePtr(delivery.CreatedAt),
		StartedAt:      cloneTimePtr(delivery.StartedAt),
		FinishedAt:     cloneTimePtr(delivery.FinishedAt),
		DeadLetters:    append([]operatorread.OperatorDeadLetterRecord(nil), delivery.DeadLetters...),
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
	_, ok := semanticview.SelectedRootInputPin(source, eventName)
	return ok
}

func declaredEventNames(source semanticview.Source) []string {
	var names []string
	for _, endpoint := range semanticview.SelectedRootInputEndpoints(source) {
		names = append(names, endpoint.Event.Canonical)
	}
	sort.Strings(names)
	return names
}

func resolveEventPublicationEventName(source semanticview.Source, eventName string) (string, error) {
	if eventDeclared(source, eventName) {
		return eventName, nil
	}
	return "", NewApplicationError(EventNotDeclaredCode, false, map[string]any{
		"event_name":      eventName,
		"declared_events": declaredEventNames(source),
		"reason":          "selected_root_input_required",
	})
}
