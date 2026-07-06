package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store"
)

const testSetupEntitiesMethod = "test.setup_entities"

type testSetupEntitiesResult struct {
	RunID    string                  `json:"run_id"`
	Entities []testSetupEntityResult `json:"entities"`
}

type testSetupEntityResult struct {
	Alias        string `json:"alias"`
	EntityID     string `json:"entity_id"`
	FlowInstance string `json:"flow_instance,omitempty"`
	EntityType   string `json:"entity_type"`
	CurrentState string `json:"current_state"`
}

func OperatorTestSetupHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if !testSetupConfigured(opts) {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		testSetupEntitiesMethod: func(ctx context.Context, req Request) (any, error) {
			return executeTestSetupEntities(ctx, req, opts, now().UTC())
		},
	}
}

func testSetupConfigured(opts OperatorReadOptions) bool {
	return opts.TestSetup != nil && opts.Idempotency != nil && opts.RunBundleContext != nil
}

func executeTestSetupEntities(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runStartIDempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		request, identity, err := testSetupEntitiesRequestFromParams(req.Params, now)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if strings.TrimSpace(identity.BundleHash) == "" {
			return store.APIIdempotencyCompletion{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{
				"reason": "test.setup_entities requires canonical bundle_hash",
			})
		}
		params := eventPublicationParams{BundleHash: identity.BundleHash, RunID: request.RunID, RunIDProvided: true}
		var selectedOpts OperatorReadOptions
		ctx, selectedOpts, _, err = resolveEventPublicationBundleScope(ctx, opts, params, identity, eventPublicationConfig{})
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		if selectedOpts.TestSetup == nil {
			return store.APIIdempotencyCompletion{}, fmt.Errorf("test setup store is required")
		}
		result, err := selectedOpts.TestSetup.SetupScenarioEntities(ctx, request)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		apiResult := testSetupEntitiesAPIResult(result.Normalized())
		response, err := json.Marshal(apiResult)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{
			ResourceID: strings.TrimSpace(result.RunID),
			Response:   response,
		}, nil
	})
	if err != nil {
		return nil, runStartIdempotencyError(err)
	}
	var result testSetupEntitiesResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		if replay {
			return nil, fmt.Errorf("decode test.setup_entities idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode test.setup_entities response: %w", err)
	}
	return result, nil
}

func testSetupEntitiesRequestFromParams(params map[string]any, now time.Time) (store.ScenarioSetupRequest, bundleIdentityParam, error) {
	identity, err := bundleIdentityInputParam(params)
	if err != nil {
		return store.ScenarioSetupRequest{}, bundleIdentityParam{}, err
	}
	runID, err := requiredUUIDParam(params, "run_id")
	if err != nil {
		return store.ScenarioSetupRequest{}, bundleIdentityParam{}, err
	}
	rawEntities, ok := params["entities"]
	if !ok || isEmptyParam(rawEntities) {
		return store.ScenarioSetupRequest{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "entities", "reason": "is required"})
	}
	list, ok := rawEntities.([]any)
	if !ok || len(list) == 0 {
		return store.ScenarioSetupRequest{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "entities", "reason": "must be a non-empty array"})
	}
	out := store.ScenarioSetupRequest{
		RunID:     runID,
		CreatedAt: now.UTC(),
		Entities:  make([]store.ScenarioSetupEntityRequest, 0, len(list)),
	}
	for i, raw := range list {
		entity, err := testSetupEntityRequestFromParam(raw, i)
		if err != nil {
			return store.ScenarioSetupRequest{}, bundleIdentityParam{}, err
		}
		out.Entities = append(out.Entities, entity)
	}
	return out, identity, nil
}

func testSetupEntityRequestFromParam(raw any, i int) (store.ScenarioSetupEntityRequest, error) {
	entity, ok := raw.(map[string]any)
	if !ok {
		return store.ScenarioSetupEntityRequest{}, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d]", i), "reason": "must be an object"})
	}
	for key := range entity {
		switch key {
		case "alias", "entity_id", "flow_instance", "entity_type", "current_state", "fields", "gates":
		default:
			return store.ScenarioSetupEntityRequest{}, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d].%s", i, key), "reason": "unknown field"})
		}
	}
	alias, err := requiredStringParam(entity, "alias")
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	entityID, err := requiredUUIDParam(entity, "entity_id")
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	flowInstance, _, err := optionalStringParam(entity, "flow_instance")
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	entityType, err := requiredStringParam(entity, "entity_type")
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	currentState, err := requiredStringParam(entity, "current_state")
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	fields, err := testSetupFieldsParam(entity, i)
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	gates, err := testSetupGatesParam(entity, i)
	if err != nil {
		return store.ScenarioSetupEntityRequest{}, err
	}
	return store.ScenarioSetupEntityRequest{
		Alias:        alias,
		EntityID:     entityID,
		FlowInstance: strings.Trim(flowInstance, "/"),
		EntityType:   entityType,
		CurrentState: currentState,
		Fields:       fields,
		Gates:        gates,
	}, nil
}

func testSetupFieldsParam(entity map[string]any, i int) (map[string]any, error) {
	raw, ok := entity["fields"]
	if !ok || raw == nil {
		return map[string]any{}, nil
	}
	fields, ok := raw.(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d].fields", i), "reason": "must be an object"})
	}
	return fields, nil
}

func testSetupGatesParam(entity map[string]any, i int) (map[string]bool, error) {
	raw, ok := entity["gates"]
	if !ok || raw == nil {
		return map[string]bool{}, nil
	}
	gates, ok := raw.(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d].gates", i), "reason": "must be an object"})
	}
	out := make(map[string]bool, len(gates))
	for name, rawValue := range gates {
		value, ok := rawValue.(bool)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d].gates.%s", i, name), "reason": "must be boolean"})
		}
		out[name] = value
	}
	return out, nil
}

func testSetupEntitiesAPIResult(result store.ScenarioSetupResult) testSetupEntitiesResult {
	out := testSetupEntitiesResult{
		RunID:    strings.TrimSpace(result.RunID),
		Entities: make([]testSetupEntityResult, 0, len(result.Entities)),
	}
	for _, entity := range result.Entities {
		out.Entities = append(out.Entities, testSetupEntityResult{
			Alias:        strings.TrimSpace(entity.Alias),
			EntityID:     strings.TrimSpace(entity.EntityID),
			FlowInstance: strings.Trim(strings.TrimSpace(entity.FlowInstance), "/"),
			EntityType:   strings.TrimSpace(entity.EntityType),
			CurrentState: strings.TrimSpace(entity.CurrentState),
		})
	}
	return out
}
