package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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

func OperatorTestSetupHandlers(opts TestSetupHandlerOptions) map[string]MethodHandler {
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

func testSetupConfigured(opts TestSetupHandlerOptions) bool {
	if opts.Setup == nil || opts.Idempotency == nil || opts.RunBundleContext == nil {
		return false
	}
	return runtimeContextManager(opts.RuntimeContexts) != nil || (opts.Source != nil && opts.SourceArtifact != nil)
}

func executeTestSetupEntities(ctx context.Context, req Request, opts TestSetupHandlerOptions, now time.Time) (any, error) {
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            runStartIDempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		request, identity, err := testSetupEntitiesRequestFromParams(req.Params, now)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if strings.TrimSpace(identity.BundleHash) == "" {
			return apiidempotency.Completion{}, NewApplicationError(UnsupportedBundleHashCode, false, map[string]any{
				"reason": "test.setup_entities requires canonical bundle_hash",
			})
		}
		params := eventPublicationParams{RunID: request.RunID, RunIDProvided: true}
		scope := EventPublicationOptions{
			RunBundleContext:          opts.RunBundleContext,
			RuntimeContexts:           opts.RuntimeContexts,
			SourceArtifact:            opts.SourceArtifact,
			Source:                    opts.Source,
			ExecutionPosture:          opts.ExecutionPosture,
			EffectiveSourceIdentity:   opts.EffectiveSourceIdentity,
			ScenarioProfileCatalog:    opts.ScenarioProfileCatalog,
			ScenarioExecutionProfiles: opts.ScenarioExecutionProfiles,
		}
		var selectedScope EventPublicationOptions
		ctx, selectedScope, _, err = resolveEventPublicationBundleScope(ctx, scope, params, identity, eventPublicationConfig{})
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		selector, err := scenarioExecutionSelectorParam(req.Params)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		ctx, err = admitScenarioExecutionSelector(ctx, selectedScope, request.RunID, true, selector)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		if profile, ok := scenarioexecution.AdmittedProfileFromContext(ctx); ok {
			request.ScenarioExecutionProfile = &profile
		}
		if err := validateTestSetupEntitiesAgainstBundle(selectedScope.Source, request); err != nil {
			return apiidempotency.Completion{}, err
		}
		result, err := opts.Setup.SetupScenarioEntities(ctx, request)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		apiResult := testSetupEntitiesAPIResult(result.Normalized())
		response, err := json.Marshal(apiResult)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{
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

func testSetupEntitiesRequestFromParams(params map[string]any, now time.Time) (runtimepipeline.ScenarioSetupRequest, bundleIdentityParam, error) {
	identity, err := bundleIdentityInputParam(params)
	if err != nil {
		return runtimepipeline.ScenarioSetupRequest{}, bundleIdentityParam{}, err
	}
	runID, err := requiredUUIDParam(params, "run_id")
	if err != nil {
		return runtimepipeline.ScenarioSetupRequest{}, bundleIdentityParam{}, err
	}
	rawEntities, ok := params["entities"]
	if !ok || isEmptyParam(rawEntities) {
		return runtimepipeline.ScenarioSetupRequest{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "entities", "reason": "is required"})
	}
	list, ok := rawEntities.([]any)
	if !ok || len(list) == 0 {
		return runtimepipeline.ScenarioSetupRequest{}, bundleIdentityParam{}, NewInvalidParamsError(map[string]any{"field": "entities", "reason": "must be a non-empty array"})
	}
	out := runtimepipeline.ScenarioSetupRequest{
		RunID:     runID,
		CreatedAt: now.UTC(),
		Entities:  make([]runtimepipeline.ScenarioSetupEntityRequest, 0, len(list)),
	}
	for i, raw := range list {
		entity, err := testSetupEntityRequestFromParam(raw, i)
		if err != nil {
			return runtimepipeline.ScenarioSetupRequest{}, bundleIdentityParam{}, err
		}
		out.Entities = append(out.Entities, entity)
	}
	return out, identity, nil
}

func testSetupEntityRequestFromParam(raw any, i int) (runtimepipeline.ScenarioSetupEntityRequest, error) {
	entity, ok := raw.(map[string]any)
	if !ok {
		return runtimepipeline.ScenarioSetupEntityRequest{}, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d]", i), "reason": "must be an object"})
	}
	for key := range entity {
		switch key {
		case "alias", "entity_id", "flow_instance", "entity_type", "current_state", "fields", "gates":
		default:
			return runtimepipeline.ScenarioSetupEntityRequest{}, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("entities[%d].%s", i, key), "reason": "unknown field"})
		}
	}
	alias, err := requiredStringParam(entity, "alias")
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	entityID, err := requiredUUIDParam(entity, "entity_id")
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	flowInstance, _, err := optionalStringParam(entity, "flow_instance")
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	entityType, err := requiredStringParam(entity, "entity_type")
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	currentState, err := requiredStringParam(entity, "current_state")
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	fields, err := testSetupFieldsParam(entity, i)
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	gates, err := testSetupGatesParam(entity, i)
	if err != nil {
		return runtimepipeline.ScenarioSetupEntityRequest{}, err
	}
	return runtimepipeline.ScenarioSetupEntityRequest{
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

func testSetupEntitiesAPIResult(result runtimepipeline.ScenarioSetupResult) testSetupEntitiesResult {
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

func validateTestSetupEntitiesAgainstBundle(source semanticview.Source, request runtimepipeline.ScenarioSetupRequest) error {
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		return NewInvalidParamsError(map[string]any{
			"field":  "bundle_hash",
			"reason": "test.setup_entities requires a loaded contract bundle source",
		})
	}
	for i, entity := range request.Entities {
		if err := validateTestSetupEntityAgainstBundle(bundle, entity, i); err != nil {
			return err
		}
	}
	return nil
}

func validateTestSetupEntityAgainstBundle(bundle *runtimecontracts.WorkflowContractBundle, entity runtimepipeline.ScenarioSetupEntityRequest, i int) error {
	flowID := strings.Trim(strings.TrimSpace(entity.FlowInstance), "/")
	if flowID == "" {
		flowID = "."
	}
	fieldPrefix := fmt.Sprintf("entities[%d]", i)
	primary, err := bundle.ResolveTestSetupPrimaryEntity(flowID, entity.EntityType)
	if err != nil {
		return NewInvalidParamsError(map[string]any{
			"field":  fieldPrefix + ".flow_instance",
			"reason": err.Error(),
		})
	}
	if strings.TrimSpace(entity.EntityType) != strings.TrimSpace(primary.EntityType) {
		return NewInvalidParamsError(map[string]any{
			"field":         fieldPrefix + ".entity_type",
			"reason":        "must match the primary entity type for the selected flow",
			"flow_instance": flowID,
			"declared_type": primary.EntityType,
			"provided_type": entity.EntityType,
		})
	}
	currentState := strings.TrimSpace(entity.CurrentState)
	if currentState == "" || !testSetupStringSliceContains(bundle.FlowStates(flowID), currentState) {
		return NewInvalidParamsError(map[string]any{
			"field":         fieldPrefix + ".current_state",
			"reason":        "must be a declared state for the selected flow",
			"flow_instance": flowID,
			"state":         currentState,
		})
	}
	if err := validateTestSetupFieldsAgainstBundle(flowID, primary, entity.Fields, fieldPrefix); err != nil {
		return err
	}
	if err := validateTestSetupGatesAgainstBundle(bundle, flowID, entity.Gates, fieldPrefix); err != nil {
		return err
	}
	return nil
}

func validateTestSetupFieldsAgainstBundle(flowID string, primary runtimecontracts.PrimaryEntityContract, fields map[string]any, fieldPrefix string) error {
	contract := entityruntime.Contract{
		FlowID:     strings.Trim(strings.TrimSpace(flowID), "/"),
		EntityType: strings.TrimSpace(primary.EntityType),
		Entity:     primary.Contract,
		Types:      primary.Types,
	}
	for field, value := range fields {
		normalized, err := entityruntime.NormalizeFieldValue(contract, field, value)
		if err != nil {
			return NewInvalidParamsError(map[string]any{
				"field":       fieldPrefix + ".fields." + field,
				"reason":      err.Error(),
				"entity_type": primary.EntityType,
			})
		}
		fields[field] = normalized
	}
	return nil
}

func validateTestSetupGatesAgainstBundle(bundle *runtimecontracts.WorkflowContractBundle, flowID string, gates map[string]bool, fieldPrefix string) error {
	declared := declaredTestSetupGateNames(bundle, flowID)
	for gate := range gates {
		if _, ok := declared[gate]; !ok {
			return NewInvalidParamsError(map[string]any{
				"field":         fieldPrefix + ".gates." + gate,
				"reason":        "is not declared for the selected flow",
				"flow_instance": flowID,
			})
		}
	}
	return nil
}

func declaredTestSetupGateNames(bundle *runtimecontracts.WorkflowContractBundle, flowID string) map[string]struct{} {
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	out := map[string]struct{}{}
	for _, record := range bundle.ScopedNodeRecords() {
		node, err := record.Identity()
		if err != nil || strings.Trim(node.FlowPath(), "/") != flowID {
			continue
		}
		for _, gate := range record.Entry.GateState.Gates {
			if name := strings.TrimSpace(gate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
	}
	for _, transition := range bundle.DerivedHandlerTransitions() {
		if strings.Trim(transition.Node.FlowPath(), "/") != flowID {
			continue
		}
		if transition.SetsGate != nil {
			if name := strings.TrimSpace(transition.SetsGate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
		for _, gate := range transition.ClearGates {
			gate = strings.TrimSpace(gate)
			if gate != "" && gate != "*" {
				out[gate] = struct{}{}
			}
		}
	}
	return out
}

func testSetupStringSliceContains(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
