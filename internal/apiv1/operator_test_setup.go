package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
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
		if err := validateTestSetupEntitiesAgainstBundle(selectedOpts.Source, request); err != nil {
			return store.APIIdempotencyCompletion{}, err
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

func validateTestSetupEntitiesAgainstBundle(source semanticview.Source, request store.ScenarioSetupRequest) error {
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

func validateTestSetupEntityAgainstBundle(bundle *runtimecontracts.WorkflowContractBundle, entity store.ScenarioSetupEntityRequest, i int) error {
	flowID := strings.Trim(strings.TrimSpace(entity.FlowInstance), "/")
	fieldPrefix := fmt.Sprintf("entities[%d]", i)
	primary, err := bundle.ResolveFlowPrimaryEntity(flowID)
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
	if err := validateTestSetupFieldsAgainstBundle(primary, entity.Fields, fieldPrefix); err != nil {
		return err
	}
	if err := validateTestSetupGatesAgainstBundle(bundle, flowID, entity.Gates, fieldPrefix); err != nil {
		return err
	}
	return nil
}

func validateTestSetupFieldsAgainstBundle(primary runtimecontracts.PrimaryEntityContract, fields map[string]any, fieldPrefix string) error {
	for field, value := range fields {
		decl, ok := primary.Contract.Fields[field]
		if !ok {
			return NewInvalidParamsError(map[string]any{
				"field":       fieldPrefix + ".fields." + field,
				"reason":      "is not declared on the selected entity type",
				"entity_type": primary.EntityType,
			})
		}
		if err := validateTestSetupFieldValue(value, decl.Type, primary.Types); err != nil {
			return NewInvalidParamsError(map[string]any{
				"field":       fieldPrefix + ".fields." + field,
				"reason":      err.Error(),
				"entity_type": primary.EntityType,
				"type":        decl.Type,
			})
		}
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
	for nodeID, node := range bundle.Nodes {
		source, ok := bundle.NodeContractSource(nodeID)
		if !ok || strings.Trim(strings.TrimSpace(source.FlowID), "/") != flowID {
			continue
		}
		for _, gate := range node.GateState.Gates {
			if name := strings.TrimSpace(gate.Name); name != "" {
				out[name] = struct{}{}
			}
		}
	}
	for _, transition := range bundle.DerivedHandlerTransitions() {
		if strings.Trim(strings.TrimSpace(transition.FlowID), "/") != flowID {
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

func validateTestSetupFieldValue(value any, typeRef string, catalog runtimecontracts.TypeCatalogDocument) error {
	return validateTestSetupFieldValueDepth(value, strings.TrimSpace(typeRef), catalog, 0)
}

func validateTestSetupFieldValueDepth(value any, typeRef string, catalog runtimecontracts.TypeCatalogDocument, depth int) error {
	if depth > 16 {
		return fmt.Errorf("type alias cycle or excessive nesting at %s", typeRef)
	}
	if scalar, ok := testSetupScalarDecl(catalog, typeRef); ok {
		return validateTestSetupFieldValueDepth(value, scalar.Base, catalog, depth+1)
	}
	if enum, ok := testSetupEnumDecl(catalog, typeRef); ok {
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("must be string for enum %s", typeRef)
		}
		for _, allowed := range enum.Values {
			if text == allowed {
				return nil
			}
		}
		return fmt.Errorf("must be one of %s", strings.Join(enum.Values, ", "))
	}
	if _, ok := testSetupNamedTypeDecl(catalog, typeRef); ok {
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("must be object for %s", typeRef)
		}
		return nil
	}
	typeRef = strings.TrimSpace(strings.ToLower(typeRef))
	switch {
	case typeRef == "", typeRef == "any", typeRef == "json":
		return nil
	case typeRef == "text" || typeRef == "string" || typeRef == "uuid" || typeRef == "timestamp":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("must be string for %s", typeRef)
		}
		if typeRef == "uuid" {
			if _, err := uuid.Parse(strings.TrimSpace(fmt.Sprint(value))); err != nil {
				return fmt.Errorf("must be UUID")
			}
		}
	case typeRef == "bool" || typeRef == "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("must be boolean")
		}
	case typeRef == "int" || typeRef == "integer":
		if !testSetupValueIsInteger(value) {
			return fmt.Errorf("must be integer")
		}
	case typeRef == "number" || typeRef == "numeric" || typeRef == "float" || typeRef == "double":
		if !testSetupValueIsNumber(value) {
			return fmt.Errorf("must be number")
		}
	case typeRef == "object" || typeRef == "map":
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("must be object")
		}
	case typeRef == "array" || typeRef == "list" || strings.HasPrefix(typeRef, "list<") || strings.HasSuffix(typeRef, "[]"):
		if _, ok := value.([]any); !ok {
			return fmt.Errorf("must be array")
		}
	default:
		return nil
	}
	return nil
}

func testSetupScalarDecl(catalog runtimecontracts.TypeCatalogDocument, name string) (runtimecontracts.ScalarTypeDecl, bool) {
	name = strings.TrimSpace(name)
	if catalog.Scalars == nil || name == "" {
		return runtimecontracts.ScalarTypeDecl{}, false
	}
	if decl, ok := catalog.Scalars[name]; ok {
		return decl, true
	}
	decl, ok := catalog.Scalars[strings.ToLower(name)]
	return decl, ok
}

func testSetupEnumDecl(catalog runtimecontracts.TypeCatalogDocument, name string) (runtimecontracts.EnumTypeDecl, bool) {
	name = strings.TrimSpace(name)
	if catalog.Enums == nil || name == "" {
		return runtimecontracts.EnumTypeDecl{}, false
	}
	if decl, ok := catalog.Enums[name]; ok {
		return decl, true
	}
	decl, ok := catalog.Enums[strings.ToLower(name)]
	return decl, ok
}

func testSetupNamedTypeDecl(catalog runtimecontracts.TypeCatalogDocument, name string) (runtimecontracts.NamedTypeDecl, bool) {
	name = strings.TrimSpace(name)
	if catalog.Types == nil || name == "" {
		return runtimecontracts.NamedTypeDecl{}, false
	}
	if decl, ok := catalog.Types[name]; ok {
		return decl, true
	}
	decl, ok := catalog.Types[strings.ToLower(name)]
	return decl, ok
}

func testSetupValueIsInteger(value any) bool {
	switch typed := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		return typed == float64(int64(typed))
	default:
		return false
	}
}

func testSetupValueIsNumber(value any) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
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
