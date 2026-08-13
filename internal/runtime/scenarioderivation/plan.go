package scenarioderivation

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const PlanVersion = "derived-scenario-plan/v1"

type Request struct {
	FlowID    string
	Input     string
	AllInputs bool
	Set       map[string]any
	ProfileID string
	Responses map[string]json.RawMessage
}

type Plan struct {
	Version      string
	FlowID       string
	PinName      string
	EventKey     string
	SchemaDigest string
	Payload      json.RawMessage
	Profile      scenarioexecution.Profile
}

func CompileCatalog(source semanticview.Source, identity scenarioexecution.EffectiveSourceIdentity, declarations ...Declaration) (*scenarioexecution.Catalog, error) {
	if source == nil {
		return nil, fmt.Errorf("derived scenario catalog requires the effective semantic source")
	}
	profiles := make([]scenarioexecution.Profile, 0)
	for _, endpoint := range semanticview.BuildAuthoredEventEndpointCensus(source).InputPins() {
		profile, err := emptyProfile(identity, endpoint)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profile)
	}
	coordinates := map[string]string{}
	for _, declaration := range declarations {
		coordinate := strings.Trim(declaration.FlowID, "/") + "\x00" + strings.TrimSpace(declaration.Input)
		if previous, duplicate := coordinates[coordinate]; duplicate {
			return nil, fmt.Errorf("derived scenario profiles %q and %q declare the same exact flow/input coordinate", previous, declaration.Name)
		}
		coordinates[coordinate] = declaration.Name
		plans, err := Compile(source, identity, Request{
			FlowID: declaration.FlowID, Input: declaration.Input, Set: declaration.Set,
			ProfileID: declaration.Name, Responses: declaration.ConnectorResponses,
		})
		if err != nil {
			return nil, fmt.Errorf("compile derived scenario profile %q: %w", declaration.Name, err)
		}
		profiles = append(profiles, plans[0].Profile)
	}
	return scenarioexecution.NewCatalog(identity, profiles)
}

func Compile(source semanticview.Source, identity scenarioexecution.EffectiveSourceIdentity, request Request) ([]Plan, error) {
	if source == nil {
		return nil, fmt.Errorf("derive requires the effective semantic source")
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	request.FlowID = strings.Trim(strings.TrimSpace(request.FlowID), "/")
	request.Input = strings.TrimSpace(request.Input)
	if request.FlowID == "" {
		return nil, fmt.Errorf("--derive requires an exact flow id")
	}
	if request.AllInputs && request.Input != "" {
		return nil, fmt.Errorf("--input and --all-inputs are mutually exclusive")
	}
	all := semanticview.BuildAuthoredEventEndpointCensus(source).InputPins()
	candidates := make([]semanticview.AuthoredEventEndpoint, 0)
	for _, endpoint := range all {
		if strings.Trim(strings.TrimSpace(endpoint.FlowID), "/") != request.FlowID {
			continue
		}
		if request.Input != "" && request.Input != strings.TrimSpace(endpoint.PinName) {
			continue
		}
		candidates = append(candidates, endpoint)
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].ID < candidates[right].ID })
	if len(candidates) == 0 {
		return nil, fmt.Errorf("flow %q has no public input matching %q", request.FlowID, request.Input)
	}
	if !request.AllInputs && len(candidates) > 1 {
		choices := make([]string, 0, len(candidates))
		for _, endpoint := range candidates {
			choices = append(choices, fmt.Sprintf("%s (event %s)", endpoint.PinName, endpoint.Event.EventKey()))
		}
		return nil, fmt.Errorf("flow %q has multiple public inputs: %s; select one with --input or use --all-inputs", request.FlowID, strings.Join(choices, ", "))
	}
	if !request.AllInputs {
		candidates = candidates[:1]
	}
	plans := make([]Plan, 0, len(candidates))
	for _, endpoint := range candidates {
		plan, err := compileEndpoint(source, identity, endpoint, request)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func compileEndpoint(source semanticview.Source, identity scenarioexecution.EffectiveSourceIdentity, endpoint semanticview.AuthoredEventEndpoint, request Request) (Plan, error) {
	resolution := semanticview.ResolveEventSchema(source, endpoint.FlowID, endpoint.Event.EventKey())
	if !resolution.HasSchema {
		return Plan{}, fmt.Errorf("derive flow %q input %q event %q has no admitted schema", endpoint.FlowID, endpoint.PinName, endpoint.Event.EventKey())
	}
	if err := resolution.UnresolvedTypeError(); err != nil {
		return Plan{}, err
	}
	canonicalSchema := runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema.Schema)
	schemaDigest, err := canonicaljson.Hash(canonicalSchema)
	if err != nil {
		return Plan{}, err
	}
	generated, err := runtimeeventschema.InhabitDeterministically(resolution.Schema.Schema, runtimeeventschema.InhabitationContext{
		Identity: strings.Join([]string{PlanVersion, identity.Digest(), endpoint.FlowID, endpoint.PinName, resolution.EventKey, schemaDigest}, "\x00"),
	})
	if err != nil {
		return Plan{}, fmt.Errorf("derive flow %q input %q: %w", endpoint.FlowID, endpoint.PinName, err)
	}
	payload, ok := generated.(map[string]any)
	if !ok {
		return Plan{}, fmt.Errorf("derive flow %q input %q requires an object event schema, got %T", endpoint.FlowID, endpoint.PinName, generated)
	}
	if len(request.Set) > 0 {
		payload = cloneObject(payload)
		applyObjectOverlay(payload, request.Set)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(resolution.Schema.Schema, payload); err != nil {
		return Plan{}, fmt.Errorf("derive flow %q input %q payload overlay failed canonical validation: %w", endpoint.FlowID, endpoint.PinName, err)
	}
	raw, err := canonicaljson.Bytes(payload)
	if err != nil {
		return Plan{}, err
	}
	profile, err := compileProfile(source, identity, endpoint, request.ProfileID, request.Responses)
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		Version: PlanVersion, FlowID: strings.Trim(strings.TrimSpace(endpoint.FlowID), "/"),
		PinName: strings.TrimSpace(endpoint.PinName), EventKey: strings.TrimSpace(resolution.EventKey),
		SchemaDigest: schemaDigest, Payload: append(json.RawMessage(nil), raw...), Profile: profile,
	}, nil
}

func compileProfile(source semanticview.Source, identity scenarioexecution.EffectiveSourceIdentity, endpoint semanticview.AuthoredEventEndpoint, profileID string, responses map[string]json.RawMessage) (scenarioexecution.Profile, error) {
	if strings.TrimSpace(profileID) == "" {
		return emptyProfile(identity, endpoint)
	}
	ids := make([]string, 0, len(responses))
	for id := range responses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	admitted := make([]scenarioexecution.ConnectorResponse, 0, len(ids))
	tools := source.ToolEntries()
	for _, id := range ids {
		tool, ok := tools[id]
		if !ok {
			return scenarioexecution.Profile{}, fmt.Errorf("scenario connector response references unknown effective tool %q", id)
		}
		digest, err := tool.OutputSchema().CanonicalHash()
		if err != nil {
			return scenarioexecution.Profile{}, fmt.Errorf("scenario connector response tool %q output_schema: %w", id, err)
		}
		admitted = append(admitted, scenarioexecution.ConnectorResponse{ToolID: id, OutputSchemaDigest: digest, Response: responses[id]})
	}
	if _, err := providerconnectors.OverlayMockResponsePlan(nil, source, admitted); err != nil {
		return scenarioexecution.Profile{}, err
	}
	return scenarioexecution.NewProfile(identity, profileID, admitted)
}

func emptyProfile(identity scenarioexecution.EffectiveSourceIdentity, endpoint semanticview.AuthoredEventEndpoint) (scenarioexecution.Profile, error) {
	id := strings.Join([]string{"derived", strings.Trim(strings.TrimSpace(endpoint.FlowID), "/"), strings.TrimSpace(endpoint.PinName)}, "/")
	return scenarioexecution.NewProfile(identity, id, nil)
}

func applyObjectOverlay(target map[string]any, overlay map[string]any) {
	keys := make([]string, 0, len(overlay))
	for key := range overlay {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := overlay[key]
		if nested, ok := value.(map[string]any); ok {
			if current, exists := target[key].(map[string]any); exists {
				applyObjectOverlay(current, nested)
				continue
			}
			target[key] = cloneObject(nested)
			continue
		}
		target[key] = value
	}
}

func cloneObject(input map[string]any) map[string]any {
	raw, _ := json.Marshal(input)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}
