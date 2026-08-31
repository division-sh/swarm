package semanticview

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
)

type EventSchemaResolution struct {
	Schema            runtimecontracts.EventSchema
	CompiledSchema    runtimecontracts.CompiledEventSchema
	StructuralType    runtimecontracts.ResolvedCatalogType
	Classification    runtimecontracts.CompiledEventSchemaClassification
	EventKey          string
	HasSchema         bool
	HasCompiled       bool
	HasStructural     bool
	HasClassification bool
	UnresolvedTypes   []string
}

func (r EventSchemaResolution) Field(name string) (runtimecontracts.ResolvedCatalogField, bool) {
	if !r.HasStructural {
		return runtimecontracts.ResolvedCatalogField{}, false
	}
	return r.StructuralType.Field(name)
}

func (r EventSchemaResolution) FieldNames() []string {
	if !r.HasStructural {
		return nil
	}
	names := make([]string, 0, len(r.StructuralType.Fields))
	for _, field := range r.StructuralType.Fields {
		if name := strings.TrimSpace(field.Name); name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (r EventSchemaResolution) RequiredFieldNames() []string {
	if !r.HasStructural {
		return nil
	}
	names := make([]string, 0, len(r.StructuralType.Fields))
	for _, field := range r.StructuralType.Fields {
		if name := strings.TrimSpace(field.Name); name != "" && !field.IsOptional {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func ResolveEventSchema(source Source, flowID, eventType string) EventSchemaResolution {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if source == nil || eventType == "" {
		return EventSchemaResolution{}
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		if schema, key, ok := runtimecontracts.EventSchemaForFlowEvent(bundle, flowID, eventType); ok {
			resolution := EventSchemaResolution{
				Schema:          schema,
				EventKey:        strings.TrimSpace(key),
				HasSchema:       true,
				UnresolvedTypes: UnsupportedJSONSchemaTypes(schema.Schema),
			}
			bindCompiledEventSchema(source, bundle, flowID, eventType, &resolution)
			bindEventSchemaClassification(source, flowID, eventType, &resolution)
			bindStructuralEventSchema(&resolution)
			return resolution
		}
	}
	proof := ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema {
		return EventSchemaResolution{}
	}
	// Some diagnostic-direct platform catalog rows reference a separate typed
	// subtype owner instead of declaring an event payload schema. Do not turn
	// that reference-only row into a closed empty-object schema.
	if proof.Entry.Source == "platform_spec" && len(proof.Entry.Payload.Properties) == 0 {
		return EventSchemaResolution{}
	}
	registry := runtimecontracts.EventSchemaRegistryFromCatalog(map[string]runtimecontracts.EventCatalogEntry{
		proof.CatalogKey: proof.Entry,
	})
	schema, ok := registry[proof.CatalogKey]
	if !ok {
		return EventSchemaResolution{}
	}
	resolution := EventSchemaResolution{
		Schema:          schema,
		EventKey:        proof.EventKey(),
		HasSchema:       true,
		UnresolvedTypes: UnsupportedJSONSchemaTypes(schema.Schema),
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		bindCompiledEventSchema(source, bundle, flowID, eventType, &resolution)
	}
	bindEventSchemaClassification(source, flowID, eventType, &resolution)
	bindStructuralEventSchema(&resolution)
	return resolution
}

func bindCompiledEventSchema(source Source, bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string, resolution *EventSchemaResolution) {
	if resolution == nil {
		return
	}
	association := BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint(flowID, eventType)
	if endpoint, ok := association.Endpoint(); ok {
		if pin, found := source.FlowInputEventPin(flowID, endpoint.PinName); found {
			if schema, owned := pin.ReceiverEventSchema(); owned {
				resolution.CompiledSchema = schema
				resolution.HasCompiled = true
				return
			}
			if schema, owned := pin.ProducerEventSchema(); owned {
				resolution.CompiledSchema = schema
				resolution.HasCompiled = true
				return
			}
		}
	}
	if schema, ok, err := bundle.ResolveCompiledFlowEventSchema(flowID, eventType); err == nil && ok {
		resolution.CompiledSchema = schema
		resolution.HasCompiled = true
		return
	}
	proof := ResolveFlowEventProof(source, flowID, eventType)
	for _, candidate := range []string{proof.Local, proof.CatalogKey} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || candidate == strings.TrimSpace(eventType) {
			continue
		}
		if schema, ok, err := bundle.ResolveCompiledFlowEventSchema(flowID, candidate); err == nil && ok {
			resolution.CompiledSchema = schema
			resolution.HasCompiled = true
			return
		}
	}
	var scoped runtimecontracts.CompiledEventSchema
	found := false
	for _, scope := range source.FlowScopes() {
		schema, ok, err := bundle.ResolveCompiledFlowEventSchema(scope.ID, resolution.EventKey)
		if err != nil || !ok {
			continue
		}
		if found && (scoped.PackageKey() != schema.PackageKey() || scoped.EventName() != schema.EventName()) {
			return
		}
		scoped = schema
		found = true
	}
	if found {
		resolution.CompiledSchema = scoped
		resolution.HasCompiled = true
	}
}

func bindEventSchemaClassification(source Source, flowID, eventType string, resolution *EventSchemaResolution) {
	if resolution == nil || !resolution.HasSchema {
		return
	}
	if resolution.HasCompiled {
		resolution.Classification = resolution.CompiledSchema.Classification()
		resolution.HasClassification = true
		return
	}
	proof := ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema {
		return
	}
	switch {
	case strings.Contains(strings.TrimSpace(proof.CatalogKey), "*"):
		resolution.Classification = runtimecontracts.CompiledEventSchemaPattern
	case strings.TrimSpace(proof.Entry.Source) == "platform_spec":
		resolution.Classification = runtimecontracts.CompiledEventSchemaPlatform
	case strings.HasPrefix(strings.TrimSpace(proof.Entry.Source), "contract_derived_activity") || strings.TrimSpace(proof.Entry.Swarm.Status) == "generated":
		resolution.Classification = runtimecontracts.CompiledEventSchemaGenerated
	default:
		return
	}
	resolution.HasClassification = true
}

func bindStructuralEventSchema(resolution *EventSchemaResolution) {
	if resolution == nil {
		return
	}
	if resolution.HasCompiled {
		if structural, ok := resolution.CompiledSchema.StructuralType(); ok {
			resolution.StructuralType = structural
			resolution.HasStructural = true
			return
		}
	}
	if !resolution.HasSchema {
		return
	}
	acceptance := runtimeeventschema.CanonicalAcceptanceSchema(resolution.Schema.Schema)
	canonical, err := canonicaljson.Bytes(acceptance)
	if err != nil {
		return
	}
	structural, err := runtimecontracts.ResolveJSONSchemaStructuralType(acceptance, "event."+canonicaljson.HashBytes(canonical))
	if err != nil {
		return
	}
	resolution.StructuralType = structural
	resolution.HasStructural = true
}

func (r EventSchemaResolution) UnresolvedTypeError() error {
	if len(r.UnresolvedTypes) == 0 {
		return nil
	}
	eventKey := strings.TrimSpace(r.EventKey)
	if eventKey == "" {
		eventKey = "event"
	}
	return fmt.Errorf("%s schema contains unresolved contract type(s): %s", eventKey, strings.Join(r.UnresolvedTypes, ", "))
}

func UnsupportedJSONSchemaTypes(schema map[string]any) []string {
	if len(schema) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	collectUnsupportedJSONSchemaTypes(schema, seen)
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func collectUnsupportedJSONSchemaTypes(schema map[string]any, out map[string]struct{}) {
	if len(schema) == 0 {
		return
	}
	if raw, ok := schema["type"]; ok {
		if typ := strings.TrimSpace(asSchemaString(raw)); typ != "" && !supportedJSONSchemaType(typ) {
			out[typ] = struct{}{}
		}
	}
	if props, ok := schema["properties"].(map[string]any); ok {
		for _, raw := range props {
			if nested, ok := raw.(map[string]any); ok {
				collectUnsupportedJSONSchemaTypes(nested, out)
			}
		}
	}
	if items, ok := schema["items"].(map[string]any); ok {
		collectUnsupportedJSONSchemaTypes(items, out)
	}
}

func supportedJSONSchemaType(typ string) bool {
	switch typ {
	case "object", "array", "string", "number", "integer", "boolean", "null":
		return true
	default:
		return false
	}
}

func asSchemaString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}
