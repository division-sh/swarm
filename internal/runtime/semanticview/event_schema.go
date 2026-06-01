package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type EventSchemaResolution struct {
	Schema          runtimecontracts.EventSchema
	EventKey        string
	HasSchema       bool
	UnresolvedTypes []string
}

func ResolveEventSchema(source Source, flowID, eventType string) EventSchemaResolution {
	flowID = strings.TrimSpace(flowID)
	eventType = strings.TrimSpace(eventType)
	if source == nil || eventType == "" {
		return EventSchemaResolution{}
	}
	if bundle, ok := Bundle(source); ok && bundle != nil {
		if schema, key, ok := runtimecontracts.EventSchemaForFlowEvent(bundle, flowID, eventType); ok {
			return EventSchemaResolution{
				Schema:          schema,
				EventKey:        strings.TrimSpace(key),
				HasSchema:       true,
				UnresolvedTypes: UnsupportedJSONSchemaTypes(schema.Schema),
			}
		}
	}
	proof := ResolveFlowEventProof(source, flowID, eventType)
	if !proof.HasSchema {
		return EventSchemaResolution{}
	}
	registry := runtimecontracts.EventSchemaRegistryFromCatalog(map[string]runtimecontracts.EventCatalogEntry{
		proof.CatalogKey: proof.Entry,
	})
	schema, ok := registry[proof.CatalogKey]
	if !ok {
		return EventSchemaResolution{}
	}
	return EventSchemaResolution{
		Schema:          schema,
		EventKey:        proof.EventKey(),
		HasSchema:       true,
		UnresolvedTypes: UnsupportedJSONSchemaTypes(schema.Schema),
	}
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
