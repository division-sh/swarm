package contracts

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"gopkg.in/yaml.v3"
)

const PlatformEventRedeclarationMessage = "Event %s is platform-emitted and auto-registered; remove the local redeclaration."

func PlatformEventCatalogEntry(platform PlatformSpecDocument, eventType string) (EventCatalogEntry, string, bool) {
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return EventCatalogEntry{}, "", false
	}
	for name, node := range platform.PlatformEvents.Catalog {
		key := eventidentity.Normalize(name)
		if key == "" || key != eventType {
			continue
		}
		return platformEventEntryFromYAMLNode(node), key, true
	}
	return EventCatalogEntry{}, "", false
}

func PlatformEventCatalogContains(platform PlatformSpecDocument, eventType string) bool {
	_, _, ok := PlatformEventCatalogEntry(platform, eventType)
	return ok
}

func PlatformEventCatalogNames(platform PlatformSpecDocument) []string {
	names := make([]string, 0, len(platform.PlatformEvents.Catalog))
	for name := range platform.PlatformEvents.Catalog {
		name = eventidentity.Normalize(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func platformEventEntryFromYAMLNode(node yaml.Node) EventCatalogEntry {
	entry := EventCatalogEntry{
		Source: "platform_spec",
		Swarm:  EventSwarmMetadata{Source: "platform"},
		Payload: EventPayloadSpec{
			Properties: map[string]EventFieldSpec{},
		},
	}
	if node.Kind == 0 {
		return entry
	}
	if node.Kind != yaml.MappingNode {
		return entry
	}
	if source := platformEventScalarValue(node, "produced_by_type"); source != "" {
		entry.EmitterType = source
	}
	if source := platformEventScalarValue(node, "source"); source != "" {
		entry.Source = source
	}
	if status := platformEventScalarValue(node, "status"); status != "" {
		entry.Status = status
	}
	if handling := platformEventScalarValue(node, "runtime_handling"); handling != "" {
		entry.RuntimeHandling = handling
	}
	if platformEventMappingValue(node, "required") != nil {
		panic("platform event required lists are retired; fields are required by default and optional fields use one trailing ? on their type")
	}
	if payload := platformEventMappingValue(node, "payload"); payload != nil {
		entry.Payload.Properties, entry.Payload.Required = platformEventPayloadSchema(*payload, "platform event payload")
	}
	if consumer := platformEventStringList(node, "consumer"); len(consumer) > 0 {
		entry.Consumer = consumer
	}
	if producer := platformEventStringList(node, "producer"); len(producer) > 0 {
		entry.Producer = producer
	}
	return entry
}

func platformEventPayloadSchema(payload yaml.Node, context string) (map[string]EventFieldSpec, []string) {
	out := map[string]EventFieldSpec{}
	if payload.Kind != yaml.MappingNode {
		return out, nil
	}
	if platformEventMappingValue(payload, "required") != nil {
		panic(fmt.Sprintf("%s required lists are retired; fields are required by default and optional fields use one trailing ? on their type", context))
	}
	required := make([]string, 0, len(payload.Content)/2)
	content := payload.Content
	for i := 0; i+1 < len(content); i += 2 {
		name := strings.TrimSpace(content[i].Value)
		if name == "" || name == "required" || name == "properties" {
			continue
		}
		field, optional, err := platformEventFieldSpec(*content[i+1], context+" field "+name)
		if err != nil {
			panic(err)
		}
		if strings.TrimSpace(field.Type) == "" {
			field.Type = "object"
		}
		out[name] = field
		if !optional {
			required = append(required, name)
		}
	}
	if props := platformEventMappingValue(payload, "properties"); props != nil {
		properties, propertyRequired := platformEventPayloadSchema(*props, context+" properties")
		for name, field := range properties {
			out[name] = field
		}
		required = append(required, propertyRequired...)
	}
	sort.Strings(required)
	return out, required
}

func platformEventFieldSpec(node yaml.Node, context string) (EventFieldSpec, bool, error) {
	switch node.Kind {
	case yaml.ScalarNode:
		typeName, optional, err := admitEventFieldTypeMarker(strings.TrimSpace(node.Value), context)
		if err != nil {
			return EventFieldSpec{}, false, err
		}
		return EventFieldSpec{Type: normalizePlatformEventFieldType(typeName)}, optional, nil
	case yaml.MappingNode:
		if platformEventMappingValue(node, "type") == nil {
			properties, required := platformEventPayloadSchema(node, context)
			rawProperties := make(map[string]any, len(properties))
			for name, field := range properties {
				rawProperties[name] = platformEventFieldJSONSchema(field)
			}
			raw := map[string]any{
				"type":                 "object",
				"properties":           rawProperties,
				"required":             required,
				"additionalProperties": false,
			}
			exact, err := AdmitToolInputSchemaMap(raw)
			if err != nil {
				return EventFieldSpec{}, false, fmt.Errorf("admit nested platform event schema: %w", err)
			}
			return EventFieldSpec{Type: "object", ExactSchema: &exact}, false, nil
		}
		typeName, optional, err := admitEventFieldTypeMarker(platformEventScalarValue(node, "type"), context)
		if err != nil {
			return EventFieldSpec{}, false, err
		}
		return EventFieldSpec{
			Type:        normalizePlatformEventFieldType(typeName),
			Description: platformEventScalarValue(node, "description"),
		}, optional, nil
	default:
		return EventFieldSpec{}, false, fmt.Errorf("%s must be a scalar or mapping", context)
	}
}

func platformEventFieldJSONSchema(field EventFieldSpec) map[string]any {
	if field.ExactSchema != nil {
		return field.ExactSchema.Projection()
	}
	typeRef := strings.TrimSpace(field.Type)
	schema, _ := eventSchemaForTypeRef(typeRef, TypeCatalogDocument{}, map[string]struct{}{})
	typeName, _ := schema["type"].(string)
	switch strings.TrimSpace(typeName) {
	case "", "string", "integer", "number", "boolean", "object", "array":
		return schema
	default:
		// Opaque platform-owned snapshot types remain dynamic leaves; their
		// containing field presence is still structurally authoritative.
		return map[string]any{}
	}
}

func normalizePlatformEventFieldType(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "array<") && strings.Contains(raw, ">") {
		inner := strings.TrimSpace(raw[len("array<"):strings.Index(raw, ">")])
		if inner != "" {
			suffix := strings.TrimSpace(raw[strings.Index(raw, ">")+1:])
			if suffix != "" {
				return inner + "[] " + suffix
			}
			return inner + "[]"
		}
	}
	return raw
}

func platformEventScalarValue(node yaml.Node, key string) string {
	if node.Kind != yaml.MappingNode {
		return ""
	}
	if value := platformEventMappingValue(node, key); value != nil && value.Kind == yaml.ScalarNode {
		return strings.TrimSpace(value.Value)
	}
	return ""
}

func platformEventMappingValue(node yaml.Node, key string) *yaml.Node {
	key = strings.TrimSpace(key)
	if node.Kind != yaml.MappingNode || key == "" {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if strings.TrimSpace(node.Content[i].Value) == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func platformEventStringList(node yaml.Node, key string) []string {
	value := platformEventMappingValue(node, key)
	if value == nil {
		return nil
	}
	switch value.Kind {
	case yaml.SequenceNode:
		out := make([]string, 0, len(value.Content))
		for _, item := range value.Content {
			if item == nil {
				continue
			}
			text := strings.TrimSpace(item.Value)
			if text != "" {
				out = append(out, text)
			}
		}
		sort.Strings(out)
		return out
	case yaml.ScalarNode:
		text := strings.TrimSpace(value.Value)
		if text == "" {
			return nil
		}
		return []string{text}
	default:
		return nil
	}
}

func sortedEventFieldNames(fields map[string]EventFieldSpec) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
