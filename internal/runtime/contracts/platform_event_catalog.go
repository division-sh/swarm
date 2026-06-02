package contracts

import (
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
	if payload := platformEventMappingValue(node, "payload"); payload != nil {
		entry.Payload.Properties = platformEventPayloadProperties(*payload)
	}
	if required := platformEventStringList(node, "required"); len(required) > 0 {
		entry.Required = required
	} else {
		entry.Required = sortedEventFieldNames(entry.Payload.Properties)
	}
	if consumer := platformEventStringList(node, "consumer"); len(consumer) > 0 {
		entry.Consumer = consumer
	}
	if producer := platformEventStringList(node, "producer"); len(producer) > 0 {
		entry.Producer = producer
	}
	return entry
}

func platformEventPayloadProperties(payload yaml.Node) map[string]EventFieldSpec {
	out := map[string]EventFieldSpec{}
	if payload.Kind != yaml.MappingNode {
		return out
	}
	content := payload.Content
	for i := 0; i+1 < len(content); i += 2 {
		name := strings.TrimSpace(content[i].Value)
		if name == "" || name == "required" || name == "properties" {
			continue
		}
		field := platformEventFieldSpec(*content[i+1])
		if strings.TrimSpace(field.Type) == "" {
			field.Type = "object"
		}
		out[name] = field
	}
	if props := platformEventMappingValue(payload, "properties"); props != nil {
		for name, field := range platformEventPayloadProperties(*props) {
			out[name] = field
		}
	}
	return out
}

func platformEventFieldSpec(node yaml.Node) EventFieldSpec {
	switch node.Kind {
	case yaml.ScalarNode:
		return EventFieldSpec{Type: normalizePlatformEventFieldType(node.Value)}
	case yaml.MappingNode:
		return EventFieldSpec{
			Type:        normalizePlatformEventFieldType(platformEventScalarValue(node, "type")),
			Description: platformEventScalarValue(node, "description"),
		}
	default:
		return EventFieldSpec{Type: "object"}
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
