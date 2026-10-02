package contracts

import (
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

const PlatformEventRedeclarationMessage = "Event %s is platform-emitted and auto-registered; remove the local redeclaration."

func PlatformEventCatalogEntry(platform PlatformSpecDocument, eventType string) (EventCatalogEntry, string, bool) {
	eventType = eventidentity.Normalize(eventType)
	if eventType == "" {
		return EventCatalogEntry{}, "", false
	}
	if entry, ok := platform.eventCatalog[eventType]; ok {
		return cloneEventCatalogEntry(entry), eventType, true
	}
	return EventCatalogEntry{}, "", false
}

func PlatformEventCatalogContains(platform PlatformSpecDocument, eventType string) bool {
	_, _, ok := PlatformEventCatalogEntry(platform, eventType)
	return ok
}

func PlatformEventCatalogNames(platform PlatformSpecDocument) []string {
	names := make([]string, 0, len(platform.eventCatalog))
	for name := range platform.eventCatalog {
		name = eventidentity.Normalize(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
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
