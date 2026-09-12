package tools

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

type EmitSchema = runtimecontracts.EventSchema

func EmitToolName(eventType string) string {
	eventType = localEmitEventType(eventType)
	return "emit_" + strings.ReplaceAll(strings.TrimSpace(eventType), ".", "_")
}

func localEmitEventType(eventType string) string {
	return eventidentity.LeafName(eventType)
}

func ValidatePayloadAgainstSchema(schema map[string]any, payload map[string]any) error {
	return runtimeeventschema.ValidatePayloadAgainstSchema(schema, payload)
}

func schemaProperties(raw any) map[string]map[string]any {
	return runtimesharedjson.SchemaProperties(raw)
}
func asString(v any) string { return runtimesharedjson.AsString(v) }
