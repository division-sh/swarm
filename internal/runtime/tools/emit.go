package tools

import (
	"sort"
	"strings"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/eventidentity"
	runtimeeventschema "swarm/internal/runtime/eventschema"
	llm "swarm/internal/runtime/llm"
	runtimesharedjson "swarm/internal/runtime/sharedjson"
)

type EmitSchema = runtimecontracts.EventSchema

func EmitToolName(eventType string) string {
	eventType = localEmitEventType(eventType)
	return "emit_" + strings.ReplaceAll(strings.TrimSpace(eventType), ".", "_")
}

func localEmitEventType(eventType string) string {
	return eventidentity.LeafName(eventType)
}

func EventTypeFromEmitToolName(toolName string, toolToEvent map[string]string) (string, bool) {
	toolName = strings.TrimSpace(toolName)
	if !strings.HasPrefix(toolName, "emit_") {
		return "", false
	}
	eventType, ok := toolToEvent[toolName]
	return eventType, ok
}

func GenerateEmitTools(
	role string,
	producerEvents func(string) []string,
	schemaFor func(string) (EmitSchema, bool),
	warn func(string, string, string, ...any),
) []llm.ToolDefinition {
	allowed := producerEvents(role)
	if len(allowed) == 0 {
		return nil
	}
	tools := make([]llm.ToolDefinition, 0, len(allowed))
	for _, eventType := range allowed {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			continue
		}
		schema, ok := schemaFor(eventType)
		if !ok {
			if warn != nil {
				warn(
					"emit-tool-missing-schema-"+eventType,
					"event-schema-registry",
					"skipping emit tool generation for %q because no explicit schema exists",
					eventType,
				)
			}
			continue
		}
		tools = append(tools, llm.ToolDefinition{
			Name:        EmitToolName(eventType),
			Description: schema.Description,
			Schema:      schema.Schema,
		})
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	return tools
}

func ValidatePayloadAgainstSchema(schema map[string]any, payload map[string]any) error {
	return runtimeeventschema.ValidatePayloadAgainstSchema(schema, payload)
}

func schemaProperties(raw any) map[string]map[string]any {
	return runtimesharedjson.SchemaProperties(raw)
}
func schemaAdditionalProps(raw any) bool { return runtimesharedjson.SchemaAdditionalProps(raw) }
func asString(v any) string              { return runtimesharedjson.AsString(v) }
