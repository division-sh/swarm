package tools

import (
	"encoding/json"

	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

type ContractSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

var supportedRuntimeToolNames = map[string]struct{}{
	"agent_message":      {},
	"schedule":           {},
	"agent_hire":         {},
	"agent_fire":         {},
	"agent_reconfigure":  {},
	"get_entity":         {},
	"save_entity_field":  {},
	"create_entity":      {},
	"get_subject_status": {},
	"query_entities":     {},
	"search_entities":    {},
	"query_metrics":      {},
	"mailbox_send":       {},
	"human_task_request": {},
	"human_task_decide":  {},
}

func LoadContractSchemasForSource(source semanticview.Source) (map[string]ContractSchemaEntry, error) {
	defs, err := registeredToolsForRuntime(source, nil)
	if err != nil {
		return nil, err
	}
	parsed := make(map[string]ContractSchemaEntry, len(defs))
	for name, entry := range defs {
		if runtimeToolHiddenFromAgents(name) {
			continue
		}
		if entry.HandlerType == implementationMCP {
			continue
		}
		parsed[name] = ContractSchemaEntry{
			Category:    entry.Category,
			Description: entry.Description,
			InputSchema: deepCloneMap(entry.InputSchema),
		}
	}
	return parsed, nil
}

func ContractDefinitionsForSource(source semanticview.Source) ([]llm.ToolDefinition, error) {
	return toolDefinitionsForRuntime(source, nil)
}

func ObjectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{
		"type":                 "object",
		"properties":           properties,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func deepCloneJSONValue(v any) any {
	if v == nil {
		return nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

func builtinRuntimeContractSchemas() map[string]ContractSchemaEntry {
	anyValueSchema := map[string]any{}
	return map[string]ContractSchemaEntry{
		"get_entity": {
			Category:    "entity_persistence",
			Description: "Read a full entity_state row by entity id.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_id": map[string]any{"type": "string"},
			}, "entity_id"),
		},
		"save_entity_field": {
			Category:    "entity_persistence",
			Description: "Write a single field into entity_state.fields JSONB.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_id": map[string]any{"type": "string"},
				"field":     map[string]any{"type": "string"},
				"value":     anyValueSchema,
			}, "entity_id", "field", "value"),
		},
		"create_entity": {
			Category:    "entity_persistence",
			Description: "Create a new entity_state row.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_id":     map[string]any{"type": "string"},
				"subject_id":    map[string]any{"type": "string"},
				"flow_instance": map[string]any{"type": "string"},
				"entity_type":   map[string]any{"type": "string"},
				"name":          map[string]any{"type": "string"},
				"initial_state": map[string]any{"type": "string"},
				"fields": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
			}, "flow_instance"),
		},
		"get_subject_status": {
			Category:    "entity_persistence",
			Description: "Query the lifecycle of a business object across flow-local entities.",
			InputSchema: ObjectSchema(map[string]any{
				"subject_id": map[string]any{"type": "string"},
			}, "subject_id"),
		},
		"query_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows using CEL filtering and optional grouping.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"filter":      map[string]any{"type": "string"},
				"select": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"limit":    map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"group_by": map[string]any{"type": "string"},
			}),
		},
		"search_entities": {
			Category:    "entity_persistence",
			Description: "Query entity_state rows by state, metadata, and JSONB field matches.",
			InputSchema: ObjectSchema(map[string]any{
				"subject_id":    map[string]any{"type": "string"},
				"flow_instance": map[string]any{"type": "string"},
				"entity_type":   map[string]any{"type": "string"},
				"current_state": map[string]any{"type": "string"},
				"filter": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
				"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
				"offset": map[string]any{"type": "integer", "minimum": 0, "maximum": 100000},
			}),
		},
		"query_metrics": {
			Category:    "entity_persistence",
			Description: "Aggregate metrics across entity_state rows.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type":   map[string]any{"type": "string"},
				"flow_instance": map[string]any{"type": "string"},
				"metric": map[string]any{
					"type": "string",
					"enum": []any{"count", "sum", "avg", "min", "max"},
				},
				"field":    map[string]any{"type": "string"},
				"group_by": map[string]any{"type": "string"},
				"filter":   map[string]any{"type": "string"},
			}, "metric"),
		},
	}
}
