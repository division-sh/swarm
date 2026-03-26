package tools

import (
	"encoding/json"
	"sort"
	"strings"

	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

type ContractSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

var supportedRuntimeToolNames = map[string]struct{}{
	"agent_message":        {},
	"schedule":             {},
	"configure_routing":    {},
	"create_flow_instance": {},
	"agent_hire":           {},
	"agent_fire":           {},
	"agent_reconfigure":    {},
	"get_entity":           {},
	"save_entity_field":    {},
	"create_entity":        {},
	"search_entities":      {},
	"query_metrics":        {},
	"mailbox_send":         {},
	"human_task_request":   {},
	"human_task_decide":    {},
	"nginx_reload":         {},
	"systemd_control":      {},
	"certbot_execute":      {},
}

func LoadContractSchemasForSource(source semanticview.Source) (map[string]ContractSchemaEntry, error) {
	parsed := map[string]ContractSchemaEntry{}
	if source == nil {
		return parsed, nil
	}
	for name, entry := range source.ToolEntries() {
		name = strings.TrimSpace(name)
		if _, ok := supportedRuntimeToolNames[name]; !ok {
			continue
		}
		schema := map[string]any{}
		raw, marshalErr := json.Marshal(entry.InputSchema)
		if marshalErr != nil {
			return nil, marshalErr
		}
		if unmarshalErr := json.Unmarshal(raw, &schema); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		parsed[name] = ContractSchemaEntry{
			Category:    entry.Category,
			Description: entry.Description,
			InputSchema: schema,
		}
	}
	return parsed, nil
}

func ContractDefinitionsForSource(source semanticview.Source) ([]llm.ToolDefinition, error) {
	entries, err := LoadContractSchemasForSource(source)
	if err != nil {
		return nil, err
	}
	for name, entry := range builtinRuntimeContractSchemas() {
		entries[name] = entry
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	defs := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		entry := entries[name]
		defs = append(defs, llm.ToolDefinition{
			Name:        name,
			Description: strings.TrimSpace(entry.Description),
			Schema:      deepCloneJSONValue(entry.InputSchema),
		})
	}
	return defs, nil
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
		"create_flow_instance": {
			Category:    "workflow_control",
			Description: "Create a new instance of a template flow at runtime.",
			InputSchema: ObjectSchema(map[string]any{
				"template":    map[string]any{"type": "string"},
				"instance_id": map[string]any{"type": "string"},
				"config": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
			}, "template"),
		},
		"get_entity": {
			Category:    "entity_persistence",
			Description: "Read a typed entity row by entity type and entity id.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"entity_id":   map[string]any{"type": "string"},
			}, "entity_type", "entity_id"),
		},
		"save_entity_field": {
			Category:    "entity_persistence",
			Description: "Write a single validated field on a typed entity row.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"entity_id":   map[string]any{"type": "string"},
				"field":       map[string]any{"type": "string"},
				"value":       anyValueSchema,
			}, "entity_type", "entity_id", "field", "value"),
		},
		"create_entity": {
			Category:    "entity_persistence",
			Description: "Create a new typed entity row with validated fields.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"entity_id":   map[string]any{"type": "string"},
				"fields": map[string]any{
					"type":                 "object",
					"properties":           map[string]any{},
					"additionalProperties": true,
				},
			}, "entity_type", "entity_id"),
		},
		"search_entities": {
			Category:    "entity_persistence",
			Description: "Query typed entity rows using simple field filters.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"filter":      map[string]any{"type": "string"},
				"select": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
				"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 1000},
			}, "entity_type"),
		},
		"query_metrics": {
			Category:    "entity_persistence",
			Description: "Aggregate metrics across typed entity rows.",
			InputSchema: ObjectSchema(map[string]any{
				"entity_type": map[string]any{"type": "string"},
				"metric": map[string]any{
					"type": "string",
					"enum": []any{"count", "sum", "avg", "min", "max"},
				},
				"field":    map[string]any{"type": "string"},
				"group_by": map[string]any{"type": "string"},
				"filter":   map[string]any{"type": "string"},
			}, "entity_type", "metric"),
		},
	}
}
