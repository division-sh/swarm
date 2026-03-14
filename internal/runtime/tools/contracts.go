package tools

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	"empireai/internal/runtime/semanticview"
)

type ContractSchemaEntry struct {
	Category    string         `yaml:"category"`
	Description string         `yaml:"description"`
	InputSchema map[string]any `yaml:"input_schema"`
}

var (
	contractSchemasOnce sync.Once
	contractSchemas     map[string]ContractSchemaEntry
	contractSchemasErr  error
)

var supportedRuntimeToolNames = map[string]struct{}{
	"agent_message":      {},
	"schedule":           {},
	"configure_routing":  {},
	"agent_hire":         {},
	"agent_fire":         {},
	"agent_reconfigure":  {},
	"get_entity":         {},
	"save_entity_field":  {},
	"create_entity":      {},
	"search_entities":    {},
	"query_metrics":      {},
	"mailbox_send":       {},
	"human_task_request": {},
	"human_task_decide":  {},
	"nginx_reload":       {},
	"systemd_control":    {},
	"certbot_execute":    {},
}

func LoadContractSchemas() (map[string]ContractSchemaEntry, error) {
	contractSchemasOnce.Do(func() {
		bundle, err := runtimecontracts.LoadWorkflowContractBundle(repoRoot())
		if err != nil {
			contractSchemasErr = fmt.Errorf("load workflow contract bundle: %w", err)
			return
		}
		source := semanticview.Wrap(bundle)
		parsed := map[string]ContractSchemaEntry{}
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if _, ok := supportedRuntimeToolNames[name]; !ok {
				continue
			}
			schema := map[string]any{}
			raw, marshalErr := json.Marshal(entry.InputSchema)
			if marshalErr != nil {
				contractSchemasErr = fmt.Errorf("marshal tool schema %s: %w", name, marshalErr)
				return
			}
			if unmarshalErr := json.Unmarshal(raw, &schema); unmarshalErr != nil {
				contractSchemasErr = fmt.Errorf("normalize tool schema %s: %w", name, unmarshalErr)
				return
			}
			parsed[name] = ContractSchemaEntry{
				Category:    entry.Category,
				Description: entry.Description,
				InputSchema: schema,
			}
		}
		contractSchemas = parsed
	})
	if contractSchemasErr != nil {
		return nil, contractSchemasErr
	}
	return contractSchemas, nil
}

func ContractDefinitions() ([]llm.ToolDefinition, error) {
	entries, err := LoadContractSchemas()
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

func repoRoot() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
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
