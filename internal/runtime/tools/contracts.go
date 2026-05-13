package tools

import (
	"encoding/json"
	"sort"
	"strings"

	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

type ContractSchemaEntry struct {
	Category        string         `yaml:"category"`
	Description     string         `yaml:"description"`
	InputSchema     map[string]any `yaml:"input_schema"`
	OutputSchema    map[string]any `yaml:"output_schema,omitempty"`
	GeneratedSchema bool           `yaml:"-"`
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
	"query_entities":     {},
	"search_entities":    {},
	"query_metrics":      {},
	"mailbox_send":       {},
	"human_task_request": {},
	"human_task_decide":  {},
	"read_flow_data":     {},
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

// This is the canonical builtin/non-MCP runtime tool inventory for supported
// verify, boot-warning, and operator-diagnostic surfaces. Authored ToolEntries
// alone are not the full runtime-available tool truth.
func RuntimeAvailableToolNamesForSource(source semanticview.Source) []string {
	names := make(map[string]struct{})
	for name := range supportedRuntimeToolNames {
		name = strings.TrimSpace(name)
		if name == "" || runtimeToolHiddenFromAgents(name) {
			continue
		}
		names[name] = struct{}{}
	}
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" || runtimeToolHiddenFromAgents(name) {
				continue
			}
			handlerType := normalizeImplementationClass(name, entry)
			if handlerType == "" || handlerType == implementationMCP {
				continue
			}
			names[name] = struct{}{}
		}
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
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
