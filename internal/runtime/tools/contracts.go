package tools

import (
	"encoding/json"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type builtinToolDraft struct {
	Category        string         `yaml:"category"`
	Description     string         `yaml:"description"`
	InputSchema     map[string]any `yaml:"input_schema"`
	OutputSchema    map[string]any `yaml:"output_schema,omitempty"`
	GeneratedSchema bool           `yaml:"-"`
}

var supportedRuntimeToolNames = map[string]struct{}{
	"schedule":          {},
	"get_entity":        {},
	"save_entity_field": {},
	"create_entity":     {},
	"query_entities":    {},
	"search_entities":   {},
	"query_metrics":     {},
	"read_flow_data":    {},
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
	for _, descriptor := range managedHITLToolDescriptors() {
		names[descriptor.name] = struct{}{}
	}
	if source != nil {
		for name, entry := range source.ToolEntries() {
			name = strings.TrimSpace(name)
			if name == "" || runtimeToolHiddenFromAgents(name) {
				continue
			}
			handlerType := entry.Handler()
			if handlerType == runtimecontracts.ToolHandlerUnspecified || handlerType == runtimecontracts.ToolHandlerMCP {
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
