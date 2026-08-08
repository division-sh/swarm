package providerconnectors

import (
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// CompileMockResponsePlan derives the exact deterministic responder catalog
// from every effective provider connector in one semantic source.
func CompileMockResponsePlan(source semanticview.Source) (*MockResponsePlan, error) {
	if source == nil {
		return nil, fmt.Errorf("compile mock connector responses: semantic source is required")
	}
	tools := source.ToolEntries()
	toolIDs := make([]string, 0, len(tools))
	for rawToolID, tool := range tools {
		if isProviderConnector(tool) {
			toolID := strings.TrimSpace(rawToolID)
			if toolID == "" || toolID != rawToolID {
				return nil, fmt.Errorf("compile mock connector responses: effective provider connector tool id %q is not canonical", rawToolID)
			}
			toolIDs = append(toolIDs, toolID)
		}
	}
	sort.Strings(toolIDs)
	if len(toolIDs) == 0 {
		return nil, nil
	}

	responses := make(map[string]any, len(toolIDs))
	for _, toolID := range toolIDs {
		tool := tools[toolID]
		if errs := validateTool(toolID, tool); len(errs) > 0 {
			parts := make([]string, 0, len(errs))
			for _, err := range errs {
				parts = append(parts, err.Error())
			}
			return nil, fmt.Errorf("compile mock connector response for tool %q: %s", toolID, strings.Join(parts, "; "))
		}
		projected, err := tool.OutputSchema().Project()
		if err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: output_schema: %w", toolID, err)
		}
		value, err := eventschema.InhabitDeterministically(projected, eventschema.InhabitationContext{
			Identity: "provider-connector-response-v1\x00" + toolID,
		})
		if err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: output_schema: %w", toolID, err)
		}
		responses[toolID] = value
	}

	plan, err := NewMockResponsePlan(responses)
	if err != nil {
		return nil, fmt.Errorf("compile mock connector response plan: %w", err)
	}
	for _, toolID := range toolIDs {
		if _, err := plan.Admit(toolID, tools[toolID]); err != nil {
			return nil, fmt.Errorf("compile mock connector response for tool %q: generated value failed canonical output_schema validation: %w", toolID, err)
		}
	}
	return plan, nil
}
