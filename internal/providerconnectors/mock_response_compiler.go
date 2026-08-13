package providerconnectors

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
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

// OverlayMockResponsePlan admits exact scenario-local replacements through
// the same tool/output owner as the generated base plan.
func OverlayMockResponsePlan(base *MockResponsePlan, source semanticview.Source, responses []scenarioexecution.ConnectorResponse) (*MockResponsePlan, error) {
	if source == nil {
		return nil, fmt.Errorf("scenario mock response overlay requires the effective semantic source")
	}
	merged := map[string]json.RawMessage{}
	if base != nil {
		for toolID, response := range base.responses {
			merged[toolID] = append(json.RawMessage(nil), response...)
		}
	}
	tools := source.ToolEntries()
	for _, response := range responses {
		tool, ok := tools[response.ToolID]
		if !ok {
			return nil, fmt.Errorf("scenario mock response references unknown effective tool %q", response.ToolID)
		}
		outputDigest, err := tool.OutputSchema().CanonicalHash()
		if err != nil {
			return nil, fmt.Errorf("scenario mock response tool %q output_schema: %w", response.ToolID, err)
		}
		if outputDigest != response.OutputSchemaDigest {
			return nil, fmt.Errorf("scenario mock response tool %q output_schema digest mismatch: profile=%s effective=%s", response.ToolID, response.OutputSchemaDigest, outputDigest)
		}
		candidate, err := NewMockResponsePlan(map[string]json.RawMessage{response.ToolID: response.Response})
		if err != nil {
			return nil, err
		}
		if _, err := candidate.Admit(response.ToolID, tool); err != nil {
			return nil, fmt.Errorf("scenario mock response tool %q: %w", response.ToolID, err)
		}
		merged[response.ToolID] = append(json.RawMessage(nil), response.Response...)
	}
	return NewMockResponsePlan(merged)
}
