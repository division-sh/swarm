package providerconnectors

import (
	"encoding/json"
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
)

// MockResponsePlan is the immutable exact-tool response catalog compiled from
// one effective semantic source. It is not an authored connector model.
type MockResponsePlan struct {
	responses map[string]json.RawMessage
}

type AdmittedMockResponse struct {
	toolID string
	raw    json.RawMessage
}

func NewMockResponsePlan(responses map[string]map[string]any) (*MockResponsePlan, error) {
	plan := &MockResponsePlan{responses: make(map[string]json.RawMessage, len(responses))}
	for rawID, response := range responses {
		toolID := strings.TrimSpace(rawID)
		if toolID == "" {
			return nil, fmt.Errorf("mock connector response tool id is required")
		}
		if toolID != rawID {
			return nil, fmt.Errorf("mock connector response tool id %q is not canonical", rawID)
		}
		if response == nil {
			return nil, fmt.Errorf("mock connector response for tool %q is required", toolID)
		}
		raw, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("encode mock connector response for tool %q: %w", toolID, err)
		}
		plan.responses[toolID] = append(json.RawMessage(nil), raw...)
	}
	return plan, nil
}

func (p *MockResponsePlan) Admit(toolID string, tool runtimecontracts.ToolSchemaEntry) (AdmittedMockResponse, error) {
	toolID = strings.TrimSpace(toolID)
	if toolID == "" {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response requires an exact tool id")
	}
	if !isProviderConnector(tool) {
		return AdmittedMockResponse{}, fmt.Errorf("mock HTTP activity %q cannot execute: only provider_connector tools with exact responders are supported", toolID)
	}
	if !strings.EqualFold(strings.TrimSpace(tool.HandlerType), "http") || tool.HTTP == nil {
		return AdmittedMockResponse{}, fmt.Errorf("mock HTTP activity %q cannot execute: provider_connector tool must retain its canonical HTTP declaration", toolID)
	}
	if p == nil {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q is not configured; provide an exact deterministic responder", toolID)
	}
	raw, ok := p.responses[toolID]
	if !ok {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q is not configured; provide an exact deterministic responder", toolID)
	}
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		return AdmittedMockResponse{}, fmt.Errorf("decode mock connector response for tool %q: %w", toolID, err)
	}
	schema := runtimecontracts.ToolInputSchemaJSONSchema(tool.OutputSchema)
	if err := eventschema.ValidatePayloadAgainstSchema(schema, response); err != nil {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q does not match output_schema: %w", toolID, err)
	}
	return AdmittedMockResponse{toolID: toolID, raw: append(json.RawMessage(nil), raw...)}, nil
}

func (r AdmittedMockResponse) Materialize() (map[string]any, error) {
	if strings.TrimSpace(r.toolID) == "" || len(r.raw) == 0 {
		return nil, fmt.Errorf("mock connector response was not admitted")
	}
	var response map[string]any
	if err := json.Unmarshal(r.raw, &response); err != nil {
		return nil, fmt.Errorf("materialize mock connector response for tool %q: %w", r.toolID, err)
	}
	return response, nil
}
