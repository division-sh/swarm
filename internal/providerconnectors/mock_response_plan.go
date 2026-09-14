package providerconnectors

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

// MockResponsePlan is the immutable exact-tool response catalog compiled from
// one effective semantic source. It is not an authored connector model.
type MockResponsePlan struct {
	responses map[string]semanticvalue.Value
}

type AdmittedMockResponse struct {
	toolID string
	value  semanticvalue.Value
}

func NewMockResponsePlan[T any](responses map[string]T) (*MockResponsePlan, error) {
	plan := &MockResponsePlan{responses: make(map[string]semanticvalue.Value, len(responses))}
	for rawID, response := range responses {
		toolID := strings.TrimSpace(rawID)
		if toolID == "" {
			return nil, fmt.Errorf("mock connector response tool id is required")
		}
		if toolID != rawID {
			return nil, fmt.Errorf("mock connector response tool id %q is not canonical", rawID)
		}
		var value semanticvalue.Value
		var err error
		if raw, ok := any(response).(json.RawMessage); ok {
			value, err = canonicaljson.Decode(raw)
		} else {
			value, err = canonicaljson.FromGo(response)
		}
		if err != nil {
			return nil, fmt.Errorf("admit mock connector response for tool %q: %w", toolID, err)
		}
		plan.responses[toolID] = value
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
	if tool.Handler() != runtimecontracts.ToolHandlerHTTP {
		return AdmittedMockResponse{}, fmt.Errorf("mock HTTP activity %q cannot execute: provider_connector tool must retain its canonical HTTP declaration", toolID)
	}
	if p == nil {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q is not configured; provide an exact deterministic responder", toolID)
	}
	value, ok := p.responses[toolID]
	if !ok {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q is not configured; provide an exact deterministic responder", toolID)
	}
	response, err := workflowexpr.ProjectSemanticValue(value)
	if err != nil {
		return AdmittedMockResponse{}, fmt.Errorf("project mock connector response for tool %q: %w", toolID, err)
	}
	if err := tool.OutputSchema().Validate(response); err != nil {
		return AdmittedMockResponse{}, fmt.Errorf("mock connector response for tool %q does not match output_schema: %w", toolID, err)
	}
	return AdmittedMockResponse{toolID: toolID, value: value}, nil
}

func (r AdmittedMockResponse) Materialize() (any, error) {
	if strings.TrimSpace(r.toolID) == "" {
		return nil, fmt.Errorf("mock connector response was not admitted")
	}
	return workflowexpr.ProjectSemanticValue(r.value)
}
