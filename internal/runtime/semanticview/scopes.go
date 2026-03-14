package semanticview

import (
	"strings"

	runtimecontracts "empireai/internal/runtime/contracts"
)

type ProjectScope struct {
	Key        string
	Depth      int
	Manifest   runtimecontracts.ProjectPackageDocument
	PromptsDir string
	Nodes      map[string]runtimecontracts.SystemNodeContract
	Events     map[string]runtimecontracts.EventCatalogEntry
	Agents     map[string]runtimecontracts.AgentRegistryEntry
	Tools      map[string]runtimecontracts.ToolSchemaEntry
	Policy     runtimecontracts.PolicyDocument
}

type FlowScope struct {
	ID            string
	Path          string
	Mode          string
	PromptsDir    string
	InputEvents   []string
	OutputEvents  []string
	AutoEmitEvent string
	Nodes         map[string]runtimecontracts.SystemNodeContract
	Events        map[string]runtimecontracts.EventCatalogEntry
	Agents        map[string]runtimecontracts.AgentRegistryEntry
	Tools         map[string]runtimecontracts.ToolSchemaEntry
	Policy        runtimecontracts.PolicyDocument
}

func ProjectScopes(source Source) []ProjectScope {
	if source == nil {
		return nil
	}
	return source.ProjectScopes()
}

func FlowScopes(source Source) []FlowScope {
	if source == nil {
		return nil
	}
	return source.FlowScopes()
}

func FlowScopeByID(source Source, flowID string) (FlowScope, bool) {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return FlowScope{}, false
	}
	return source.FlowScopeByID(flowID)
}

func flowModeFromView(view runtimecontracts.FlowContractView) string {
	if mode := strings.TrimSpace(view.Schema.Mode); mode != "" {
		return mode
	}
	return strings.TrimSpace(view.Paths.Mode)
}

func flowScopeFromView(view runtimecontracts.FlowContractView) FlowScope {
	return FlowScope{
		ID:            strings.TrimSpace(view.Paths.ID),
		Path:          strings.Trim(strings.TrimSpace(view.Path), "/"),
		Mode:          flowModeFromView(view),
		PromptsDir:    strings.TrimSpace(view.Paths.PromptsDir),
		InputEvents:   append([]string{}, view.Schema.Pins.Inputs.Events...),
		OutputEvents:  append([]string{}, view.Schema.Pins.Outputs.Events...),
		AutoEmitEvent: strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event),
		Nodes:         view.Nodes,
		Events:        view.Events,
		Agents:        view.Agents,
		Tools:         view.Tools,
		Policy:        view.Policy,
	}
}
