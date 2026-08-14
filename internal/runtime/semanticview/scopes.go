package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type ProjectScope struct {
	Key          string
	OwningFlowID string
	Depth        int
	Manifest     runtimecontracts.ProjectPackageDocument
	Nodes        map[string]runtimecontracts.SystemNodeContract
	Events       map[string]runtimecontracts.EventCatalogEntry
	Agents       map[string]runtimecontracts.AgentRegistryEntry
	AgentURIs    map[string]string
	Tools        map[string]runtimecontracts.ToolSchemaEntry
	Policy       runtimecontracts.PolicyDocument
}

type FlowScope struct {
	ID            string
	OwningFlowID  string
	Path          string
	PackageKey    string
	Mode          string
	DataDir       string
	InputEvents   []string
	OutputEvents  []string
	AutoEmitEvent string
	Nodes         map[string]runtimecontracts.SystemNodeContract
	Events        map[string]runtimecontracts.EventCatalogEntry
	Agents        map[string]runtimecontracts.AgentRegistryEntry
	AgentURIs     map[string]string
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

// RootExecutionFlowID returns the authored flow scope used to execute root
// handlers. Durable declaration identities may still represent root as an
// explicit empty FlowID.
func RootExecutionFlowID(source Source) string {
	if source == nil {
		return ""
	}
	bundle, ok := Bundle(source)
	if ok && bundle != nil && bundle.FlowTree.Root != nil {
		root := bundle.FlowTree.Root
		for _, candidate := range []string{root.Paths.ID, root.Paths.Flow, root.Path, root.Schema.Name} {
			if candidate = strings.Trim(strings.TrimSpace(candidate), "/"); candidate != "" {
				return candidate
			}
		}
	}
	return strings.TrimSpace(source.WorkflowName())
}

func flowModeFromView(view runtimecontracts.FlowContractView) string {
	if mode := strings.TrimSpace(view.Schema.Mode); mode != "" {
		return mode
	}
	return strings.TrimSpace(view.Paths.Mode)
}

func owningFlowIDFromView(view *runtimecontracts.FlowContractView) string {
	if view == nil {
		return ""
	}
	if flowID := strings.TrimSpace(view.Paths.ID); flowID != "" {
		return flowID
	}
	for parent := view.Parent; parent != nil; parent = parent.Parent {
		if flowID := strings.TrimSpace(parent.Paths.ID); flowID != "" {
			return flowID
		}
	}
	return ""
}

func flowScopeFromView(view runtimecontracts.FlowContractView) FlowScope {
	return FlowScope{
		ID:            strings.TrimSpace(view.Paths.ID),
		OwningFlowID:  owningFlowIDFromView(&view),
		Path:          strings.Trim(strings.TrimSpace(view.Path), "/"),
		PackageKey:    strings.TrimSpace(view.Paths.PackageKey),
		Mode:          flowModeFromView(view),
		DataDir:       strings.TrimSpace(view.Paths.DataDir),
		InputEvents:   append([]string{}, view.Schema.Pins.Inputs.Events...),
		OutputEvents:  append([]string{}, view.Schema.Pins.Outputs.Events...),
		AutoEmitEvent: strings.TrimSpace(view.Schema.AutoEmitOnCreate.Event),
		Nodes:         view.Nodes,
		Events:        view.Events,
		Agents:        runtimecontracts.EffectiveAgentRegistryEntries(view.Agents),
		AgentURIs:     cloneStringMap(view.AgentURIs),
		Tools:         toolEntryMapSnapshot(view.Tools),
		Policy:        view.Policy,
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
