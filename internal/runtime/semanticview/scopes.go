package semanticview

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type FlowScope struct {
	ID            string
	OwningFlowID  string
	Path          string
	Mode          string
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

// RootExecutionFlowID returns the canonical selected-root flow identity.
func RootExecutionFlowID(source Source) string {
	if source == nil {
		return ""
	}
	bundle, ok := Bundle(source)
	if ok && bundle != nil && bundle.FlowTree.Root != nil {
		return "."
	}
	return ""
}

// RootExecutionCoordinate is the exact root receiver identity for one run.
// The authored flow ID and run ID are independent facts and must agree
// together; UUID shape alone never establishes root ownership.
type RootExecutionCoordinate struct {
	flowID string
	runID  string
}

func AdmitRootExecutionCoordinate(source Source, runID string) (RootExecutionCoordinate, error) {
	flowID := strings.TrimSpace(RootExecutionFlowID(source))
	runID = strings.TrimSpace(runID)
	if flowID == "" || runID == "" {
		return RootExecutionCoordinate{}, fmt.Errorf("root execution coordinate requires authored flow and current run identity")
	}
	return RootExecutionCoordinate{flowID: flowID, runID: runID}, nil
}

func (c RootExecutionCoordinate) Valid() bool {
	return strings.TrimSpace(c.flowID) != "" && strings.TrimSpace(c.runID) != ""
}

func (c RootExecutionCoordinate) FlowID() string {
	if !c.Valid() {
		return ""
	}
	return c.flowID
}

func (c RootExecutionCoordinate) RunID() string {
	if !c.Valid() {
		return ""
	}
	return c.runID
}

func (c RootExecutionCoordinate) Matches(flowID, runID string) bool {
	return c.Valid() && strings.TrimSpace(flowID) == c.flowID && strings.TrimSpace(runID) == c.runID
}

func flowModeFromView(view runtimecontracts.FlowContractView) string {
	if mode := strings.TrimSpace(view.Schema.Mode); mode != "" {
		return mode
	}
	return runtimecontracts.FlowModeStatic
}

func owningFlowIDFromView(view *runtimecontracts.FlowContractView) string {
	if view == nil {
		return ""
	}
	if flowID := strings.TrimSpace(view.Paths.FlowPath); flowID != "" {
		return flowID
	}
	for parent := view.Parent; parent != nil; parent = parent.Parent {
		if flowID := strings.TrimSpace(parent.Paths.FlowPath); flowID != "" {
			return flowID
		}
	}
	return ""
}

func flowScopeFromView(view runtimecontracts.FlowContractView, inputEvents, outputEvents []string) FlowScope {
	return FlowScope{
		ID:            strings.TrimSpace(view.Paths.FlowPath),
		OwningFlowID:  owningFlowIDFromView(&view),
		Path:          strings.Trim(strings.TrimSpace(view.Path), "/"),
		Mode:          flowModeFromView(view),
		InputEvents:   append([]string{}, inputEvents...),
		OutputEvents:  append([]string{}, outputEvents...),
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
