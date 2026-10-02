package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Event = events.Event
type SystemNodeEventHandler = runtimecontracts.SystemNodeEventHandler

func canonicalHandlerRoute(source semanticview.Source, flowID, statePath string, evt Event) (runtimeflowidentity.Route, error) {
	flowID = strings.TrimSpace(flowID)
	statePath = strings.Trim(strings.TrimSpace(statePath), "/")
	if target := evt.TargetRoute().Normalized(); target.FlowInstance != "" && (target.FlowID == "" || target.FlowID == flowID) {
		return workflowInstanceRouteForExecution(source, flowID, target.FlowInstance)
	}
	if flowID != "" {
		if source != nil && flowID == strings.TrimSpace(semanticview.RootExecutionFlowID(source)) {
			return workflowInstanceRouteForExecution(source, flowID, evt.RunID())
		}
		if statePath != "" {
			return workflowInstanceRouteForExecution(source, flowID, statePath)
		}
		if source != nil {
			if schema, ok := source.FlowSchemaByID(flowID); ok && strings.EqualFold(strings.TrimSpace(schema.EffectiveMode()), "template") {
				return workflowInstanceRouteForExecution(source, flowID, evt.FlowInstance())
			}
		}
		return workflowInstanceRouteForExecution(source, flowID, "")
	}
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		return workflowInstanceRouteForPath(runID)
	}
	return runtimeflowidentity.Route{}, fmt.Errorf("materializing handler requires an exact workflow instance route")
}

func handlerExecutionStateSnapshot(handler SystemNodeEventHandler, entityID string, state WorkflowState, workflowName string, workflowVersion string) (runtimeengine.StateSnapshot, error) {
	snapshot := runtimeengine.StateSnapshot{
		EntityID:        identity.NormalizeEntityID(entityID),
		WorkflowName:    strings.TrimSpace(workflowName),
		WorkflowVersion: strings.TrimSpace(workflowVersion),
		StateCarrier: runtimeengine.NewStateCarrier(
			nil,
			nil,
			map[string]map[string]any{},
		),
	}
	snapshot.StateCarrier.Control = state.Control
	snapshot.CurrentState = strings.TrimSpace(string(state.Stage))
	snapshot.StateCarrier.Fields = cloneStringAnyMap(state.Metadata)
	return snapshot, nil
}
