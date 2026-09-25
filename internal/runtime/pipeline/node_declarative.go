package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Event = events.Event
type SystemNodeEventHandler = runtimecontracts.SystemNodeEventHandler

func canonicalHandlerMaterializationTarget(source semanticview.Source, flowID string, handler SystemNodeEventHandler, evt Event, blueprint events.RouteIdentity) (events.RouteIdentity, error) {
	blueprint = blueprint.Normalized()
	if blueprint.FlowInstance == "" {
		return events.RouteIdentity{}, fmt.Errorf("materializing handler requires an exact receiver flow instance")
	}
	want := blueprint
	want.FlowID = strings.TrimSpace(flowID)
	want.EntityID = runtimeflowidentity.EntityID(blueprint.FlowInstance)
	if blueprint.EntityID != "" && blueprint.EntityID != want.EntityID {
		return events.RouteIdentity{}, fmt.Errorf("materializing target entity %q disagrees with canonical future entity %q", blueprint.EntityID, want.EntityID)
	}
	return want.Normalized(), nil
}

func ensureHandlerEntityIDAtNode(source semanticview.Source, node identity.ExecutableNode, handlerEvent events.EventType, flowID string, handler SystemNodeEventHandler, entityID string, evt Event) (string, Event, error) {
	entityID = strings.TrimSpace(firstNonEmptyString(entityID, evt.TargetRoute().EntityID))
	if entityID != "" {
		if strings.TrimSpace(evt.EntityID()) == "" {
			resolved, err := events.ResolveEnvelope(evt, events.EnvelopeForEntityID(evt.NormalizedEnvelope(), entityID))
			if err != nil {
				return "", evt, err
			}
			evt = resolved
		}
		return entityID, evt, nil
	}
	if !handlerExecutionEntityRequirementForNode(source, node, handlerEvent, flowID, handler).materializes() {
		return "", evt, nil
	}
	route, err := canonicalHandlerRoute(source, flowID, "", evt)
	if err != nil {
		return "", evt, err
	}
	entityID = runtimeflowidentity.EntityID(route.InstancePath)
	envelope := events.EnvelopeForFlowInstance(evt.NormalizedEnvelope(), route.InstancePath)
	envelope = events.EnvelopeForEntityID(envelope, entityID)
	resolved, err := events.ResolveEnvelope(evt, envelope)
	if err != nil {
		return "", evt, err
	}
	return entityID, resolved, nil
}

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
			if schema, ok := source.FlowSchemaByID(flowID); ok && strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
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

func prepareHandlerMaterializationStateAtNode(source semanticview.Source, node identity.ExecutableNode, handlerEvent events.EventType, flowID string, handler SystemNodeEventHandler, route runtimeflowidentity.Route, entityID string, state *WorkflowState) error {
	if state == nil || !handlerExecutionEntityRequirementForNode(source, node, handlerEvent, flowID, handler).materializes() {
		return nil
	}
	if !route.Valid() {
		return fmt.Errorf("materializing handler requires an exact workflow instance route")
	}
	state.Metadata = workflowMaterializeEntityFields(source, flowID, state.Metadata)
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	if existing := strings.Trim(strings.TrimSpace(state.Control.FlowPath), "/"); existing != "" && existing != route.InstancePath {
		return fmt.Errorf("materializing handler flow_path %q disagrees with exact value %q", existing, route.InstancePath)
	}
	if existing := strings.TrimSpace(state.Control.InstanceID); existing != "" && existing != route.InstanceID {
		return fmt.Errorf("materializing handler instance_id %q disagrees with exact value %q", existing, route.InstanceID)
	}
	entityType, err := requireWorkflowEntityType(source, flowID)
	if err != nil {
		return err
	}
	if existing := strings.TrimSpace(state.Control.EntityType); existing != "" && existing != entityType {
		return fmt.Errorf("materializing handler entity_type %q disagrees with canonical contract %q", existing, entityType)
	}
	state.Control.FlowPath = route.InstancePath
	state.Control.StorageRef = route.InstancePath
	state.Control.InstanceID = route.InstanceID
	state.Control.EntityType = entityType
	state.EntityID = strings.TrimSpace(entityID)
	if strings.TrimSpace(string(state.Stage)) == "" {
		initialStage, err := workflowInitialStateForFlow(source, flowID)
		if err != nil {
			return err
		}
		state.Stage = NormalizeWorkflowStateID(initialStage)
	}
	return nil
}

func gateSpecName(spec *runtimecontracts.GateSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.Name)
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

func workflowDataWritesEntityFields(spec runtimecontracts.WorkflowDataAccumulation, allowedFields map[string]struct{}) bool {
	for _, write := range spec.Writes {
		targetField := normalizeEntityWriteTarget(write.Target())
		if targetField == "" {
			continue
		}
		if _, ok := allowedFields[targetField]; ok {
			return true
		}
	}
	return false
}

func computeStoresEntityField(spec *runtimecontracts.ComputeSpec, allowedFields map[string]struct{}) bool {
	if spec == nil {
		return false
	}
	targetField := normalizeEntityWriteTarget(spec.StoreAs)
	if targetField == "" {
		return false
	}
	_, ok := allowedFields[targetField]
	return ok
}

func normalizeEntityWriteTarget(target string) string {
	path, entityTarget, err := entityruntime.EntityWritePath(target)
	if err != nil || !entityTarget {
		return ""
	}
	field, _, _ := strings.Cut(path, ".")
	return strings.TrimSpace(field)
}
