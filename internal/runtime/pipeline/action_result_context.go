package pipeline

import (
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func actionResultProducerRoute(source semanticview.Source, flowID, entityID string, evt events.Event, state runtimeengine.StateSnapshot, admitted events.RouteIdentity) events.RouteIdentity {
	flowID = strings.TrimSpace(flowID)
	entityID = strings.TrimSpace(entityID)
	candidates := []events.RouteIdentity{
		admitted,
		evt.TargetRoute(),
		{
			FlowID:       flowID,
			FlowInstance: asString(state.StateCarrier.Metadata["flow_path"]),
			EntityID:     entityID,
		},
		staticActionResultProducerRoute(source, flowID, entityID),
		{
			FlowID:       flowID,
			FlowInstance: evt.FlowInstance(),
			EntityID:     entityID,
		},
	}
	for _, candidate := range candidates {
		if route, ok := normalizeActionResultProducerRouteCandidate(source, flowID, entityID, candidate); ok {
			return route
		}
	}
	return events.RouteIdentity{
		FlowID:   flowID,
		EntityID: entityID,
	}.Normalized()
}

func normalizeActionResultProducerRouteCandidate(source semanticview.Source, flowID, entityID string, route events.RouteIdentity) (events.RouteIdentity, bool) {
	route = route.Normalized()
	if route.Empty() {
		return events.RouteIdentity{}, false
	}
	if flowID != "" && route.FlowID != "" && route.FlowID != flowID {
		return events.RouteIdentity{}, false
	}
	route.FlowID = firstNonEmptyString(flowID, route.FlowID)
	if entityID != "" {
		route.EntityID = entityID
	}
	if route.FlowInstance == "" {
		if route.FlowID != "" {
			if flowPath := actionResultFlowPath(source, route.FlowID); flowPath != "" {
				return events.RouteIdentity{}, false
			}
		}
		return route.Normalized(), true
	}
	if actionResultFlowInstanceBelongsToFlow(source, flowID, route.FlowInstance) {
		return route.Normalized(), true
	}
	return events.RouteIdentity{}, false
}

func staticActionResultProducerRoute(source semanticview.Source, flowID, entityID string) events.RouteIdentity {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return events.RouteIdentity{}
	}
	scope, ok := source.FlowScopeByID(flowID)
	if ok && strings.EqualFold(strings.TrimSpace(scope.Mode), "template") {
		return events.RouteIdentity{}
	}
	flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
	if flowPath == "" {
		flowPath = strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
	}
	if flowPath == "" {
		return events.RouteIdentity{}
	}
	return events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: flowPath,
		EntityID:     strings.TrimSpace(entityID),
	}.Normalized()
}

func actionResultFlowInstanceBelongsToFlow(source semanticview.Source, flowID, flowInstance string) bool {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		return false
	}
	flowPath := actionResultFlowPath(source, flowID)
	if flowPath == "" {
		return source == nil
	}
	if flowInstance == flowPath {
		return true
	}
	if !actionResultFlowAllowsDescendantInstances(source, flowID) {
		return false
	}
	return strings.HasPrefix(flowInstance, flowPath+"/")
}

func actionResultFlowAllowsDescendantInstances(source semanticview.Source, flowID string) bool {
	if source == nil {
		return true
	}
	scope, ok := source.FlowScopeByID(strings.TrimSpace(flowID))
	if !ok {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(scope.Mode), "template")
}

func actionResultFlowPath(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if source == nil || flowID == "" {
		return ""
	}
	if scope, ok := source.FlowScopeByID(flowID); ok {
		if path := strings.Trim(strings.TrimSpace(scope.Path), "/"); path != "" {
			return path
		}
	}
	if path := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"); path != "" {
		return path
	}
	return flowID
}

func actionResultEventType(source semanticview.Source, flowID, eventType string, producerRoute events.RouteIdentity) string {
	eventType = runtimeeventidentity.Normalize(eventType)
	flowID = strings.TrimSpace(flowID)
	if eventType == "" || source == nil || flowID == "" {
		return eventType
	}
	flowPath := actionResultFlowPath(source, flowID)
	if flowPath == "" {
		return eventType
	}
	localEvent := actionResultLocalFlowEvent(source, flowID, flowPath, producerRoute.FlowInstance, eventType)
	if localEvent == "" {
		return eventType
	}
	namespace := runtimeeventidentity.Normalize(producerRoute.FlowInstance)
	if namespace == "" || !actionResultFlowInstanceBelongsToFlow(source, flowID, namespace) {
		namespace = flowPath
	}
	return namespace + "/" + localEvent
}

func actionResultLocalFlowEvent(source semanticview.Source, flowID, flowPath, flowInstance, eventType string) string {
	scope, ok := semanticview.FlowScopeByID(source, flowID)
	if !ok {
		return ""
	}
	localEvents := actionResultFlowLocalEvents(scope)
	if _, ok := localEvents[eventType]; ok {
		return eventType
	}
	for _, prefix := range []string{flowInstance, flowPath} {
		prefix = runtimeeventidentity.Normalize(prefix)
		if prefix == "" || !strings.HasPrefix(eventType, prefix+"/") {
			continue
		}
		local := strings.TrimPrefix(eventType, prefix+"/")
		if _, ok := localEvents[local]; ok {
			return local
		}
	}
	if resolved := runtimeeventidentity.Normalize(source.ResolveFlowEventReference(flowID, eventType)); resolved != "" && resolved != eventType {
		return actionResultLocalFlowEvent(source, flowID, flowPath, flowInstance, resolved)
	}
	return ""
}

func actionResultFlowLocalEvents(scope semanticview.FlowScope) map[string]struct{} {
	out := make(map[string]struct{}, len(scope.Events)+len(scope.OutputEvents)+1)
	for eventType := range scope.Events {
		if eventType = runtimeeventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	for _, eventType := range scope.OutputEvents {
		if eventType = runtimeeventidentity.Normalize(eventType); eventType != "" {
			out[eventType] = struct{}{}
		}
	}
	if eventType := runtimeeventidentity.Normalize(scope.AutoEmitEvent); eventType != "" {
		out[eventType] = struct{}{}
	}
	return out
}
