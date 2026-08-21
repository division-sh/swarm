package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeeventidentity "github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func workflowNodeProducerSource(ctx context.Context, source semanticview.Source, node runtimeidentity.ExecutableNode, flowID, entityID string, admittedSource events.RoutingSource) (events.RoutingSource, error) {
	route := admittedSource.Route().Normalized()
	if route.Empty() {
		route = events.RouteIdentity{FlowID: strings.TrimSpace(flowID), EntityID: strings.TrimSpace(entityID)}
	} else if strings.TrimSpace(entityID) != "" {
		route.EntityID = strings.TrimSpace(entityID)
	}
	if application, ok := deliveryTargetApplicationFromContext(ctx); ok {
		if err := application.Validate(); err != nil {
			return events.RoutingSource{}, err
		}
		if strings.TrimSpace(entityID) != application.EntityID() {
			return events.RoutingSource{}, fmt.Errorf("workflow node producer entity disagrees with admitted delivery target application")
		}
		route = application.Owner().Route()
	} else if _, ok := runtimedelivery.RouteFromContext(ctx); ok {
		return events.RoutingSource{}, fmt.Errorf("stamped workflow node producer requires delivery target application")
	}
	return runtimepinrouting.AdmitNodeExecutionRoutingSource(source, node, flowID, route)
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

func actionResultFlowInstanceBelongsToFlow(source semanticview.Source, flowID, flowInstance string) bool {
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	flowPath := actionResultFlowPath(source, flowID)
	if flowInstance == "" || flowPath == "" {
		return false
	}
	if flowInstance == flowPath {
		return true
	}
	scope, ok := semanticview.FlowScopeByID(source, strings.TrimSpace(flowID))
	return ok && strings.EqualFold(strings.TrimSpace(scope.Mode), "template") && strings.HasPrefix(flowInstance, flowPath+"/")
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
