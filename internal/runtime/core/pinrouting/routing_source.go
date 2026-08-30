package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// AdmitNodeExecutionRoutingSource admits the exact source fact at the node
// execution boundary. Downstream event producers copy this value unchanged.
func AdmitNodeExecutionRoutingSource(source semanticview.Source, node runtimeidentity.ExecutableNode, executionFlowID string, route events.RouteIdentity) (events.RoutingSource, error) {
	if source == nil {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires semantic source")
	}
	executionFlowID = strings.TrimSpace(executionFlowID)
	if executionFlowID == "" || !node.Valid() {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires exact flow and node owners")
	}
	if _, ok := source.ExecutableNode(node); !ok {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires declared node %q", node.Key())
	}
	semanticScope, err := semanticview.ResolveExecutableNodeSemanticScope(source, node)
	if err != nil {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires semantic scope for %q: %w", node.Key(), err)
	}
	owner := semanticScope.Declaration.Source
	route = route.Normalized()
	scope, ok := semanticScope.OwningFlow()
	if !ok {
		return events.RoutingSource{}, fmt.Errorf("flow node %q routing source references missing flow %q", node.Key(), node.FlowPath())
	}
	return admitFlowExecutionRoutingSource(source, "node", node.Key(), owner, route, scope)
}

// AdmitAgentExecutionRoutingSource admits the exact source fact from the
// actor's declaration-owned execution scope. Tool-produced events copy this
// value unchanged.
func AdmitAgentExecutionRoutingSource(source semanticview.Source, actor models.AgentConfig, entityID string) (events.RoutingSource, error) {
	if source == nil {
		return events.RoutingSource{}, fmt.Errorf("agent execution routing source requires semantic source")
	}
	scope, err := semanticview.ResolveAgentExecutionSemanticScope(source, actor)
	if err != nil {
		return events.RoutingSource{}, err
	}
	identity := scope.Identity()
	owner := scope.ContractSource()
	ownerFlowID := strings.TrimSpace(scope.Declaration().OwnerFlowID)
	route := events.RouteIdentity{EntityID: strings.TrimSpace(entityID)}
	if instancePath := strings.TrimSpace(identity.Route.Normalize().InstancePath); instancePath != "" {
		route.FlowID = ownerFlowID
		route.FlowInstance = instancePath
	}
	if ownerFlowID != "" {
		if sourceFlowID := strings.TrimSpace(owner.FlowPath); sourceFlowID != "" && sourceFlowID != ownerFlowID {
			return events.RoutingSource{}, fmt.Errorf("agent %q declaration source flow %q conflicts with canonical owning flow %q", identity.AgentID(), sourceFlowID, ownerFlowID)
		}
		owner.FlowPath = ownerFlowID
		flow, ok := scope.OwningFlow()
		if !ok {
			return events.RoutingSource{}, fmt.Errorf("agent %q routing source references missing owning flow %q", identity.AgentID(), ownerFlowID)
		}
		return admitFlowExecutionRoutingSource(source, "agent", identity.AgentID(), owner, route, flow)
	}
	return admitDeclaredExecutionRoutingSource(source, "agent", identity.AgentID(), owner, route)
}

// AdmitFlowExecutionRoutingSource admits an exact lifecycle/control anchor
// owned by one declared flow execution.
func AdmitFlowExecutionRoutingSource(source semanticview.Source, flowID string, route events.RouteIdentity) (events.RoutingSource, error) {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		flowID = "."
	}
	return admitDeclaredExecutionRoutingSource(source, "flow", flowID, runtimecontracts.ContractItemSource{FlowPath: flowID, Family: "flow"}, route)
}

func admitDeclaredExecutionRoutingSource(source semanticview.Source, ownerType, ownerID string, owner runtimecontracts.ContractItemSource, route events.RouteIdentity) (events.RoutingSource, error) {
	route = route.Normalized()
	owner.FlowPath = strings.TrimSpace(owner.FlowPath)
	scope, ok := exactRoutingFlowScope(source, owner.FlowPath)
	if !ok {
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source references missing flow %q", ownerType, ownerID, owner.FlowPath)
	}
	return admitFlowExecutionRoutingSource(source, ownerType, ownerID, owner, route, scope)
}

func admitFlowExecutionRoutingSource(source semanticview.Source, ownerType, ownerID string, owner runtimecontracts.ContractItemSource, route events.RouteIdentity, scope semanticview.FlowScope) (events.RoutingSource, error) {
	owner.FlowPath = strings.TrimSpace(owner.FlowPath)
	if owner.FlowPath == "" {
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source requires declared flow_path", ownerType, ownerID)
	}
	if owner.FlowPath == "." {
		return admitSelectedRootExecutionRoutingSource(ownerType, ownerID, route)
	}
	if route.FlowID != "" && route.FlowID != owner.FlowPath {
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source flow_id %q conflicts with declared flow %q", ownerType, ownerID, route.FlowID, owner.FlowPath)
	}
	route.FlowID = owner.FlowPath
	flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
	if flowPath == "" {
		flowPath = strings.Trim(strings.TrimSpace(source.FlowPath(owner.FlowPath)), "/")
	}
	switch strings.TrimSpace(scope.Mode) {
	case runtimecontracts.FlowModeTemplate:
		if route.FlowInstance == "" || (flowPath != "" && route.FlowInstance != flowPath && !strings.HasPrefix(route.FlowInstance, flowPath+"/")) {
			return events.RoutingSource{}, fmt.Errorf("template %s %q routing source requires a concrete instance of %q", ownerType, ownerID, flowPath)
		}
		return events.NewConcreteTemplateInstanceRoutingSource(route)
	case runtimecontracts.FlowModeStatic:
		if flowPath == "" {
			return events.RoutingSource{}, fmt.Errorf("static %s %q routing source requires declared flow path", ownerType, ownerID)
		}
		// A static declaration is the complete producer identity. Inbound
		// wildcard descendants must not become a second instance dialect.
		route.FlowInstance = flowPath
		return events.NewStaticFlowRoutingSource(route)
	case runtimecontracts.FlowModeSingleton:
		if flowPath == "" || (route.FlowInstance != "" && route.FlowInstance != flowPath && !strings.HasPrefix(route.FlowInstance, flowPath+"/")) {
			return events.RoutingSource{}, fmt.Errorf("singleton %s %q routing source requires an instance owned by flow path %q", ownerType, ownerID, flowPath)
		}
		// Singleton runtime instances are concrete lifecycle rows, but their
		// authored routing identity is the one static flow endpoint.
		route.FlowInstance = flowPath
		return events.NewStaticFlowRoutingSource(route)
	default:
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source has unsupported mode %q", ownerType, ownerID, scope.Mode)
	}
}

func admitSelectedRootExecutionRoutingSource(ownerType, ownerID string, route events.RouteIdentity) (events.RoutingSource, error) {
	route = route.Normalized()
	if route.FlowID != "" && route.FlowID != "." {
		return events.RoutingSource{}, fmt.Errorf("root %s %q routing source flow_id %q conflicts with selected root %q", ownerType, ownerID, route.FlowID, ".")
	}
	if route.EntityID != "" {
		return events.NewRootRoutingSource(route.EntityID)
	}
	if route.FlowID != "." || route.FlowInstance == "" {
		return events.RoutingSource{}, fmt.Errorf("root %s %q entityless routing source requires the exact selected-run flow route", ownerType, ownerID)
	}
	return events.NewStaticFlowRoutingSource(route)
}

func exactRoutingFlowScope(source semanticview.Source, flowID string) (semanticview.FlowScope, bool) {
	if scope, ok := semanticview.FlowScopeByID(source, flowID); ok {
		return scope, true
	}
	for _, scope := range source.FlowScopes() {
		if strings.Trim(strings.TrimSpace(scope.Path), "/") == strings.Trim(strings.TrimSpace(flowID), "/") {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}
