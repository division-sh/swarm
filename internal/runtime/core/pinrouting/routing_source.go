package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
	owner, ok := source.ExecutableNodeSource(node)
	if !ok {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires declared source for %q", node.Key())
	}
	route = route.Normalized()
	if strings.TrimSpace(owner.Layer) == "project" && strings.TrimSpace(owner.FlowID) != "" {
		scope, ok := semanticview.FlowScopeByID(source, owner.FlowID)
		if !ok {
			return events.RoutingSource{}, fmt.Errorf("project node %q routing source references missing owning flow %q", node.Key(), owner.FlowID)
		}
		return admitFlowExecutionRoutingSource(source, "node", node.Key(), owner, route, scope)
	}
	if strings.TrimSpace(owner.Layer) == "project" && route.EntityID == "" {
		if route.FlowID != strings.TrimSpace(source.WorkflowName()) || route.FlowInstance == "" {
			return events.RoutingSource{}, fmt.Errorf("project node %q entityless routing source requires the exact selected-run flow route", node.Key())
		}
		return events.NewStaticFlowRoutingSource(route)
	}
	if owner.Layer == "flow" {
		scope, ok := semanticview.ExecutableNodeFlowScope(source, node)
		if !ok {
			return events.RoutingSource{}, fmt.Errorf("flow node %q routing source references missing flow %q", node.Key(), node.FlowID())
		}
		return admitFlowExecutionRoutingSource(source, "node", node.Key(), owner, route, scope)
	}
	return admitDeclaredExecutionRoutingSource(source, "node", node.Key(), owner, route)
}

// AdmitAgentExecutionRoutingSource admits the exact source fact from the
// actor's typed identity. Tool-produced events copy this value unchanged.
func AdmitAgentExecutionRoutingSource(source semanticview.Source, identity agentidentity.Identity, entityID string) (events.RoutingSource, error) {
	if source == nil {
		return events.RoutingSource{}, fmt.Errorf("agent execution routing source requires semantic source")
	}
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return events.RoutingSource{}, fmt.Errorf("agent execution routing source requires typed identity: %w", err)
	}
	scopeKey, _, instancePath, present := identity.Route.Fields()
	owner := runtimecontracts.ContractItemSource{Layer: "project"}
	route := events.RouteIdentity{EntityID: strings.TrimSpace(entityID)}
	if present {
		owner = runtimecontracts.ContractItemSource{Layer: "flow", FlowID: scopeKey}
		route.FlowID = scopeKey
		route.FlowInstance = instancePath
	}
	return admitDeclaredExecutionRoutingSource(source, "agent", identity.AgentID(), owner, route)
}

// AdmitFlowExecutionRoutingSource admits an exact lifecycle/control anchor
// owned by one declared flow execution.
func AdmitFlowExecutionRoutingSource(source semanticview.Source, flowID string, route events.RouteIdentity) (events.RoutingSource, error) {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return admitDeclaredExecutionRoutingSource(source, "flow", "root", runtimecontracts.ContractItemSource{Layer: "project"}, route)
	}
	return admitDeclaredExecutionRoutingSource(source, "flow", flowID, runtimecontracts.ContractItemSource{Layer: "flow", FlowID: flowID}, route)
}

func admitDeclaredExecutionRoutingSource(source semanticview.Source, ownerType, ownerID string, owner runtimecontracts.ContractItemSource, route events.RouteIdentity) (events.RoutingSource, error) {
	route = route.Normalized()
	owner.Layer = strings.TrimSpace(owner.Layer)
	owner.FlowID = strings.TrimSpace(owner.FlowID)
	switch owner.Layer {
	case "project":
		if route.EntityID == "" {
			return events.RoutingSource{}, fmt.Errorf("project %s %q routing source requires entity_id", ownerType, ownerID)
		}
		return events.NewRootRoutingSource(route.EntityID)
	case "flow":
		scope, ok := exactRoutingFlowScope(source, owner.PackageKey, owner.FlowID)
		if !ok {
			return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source references missing flow %q", ownerType, ownerID, owner.FlowID)
		}
		return admitFlowExecutionRoutingSource(source, ownerType, ownerID, owner, route, scope)
	default:
		return events.RoutingSource{}, fmt.Errorf("%s %q routing source has unsupported declaration layer %q", ownerType, ownerID, owner.Layer)
	}
}

func admitFlowExecutionRoutingSource(source semanticview.Source, ownerType, ownerID string, owner runtimecontracts.ContractItemSource, route events.RouteIdentity, scope semanticview.FlowScope) (events.RoutingSource, error) {
	owner.FlowID = strings.TrimSpace(owner.FlowID)
	if owner.FlowID == "" {
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source requires declared flow_id", ownerType, ownerID)
	}
	if route.FlowID != "" && route.FlowID != owner.FlowID {
		return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source flow_id %q conflicts with declared flow %q", ownerType, ownerID, route.FlowID, owner.FlowID)
	}
	route.FlowID = owner.FlowID
	flowPath := strings.Trim(strings.TrimSpace(scope.Path), "/")
	if flowPath == "" {
		flowPath = strings.Trim(strings.TrimSpace(source.FlowPath(owner.FlowID)), "/")
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

func exactRoutingFlowScope(source semanticview.Source, packageKey, flowID string) (semanticview.FlowScope, bool) {
	packageKey = strings.Trim(strings.TrimSpace(packageKey), "/")
	if packageKey == "" {
		return semanticview.FlowScopeByID(source, flowID)
	}
	for _, scope := range source.FlowScopes() {
		candidate := strings.Trim(strings.TrimSpace(scope.PackageKey), "/")
		if candidate == "" {
			candidate = runtimeidentity.RootPackageKey
		}
		if candidate == packageKey && strings.TrimSpace(scope.ID) == strings.TrimSpace(flowID) {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}
