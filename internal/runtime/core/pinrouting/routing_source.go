package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// AdmitNodeExecutionRoutingSource admits the exact source fact at the node
// execution boundary. Downstream event producers copy this value unchanged.
func AdmitNodeExecutionRoutingSource(source semanticview.Source, executionFlowID, nodeID string, route events.RouteIdentity) (events.RoutingSource, error) {
	if source == nil {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires semantic source")
	}
	executionFlowID = strings.TrimSpace(executionFlowID)
	nodeID = strings.TrimSpace(nodeID)
	if executionFlowID == "" || nodeID == "" {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires exact flow and node owners")
	}
	if _, _, ok := semanticview.ResolveFlowNodeDeclaration(source, executionFlowID, nodeID); !ok {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source requires node %q in exact flow scope %q", nodeID, executionFlowID)
	}
	owner := runtimecontracts.ContractItemSource{Layer: "flow", FlowID: executionFlowID}
	if executionFlowID == semanticview.RootExecutionFlowID(source) {
		owner = runtimecontracts.ContractItemSource{Layer: "project"}
	}
	route = route.Normalized()
	if strings.TrimSpace(owner.Layer) == "project" && route.EntityID == "" {
		if route.FlowID != strings.TrimSpace(source.WorkflowName()) || route.FlowInstance == "" {
			return events.RoutingSource{}, fmt.Errorf("project node %q entityless routing source requires the exact selected-run flow route", nodeID)
		}
		return events.NewStaticFlowRoutingSource(route)
	}
	return admitDeclaredExecutionRoutingSource(source, "node", nodeID, owner, route)
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
		if owner.FlowID == "" {
			return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source requires declared flow_id", ownerType, ownerID)
		}
		if route.FlowID != "" && route.FlowID != owner.FlowID {
			return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source flow_id %q conflicts with declared flow %q", ownerType, ownerID, route.FlowID, owner.FlowID)
		}
		route.FlowID = owner.FlowID
		scope, ok := semanticview.FlowScopeByID(source, owner.FlowID)
		if !ok {
			return events.RoutingSource{}, fmt.Errorf("flow %s %q routing source references missing flow %q", ownerType, ownerID, owner.FlowID)
		}
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
	default:
		return events.RoutingSource{}, fmt.Errorf("%s %q routing source has unsupported declaration layer %q", ownerType, ownerID, owner.Layer)
	}
}
