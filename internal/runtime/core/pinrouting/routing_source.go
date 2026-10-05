package pinrouting

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// AdmitNodeExecutionRoutingSource admits the exact source fact at the node
// execution boundary after its caller admits construction or exact committed
// delivery evidence. Declaration identity is checked here; a path prefix is
// neither construction evidence nor output-consumer authority.
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
	if executionFlowID != scope.ID || (route.FlowID != "" && route.FlowID != scope.ID) {
		return events.RoutingSource{}, fmt.Errorf("node execution routing source conflicts with its exact declared flow")
	}
	if owner.FlowPath == "." {
		return admitSelectedRootExecutionRoutingSource("node", node.Key(), route)
	}
	route.FlowID = scope.ID
	switch scope.Mode {
	case runtimecontracts.FlowModeStatic:
		return events.NewStaticFlowRoutingSource(route)
	case runtimecontracts.FlowModeTemplate:
		return events.NewConcreteTemplateInstanceRoutingSource(route)
	default:
		return events.RoutingSource{}, fmt.Errorf("node execution routing source has an unsupported declared mode")
	}
}

// AdmitAgentExecutionRoutingSource admits the exact source fact from the
// actor's declaration-owned execution scope. Tool-produced events copy this
// value unchanged.
func AdmitAgentExecutionRoutingSource(source semanticview.Source, actor models.AgentConfig, entityID string, construction semanticview.AgentExecutionConstruction) (events.RoutingSource, error) {
	if source == nil {
		return events.RoutingSource{}, fmt.Errorf("agent execution routing source requires semantic source")
	}
	scope, err := semanticview.ResolveAgentExecutionSemanticScope(source, actor, construction)
	if err != nil {
		return events.RoutingSource{}, err
	}
	identity := scope.Identity()
	owner := scope.ContractSource()
	ownerFlowID := strings.TrimSpace(scope.Declaration().OwnerFlowID)
	if strings.TrimSpace(entityID) != actor.EffectiveEntityID() {
		return events.RoutingSource{}, fmt.Errorf("agent routing source entity conflicts with its exact actor owner")
	}
	route := events.RouteIdentity{EntityID: strings.TrimSpace(entityID)}
	if ownerFlowID == "." {
		// A root identity has no instance path; its live run is the selected
		// execution coordinate even when the agent owns no entity.
		route.FlowID = ownerFlowID
		route.FlowInstance = identity.RunID
	}
	if instancePath := strings.TrimSpace(identity.Route.Normalize().InstancePath); instancePath != "" {
		route.FlowID = ownerFlowID
		route.FlowInstance = instancePath
	}
	if ownerFlowID == "." {
		return admitSelectedRootExecutionRoutingSource("agent", identity.AgentID(), route)
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
		switch flow.Mode {
		case runtimecontracts.FlowModeStatic:
			return events.NewStaticFlowRoutingSource(route)
		case runtimecontracts.FlowModeTemplate:
			return events.NewConcreteTemplateInstanceRoutingSource(route)
		default:
			return events.RoutingSource{}, fmt.Errorf("agent routing source has an unsupported declared mode")
		}
	}
	return events.RoutingSource{}, fmt.Errorf("agent routing source lacks its exact declared owner")
}

// AdmitFlowExecutionRoutingSource admits an exact lifecycle/control anchor
// owned by one declared flow execution.
func AdmitFlowExecutionRoutingSource(source semanticview.Source, runID string, instance flowidentity.Instance, route events.RouteIdentity) (events.RoutingSource, error) {
	if err := instance.ValidateConstruction(source, runID); err != nil {
		return events.RoutingSource{}, fmt.Errorf("flow execution routing source requires construction: %w", err)
	}
	route = route.Normalized()
	if route.FlowID != instance.TemplateID || route.FlowInstance != instance.InstancePath || route.EntityID != instance.EntityID {
		return events.RoutingSource{}, fmt.Errorf("flow execution routing source conflicts with its exact constructed owner")
	}
	scope, ok := source.FlowScopeByID(instance.TemplateID)
	if !ok {
		return events.RoutingSource{}, fmt.Errorf("flow execution routing source references missing declared flow")
	}
	if instance.TemplateID == semanticview.RootExecutionFlowID(source) {
		return admitSelectedRootExecutionRoutingSource("flow", instance.TemplateID, route)
	}
	switch scope.Mode {
	case runtimecontracts.FlowModeStatic:
		return events.NewStaticFlowRoutingSource(route)
	case runtimecontracts.FlowModeTemplate:
		return events.NewConcreteTemplateInstanceRoutingSource(route)
	default:
		return events.RoutingSource{}, fmt.Errorf("flow execution routing source has unsupported declared mode %q", scope.Mode)
	}
}

func admitSelectedRootExecutionRoutingSource(ownerType, ownerID string, route events.RouteIdentity) (events.RoutingSource, error) {
	route = route.Normalized()
	if route.FlowID != "" && route.FlowID != "." {
		return events.RoutingSource{}, fmt.Errorf("root %s %q routing source flow_id %q conflicts with selected root %q", ownerType, ownerID, route.FlowID, ".")
	}
	if route.FlowID != "." || route.FlowInstance == "" {
		return events.RoutingSource{}, fmt.Errorf("root %s %q entityless routing source requires the exact selected-run flow route", ownerType, ownerID)
	}
	return events.NewStaticFlowRoutingSource(route)
}
