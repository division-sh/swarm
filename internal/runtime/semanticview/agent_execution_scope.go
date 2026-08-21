package semanticview

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// AgentExecutionSemanticScope is the admitted agreement between one live actor
// identity and its exact physical contract declaration. Its fields are private
// so producers cannot synthesize or partially reconstruct the scope.
type AgentExecutionSemanticScope struct {
	declaration AgentDeclaration
	identity    agentidentity.Identity
	flow        FlowScope
	hasFlow     bool
}

func ResolveAgentExecutionSemanticScope(source Source, actor models.AgentConfig) (AgentExecutionSemanticScope, error) {
	if source == nil {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope requires semantic source")
	}
	identity, err := actor.ConcreteIdentity()
	if err != nil {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope requires concrete identity: %w", err)
	}
	if identity.Name.Source != agentidentity.NameSourceDeclared {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope requires a declared agent identity")
	}
	declaration, ok := resolveAgentDeclarationByName(source, "", identity.AgentID(), identity.Name.Owner)
	if !ok {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope has no exact declaration for %s", identity.Description())
	}
	plan, err := ScopedAgentNamePlan(source, declaration)
	if err != nil {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope declaration name: %w", err)
	}
	name, err := plan.Materialize()
	if err != nil {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution semantic scope materialize declaration name: %w", err)
	}
	if identity.Name.Normalize() != name.Normalize() {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution identity name conflicts with declaration owner")
	}
	ownerFlowID := strings.TrimSpace(declaration.OwnerFlowID)
	if strings.TrimSpace(actor.FlowID) != ownerFlowID {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution flow_id %q conflicts with declaration owner flow %q", actor.FlowID, ownerFlowID)
	}
	scope := AgentExecutionSemanticScope{declaration: declaration, identity: identity}
	if ownerFlowID == "" {
		if identity.Route.Presence != agentidentity.RouteRoot {
			return AgentExecutionSemanticScope{}, fmt.Errorf("root agent declaration requires an explicit root identity route")
		}
		return scope, nil
	}
	flow, ok := source.FlowScopeByID(ownerFlowID)
	if !ok {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent declaration references missing owning flow %q", ownerFlowID)
	}
	if identity.Route.Presence != agentidentity.RoutePresent {
		return AgentExecutionSemanticScope{}, fmt.Errorf("flow agent declaration %q requires a concrete flow identity route", ownerFlowID)
	}
	flowPath := strings.Trim(strings.TrimSpace(flow.Path), "/")
	if flowPath == "" {
		flowPath = strings.Trim(strings.TrimSpace(source.FlowPath(ownerFlowID)), "/")
	}
	if flowPath == "" {
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent declaration owner flow %q has no semantic path", ownerFlowID)
	}
	route := identity.Route.Normalize()
	switch strings.TrimSpace(flow.Mode) {
	case runtimecontracts.FlowModeTemplate:
		if route.ScopeKey != flowPath || route.InstancePath == flowPath || !strings.HasPrefix(route.InstancePath, flowPath+"/") {
			return AgentExecutionSemanticScope{}, fmt.Errorf("template agent execution route %q is not a concrete instance of declaration flow %q", route.InstancePath, flowPath)
		}
	case runtimecontracts.FlowModeStatic, runtimecontracts.FlowModeSingleton:
		if route.InstancePath != flowPath {
			return AgentExecutionSemanticScope{}, fmt.Errorf("agent execution route %q conflicts with declaration flow path %q", route.InstancePath, flowPath)
		}
	default:
		return AgentExecutionSemanticScope{}, fmt.Errorf("agent declaration owner flow %q has unsupported mode %q", ownerFlowID, flow.Mode)
	}
	scope.flow = flow
	scope.hasFlow = true
	return scope, nil
}

func (s AgentExecutionSemanticScope) Declaration() AgentDeclaration {
	return s.declaration
}

func (s AgentExecutionSemanticScope) Identity() agentidentity.Identity {
	return s.identity
}

func (s AgentExecutionSemanticScope) ContractSource() runtimecontracts.ContractItemSource {
	return s.declaration.Source
}

func (s AgentExecutionSemanticScope) OwningFlow() (FlowScope, bool) {
	return s.flow, s.hasFlow
}
