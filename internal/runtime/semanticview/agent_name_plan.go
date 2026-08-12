package semanticview

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

// AgentNamePlan binds one scoped declaration coordinate to its effective
// public name. The declaration URI remains map-key based; an authored id only
// changes the public name.
type AgentNamePlan struct {
	ScopeKind   string
	ScopeID     string
	OwnerFlowID string
	LocalID     string
	OwnerURI    string
	AgentID     string
}

func (p AgentNamePlan) Normalize() AgentNamePlan {
	p.ScopeKind = strings.TrimSpace(p.ScopeKind)
	p.ScopeID = strings.TrimSpace(p.ScopeID)
	p.OwnerFlowID = strings.TrimSpace(p.OwnerFlowID)
	p.LocalID = strings.TrimSpace(p.LocalID)
	p.OwnerURI = strings.TrimSpace(p.OwnerURI)
	p.AgentID = strings.TrimSpace(p.AgentID)
	return p
}

func (p AgentNamePlan) Validate() error {
	p = p.Normalize()
	if p.LocalID == "" {
		return fmt.Errorf("agent declaration local id is required")
	}
	if p.OwnerURI == "" {
		return fmt.Errorf("agent declaration %q is missing its scoped owner URI", p.LocalID)
	}
	if p.AgentID == "" {
		return fmt.Errorf("agent declaration %q has no effective public name", p.LocalID)
	}
	return nil
}

func (p AgentNamePlan) Materialize() (agentidentity.Name, error) {
	p = p.Normalize()
	if err := p.Validate(); err != nil {
		return agentidentity.Name{}, err
	}
	return agentidentity.DeclaredName(p.AgentID, p.OwnerURI)
}

// EffectiveRole keeps declaration-derived actor and authority projections on
// one role. Required-agent socket identity remains the scoped local map key.
func (p AgentNamePlan) EffectiveRole(entry runtimecontracts.AgentRegistryEntry) string {
	if role := strings.TrimSpace(entry.Role); role != "" {
		return role
	}
	return strings.TrimSpace(p.AgentID)
}

func (d AgentDeclaration) NamePlan() (AgentNamePlan, error) {
	agentID, err := runtimecontracts.DeclaredAgentID(d.LocalID, d.Entry)
	if err != nil {
		return AgentNamePlan{}, err
	}
	plan := AgentNamePlan{
		ScopeKind:   d.ScopeKind,
		ScopeID:     d.ScopeID,
		OwnerFlowID: d.OwnerFlowID,
		LocalID:     d.LocalID,
		OwnerURI:    d.OwnerURI,
		AgentID:     agentID,
	}.Normalize()
	if err := plan.Validate(); err != nil {
		return AgentNamePlan{}, err
	}
	return plan, nil
}

func ScopedAgentNamePlan(source Source, declaration AgentDeclaration) (AgentNamePlan, error) {
	owner, ok := ScopedAgentDeclarationOwner(source, declaration)
	if !ok {
		return AgentNamePlan{}, fmt.Errorf("agent declaration %q is missing a unique scoped owner", strings.TrimSpace(declaration.LocalID))
	}
	declaration.OwnerURI = owner
	return declaration.NamePlan()
}

func ProjectAgentNamePlan(source Source, scope ProjectScope, localID string) (AgentNamePlan, error) {
	return agentNamePlanForScope(source, "project", scope.Key, scope.OwningFlowID, scope.AgentURIs[localID], localID)
}

func FlowAgentNamePlan(source Source, scope FlowScope, localID string) (AgentNamePlan, error) {
	return agentNamePlanForScope(source, "flow", scope.ID, scope.ID, scope.AgentURIs[localID], localID)
}

func agentNamePlanForScope(source Source, scopeKind, scopeID, ownerFlowID, ownerURI, localID string) (AgentNamePlan, error) {
	if source == nil {
		return AgentNamePlan{}, fmt.Errorf("semantic source is required")
	}
	scopeKind = strings.TrimSpace(scopeKind)
	scopeID = strings.TrimSpace(scopeID)
	ownerFlowID = strings.TrimSpace(ownerFlowID)
	ownerURI = strings.TrimSpace(ownerURI)
	localID = strings.TrimSpace(localID)
	if localID == "" {
		return AgentNamePlan{}, fmt.Errorf("agent declaration local id is required")
	}
	ownerMatches := []AgentDeclaration{}
	exact := []AgentDeclaration{}
	projected := []AgentDeclaration{}
	available := []string{}
	for _, declaration := range AgentDeclarations(source) {
		if strings.TrimSpace(declaration.LocalID) != localID {
			continue
		}
		available = append(available, declaration.Label(true))
		if ownerURI != "" && strings.TrimSpace(declaration.OwnerURI) == ownerURI {
			ownerMatches = append(ownerMatches, declaration)
		}
		if strings.TrimSpace(declaration.ScopeKind) == scopeKind &&
			strings.TrimSpace(declaration.ScopeID) == scopeID &&
			strings.TrimSpace(declaration.OwnerFlowID) == ownerFlowID {
			exact = append(exact, declaration)
			continue
		}
		if scopeKind == "flow" && strings.TrimSpace(declaration.ScopeKind) == "project" &&
			strings.TrimSpace(declaration.OwnerFlowID) == ownerFlowID {
			projected = append(projected, declaration)
		}
	}
	if len(ownerMatches) > 1 {
		return AgentNamePlan{}, fmt.Errorf("agent declaration %q at owner %q resolved to multiple declarations", localID, ownerURI)
	}
	if len(ownerMatches) == 1 {
		return ScopedAgentNamePlan(source, ownerMatches[0])
	}
	if len(exact) > 1 {
		return AgentNamePlan{}, fmt.Errorf("agent declaration %q in %s scope %q resolved to multiple exact declarations", localID, scopeKind, scopeID)
	}
	if len(exact) == 1 {
		return ScopedAgentNamePlan(source, exact[0])
	}
	// Package-backed flow declarations canonicalize to their project scope.
	// The flow projection may consume that owner only when it is unique.
	if scopeKind == "flow" && len(projected) == 1 {
		return ScopedAgentNamePlan(source, projected[0])
	}
	return AgentNamePlan{}, fmt.Errorf("agent declaration %q in %s scope %q is not canonical; candidates: %s", localID, scopeKind, scopeID, strings.Join(available, ", "))
}

func AgentNamePlans(source Source) ([]AgentNamePlan, error) {
	declarations := AgentDeclarations(source)
	plans := make([]AgentNamePlan, 0, len(declarations))
	owners := make(map[string]AgentNamePlan, len(declarations))
	for _, declaration := range declarations {
		plan, err := ScopedAgentNamePlan(source, declaration)
		if err != nil {
			return nil, err
		}
		scopeKey := strings.Join([]string{plan.ScopeKind, plan.ScopeID, plan.OwnerFlowID, plan.AgentID}, "\x00")
		if previous, exists := owners[scopeKey]; exists && previous.LocalID != plan.LocalID {
			return nil, fmt.Errorf(
				"agent declarations %q and %q in scope %q derive the same effective public name %q",
				previous.LocalID,
				plan.LocalID,
				plan.ScopeID,
				plan.AgentID,
			)
		}
		owners[scopeKey] = plan
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool {
		left := strings.Join([]string{plans[i].ScopeKind, plans[i].ScopeID, plans[i].OwnerFlowID, plans[i].LocalID}, "\x00")
		right := strings.Join([]string{plans[j].ScopeKind, plans[j].ScopeID, plans[j].OwnerFlowID, plans[j].LocalID}, "\x00")
		return left < right
	})
	return plans, nil
}
