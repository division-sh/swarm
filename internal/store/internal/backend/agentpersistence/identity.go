package agentpersistence

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func IdentityFields(identity runtimeagentidentity.Identity) (runtimeagentidentity.StorageFields, error) {
	fields, err := identity.Normalize().StorageFields()
	if err != nil {
		return runtimeagentidentity.StorageFields{}, fmt.Errorf("agent identity storage fields: %w", err)
	}
	return fields, nil
}

func IdentityFromColumns(
	agentID,
	nameOwner,
	nameSource,
	routePresence,
	flowScopeKey,
	flowInstanceID,
	flowInstance string,
) (runtimeagentidentity.Identity, error) {
	return runtimeagentidentity.FromStorageFields(runtimeagentidentity.StorageFields{
		AgentID:          agentID,
		NameOwner:        nameOwner,
		NameSource:       nameSource,
		RoutePresence:    routePresence,
		FlowScopeKey:     flowScopeKey,
		FlowInstanceID:   flowInstanceID,
		FlowInstancePath: flowInstance,
	})
}

func (s *AgentPostgresOwner) ResolveOperatorAgentIdentity(ctx context.Context, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	return resolveOperatorAgentIdentity(ctx, s.agents, agentID, flowInstance)
}

func (s *AgentSQLiteOwner) ResolveOperatorAgentIdentity(ctx context.Context, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	return resolveOperatorAgentIdentity(ctx, s.agents, agentID, flowInstance)
}

func resolveOperatorAgentIdentity(ctx context.Context, source AgentSource, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	agentID = strings.TrimSpace(agentID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if agentID == "" {
		return runtimeagentidentity.Identity{}, operatorread.ErrAgentNotFound
	}
	rows, err := source.LoadAgents(ctx)
	if err != nil {
		return runtimeagentidentity.Identity{}, err
	}
	candidates := make([]runtimeagentidentity.Identity, 0)
	for _, row := range rows {
		identity, err := row.Config.ConcreteIdentity()
		if err != nil {
			return runtimeagentidentity.Identity{}, err
		}
		if identity.AgentID() != agentID {
			continue
		}
		if flowInstance != "" && identity.FlowInstance() != flowInstance {
			continue
		}
		candidates = append(candidates, identity)
	}
	if len(candidates) == 0 {
		return runtimeagentidentity.Identity{}, operatorread.ErrAgentNotFound
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return runtimeagentidentity.Less(candidates[i], candidates[j])
	})
	descriptions := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		descriptions = append(descriptions, candidate.Description())
	}
	return runtimeagentidentity.Identity{}, fmt.Errorf(
		"%w: agent_id %q matches %s",
		operatorread.ErrAgentTargetAmbiguous,
		agentID,
		strings.Join(descriptions, ", "),
	)
}

func IsAgentTargetAmbiguous(err error) bool {
	return operatorread.IsAgentTargetAmbiguous(err)
}
