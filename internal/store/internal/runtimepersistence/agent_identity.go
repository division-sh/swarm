package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func agentIdentityFields(identity runtimeagentidentity.Identity) (runtimeagentidentity.StorageFields, error) {
	fields, err := identity.Normalize().StorageFields()
	if err != nil {
		return runtimeagentidentity.StorageFields{}, fmt.Errorf("agent identity storage fields: %w", err)
	}
	return fields, nil
}

func agentIdentityFromColumns(
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

type agentIdentityReadSource interface {
	LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error)
}

func (s *PostgresStore) ResolveOperatorAgentIdentity(ctx context.Context, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	return resolveOperatorAgentIdentity(ctx, s, agentID, flowInstance)
}

func (s *SQLiteRuntimeStore) ResolveOperatorAgentIdentity(ctx context.Context, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	return resolveOperatorAgentIdentity(ctx, s, agentID, flowInstance)
}

func resolveOperatorAgentIdentity(ctx context.Context, source agentIdentityReadSource, agentID, flowInstance string) (runtimeagentidentity.Identity, error) {
	agentID = strings.TrimSpace(agentID)
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if agentID == "" {
		return runtimeagentidentity.Identity{}, ErrAgentNotFound
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
		return runtimeagentidentity.Identity{}, ErrAgentNotFound
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
		ErrAgentTargetAmbiguous,
		agentID,
		strings.Join(descriptions, ", "),
	)
}

func IsAgentTargetAmbiguous(err error) bool {
	return errors.Is(err, ErrAgentTargetAmbiguous)
}
