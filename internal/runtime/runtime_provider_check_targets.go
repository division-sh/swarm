package runtime

import (
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

type ManagedProviderCheckTarget struct {
	Actor actors.AgentConfig
	Plan  agentidentity.Plan
}

// CompileManagedProviderCheckTargets shares the exact runtime census without
// constructing an executable manager, hydrating actors, or taking authority.
func CompileManagedProviderCheckTargets(live []actors.AgentConfig, blueprints []manager.AgentMaterializationBlueprint) ([]ManagedProviderCheckTarget, error) {
	configs, err := managedProviderPreflightConfigs(live, blueprints)
	if err != nil {
		return nil, err
	}
	targets := make([]ManagedProviderCheckTarget, len(configs))
	for i, candidate := range configs {
		targets[i] = ManagedProviderCheckTarget{Actor: candidate.config, Plan: candidate.plan}
	}
	return targets, nil
}
