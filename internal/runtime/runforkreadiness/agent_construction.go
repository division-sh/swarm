package runforkreadiness

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// AgentConstruction consumes only the fixed plan's header and recorded config.
// It neither constructs a receiver nor reads a mutable source-run projection.
func AgentConstruction(source semanticview.Source, plan runfork.RunForkPlan, agent agentidentity.Plan) (flowidentity.Instance, error) {
	live, err := agent.Live(plan.SourceRunID)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if live.Route.Presence == agentidentity.RouteRoot {
		return flowidentity.Instance{}, nil
	}
	route := flowidentity.Route{ScopeKey: live.Route.ScopeKey, InstanceID: live.Route.InstanceID, InstancePath: live.Route.InstancePath}
	constructed, _, found, err := runforkadmission.FixedConstructionForRoute(source, plan, route)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	if !found {
		return flowidentity.Instance{}, fmt.Errorf("selected agent has no fixed-revision construction header for %q", route.InstancePath)
	}
	return constructed, nil
}
