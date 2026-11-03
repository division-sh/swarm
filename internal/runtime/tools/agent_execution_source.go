package tools

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (e *Executor) agentExecutionRoutingSource(ctx context.Context, actor actors.AgentConfig) (events.RoutingSource, error) {
	agent, err := actor.ConcreteIdentity()
	if err != nil {
		return events.RoutingSource{}, err
	}
	if correlation.RunIDFromContext(ctx) != agent.RunID {
		return events.RoutingSource{}, fmt.Errorf("agent producer requires its exact execution run")
	}
	if agent.Route.Presence == agentidentity.RouteRoot {
		producer, err := pinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor, actor.EffectiveEntityID(), nil)
		if err != nil {
			return events.RoutingSource{}, err
		}
		if life := events.DeliveryContextFromContext(ctx).Life; life != nil {
			return producer.WithInstanceLife(agent.RunID, *life)
		}
		return producer, nil
	}
	if e.workflowInstances == nil {
		return events.RoutingSource{}, fmt.Errorf("agent producer requires constructed flow persistence")
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: agent.RunID, Route: flowidentity.Route{
		ScopeKey: agent.Route.ScopeKey, InstanceID: agent.Route.InstanceID, InstancePath: agent.Route.InstancePath,
	}}
	instance, found, err := e.workflowInstances.Load(ctx, owner)
	if err != nil {
		return events.RoutingSource{}, fmt.Errorf("load agent producer construction: %w", err)
	}
	if !found {
		return events.RoutingSource{}, fmt.Errorf("agent producer has no exact constructed flow header")
	}
	constructed, err := instance.ConstructionIdentity(owner)
	if err != nil {
		return events.RoutingSource{}, err
	}
	producer, err := pinrouting.AdmitAgentExecutionRoutingSource(e.workflowSource, actor, actor.EffectiveEntityID(), constructed)
	if err != nil {
		return events.RoutingSource{}, err
	}
	if life := events.DeliveryContextFromContext(ctx).Life; life != nil {
		if life.Life != instance.Life {
			return events.RoutingSource{}, pipeline.InstanceRestarted(owner, life.Life, instance.Life)
		}
		return producer.WithInstanceLife(agent.RunID, *life)
	}
	if instance.Life != 1 {
		return events.RoutingSource{}, fmt.Errorf("agent producer requires its captured instance life")
	}
	return producer, nil
}
