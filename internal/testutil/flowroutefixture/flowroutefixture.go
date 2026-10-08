// Package flowroutefixture constructs route-table inputs for tests without
// granting runtime activation authority.
package flowroutefixture

import (
	"context"
	"errors"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

func Publish(eventBus *runtimebus.EventBus, req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	if eventBus == nil || eventBus.RouteTable() == nil {
		return errors.New("flow route fixture requires an EventBus route table")
	}
	return eventBus.RouteTable().AddFlowInstanceRoute(req)
}

func StageAndPublish(ctx context.Context, eventBus *runtimebus.EventBus, req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	if eventBus == nil {
		return errors.New("flow route fixture requires an EventBus")
	}
	committed, commitErr := eventBus.StageFlowInstanceRouteContext(ctx, req)
	if !committed.Acknowledged {
		return errors.Join(commitErr, errors.New("flow route fixture topology commit was not acknowledged"))
	}
	return errors.Join(commitErr, Publish(eventBus, req))
}
