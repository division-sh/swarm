package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func workflowNodeProducerSource(ctx context.Context, source semanticview.Source, node runtimeidentity.ExecutableNode, flowID, entityID string, admittedSource events.RoutingSource) (events.RoutingSource, error) {
	route := admittedSource.Route().Normalized()
	if route.Empty() {
		route = events.RouteIdentity{FlowID: strings.TrimSpace(flowID), EntityID: strings.TrimSpace(entityID)}
	} else if strings.TrimSpace(entityID) != "" {
		route.EntityID = strings.TrimSpace(entityID)
	}
	if application, ok := deliveryTargetApplicationFromContext(ctx); ok {
		if err := application.Validate(); err != nil {
			return events.RoutingSource{}, err
		}
		if strings.TrimSpace(entityID) != application.EntityID() {
			return events.RoutingSource{}, fmt.Errorf("workflow node producer entity disagrees with admitted delivery target application")
		}
		route = application.Owner().Route()
	} else if _, ok := runtimedelivery.RouteFromContext(ctx); ok {
		return events.RoutingSource{}, fmt.Errorf("stamped workflow node producer requires delivery target application")
	}
	return runtimepinrouting.AdmitNodeExecutionRoutingSource(source, node, flowID, route)
}
