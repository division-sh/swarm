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

func workflowNodeProducerSource(ctx context.Context, source semanticview.Source, node runtimeidentity.ExecutableNode, flowID, entityID string, executionRoute events.RouteIdentity) (events.RoutingSource, error) {
	route := executionRoute.Normalized()
	if route.FlowInstance == "" || route.FlowID != strings.TrimSpace(flowID) || route.EntityID != strings.TrimSpace(entityID) {
		return events.RoutingSource{}, fmt.Errorf("workflow node producer disagrees with its exact execution coordinate")
	}
	if application, ok := deliveryTargetApplicationFromContext(ctx); ok {
		if err := application.Validate(); err != nil {
			return events.RoutingSource{}, err
		}
		if strings.TrimSpace(entityID) != application.EntityID() {
			return events.RoutingSource{}, fmt.Errorf("workflow node producer entity disagrees with admitted delivery target application")
		}
		if !application.previewOnly() {
			constructed, err := requireWorkflowInstanceIdentity(application.Route(), runtimeidentity.NormalizeEntityID(application.EntityID()), application.instance)
			if err != nil || constructed.ValidateConstruction(source, application.Event().RunID()) != nil {
				return events.RoutingSource{}, fmt.Errorf("workflow node producer requires its exact constructed instance")
			}
		}
		route = application.Owner().Route()
	} else if _, ok := runtimedelivery.RouteFromContext(ctx); ok {
		return events.RoutingSource{}, fmt.Errorf("stamped workflow node producer requires delivery target application")
	}
	return runtimepinrouting.AdmitNodeExecutionRoutingSource(source, node, flowID, route)
}
