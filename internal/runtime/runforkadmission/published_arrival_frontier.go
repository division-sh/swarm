package runforkadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// A published occurrence's typed handle selects its original arm. Name-based
// pubsub cannot turn it into a new output or bind it to a later stage entry.
func admitPublishedArrivalFrontier(source semanticview.Source, plan runfork.RunForkPlan, publication runfork.PublishedArrival) (forkrecipient.Evidence, error) {
	event := publication.Event()
	recipient, target, handler, found, err := pipeline.ResolveWorkflowJoinOccurrenceDeliveryTarget(source, event)
	if err != nil || !found {
		return forkrecipient.Evidence{}, fmt.Errorf("resolve retained arrival declaration: found=%v: %w", found, err)
	}
	bound := false
	for _, pending := range plan.PendingWork {
		if pending.EventID != event.ID() || pending.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		if pending.DeliveryID == "" || pending.DeliveryRoute.Recipient != recipient ||
			!pending.DeliveryRoute.Target.ExistingEntity() || !events.SameRouteIdentity(pending.DeliveryRoute.Target.Route(), target) ||
			!pending.DeliveryRoute.ConnectClaim.Empty() || !pending.DeliveryRoute.AgentIdentity.IsZero() {
			return forkrecipient.Evidence{}, fmt.Errorf("retained arrival frontier contradicts its original durable binding")
		}
		if bound {
			return forkrecipient.Evidence{}, fmt.Errorf("retained arrival frontier repeats its original binding")
		}
		bound = true
	}
	if !bound {
		return forkrecipient.Evidence{}, fmt.Errorf("retained arrival frontier lacks unfinished durable work")
	}
	path := target.FlowInstance
	if handler.Node().FlowPath() == semanticview.RootExecutionFlowID(source) && path == plan.SourceRunID {
		path = semanticview.RootExecutionFlowID(source)
	}
	handlerEvent, exact := handler.EventOverride()
	if !exact {
		return forkrecipient.Evidence{}, fmt.Errorf("retained arrival requires its exact original handler event")
	}
	return forkrecipient.NewLocal(forkrecipient.Input{Recipient: recipient, Path: path,
		HandlerNode: handler.Node(), HandlerEvent: handlerEvent, RouteSource: "retained_arrival"})
}
