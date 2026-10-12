package runforkadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func admitPendingJoinFrontier(source semanticview.Source, plan runfork.RunForkPlan, pending genericschedule.TransferredJoinOccurrence) (forkrecipient.Evidence, error) {
	recipient, target, handler, found, err := pipeline.ResolvePendingJoinOccurrenceDeliveryTarget(source, pending)
	if err != nil || !found {
		return forkrecipient.Evidence{}, fmt.Errorf("resolve pending join declaration: found=%v: %w", found, err)
	}
	path := target.FlowInstance
	if handler.Node().FlowPath() == semanticview.RootExecutionFlowID(source) && path == plan.SourceRunID {
		path = semanticview.RootExecutionFlowID(source)
	}
	event, exact := handler.EventOverride()
	if !exact {
		return forkrecipient.Evidence{}, fmt.Errorf("pending join requires its exact bound handler")
	}
	return forkrecipient.NewLocal(forkrecipient.Input{Recipient: recipient, Path: path, HandlerNode: handler.Node(), HandlerEvent: event, RouteSource: "retained_join"})
}

// A published occurrence's typed handle selects its original arm. Name-based
// pubsub cannot turn it into a new output or bind it to a later stage entry.
func admitPublishedJoinFrontier(source semanticview.Source, plan runfork.RunForkPlan, publication runfork.PublishedJoinOccurrence) (forkrecipient.Evidence, error) {
	event := publication.Event()
	recipient, target, handler, found, err := pipeline.ResolvePublishedJoinOccurrenceDeliveryTarget(source, publication)
	if err != nil || !found {
		return forkrecipient.Evidence{}, fmt.Errorf("resolve retained join declaration: found=%v: %w", found, err)
	}
	bound := false
	payload, err := canonicaljson.Decode(event.Payload())
	if err != nil {
		return forkrecipient.Evidence{}, err
	}
	object, valid := payload.Interface().(map[string]any)
	if !valid {
		return forkrecipient.Evidence{}, fmt.Errorf("retained join requires its exact handle payload")
	}
	_, ref, valid := timeridentity.ParseJoinHandle(object)
	if !valid {
		return forkrecipient.Evidence{}, fmt.Errorf("retained join requires its exact declaration")
	}
	for _, pending := range plan.PendingWork {
		if pending.EventID != event.ID() || pending.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
			continue
		}
		if ref.Mode() == timeridentity.JoinRefModeFanOutDelivery && (pending.DeliveryRoute.Recipient != recipient || !events.SameRouteIdentity(pending.DeliveryRoute.Target.Route(), target)) {
			continue
		}
		if pending.DeliveryID == "" || pending.DeliveryRoute.Recipient != recipient ||
			!pending.DeliveryRoute.Target.ExistingEntity() || !events.SameRouteIdentity(pending.DeliveryRoute.Target.Route(), target) ||
			!pending.DeliveryRoute.ConnectClaim.Empty() || !pending.DeliveryRoute.AgentIdentity.IsZero() {
			return forkrecipient.Evidence{}, fmt.Errorf("retained join frontier contradicts its original durable binding")
		}
		if bound {
			return forkrecipient.Evidence{}, fmt.Errorf("retained join frontier repeats its original binding")
		}
		bound = true
	}
	if !bound {
		return forkrecipient.Evidence{}, fmt.Errorf("retained join frontier lacks unfinished durable work")
	}
	path := target.FlowInstance
	if handler.Node().FlowPath() == semanticview.RootExecutionFlowID(source) && path == plan.SourceRunID {
		path = semanticview.RootExecutionFlowID(source)
	}
	handlerEvent, exact := handler.EventOverride()
	if !exact {
		return forkrecipient.Evidence{}, fmt.Errorf("retained join requires its exact original handler event")
	}
	return forkrecipient.NewLocal(forkrecipient.Input{Recipient: recipient, Path: path,
		HandlerNode: handler.Node(), HandlerEvent: handlerEvent, RouteSource: "retained_join"})
}
