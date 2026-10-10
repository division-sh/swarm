package runforkpersistence

import (
	"bytes"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// Already published work uses the original cut's event and delivery, never a
// newly admitted generic schedule or current source-run delivery lookup.
func prepareRunForkPublishedArrivalTransfer(snapshot *runForkRevisionSnapshot, plan runfork.RunForkPlan, childRunID, sourceEventID string) (genericschedule.PublishedJoinContinuation, error) {
	if snapshot == nil || snapshot.RunID != plan.SourceRunID || snapshot.Revision != plan.ForkPoint.Revision {
		return genericschedule.PublishedJoinContinuation{}, fmt.Errorf("published arrival transfer requires its exact bound cut")
	}
	projected, err := prepareRunForkArrivalJoinSchedules(plan, childRunID)
	if err != nil {
		return genericschedule.PublishedJoinContinuation{}, err
	}
	var continuation genericschedule.PublishedJoinContinuation
	for _, row := range projected {
		if row.source.CurrentEventID != sourceEventID {
			continue
		}
		if continuation.Present() {
			return genericschedule.PublishedJoinContinuation{}, fmt.Errorf("published arrival transfer repeats a source event")
		}
		admitted, err := runForkPublishedArrivalEvidence(snapshot, row.source)
		if err != nil {
			return genericschedule.PublishedJoinContinuation{}, err
		}
		if _, err := requireRunForkPublishedArrivalDelivery(snapshot, row.source, admitted.Event()); err != nil {
			return genericschedule.PublishedJoinContinuation{}, err
		}
		continuation, err = genericschedule.ProjectPublishedJoinContinuation(row.source, admitted.Event(), row.command)
		if err != nil {
			return genericschedule.PublishedJoinContinuation{}, err
		}
	}
	if !continuation.Present() {
		return genericschedule.PublishedJoinContinuation{}, fmt.Errorf("published arrival transfer lacks its retained source occurrence")
	}
	return continuation, nil
}

func requireRunForkPublishedArrivalDelivery(snapshot *runForkRevisionSnapshot, activation genericschedule.Activation, event events.Event) (events.DeliveryRoute, error) {
	if snapshot == nil || snapshot.RunID != activation.Command.RunID || event.RunID() != snapshot.RunID || event.ID() != activation.CurrentEventID {
		return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery requires its exact source occurrence")
	}
	payload, object := activation.Command.Payload.Interface().(map[string]any)
	if !object {
		return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery requires an object handle")
	}
	_, ref, valid := timeridentity.ParseJoinHandle(payload)
	if !valid || ref.Mode() != timeridentity.JoinRefModeArrival {
		return events.DeliveryRoute{}, fmt.Errorf("published arrival transfer requires its strict source join handle")
	}
	entry := ref.StageEntry()
	want := events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: entry.InstancePath, EntityID: entry.EntityID}
	found := false
	var original events.DeliveryRoute
	for _, delivery := range snapshot.Deliveries {
		row := delivery.Snapshot
		if row.EventID != event.ID() {
			continue
		}
		if row.RunID != snapshot.RunID || row.DeliveryID == "" || delivery.FirstRevision <= 0 ||
			delivery.Revision < delivery.FirstRevision || delivery.Revision > snapshot.Revision {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery contradicts its fixed cut")
		}
		if row.Status != deliverylifecycle.StatusPending && row.Status != deliverylifecycle.StatusInProgress && row.Status != deliverylifecycle.StatusFailed {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival transfer requires unfinished source work")
		}
		route := row.Route
		node, exact := route.Recipient.Node()
		if !exact || !node.Equal(ref.Node()) || !route.Target.ExistingEntity() ||
			!events.SameRouteIdentity(route.Target.Route(), want) || !route.ConnectClaim.Empty() || !route.AgentIdentity.IsZero() {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery contradicts its original exact join owner")
		}
		identity, err := route.Identity()
		if err != nil || identity != row.RouteIdentity || row.SubscriberClass != deliverylifecycle.SubscriberNode || row.SubscriberID != ref.Node().Key() {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery contradicts its retained identity")
		}
		id, err := deliverylifecycle.DeliveryID(event.ID(), route)
		if err != nil || id != row.DeliveryID {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival delivery changed its exact obligation identity")
		}
		if found {
			return events.DeliveryRoute{}, fmt.Errorf("published arrival repeats its original delivery")
		}
		found = true
		original = route.Normalized()
	}
	if !found {
		return events.DeliveryRoute{}, fmt.Errorf("published arrival lacks its original durable delivery")
	}
	return original, nil
}

func (a runForkSourceStateAdmission) publishedArrival(event runfork.RunForkSelectedContractSourceEvent, state *runForkProjectedSourceState) (runfork.RunForkSelectedContractSourceEvent, error) {
	if state == nil {
		return event, fmt.Errorf("published arrival requires its fixed-cut constructed owner")
	}
	entities, err := loadRunForkEntityStates(a.snapshot)
	if err != nil {
		return event, err
	}
	entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(a.snapshot, entities)
	if err != nil {
		return event, err
	}
	arrivals, err := loadRunForkArrivalJoinSchedules(a.snapshot, entities)
	if err != nil {
		return event, err
	}
	plan := runfork.RunForkPlan{SourceRunID: a.snapshot.RunID, Entities: entities,
		JoinSchedules: arrivals, ForkPoint: runfork.RunForkPoint{Revision: a.snapshot.Revision}}
	for _, reply := range a.snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	continuation, err := prepareRunForkPublishedArrivalTransfer(a.snapshot, plan, a.forkRunID, event.SourceEventID)
	if err != nil {
		return event, err
	}
	original := continuation.SourceEvent()
	if original.Type() != events.EventType(event.EventName) || original.ExecutionMode() != event.ExecutionMode || !bytes.Equal(original.Payload(), event.Payload) {
		return event, fmt.Errorf("selected arrival source contradicts its original publication")
	}
	// The sealed projection is also what publication later consumes. A source
	// payload copied through the ordinary business-event path cannot select it.
	projected, err := continuation.Event(activityidentity.ForkLineageEventID(a.forkRunID, event.SourceEventID), runfork.RunForkSelectedContractExecutionOwner)
	if err != nil {
		return event, err
	}
	if event.RoutingSource != projected.RoutingSource() {
		return event, fmt.Errorf("selected arrival producer disagrees with its exact child projection")
	}
	var activation genericschedule.Activation
	for _, arrival := range arrivals {
		if arrival.CurrentEventID == event.SourceEventID {
			activation = arrival
		}
	}
	route, err := requireRunForkPublishedArrivalDelivery(a.snapshot, activation, original)
	if err != nil {
		return event, err
	}
	route.Context, err = projectRunForkConstructionContext(plan, a.forkRunID, route.Context)
	if err != nil {
		return event, err
	}
	command := continuation.ChildCommand()
	target := command.RoutingSource.Route()
	if command.RoutingSource.Kind() == events.RoutingSourceRoot {
		target.FlowID, target.FlowInstance = ".", a.forkRunID
	}
	route.Target, err = events.NewExistingEntityTarget(target)
	if err != nil {
		return event, err
	}
	// Source construction initialization is retained in source history; the
	// child already owns its separately admitted exact materialization.
	route.Initialization = events.ReceiverInitialization{}
	event.PublishedArrival, event.PublishedArrivalRoute, event.Payload = continuation, route, projected.Payload()
	return event, nil
}
