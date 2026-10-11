package runforkpersistence

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
)

// A published barrier retains its exact event and unfinished outcome delivery.
// This proves fixed-cut membership, not a new timer or current execution grant.
func runForkPublishedFanOutBarrierEvidence(snapshot *runForkRevisionSnapshot, barrier fanoutbarrier.Barrier, activation genericschedule.Activation) (events.AdmittedEvent, events.DeliveryRoute, error) {
	if barrier.Status != fanoutbarrier.StatusClosedPending || activation.Status != genericschedule.StatusFired {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, fmt.Errorf("published fan-out continuation requires unfinished barrier work")
	}
	if err := genericschedule.ValidateFanOutBarrierScheduleRelation(barrier, activation); err != nil {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, err
	}
	admitted, err := runForkPublishedJoinEvidence(snapshot, activation)
	if err != nil {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, err
	}
	event := admitted.Event()
	ref, _ := barrier.Registration.Handle.JoinRef()
	want := events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: barrier.Registration.Route.InstancePath, EntityID: barrier.Registration.EntityID}
	var routes []events.DeliveryRoute
	var original events.DeliveryRoute
	for _, delivery := range snapshot.Deliveries {
		row := delivery.Snapshot
		if row.EventID != event.ID() {
			continue
		}
		routes = append(routes, row.Route)
		if row.Route.Recipient != events.MustNodeDeliveryRecipient(ref.Node()) || !events.SameRouteIdentity(row.Route.Target.Route(), want) {
			continue
		}
		if err := validateRunForkPublishedJoinDelivery(snapshot, delivery, event, ref, want, true); err != nil {
			return events.AdmittedEvent{}, events.DeliveryRoute{}, err
		}
		if !original.Recipient.Empty() {
			return events.AdmittedEvent{}, events.DeliveryRoute{}, fmt.Errorf("published fan-out barrier repeats its exact outcome delivery")
		}
		original = row.Route.Normalized()
	}
	if original.Recipient.Empty() {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, fmt.Errorf("published fan-out barrier lacks its exact unfinished outcome delivery")
	}
	var settlement events.RouteSettlement
	if err := json.Unmarshal(runForkPublishedEventSettlement(snapshot, event.ID()), &settlement); err != nil {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, err
	}
	if err := settlement.Validate(routes); err != nil {
		return events.AdmittedEvent{}, events.DeliveryRoute{}, err
	}
	return admitted, original, nil
}

func runForkPublishedEventSettlement(snapshot *runForkRevisionSnapshot, eventID string) json.RawMessage {
	for _, row := range snapshot.Events {
		if row.EventID == eventID {
			return row.RouteSettlement
		}
	}
	return nil
}
