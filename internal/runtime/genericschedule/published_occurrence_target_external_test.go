package genericschedule_test

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

// The real bus projection changes recipient facts, not emission evidence.
// These are model proofs, not durable route or downstream settlement proofs.
func TestGenericSchedulePublishedOccurrenceAcceptsPreparedTargetProjection(t *testing.T) {
	for _, flow := range []string{".", "orders"} {
		t.Run(flow, func(t *testing.T) {
			runID, entityID, path := uuid.NewString(), uuid.NewString(), "orders/order-1"
			var source events.RoutingSource
			var err error
			if flow == "." {
				entityID, path = runID, ""
				source, err = events.NewRootRoutingSource(runID)
			} else {
				source, err = events.NewFlowOwnedControlRoutingSource(events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: entityID})
			}
			if err != nil {
				t.Fatal(err)
			}
			arm := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
			due := arm.Add(time.Minute)
			command := genericschedule.AdmissionCommand{ScheduleKey: "projection-proof", RunID: runID, EntityID: entityID, FlowInstance: path,
				OwnerKind: genericschedule.OwnerSystem, OwnerID: "runtime", EventType: "platform.generic_schedule_proof",
				Payload: semanticvalue.EmptyObject(), RoutingSource: source, ExecutionMode: executionmode.Live,
				Due: genericschedule.AbsoluteDue(due), TaskID: "projection-proof"}
			hash, err := command.ImmutableHash()
			if err != nil {
				t.Fatal(err)
			}
			activation := genericschedule.Activation{ID: uuid.NewString(), Command: command, ImmutableHash: hash,
				AdmittedAt: arm, InitialDueAt: due, CurrentDueAt: due, CurrentEventAdmittedAt: due.Add(time.Second),
				Status: genericschedule.StatusFired, FiredAt: due.Add(2 * time.Second), AcceptedAt: due.Add(2 * time.Second)}
			activation.CurrentEventID = genericschedule.OccurrenceEventID(activation.ID, due)
			if err := activation.Validate(); err != nil {
				t.Fatal(err)
			}
			event, err := events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{RunID: runID, Facts: events.EventFacts{
				ID: activation.CurrentEventID, Type: events.EventType(command.EventType),
				Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: genericschedule.OccurrenceProducerID()},
				TaskID:   command.TaskID, Payload: []byte(`{}`), RoutingSource: source,
				Envelope: events.EventEnvelope{EntityID: entityID, FlowInstance: path}, CreatedAt: due, ExecutionMode: executionmode.Live,
			}})
			if err != nil {
				t.Fatal(err)
			}
			first := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "worker", "first")),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/one", EntityID: uuid.NewString()})}
			second := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "worker", "second")),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/two", EntityID: uuid.NewString()})}
			for _, routes := range [][]events.DeliveryRoute{{first}, {first, second}} {
				projected, changed, err := bus.ResolvePreparedPublishEventTargetProjection(event, routes)
				if err != nil || !changed || projected.RoutingSource() != source || !events.SameRouteIdentity(projected.Envelope().Source, event.Envelope().Source) {
					t.Fatalf("canonical bus target projection: changed=%t err=%v", changed, err)
				}
				got, err := activation.ValidatePublishedOccurrence(projected)
				if err != nil || got.EventID != activation.CurrentEventID || !got.DueAt.Equal(due) || !got.AdmittedAt.Equal(activation.CurrentEventAdmittedAt) {
					t.Fatalf("recipient target projection rejected immutable emission: occurrence=%+v err=%v", got, err)
				}
			}
		})
	}
}
