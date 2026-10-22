package pipelinepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func continuedRecipientFixture(t *testing.T, status deliverylifecycle.Status) (eventrecord.AdmittedRecord, []deliverylifecycle.Snapshot, string) {
	t.Helper()
	runID, sourceRunID, sourceEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(runID, sourceRunID, sourceEventID, "fixture-selected-authority", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "continued")
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.SelectedForkReplay(uuid.NewString(), "item.ready", eventtest.Producer(events.EventProducerNode, node.Key()), "", []byte(`{}`), 0, lineage, events.EventEnvelope{}, time.Now().UTC().Truncate(time.Microsecond))
	event, err = eventtest.AdmitPayload(event, "", "item.ready")
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteHistoricalRunForkReplay, events.ConnectEvaluationLedger{})
	if err != nil {
		t.Fatal(err)
	}
	target, err := events.NewEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}
	routeID, err := route.Identity()
	if err != nil {
		t.Fatal(err)
	}
	deliveryID, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	row := deliverylifecycle.Snapshot{DeliveryID: deliveryID, EventID: event.ID(), RunID: runID, Route: route, RouteIdentity: routeID,
		SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: node.Key(), Status: status}
	if status == deliverylifecycle.StatusCanceled {
		row.ReasonCode, row.SettledAt = "terminate", event.CreatedAt()
	}
	return eventrecord.AdmittedRecord{Event: admitted, Settlement: settlement}, []deliverylifecycle.Snapshot{row}, sourceEventID
}

func TestContinuedFanOutRecipientKeepsExactLineageAndPendingDisposition(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   deliverylifecycle.Status
		retained fanoutobligation.InheritedTerminalDisposition
		want     fanoutobligation.InheritedTerminalDisposition
		terminal bool
	}{
		{"pending_after_failure", deliverylifecycle.StatusPending, "dead_lettered", "", false},
		{"retry_after_cancellation", deliverylifecycle.StatusFailed, "canceled", "", false},
		{"executing_after_failure", deliverylifecycle.StatusInProgress, "dead_lettered", "", false},
		{"failure_retained", deliverylifecycle.StatusDelivered, "dead_lettered", "dead_lettered", true},
		{"cancellation_retained", deliverylifecycle.StatusDelivered, "canceled", "canceled", true},
		{"child_failure", deliverylifecycle.StatusDeadLetter, "canceled", "dead_lettered", true},
		{"child_cancellation", deliverylifecycle.StatusCanceled, "succeeded", "canceled", true},
		{"no_source_terminal_partition", deliverylifecycle.StatusDelivered, "", "succeeded", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			publication, deliveries, sourceEventID := continuedRecipientFixture(t, test.status)
			before := append([]deliverylifecycle.Snapshot(nil), deliveries...)
			got, terminal, err := continuedFanOutRecipientDisposition(publication.Event.Event().RunID(), sourceEventID, publication, deliveries, test.retained)
			if err != nil || got != test.want || terminal != test.terminal || !reflect.DeepEqual(before, deliveries) {
				t.Fatalf("continued recipient result=%s terminal=%t err=%v; want=%s terminal=%t, unchanged=%t", got, terminal, err, test.want, test.terminal, reflect.DeepEqual(before, deliveries))
			}
		})
	}
}

func TestContinuedFanOutRecipientRefusesCrossedAndMissingEvidence(t *testing.T) {
	for _, fault := range []string{"source_event", "child_run", "delivery_run", "delivery_event", "missing_recipients", "no_route_contribution", "ordinary_publication"} {
		t.Run(fault, func(t *testing.T) {
			publication, deliveries, sourceEventID := continuedRecipientFixture(t, deliverylifecycle.StatusDelivered)
			runID, retained := publication.Event.Event().RunID(), fanoutobligation.InheritedDeadLettered
			switch fault {
			case "source_event":
				sourceEventID = uuid.NewString()
			case "child_run":
				runID = uuid.NewString()
			case "delivery_run":
				deliveries[0].RunID = uuid.NewString()
			case "delivery_event":
				deliveries[0].EventID = uuid.NewString()
			case "missing_recipients":
				deliveries = nil
			case "no_route_contribution":
				retained = fanoutobligation.InheritedNoRoute
			case "ordinary_publication":
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "item.ready", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
				var err error
				event, err = eventtest.AdmitPayload(event, "", "item.ready")
				if err != nil {
					t.Fatal(err)
				}
				publication.Event, err = events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
			}
			got, terminal, err := continuedFanOutRecipientDisposition(runID, sourceEventID, publication, deliveries, retained)
			if err == nil || got != "" || terminal {
				t.Fatalf("crossed evidence gained settlement: %s %t %v", got, terminal, err)
			}
		})
	}
}
