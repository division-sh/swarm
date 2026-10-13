package runforkpersistence

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func replayCutFixture(t *testing.T) (*runForkRevisionSnapshot, events.Event, deliverylifecycle.Snapshot) {
	t.Helper()
	snapshot, event, delivery := replayReceiverProjectionFixture(t, "observer")
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("1", 64), FlowID: ".", EventKey: string(event.Type()),
		SchemaDigest: "sha256:" + strings.Repeat("2", 64), SchemaClass: events.PayloadSchemaAuthored,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		t.Fatal(err)
	}
	event, err = events.ApplyPayloadAdmission(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Revision = 7
	snapshot.Events = []runForkRevisionEvent{genericPublicationRevisionEvent(record, event)}
	for i := range snapshot.Deliveries {
		snapshot.Deliveries[i].runForkRevisionedFact = runForkRevisionedFact{FirstRevision: 3, Revision: 5}
		snapshot.Deliveries[i].Snapshot.FinalSelection = deliverylifecycle.AbsentSelection()
	}
	delivery = snapshot.Deliveries[len(snapshot.Deliveries)-1].Snapshot
	return snapshot, event, delivery
}

func TestRunForkReplayCutRetainsExactPublicationAndReceiver(t *testing.T) {
	snapshot, event, sourceDelivery := replayCutFixture(t)
	before := projectionJSON(t, snapshot)
	cut, err := newRunForkReplayCut(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got, err := cut.event(event.ID())
	if err != nil || got.RunID() != snapshot.RunID || got.ID() != event.ID() || got.Type() != event.Type() || !bytes.Equal(got.Payload(), event.Payload()) {
		t.Fatalf("fixed event lost: %+v %v", got, err)
	}
	if binding, ok := got.PayloadAdmission(); !ok || binding.Binding().EventKey() != string(event.Type()) {
		t.Fatal("fixed schema binding lost")
	}
	delivery, err := cut.delivery(event.ID(), sourceDelivery.DeliveryID)
	if err != nil || !reflect.DeepEqual(delivery, sourceDelivery) {
		t.Fatalf("fixed delivery changed: %+v %v", delivery, err)
	}
	// A different current source story does not enter this private cut projection.
	current := sourceDelivery
	current.Status = deliverylifecycle.StatusDelivered
	current.Route.Initialization = events.ReceiverInitialization{}
	current.Route.ConnectClaim = events.ConnectExecutionClaim{}
	again, err := cut.delivery(event.ID(), sourceDelivery.DeliveryID)
	if err != nil || !reflect.DeepEqual(again, sourceDelivery) || reflect.DeepEqual(again, current) {
		t.Fatal("fixed delivery borrowed a current source story")
	}
	child, err := projectRunForkReplayInitializedReceiver(snapshot, got, again, uuid.NewString())
	if err != nil || !child.Target.ExistingEntity() || !child.Initialization.Empty() || child.ConnectClaim != sourceDelivery.Route.ConnectClaim {
		t.Fatalf("fixed receiver preparation lost exact ownership: %+v %v", child, err)
	}
	if projectionJSON(t, snapshot) != before {
		t.Fatal("cut preparation changed historical evidence")
	}
	for _, request := range []struct{ event, delivery string }{{uuid.NewString(), sourceDelivery.DeliveryID}, {event.ID(), uuid.NewString()}} {
		if _, err := cut.delivery(request.event, request.delivery); err == nil {
			t.Fatal("absent or foreign publication/delivery was reconstructed")
		}
	}
	if _, err := cut.event(uuid.NewString()); err == nil {
		t.Fatal("missing cut event was reconstructed")
	}
	if _, err := (runForkReplayCut{}).event(event.ID()); err == nil {
		t.Fatal("empty cut became publication evidence")
	}
}

func TestRunForkReplayCutRejectsForeignDuplicateAndUnversionedFacts(t *testing.T) {
	for _, fault := range []string{"nil", "run", "revision", "event_id", "event_run", "event_first", "event_before_first", "event_after_cut", "event_duplicate", "delivery_id", "delivery_event", "delivery_run", "delivery_first", "delivery_before_first", "delivery_after_cut", "delivery_duplicate"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, _, _ := replayCutFixture(t)
			switch fault {
			case "nil":
				snapshot = nil
			case "run":
				snapshot.RunID = uuid.NewString()
			case "revision":
				snapshot.Revision = 0
			case "event_id":
				snapshot.Events[0].EventID = ""
			case "event_run":
				snapshot.Events[0].RunID = uuid.NewString()
			case "event_first":
				snapshot.Events[0].FirstRevision = 0
			case "event_before_first":
				snapshot.Events[0].Revision = 2
			case "event_after_cut":
				snapshot.Events[0].Revision = 8
			case "event_duplicate":
				snapshot.Events = append(snapshot.Events, snapshot.Events[0])
			case "delivery_id":
				snapshot.Deliveries[0].Snapshot.DeliveryID = ""
			case "delivery_event":
				snapshot.Deliveries[0].Snapshot.EventID = uuid.NewString()
			case "delivery_run":
				snapshot.Deliveries[0].Snapshot.RunID = uuid.NewString()
			case "delivery_first":
				snapshot.Deliveries[0].FirstRevision = 0
			case "delivery_before_first":
				snapshot.Deliveries[0].Revision = 2
			case "delivery_after_cut":
				snapshot.Deliveries[0].Revision = 8
			case "delivery_duplicate":
				snapshot.Deliveries = append(snapshot.Deliveries, snapshot.Deliveries[0])
			}
			before := projectionJSON(t, snapshot)
			cut, err := newRunForkReplayCut(snapshot)
			if err == nil || !reflect.DeepEqual(cut, runForkReplayCut{}) {
				t.Fatalf("hostile cut became usable: %+v %v", cut, err)
			}
			if projectionJSON(t, snapshot) != before {
				t.Fatal("refusal repaired historical evidence")
			}
		})
	}
}

func TestRunForkReplayCutConsumesCanonicalEventDecoder(t *testing.T) {
	for _, fault := range []string{"payload", "schema", "nested_run", "route_settlement"} {
		t.Run(fault, func(t *testing.T) {
			snapshot, event, _ := replayCutFixture(t)
			switch fault {
			case "payload":
				snapshot.Events[0].Payload = []byte(`{"n":`)
			case "schema":
				snapshot.Events[0].PayloadSchemaDigest = ""
			case "nested_run":
				snapshot.Events[0].SelectedForkSourceEventID = uuid.NewString()
			case "route_settlement":
				snapshot.Events[0].RouteSettlement = []byte(`{"unknown":true}`)
			}
			cut, err := newRunForkReplayCut(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := cut.event(event.ID()); err == nil || !reflect.DeepEqual(got, events.Event{}) {
				t.Fatalf("corrupt event acquired admission: %+v %v", got, err)
			}
		})
	}
}
