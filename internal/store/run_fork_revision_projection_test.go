package store

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
)

func TestRunForkRevisionProjectionPreservesSourceRouteSeparatelyFromReceiverContext(t *testing.T) {
	at := time.Unix(1700001000, 0).UTC()
	snapshot := &runForkRevisionSnapshot{
		Events: []runForkRevisionEvent{{
			EventID:      "event-1",
			EventName:    "producer/inst-1/scan.requested",
			FlowInstance: "consumer/inst-9",
			SourceRoute: events.RouteIdentity{
				FlowID:       " producer ",
				FlowInstance: "/producer/inst-1/",
				EntityID:     "source-entity",
			},
		}},
		Deliveries: []runForkRevisionDelivery{{
			Snapshot: runtimedelivery.Snapshot{
				DeliveryID:      "delivery-1",
				EventID:         "event-1",
				SubscriberClass: runtimedelivery.SubscriberNode,
				SubscriberID:    "source-node",
				Status:          runtimedelivery.StatusPending,
				CreatedAt:       at,
			},
		}},
	}

	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		t.Fatalf("loadRunForkPendingWorkFromRevision: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending work = %#v, want one row", pending)
	}
	if got, want := pending[0].FlowInstance, "consumer/inst-9"; got != want {
		t.Fatalf("receiver flow instance = %q, want %q", got, want)
	}
	if got, want := pending[0].SourceRoute, (events.RouteIdentity{FlowID: "producer", FlowInstance: "producer/inst-1", EntityID: "source-entity"}); got != want {
		t.Fatalf("source route = %#v, want %#v", got, want)
	}

	facts := loadRunForkSourceFactsFromRevision(snapshot, nil)
	if _, ok := stringSliceSet(facts.FlowInstances)["consumer/inst-9"]; !ok {
		t.Fatalf("receiver flow instances = %v, want consumer/inst-9", facts.FlowInstances)
	}
	if _, ok := stringSliceSet(facts.SourceFlows)["producer"]; !ok {
		t.Fatalf("source flows = %v, want producer", facts.SourceFlows)
	}
	if _, ok := stringSliceSet(facts.SourceFlows)["consumer"]; ok {
		t.Fatalf("source flows = %v, receiver context must not become source identity", facts.SourceFlows)
	}
}

func TestRunForkRevisionProjectionRejectsMalformedSourceRoute(t *testing.T) {
	snapshot := &runForkRevisionSnapshot{}
	err := appendRunForkRevisionFact(
		snapshot,
		runforkrevision.FamilyEvents,
		runForkRevisionedFact{FirstRevision: 1, Revision: 1},
		[]byte(`{"event_id":"event-1","event_name":"producer/scan.requested","source_route":{"flow_instance":17}}`),
	)
	if err == nil || !strings.Contains(err.Error(), "decode run fork events revision fact") {
		t.Fatalf("malformed source route error = %v, want typed revision decode failure", err)
	}
}
