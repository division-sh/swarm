package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func TestFanOutReceiverInitializationDoesNotBecomeChildPublication(t *testing.T) {
	for _, flow := range []string{".", "worker", "parent/worker"} {
		for _, supplier := range []string{"node", "flow"} {
			if flow == "." && supplier == "flow" {
				continue // Root has no flow activation supplier.
			}
			t.Run(flow+"/"+supplier, func(t *testing.T) {
				plan, capsule := fanOutOwnershipFixture(t, flow, "materializing")
				plan.SourceRunID = uuid.NewString()
				if flow == "." {
					capsule.EntityID = plan.SourceRunID
					capsule.Route = flowidentity.StoredRoute(".", plan.SourceRunID, plan.SourceRunID)
					capsule.ProducerSource = eventtest.RootRoutingSource(plan.SourceRunID)
					capsule.Receiver.Target = events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: plan.SourceRunID, EntityID: plan.SourceRunID})
				}
				event := eventtest.ExistingRunRootIngress(uuid.NewString(), "scatter.requested", "operator", "", json.RawMessage(`{}`), 0, plan.SourceRunID, events.EventEnvelope{}, time.Now())
				capsule.Lineage = events.LineageFromEvent(event)
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(capsule.Receiver.Node), Target: capsule.Receiver.Target}
				var err error
				if supplier == "flow" {
					route.Initialization, err = events.AdmitFlowReceiverInitialization(event, route.Target)
				} else {
					route.Initialization, err = events.AdmitNodeReceiverInitialization(event, route.Target, capsule.Receiver.Node)
				}
				if err != nil {
					t.Fatal(err)
				}
				before, err := json.Marshal(route)
				if err != nil {
					t.Fatal(err)
				}
				receiver, err := fanoutobligation.ProjectExecutionReceiver(route)
				if err != nil {
					t.Fatal(err)
				}
				capsule.Receiver = &receiver
				childID := uuid.NewString()
				child, err := projectRunForkFanOutExecutionOwnership(plan, childID, capsule)
				if err != nil {
					t.Fatal(err)
				}
				assertFanOutCapsuleFieldPartition(t, capsule, child)
				raw, err := json.Marshal(child)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(raw, []byte(`"receiver_initialization"`)) || bytes.Contains(raw, []byte(`"delivery_route"`)) || bytes.Contains(raw, []byte(`"dependency"`)) {
					t.Fatalf("capsule retained publication authority: %s", raw)
				}
				var restored fanoutobligation.Capsule
				if err := json.Unmarshal(raw, &restored); err != nil || restored.Validate() != nil || !restored.Equal(child) {
					t.Fatalf("child receiver roundtrip: %v", err)
				}
				if restored.Lineage.RunID != event.RunID() || restored.Lineage.ParentEventID != event.ID() || restored.Receiver.Target.Code() != route.Target.Code() {
					t.Fatal("projection changed historical trigger or ownership kind")
				}
				after, _ := json.Marshal(route)
				if !bytes.Equal(before, after) || route.Initialization.ValidateEvent(event) != nil {
					t.Fatal("execution projection mutated source publication")
				}
				// Neither the retired full route nor smuggled initialization fields
				// can become a second reader path in the new capsule wire.
				for _, hostile := range [][]byte{
					bytes.Replace(raw, []byte(`"receiver":`), []byte(`"delivery_route":`), 1),
					bytes.Replace(raw, []byte(`"receiver":{`), []byte(`"receiver":{"receiver_initialization":null,`), 1),
				} {
					if err := json.Unmarshal(hostile, &restored); err == nil {
						t.Fatalf("publication authority accepted in execution capsule: %s", hostile)
					}
				}
			})
		}
	}
}
