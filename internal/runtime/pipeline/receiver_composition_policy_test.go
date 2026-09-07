package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestReceiverCompositionMissingStatePolicy(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("receiver-policy"), "review/one/work.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	for _, test := range []struct{ node, want string }{
		{"entityless", "entityless_receiver"}, {"materializer", "materializing_entity"}, {"entity-reader", "reject"},
	} {
		t.Run(test.node, func(t *testing.T) {
			node := pipelineNode(t, "review", test.node)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			req := DeliveryTargetOwnershipRequest{Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node), Handler: handler.ForEvent("work.ready"), Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}}
			got, err := ClassifyDeliveryTargetOwnership(req)
			if test.want == "reject" {
				if err == nil {
					t.Fatal("required state absence accepted")
				}
			} else if err != nil || got.Code() != test.want {
				t.Fatalf("owner=%s err=%v want %s", got.Code(), err, test.want)
			}
			if test.node == "entityless" {
				req.Blueprint.EntityID = eventtest.UUID("unavailable-exact-receiver")
				if _, err := ClassifyDeliveryTargetOwnership(req); err == nil {
					t.Fatal("exact target silently downgraded to entityless")
				}
			}
		})
	}
}
