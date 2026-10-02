package pipeline

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestReceiverCompositionMissingStatePolicy(t *testing.T) {
	source := deliveryTargetOwnershipSource(t)
	evt := eventtest.RunCreatingRootIngress(eventtest.UUID("receiver-policy"), "review/one/work.ready", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	for _, name := range []string{"entityless", "materializer", "entity-reader"} {
		t.Run(name, func(t *testing.T) {
			node := pipelineNode(t, "review", name)
			handler, err := AdmitDeliveryTargetHandler(source, node)
			if err != nil {
				t.Fatal(err)
			}
			req := DeliveryTargetOwnershipRequest{Source: source, Event: evt, Recipient: events.MustNodeDeliveryRecipient(node), Handler: handler.ForEvent("work.ready"), Blueprint: events.RouteIdentity{FlowID: "review", FlowInstance: "review/one"}}
			got, err := ClassifyDeliveryTargetOwnership(req)
			if err == nil || !got.Empty() || !strings.Contains(err.Error(), "construct it before handler delivery") {
				t.Fatalf("missing construction acquired execution ownership: owner=%s err=%v", got.Code(), err)
			}
			if name == "entityless" {
				req.Blueprint.EntityID = eventtest.UUID("unavailable-exact-receiver")
				if _, err := ClassifyDeliveryTargetOwnership(req); err == nil {
					t.Fatal("exact target silently downgraded to entityless")
				}
			}
		})
	}
}
