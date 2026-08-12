package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestWorkflowEventEntityIDPrefersTypedDeliveryTarget(t *testing.T) {
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, "source-entity")
	envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{
		FlowID:       "child",
		FlowInstance: "child",
		EntityID:     "receiver-entity",
	})
	evt := eventtest.RunCreatingRootIngress("", "task.ready", "", "", nil, 0, "", "", envelope, time.Time{})

	if got := workflowEventEntityID(evt); got != "receiver-entity" {
		t.Fatalf("workflowEventEntityID = %q, want receiver-entity", got)
	}
}

func TestWorkflowEventEntityIDDoesNotInterpretJournalEntityAsReceiverOwner(t *testing.T) {
	evt := eventtest.RunCreatingRootIngress("", "task.ready", "", "", nil, 0, "", "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "source-entity"), time.Time{})

	if got := workflowEventEntityID(evt); got != "" {
		t.Fatalf("workflowEventEntityID = %q, want no receiver identity", got)
	}
}

func TestWorkflowDeliveryEntityIDUsesAdmittedTargetOwner(t *testing.T) {
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, "source-entity")
	envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{
		FlowID:       "producer",
		FlowInstance: "producer",
		EntityID:     "event-target-entity",
	})
	evt := eventtest.RunCreatingRootIngress("", "task.ready", "", "", nil, 0, "", "", envelope, time.Time{})
	ctx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient("producer-node"),
		Target: events.RouteIdentity{
			FlowID:       "producer",
			FlowInstance: "producer",
			EntityID:     "persisted-target-entity",
		},
	})

	if got := workflowDeliveryEntityID(ctx, evt); got != "persisted-target-entity" {
		t.Fatalf("workflowDeliveryEntityID = %q, want persisted-target-entity", got)
	}
}
