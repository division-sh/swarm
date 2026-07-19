package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
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

func TestWorkflowEventEntityIDFallsBackToJournalEntity(t *testing.T) {
	evt := eventtest.RunCreatingRootIngress("", "task.ready", "", "", nil, 0, "", "",
		events.EnvelopeForEntityID(events.EventEnvelope{}, "source-entity"), time.Time{})

	if got := workflowEventEntityID(evt); got != "source-entity" {
		t.Fatalf("workflowEventEntityID = %q, want source-entity", got)
	}
}
