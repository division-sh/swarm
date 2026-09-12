package bus

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

func TestPublicationCandidateKeysNeverInventSourceAliases(t *testing.T) {
	for _, eventType := range []string{
		"result.done", "worker/result.done", "worker/instance-a/result.done",
		"worker/instance-b/result.done", "mailbox.card_decided",
	} {
		t.Run(eventType, func(t *testing.T) {
			source := eventtest.ConcreteTemplateRoutingSource("worker", "worker/instance-a", eventtest.UUID("source-owner"))
			event := eventtest.RunCreatingRootIngressWithRoutingSource(
				eventtest.UUID("event"), events.EventType(eventType), "", "", nil, 0, "", "",
				events.EnvelopeForFlowInstance(events.EventEnvelope{}, "worker/instance-a"), source, time.Time{},
			)
			if got := routedEventKeysForPlan(event); !reflect.DeepEqual(got, []string{eventType}) {
				t.Fatalf("candidate lookup invented publication authority: got=%v want=[%s]", got, eventType)
			}
		})
	}
}
