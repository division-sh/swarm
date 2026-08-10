package runtime

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func newStandaloneRuntimePlatformControlEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time, posture executionposture.Posture) (events.Event, error) {
	return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformControlEventFacts(eventType, payload, envelope, createdAt, posture)})
}

func newStandaloneRuntimePlatformDiagnosticEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time, posture executionposture.Posture) (events.Event, error) {
	return events.NewStandaloneRuntimeDiagnosticEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformDiagnosticEventFacts(eventType, payload, envelope, createdAt, posture)})
}

func runtimePlatformControlEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time, posture executionposture.Posture) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NewPlatformControlRoutingSource(), CreatedAt: createdAt, ExecutionMode: posture.RootMode(),
	}
}

func runtimePlatformDiagnosticEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time, posture executionposture.Posture) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NoRoutingSource(), CreatedAt: createdAt, ExecutionMode: posture.RootMode(),
	}
}
