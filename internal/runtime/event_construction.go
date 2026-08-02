package runtime

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func newStandaloneRuntimePlatformControlEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformControlEventFacts(eventType, payload, envelope, createdAt)})
}

func newStandaloneRuntimePlatformDiagnosticEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeDiagnosticEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformDiagnosticEventFacts(eventType, payload, envelope, createdAt)})
}

func runtimePlatformControlEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NewPlatformControlRoutingSource(), CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}
}

func runtimePlatformDiagnosticEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NoRoutingSource(), CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}
}
