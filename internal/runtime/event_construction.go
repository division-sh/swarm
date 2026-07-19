package runtime

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func newStandaloneRuntimePlatformControlEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformEventFacts(eventType, payload, envelope, createdAt)})
}

func newStandaloneRuntimePlatformDiagnosticEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeDiagnosticEvent(events.StandaloneRuntimeEventInput{Facts: runtimePlatformEventFacts(eventType, payload, envelope, createdAt)})
}

func runtimePlatformEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}
}
