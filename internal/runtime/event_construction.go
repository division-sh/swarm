package runtime

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func newRuntimePlatformControlEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewRuntimeControlEvent(events.RuntimeEventInput{Facts: runtimePlatformEventFacts(eventType, payload, envelope, createdAt)})
}

func newRuntimePlatformDiagnosticEvent(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewRuntimeDiagnosticEvent(events.RuntimeEventInput{Facts: runtimePlatformEventFacts(eventType, payload, envelope, createdAt)})
}

func runtimePlatformEventFacts(eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}
}
