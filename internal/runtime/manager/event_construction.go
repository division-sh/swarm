package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func newPlatformRuntimeControlEvent(ctx context.Context, runID, parentEventID string, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewRuntimeControlEvent(platformRuntimeEventInput(ctx, runID, parentEventID, eventType, payload, envelope, createdAt))
}

func newPlatformRuntimeDiagnosticEvent(ctx context.Context, runID, parentEventID string, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewRuntimeDiagnosticEvent(platformRuntimeEventInput(ctx, runID, parentEventID, eventType, payload, envelope, createdAt))
}

func platformRuntimeEventInput(ctx context.Context, runID, parentEventID string, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.RuntimeEventInput {
	mode := executionmode.Live
	if contextualMode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok {
		mode = executionmode.Mode(contextualMode)
	}
	return events.RuntimeEventInput{Facts: events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, CreatedAt: createdAt, ExecutionMode: mode,
	}, RunID: runID, ParentEventID: parentEventID}
}
