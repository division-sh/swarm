package manager

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func newPlatformCausalRuntimeControlEvent(lineage events.EventLineage, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewCausalRuntimeControlEvent(events.CausalRuntimeEventInput{Facts: platformRuntimeControlEventFacts(lineage.ExecutionMode, eventType, payload, envelope, createdAt), Lineage: lineage})
}

func newPlatformCausalRuntimeDiagnosticEvent(lineage events.EventLineage, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewCausalRuntimeDiagnosticEvent(events.CausalRuntimeEventInput{Facts: platformRuntimeDiagnosticEventFacts(lineage.ExecutionMode, eventType, payload, envelope, createdAt), Lineage: lineage})
}

func newPlatformStandaloneRuntimeControlEvent(posture executionposture.Posture, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: platformRuntimeControlEventFacts(posture.RootMode(), eventType, payload, envelope, createdAt)})
}

func newPlatformStandaloneRuntimeDiagnosticEvent(mode executionmode.Mode, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	return events.NewStandaloneRuntimeDiagnosticEvent(events.StandaloneRuntimeEventInput{Facts: platformRuntimeDiagnosticEventFacts(mode, eventType, payload, envelope, createdAt)})
}

func newPlatformContextualRuntimeDiagnosticEvent(ctx context.Context, posture executionposture.Posture, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) (events.Event, error) {
	if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
		return newPlatformCausalRuntimeDiagnosticEvent(events.LineageFromEvent(inbound), eventType, payload, envelope, createdAt)
	}
	mode := posture.RootMode()
	if contextualMode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok {
		mode = executionmode.Mode(contextualMode)
	}
	return newPlatformStandaloneRuntimeDiagnosticEvent(mode, eventType, payload, envelope, createdAt)
}

func platformRuntimeControlEventFacts(mode executionmode.Mode, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NewPlatformControlRoutingSource(), CreatedAt: createdAt, ExecutionMode: mode,
	}
}

func platformRuntimeDiagnosticEventFacts(mode executionmode.Mode, eventType events.EventType, payload json.RawMessage, envelope events.EventEnvelope, createdAt time.Time) events.EventFacts {
	return events.EventFacts{
		Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload: payload, Envelope: envelope, RoutingSource: events.NoRoutingSource(), CreatedAt: createdAt, ExecutionMode: mode,
	}
}
