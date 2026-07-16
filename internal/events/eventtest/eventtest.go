// Package eventtest owns semantic event fixture construction for tests.
//
// Production code must use internal/events constructors directly. Tests should
// choose the helper that names their fixture intent instead of constructing a
// broad projection event and patching runtime-owned envelope fields afterward.
package eventtest

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

// RootIngress builds a test fixture for a root ingress event.
func RootIngress(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRootIngressEvent(id, eventType, events.ExternalProducer(fixtureProducerID(sourceAgent, "eventtest-external")), taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RuntimeControl builds a test fixture for a runtime control event.
func RuntimeControl(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRuntimeControlEvent(id, eventType, events.PlatformProducer(fixtureProducerID(sourceAgent, "runtime")), taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RuntimeDiagnostic builds a test fixture for a runtime diagnostic event.
func RuntimeDiagnostic(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRuntimeDiagnosticEvent(id, eventType, events.PlatformProducer(fixtureProducerID(sourceAgent, "runtime")), taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// DiagnosticDirect builds a test fixture for direct diagnostic persistence.
func DiagnosticDirect(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewDiagnosticDirectEvent(id, eventType, events.PlatformProducer(fixtureProducerID(sourceAgent, "runtime")), taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// Child builds a test fixture for a runtime child event derived from a parent.
func Child(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, parent events.Event, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewChildEvent(id, eventType, events.AgentProducer(fixtureProducerID(sourceAgent, "eventtest-agent")), taskID, payload, chainDepth, parent, envelope, createdAt)
}

// ChildWithLineage builds a test fixture for a runtime child event when the
// parent carrier is not available in the fixture.
func ChildWithLineage(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewChildEventWithLineage(id, eventType, events.AgentProducer(fixtureProducerID(sourceAgent, "eventtest-agent")), taskID, payload, chainDepth, lineage, envelope, createdAt)
}

// Replay builds a test fixture for replaying an already-recorded event.
func Replay(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewReplayEvent(id, eventType, events.AgentProducer(fixtureProducerID(sourceAgent, "eventtest-agent")), taskID, payload, chainDepth, lineage, envelope, createdAt)
}

// PersistedProjection builds a persisted projection/readback fixture from
// authoritative event facts. Runtime producer fixtures should use RootIngress,
// ChildWithLineage, Replay, RuntimeControl, RuntimeDiagnostic, or
// DiagnosticDirect instead.
func PersistedProjection(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewProjectionEvent(id, eventType, events.AgentProducer(fixtureProducerID(sourceAgent, "eventtest-agent")), taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func fixtureProducerID(candidate, fallback string) string {
	candidate = strings.TrimSpace(candidate)
	if candidate != "" {
		return candidate
	}
	return fallback
}

// PersistedProjectionForProducer builds a persisted/readback fixture with an
// exact producer identity.
func PersistedProjectionForProducer(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewProjectionEvent(id, eventType, producer, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RouteProbe builds a route-resolution probe fixture.
func RouteProbe(eventType events.EventType) events.Event {
	return events.NewRouteProbeEvent(eventType)
}

// TargetRouted returns a fixture event projected onto a concrete delivery
// target route.
func TargetRouted(evt events.Event, target events.RouteIdentity) events.Event {
	return evt.WithTargetRoute(target)
}

// MalformedChildWithoutRunLineage builds the explicit negative fixture for
// admission tests that assert child events without run lineage are rejected.
func MalformedChildWithoutRunLineage(eventType events.EventType, sourceAgent string, payload json.RawMessage) events.Event {
	return events.NewChildEventWithLineage(
		"",
		eventType,
		events.AgentProducer(sourceAgent),
		"",
		payload,
		0,
		events.EventLineage{ExecutionMode: executionmode.Live},
		events.EventEnvelope{},
		time.Time{},
	)
}

// MalformedProjectionWithoutAuthoritativeFacts builds the explicit negative
// fixture for projection persistence tests that assert authoritative facts are
// required.
func MalformedProjectionWithoutAuthoritativeFacts(eventType events.EventType, sourceAgent string, payload json.RawMessage) events.Event {
	return events.NewProjectionEvent(
		"",
		eventType,
		events.AgentProducer(sourceAgent),
		"",
		payload,
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Time{},
	)
}

// MalformedProjectionWithoutAuthoritativeRun builds the explicit negative
// fixture for projection persistence tests where an event id and timestamp are
// present but run lineage is intentionally absent.
func MalformedProjectionWithoutAuthoritativeRun(id string, eventType events.EventType, sourceAgent string, payload json.RawMessage, createdAt time.Time) events.Event {
	return events.NewProjectionEvent(
		id,
		eventType,
		events.AgentProducer(sourceAgent),
		"",
		payload,
		0,
		"",
		"",
		events.EventEnvelope{},
		createdAt,
	)
}
