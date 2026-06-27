// Package eventtest owns semantic event fixture construction for tests.
//
// Production code must use internal/events constructors directly. Tests should
// choose the helper that names their fixture intent instead of constructing a
// broad projection event and patching runtime-owned envelope fields afterward.
package eventtest

import (
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

// RootIngress builds a test fixture for a root ingress event.
func RootIngress(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRootIngressEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RuntimeControl builds a test fixture for a runtime control event.
func RuntimeControl(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRuntimeControlEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RuntimeDiagnostic builds a test fixture for a runtime diagnostic event.
func RuntimeDiagnostic(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewRuntimeDiagnosticEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// DiagnosticDirect builds a test fixture for direct diagnostic persistence.
func DiagnosticDirect(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewDiagnosticDirectEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// Child builds a test fixture for a runtime child event derived from a parent.
func Child(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, parent events.Event, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewChildEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, parent, envelope, createdAt)
}

// ChildWithLineage builds a test fixture for a runtime child event when the
// parent carrier is not available in the fixture.
func ChildWithLineage(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewChildEventWithLineage(id, eventType, sourceAgent, taskID, payload, chainDepth, lineage, envelope, createdAt)
}

// Replay builds a test fixture for replaying an already-recorded event.
func Replay(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewReplayEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, lineage, envelope, createdAt)
}

// Projection builds a persisted projection/readback fixture from authoritative
// event facts. Prefer the runtime-intent helpers above when a test fixture is
// modeling a producer-created event.
func Projection(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return events.NewProjectionEvent(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// RouteProbe builds a route-resolution probe fixture.
func RouteProbe(eventType events.EventType) events.Event {
	return events.NewRouteProbeEvent(eventType)
}

// WithEnvelope keeps legacy fixture migration behind this test-only owner.
// Prefer passing the envelope to the semantic fixture constructor when possible.
func WithEnvelope(evt events.Event, envelope events.EventEnvelope) events.Event {
	return evt.WithEnvelope(envelope)
}

// WithEntityID keeps legacy fixture migration behind this test-only owner.
// Prefer passing EntityID through EventEnvelope at construction when possible.
func WithEntityID(evt events.Event, entityID string) events.Event {
	return evt.WithEntityID(entityID)
}

// WithFlowInstance keeps legacy fixture migration behind this test-only owner.
// Prefer passing FlowInstance through EventEnvelope at construction when possible.
func WithFlowInstance(evt events.Event, flowInstance string) events.Event {
	return evt.WithFlowInstance(flowInstance)
}

// WithSourceRoute keeps route-context fixture patching behind this test-only
// owner while route identity remains the concept under test.
func WithSourceRoute(evt events.Event, route events.RouteIdentity) events.Event {
	return evt.WithSourceRoute(route)
}

// WithTargetRoute keeps route-context fixture patching behind this test-only
// owner while route identity remains the concept under test.
func WithTargetRoute(evt events.Event, route events.RouteIdentity) events.Event {
	return evt.WithTargetRoute(route)
}

// WithTargetSet keeps route-context fixture patching behind this test-only owner
// while route identity remains the concept under test.
func WithTargetSet(evt events.Event, routes []events.RouteIdentity) events.Event {
	return evt.WithTargetSet(routes)
}
