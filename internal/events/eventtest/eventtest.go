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
	return RootIngressWithMode(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt, executionmode.Live)
}

// RootIngressWithMode builds a root-ingress fixture with an explicit causal
// execution mode for exact persistence and duplicate tests.
func RootIngressWithMode(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time, mode executionmode.Mode) events.Event {
	if strings.TrimSpace(parentEventID) != "" {
		panic("root-ingress fixture cannot carry a causal parent")
	}
	return mustEvent(events.NewRootIngressEvent(events.RootIngressEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerExternal, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, mode), RunID: runID}))
}

// OperatorInjected builds a root operator event with optional typed reference provenance.
func OperatorInjected(id string, eventType events.EventType, producerID, taskID string, payload json.RawMessage, chainDepth int, runID string, provenance *events.OperatorReferenceProvenance, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, producerID, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	return mustEvent(events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{Facts: facts, RunID: runID, Provenance: provenance}))
}

// RuntimeControl builds a test fixture for a runtime control event.
func RuntimeControl(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewRuntimeControlEvent(events.RuntimeEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live), RunID: runID, ParentEventID: parentEventID}))
}

// RuntimeDiagnostic builds a test fixture for a runtime diagnostic event.
func RuntimeDiagnostic(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewRuntimeDiagnosticEvent(events.RuntimeEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live), RunID: runID, ParentEventID: parentEventID}))
}

// DiagnosticDirect builds a test fixture for direct diagnostic persistence.
func DiagnosticDirect(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewDiagnosticDirectEvent(events.DiagnosticDirectEventInput{
		Facts: fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live),
		RunID: runID, ParentEventID: parentEventID,
	}))
}

// Child builds a test fixture for a runtime child event derived from a parent.
func Child(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, parent events.Event, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return ChildWithLineage(id, eventType, sourceAgent, taskID, payload, chainDepth, events.LineageFromEvent(parent), envelope, createdAt)
}

// ChildWithLineage builds a test fixture for a runtime child event when the
// parent carrier is not available in the fixture.
func ChildWithLineage(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerAgent, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode), Lineage: lineage}))
}

// Replay builds a test fixture for replaying an already-recorded event.
func Replay(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewReplayEvent(events.ReplayEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerAgent, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode), Lineage: lineage}))
}

// SelectedForkReplay builds a cross-run replay fixture with an exact typed lineage owner.
func SelectedForkReplay(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, lineage events.SelectedForkLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode())
	return mustEvent(events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: facts, Lineage: lineage}))
}

// PersistedProjection builds a persisted projection/readback fixture from
// authoritative event facts. Runtime producer fixtures should use RootIngress,
// ChildWithLineage, Replay, RuntimeControl, RuntimeDiagnostic, or
// DiagnosticDirect instead.
func PersistedProjection(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return persistedFixture(id, eventType, events.ProducerClaim{Type: events.EventProducerAgent, ID: fixtureProducerID(sourceAgent, "eventtest-agent")}, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
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
	return persistedFixture(id, eventType, events.ProducerClaim{Type: producer.Type(), ID: producer.ID()}, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

func Producer(producerType events.EventProducerType, id string) events.ProducerIdentity {
	producer, err := events.NewProducerIdentity(producerType, id)
	if err != nil {
		panic(err)
	}
	return producer
}

// RouteProbe builds a route-resolution probe fixture.
func RouteProbe(eventType events.EventType) events.RouteProbe {
	probe, err := events.NewRouteProbe(eventType)
	if err != nil {
		panic(err)
	}
	return probe
}

// TargetRouted returns a fixture event projected onto a concrete delivery
// target route.
func TargetRouted(evt events.Event, target events.RouteIdentity) events.Event {
	resolved, err := events.ResolveEnvelope(evt, events.EnvelopeForTargetRoute(evt.NormalizedEnvelope(), target))
	if err != nil {
		panic(err)
	}
	return resolved
}

// ForDelivery attaches platform-owned delivery context to a test fixture.
func ForDelivery(evt events.Event, deliveryContext events.DeliveryContext) events.Event {
	delivery, err := events.NewDeliveryEvent(evt, events.DeliveryRoute{Context: deliveryContext})
	if err != nil {
		panic(err)
	}
	return delivery.Event()
}

// InExecutionMode applies an explicit execution mode to a test fixture.
func InExecutionMode(evt events.Event, mode executionmode.Mode) events.Event {
	return rebuild(evt, evt.TaskID(), mode, evt.NormalizedEnvelope())
}

func WithTaskID(evt events.Event, taskID string) events.Event {
	return rebuild(evt, taskID, evt.ExecutionMode(), evt.NormalizedEnvelope())
}

func WithEnvelope(evt events.Event, envelope events.EventEnvelope) events.Event {
	return rebuild(evt, evt.TaskID(), evt.ExecutionMode(), envelope)
}

func rebuild(evt events.Event, taskID string, mode executionmode.Mode, envelope events.EventEnvelope) events.Event {
	facts := fixtureFacts(evt.ID(), evt.Type(), evt.ProducerType(), evt.SourceAgent(), taskID, evt.Payload(), evt.ChainDepth(), envelope, evt.CreatedAt(), mode)
	facts.RoutingSource = fixtureRoutingSource(envelope)
	switch evt.AdmissionClass() {
	case events.EventAdmissionRootIngress:
		return mustEvent(events.NewRootIngressEvent(events.RootIngressEventInput{Facts: facts, RunID: evt.RunID()}))
	case events.EventAdmissionOperatorInjected:
		var provenance *events.OperatorReferenceProvenance
		if value, ok := evt.OperatorReference(); ok {
			provenance = &value
		}
		return mustEvent(events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{Facts: facts, RunID: evt.RunID(), Provenance: provenance}))
	case events.EventAdmissionChild:
		return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: events.EventLineage{RunID: evt.RunID(), ParentEventID: evt.ParentEventID(), TaskID: taskID, ExecutionMode: mode}}))
	case events.EventAdmissionReplay:
		return mustEvent(events.NewReplayEvent(events.ReplayEventInput{Facts: facts, Lineage: events.EventLineage{RunID: evt.RunID(), ParentEventID: evt.ParentEventID(), TaskID: taskID, ExecutionMode: mode}}))
	case events.EventAdmissionRuntimeControl:
		return mustEvent(events.NewRuntimeControlEvent(events.RuntimeEventInput{Facts: facts, RunID: evt.RunID(), ParentEventID: evt.ParentEventID()}))
	case events.EventAdmissionRuntimeDiagnostic:
		return mustEvent(events.NewRuntimeDiagnosticEvent(events.RuntimeEventInput{Facts: facts, RunID: evt.RunID(), ParentEventID: evt.ParentEventID()}))
	case events.EventAdmissionDiagnosticDirect:
		return mustEvent(events.NewDiagnosticDirectEvent(events.DiagnosticDirectEventInput{Facts: facts, RunID: evt.RunID(), ParentEventID: evt.ParentEventID()}))
	case events.EventAdmissionSelectedForkReplay:
		lineage, ok := evt.SelectedForkLineage()
		if !ok {
			panic("selected-fork fixture has no lineage")
		}
		updated, err := events.NewSelectedForkLineage(lineage.DestinationRunID(), lineage.SourceRunID(), lineage.SourceEventID(), lineage.AuthorityStamp(), taskID, mode)
		if err != nil {
			panic(err)
		}
		return mustEvent(events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: facts, Lineage: updated}))
	default:
		panic("unsupported event fixture class")
	}
}

func fixtureFacts(id string, eventType events.EventType, producerType events.EventProducerType, producerID, taskID string, payload json.RawMessage, chainDepth int, envelope events.EventEnvelope, createdAt time.Time, mode executionmode.Mode) events.EventFacts {
	producerID = fixtureProducerID(producerID, "eventtest-producer")
	routingSource := fixtureRoutingSource(envelope)
	if routingSource.Kind() == events.RoutingSourceDeclaredIngress {
		envelope.Source = events.RouteIdentity{}
	}
	return events.EventFacts{
		ID: id, Type: eventType, Producer: events.ProducerClaim{Type: producerType, ID: producerID},
		TaskID: taskID, Payload: payload, ChainDepth: chainDepth, Envelope: envelope,
		RoutingSource: routingSource, CreatedAt: createdAt, ExecutionMode: mode,
	}
}

func fixtureRoutingSource(envelope events.EventEnvelope) events.RoutingSource {
	source := envelope.Source.Normalized()
	if source.Empty() {
		return events.NoRoutingSource()
	}
	if source.FlowInstance != "" {
		routingSource, err := events.NewRuntimeRoutingSource(source.FlowID, source.FlowInstance, source.EntityID)
		if err != nil {
			panic(err)
		}
		return routingSource
	}
	routingSource, err := events.NewDeclaredIngressRoutingSource(source.FlowID, source.FlowInstance, source.EntityID, "eventtest")
	if err != nil {
		panic(err)
	}
	return routingSource
}

func persistedFixture(id string, eventType events.EventType, producer events.ProducerClaim, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type, producer.ID, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	if parentEventID != "" {
		return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: events.EventLineage{RunID: runID, ParentEventID: parentEventID, TaskID: taskID, ExecutionMode: executionmode.Live}}))
	}
	return mustEvent(events.NewRootIngressEvent(events.RootIngressEventInput{Facts: facts, RunID: runID}))
}

func mustEvent(event events.Event, err error) events.Event {
	if err != nil {
		panic(err)
	}
	return event
}
