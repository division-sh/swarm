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
	"github.com/google/uuid"
)

const fixtureBundleHash = "bundle-v1:sha256:0000000000000000000000000000000000000000000000000000000000000000"

// AdmitPayload binds a fixture's current payload to deterministic schema
// evidence. Tests exercising real schema admission should use the runtime
// admitter instead.
func AdmitPayload(event events.Event, flowID, eventKey string) (events.Event, error) {
	admission, err := PayloadAdmission(event, flowID, eventKey)
	if err != nil {
		return events.Event{}, err
	}
	return events.ApplyPayloadAdmission(event, admission)
}

// PayloadAdmission returns deterministic schema evidence for fixture
// admitters without granting them ownership of the event value.
func PayloadAdmission(event events.Event, flowID, eventKey string) (events.PayloadAdmission, error) {
	binding, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: fixtureBundleHash, BundleSource: "ephemeral", FlowID: strings.TrimSpace(flowID),
		EventKey: strings.TrimSpace(eventKey), SchemaDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		SchemaClass: events.PayloadSchemaSchemaLess,
	})
	if err != nil {
		return events.PayloadAdmission{}, err
	}
	admission, err := events.NewPayloadAdmission(event.Payload(), binding)
	if err != nil {
		return events.PayloadAdmission{}, err
	}
	return admission, nil
}

// UUID returns a stable UUID for a semantic fixture label.
func UUID(label string) string {
	if parsed, err := uuid.Parse(strings.TrimSpace(label)); err == nil {
		return parsed.String()
	}
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm-eventtest:"+label)).String()
}

// RunCreatingRootIngress builds a root fixture authorized to create its run.
func RunCreatingRootIngress(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return RunCreatingRootIngressWithMode(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt, executionmode.Live)
}

// RunCreatingRootIngressWithRoutingSource builds a fixture with an explicit
// admitted producer-source fact. Routing tests use this instead of inferring
// source authority from event or envelope shape.
func RunCreatingRootIngressWithRoutingSource(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	if strings.TrimSpace(parentEventID) != "" {
		panic("root-ingress fixture cannot carry a causal parent")
	}
	facts := fixtureFacts(id, eventType, events.EventProducerExternal, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	facts.RoutingSource = source
	return mustEvent(events.NewRunCreatingRootIngressEvent(events.RunCreatingRootIngressEventInput{Facts: facts, RunID: runID}))
}

// RunCreatingRootIngressWithMode builds a run-creating root fixture with an
// explicit execution mode for exact persistence and duplicate tests.
func RunCreatingRootIngressWithMode(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time, mode executionmode.Mode) events.Event {
	if strings.TrimSpace(parentEventID) != "" {
		panic("root-ingress fixture cannot carry a causal parent")
	}
	return mustEvent(events.NewRunCreatingRootIngressEvent(events.RunCreatingRootIngressEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerExternal, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, mode), RunID: runID}))
}

// ExistingRunRootIngress builds a root fixture that requires its run to exist
// and remain active.
func ExistingRunRootIngress(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerExternal, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live), RunID: runID}))
}

// ExistingRunRootIngressWithRoutingSource builds an existing-run fixture with
// an explicit admitted producer-source fact for routing and replay tests.
func ExistingRunRootIngressWithRoutingSource(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID string, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	return ExistingRunRootIngressWithRoutingSourceAndMode(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, envelope, source, createdAt, executionmode.Live)
}

// ExistingRunRootIngressWithRoutingSourceAndMode builds an existing-run
// fixture with explicit routing authority and causal execution mode.
func ExistingRunRootIngressWithRoutingSourceAndMode(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID string, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time, mode executionmode.Mode) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerExternal, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, mode)
	facts.RoutingSource = source
	return mustEvent(events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: runID}))
}

// OperatorInjected builds a root operator event with optional typed reference provenance.
func OperatorInjected(id string, eventType events.EventType, producerID, taskID string, payload json.RawMessage, chainDepth int, runID string, provenance *events.OperatorReferenceProvenance, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerExternal, producerID, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	return mustEvent(events.NewOperatorInjectedEvent(events.OperatorInjectedEventInput{Facts: facts, RunID: runID, Provenance: provenance}))
}

// RuntimeControl builds a test fixture for a runtime control event.
func RuntimeControl(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	if route := facts.RoutingSource.Route(); !route.Empty() {
		var err error
		facts.RoutingSource, err = events.NewFlowOwnedControlRoutingSource(route)
		if err != nil {
			panic(err)
		}
	} else {
		facts.RoutingSource = events.NewPlatformControlRoutingSource()
	}
	return mustEvent(runtimeControlFixture(facts, runID, parentEventID))
}

// RuntimeControlWithRoutingSource builds a runtime control fixture preserving
// an exact admitted routing-source fact.
func RuntimeControlWithRoutingSource(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	facts.RoutingSource = source
	return mustEvent(runtimeControlFixture(facts, runID, parentEventID))
}

// RuntimeDiagnostic builds a test fixture for a runtime diagnostic event.
func RuntimeDiagnostic(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	facts.Envelope.Source = events.RouteIdentity{}
	facts.RoutingSource = events.NoRoutingSource()
	return mustEvent(runtimeDiagnosticFixture(facts, runID, parentEventID))
}

// DiagnosticDirect builds a test fixture for direct diagnostic persistence.
func DiagnosticDirect(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	return mustEvent(diagnosticDirectFixture(facts, runID, parentEventID))
}

// RunCreatingDiagnosticDirect builds a test fixture for the closed direct
// diagnostic subtype whose named operation creates newly allocated work.
func RunCreatingDiagnosticDirect(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerPlatform, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	return mustEvent(events.NewRunCreatingDiagnosticDirectEvent(events.RunCreatingRuntimeEventInput{Facts: facts, RunID: runID}))
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

// ChildWithLineageAndRoutingSource builds a child fixture with an exact
// admitted producer-source fact. Connect tests use it instead of deriving
// source authority from envelope or event-name shape.
func ChildWithLineageAndRoutingSource(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, events.EventProducerAgent, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode)
	facts.RoutingSource = source
	return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: lineage}))
}

// ChildForProducerWithRoutingSource builds a child fixture with exact producer
// and admitted routing-source facts.
func ChildForProducerWithRoutingSource(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode)
	facts.RoutingSource = source
	return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: lineage}))
}

// Replay builds a test fixture for replaying an already-recorded event.
func Replay(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewReplayEvent(events.ReplayEventInput{Facts: fixtureFacts(id, eventType, events.EventProducerAgent, sourceAgent, taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode), Lineage: lineage}))
}

// ReplayForProducer builds a replay fixture preserving an exact source
// producer role.
func ReplayForProducer(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, lineage events.EventLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	return mustEvent(events.NewReplayEvent(events.ReplayEventInput{
		Facts:   fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode),
		Lineage: lineage,
	}))
}

// SelectedForkReplay builds a cross-run replay fixture with an exact typed lineage owner.
func SelectedForkReplay(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, lineage events.SelectedForkLineage, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, lineage.ExecutionMode())
	return mustEvent(events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: facts, Lineage: lineage}))
}

// PersistedProjection builds a persisted projection/readback fixture from
// authoritative event facts. Runtime producer fixtures should use the exact
// class-specific construction helper instead.
func PersistedProjection(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	producerType := events.EventProducerExternal
	if strings.TrimSpace(parentEventID) != "" {
		producerType = events.EventProducerAgent
	}
	return persistedFixture(id, eventType, events.ProducerClaim{Type: producerType, ID: fixtureProducerID(sourceAgent, "eventtest-producer")}, taskID, payload, chainDepth, runID, parentEventID, envelope, createdAt)
}

// PersistedProjectionWithRoutingSource builds a persisted readback fixture
// from the exact routing-source fact stored with the event. Readback tests use
// this helper rather than reconstructing source authority from route shape.
func PersistedProjectionWithRoutingSource(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, source events.RoutingSource, createdAt time.Time) events.Event {
	producerType := events.EventProducerExternal
	if strings.TrimSpace(parentEventID) != "" {
		producerType = events.EventProducerAgent
	}
	facts := events.EventFacts{
		ID: id, Type: eventType,
		Producer: events.ProducerClaim{Type: producerType, ID: fixtureProducerID(sourceAgent, "eventtest-producer")},
		TaskID:   taskID, Payload: payload, ChainDepth: chainDepth, Envelope: envelope,
		RoutingSource: source, CreatedAt: createdAt, ExecutionMode: executionmode.Live,
	}
	if source.Kind() == events.RoutingSourceExternalIngress {
		facts.Envelope.Source = events.RouteIdentity{}
	}
	if strings.TrimSpace(parentEventID) != "" {
		return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: taskID, ExecutionMode: executionmode.Live,
		}}))
	}
	return mustEvent(events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: runID}))
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

// PersistedChildForProducer builds an exact child-event readback fixture for
// tests that need a non-agent producer role.
func PersistedChildForProducer(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	if strings.TrimSpace(parentEventID) == "" {
		panic("persisted child fixture requires parent_event_id")
	}
	facts := fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	return mustEvent(events.NewChildEvent(events.ChildEventInput{
		Facts: facts,
		Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: taskID, ExecutionMode: executionmode.Live,
		},
	}))
}

// PersistedRuntimeControlForProducer builds an exact platform runtime-control
// readback fixture.
func PersistedRuntimeControlForProducer(id string, eventType events.EventType, producer events.ProducerIdentity, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type(), producer.ID(), taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	facts.RoutingSource = runtimeControlRoutingSource(envelope)
	return mustEvent(runtimeControlFixture(facts, runID, parentEventID))
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
	facts.RoutingSource = evt.RoutingSource()
	switch evt.AdmissionClass() {
	case events.EventAdmissionRootIngress:
		panic("root-ingress fixture mutation requires an explicit run-creating or existing-run helper")
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
		return mustEvent(runtimeControlFixture(facts, evt.RunID(), evt.ParentEventID()))
	case events.EventAdmissionRuntimeDiagnostic:
		return mustEvent(runtimeDiagnosticFixture(facts, evt.RunID(), evt.ParentEventID()))
	case events.EventAdmissionDiagnosticDirect:
		return mustEvent(diagnosticDirectFixture(facts, evt.RunID(), evt.ParentEventID()))
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
	if routingSource.Kind() == events.RoutingSourceExternalIngress {
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
		var (
			routingSource events.RoutingSource
			err           error
		)
		if source.FlowInstance == source.FlowID {
			routingSource, err = events.NewStaticFlowRoutingSource(source)
		} else {
			routingSource, err = events.NewConcreteTemplateInstanceRoutingSource(source)
		}
		if err != nil {
			panic(err)
		}
		return routingSource
	}
	routingSource, err := events.NewExternalIngressRoutingSource(source.FlowID, source.EntityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		panic(err)
	}
	return routingSource
}

func RootRoutingSource(entityID string) events.RoutingSource {
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		panic(err)
	}
	return source
}

func StaticFlowRoutingSource(flowID, flowInstance, entityID string) events.RoutingSource {
	source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{
		FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
	})
	if err != nil {
		panic(err)
	}
	return source
}

func ConcreteTemplateRoutingSource(flowID, flowInstance, entityID string) events.RoutingSource {
	source, err := events.NewConcreteTemplateInstanceRoutingSource(events.RouteIdentity{
		FlowID: flowID, FlowInstance: flowInstance, EntityID: entityID,
	})
	if err != nil {
		panic(err)
	}
	return source
}

func persistedFixture(id string, eventType events.EventType, producer events.ProducerClaim, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	facts := fixtureFacts(id, eventType, producer.Type, producer.ID, taskID, payload, chainDepth, envelope, createdAt, executionmode.Live)
	if parentEventID != "" {
		return mustEvent(events.NewChildEvent(events.ChildEventInput{Facts: facts, Lineage: events.EventLineage{RunID: runID, ParentEventID: parentEventID, TaskID: taskID, ExecutionMode: executionmode.Live}}))
	}
	if producer.Type == events.EventProducerPlatform {
		facts.RoutingSource = runtimeControlRoutingSource(envelope)
		return mustEvent(runtimeControlFixture(facts, runID, ""))
	}
	return mustEvent(events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: facts, RunID: runID}))
}

func runtimeControlRoutingSource(envelope events.EventEnvelope) events.RoutingSource {
	route := envelope.Source.Normalized()
	if route.Empty() {
		return events.NewPlatformControlRoutingSource()
	}
	routingSource, err := events.NewFlowOwnedControlRoutingSource(route)
	if err != nil {
		panic(err)
	}
	return routingSource
}

func runtimeControlFixture(facts events.EventFacts, runID, parentEventID string) (events.Event, error) {
	if strings.TrimSpace(parentEventID) != "" {
		return events.NewCausalRuntimeControlEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: facts.TaskID, ExecutionMode: facts.ExecutionMode,
		}})
	}
	if strings.TrimSpace(runID) != "" {
		return events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
	}
	return events.NewStandaloneRuntimeControlEvent(events.StandaloneRuntimeEventInput{Facts: facts})
}

func runtimeDiagnosticFixture(facts events.EventFacts, runID, parentEventID string) (events.Event, error) {
	if strings.TrimSpace(parentEventID) != "" {
		return events.NewCausalRuntimeDiagnosticEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: facts.TaskID, ExecutionMode: facts.ExecutionMode,
		}})
	}
	if strings.TrimSpace(runID) != "" {
		return events.NewRunScopedRuntimeDiagnosticEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
	}
	return events.NewStandaloneRuntimeDiagnosticEvent(events.StandaloneRuntimeEventInput{Facts: facts})
}

func diagnosticDirectFixture(facts events.EventFacts, runID, parentEventID string) (events.Event, error) {
	if strings.TrimSpace(parentEventID) != "" {
		return events.NewCausalDiagnosticDirectEvent(events.CausalRuntimeEventInput{Facts: facts, Lineage: events.EventLineage{
			RunID: runID, ParentEventID: parentEventID, TaskID: facts.TaskID, ExecutionMode: facts.ExecutionMode,
		}})
	}
	if strings.TrimSpace(runID) != "" {
		return events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
	}
	return events.NewStandaloneDiagnosticDirectEvent(events.StandaloneRuntimeEventInput{Facts: facts})
}

func mustEvent(event events.Event, err error) events.Event {
	if err != nil {
		panic(err)
	}
	return event
}
