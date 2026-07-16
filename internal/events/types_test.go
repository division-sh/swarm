package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestProducerIdentityRequiresExactValidatedPair(t *testing.T) {
	producer, err := NewProducerIdentity(EventProducerType(" node "), " declarative-node ")
	if err != nil {
		t.Fatalf("NewProducerIdentity: %v", err)
	}
	if producer.Type() != EventProducerNode || producer.ID() != "declarative-node" {
		t.Fatalf("producer = %q/%q, want node/declarative-node", producer.Type(), producer.ID())
	}

	for _, test := range []struct {
		name         string
		producerType EventProducerType
		producerID   string
	}{
		{name: "missing type", producerID: "node-a"},
		{name: "unknown type", producerType: "worker", producerID: "node-a"},
		{name: "missing id", producerType: EventProducerNode},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProducerIdentity(test.producerType, test.producerID); err == nil {
				t.Fatalf("NewProducerIdentity(%q, %q) succeeded, want fail-closed validation", test.producerType, test.producerID)
			}
		})
	}
}

func TestProjectAndClonePreserveAllEventOwnedFacts(t *testing.T) {
	createdAt := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	envelope := EnvelopeForSourceRoute(EventEnvelope{}, RouteIdentity{FlowID: "source", FlowInstance: "source/one", EntityID: "entity-source"})
	envelope = EnvelopeForTargetSet(envelope, []RouteIdentity{
		{FlowID: "target", FlowInstance: "target/one", EntityID: "entity-one"},
		{FlowID: "target", FlowInstance: "target/two", EntityID: "entity-two"},
	})
	original := NewChildEventWithLineage(
		"event-1",
		EventType("phrase.completed"),
		NodeProducer("declarative-node"),
		"task-1",
		json.RawMessage(`{"text":"how are you"}`),
		3,
		EventLineage{RunID: "run-1", ParentEventID: "parent-1", ExecutionMode: executionmode.Mock},
		envelope,
		createdAt,
	).WithDeliveryContext(DeliveryContext{Reply: &ReplyContextRef{ID: "reply-1"}})

	cloned := original.Clone()
	projected := Project(original)
	for name, candidate := range map[string]Event{"clone": cloned, "projection": projected} {
		t.Run(name, func(t *testing.T) {
			if candidate.AdmissionClass() != EventAdmissionChild {
				t.Fatalf("admission class = %q, want preserved child", candidate.AdmissionClass())
			}
			if !candidate.Producer().Equal(original.Producer()) || candidate.SourceAgent() != "declarative-node" || candidate.ProducerType() != EventProducerNode {
				t.Fatalf("producer = %q/%q, want preserved node/declarative-node", candidate.ProducerType(), candidate.SourceAgent())
			}
			if candidate.ID() != original.ID() || candidate.Type() != original.Type() || candidate.TaskID() != original.TaskID() {
				t.Fatalf("event identity changed: got %q/%q/%q want %q/%q/%q", candidate.ID(), candidate.Type(), candidate.TaskID(), original.ID(), original.Type(), original.TaskID())
			}
			if candidate.ChainDepth() != 3 || candidate.RunID() != "run-1" || candidate.ParentEventID() != "parent-1" {
				t.Fatalf("lineage changed: depth=%d run=%q parent=%q", candidate.ChainDepth(), candidate.RunID(), candidate.ParentEventID())
			}
			if candidate.ExecutionMode() != executionmode.Mock || !candidate.CreatedAt().Equal(createdAt) {
				t.Fatalf("runtime facts changed: mode=%q created_at=%s", candidate.ExecutionMode(), candidate.CreatedAt())
			}
			if string(candidate.Payload()) != `{"text":"how are you"}` || candidate.DeliveryContext().ReplyContextID() != "reply-1" {
				t.Fatalf("payload/context changed: payload=%s reply=%q", candidate.Payload(), candidate.DeliveryContext().ReplyContextID())
			}
			if candidate.SourceRoute() != original.SourceRoute() || len(candidate.TargetRoutes()) != 2 {
				t.Fatalf("envelope changed: source=%#v targets=%#v", candidate.SourceRoute(), candidate.TargetRoutes())
			}
		})
	}

	payload := projected.Payload()
	payload[2] = 'X'
	targets := projected.TargetRoutes()
	targets[0].EntityID = "mutated"
	deliveryContext := projected.DeliveryContext()
	deliveryContext.Reply.ID = "mutated"
	if string(original.Payload()) != `{"text":"how are you"}` || original.TargetRoutes()[0].EntityID != "entity-one" || original.DeliveryContext().ReplyContextID() != "reply-1" {
		t.Fatalf("projection aliases original event-owned facts")
	}

	changed := Project(original, ProjectID("event-2"), ProjectEnvelope(EnvelopeForEntityID(EventEnvelope{}, "entity-new")))
	if changed.ID() != "event-2" || changed.EntityID() != "entity-new" {
		t.Fatalf("explicit projection changes not applied: id=%q entity=%q", changed.ID(), changed.EntityID())
	}
	if !changed.Producer().Equal(original.Producer()) || changed.RunID() != original.RunID() || changed.ExecutionMode() != original.ExecutionMode() {
		t.Fatalf("explicit projection dropped unrelated event-owned facts")
	}
}

func TestEventEnvelopeOwnsCanonicalIdentity(t *testing.T) {
	evt := NewProjectionEvent(
		"",
		"",
		PlatformProducer("runtime"),
		"",
		json.RawMessage(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`),
		0,
		"",
		"",
		EventEnvelope{
			EntityID:     "env-ent",
			FlowInstance: "review/inst-1",
		},
		time.Time{},
	)

	if got := evt.EntityID(); got != "env-ent" {
		t.Fatalf("EntityID() = %q, want env-ent", got)
	}
	if got := evt.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("FlowInstance() = %q, want review/inst-1", got)
	}
	if got := evt.Scope(); got != EventScopeEntity {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeEntity)
	}
}

func TestEventWithoutEnvelopeFailsClosedToGlobalScope(t *testing.T) {
	evt := NewProjectionEvent("", "", PlatformProducer("runtime"), "", json.RawMessage(`{"entity_id":"payload-ent","flow_instance":"payload-flow"}`), 0, "", "", EventEnvelope{}, time.Time{})

	if got := evt.EntityID(); got != "" {
		t.Fatalf("EntityID() = %q, want empty without envelope metadata", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("FlowInstance() = %q, want empty without envelope metadata", got)
	}
	if got := evt.Scope(); got != EventScopeGlobal {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeGlobal)
	}
}

func TestEventTargetSetDoesNotMaterializeFirstTargetProjection(t *testing.T) {
	envelope := EnvelopeForSourceRoute(EventEnvelope{}, RouteIdentity{
		EntityID:     "source-ent",
		FlowInstance: "source-flow",
	})
	envelope = EnvelopeForTargetSet(envelope, []RouteIdentity{
		{EntityID: "target-ent-1", FlowInstance: "target-flow-1"},
		{EntityID: "target-ent-2", FlowInstance: "target-flow-2"},
	})
	evt := NewProjectionEvent("", "", PlatformProducer("runtime"), "", nil, 0, "", "", envelope, time.Time{})

	if got := evt.EntityID(); got != "" {
		t.Fatalf("EntityID() = %q, want empty before per-recipient delivery target", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("FlowInstance() = %q, want empty before per-recipient delivery target", got)
	}
	if got := evt.Scope(); got != EventScopeGlobal {
		t.Fatalf("Scope() = %q, want %q", got, EventScopeGlobal)
	}
	if got := evt.TargetRoute(); !got.Empty() {
		t.Fatalf("TargetRoute() = %#v, want empty singular target", got)
	}
	if got := evt.TargetRoutes(); len(got) != 2 {
		t.Fatalf("TargetRoutes() count = %d, want 2", len(got))
	}

	delivered := NewProjectionEvent("", "", PlatformProducer("runtime"), "", nil, 0, "", "", EnvelopeForTargetRoute(evt.NormalizedEnvelope(), RouteIdentity{EntityID: "target-ent-2", FlowInstance: "target-flow-2"}), time.Time{})
	if got := delivered.EntityID(); got != "target-ent-2" {
		t.Fatalf("delivered EntityID() = %q, want target-ent-2", got)
	}
	if got := delivered.FlowInstance(); got != "target-flow-2" {
		t.Fatalf("delivered FlowInstance() = %q, want target-flow-2", got)
	}
	if got := delivered.TargetRoutes(); len(got) != 0 {
		t.Fatalf("delivered TargetRoutes() count = %d, want 0 after singular delivery target", len(got))
	}
}

func TestEventContextMapOmitsLegacyReceiverProjectionFields(t *testing.T) {
	envelope := EnvelopeForSourceRoute(EventEnvelope{
		EntityID:     "legacy-ent",
		FlowInstance: "legacy-flow",
		Scope:        EventScopeEntity,
	}, RouteIdentity{EntityID: "source-ent", FlowInstance: "source-flow", FlowID: "source"})
	envelope = EnvelopeForTargetRoute(envelope, RouteIdentity{EntityID: "target-ent", FlowInstance: "target-flow", FlowID: "target"})
	evt := NewProjectionEvent(
		"evt-1",
		EventType("custom.triggered"),
		PlatformProducer("runtime"),
		"task-1",
		nil,
		0,
		"run-1",
		"parent-1",
		envelope,
		time.Date(2026, 7, 3, 1, 2, 3, 0, time.UTC),
	)

	got := evt.ContextMap("ready")
	if _, ok := got["entity_id"]; ok {
		t.Fatalf("ContextMap exposed legacy entity_id = %#v", got["entity_id"])
	}
	if _, ok := got["flow_instance"]; ok {
		t.Fatalf("ContextMap exposed legacy flow_instance = %#v", got["flow_instance"])
	}
	for _, field := range []string{"id", "type", "trigger_event_type", "source_agent", "task_id", "source", "target", "source_event_id", "emitted_at", "current_state", "run_id", "scope"} {
		if _, ok := got[field]; !ok {
			t.Fatalf("ContextMap missing supported event field %q in %#v", field, got)
		}
	}
}

func TestValidateEventContextReferenceRejectsLegacyReceiverProjections(t *testing.T) {
	for _, ref := range []string{"entity_id", "flow_instance"} {
		t.Run(ref, func(t *testing.T) {
			err := ValidateEventContextReference(ref)
			if err == nil {
				t.Fatalf("expected %s to be unsupported", ref)
			}
			if !strings.Contains(err.Error(), "_entity.") {
				t.Fatalf("error = %q, want replacement guidance", err.Error())
			}
		})
	}
}

func TestValidateEventContextReferenceAllowsRouteIdentity(t *testing.T) {
	for _, ref := range []string{
		"id",
		"type",
		"source.entity_id",
		"source.flow_instance",
		"source.flow_id",
		"target.entity_id",
		"target.flow_instance",
		"target.flow_id",
		"target_set",
		"source_event_id",
		"emitted_at",
		"trigger_event_type",
		"current_state",
		"run_id",
		"scope",
	} {
		t.Run(ref, func(t *testing.T) {
			if err := ValidateEventContextReference(ref); err != nil {
				t.Fatalf("ValidateEventContextReference(%q) error = %v", ref, err)
			}
		})
	}
}

func TestEventPayloadIsImmutableThroughConstructorAndAccessor(t *testing.T) {
	payload := json.RawMessage(`{"level":"warn"}`)
	evt := NewProjectionEvent(
		"evt-1",
		EventType("diagnostic.emitted"),
		PlatformProducer("runtime"),
		"task-1",
		payload,
		0,
		"run-1",
		"parent-1",
		EventEnvelope{},
		time.Time{},
	)

	payload[10] = 'e'
	if got := string(evt.Payload()); got != `{"level":"warn"}` {
		t.Fatalf("Payload() after caller mutation = %s, want original payload", got)
	}

	got := evt.Payload()
	got[10] = 'e'
	if again := string(evt.Payload()); again != `{"level":"warn"}` {
		t.Fatalf("Payload() after accessor mutation = %s, want original payload", again)
	}
}

func TestEventProjectionMethodsReturnCopies(t *testing.T) {
	evt := NewProjectionEvent(
		"evt-1",
		EventType("root.started"),
		PlatformProducer("runtime"),
		"task-1",
		nil,
		0,
		"run-1",
		"parent-1",
		EventEnvelope{},
		time.Time{},
	)

	projected := evt.WithEntityID("entity-1").WithFlowInstance("flow/inst-1")

	if got := evt.EntityID(); got != "" {
		t.Fatalf("original EntityID() = %q, want unchanged empty identity", got)
	}
	if got := evt.FlowInstance(); got != "" {
		t.Fatalf("original FlowInstance() = %q, want unchanged empty identity", got)
	}
	if got := projected.EntityID(); got != "entity-1" {
		t.Fatalf("projected EntityID() = %q, want entity-1", got)
	}
	if got := projected.FlowInstance(); got != "flow/inst-1" {
		t.Fatalf("projected FlowInstance() = %q, want flow/inst-1", got)
	}
}

func TestDeliveryPayloadProjectionIsCanonicalAndIsolated(t *testing.T) {
	input := map[string]string{" validation_case_id ": " case-1 "}
	projection, err := NewDeliveryPayloadProjection(input)
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	input[" validation_case_id "] = "mutated"
	fields := projection.Fields()
	if fields["validation_case_id"] != "case-1" {
		t.Fatalf("projection fields = %#v, want canonical copied field", fields)
	}
	fields["validation_case_id"] = "mutated-again"
	if got := projection.Fields()["validation_case_id"]; got != "case-1" {
		t.Fatalf("projection accessor mutated owner = %q, want case-1", got)
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var roundTrip DeliveryPayloadProjection
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if roundTrip != projection {
		t.Fatalf("round trip = %#v, want %#v", roundTrip, projection)
	}
}

func TestValidateDeliveryRouteProjectionsRejectsConflictingFacts(t *testing.T) {
	first, err := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	if err != nil {
		t.Fatalf("first projection: %v", err)
	}
	second, err := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	if err != nil {
		t.Fatalf("second projection: %v", err)
	}
	route := DeliveryRoute{SubscriberType: "node", SubscriberID: "validator", Target: RouteIdentity{FlowID: "validation", FlowInstance: "validation/one"}}
	left, right := route, route
	left.PayloadProjection = first
	right.PayloadProjection = second
	if err := ValidateDeliveryRouteProjections([]DeliveryRoute{left, right}); err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
		t.Fatalf("ValidateDeliveryRouteProjections error = %v, want conflict", err)
	}
	if got := NormalizeDeliveryRoutes([]DeliveryRoute{left, right}); len(got) != 2 {
		t.Fatalf("NormalizeDeliveryRoutes merged conflicting projection facts: %#v", got)
	}
}
