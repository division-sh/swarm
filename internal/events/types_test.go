package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestConstructedEventClonePreservesAllOwnedFacts(t *testing.T) {
	source, err := NewRuntimeRoutingSource("source", "source/one", "entity-source")
	if err != nil {
		t.Fatalf("NewRuntimeRoutingSource: %v", err)
	}
	envelope := EnvelopeForTargetSet(EventEnvelope{Source: source.Route()}, []RouteIdentity{
		{FlowID: "target", FlowInstance: "target/one", EntityID: "entity-one"},
		{FlowID: "target", FlowInstance: "target/two", EntityID: "entity-two"},
	})
	createdAt := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	original, err := NewChildEvent(ChildEventInput{
		Facts: EventFacts{
			ID: "event-1", Type: "phrase.completed", Producer: ProducerClaim{Type: EventProducerNode, ID: "declarative-node"},
			TaskID: "task-1", Payload: []byte(`{"text":"how are you"}`), ChainDepth: 3,
			Envelope: envelope, RoutingSource: source, CreatedAt: createdAt, ExecutionMode: executionmode.Mock,
		},
		Lineage: EventLineage{RunID: "run-1", ParentEventID: "parent-1", TaskID: "task-1", ExecutionMode: executionmode.Mock},
	})
	if err != nil {
		t.Fatalf("NewChildEvent: %v", err)
	}
	clone := original.Clone()
	if clone.AdmissionClass() != EventAdmissionChild || !clone.Producer().Equal(original.Producer()) || clone.RoutingSource().Route() != source.Route() {
		t.Fatalf("clone lost semantic ownership: %#v", clone)
	}
	if clone.RunID() != "run-1" || clone.ParentEventID() != "parent-1" || clone.ExecutionMode() != executionmode.Mock {
		t.Fatalf("clone lineage changed: run=%q parent=%q mode=%q", clone.RunID(), clone.ParentEventID(), clone.ExecutionMode())
	}
	payload := clone.Payload()
	payload[2] = 'X'
	targets := clone.TargetRoutes()
	targets[0].EntityID = "mutated"
	if string(original.Payload()) != `{"text":"how are you"}` || original.TargetRoutes()[0].EntityID != "entity-one" {
		t.Fatal("clone aliases event-owned facts")
	}
}

func TestResolvedEnvelopeCannotRewriteRoutingSource(t *testing.T) {
	source, err := NewRuntimeRoutingSource("producer", "producer/one", "entity-one")
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewRootIngressEvent(RootIngressEventInput{Facts: EventFacts{
		Type: "task.started", Producer: ProducerClaim{Type: EventProducerExternal, ID: "provider"}, Payload: []byte(`{}`),
		Envelope: EventEnvelope{Source: source.Route()}, RoutingSource: source, ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveEnvelope(event, EventEnvelope{Source: RouteIdentity{FlowID: "other", FlowInstance: "other/one", EntityID: "other-entity"}}); err == nil {
		t.Fatal("ResolveEnvelope rewrote routing source")
	}
	resolved, err := ResolveEnvelope(event, EnvelopeForTargetRoute(event.NormalizedEnvelope(), RouteIdentity{FlowID: "target", FlowInstance: "target/one", EntityID: "target-entity"}))
	if err != nil {
		t.Fatalf("ResolveEnvelope target: %v", err)
	}
	if resolved.TargetRoute().FlowInstance != "target/one" || resolved.RoutingSource().Route() != source.Route() {
		t.Fatalf("resolved event = %#v", resolved.NormalizedEnvelope())
	}
}

func TestDeclaredIngressRoutingSourceRemainsOpaqueToEnvelopeRouting(t *testing.T) {
	source, err := NewDeclaredIngressRoutingSource("telegram-ingress", "", "entity-one", "provider_admission_plan")
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewRootIngressEvent(RootIngressEventInput{Facts: EventFacts{
		Type: "inbound.telegram", Producer: ProducerClaim{Type: EventProducerExternal, ID: "inbound-gateway"},
		Payload: []byte(`{}`), RoutingSource: source, ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceRoute() != (RouteIdentity{}) {
		t.Fatalf("declared ingress envelope source = %#v, want absent until the canonical routing evaluator interprets it", event.SourceRoute())
	}
	if got := event.RoutingSource().Route(); got != source.Route() {
		t.Fatalf("typed routing source = %#v, want %#v", got, source.Route())
	}
	resolved, err := ResolveEnvelope(event, EnvelopeForTargetRoute(event.NormalizedEnvelope(), RouteIdentity{FlowID: "telegram-chat", FlowInstance: "telegram-chat/one", EntityID: "entity-one"}))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.SourceRoute() != (RouteIdentity{}) || resolved.RoutingSource().Route() != source.Route() {
		t.Fatalf("resolved ingress source facts = envelope:%#v typed:%#v", resolved.SourceRoute(), resolved.RoutingSource().Route())
	}
}

func TestRuntimeRoutingSourceFromRouteRequiresExactClaim(t *testing.T) {
	for _, tc := range []struct {
		name      string
		route     RouteIdentity
		wantEmpty bool
		wantError bool
	}{
		{name: "absent", wantEmpty: true},
		{name: "flow context only", route: RouteIdentity{FlowID: "root"}, wantEmpty: true},
		{name: "entity fact only", route: RouteIdentity{EntityID: "entity-1"}, wantEmpty: true},
		{name: "flow and entity without instance", route: RouteIdentity{FlowID: "root", EntityID: "entity-1"}, wantError: true},
		{name: "instance without entity", route: RouteIdentity{FlowID: "root", FlowInstance: "root/one"}, wantEmpty: true},
		{name: "exact instance", route: RouteIdentity{FlowID: "root", FlowInstance: "root/one", EntityID: "entity-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := RuntimeRoutingSourceFromRoute(tc.route)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %t", err, tc.wantError)
			}
			if err == nil && source.Empty() != tc.wantEmpty {
				t.Fatalf("source empty = %t, want %t", source.Empty(), tc.wantEmpty)
			}
		})
	}
}

func TestEventDeliveryProjectionCannotPersist(t *testing.T) {
	event, err := NewRootIngressEvent(RootIngressEventInput{Facts: EventFacts{
		Type: "message.received", Producer: ProducerClaim{Type: EventProducerExternal, ID: "telegram"},
		Payload: []byte(`{"text":"hello"}`), ExecutionMode: executionmode.Live,
	}})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := NewDeliveryPayloadProjection(map[string]string{"chat_id": "123"})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := NewDeliveryEvent(event, DeliveryRoute{PayloadProjection: projection, Context: DeliveryContext{Reply: &ReplyContextRef{ID: "reply-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(delivery.JournalEvent().Payload()) != `{"text":"hello"}` {
		t.Fatalf("journal payload changed: %s", delivery.JournalEvent().Payload())
	}
	if !strings.Contains(string(delivery.Event().Payload()), `"chat_id":"123"`) || delivery.Event().DeliveryContext().ReplyContextID() != "reply-1" {
		t.Fatalf("delivery view = %s / %#v", delivery.Event().Payload(), delivery.Event().DeliveryContext())
	}
}

func TestValidateEventContextReferenceRejectsLegacyReceiverProjections(t *testing.T) {
	for _, ref := range []string{"entity_id", "flow_instance"} {
		if err := ValidateEventContextReference(ref); err == nil || !strings.Contains(err.Error(), "_entity.") {
			t.Fatalf("ValidateEventContextReference(%q) = %v", ref, err)
		}
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
	fields["validation_case_id"] = "mutated-again"
	if got := projection.Fields()["validation_case_id"]; got != "case-1" {
		t.Fatalf("projection owner mutated = %q", got)
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip DeliveryPayloadProjection
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip != projection {
		t.Fatalf("round trip = %#v, want %#v", roundTrip, projection)
	}
}

func TestValidateDeliveryRouteProjectionsRejectsConflictingFacts(t *testing.T) {
	first, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-1"})
	second, _ := NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "case-2"})
	route := DeliveryRoute{SubscriberType: "node", SubscriberID: "validator", Target: RouteIdentity{FlowID: "validation", FlowInstance: "validation/one"}}
	left, right := route, route
	left.PayloadProjection, right.PayloadProjection = first, second
	if err := ValidateDeliveryRouteProjections([]DeliveryRoute{left, right}); err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
		t.Fatalf("ValidateDeliveryRouteProjections error = %v", err)
	}
}
