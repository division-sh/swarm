package bus

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPublicationCandidateKeysNeverInventSourceAliases(t *testing.T) {
	for _, eventType := range []string{
		"result.done", "worker/result.done", "worker/instance-a/result.done",
		"worker/instance-b/result.done", "mailbox.card_decided",
	} {
		t.Run(eventType, func(t *testing.T) {
			source := eventtest.ConcreteTemplateRoutingSource("worker", "worker/instance-a", eventtest.UUID("source-owner"))
			event := eventtest.RunCreatingRootIngressWithRoutingSource(
				eventtest.UUID("event"), events.EventType(eventType), "", "", nil, 0, "", "",
				events.EnvelopeForFlowInstance(events.EventEnvelope{}, "worker/instance-a"), source, time.Time{},
			)
			if got := routedEventKeysForPlan(event); !reflect.DeepEqual(got, []string{eventType}) {
				t.Fatalf("candidate lookup invented publication authority: got=%v want=[%s]", got, eventType)
			}
		})
	}
}

func TestPublicationDiagnosticDoesNotInventReceiverLocalIdentity(t *testing.T) {
	eb := &EventBus{semanticSource: semanticview.Wrap(routedNodeStaticValidationBundle())}
	subscriber := Subscriber{
		Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, "validation", "entity-writer")),
		Path:      "validation", MatchPattern: "validation/thing.reviewed",
	}
	for _, tc := range []struct{ event, want string }{
		{"validation/thing.reviewed", "thing.reviewed"},
		{"thing.reviewed", "thing.reviewed"},
		{"other/thing.reviewed", ""},
		{"validation/sibling/thing.reviewed", ""},
		{"validation/not.declared", ""},
	} {
		if got := eb.localizedSubscriberEvent(tc.event, subscriber); got != tc.want {
			t.Fatalf("%s diagnostic=%q want=%q", tc.event, got, tc.want)
		}
	}
	if got := (&EventBus{}).localizedSubscriberEvent("foreign/thing.reviewed", subscriber); got != "" {
		t.Fatalf("missing declaration fabricated diagnostic=%q", got)
	}
	subscriber.LocalizedEvent = "thing.reviewed"
	if got := eb.localizedSubscriberEvent("producer/different.output", subscriber); got != "thing.reviewed" {
		t.Fatalf("admitted renamed connect lost receiver input: %q", got)
	}
}

func TestRoutedNodeCarrierUsesExactDeclarationAndExecutionScope(t *testing.T) {
	const declaration = "left/child"
	const instance = "left/parent-instance/child/child-instance"
	source := eventtest.ConcreteTemplateRoutingSource(declaration, instance, eventtest.UUID("owner"))
	event := eventtest.RunCreatingRootIngressWithRoutingSource(
		eventtest.UUID("event"), events.EventType(instance+"/result.done"), "", "", nil, 0, "", "",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, instance), source, time.Time{},
	)
	for _, tc := range []struct {
		name, flow, path string
		match            bool
	}{
		{"exact nested", declaration, instance, true},
		{"declaration placeholder", declaration, declaration, false},
		{"sibling instance", declaration, "left/other/child/child-instance", false},
		{"same leaf wrong declaration", "right/child", instance, false},
		{"parent declaration", "left", instance, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subscriber := Subscriber{Recipient: events.MustNodeDeliveryRecipient(testFlowNode(t, tc.flow, "listener")), Path: tc.path}
			if got := routedNodeMatchesConcreteFlowInstanceEvent(event, subscriber); got != tc.match {
				t.Fatalf("same-instance authority=%t want=%t", got, tc.match)
			}
			aliases := routedNodeInternalSubscriptionAliases(event, []Subscriber{subscriber})
			want := []string{string(event.Type())}
			if tc.match {
				want = append(want, declaration+"/result.done")
			}
			if !reflect.DeepEqual(aliases, want) {
				t.Fatalf("aliases=%v want=%v", aliases, want)
			}
			if event.Type() != events.EventType(instance+"/result.done") || event.RoutingSource() != source {
				t.Fatal("receiver carrier mutated producer")
			}
		})
	}
}
