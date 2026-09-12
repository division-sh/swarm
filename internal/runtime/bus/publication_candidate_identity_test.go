package bus

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPublicationHistoryUsesExactSourceInstance(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyPublicationTextSites(t, "template"))
	eb := &EventBus{semanticSource: source}
	scope := authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleHash)
	ctx := authoractivity.WithScope(context.Background(), scope)
	routingSource := eventtest.ConcreteTemplateRoutingSource("source", "source/first", eventtest.UUID("owner"))
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{"source/first/result.direct", true},
		{"source/second/result.direct", false},
		{"source/result.direct", false},
		{"result.direct", false},
		{"sibling/result.direct", false},
		{"source/first/sibling/result.direct", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := eventtest.ExistingRunRootIngressWithRoutingSource(eventtest.UUID(tc.name), events.EventType(tc.name), "history-test", "", nil, 0, eventtest.UUID("run"), events.EnvelopeForSourceRoute(events.EventEnvelope{}, routingSource.Route()), routingSource, time.Now().UTC())
			resolved, err := eb.withAuthorActivityEventDescriptor(ctx, event)
			if !tc.valid {
				if err == nil {
					t.Fatal("foreign/noncanonical instance selected declaration metadata")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			descriptor, found, err := authoractivity.ResolvedEventDescriptorFromContext(resolved, scope, tc.name)
			if err != nil || !found || descriptor.EventType != tc.name || descriptor.Disposition != authoractivity.StoryAuthored {
				t.Fatalf("descriptor = %#v found=%t err=%v", descriptor, found, err)
			}
			if event.Type() != events.EventType(tc.name) || event.RoutingSource() != routingSource {
				t.Fatal("history projection changed durable facts")
			}
		})
	}
}

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
	root := t.TempDir()
	write := func(path, body string) {
		t.Helper()
		path = filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("schema.yaml", "name: publication-diagnostic\n")
	write("validation/schema.yaml", "name: validation\nmode: template\ninstance: review_id\ninitial_state: active\nstates: [active]\n")
	write("validation/events.yaml", "thing.reviewed:\n  review_id: text\n")
	write("validation/nodes.yaml", `entity-writer:
  execution_type: system_node
  subscribes_to: [thing.reviewed]
  event_handlers:
    thing.reviewed:
      guard:
        id: selected_owner
        check: '_entity.id != ""'
`)
	eb := &EventBus{semanticSource: loadConnectRoutePlanCanonicalSource(t, root)}
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
