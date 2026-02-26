package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
)

func TestEventBus_DeliverByType_UsesSubscriptions(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("a1", events.EventType("foo.*"))

	bus.deliverByType(events.Event{
		ID:          "e1",
		Type:        events.EventType("foo.bar"),
		SourceAgent: "x",
	})

	select {
	case evt := <-ch:
		if string(evt.Type) != "foo.bar" {
			t.Fatalf("unexpected evt: %s", evt.Type)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected delivered event")
	}
}

func TestEventBus_PublishRejectsInvalidEventType(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	err := bus.Publish(context.Background(), events.Event{
		ID:          "e-invalid",
		Type:        events.EventType("Bad Event"),
		SourceAgent: "x",
	})
	if err == nil {
		t.Fatal("expected invalid event type error")
	}
}

func TestIsValidEventTypeName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{name: "valid", in: "scan.requested", want: true},
		{name: "valid underscore", in: "heartbeat.opco_ceo", want: true},
		{name: "empty", in: "", want: false},
		{name: "space", in: "scan requested", want: false},
		{name: "uppercase", in: "Scan.Requested", want: false},
		{name: "slash", in: "scan/requested", want: false},
		{name: "empty token", in: "scan..requested", want: false},
	}
	for _, tc := range cases {
		if got := isValidEventTypeName(tc.in); got != tc.want {
			t.Fatalf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestEventBusFactoryRoutingClassification(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	if !bus.isFactoryEvent(events.EventType("opco.spinup_requested")) {
		t.Fatal("expected opco.* to be classified as factory event")
	}
	if !bus.isFactoryEvent(events.EventType("validation.started")) {
		t.Fatal("expected validation.* to be classified as factory event")
	}
	if bus.isFactoryEvent(events.EventType("bug_reported")) {
		t.Fatal("expected short OpCo event to be non-factory")
	}
	if bus.isFactoryEvent(events.EventType("qa.validation_failed")) {
		t.Fatal("expected qa.* to be non-factory (OpCo internal)")
	}
}
