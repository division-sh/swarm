package bus_test

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
)

func TestEventBusPublish_UsesPayloadValidator(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(eventType string, payload []byte) error {
			if strings.TrimSpace(eventType) != "task.completed" {
				t.Fatalf("unexpected event type %q", eventType)
			}
			if string(payload) != `{"ok":true}` {
				t.Fatalf("unexpected payload %s", string(payload))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEventBusPublish_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.Publish(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	})
	if err == nil || !strings.Contains(err.Error(), "payload validation for task.completed") {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}
