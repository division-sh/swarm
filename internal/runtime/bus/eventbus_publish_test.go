package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimecorrelation "empireai/internal/runtime/correlation"
)

type waitInterceptor struct {
	started chan struct{}
	release chan struct{}
}

func (w waitInterceptor) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return true, nil, nil
}

type deferredChainInterceptor struct{}

func (deferredChainInterceptor) Intercept(_ context.Context, evt events.Event) (bool, []events.Event, error) {
	next := ""
	switch evt.Type {
	case events.EventType("custom.root"):
		next = "custom.middle"
	case events.EventType("custom.middle"):
		next = "custom.leaf"
	case events.EventType("custom.leaf"):
		next = "custom.final"
	default:
		return true, nil, nil
	}
	return false, []events.Event{(events.Event{
		Type:      events.EventType(next),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(evt.EntityID())}, nil
}

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
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusPublishDirect_PayloadValidatorFailureAbortsPublish(t *testing.T) {
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		PayloadValidator: func(string, []byte) error {
			return context.DeadlineExceeded
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	err = eb.PublishDirect(context.Background(), events.Event{
		Type:    "task.completed",
		Payload: []byte(`{}`),
	}, []string{"agent-a"})
	if err == nil || !errors.Is(err, runtimebus.ErrPayloadValidation) {
		t.Fatalf("expected payload validator failure, got %v", err)
	}
}

func TestEventBusWaitForQuiescenceWaitsForPublishCompletion(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	eb, err := runtimebus.NewEventBusWithOptions(runtimebus.InMemoryEventStore{}, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{waitInterceptor{started: started, release: release}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- eb.Publish(context.Background(), events.Event{Type: "task.completed", Payload: []byte(`{}`)})
	}()

	select {
	case <-started:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interceptor did not start")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForQuiescence error = %v, want deadline exceeded while publish is blocked", err)
	}

	close(release)
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("publish did not finish")
	}

	waitCtx, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if err := eb.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after publish completion: %v", err)
	}
}

func TestEventBusPublish_InterceptsMultiHopDeferredChains(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBusWithOptions(store, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{deferredChainInterceptor{}},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := eb.Publish(context.Background(), (events.Event{
		Type:      events.EventType("custom.root"),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID("ent-1")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := store.eventTypes(); len(got) < 4 || got[0] != "custom.root" || got[1] != "custom.middle" || got[2] != "custom.leaf" || got[3] != "custom.final" {
		t.Fatalf("persisted event types prefix = %v, want prefix [custom.root custom.middle custom.leaf custom.final]", got)
	}
}

func TestEventBusPublish_InheritsTraceAndParentFromInboundContext(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), events.Event{
		ID:      "evt-parent",
		Type:    events.EventType("task.started"),
		TraceID: "trace-abc",
	})
	if err := eb.Publish(ctx, events.Event{
		ID:   "evt-child",
		Type: events.EventType("task.completed"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(store.events) != 1 {
		found := false
		for _, evt := range store.events {
			if evt.ID != "evt-child" {
				continue
			}
			found = true
			if got := evt.TraceID; got != "trace-abc" {
				t.Fatalf("persisted trace_id = %q, want trace-abc", got)
			}
			if got := evt.ParentEventID; got != "evt-parent" {
				t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
			}
		}
		if !found {
			t.Fatalf("persisted events = %#v, want child event", store.events)
		}
		return
	}
	if got := store.events[0].TraceID; got != "trace-abc" {
		t.Fatalf("persisted trace_id = %q, want trace-abc", got)
	}
	if got := store.events[0].ParentEventID; got != "evt-parent" {
		t.Fatalf("persisted parent_event_id = %q, want evt-parent", got)
	}
}

func TestEventBusPublish_ZeroRecipientsDoesNotEmitContradiction(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-zero",
		Type: events.EventType("custom.no_subscribers"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "custom.no_subscribers" {
		t.Fatalf("persisted event types = %v, want [custom.no_subscribers]", got)
	}
}

func TestEventBusPublish_RuntimeLogBypassesContradictionRouting(t *testing.T) {
	store := &recordingEventStore{}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	if err := eb.Publish(context.Background(), events.Event{
		ID:   "evt-log",
		Type: events.EventType("platform.runtime_log"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := store.eventTypes()
	if len(got) != 1 || got[0] != "platform.runtime_log" {
		t.Fatalf("persisted event types = %v, want [platform.runtime_log]", got)
	}
}
