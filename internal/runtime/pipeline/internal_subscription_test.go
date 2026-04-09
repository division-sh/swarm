package pipeline

import (
	"context"
	"testing"

	"swarm/internal/events"
	runtimeengine "swarm/internal/runtime/engine"
)

type recordingInternalSubscriptionBus struct {
	mode       string
	subscriber string
	eventTypes []events.EventType
}

func (b *recordingInternalSubscriptionBus) Subscribe(subscriber string, eventTypes ...events.EventType) <-chan events.Event {
	b.mode = "subscribe"
	b.subscriber = subscriber
	b.eventTypes = append([]events.EventType(nil), eventTypes...)
	return make(chan events.Event, 1)
}

func (b *recordingInternalSubscriptionBus) SubscribeInternal(subscriber string, eventTypes ...events.EventType) <-chan events.Event {
	b.mode = "internal"
	b.subscriber = subscriber
	b.eventTypes = append([]events.EventType(nil), eventTypes...)
	return make(chan events.Event, 1)
}

func (*recordingInternalSubscriptionBus) Publish(context.Context, events.Event) error { return nil }
func (*recordingInternalSubscriptionBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*recordingInternalSubscriptionBus) ResolveSubscribedRecipients(string) []string { return nil }
func (*recordingInternalSubscriptionBus) LogRuntime(context.Context, RuntimeLogEntry) error {
	return nil
}
func (*recordingInternalSubscriptionBus) EngineOutbox() runtimeengine.OutboxWriter {
	return noOpEngineOutbox{}
}
func (*recordingInternalSubscriptionBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func TestPipelineCoordinatorSubscribe_UsesInternalSubscribers(t *testing.T) {
	bus := &recordingInternalSubscriptionBus{}
	pc := &PipelineCoordinator{bus: bus}

	ch := pc.subscribe()
	if ch == nil {
		t.Fatal("subscribe returned nil channel")
	}
	if bus.mode != "internal" {
		t.Fatalf("subscription mode = %q, want internal", bus.mode)
	}
	if bus.subscriber != runtimeWorkflowID {
		t.Fatalf("subscriber = %q, want %q", bus.subscriber, runtimeWorkflowID)
	}
}

func TestSystemNodeRunnerSubscribe_UsesInternalSubscribers(t *testing.T) {
	bus := &recordingInternalSubscriptionBus{}
	runner := newSystemNodeRunner("scan-orchestrator", bus, nil, func() []events.EventType {
		return []events.EventType{"custom.trigger"}
	}, func(context.Context, events.Event) error { return nil })
	if runner == nil {
		t.Fatal("newSystemNodeRunner returned nil")
	}

	ch := runner.subscribe()
	if ch == nil {
		t.Fatal("subscribe returned nil channel")
	}
	if bus.mode != "internal" {
		t.Fatalf("subscription mode = %q, want internal", bus.mode)
	}
	if bus.subscriber != "scan-orchestrator" {
		t.Fatalf("subscriber = %q, want scan-orchestrator", bus.subscriber)
	}
	if len(bus.eventTypes) != 1 || bus.eventTypes[0] != "custom.trigger" {
		t.Fatalf("event types = %#v, want [custom.trigger]", bus.eventTypes)
	}
}
