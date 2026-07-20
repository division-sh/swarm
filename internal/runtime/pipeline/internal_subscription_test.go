package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type recordingInternalSubscriptionBus struct {
	mode       string
	subscriber string
	eventTypes []events.EventType
}

func (b *recordingInternalSubscriptionBus) SubscribeInternal(subscriber string, eventTypes ...events.EventType) <-chan *worklifetime.EventDelivery {
	b.mode = "internal"
	b.subscriber = subscriber
	b.eventTypes = append([]events.EventType(nil), eventTypes...)
	return make(chan *worklifetime.EventDelivery, 1)
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

func TestActivityDispatcherSubscribe_UsesInternalSubscribers(t *testing.T) {
	bus := &recordingInternalSubscriptionBus{}
	node := newActivityBackgroundNode(&PipelineCoordinator{}, bus)
	ready := make(chan struct{})
	node.AddSubscriptionReadyHook(func() { close(ready) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		node.Run(ctx)
	}()
	<-ready
	cancel()
	<-done

	if bus.mode != "internal" {
		t.Fatalf("subscription mode = %q, want internal", bus.mode)
	}
	if bus.subscriber != activityDispatcherSubscriberID {
		t.Fatalf("subscriber = %q, want %q", bus.subscriber, activityDispatcherSubscriberID)
	}
	if len(bus.eventTypes) != 1 || bus.eventTypes[0] != activityRequestEventType {
		t.Fatalf("event types = %#v, want [%s]", bus.eventTypes, activityRequestEventType)
	}
}
