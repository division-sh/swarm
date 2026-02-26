package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"empireai/internal/events"
)

type atomicStoreStub struct {
	mu            sync.Mutex
	appendCalls   int
	insertCalls   int
	atomicCalls   int
	lastDeliverTo []string
}

func (s *atomicStoreStub) AppendEvent(_ context.Context, _ events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendCalls++
	return nil
}

func (s *atomicStoreStub) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertCalls++
	return nil
}

func (s *atomicStoreStub) PersistEventWithDeliveries(_ context.Context, _ events.Event, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.atomicCalls++
	s.lastDeliverTo = append([]string(nil), agentIDs...)
	return nil
}

func TestEventBusPublish_UsesAtomicPersistenceWhenAvailable(t *testing.T) {
	store := &atomicStoreStub{}
	bus := NewEventBus(store)
	ch := bus.Subscribe("empire-coordinator", events.EventType("system.directive"))

	evt := events.Event{
		ID:          "4f72f905-14fc-4769-bf3d-817a83953f9a",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive_text":"go"}`),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("expected subscribed delivery")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.atomicCalls != 1 {
		t.Fatalf("expected atomic persistence call=1, got %d", store.atomicCalls)
	}
	if store.appendCalls != 0 || store.insertCalls != 0 {
		t.Fatalf("expected non-atomic writes skipped, append=%d insert=%d", store.appendCalls, store.insertCalls)
	}
	if len(store.lastDeliverTo) != 1 || store.lastDeliverTo[0] != "empire-coordinator" {
		t.Fatalf("unexpected atomic recipients: %#v", store.lastDeliverTo)
	}
}
