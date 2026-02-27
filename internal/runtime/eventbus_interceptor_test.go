package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

type interceptStoreStub struct {
	mu         sync.Mutex
	events     []events.Event
	deliveries map[string][]string
}

func (s *interceptStoreStub) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}

func (s *interceptStoreStub) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type interceptStub struct {
	passthrough bool
	deferred    []events.Event
}

func (s interceptStub) Intercept(_ context.Context, _ events.Event) (bool, []events.Event, error) {
	return s.passthrough, s.deferred, nil
}

func TestEventBus_Publish_InterceptorConsumeWithDeferred(t *testing.T) {
	store := &interceptStoreStub{}
	bus := NewEventBus(store)
	d := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("portfolio.digest_compiled"),
		SourceAgent: "pipeline-coordinator",
		Payload:     mustJSON(map[string]any{"ok": true}),
		CreatedAt:   time.Now(),
	}
	bus.SetInterceptors(interceptStub{
		passthrough: false,
		deferred:    []events.Event{d},
	})

	ch := bus.Subscribe("agent-a", events.EventType("portfolio.digest_compiled"))
	in := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  uuid.NewString(),
		Payload:     mustJSON(map[string]any{"vertical_id": uuid.NewString()}),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), in); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.Type != d.Type {
			t.Fatalf("expected deferred type %s, got %s", d.Type, got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("expected deferred delivery")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.events) != 2 {
		t.Fatalf("expected 2 persisted events (inbound + deferred), got %d", len(store.events))
	}
	if _, ok := store.deliveries[in.ID]; ok {
		t.Fatalf("expected consumed inbound event to have no delivery manifest")
	}
	if got := len(store.deliveries[d.ID]); got != 1 {
		t.Fatalf("expected deferred delivery manifest size=1, got %d", got)
	}
}
