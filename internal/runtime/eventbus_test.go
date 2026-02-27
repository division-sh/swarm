package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
)

type captureStore struct {
	events     []events.Event
	deliveries map[string][]string
}

func (s *captureStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func (s *captureStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type failingDeliveryStore struct{}

func (failingDeliveryStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (failingDeliveryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return errors.New("insert failed")
}

type selectiveFailStore struct {
	active []string
}

func (s selectiveFailStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (s selectiveFailStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return errors.New("insert failed")
}
func (s selectiveFailStore) ListActiveAgentIDs(_ context.Context) ([]string, error) {
	return append([]string(nil), s.active...), nil
}

func TestEventBusOpCoRoutingPersistsDeliveries(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)

	_ = bus.Subscribe("support-a", events.EventType("bug_reported"))
	_ = bus.Subscribe("cto-a", events.EventType("bug_reported"))

	if err := bus.SetRoutingTable("vertical-a", &RoutingTable{
		VerticalID: "vertical-a",
		Routes: []Route{
			{EventPattern: "bug_reported", SubscriberID: "support-a", Status: "active"},
			{EventPattern: "bug_reported", SubscriberID: "cto-a", Status: "active"},
		},
	}); err != nil {
		t.Fatalf("set routing table: %v", err)
	}

	payload, _ := json.Marshal(map[string]string{"bug": "x"})
	evt := events.Event{
		ID:          "11111111-1111-1111-1111-111111111111",
		Type:        events.EventType("bug_reported"),
		SourceAgent: "support-a",
		VerticalID:  "vertical-a",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d, ok := store.deliveries[evt.ID]
	if !ok {
		t.Fatalf("expected delivery manifest for event")
	}
	if len(d) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(d))
	}
}

func TestEventBusFactoryEventWithVerticalIDUsesSubscriptions(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	ch := bus.Subscribe("validation-coordinator", events.EventType("validation.started"))

	evt := events.Event{
		ID:          "22222222-2222-2222-2222-222222222222",
		Type:        events.EventType("validation.started"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  "vertical-b",
		Payload:     []byte(`{"vertical_id":"vertical-b"}`),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-ch:
		if got.ID != evt.ID {
			t.Fatalf("expected fallback event %s, got %s", evt.ID, got.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected delivery to static subscriber")
	}
	if len(store.events) != 1 {
		t.Fatalf("expected only published factory event (no contradiction), got %d", len(store.events))
	}
}

func TestEventBusPublish_ReturnsErrorWhenDeliveryPersistenceFails(t *testing.T) {
	bus := NewEventBus(failingDeliveryStore{})
	_ = bus.Subscribe("empire-coordinator", events.EventType("system.directive"))
	err := bus.Publish(context.Background(), events.Event{
		ID:          "33333333-3333-3333-3333-333333333333",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     []byte(`{"directive_text":"x"}`),
		CreatedAt:   time.Now(),
	})
	if err == nil {
		t.Fatal("expected delivery persistence error")
	}
}

func TestEventBusPublish_AllowsNonAgentEphemeralSubscribersWithoutDeliveryRows(t *testing.T) {
	bus := NewEventBus(selectiveFailStore{active: []string{"empire-coordinator"}})
	ch := bus.Subscribe("telegram-delivery-loop", events.EventType("human_task.approved"))
	err := bus.Publish(context.Background(), events.Event{
		ID:          "44444444-4444-4444-4444-444444444444",
		Type:        events.EventType("human_task.approved"),
		SourceAgent: "empire-coordinator",
		Payload:     []byte(`{"task_id":"t1"}`),
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("expected ephemeral delivery without persistence failure, got: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("expected event delivered to ephemeral subscriber")
	}
}
