package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"empireai/internal/events"
)

func TestEventBusCycleTracker_BlocksLoopAndEmitsEscalation(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	bus.SetCycleTracker(NewOpCoCycleTracker(nil))

	verticalID := "11111111-1111-1111-1111-111111111111"
	qaID := "qa-agent-" + verticalID
	ctoID := "cto-agent-" + verticalID
	_ = bus.Subscribe(ctoID, events.EventType("qa.validation_failed"), events.EventType("cycle_limit_reached"))
	if err := bus.SetRoutingTable(verticalID, &RoutingTable{
		VerticalID: verticalID,
		Routes: []Route{
			{EventPattern: "cycle_limit_reached", SubscriberID: ctoID, Status: "active"},
		},
	}); err != nil {
		t.Fatalf("set routing table: %v", err)
	}

	var fifthEventID string
	for i := 1; i <= 5; i++ {
		eventID := fmt.Sprintf("qa-loop-%d", i)
		if i == 5 {
			fifthEventID = eventID
		}
		if err := bus.Publish(context.Background(), events.Event{
			ID:          eventID,
			Type:        events.EventType("qa.validation_failed"),
			SourceAgent: qaID,
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"attempt": i}),
			CreatedAt:   time.Now(),
		}); err != nil {
			t.Fatalf("publish loop event %d: %v", i, err)
		}
	}

	if _, ok := store.deliveries[fifthEventID]; ok {
		t.Fatalf("expected 5th loop event to be blocked, got deliveries=%v", store.deliveries[fifthEventID])
	}

	cycleEvents := 0
	for _, evt := range store.events {
		if string(evt.Type) != "cycle_limit_reached" {
			continue
		}
		cycleEvents++
		var payload map[string]any
		_ = json.Unmarshal(evt.Payload, &payload)
		if asInt(payload["count"]) < 5 {
			t.Fatalf("expected cycle escalation count >= 5, got payload=%v", payload)
		}
	}
	if cycleEvents == 0 {
		t.Fatal("expected cycle_limit_reached escalation event")
	}
}

func TestEventBusCycleTracker_CycleResetClearsCounter(t *testing.T) {
	store := &captureStore{}
	bus := NewEventBus(store)
	bus.SetCycleTracker(NewOpCoCycleTracker(nil))

	verticalID := "22222222-2222-2222-2222-222222222222"
	qaID := "qa-agent-" + verticalID
	ctoID := "cto-agent-" + verticalID
	_ = bus.Subscribe(ctoID, events.EventType("cycle_limit_reached"))
	if err := bus.SetRoutingTable(verticalID, &RoutingTable{
		VerticalID: verticalID,
		Routes: []Route{
			{EventPattern: "cycle_limit_reached", SubscriberID: ctoID, Status: "active"},
		},
	}); err != nil {
		t.Fatalf("set routing table: %v", err)
	}

	publishQA := func(id string) {
		t.Helper()
		if err := bus.Publish(context.Background(), events.Event{
			ID:          id,
			Type:        events.EventType("qa.validation_failed"),
			SourceAgent: qaID,
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"id": id}),
			CreatedAt:   time.Now(),
		}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}
	for i := 0; i < 4; i++ {
		publishQA(fmt.Sprintf("pre-reset-%d", i))
	}
	if err := bus.Publish(context.Background(), events.Event{
		ID:          "cycle-reset-1",
		Type:        events.EventType("cycle_reset"),
		SourceAgent: ctoID,
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"event_pattern": "qa.validation_failed",
			"reason":        "loop fixed",
		}),
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("publish cycle_reset: %v", err)
	}
	for i := 0; i < 4; i++ {
		publishQA(fmt.Sprintf("post-reset-%d", i))
	}
	for _, evt := range store.events {
		if string(evt.Type) == "cycle_limit_reached" {
			t.Fatalf("did not expect escalation before limit after reset, got event=%s", evt.ID)
		}
	}
	publishQA("post-reset-5")
	found := false
	for _, evt := range store.events {
		if string(evt.Type) == "cycle_limit_reached" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected escalation after 5 events following cycle reset")
	}
}
