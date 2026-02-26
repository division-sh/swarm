package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
)

type recoveryStoreStub struct {
	missing    []events.Event
	deliveries map[string][]string
	receipts   map[string]string
}

func (s *recoveryStoreStub) AppendEvent(_ context.Context, _ events.Event) error { return nil }

func (s *recoveryStoreStub) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	cp := append([]string(nil), agentIDs...)
	s.deliveries[eventID] = cp
	return nil
}

func (s *recoveryStoreStub) ListEventsMissingPipelineReceipt(_ context.Context, _ time.Time, _ int) ([]events.Event, error) {
	return append([]events.Event(nil), s.missing...), nil
}

func (s *recoveryStoreStub) UpsertPipelineReceipt(_ context.Context, eventID, status, _ string) error {
	if s.receipts == nil {
		s.receipts = make(map[string]string)
	}
	s.receipts[eventID] = status
	return nil
}

func TestRecoveryManager_ReplaysMissingPipelineReceiptEvents(t *testing.T) {
	store := &recoveryStoreStub{
		missing: []events.Event{
			{
				ID:          "11111111-1111-1111-1111-111111111111",
				Type:        events.EventType("system.directive"),
				SourceAgent: "human",
				Payload:     []byte(`{"directive_text":"test"}`),
				CreatedAt:   time.Now().UTC(),
			},
		},
	}
	bus := NewEventBus(store)
	ch := bus.Subscribe("empire-coordinator", events.EventType("system.directive"))
	r := NewRecoveryManagerWith(store, bus)

	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got := len(store.deliveries["11111111-1111-1111-1111-111111111111"]); got != 1 {
		t.Fatalf("expected 1 delivery for replayed event, got %d", got)
	}
	if status := store.receipts["11111111-1111-1111-1111-111111111111"]; status != "processed" {
		t.Fatalf("expected processed receipt, got %q", status)
	}
	select {
	case evt := <-ch:
		if evt.ID != "11111111-1111-1111-1111-111111111111" {
			t.Fatalf("unexpected delivered event id: %s", evt.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected replayed event delivered to subscriber")
	}
}

func TestRecoveryManager_NoOp(t *testing.T) {
	r := NewRecoveryManager()
	if r == nil {
		t.Fatal("expected recovery manager")
	}
	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRecoveryManager_ReplayContinuesAfterPublishError(t *testing.T) {
	store := &recoveryStoreStub{
		missing: []events.Event{
			{
				ID:          "22222222-2222-2222-2222-222222222222",
				Type:        events.EventType("INVALID TYPE"),
				SourceAgent: "runtime",
				Payload:     []byte(`{}`),
				CreatedAt:   time.Now().UTC(),
			},
			{
				ID:          "33333333-3333-3333-3333-333333333333",
				Type:        events.EventType("system.directive"),
				SourceAgent: "human",
				Payload:     []byte(`{"directive_text":"ok"}`),
				CreatedAt:   time.Now().UTC(),
			},
		},
	}
	bus := NewEventBus(store)
	ch := bus.Subscribe("empire-coordinator", events.EventType("system.directive"))
	r := NewRecoveryManagerWith(store, bus)

	if err := r.Recover(context.Background()); err == nil {
		t.Fatal("expected first replay error to be returned")
	}
	if got := len(store.deliveries["33333333-3333-3333-3333-333333333333"]); got != 1 {
		t.Fatalf("expected second event to still be replayed, got deliveries=%d", got)
	}
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("expected second replayed event delivered")
	}
}
