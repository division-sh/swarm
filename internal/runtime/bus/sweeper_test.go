package bus_test

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
)

type sweeperTestStore struct {
	events      []events.Event
	deliveries  map[string][]string
	receipts    map[string]string
	receiptErrs map[string]string
}

func (s *sweeperTestStore) AppendEvent(context.Context, events.Event) error { return nil }
func (s *sweeperTestStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (s *sweeperTestStore) UpsertPipelineReceipt(_ context.Context, eventID, status, errText string) error {
	if s.receipts == nil {
		s.receipts = map[string]string{}
	}
	if s.receiptErrs == nil {
		s.receiptErrs = map[string]string{}
	}
	s.receipts[eventID] = status
	s.receiptErrs[eventID] = errText
	return nil
}
func (s *sweeperTestStore) ListEventsMissingPipelineReceipt(context.Context, time.Time, int) ([]events.Event, error) {
	return append([]events.Event(nil), s.events...), nil
}
func (s *sweeperTestStore) ListEventDeliveryRecipients(_ context.Context, eventID string) ([]string, error) {
	return append([]string(nil), s.deliveries[eventID]...), nil
}

func TestSweepUndispatchedUsesPersistedDeliveryRecipients(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.Event{
			(events.Event{
				ID:        "evt-1",
				Type:      events.EventType("custom.emitted"),
				Payload:   []byte(`{"entity_id":"ent-1"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-1"),
		},
		deliveries: map[string][]string{"evt-1": {"agent-a"}},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-a")

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-1"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	select {
	case evt := <-ch:
		if evt.ID != "evt-1" {
			t.Fatalf("delivered event id = %q, want evt-1", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected swept delivery")
	}
}

func TestSweepUndispatchedFallsBackToSubscribedRouting(t *testing.T) {
	store := &sweeperTestStore{
		events: []events.Event{
			(events.Event{
				ID:        "evt-2",
				Type:      events.EventType("custom.routed"),
				Payload:   []byte(`{"entity_id":"ent-2"}`),
				CreatedAt: time.Now().UTC(),
			}).WithEntityID("ent-2"),
		},
		deliveries: map[string][]string{},
	}
	eb, err := runtimebus.NewEventBus(store)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	ch := eb.Subscribe("agent-b", events.EventType("custom.routed"))

	count, err := eb.SweepUndispatched(context.Background(), time.Hour, 10)
	if err != nil {
		t.Fatalf("SweepUndispatched: %v", err)
	}
	if count != 1 {
		t.Fatalf("swept count = %d, want 1", count)
	}
	if got := store.receipts["evt-2"]; got != "processed" {
		t.Fatalf("receipt status = %q, want processed", got)
	}
	select {
	case evt := <-ch:
		if evt.ID != "evt-2" {
			t.Fatalf("delivered event id = %q, want evt-2", evt.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected subscribed swept delivery")
	}
}
