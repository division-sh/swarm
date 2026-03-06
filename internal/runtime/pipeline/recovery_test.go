package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"empireai/internal/events"
)

type recoveryStoreStub struct {
	missing   []events.Event
	lastSince time.Time
	lastLimit int
}

func (s *recoveryStoreStub) AppendEvent(_ context.Context, _ events.Event) error { return nil }

func (s *recoveryStoreStub) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *recoveryStoreStub) ListEventsMissingPipelineReceipt(_ context.Context, since time.Time, limit int) ([]events.Event, error) {
	s.lastSince = since
	s.lastLimit = limit
	return append([]events.Event(nil), s.missing...), nil
}

type recoveryPublisherStub struct {
	published []events.Event
	failFor   map[string]error
}

func (p *recoveryPublisherStub) Publish(_ context.Context, evt events.Event) error {
	if err := p.failFor[evt.ID]; err != nil {
		return err
	}
	p.published = append(p.published, evt)
	return nil
}

func TestRecoveryManager_ReplaysMissingPipelineReceiptEvents(t *testing.T) {
	store := &recoveryStoreStub{
		missing: []events.Event{{
			ID:          "11111111-1111-1111-1111-111111111111",
			Type:        events.EventType("system.directive"),
			SourceAgent: "human",
			Payload:     []byte(`{"directive_text":"test"}`),
			CreatedAt:   time.Now().UTC(),
		}},
	}
	pub := &recoveryPublisherStub{}
	r := NewRecoveryManagerWith(store, pub)

	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].ID != store.missing[0].ID {
		t.Fatalf("expected replayed event to be published once, got %#v", pub.published)
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
				Type:        events.EventType("system.directive"),
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
	pub := &recoveryPublisherStub{failFor: map[string]error{
		"22222222-2222-2222-2222-222222222222": errors.New("boom"),
	}}
	r := NewRecoveryManagerWith(store, pub)

	if err := r.Recover(context.Background()); err == nil {
		t.Fatal("expected first replay error to be returned")
	}
	if len(pub.published) != 1 || pub.published[0].ID != "33333333-3333-3333-3333-333333333333" {
		t.Fatalf("expected second event to still be replayed, got %#v", pub.published)
	}
}

func TestRecoveryManager_DefaultWindowAndLimitApplied(t *testing.T) {
	store := &recoveryStoreStub{}
	pub := &recoveryPublisherStub{}
	r := NewRecoveryManagerWith(store, pub)
	r.window = 0
	r.limit = 0

	before := time.Now()
	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if store.lastLimit != 500 {
		t.Fatalf("expected fallback limit=500, got %d", store.lastLimit)
	}
	if store.lastSince.Before(before.Add(-16*time.Minute)) || store.lastSince.After(time.Now().Add(-14*time.Minute)) {
		t.Fatalf("expected fallback since around now-15m, got %s", store.lastSince.UTC().Format(time.RFC3339))
	}
}

func TestRecoveryManager_RespectsCanceledContextBeforeReplay(t *testing.T) {
	store := &recoveryStoreStub{
		missing: []events.Event{{
			ID:          "44444444-4444-4444-4444-444444444444",
			Type:        events.EventType("system.directive"),
			SourceAgent: "human",
			Payload:     []byte(`{"directive_text":"will_not_replay"}`),
			CreatedAt:   time.Now().UTC(),
		}},
	}
	pub := &recoveryPublisherStub{}
	r := NewRecoveryManagerWith(store, pub)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Recover(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
	if len(pub.published) != 0 {
		t.Fatalf("expected no publishes when context is canceled, got %#v", pub.published)
	}
}
