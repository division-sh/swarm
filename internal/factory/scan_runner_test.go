package factory

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestScanners_SynthesizeSignals_NoAPIKeys(t *testing.T) {
	t.Setenv("GOOGLE_MAPS_API_KEY", "")
	t.Setenv("YELP_API_KEY", "")

	ctx := context.Background()
	if out, err := (GoogleMapsScanner{}).Scan(ctx, "New York", "full"); err != nil || len(out) == 0 {
		t.Fatalf("GoogleMapsScanner synth err=%v len=%d", err, len(out))
	}
	if out, err := (InstagramScanner{}).Scan(ctx, "New York", "discovery"); err != nil || len(out) == 0 {
		t.Fatalf("InstagramScanner synth err=%v len=%d", err, len(out))
	}
	if out, err := (ReviewScanner{}).Scan(ctx, "New York", "full"); err != nil || len(out) == 0 {
		t.Fatalf("ReviewScanner synth err=%v len=%d", err, len(out))
	}
	_ = synthesizeSignals("x", "y", "full", []string{"a"})
	_ = minInt(1, 2)
}

func TestScanRequestedRunner_Run_EndToEnd(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := runtime.InMemoryEventStore{}
	bus := runtime.NewEventBus(store)

	r := NewScanRequestedRunner(db, store, nil, bus)
	done := bus.Subscribe("t", events.EventType("scan.completed"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	time.Sleep(50 * time.Millisecond) // allow subscription to register

	payload := []byte(`{"geography":"us","depth":"discovery","count":2,"mode":"seed","campaign_id":"c1"}`)
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "tester",
		Payload:     payload,
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish scan.requested: %v", err)
	}

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("expected scan.completed")
	}
}
