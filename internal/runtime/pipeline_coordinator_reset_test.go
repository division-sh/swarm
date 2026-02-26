package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_ReSubscribesAfterBusReset(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pc.Run(ctx)
	time.Sleep(50 * time.Millisecond)

	watch := bus.Subscribe("watch-initial", events.EventType("market_research.scan_assigned"))
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "test",
		Payload:     mustJSON(map[string]any{"mode": "saas_gap", "scan_id": "scan-1"}),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish pre-reset: %v", err)
	}
	select {
	case <-watch:
	case <-time.After(400 * time.Millisecond):
		t.Fatal("expected market_research.scan_assigned before reset")
	}

	bus.ResetInMemoryState()
	time.Sleep(40 * time.Millisecond)
	if got := len(pc.SnapshotScans()); got != 0 {
		t.Fatalf("expected coordinator in-memory scan state reset, got %d", got)
	}

	watchAfter := bus.Subscribe("watch-after", events.EventType("market_research.scan_assigned"))
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if err := bus.Publish(context.Background(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("scan.requested"),
			SourceAgent: "test",
			Payload:     mustJSON(map[string]any{"mode": "saas_gap", "scan_id": uuid.NewString()}),
			CreatedAt:   time.Now(),
		}); err != nil {
			t.Fatalf("publish after reset: %v", err)
		}
		select {
		case <-watchAfter:
			return
		case <-time.After(75 * time.Millisecond):
		}
	}
	t.Fatal("expected market_research.scan_assigned after reset and coordinator re-subscribe")
}
