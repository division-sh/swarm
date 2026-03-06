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
	subscribeSignals := make(chan struct{}, 4)
	pc.testSubscribeHook = func() {
		select {
		case subscribeSignals <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pc.Run(ctx)

	waitForSignal(t, subscribeSignals, "initial pipeline-coordinator subscription")

	watch := bus.Subscribe("watch-initial", events.EventType("market_research.scan_assigned"))
	publishScanRequested(t, bus, "scan-1")
	evt := waitForEventType(t, watch, "market_research.scan_assigned")
	payload := parsePayloadMap(evt.Payload)
	if got := asString(payload["scan_id"]); got != "scan-1" {
		t.Fatalf("expected initial assigned scan_id=scan-1, got %q", got)
	}
	assertScanAccumulatorIDs(t, pc, []string{"scan-1"})

	bus.ResetInMemoryState()
	waitForChannelClosed(t, watch, "initial watcher reset")
	waitForSignal(t, subscribeSignals, "pipeline-coordinator resubscription after reset")

	watchAfter := bus.Subscribe("watch-after", events.EventType("market_research.scan_assigned"))
	publishScanRequested(t, bus, "scan-after-reset")
	evt = waitForEventType(t, watchAfter, "market_research.scan_assigned")
	payload = parsePayloadMap(evt.Payload)
	if got := asString(payload["scan_id"]); got != "scan-after-reset" {
		t.Fatalf("expected post-reset assigned scan_id=scan-after-reset, got %q", got)
	}
	assertScanAccumulatorIDs(t, pc, []string{"scan-after-reset"})
}

func publishScanRequested(t *testing.T, bus *EventBus, scanID string) {
	t.Helper()
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "test",
		Payload: mustJSON(map[string]any{
			"mode":    "saas_gap",
			"scan_id": scanID,
		}),
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("publish scan.requested failed: %v", err)
	}
}

func waitForSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitForChannelClosed(t *testing.T, ch <-chan events.Event, label string) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel close for %s", label)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("timed out waiting for channel close: %s", label)
	}
}

func assertScanAccumulatorIDs(t *testing.T, pc *FactoryPipelineCoordinator, want []string) {
	t.Helper()
	got := pc.SnapshotScans()
	if len(got) != len(want) {
		t.Fatalf("expected %d scan accumulators, got %+v", len(want), got)
	}
	for i, scanID := range want {
		if gotID := asString(got[i]["scan_id"]); gotID != scanID {
			t.Fatalf("scan accumulator %d: got %q want %q (snapshot=%+v)", i, gotID, scanID, got)
		}
	}
}
