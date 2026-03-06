package runtime

import (
	"context"
	"empireai/internal/events"
	"github.com/google/uuid"
	"testing"
	"time"
)

type recoveryStoreStub struct {
	missing    []events.Event
	deliveries map[string][]string
	receipts   map[string]string
	lastSince  time.Time
	lastLimit  int
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

func (s *recoveryStoreStub) ListEventsMissingPipelineReceipt(_ context.Context, since time.Time, limit int) ([]events.Event, error) {
	s.lastSince = since
	s.lastLimit = limit
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
	evt := requireBufferedEvent(t, ch, "replayed event")
	if evt.ID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("unexpected delivered event id: %s", evt.ID)
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
	_ = requireBufferedEvent(t, ch, "second replayed event")
}

func TestRecoveryManager_DefaultWindowAndLimitApplied(t *testing.T) {
	store := &recoveryStoreStub{}
	bus := NewEventBus(store)
	r := NewRecoveryManagerWith(store, bus)
	r.window = 0
	r.limit = 0

	before := time.Now()
	if err := r.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if store.lastLimit != 500 {
		t.Fatalf("expected fallback limit=500, got %d", store.lastLimit)
	}
	// Fallback window is 15 minutes; allow a small execution margin.
	if store.lastSince.Before(before.Add(-16*time.Minute)) || store.lastSince.After(time.Now().Add(-14*time.Minute)) {
		t.Fatalf("expected fallback since around now-15m, got %s", store.lastSince.UTC().Format(time.RFC3339))
	}
}

func TestRecoveryManager_RespectsCanceledContextBeforeReplay(t *testing.T) {
	store := &recoveryStoreStub{
		missing: []events.Event{
			{
				ID:          "44444444-4444-4444-4444-444444444444",
				Type:        events.EventType("system.directive"),
				SourceAgent: "human",
				Payload:     []byte(`{"directive_text":"will_not_replay"}`),
				CreatedAt:   time.Now().UTC(),
			},
		},
	}
	bus := NewEventBus(store)
	r := NewRecoveryManagerWith(store, bus)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Recover(ctx); err == nil {
		t.Fatal("expected canceled context error")
	}
	if _, ok := store.deliveries["44444444-4444-4444-4444-444444444444"]; ok {
		t.Fatal("expected no deliveries when context is canceled before replay loop")
	}
}

func requireBufferedEvent(t *testing.T, ch <-chan events.Event, label string) events.Event {
	t.Helper()
	select {
	case evt := <-ch:
		return evt
	default:
		t.Fatalf("expected %s to already be buffered", label)
		return events.Event{}
	}
}
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
