package pipeline

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	runtimetestkit "empireai/internal/runtime/testkit"
	"github.com/google/uuid"
)

func TestScanOrchestrator_HandleScanTimeoutTimerForceCompletesExpiredScan(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &ScanOrchestrator{coordinator: pc.scanCoordinator}
	ctx := context.Background()

	scanID := uuid.NewString()
	pc.scanCoordinator.mu.Lock()
	pc.scanCoordinator.scans[scanID] = &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  uuid.NewString(),
		Mode:        "saas_gap",
		Geography:   "us",
		Expected:    1,
		CompletedBy: map[string]struct{}{},
		CreatedAt:   time.Now().UTC().Add(-scanTimeout - time.Minute),
	}
	pc.scanCoordinator.mu.Unlock()

	ch := bus.Subscribe("watch-scan-timeout", events.EventType("scan.completed"))
	if handled := n.Handle(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("timer.scan_timeout")}); !handled {
		t.Fatal("expected timer.scan_timeout to be handled")
	}

	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"scan.completed"}, 0)["scan.completed"]
	var payload map[string]any
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("decode scan.completed payload: %v", err)
	}
	if scan := asString(payload["scan_id"]); scan != scanID {
		t.Fatalf("expected scan_id %q, got %q", scanID, scan)
	}
	if !boolFromAny(payload["timed_out"]) {
		t.Fatalf("expected timed_out=true, got %#v", payload["timed_out"])
	}
}

func TestScanOrchestrator_HandleCampaignDeadlineTimerIsRouted(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &ScanOrchestrator{coordinator: pc.scanCoordinator}
	ctx := context.Background()

	if handled := n.Handle(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("timer.campaign_deadline")}); !handled {
		t.Fatal("expected timer.campaign_deadline to be handled")
	}
}
