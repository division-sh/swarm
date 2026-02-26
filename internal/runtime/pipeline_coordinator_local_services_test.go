package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_LocalServicesDispatchesAllScannerAssignments(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	watch := bus.Subscribe(
		"watch-local-services",
		events.EventType("scanner.google_maps.scan_assigned"),
		events.EventType("scanner.instagram.scan_assigned"),
		events.EventType("scanner.reviews.scan_assigned"),
		events.EventType("scanner.directories.scan_assigned"),
		events.EventType("scanner.job_boards.scan_assigned"),
	)

	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "empire-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     uuid.NewString(),
			"mode":        "local_services",
			"geography":   "Argentina",
			"priority":    "high",
			"campaign_id": uuid.NewString(),
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish scan.requested: %v", err)
	}

	seen := map[string]struct{}{}
	deadline := time.After(800 * time.Millisecond)
	for len(seen) < localServicesScannerExpected {
		select {
		case evt := <-watch:
			seen[string(evt.Type)] = struct{}{}
		case <-deadline:
			t.Fatalf("expected %d local_services scanner assignments, got %d (%v)", localServicesScannerExpected, len(seen), seen)
		}
	}
}

func TestFactoryPipelineCoordinator_LocalServicesCompletionRequiresAllScannerTypes(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	bus.SetInterceptors(pc)

	scanID := uuid.NewString()
	if err := bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "empire-coordinator",
		Payload: mustJSON(map[string]any{
			"scan_id":     scanID,
			"mode":        "local_services",
			"geography":   "Argentina",
			"campaign_id": uuid.NewString(),
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish scan.requested: %v", err)
	}

	doneCh := bus.Subscribe("watch-scan-complete", events.EventType("scan.completed"))
	scannerCompletions := []events.EventType{
		events.EventType("scanner.google_maps.scan_complete"),
		events.EventType("scanner.instagram.scan_complete"),
		events.EventType("scanner.reviews.scan_complete"),
		events.EventType("scanner.directories.scan_complete"),
		events.EventType("scanner.job_boards.scan_complete"),
	}

	for i, evtType := range scannerCompletions {
		if err := bus.Publish(context.Background(), events.Event{
			ID:          uuid.NewString(),
			Type:        evtType,
			SourceAgent: "scanner-agent",
			Payload: mustJSON(map[string]any{
				"scan_id": scanID,
			}),
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("publish %s: %v", evtType, err)
		}

		if i < len(scannerCompletions)-1 {
			select {
			case evt := <-doneCh:
				t.Fatalf("scan.completed emitted too early after %d completions: %s", i+1, evt.Type)
			case <-time.After(80 * time.Millisecond):
			}
		}
	}

	select {
	case evt := <-doneCh:
		payload := parsePayloadMap(evt.Payload)
		if got := asInt(payload["agents_expected"]); got != localServicesScannerExpected {
			t.Fatalf("expected agents_expected=%d, got %d payload=%v", localServicesScannerExpected, got, payload)
		}
		if got := asInt(payload["agents_complete"]); got != localServicesScannerExpected {
			t.Fatalf("expected agents_complete=%d, got %d payload=%v", localServicesScannerExpected, got, payload)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("expected scan.completed after all local_services scanner completions")
	}
}
