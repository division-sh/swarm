package pipeline

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestDiscoveryAggregator_HandleSynthesisResolved(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	n := &DiscoveryAggregator{coordinator: pc}

	if handled := n.Handle(context.Background(), events.Event{
		ID:      uuid.NewString(),
		Type:    events.EventType("synthesis.resolved"),
		Payload: mustJSON(map[string]any{"resolved_assessment": "keep opportunity a"}),
	}); !handled {
		t.Fatal("expected synthesis.resolved to be handled")
	}
}

func TestDiscoveryAggregator_HandleSynthesisResolved_EmitsVerticalDiscovered(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &DiscoveryAggregator{coordinator: pc}
	ch := bus.Subscribe("discovery-aggregator-test", events.EventType("vertical.discovered"))

	if handled := n.Handle(context.Background(), events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("synthesis.resolved"),
		SourceAgent: "discovery-coordinator",
		Payload: mustJSON(map[string]any{
			"resolution":        "conflict_resolved",
			"opportunity_name":  "AI Intake Automation",
			"geography":         "US",
			"mode":              "saas_gap",
			"scan_id":           "scan-123",
			"campaign_id":       "campaign-123",
			"signal_strength":   0.82,
			"resolved_assessment": "merged conflicting reports",
		}),
	}); !handled {
		t.Fatal("expected synthesis.resolved to be handled")
	}

	select {
	case evt := <-ch:
		if evt.Type != events.EventType("vertical.discovered") {
			t.Fatalf("expected vertical.discovered, got %s", evt.Type)
		}
		payload := parsePayloadMap(evt.Payload)
		if got := asString(payload["name"]); got != "AI Intake Automation" {
			t.Fatalf("expected discovered name to be preserved, got %q payload=%v", got, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected vertical.discovered after synthesis.resolved")
	}
}

func TestDiscoveryAggregator_HandleSynthesisResolved_IrreconcilableDoesNotEmit(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	n := &DiscoveryAggregator{coordinator: pc}
	ch := bus.Subscribe("discovery-aggregator-test-noemit", events.EventType("vertical.discovered"))

	if handled := n.Handle(context.Background(), events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("synthesis.resolved"),
		Payload: mustJSON(map[string]any{
			"resolution": "irreconcilable",
		}),
	}); !handled {
		t.Fatal("expected synthesis.resolved to be handled")
	}

	select {
	case evt := <-ch:
		t.Fatalf("expected no vertical.discovered, got %s", evt.Type)
	case <-time.After(200 * time.Millisecond):
	}
}
