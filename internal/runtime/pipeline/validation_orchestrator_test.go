package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestValidationOrchestrator_HandlesRevisionEvents(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	n := &ValidationOrchestrator{coordinator: pc}
	ctx := context.Background()
	verticalID := uuid.NewString()

	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID: verticalID,
		Status:     "active",
	}

	if handled := n.Handle(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_requested"),
		VerticalID: verticalID,
	}); !handled {
		t.Fatal("expected spec.revision_requested to be handled")
	}

	if handled := n.Handle(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "tighten scope"}),
	}); !handled {
		t.Fatal("expected spec.revision_needed to be handled")
	}
}
