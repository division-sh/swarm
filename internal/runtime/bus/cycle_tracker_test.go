package bus

import (
	"context"
	"strings"
	"testing"

	"empireai/internal/events"
	"github.com/google/uuid"
)

func TestCycleTrackerCircuitBreakerEscalates(t *testing.T) {
	ctx := context.Background()
	tracker := NewOpCoCycleTracker(nil)
	verticalID := uuid.NewString()

	var escalated bool
	var escalation *events.Event
	for i := 0; i < defaultOpCoCycleLimit; i++ {
		escalated, escalation = tracker.Check(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("qa.validation_failed"),
			VerticalID:  verticalID,
			SourceAgent: "opco-qa-" + verticalID,
			Payload:     mustJSON(map[string]any{"cycle": i + 1}),
		})
	}
	if !escalated || escalation == nil || strings.TrimSpace(string(escalation.Type)) != "cycle_limit_reached" {
		t.Fatalf("expected cycle_limit_reached escalation, got escalated=%v event=%+v", escalated, escalation)
	}
}
