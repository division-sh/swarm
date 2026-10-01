package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJournaledActivityResultRejectsMissingCompletionTimestamp(t *testing.T) {
	zero := time.Time{}
	for _, tc := range []struct {
		name string
		at   *time.Time
	}{{"missing", nil}, {"zero", &zero}} {
		for _, target := range []string{"publication", "emission_plan"} {
			t.Run(tc.name+"/"+target, func(t *testing.T) {
				bus := &recordingPipelineBus{}
				dispatcher := pipelineActivityDispatcher{coordinator: &PipelineCoordinator{bus: bus}}
				if target == "emission_plan" {
					dispatcher.emissions = &pipelineEmissionPlan{}
				}
				intent := NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString())
				receipt := activityAttemptStartRecord(intent, activityInputHash(intent.Input)).withTerminal(
					ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent,
					activitySuccessPayload(intent, map[string]any{"ok": true}), nil,
				)
				receipt.CompletedAt = tc.at
				err := dispatcher.publishJournaledActivityResult(context.Background(), intent, receipt)
				if err == nil || !strings.Contains(err.Error(), "completion timestamp") || len(bus.publishes) != 0 || (dispatcher.emissions != nil && len(dispatcher.emissions.events) != 0) {
					t.Fatalf("invalid terminal time escaped: err=%v publications=%d", err, len(bus.publishes))
				}
			})
		}
	}
}

func TestJournaledActivityEmissionPlanPreservesCompletionTimestampPrecision(t *testing.T) {
	intent := NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString())
	completed := time.Date(2026, time.September, 30, 12, 0, 0, 123456789, time.FixedZone("fixture", 3600))
	receipt := activityAttemptStartRecord(intent, activityInputHash(intent.Input)).withTerminal(
		ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent,
		activitySuccessPayload(intent, map[string]any{"ok": true}), nil,
	)
	receipt.CompletedAt = &completed
	dispatcher := pipelineActivityDispatcher{emissions: &pipelineEmissionPlan{}}
	for range 2 {
		if err := dispatcher.publishJournaledActivityResult(context.Background(), intent, receipt); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range dispatcher.emissions.events {
		if !event.CreatedAt().Equal(completed.UTC().Truncate(time.Microsecond)) || event.ID() != receipt.ResultEventID {
			t.Fatalf("emission result changed durable facts: time=%v id=%s", event.CreatedAt(), event.ID())
		}
	}
	if !receipt.CompletedAt.Equal(completed) {
		t.Fatal("publication changed the receipt's timestamp precision")
	}
}
