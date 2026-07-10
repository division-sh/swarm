package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
)

func TestEventPublishDeliveriesExposeFailureEvidence(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	deliveries := eventPublishDeliveries([]store.OperatorEventDelivery{{
		DeliveryID:     "delivery-1",
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Status:         "dead_letter",
		ReasonCode:     "retry_exhausted",
		Failure:        testFailure("retry_exhausted"),
		RetryCount:     2,
		CreatedAt:      &at,
		FinishedAt:     &at,
		DeadLetters: []store.OperatorDeadLetterRecord{{
			DeadLetterID: "dead-1",
			Failure:      *testFailure("retry_exhausted"),
			RetryCount:   2,
			CreatedAt:    at,
		}},
	}})
	if len(deliveries) != 1 {
		t.Fatalf("deliveries len = %d, want 1", len(deliveries))
	}
	got := deliveries[0]
	if got.RetryCount != 2 || got.Attempt != 3 || got.RetryEligible || !got.Terminal || got.ReasonCode != "retry_exhausted" || got.Failure == nil || got.Failure.Detail.Code != "retry_exhausted" || len(got.DeadLetters) != 1 {
		t.Fatalf("delivery failure evidence = %#v", got)
	}
}

func TestEventReplayTargetsExposeOriginalFailureEvidence(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	original := store.OperatorEventFull{
		EventID:   "event-1",
		EventName: "task.failed",
		Deliveries: []store.OperatorEventDelivery{{
			DeliveryID:     "delivery-1",
			SubscriberType: "agent",
			SubscriberID:   "agent-a",
			Status:         "failed",
			ReasonCode:     "handler_error",
			Failure:        testFailure("handler_failed"),
			RetryCount:     1,
			CreatedAt:      &at,
		}},
	}
	deliveries, subscribers, err := eventReplayTargets(original, nil)
	if err != nil {
		t.Fatalf("eventReplayTargets: %v", err)
	}
	if len(subscribers) != 1 || subscribers[0] != "agent-a" || len(deliveries) != 1 {
		t.Fatalf("targets subscribers=%#v deliveries=%#v", subscribers, deliveries)
	}
	got := deliveries[0]
	if got.RetryCount != 1 || got.Attempt != 2 || !got.RetryEligible || got.Terminal || got.ReasonCode != "handler_error" || got.Failure == nil || got.Failure.Detail.Code != "handler_failed" {
		t.Fatalf("replay delivery evidence = %#v", got)
	}
}
