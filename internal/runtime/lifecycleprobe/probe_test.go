package lifecycleprobe

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProbeWaitReturnsHistoricalSignal(t *testing.T) {
	probe := New()
	probe.NotifyLifecycle(context.Background(), Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        "event-1",
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Status:         "delivered",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := probe.WaitForDeliveryStatus(ctx, "event-1", "node", "node-a", "delivered")
	if err != nil {
		t.Fatalf("WaitForDeliveryStatus: %v", err)
	}
	if got.Kind != DeliveryStatusChanged || got.EventID != "event-1" || got.Status != "delivered" {
		t.Fatalf("signal = %#v, want delivered event-1", got)
	}
}

func TestProbeWaitMatchesFutureSignal(t *testing.T) {
	probe := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan Signal, 1)
	go func() {
		got, err := probe.WaitForPostCommitDispatchStarted(ctx, "event-2")
		if err != nil {
			t.Errorf("WaitForPostCommitDispatchStarted: %v", err)
			return
		}
		done <- got
	}()

	probe.NotifyLifecycle(context.Background(), Signal{
		Kind:    PostCommitDispatchStarted,
		EventID: "event-2",
	})

	select {
	case got := <-done:
		if got.Kind != PostCommitDispatchStarted || got.EventID != "event-2" {
			t.Fatalf("signal = %#v, want post-commit start event-2", got)
		}
	case <-ctx.Done():
		t.Fatalf("waiter did not receive signal: %v", ctx.Err())
	}
}

func TestProbeWaitReturnsContextError(t *testing.T) {
	probe := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := probe.WaitForDeliveryStatus(ctx, "event-3", "node", "node-a", "delivered")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForDeliveryStatus error = %v, want context canceled", err)
	}
}

func TestProbeWaitAfterConsumesDistinctHistoricalSignals(t *testing.T) {
	probe := New()
	times := []time.Time{
		time.Unix(1, 0).UTC(),
		time.Unix(2, 0).UTC(),
		time.Unix(3, 0).UTC(),
	}
	for i, status := range []string{"in_progress", "failed", "in_progress"} {
		probe.NotifyLifecycle(context.Background(), Signal{
			Kind:           DeliveryStatusChanged,
			EventID:        "event-4",
			SubscriberType: "node",
			SubscriberID:   "node-a",
			Status:         status,
			At:             times[i],
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var cursor Cursor
	first, next, err := probe.WaitAfter(ctx, cursor, Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        "event-4",
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Status:         "in_progress",
	})
	if err != nil {
		t.Fatalf("WaitAfter first in_progress: %v", err)
	}
	failed, next, err := probe.WaitAfter(ctx, next, Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        "event-4",
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Status:         "failed",
	})
	if err != nil {
		t.Fatalf("WaitAfter failed: %v", err)
	}
	second, _, err := probe.WaitAfter(ctx, next, Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        "event-4",
		SubscriberType: "node",
		SubscriberID:   "node-a",
		Status:         "in_progress",
	})
	if err != nil {
		t.Fatalf("WaitAfter second in_progress: %v", err)
	}
	if first.At != times[0] || failed.At != times[1] || second.At != times[2] {
		t.Fatalf("WaitAfter sequence times = %s, %s, %s; want %s, %s, %s",
			first.At, failed.At, second.At, times[0], times[1], times[2])
	}
}

func TestProbeWaitForDeliveryStatusAllowsSubscriberIDWildcard(t *testing.T) {
	probe := New()
	probe.NotifyLifecycle(context.Background(), Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        "event-5",
		SubscriberType: "node",
		SubscriberID:   "reviewer",
		Status:         "delivered",
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := probe.WaitForDeliveryStatus(ctx, "event-5", "node", "", "delivered")
	if err != nil {
		t.Fatalf("WaitForDeliveryStatus wildcard subscriber id: %v", err)
	}
	if got.SubscriberID != "reviewer" {
		t.Fatalf("subscriber id = %q, want reviewer", got.SubscriberID)
	}
}
