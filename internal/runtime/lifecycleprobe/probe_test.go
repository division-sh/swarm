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
