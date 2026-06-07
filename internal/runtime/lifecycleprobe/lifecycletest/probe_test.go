package lifecycletest

import (
	"context"
	"testing"
	"time"

	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

func TestRequireDeliveryStatusReturnsHistoricalSignal(t *testing.T) {
	probe := New(t)
	probe.NotifyLifecycle(context.Background(), runtimelifecycleprobe.Signal{
		Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
		EventID:        "event-1",
		SubscriberType: SubscriberNode,
		SubscriberID:   "node-a",
		Status:         StatusDelivered,
	})

	got := probe.RequireNodeDelivered("event-1", "node-a")
	if got.EventID != "event-1" || got.SubscriberID != "node-a" || got.Status != StatusDelivered {
		t.Fatalf("signal = %#v, want delivered node-a event-1", got)
	}
}

func TestExpectationWaitsForFutureSignalsInOrder(t *testing.T) {
	probe := New(t, WithTimeout(time.Second))
	done := make(chan []runtimelifecycleprobe.Signal, 1)
	go func() {
		done <- probe.Expect("event-2").
			PostCommitDispatchStarted().
			NodeInProgress("node-a").
			HandlerStarted("node-a").
			HandlerCompleted("node-a").
			NodeDelivered("node-a").
			Require()
	}()

	signals := []runtimelifecycleprobe.Signal{
		{Kind: runtimelifecycleprobe.PostCommitDispatchStarted, EventID: "event-2"},
		{Kind: runtimelifecycleprobe.DeliveryStatusChanged, EventID: "event-2", SubscriberType: SubscriberNode, SubscriberID: "node-a", Status: StatusInProgress},
		{Kind: runtimelifecycleprobe.HandlerStarted, EventID: "event-2", SubscriberType: SubscriberNode, SubscriberID: "node-a"},
		{Kind: runtimelifecycleprobe.HandlerCompleted, EventID: "event-2", SubscriberType: SubscriberNode, SubscriberID: "node-a"},
		{Kind: runtimelifecycleprobe.DeliveryStatusChanged, EventID: "event-2", SubscriberType: SubscriberNode, SubscriberID: "node-a", Status: StatusDelivered},
	}
	for _, signal := range signals {
		probe.NotifyLifecycle(context.Background(), signal)
	}

	select {
	case got := <-done:
		if len(got) != len(signals) {
			t.Fatalf("signals len = %d, want %d", len(got), len(signals))
		}
	case <-time.After(time.Second):
		t.Fatal("expectation did not receive future lifecycle signals")
	}
}

func TestExpectationConsumesDistinctHistoricalSignals(t *testing.T) {
	probe := New(t, WithTimeout(time.Second))
	times := []time.Time{
		time.Unix(1, 0).UTC(),
		time.Unix(2, 0).UTC(),
		time.Unix(3, 0).UTC(),
	}
	for i, status := range []string{StatusInProgress, StatusFailed, StatusInProgress} {
		probe.NotifyLifecycle(context.Background(), runtimelifecycleprobe.Signal{
			Kind:           runtimelifecycleprobe.DeliveryStatusChanged,
			EventID:        "event-3",
			SubscriberType: SubscriberNode,
			SubscriberID:   "node-a",
			Status:         status,
			At:             times[i],
		})
	}

	got := probe.Expect("event-3").
		NodeInProgress("node-a").
		NodeFailed("node-a").
		NodeInProgress("node-a").
		Require()
	if len(got) != 3 {
		t.Fatalf("signals len = %d, want 3", len(got))
	}
	if got[0].At != times[0] || got[1].At != times[1] || got[2].At != times[2] {
		t.Fatalf("expectation times = %s, %s, %s; want %s, %s, %s",
			got[0].At, got[1].At, got[2].At, times[0], times[1], times[2])
	}
}
