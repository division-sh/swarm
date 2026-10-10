package channeldelivery

import (
	"context"
	"sync"
	"testing"
)

func TestChannelReconcileSignalAcknowledgedChangesOnly(t *testing.T) {
	var signal ReconcileSignal
	sub, err := signal.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	for _, tc := range []struct {
		ack    bool
		demand ReconcileDemand
	}{{false, ReconcileOrdinary}, {false, ReconcileNative}, {true, 0}} {
		if err := signal.PublishAcknowledged(tc.ack, tc.demand); err != nil {
			t.Fatal(err)
		}
	}
	if demand, start := sub.BeginPass(); demand != 0 || start.Sequence != 0 {
		t.Fatalf("unacknowledged/no-op changes woke worker: %v %+v", demand, start)
	}
	if err := signal.PublishAcknowledged(true, ReconcileDemand(128)); err == nil {
		t.Fatal("unknown scope admitted")
	}
	if err := signal.PublishAcknowledged(true, ReconcileOrdinary); err != nil {
		t.Fatal(err)
	}
	if demand, start := sub.BeginPass(); demand != ReconcileOrdinary || start.Sequence != 1 || start.Subscription == 0 {
		t.Fatalf("acknowledged ordinary change lost: %v %+v", demand, start)
	}
}

func TestChannelReconcileSignalCoalescesAndKeepsInPassChange(t *testing.T) {
	var signal ReconcileSignal
	sub, err := signal.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Close)
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = signal.PublishAcknowledged(true, ReconcileOrdinary|ReconcileNative)
		}()
	}
	group.Wait()
	demand, first := sub.BeginPass()
	if demand != ReconcileOrdinary|ReconcileNative || first.Sequence != 32 {
		t.Fatalf("coalesced scope/count changed: %v %+v", demand, first)
	}
	if err := signal.PublishAcknowledged(true, ReconcileOrdinary); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Wake():
	default:
		t.Fatal("delivery created after the scan was not kept pending")
	}
	demand, next := sub.BeginPass()
	if demand != ReconcileOrdinary || next.Sequence != first.Sequence+1 {
		t.Fatalf("in-pass change lost: %v %+v", demand, next)
	}
	if demand, _ := sub.BeginPass(); demand != 0 {
		t.Fatal("consumed hints sustain empty passes")
	}
}

func TestChannelReconcileSubscriptionRetirementAndReplacement(t *testing.T) {
	var signal ReconcileSignal
	ctx, cancel := context.WithCancel(context.Background())
	old, err := signal.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, oldMark := old.BeginPass()
	if _, err := signal.Subscribe(context.Background()); err == nil {
		t.Fatal("a second worker subscribed")
	}
	cancel()
	if err := signal.PublishAcknowledged(true, ReconcileNative); err != nil {
		t.Fatal(err)
	}
	if demand, mark := old.BeginPass(); demand != 0 || mark.Subscription != 0 {
		t.Fatal("retired worker obtained a pass")
	}
	old.Close()
	next, err := signal.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(next.Close)
	old.Close()
	if err := signal.PublishAcknowledged(true, ReconcileNative); err != nil {
		t.Fatal(err)
	}
	demand, mark := next.BeginPass()
	if demand != ReconcileNative || mark.Subscription == oldMark.Subscription {
		t.Fatalf("predecessor altered successor subscription: %v %+v", demand, mark)
	}
}

func TestChannelReconcilePassRejectsForeignAndPreCutEvidence(t *testing.T) {
	var selected, foreign ReconcileSignal
	subscription, err := selected.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	other, err := foreign.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_, initial := subscription.BeginPass()
	cut, _ := selected.Mark()
	_, foreignStart := other.BeginPass()
	_, foreignLater := other.BeginPass()
	_, selectedLater := subscription.BeginPass()
	if initial.StartedAfter(cut) || foreignStart.StartedAfter(cut) || foreignLater.StartedAfter(cut) || !selectedLater.StartedAfter(cut) {
		t.Fatalf("invalid pass credit: initial=%#v foreign=%#v successor=%#v cut=%#v", initial, foreignLater, selectedLater, cut)
	}
	if selectedLater.StartedAfter(ReconcileMark{Subscription: cut.Subscription, Pass: cut.Pass}) {
		t.Fatal("forged counter-only cut received pass credit")
	}
}
