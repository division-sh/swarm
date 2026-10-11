package transactiontest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCollectorStopAndJoinRetainsUnfinishedAndFailedAttempts(t *testing.T) {
	var slot Slot
	collector, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	held := slot.Begin(true, false)
	held.Begun()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := collector.StopAndJoin(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unfinished join did not preserve its cancellation: %v", err)
	}
	if collector.Snapshot().Active != 1 || slot.Begin(true, false) != nil {
		t.Fatal("join timeout lost captured work or left future capture open")
	}
	held.BeforeCommit()
	held.Committed()
	held.Finish(errors.New("genuine held cleanup failure"))
	joinCtx, stopJoin := context.WithTimeout(context.Background(), time.Second)
	defer stopJoin()
	if err := collector.StopAndJoin(joinCtx); err != nil {
		t.Fatal(err)
	}
	snapshot := collector.Snapshot()
	if snapshot.Active != 0 || snapshot.Total.ReadCommits != 1 || snapshot.Total.CleanupFailures != 1 {
		t.Fatalf("join discarded held failure: %+v", snapshot)
	}
}
