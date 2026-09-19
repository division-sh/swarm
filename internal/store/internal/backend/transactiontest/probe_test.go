package transactiontest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProbeCommitOutcomesAndDelaySelection(t *testing.T) {
	for _, scope := range []DelayScope{DelayAllCommits, DelayAllWrites, DelayServingWrites} {
		t.Run(string(scope), func(t *testing.T) {
			var slot Slot
			collector, restore, err := slot.Install(Options{Delay: time.Millisecond, DelayScope: scope})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, tc := range []struct {
				read, ack, failCommit bool
				op                    Operation
			}{
				{false, true, false, FanOutClaim},
				{false, true, false, Other},
				{true, true, false, FanOutLoad},
				{false, false, true, FanOutChunk},
			} {
				a := slot.Begin(tc.read, true)
				a.Begun()
				ctx := WithAttempt(context.Background(), a)
				Mark(ctx, tc.op)
				Mark(ctx, FanOutProducer)
				a.BeforeCommit()
				if tc.ack {
					a.Committed()
				}
				if tc.failCommit {
					a.RollbackAttempted()
				}
				a.Finish(errors.New("commit or post-commit cleanup failure"))
			}
			got := collector.Snapshot()
			wantDelays := uint64(4)
			if scope == DelayAllWrites {
				wantDelays = 3
			}
			if scope == DelayServingWrites {
				wantDelays = 2
			}
			if got.Total.WriteCommits != 2 || got.Total.ReadCommits != 1 || got.Total.Failed != 1 || got.Total.CommitFailures != 1 || got.Total.CleanupFailures != 3 || got.Total.DelayedCommits != wantDelays || got.Retained != got.Total || got.Active != 0 {
				t.Fatalf("commit receipts changed outcome or delay meaning: %+v", got)
			}
		})
	}
}

func TestCommitAcknowledgementTimesPrecedeReorderedCleanup(t *testing.T) {
	var slot Slot
	collector, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	first := slot.Begin(false, false)
	second := slot.Begin(false, false)
	for _, a := range []*Attempt{first, second} {
		a.Begun()
		Mark(WithAttempt(context.Background(), a), FanOutChunk)
		a.BeforeCommit()
	}
	beforeAck := time.Now()
	first.Committed()
	second.Committed()
	afterAck := time.Now()
	time.Sleep(time.Millisecond)
	second.Finish(nil)
	first.Finish(errors.New("post-commit cleanup failed"))
	got := collector.Snapshot().ByOperation[FanOutChunk]
	if !got.FirstCommitAt.Equal(first.committedAt) || !got.LastCommitAt.Equal(second.committedAt) || got.FirstCommitAt.Before(beforeAck) || got.LastCommitAt.After(afterAck) || got.WriteCommits != 2 || got.CleanupFailures != 1 {
		t.Fatalf("acknowledgements reflect cleanup rather than commit return: %+v", got)
	}
}
