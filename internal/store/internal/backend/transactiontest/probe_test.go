package transactiontest

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCollectorStopJoinsCapturedAttemptRegistration(t *testing.T) {
	var slot Slot
	collector, _, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Pause Begin after it captures this collector, before Active registration.
	collector.mu.Lock()
	locked := true
	begun := make(chan *Attempt, 1)
	go func() { begun <- slot.Begin(false, true) }()
	var attempt *Attempt
	defer func() {
		if locked {
			collector.mu.Unlock()
		}
		collector.Stop()
		if attempt == nil {
			attempt = <-begun
		}
		attempt.Finish(errors.New("test cleanup"))
	}()
	waitProbeMutexBlocked(t, "(*Slot).Begin", nil)
	stopped := make(chan struct{})
	go func() {
		collector.Stop()
		close(stopped)
	}()
	waitProbeMutexBlocked(t, "(*Collector).Stop", stopped)
	collector.mu.Unlock()
	locked = false
	attempt = <-begun
	<-stopped
	if attempt == nil || collector.Snapshot().Active != 1 || slot.Begin(false, false) != nil {
		t.Fatal("Stop lost the captured attempt or accepted an uncaptured successor")
	}
	attempt.Begun()
	BeginWorkflowHeaderJSON(WithAttempt(context.Background(), attempt), 17).End(nil)
	attempt.BeforeCommit()
	attempt.Committed()
	attempt.Finish(nil)
	got := collector.Snapshot()
	if got.Active != 0 || got.Total.BeginAttempts != 1 || got.Total.WriteCommits != 1 || got.Total.JSONCopies.WorkflowHeader.CommittedBytes != 17 {
		t.Fatalf("Stop published an incomplete captured receipt: %+v", got)
	}
}

// Mutex waits are not durably blocked under synctest. Observe the controlled
// blocked frame instead of relying on a wall-clock sleep or a production hook.
func waitProbeMutexBlocked(t *testing.T, method string, returned <-chan struct{}) {
	t.Helper()
	stack := make([]byte, 1<<20)
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-returned:
			t.Fatalf("%s returned before the captured attempt registered", method)
		default:
		}
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "[sync.Mutex.Lock]") && strings.Contains(goroutine, method) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not reach its controlled mutex wait", method)
		}
		runtime.Gosched()
	}
}

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
