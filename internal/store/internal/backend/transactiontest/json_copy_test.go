package transactiontest

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestJSONCopyReceiptOutcomes(t *testing.T) {
	for _, outcome := range []string{"committed", "cleanup_failed", "rollback", "commit_unknown", "failed_write", "unfinished", "no_write"} {
		t.Run(outcome, func(t *testing.T) {
			var slot Slot
			collector, restore, err := slot.Install(Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			a := slot.Begin(false, true)
			a.Begun()
			ctx := WithAttempt(context.Background(), a)
			Mark(ctx, WorkflowMutation)
			want := CopyCounts{}
			if outcome != "no_write" {
				write := BeginWorkflowHeaderJSON(ctx, 17)
				want = CopyCounts{Calls: 1, Copies: 1, SubmittedBytes: 17}
				switch outcome {
				case "unfinished":
					want.UnfinishedBytes = 17
				case "failed_write":
					write.End(errors.New("write refused"))
					want.FailedCalls, want.FailedBytes = 1, 17
				default:
					write.End(nil)
					write.End(errors.New("duplicate notification"))
					want.SucceededCalls, want.SucceededBytes = 1, 17
				}
			}
			var finalErr error
			switch outcome {
			case "committed", "cleanup_failed":
				a.BeforeCommit()
				a.Committed()
				want.CommittedBytes = 17
				if outcome == "cleanup_failed" {
					finalErr = errors.New("cleanup failed after acknowledgment")
				}
			case "commit_unknown":
				a.BeforeCommit()
				a.RollbackAttempted()
				want.IndeterminateBytes, want.RollbackAttemptedBytes = 17, 17
				finalErr = errors.New("commit outcome unknown")
			case "rollback":
				a.RollbackAttempted()
				want.UncommittedBytes, want.RollbackAttemptedBytes = 17, 17
				finalErr = errors.New("later owner refused")
			}
			a.Finish(finalErr)
			a.Finish(finalErr)
			got := collector.Snapshot()
			if got.Active != 0 || got.Total.JSONCopies.WorkflowHeader != want || got.Total.JSONCopies.EntityMetadata != (CopyCounts{}) || got.Retained.JSONCopies != got.Total.JSONCopies || got.ByOperation[WorkflowMutation].JSONCopies != got.Total.JSONCopies {
				t.Fatalf("outcome=%s receipt=%+v want=%+v", outcome, got, want)
			}
		})
	}
}

func TestJSONCopyReceiptDisabledAndCollectorReplacement(t *testing.T) {
	ctx := context.Background()
	BeginWorkflowHeaderJSON(ctx, 9).End(nil)
	BeginEntityMetadataJSON(ctx, 11, 2).End(nil)
	var slot Slot
	first, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	a := slot.Begin(false, false)
	a.Begun()
	ctx = WithAttempt(ctx, a)
	BeginEntityMetadataJSON(ctx, 11, 2).End(nil)
	BeginEntityMetadataJSON(ctx, 0, 0).End(nil)
	first.Stop()
	if slot.Begin(false, false) != nil || first.Snapshot().Active != 1 {
		t.Fatal("stop lost captured attempt or admitted another one")
	}
	second, stopSecond, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer stopSecond()
	restore()
	a.BeforeCommit()
	a.Committed()
	a.Finish(nil)
	b := slot.Begin(false, false)
	if b == nil {
		t.Fatal("stale cleanup detached the successor collector")
	}
	b.Begun()
	BeginWorkflowHeaderJSON(WithAttempt(context.Background(), b), 7).End(nil)
	b.RollbackAttempted()
	b.Finish(errors.New("refusal"))
	if got := first.Snapshot(); got.Active != 0 || got.Total.JSONCopies.EntityMetadata != (CopyCounts{Calls: 1, Copies: 2, SubmittedBytes: 11, SucceededCalls: 1, SucceededBytes: 11, CommittedBytes: 11}) || got.Total.JSONCopies.WorkflowHeader != (CopyCounts{}) {
		t.Fatalf("captured attempt lost exact attribution: %+v", got)
	}
	if got := second.Snapshot(); got.Active != 0 || got.Total.JSONCopies.WorkflowHeader.UncommittedBytes != 7 || got.Total.JSONCopies.EntityMetadata != (CopyCounts{}) {
		t.Fatalf("successor borrowed old bytes/outcomes: %+v", got)
	}
}

func TestJSONCopyReceiptConcurrentRetryAttempts(t *testing.T) {
	var slot Slot
	collector, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	var wg sync.WaitGroup
	for index := 0; index < 32; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for retry := 0; retry < 2; retry++ {
				a := slot.Begin(false, false)
				a.Begun()
				ctx := WithAttempt(context.Background(), a)
				Mark(ctx, WorkflowMutation)
				BeginWorkflowHeaderJSON(ctx, 3).End(nil)
				BeginEntityMetadataJSON(ctx, 5, 1).End(nil)
				if retry == 0 {
					a.RollbackAttempted()
					a.Finish(errors.New("retry"))
				} else {
					a.BeforeCommit()
					a.Committed()
					a.Finish(nil)
				}
			}
		}()
	}
	wg.Wait()
	got := collector.Snapshot()
	for _, copy := range []struct {
		counts CopyCounts
		bytes  uint64
	}{{got.Total.JSONCopies.WorkflowHeader, 3}, {got.Total.JSONCopies.EntityMetadata, 5}} {
		want := CopyCounts{Calls: 64, Copies: 64, SubmittedBytes: 64 * copy.bytes, SucceededCalls: 64, SucceededBytes: 64 * copy.bytes, CommittedBytes: 32 * copy.bytes, UncommittedBytes: 32 * copy.bytes, RollbackAttemptedBytes: 32 * copy.bytes}
		if copy.counts != want {
			t.Fatalf("concurrent retries lost or merged attempts: %+v want %+v", copy.counts, want)
		}
	}
	if got.Active != 0 || got.Total.BeginAttempts != 64 || got.Total.WriteCommits != 32 || got.Total.RollbackAttempts != 32 || got.ByOperation[WorkflowMutation].JSONCopies != got.Total.JSONCopies {
		t.Fatalf("concurrent outcomes lost: %+v", got)
	}
}
