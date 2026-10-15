package transactiontest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRevisionReceiptScopesAndFailure(t *testing.T) {
	ctx := context.Background()
	BeginRevision(ctx).End()
	BeginRevisionLock(ctx).End()
	CountRevisionSQL(ctx, RevisionExec)
	var slot Slot
	collector, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	a := slot.Begin(false, true)
	a.Begun()
	ctx = WithAttempt(ctx, a)
	Mark(ctx, PipelineSettlement)
	phase := BeginRevision(ctx)
	lock := BeginRevisionLock(ctx)
	time.Sleep(time.Millisecond)
	CountRevisionSQL(ctx, RevisionQueryRow)
	lock.End()
	CountRevisionSQL(ctx, RevisionExec)
	CountRevisionSQL(ctx, RevisionQuery)
	phase.End()
	a.RollbackAttempted()
	a.Finish(errors.New("revision callback failed"))
	snapshot := collector.Snapshot()
	r := snapshot.Total.Revision
	if r.Finalizations != 1 || r.LockPhases != 1 || r.ExecCalls != 1 || r.QueryCalls != 1 || r.QueryRowCalls != 1 || r.LockDuration < time.Millisecond || r.Duration < r.LockDuration {
		t.Fatalf("revision receipt = %+v", r)
	}
	if snapshot.Total.Failed != 1 || snapshot.Total.CommitAttempts != 0 || snapshot.Retained.Revision != r || snapshot.ByOperation[PipelineSettlement].Revision != r {
		t.Fatalf("failed retained revision lost attribution: %+v", snapshot)
	}
	wantCalls := RevisionSQLCounts{ExecCalls: 1, QueryCalls: 1, QueryRowCalls: 1}
	if r.UncommittedSQL != wantCalls || r.RollbackAttemptedSQL != wantCalls || r.CommittedSQL != (RevisionSQLCounts{}) || r.IndeterminateSQL != (RevisionSQLCounts{}) {
		t.Fatalf("revision SQL outcome lost: %+v", r)
	}
}

func TestRevisionSQLReceiptCommitAndIndeterminateOutcomes(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		var slot Slot
		collector, stop, err := slot.Install(Options{})
		if err != nil {
			t.Fatal(err)
		}
		a := slot.Begin(false, false)
		a.Begun()
		ctx := WithAttempt(context.Background(), a)
		CountRevisionSQL(ctx, RevisionExec)
		a.BeforeCommit()
		if acknowledged {
			a.Committed()
		} else {
			a.RollbackAttempted()
		}
		a.Finish(nil)
		r := collector.Snapshot().Total.Revision
		want := RevisionSQLCounts{ExecCalls: 1}
		if acknowledged && (r.CommittedSQL != want || r.IndeterminateSQL != (RevisionSQLCounts{})) ||
			!acknowledged && (r.IndeterminateSQL != want || r.RollbackAttemptedSQL != want || r.CommittedSQL != (RevisionSQLCounts{})) {
			t.Fatalf("acknowledged=%v revision SQL outcome=%+v", acknowledged, r)
		}
		stop()
	}
}
