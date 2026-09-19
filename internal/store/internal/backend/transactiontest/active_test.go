package transactiontest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestActiveTransactionTailClassification(t *testing.T) {
	var slot Slot
	collector, restore, err := slot.Install(Options{Delay: 20 * time.Millisecond, DelayScope: DelayAllCommits})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	write := slot.Begin(false, true)
	read := slot.Begin(true, false)
	assertActive := func(class ActiveClass, count uint64) {
		t.Helper()
		snapshot := collector.Snapshot()
		var total uint64
		for _, n := range snapshot.ActiveByClass {
			total += n
		}
		if total != snapshot.Active || snapshot.ActiveByClass[class] != count {
			t.Fatalf("active class %v count %d: %+v", class, count, snapshot)
		}
	}
	assertActive(ActiveClass{Operation: Other, Phase: PhaseBeginning, Retained: true}, 1)
	write.Begun()
	read.Begun()
	Mark(WithAttempt(context.Background(), write), PipelineSettlement)
	Mark(WithAttempt(context.Background(), read), FanOutObservation)
	assertActive(ActiveClass{Operation: PipelineSettlement, Phase: PhaseOperation, Retained: true}, 1)
	frozen := collector.Snapshot()
	for class := range frozen.ActiveByClass {
		delete(frozen.ActiveByClass, class)
	}
	assertActive(ActiveClass{Operation: FanOutObservation, Phase: PhaseOperation, ReadOnly: true}, 1)
	done := make(chan struct{})
	go func() {
		write.BeforeCommit()
		close(done)
	}()
	delayClass := ActiveClass{Operation: PipelineSettlement, Phase: PhaseCommitDelay, Retained: true}
	deadline := time.Now().Add(time.Second)
	for collector.Snapshot().ActiveByClass[delayClass] == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	assertActive(delayClass, 1)
	<-done
	assertActive(ActiveClass{Operation: PipelineSettlement, Phase: PhaseCommitCall, Retained: true}, 1)
	write.Committed()
	read.BeforeCommit()
	read.CommitFailed()
	assertActive(ActiveClass{Operation: FanOutObservation, Phase: PhaseCommitFailureCleanup, ReadOnly: true}, 1)
	read.RollbackAttempted()
	assertActive(ActiveClass{Operation: PipelineSettlement, Phase: PhaseAcknowledgedCleanup, Retained: true}, 1)
	assertActive(ActiveClass{Operation: FanOutObservation, Phase: PhaseRollbackCleanup, ReadOnly: true}, 1)
	write.Finish(nil)
	read.Finish(errors.New("observation failed"))
	snapshot := collector.Snapshot()
	if snapshot.Active != 0 || len(snapshot.ActiveByClass) != 0 || snapshot.Total.WriteCommits != 1 || snapshot.Total.Failed != 1 {
		t.Fatalf("settled transaction left a live tail: %+v", snapshot)
	}
}
