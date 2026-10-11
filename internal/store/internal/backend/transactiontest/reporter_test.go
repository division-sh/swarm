package transactiontest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReporterWindowRequiresFinalReceiptAndPreservesSuccessor(t *testing.T) {
	var slot Slot
	delay := time.Millisecond
	old, restore, err := slot.Install(Options{Delay: delay, DelayScope: DelayServingWrites})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	submitted := time.Now()
	commit := func(operation Operation, finish bool) *Attempt {
		a := slot.Begin(false, false)
		if a == nil {
			t.Fatal("successor collector was detached")
		}
		a.Begun()
		Mark(WithAttempt(context.Background(), a), operation)
		a.BeforeCommit()
		a.Committed()
		if finish {
			a.Finish(nil)
		}
		return a
	}
	commit(FanOutClaim, true)
	last := commit(FanOutChunk, false)
	// The native COMMIT has happened, but its Finish receipt is held. A visible
	// cursor or the acknowledged-but-unregistered attempt must not close the window.
	if err := ValidateReporterIssuance(old.Snapshot(), submitted, 1, delay); err == nil {
		t.Fatal("held final receipt admitted early detachment")
	}
	last.Finish(nil)
	issuance := old.Snapshot()
	if err := ValidateReporterIssuance(issuance, submitted, 1, delay); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"missing delay", "wrong scope", "missing claim", "separate release", "failed attempt"} {
		t.Run(mutation, func(t *testing.T) {
			bad := old.Snapshot()
			switch mutation {
			case "missing delay":
				counts := bad.ByOperation[FanOutChunk]
				counts.DelayedCommits--
				bad.ByOperation[FanOutChunk] = counts
			case "wrong scope":
				bad.Total.DelayedCommits++
			case "missing claim":
				delete(bad.ByOperation, FanOutClaim)
			case "separate release":
				bad.ByOperation[FanOutRelease] = Counts{WriteCommits: 1}
			case "failed attempt":
				counts := bad.ByOperation[FanOutChunk]
				counts.Failed++
				bad.ByOperation[FanOutChunk] = counts
			}
			if err := ValidateReporterIssuance(bad, submitted, 1, delay); err == nil {
				t.Fatal("counterexample admitted detachment")
			}
		})
	}
	captured := slot.Begin(false, true)
	captured.Begun()
	Mark(WithAttempt(context.Background(), captured), FanOutChunk)
	old.Stop()
	successor, stopSuccessor, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer stopSuccessor()
	if err := ValidateReporterContinuation(issuance, old.Snapshot(), successor.Snapshot()); err == nil {
		t.Fatal("empty successor earned continuation credit")
	}
	// Old Stop and its registered restoration both run after replacement.
	old.Stop()
	restore()
	commit(FanOutClaim, true)
	commit(FanOutChunk, true)
	if err := ValidateReporterContinuation(issuance, old.Snapshot(), successor.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if old.Snapshot().Active != 1 || successor.Snapshot().Active != 0 {
		t.Fatal("precaptured work disappeared or moved to the successor")
	}
	captured.Finish(errors.New("captured failure after detachment"))
	final := old.Snapshot()
	if final.Active != 0 || final.Total.Failed != 1 || final.Total.RollbackAttempts != 0 || successor.Snapshot().Total.Failed != 0 {
		t.Fatalf("captured failure was lost or reassigned: old=%+v successor=%+v", final, successor.Snapshot())
	}
	if err := ValidateReporterContinuation(issuance, final, successor.Snapshot()); err == nil {
		t.Fatal("late captured error was erased from continuation admission")
	}
}

func TestReporterStressRejectsEarlyStopAndServingOnly(t *testing.T) {
	delay := time.Millisecond
	issuance := Snapshot{Total: Counts{WriteCommits: 40}}
	final := Snapshot{
		Total: Counts{WriteCommits: 43},
		ByOperation: map[Operation]Counts{
			FanOutClaim: {WriteCommits: 21, DelayedCommits: 21, InjectedDelay: 21 * delay},
			FanOutChunk: {WriteCommits: 21, DelayedCommits: 21, InjectedDelay: 21 * delay},
			Other:       {WriteCommits: 1},
		},
	}
	if err := ValidateReporterStress(issuance, final, delay); err == nil {
		t.Fatal("serving-only injection earned all-commit stress credit")
	}
	final.ByOperation[Other] = Counts{WriteCommits: 1, DelayedCommits: 1, InjectedDelay: delay}
	if err := ValidateReporterStress(issuance, final, delay); err != nil {
		t.Fatal(err)
	}
	// A rolled-back speculative read has no COMMIT attempt and must not become
	// either an acknowledged write or a missing all-commit injection.
	final.ByOperation[Other] = Counts{WriteCommits: 1, DelayedCommits: 1, InjectedDelay: delay, Failed: 1, RollbackAttempts: 1}
	if err := ValidateReporterStress(issuance, final, delay); err != nil {
		t.Fatal(err)
	}
	final.ByOperation[FanOutChunk] = Counts{WriteCommits: 20, DelayedCommits: 20, InjectedDelay: 20 * delay}
	if err := ValidateReporterStress(issuance, final, delay); err == nil {
		t.Fatal("stopped-before-mixed collector earned stress credit")
	}
}
