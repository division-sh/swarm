package transactiontest

import (
	"testing"
)

func TestCollectorAtomicReplacementNeverLosesBegin(t *testing.T) {
	var slot Slot
	old, restore, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	// Hold Begin after capture while it still owns the slot lock. Replacement
	// must join that registration, then publish the successor without a nil gap.
	old.mu.Lock()
	locked := true
	defer func() {
		if locked {
			old.mu.Unlock()
		}
	}()
	begun := make(chan *Attempt, 1)
	go func() { begun <- slot.Begin(false, true) }()
	waitProbeMutexBlocked(t, "(*Slot).Begin", nil)
	replaced := make(chan *Collector, 1)
	replaceErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		next, _, err := old.Replace(Options{})
		replaced <- next
		replaceErr <- err
		close(done)
	}()
	waitProbeMutexBlocked(t, "(*Collector).Replace", done)
	old.mu.Unlock()
	locked = false
	first := <-begun
	next := <-replaced
	if err := <-replaceErr; err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	if first == nil || first.collector != old || old.Snapshot().Active != 1 {
		t.Fatal("replacement lost the captured predecessor attempt")
	}
	old.Stop()
	restore()
	second := slot.Begin(false, false)
	if second == nil || second.collector != next || next.Snapshot().Active != 1 {
		t.Fatal("replacement or stale cleanup lost the successor attempt")
	}
	first.Begun()
	first.BeforeCommit()
	first.Committed()
	first.Finish(nil)
	second.Begun()
	second.BeforeCommit()
	second.Committed()
	second.Finish(nil)
	if old.Snapshot().Active != 0 || next.Snapshot().Active != 0 || old.Snapshot().Total.WriteCommits != 1 || next.Snapshot().Total.WriteCommits != 1 {
		t.Fatalf("replacement reassigned or lost receipts: old=%+v next=%+v", old.Snapshot(), next.Snapshot())
	}
	if _, _, err := old.Replace(Options{}); err == nil {
		t.Fatal("stale predecessor replaced the successor")
	}
	if _, _, err := next.Replace(Options{DelayScope: "invalid"}); err == nil {
		t.Fatal("invalid replacement changed the current collector")
	}
	unchanged := slot.Begin(true, false)
	if unchanged == nil || unchanged.collector != next {
		t.Fatal("invalid replacement detached the successor")
	}
	unchanged.Finish(nil)
}

func TestCollectorTwoStepReplacementHasUnobservedBegin(t *testing.T) {
	var slot Slot
	old, _, err := slot.Install(Options{})
	if err != nil {
		t.Fatal(err)
	}
	old.Stop()
	if slot.Begin(false, false) != nil {
		t.Fatal("negative control no longer reproduces the two-step observation gap")
	}
}
