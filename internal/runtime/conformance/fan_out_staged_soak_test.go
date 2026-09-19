package conformance

import (
	"fmt"
	"testing"
	"time"
)

// Requires a coordinated >=35m package budget for both stores. Rolling waves
// retain at least22 durable unfinished intents for the full15m before draining.
func TestIssue2394TwentyTwoIntentFifteenMinuteSoakBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSupplementalPressureSoak(t, backend, 22, 15*time.Minute, time.Second) })
	}
}

// Sparse staged arrivals are a recovery control, not22-concurrent-intent soak
// evidence. Keep the previously qualified finite control distinct.
func TestIssue2394SupplementalSoakFiniteProbeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSupplementalStagedSoak(t, backend, 3, 2, 2200*time.Millisecond) })
	}
}

func proveSupplementalStagedSoak(t *testing.T, backend string, intents, cardinality int, span time.Duration) {
	t.Helper()
	p := newSupplementalServingProbe(0)
	f := newSupplementalServingFixture(t, backend, p)
	_, run := f.startRun(t, 0)
	installSupplementalScanObserver(t, f, p, nil)
	f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
	started := time.Now()
	observation := time.NewTicker(15 * time.Second)
	defer observation.Stop()
	var firstArrival, lastArrival, lastEffect time.Time
	for batch := 0; batch < intents; batch++ {
		due := started.Add(time.Duration(batch) * span / time.Duration(intents-1))
		wait := time.NewTimer(time.Until(due))
	waitArrival:
		for {
			select {
			case <-wait.C:
				break waitArrival
			case <-observation.C:
				logSupplementalState(t, f, p, fmt.Sprintf("staged_wait_next=%d", batch))
			}
		}
		wait.Stop()
		// Each recovery-control stage submits new real work at its due time;
		// this does not establish continuously concurrent unfinished intents.
		arrival := time.Now()
		if firstArrival.IsZero() {
			firstArrival = arrival
		}
		lastArrival = arrival
		logSupplementalState(t, f, p, fmt.Sprintf("before_arrival=%d", batch))
		submitSupplementalIntent(t, f, run, batch, cardinality)
		wake := requireSupplementalWake(t, p, batch+1)
		r := waitSupplementalReceipt(t, p, cardinality)
		if r.Key.RunID != run || r.ClaimAt.Before(arrival) || r.Discovery.Before(arrival) {
			t.Fatalf("stage%d was not served from its real arrival: %+v", batch, r)
		}
		// The notifier is after durable ingress. A concurrent selector may see
		// the commit before the dropped notifier is called; negative deltas are
		// retained rather than inventing an eligibility timestamp.
		if r.Discovery.Sub(wake) > time.Second || r.ClaimAt.Sub(wake) > time.Second {
			t.Fatalf("stage%d missed unchanged production1s recovery: wake=%s discovery=%s claim=%s", batch, wake, r.Discovery, r.ClaimAt)
		}
		lastEffect = r.CommitReturned
		logSupplementalState(t, f, p, fmt.Sprintf("after_arrival=%d", batch))
		t.Logf("stage=%d scheduled=%s arrival=%s post_commit_wake_suppressed=%s discovered=%s actual_claim=%s claim_delay=%s cursor_delta=%d commit_return=%s handoff_done=%s", batch, due.Format(time.RFC3339Nano), arrival.Format(time.RFC3339Nano), wake.Format(time.RFC3339Nano), r.Discovery.Format(time.RFC3339Nano), r.ClaimAt.Format(time.RFC3339Nano), r.ClaimAt.Sub(wake), cardinality, r.CommitReturned.Format(time.RFC3339Nano), r.FinishedAt.Format(time.RFC3339Nano))
		assertSupplementalEffects(t, f, run, 0, batch+1, cardinality)
	}
	if lastArrival.Before(started.Add(span)) || !lastEffect.After(lastArrival) || firstArrival.Before(started) {
		t.Fatalf("staged work did not span required duration: start=%s first=%s last=%s effect=%s span=%s", started, firstArrival, lastArrival, lastEffect, span)
	}
	s := p.snapshot()
	if s.Err != nil || s.Turns != intents || s.Finished != intents || s.DroppedWakes != intents || s.Loaded != 0 || s.PendingCommits != 0 || s.Commit.Count != intents || s.FoundScans != intents || s.EmptyScans < 2 {
		t.Fatalf("bounded sustained accounting: %+v want_intents=%d", s, intents)
	}
	select {
	case extra := <-p.completed:
		t.Fatalf("extra staged disposition: %+v", extra)
	default:
	}
	t.Logf("staged recovery control backend=%s arrivals=%d rows_each=%d scheduled_span=%s elapsed=%s stats=%+v; not concurrent-pressure soak evidence", backend, intents, cardinality, span, time.Since(started), s)
}
