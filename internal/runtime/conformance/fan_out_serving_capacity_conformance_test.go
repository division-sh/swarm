package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutServingM04CrossRunHeldTurnCapacityOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveServingMatrixCapacity(t, backend, false) })
	}
}

func TestFanOutServingM17SingleRunMultiIntentWorkConservationOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveServingMatrixCapacity(t, backend, true) })
	}
}

func proveServingMatrixCapacity(t *testing.T, backend string, sameRun bool) {
	t.Helper()
	const count = 5
	capacity := 1
	if backend == "postgres" {
		capacity = 4
	}
	probe := newServingMatrixProbe(servingMatrixEvaluation, -1)
	f := newServingMatrixFixture(t, backend, nil, probe)
	runIDs := make([]string, count)
	for i := range runIDs {
		if i == 0 || !sameRun {
			_, runIDs[i] = f.startRun(t, 0)
		} else {
			runIDs[i] = runIDs[0]
		}
	}
	// Run creation deliberately waits for bus-wide quiescence. Materialize all
	// independent runs before holding any serving turn under that same bus.
	for i := range runIDs {
		f.submit(t, 0, runIDs[i], fmt.Sprintf("capacity-%d", i))
	}
	var held []*servingMatrixTurn
	keys := make(map[fanoutobligation.IntentKey]bool)
	runs := make(map[string]bool)
	for i := 0; i < capacity; i++ {
		turn := waitServingMatrixHeld(t, probe)
		if turn.claim.Key != turn.key || keys[turn.key] || turn.loadedAt.IsZero() {
			t.Fatalf("capacity must cover distinct genuinely claimed/evaluated intents: %+v", turn.claim)
		}
		held = append(held, turn)
		keys[turn.key], runs[turn.key.RunID] = true, true
	}
	if sameRun && len(runs) != 1 || !sameRun && len(runs) != capacity {
		t.Fatalf("held turn run scope: sameRun=%t runs=%v capacity=%d", sameRun, runs, capacity)
	}
	waitServingMatrixIntentCount(t, f, count)
	f.runtimes[0].fanOutServing.Wake()
	select {
	case extra := <-probe.held:
		t.Fatalf("production default capacity exceeded: capacity=%d extra=%+v", capacity, extra.claim)
	case <-time.After(200 * time.Millisecond):
	}
	active, peak, duplicates, _ := probe.snapshot()
	if active != capacity || peak != capacity || duplicates != 0 {
		t.Fatalf("actual loaded-phase overlap: active=%d peak=%d duplicate=%d want=%d", active, peak, duplicates, capacity)
	}
	// Real control/read access remains available while all fan-out permits are
	// occupied. This observes the configured selected pool, not just arithmetic.
	readCtx, cancelRead := context.WithTimeout(testAuthorActivityContextForBundle(context.Background(), f.runtimes[0].sourceArtifactFact), 500*time.Millisecond)
	summary, err := f.selected.FanOutRunSummary(readCtx, runIDs[0], time.Now().UTC())
	cancelRead()
	if err != nil || summary.Cursor != 0 || summary.Committed != 0 {
		t.Fatalf("control/read access while all permits held: summary=%+v err=%v", summary, err)
	}
	var leased, untouched, cursor, outcomes int
	if err := f.db.QueryRow(`SELECT SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),SUM(CASE WHEN claim_generation=0 THEN 1 ELSE 0 END),SUM(cursor) FROM fan_out_intents`).Scan(&leased, &untouched, &cursor); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes`).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if leased != capacity || untouched != count-capacity || cursor != 0 || outcomes != 0 {
		t.Fatalf("capacity must precede durable claim: leased=%d untouched=%d cursor=%d outcomes=%d capacity=%d", leased, untouched, cursor, outcomes, capacity)
	}
	held[0].release()
	first := waitServingMatrixReceipt(t, probe, time.Now().Add(5*time.Second))
	if first.err != nil || !first.result.Refill || first.turn != held[0] {
		t.Fatalf("released exact turn: key=%+v result=%+v err=%v", first.turn.key, first.result, first.err)
	}
	next := waitServingMatrixHeld(t, probe)
	if keys[next.key] || next.claim.Key != next.key {
		t.Fatalf("released slot did not select a new exact intent: %+v", next.claim)
	}
	if sameRun && next.key.RunID != runIDs[0] {
		t.Fatalf("single-run spare capacity was diverted: %+v", next.key)
	}
	active, peak, duplicates, _ = probe.snapshot()
	if active != capacity || peak != capacity || duplicates != 0 {
		t.Fatalf("replacement turn did not refill exactly one free slot: active=%d peak=%d duplicate=%d want=%d", active, peak, duplicates, capacity)
	}
	probe.releaseAll()
	for i := 1; i < count; i++ {
		receipt := waitServingMatrixReceipt(t, probe, time.Now().Add(5*time.Second))
		if receipt.err != nil || !receipt.result.Refill {
			t.Fatalf("capacity backlog turn failed: key=%+v err=%v", receipt.turn.key, receipt.err)
		}
		t.Logf("exact intent %s: claim-entry-to-loaded=%s commit-entry-to-turn-finish=%s (evaluation includes explicit test hold)", receipt.turn.key.TriggeringDeliveryID, receipt.turn.loadedAt.Sub(receipt.turn.claimAt), receipt.at.Sub(receipt.turn.commitAt))
	}
	if sameRun {
		f.assertSettled(t, 0, runIDs[0], count)
	} else {
		for _, runID := range runIDs {
			f.assertSettled(t, 0, runID, 1)
		}
	}
	_, peak, duplicates, turns := probe.snapshot()
	if peak != capacity || duplicates != 0 || turns != count {
		t.Fatalf("final exact capacity accounting: peak=%d duplicates=%d turns=%d", peak, duplicates, turns)
	}
	t.Logf("held real loaded turns: backend=%s sameRun=%t capacity=%d; one released slot refilled while others remained held; exact publication histories preserved", backend, sameRun, capacity)
}

func waitServingMatrixIntentCount(t *testing.T, f *servingMatrixFixture, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var actual int
		if err := f.db.QueryRow(`SELECT COUNT(*) FROM fan_out_intents`).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if actual == count {
			return
		}
		if actual > count || time.Now().After(deadline) {
			t.Fatalf("real ingress durable intent count=%d want=%d", actual, count)
		}
		time.Sleep(time.Millisecond)
	}
}
