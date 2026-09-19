package conformance

import (
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func TestFanOutServingM01ThreeShortCoalescedBacklogOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			workers := 1
			probe := newServingMatrixProbe(servingMatrixEvaluation, 1)
			f := newServingMatrixFixture(t, backend, &workers, probe)
			_, runID := f.startRun(t, 0)
			emptyGate := make(chan struct{})
			releaseEmpty := sync.OnceFunc(func() { close(emptyGate) })
			defer releaseEmpty()
			scans := observeServingMatrixScans(t, f.runtimes[0], emptyGate)
			dropped := servingMatrixDroppedWake{calls: make(chan time.Time, 16)}
			f.runtimes[0].pipeline.InstallFanOutWorkNotifier(dropped)
			// Form the complete real backlog behind a completed empty read. The
			// sub-tick serving budget does not include fixture ingress setup.
			f.submit(t, 0, runID, "coalesced-0")
			f.submit(t, 0, runID, "coalesced-1")
			f.submit(t, 0, runID, "coalesced-2")
			waitServingMatrixIntentCount(t, f, 3)
			releaseEmpty()
			f.runtimes[0].fanOutServing.Wake()
			first := waitServingMatrixHeld(t, probe)
			discovered := waitServingMatrixDiscovery(t, scans, first.key)
			var intents, cursor int
			if err := f.db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intents, &cursor); err != nil || intents != 3 || cursor != 0 {
				t.Fatalf("coalesced durable backlog before release: intents=%d cursor=%d err=%v", intents, cursor, err)
			}
			for i := 0; i < 32; i++ {
				f.runtimes[0].fanOutServing.Wake()
			}
			first.release()
			// Finish before another production recovery tick can rescue backlog.
			deadline := discovered.at.Add(750 * time.Millisecond)
			seen := make(map[fanoutobligation.IntentKey]bool)
			for i := 0; i < 3; i++ {
				receipt := waitServingMatrixReceipt(t, probe, deadline)
				if receipt.err != nil || !receipt.result.Refill || seen[receipt.turn.key] {
					t.Fatalf("closed short turn lost queue refill: result=%+v key=%+v duplicate=%t err=%v", receipt.result, receipt.turn.key, seen[receipt.turn.key], receipt.err)
				}
				seen[receipt.turn.key] = true
				if len(receipt.turn.command.Outcomes) != 1 || receipt.turn.command.Outcomes[0].Publication == nil {
					t.Fatalf("short turn did not use real evaluator/publication: %+v", receipt.turn.command)
				}
			}
			f.assertSettled(t, 0, runID, 3)
			_, peak, duplicates, turns := probe.snapshot()
			if peak != 1 || duplicates != 0 || turns != 3 {
				t.Fatalf("coalesced serving ownership: peak=%d duplicates=%d turns=%d", peak, duplicates, turns)
			}
			t.Log("M01: three real short intents drained through one shared permit before sweep rescue, with coalesced hints and exact durable histories")
		})
	}
}

func TestFanOutServingM02ProductionRecoveryWithoutWakeOnBothBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			probe := newServingMatrixProbe(servingMatrixUnheld, 0)
			f := newServingMatrixFixture(t, backend, nil, probe)
			_, runID := f.startRun(t, 0)
			scans := observeServingMatrixScans(t, f.runtimes[0])
			dropped := servingMatrixDroppedWake{calls: make(chan time.Time, 16)}
			f.runtimes[0].pipeline.InstallFanOutWorkNotifier(dropped)
			f.submit(t, 0, runID, "missing-wake")
			var eligibleAt time.Time
			select {
			case eligibleAt = <-dropped.calls:
			case <-time.After(5 * time.Second):
				t.Fatal("real durable ingress did not reach its dropped post-commit wake")
			}
			var attempt servingMatrixAttempt
			select {
			case attempt = <-probe.attempts:
			case <-time.After(2 * time.Second):
				t.Fatal("production recovery did not enter the actual claim operation")
			}
			discovery := waitServingMatrixDiscovery(t, scans, attempt.key)
			if attempt.key.RunID != runID || attempt.at.Before(discovery.at) {
				t.Fatalf("recovery timestamps lack exact candidate/claim ordering: discovery=%+v attempt=%+v", discovery, attempt)
			}
			if delay := discovery.at.Sub(eligibleAt); delay > time.Second {
				t.Fatalf("production discovery exceeded unchanged 1s bound: %s", delay)
			}
			if delay := attempt.at.Sub(eligibleAt); delay > time.Second {
				t.Fatalf("actual claim entry exceeded unchanged 1s bound: %s", delay)
			}
			receipt := waitServingMatrixReceipt(t, probe, time.Now().Add(5*time.Second))
			if receipt.err != nil || !receipt.result.Refill {
				t.Fatalf("recovered production turn: %+v err=%v", receipt.result, receipt.err)
			}
			f.assertSettled(t, 0, runID, 1)
			t.Logf("M02: real empty scan, missing post-commit wake; discovery=%s actual-claim-entry=%s discovery-to-entry=%s claim-to-loaded=%s", discovery.at.Sub(eligibleAt), attempt.at.Sub(eligibleAt), attempt.at.Sub(discovery.at), receipt.turn.loadedAt.Sub(attempt.at))
		})
	}
}

func waitServingMatrixDiscovery(t *testing.T, scans <-chan servingMatrixScan, key fanoutobligation.IntentKey) servingMatrixScan {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case scan := <-scans:
			if scan.err != nil {
				t.Fatalf("shared selector failed: %v", scan.err)
			}
			if scan.found && scan.candidate.Key == key {
				return scan
			}
		case <-deadline.C:
			t.Fatal("actual serving selector never observed the exact claimed candidate")
		}
	}
}
