package conformance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// Full supplemental cost: 20x25 publications in each layout, on each backend
// (2,000 publications total). Coordinate this named run; no race-only skip.
func TestIssue2394TwentyIntentContentionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { compareSupplementalContention(t, backend, 20, 25) })
	}
}

func TestIssue2394SupplementalContentionFiniteProbeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { compareSupplementalContention(t, backend, 5, 2) })
	}
}

type supplementalContentionResult struct {
	Stats                                                            supplementalStats
	IngressToCommitReturn, ReleasedToCommitReturn, ReleasedToHandoff time.Duration
	ReleasedRowsPerSecond                                            float64
	ChunkTransactions, WorkloadTransactions                          storetest.TransactionCounts
}

func compareSupplementalContention(t *testing.T, backend string, intents, cardinality int) {
	t.Helper()
	var same, separate supplementalContentionResult
	t.Run("one_run", func(t *testing.T) { same = proveSupplementalContention(t, backend, intents, cardinality, true) })
	t.Run("independent_runs", func(t *testing.T) { separate = proveSupplementalContention(t, backend, intents, cardinality, false) })
	if t.Failed() {
		return
	}
	t.Logf("M17 comparison backend=%s intents=%d rows_per_intent=%d one_run=%+v independent_runs=%+v", backend, intents, cardinality, same, separate)
	t.Log("commit duration includes mutation admission/serialization and commit; handoff is commit-return through real coordinator return. Revision.LockDuration separately measures parent/revision-head acquisition SQL elapsed, not pure server-side lock wait or all prior mutation-admission wait. Pending commit calls are NOT parallel writes; no lock-removal or speedup assertion.")
}

func proveSupplementalContention(t *testing.T, backend string, intents, cardinality int, sameRun bool) supplementalContentionResult {
	t.Helper()
	capacity := 1
	if backend == "postgres" {
		capacity = 4
	}
	p := newSupplementalServingProbe(capacity)
	f := newSupplementalServingFixture(t, backend, p)
	transactions := storetest.CollectTransactions(t, f.selected, storetest.TransactionProbeOptions{})
	defer func() {
		// Preserve real owner receipts even when an existing execution assertion fails.
		t.Logf("M17 transactions post-fixture/pre-root-run through test return (implicit autocommit SQL and teardown excluded; revision SQL counts cover finalizers only): %+v", transactions.Snapshot())
	}()
	runs := make([]string, intents)
	for i := range runs {
		if i == 0 || !sameRun {
			_, runs[i] = f.startRun(t, 0)
		} else {
			runs[i] = runs[0]
		}
	}
	// A real completed empty selector read gates only fixture ingress setup.
	// This is not a second queue or an alternate selection/claim authority.
	emptyGate := make(chan struct{})
	releaseEmpty := sync.OnceFunc(func() { close(emptyGate) })
	defer releaseEmpty()
	installSupplementalScanObserver(t, f, p, emptyGate)
	// The test deliberately withholds selection while creating the full backlog.
	// Occupy the real capacity during that setup, rather than falsely exposing a
	// free serving opportunity to D3. These permits acquire no durable claims.
	setupPermits := make([]*startupownership.FanOutTurnPermit, 0, capacity)
	releaseSetup := sync.OnceFunc(func() {
		for _, permit := range setupPermits {
			permit.Done()
		}
	})
	defer releaseSetup()
	for i := 0; i < capacity; i++ {
		permit, available, err := f.runtimes[0].fanOutServing.BeginTurn(context.Background())
		if err != nil || !available {
			t.Fatalf("reserve real setup capacity %d/%d: available=%t err=%v", i+1, capacity, available, err)
		}
		setupPermits = append(setupPermits, permit)
	}
	f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
	started := time.Now()
	for i, run := range runs {
		submitSupplementalIntent(t, f, run, i, cardinality)
	}
	waitServingMatrixIntentCount(t, f, intents)
	releaseSetup()
	releaseEmpty()
	f.runtimes[0].fanOutServing.Wake()
	loadedKeys := waitSupplementalPhases(t, p, "loaded", capacity)
	loadedRuns := make(map[string]bool)
	for key := range loadedKeys {
		loadedRuns[key.RunID] = true
	}
	if sameRun && len(loadedRuns) != 1 || !sameRun && len(loadedRuns) != capacity {
		t.Fatalf("real loaded overlap run scope=%v sameRun=%t", loadedRuns, sameRun)
	}
	stats := p.snapshot()
	if stats.Err != nil || stats.Loaded != capacity || stats.PeakLoaded != capacity {
		t.Fatalf("actual loaded phases=%+v capacity=%d", stats, capacity)
	}
	var claimed, cursor, outcomes int
	if err := f.db.QueryRow(`SELECT SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),SUM(cursor) FROM fan_out_intents`).Scan(&claimed, &cursor); err != nil {
		t.Fatal(err)
	}
	if err := f.db.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes`).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if claimed != capacity || cursor != 0 || outcomes != 0 {
		t.Fatalf("capacity before writes: claims=%d cursor=%d outcomes=%d", claimed, cursor, outcomes)
	}
	p.releaseLoaded()
	ready := waitSupplementalPhases(t, p, "commit_ready", capacity)
	for key := range ready {
		if !loadedKeys[key] {
			t.Fatalf("commit barrier changed exact intent: %+v", key)
		}
	}
	// The first admitted evaluators now enter actual store commits together.
	// PostgreSQL's canonical same-run revision writer is deliberately untouched.
	released := time.Now()
	p.releaseCommits()
	seen := make(map[fanoutobligation.IntentKey]bool, intents)
	var lastCommit, lastHandoff time.Time
	for i := 0; i < intents; i++ {
		r := waitSupplementalReceipt(t, p, cardinality)
		if seen[r.Key] {
			t.Fatalf("duplicate exact completed intent: %+v", r.Key)
		}
		seen[r.Key] = true
		if r.CommitReturned.After(lastCommit) {
			lastCommit = r.CommitReturned
		}
		if r.FinishedAt.After(lastHandoff) {
			lastHandoff = r.FinishedAt
		}
		t.Logf("intent=%s claim=%s claim_to_loaded=%s planning_excluding_holds=%s commit_call=%s post_commit_handoff=%s explicit_hold=%s",
			r.Key.String(), r.ClaimDuration, r.LoadedAt.Sub(r.ClaimAt), r.CommitAt.Sub(r.LoadedAt)-r.Hold, r.CommitReturned.Sub(r.CommitAt), r.FinishedAt.Sub(r.CommitReturned), r.Hold)
	}
	if sameRun {
		assertSupplementalEffects(t, f, runs[0], 0, intents, cardinality)
	} else {
		for i, run := range runs {
			assertSupplementalEffects(t, f, run, i, 1, cardinality)
		}
	}
	stats = p.snapshot()
	if stats.Err != nil || stats.Turns != intents || stats.Finished != intents || stats.Loaded != 0 || stats.PeakLoaded != capacity || stats.PendingCommits != 0 || stats.PeakPendingCommits > capacity || stats.Commit.Count != intents {
		t.Fatalf("supplemental exact phase accounting: %+v", stats)
	}
	if backend == "postgres" && stats.PeakPendingCommits < 2 {
		t.Fatalf("no actual overlapping commit calls; contention unproven: %+v", stats)
	}
	if backend == "sqlite" && stats.PeakPendingCommits != 1 {
		t.Fatalf("SQLite exceeded single real serving commit: %+v", stats)
	}
	snapshot := transactions.Snapshot()
	chunk := snapshot.ByOperation[storetest.TransactionFanOutChunk]
	revision := chunk.Revision
	if chunk.WriteCommits != uint64(intents) || revision.Finalizations != uint64(intents) || revision.LockPhases != uint64(intents) || revision.LockDuration <= 0 || revision.Duration < revision.LockDuration || revision.ExecCalls == 0 || revision.QueryCalls == 0 || revision.QueryRowCalls == 0 {
		t.Fatalf("missing separate actual chunk transaction/revision-lock phases: %+v", chunk)
	}
	return supplementalContentionResult{
		Stats: stats, IngressToCommitReturn: lastCommit.Sub(started), ReleasedToCommitReturn: lastCommit.Sub(released), ReleasedToHandoff: lastHandoff.Sub(released),
		ReleasedRowsPerSecond: float64(intents*cardinality) / lastCommit.Sub(released).Seconds(),
		ChunkTransactions:     chunk, WorkloadTransactions: snapshot.Total,
	}
}

func waitSupplementalPhases(t *testing.T, p *supplementalServingProbe, name string, count int) map[fanoutobligation.IntentKey]bool {
	t.Helper()
	keys := make(map[fanoutobligation.IntentKey]bool, count)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for len(keys) < count {
		select {
		case phase := <-p.phases:
			if phase.Name != name || keys[phase.Key] {
				t.Fatalf("unexpected/duplicate held phase: %+v want=%s", phase, name)
			}
			keys[phase.Key] = true
		case r := <-p.completed:
			t.Fatalf("turn exited before required %s overlap: %+v", name, r)
		case <-deadline.C:
			t.Fatalf("real %s overlap never reached%d: %+v", name, count, p.snapshot())
		}
	}
	return keys
}

func installSupplementalScanObserver(t *testing.T, f *servingMatrixFixture, p *supplementalServingProbe, emptyGate <-chan struct{}) {
	t.Helper()
	var once sync.Once
	f.runtimes[0].fanOutServing.SetTestScanObserver(func(c startupownership.FanOutCandidate, found bool, err error) {
		p.scan(c, found, err)
		if emptyGate != nil && !found && err == nil {
			once.Do(func() { <-emptyGate })
		}
	})
	t.Cleanup(func() { f.runtimes[0].fanOutServing.SetTestScanObserver(nil) })
	f.runtimes[0].fanOutServing.Wake()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := p.snapshot()
		if s.Err != nil {
			t.Fatal(s.Err)
		}
		if s.EmptyScans > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("actual shared selector did not complete initial empty scan")
		}
		time.Sleep(time.Millisecond)
	}
}

func logSupplementalState(t *testing.T, f *servingMatrixFixture, p *supplementalServingProbe, label string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var intents, cursor, owed, claimed, retry, blocked int
	err := f.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cursor),0),COALESCE(SUM(cardinality-cursor),0),COALESCE(SUM(CASE WHEN claim_owner IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN retry_ready_at IS NOT NULL THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='blocked' THEN 1 ELSE 0 END),0) FROM fan_out_intents`).Scan(&intents, &cursor, &owed, &claimed, &retry, &blocked)
	if err != nil {
		t.Fatal(err)
	}
	s := p.snapshot()
	if s.Err != nil || retry != 0 || blocked != 0 {
		t.Fatalf("supplemental progress failure label=%s state=%+v retry=%d blocked=%d", label, s, retry, blocked)
	}
	t.Logf("%s observation=%s intents=%d cursor=%d owed=%d claimed=%d retry=%d blocked=%d scans=%d empty=%d found=%d last_empty=%s last_found=%s last_claim=%s last_commit=%s dropped_wakes=%d",
		label, time.Now().UTC().Format(time.RFC3339Nano), intents, cursor, owed, claimed, retry, blocked, s.Scans, s.EmptyScans, s.FoundScans, s.LastEmpty.Format(time.RFC3339Nano), s.LastFound.Format(time.RFC3339Nano), s.LastClaim.Format(time.RFC3339Nano), s.LastCommit.Format(time.RFC3339Nano), s.DroppedWakes)
}

func requireSupplementalWake(t *testing.T, p *supplementalServingProbe, count int) time.Time {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s := p.snapshot()
		if s.Err != nil {
			t.Fatal(s.Err)
		}
		if s.DroppedWakes == count {
			return s.LastWake
		}
		if s.DroppedWakes > count || time.Now().After(deadline) {
			t.Fatal(fmt.Sprintf("missing/excess post-commit wake: %+v want=%d", s, count))
		}
		time.Sleep(time.Millisecond)
	}
}
