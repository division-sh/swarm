package conformance

import (
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

// A finite suffix control complements, but does not replace, the staged soak.
// Both ten-row intents exist before the shared selector resumes; neither
// post-commit ingress notification is forwarded to the serving registration.
func TestIssue2394TwoTenLostWakeSuffixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			p := newSupplementalServingProbe(0)
			f := newSupplementalServingFixture(t, backend, p)
			_, run := f.startRun(t, 0)
			emptyGate := make(chan struct{})
			releaseEmpty := sync.OnceFunc(func() { close(emptyGate) })
			defer releaseEmpty()
			installSupplementalScanObserver(t, f, p, emptyGate)
			f.runtimes[0].pipeline.InstallFanOutWorkNotifier(p)
			submitSupplementalIntent(t, f, run, 0, 10)
			submitSupplementalIntent(t, f, run, 1, 10)
			waitServingMatrixIntentCount(t, f, 2)
			lastSuppressedWake := requireSupplementalWake(t, p, 2)
			var cardinality, cursor, outcomes int
			if err := f.db.QueryRow(`SELECT SUM(cardinality),SUM(cursor) FROM fan_out_intents`).Scan(&cardinality, &cursor); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes`).Scan(&outcomes); err != nil {
				t.Fatal(err)
			}
			if cardinality != 20 || cursor != 0 || outcomes != 0 {
				t.Fatalf("two-ten durable backlog before selection: cardinality=%d cursor=%d outcomes=%d", cardinality, cursor, outcomes)
			}
			// No new Wake call here: production recovery and completed-turn refill
			// alone must select, claim, evaluate, commit and hand off both intents.
			releaseEmpty()
			seen := make(map[fanoutobligation.IntentKey]bool, 2)
			var first supplementalReceipt
			for i := 0; i < 2; i++ {
				r := waitSupplementalReceipt(t, p, 10)
				if r.Key.RunID != run || seen[r.Key] {
					t.Fatalf("two-ten suffix lost an exact intent: key=%+v duplicate=%t", r.Key, seen[r.Key])
				}
				seen[r.Key] = true
				if r.ClaimAt.Sub(r.Discovery) > time.Second {
					t.Fatalf("selected two-ten intent missed actual claim entry: %+v", r)
				}
				if i == 0 {
					first = r
				} else if backend == "sqlite" {
					// SQLite's second opportunity begins only after the first real
					// finite caller finishes, not while that permit is occupied.
					if r.ClaimAt.Before(first.FinishedAt) || r.ClaimAt.Sub(first.FinishedAt) > time.Second {
						t.Fatalf("SQLite suffix did not follow the released shared permit: first=%+v suffix=%+v", first, r)
					}
				}
				t.Logf("two-ten exact intent=%s last_suppressed_ingress_wake=%s discovery=%s claim=%s commit_return=%s handoff_done=%s", r.Key.String(), lastSuppressedWake.Format(time.RFC3339Nano), r.Discovery.Format(time.RFC3339Nano), r.ClaimAt.Format(time.RFC3339Nano), r.CommitReturned.Format(time.RFC3339Nano), r.FinishedAt.Format(time.RFC3339Nano))
			}
			assertSupplementalEffects(t, f, run, 0, 2, 10)
			s := p.snapshot()
			if s.Err != nil || s.Turns != 2 || s.Finished != 2 || s.DroppedWakes != 2 || s.FoundScans != 2 || s.Commit.Count != 2 || s.Loaded != 0 || s.PendingCommits != 0 {
				t.Fatalf("two-ten exact completed-turn accounting: %+v", s)
			}
			logSupplementalState(t, f, p, "two_ten_final_suffix_settled")
			t.Log("M32 finite two-ten control: both real closed turns refilled, all20 exact effects and both0->10 durable histories preserved; not historical-cause or soak-duration proof")
		})
	}
}
