package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFanOutRetryWaitYieldsPreservesRestartAndCancelsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			old := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 64, time.Now().UTC().Add(-time.Minute))
			healthy := seedFanOutOwnerIntent(t, ctx, db, old, 1, time.Now().UTC().Add(-time.Second))
			request := pipeline.FanOutClaimRequest{Owner: "retry-test", BundleHash: old.bundleHash, Now: time.Now().UTC(), Lease: time.Minute}
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || claim.Key.ElementRef.SemanticPath != old.semanticPath {
				t.Fatalf("first claim: %+v found=%v err=%v", claim, found, err)
			}
			before := time.Now().UTC()
			if err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: before.Add(-time.Hour), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			var rawDue, rawObserved any
			if err := db.QueryRowContext(ctx, `SELECT retry_ready_at,last_served_at FROM fan_out_intents WHERE run_id=$1 AND semantic_path=$2`, old.runID, old.semanticPath).Scan(&rawDue, &rawObserved); err != nil {
				t.Fatal(err)
			}
			due, ok, err := sqliteTimeValue(rawDue)
			if err != nil || !ok {
				t.Fatalf("retry due: %v %v %v", due, ok, err)
			}
			observed, ok, err := sqliteTimeValue(rawObserved)
			if err != nil || !ok || due.Sub(observed) != time.Second || observed.Before(before.Truncate(time.Microsecond)) {
				t.Fatalf("retry due must derive from selected-store observation: due=%v observation=%v err=%v", due, observed, err)
			}
			intent, next, found, err := restarted.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || next.Key.ElementRef.SemanticPath != healthy.semanticPath || intent.Retry != nil {
				t.Fatalf("retry wait did not yield to healthy work: intent=%+v found=%v err=%v", intent, found, err)
			}
			if _, err := restarted.CommitFanOutChunk(ctx, rejectedFanOutChunk(next, 0, 1, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			if _, _, found, err := restarted.ClaimFanOutIntent(ctx, request); err != nil || found {
				t.Fatalf("retry spun before due: found=%v err=%v", found, err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, old, 0, 0)
			if remaining := time.Until(due.Add(time.Millisecond)); remaining > 0 {
				time.Sleep(remaining)
			}
			intent, claim, found, err = restarted.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || intent.NextChunkSize != 16 || intent.Retry != nil || claim.Key.ElementRef.SemanticPath != old.semanticPath {
				t.Fatalf("due retry did not resume exact reduced range: intent=%+v found=%v err=%v", intent, found, err)
			}
			if err := restarted.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			if err := restarted.CancelRunFanOut(ctx, old.runID, "run_stopped", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			summary, err := restarted.FanOutRunSummary(ctx, old.runID, time.Now().UTC())
			if err != nil || summary.Canceled != 64 || summary.Owed != 0 || summary.Committed != 0 || summary.SemanticRejected != 1 {
				t.Fatalf("retry-wait cancellation summary: %+v err=%v", summary, err)
			}
			var status string
			if err := db.QueryRowContext(ctx, `SELECT status,retry_ready_at FROM fan_out_intents WHERE run_id=$1 AND semantic_path=$2`, old.runID, old.semanticPath).Scan(&status, &rawDue); err != nil || status != string(fanoutobligation.StatusCanceled) || rawDue != nil {
				t.Fatalf("cancellation retained retry readiness: status=%s due=%v err=%v", status, rawDue, err)
			}
		})
	}
}
