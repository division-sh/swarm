package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

func TestFanOutGrantedActualCommitCountBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
			defer cancel()
			selected, sibling, db, postgres := newFanOutOwnerPairForTest(t, backend)
			if postgres {
				independent := newPostgresStoreWithBackend(mustPostgresBackend(db))
				independent.acceptCurrentSchemaForTest()
				sibling = independent
			}
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 4, time.Now().UTC())
			owner, _, grant, _ := grantedFanOutOwnerForTest(t, ctx, selected, fixture)
			ctx = testAuthorActivityContextForBundle(fixture.bundleHash)
			const delay = 30 * time.Millisecond
			collector, restore, err := InstallTransactionProbeForTest(selected, transactiontest.Options{Delay: delay, DelayScope: transactiontest.DelayServingWrites})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restore)
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID,
				ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			started := time.Now()
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "physical-commit-proof", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			input, err := owner.LoadFanOutEvaluation(ctx, claim)
			if err != nil || len(input.Items) != 4 {
				t.Fatalf("load count=%d err=%v", len(input.Items), err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			snapshot := collector.Snapshot()
			if snapshot.Active != 0 || snapshot.Total.WriteCommits != 2 || snapshot.Total.ReadCommits != 1 || snapshot.Total.Failed != 0 {
				t.Fatalf("clean serving must physically commit two writes and one read: %+v", snapshot)
			}
			if snapshot.ByOperation[transactiontest.FanOutClaim].WriteCommits != 1 || snapshot.ByOperation[transactiontest.FanOutChunk].WriteCommits != 1 || snapshot.ByOperation[transactiontest.FanOutLoad].ReadCommits != 1 {
				t.Fatalf("actual transaction labels disagree: %+v", snapshot.ByOperation)
			}
			if snapshot.Total.DelayedCommits != 2 || snapshot.Total.InjectedDelay != 2*delay || elapsed < 2*delay {
				t.Fatalf("delay did not apply to exact physical serving commits: elapsed=%v receipt=%+v", elapsed, snapshot)
			}
			claimReceipt := snapshot.ByOperation[transactiontest.FanOutClaim]
			loadReceipt := snapshot.ByOperation[transactiontest.FanOutLoad]
			chunkReceipt := snapshot.ByOperation[transactiontest.FanOutChunk]
			if claimReceipt.FirstCommitAt.Before(started) || loadReceipt.FirstCommitAt.Before(claimReceipt.LastCommitAt) || chunkReceipt.FirstCommitAt.Before(loadReceipt.LastCommitAt) || chunkReceipt.LastCommitAt.After(time.Now()) || !chunkReceipt.FirstCommitAt.Equal(chunkReceipt.LastCommitAt) {
				t.Fatalf("physical acknowledgement times are missing or out of order: %+v", snapshot.ByOperation)
			}
			revision := chunkReceipt.Revision
			if revision.Finalizations != 1 || revision.LockPhases != 1 || revision.Duration <= 0 || revision.LockDuration <= 0 || revision.Duration < revision.LockDuration || revision.ExecCalls == 0 || revision.QueryCalls == 0 || revision.QueryRowCalls == 0 {
				t.Fatalf("chunk revision SQL/elapsed receipt missing: %+v", revision)
			}
			if claimReceipt.Revision.Finalizations != 0 || loadReceipt.Revision.Finalizations != 0 || snapshot.Total.Revision != revision {
				t.Fatalf("revision receipt leaked into non-finalizing work: %+v", snapshot)
			}
			if _, err := owner.LoadFanOutEvaluation(ctx, claim); err == nil {
				t.Fatal("closed claim unexpectedly loaded")
			}
			failed := collector.Snapshot()
			if failed.Total.Failed != 1 || failed.Total.RollbackAttempts != 1 || failed.Total.CommitFailures != 0 || failed.Total.WriteCommits != 2 || failed.Total.ReadCommits != 1 {
				t.Fatalf("failed read was counted as a successful commit: %+v", failed)
			}
			if _, err := sibling.FanOutRunSummary(ctx, fixture.runID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if after := collector.Snapshot(); after.Total != failed.Total {
				t.Fatal("probe leaked to another backend over the same database")
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			retired := collector.Snapshot()
			if retired.Total.WriteCommits != failed.Total.WriteCommits+1 {
				t.Fatalf("retirement commit missing or double counted: %+v", retired)
			}
			if postgres && retired.Retained.WriteCommits != 1 {
				t.Fatalf("retained PostgreSQL commit missing: %+v", retired)
			}
			if postgres && (retired.Retained.FirstCommitAt.IsZero() || retired.Retained.LastCommitAt.Before(chunkReceipt.LastCommitAt)) {
				t.Fatalf("retained acknowledgement timestamp missing: %+v", retired.Retained)
			}
			restore()
			restore()
			if _, err := selected.FanOutRunSummary(ctx, fixture.runID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if after := collector.Snapshot(); after.Total != retired.Total {
				t.Fatal("probe remained installed after cleanup")
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 4, 4)
		})
	}
}
