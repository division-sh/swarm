package runtimepersistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestFanOutRetrySQLPolicyAndRestartRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			at := time.Now().UTC().Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 33, at)
			// The SQL operation must implement exactly the shared retry policy
			// for every valid persisted budget, including odd and floor values.
			for budget := 32; budget >= 1; budget-- {
				if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET next_chunk_size=$1 WHERE run_id=$2`, budget, fixture.runID); err != nil {
					t.Fatal(err)
				}
				at = at.Add(time.Second)
				intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "retry", BundleHash: fixture.bundleHash, Now: at, Lease: time.Minute})
				if err != nil || !found || intent.NextChunkSize != budget {
					t.Fatalf("claim budget%d: %+v found=%v err=%v", budget, intent, found, err)
				}
				if err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: at, ObservedDuration: 0}); err != nil {
					t.Fatal(err)
				}
				var next int
				if err := db.QueryRowContext(ctx, `SELECT next_chunk_size FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&next); err != nil {
					t.Fatal(err)
				}
				if next != fanoutobligation.RetryChunkSize(budget) {
					t.Fatalf("SQL retry%d = %d, owner wants%d", budget, next, fanoutobligation.RetryChunkSize(budget))
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
			}
			at = at.Add(time.Second)
			intent, claim, found, err := restarted.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "restarted", BundleHash: fixture.bundleHash, Now: at, Lease: time.Minute})
			if err != nil || !found || intent.NextChunkSize != 1 {
				t.Fatalf("restart must retain valid floor tuning: %+v found=%v err=%v", intent, found, err)
			}
			input, err := restarted.LoadFanOutEvaluation(ctx, claim)
			if err != nil || len(input.Items) != 1 || input.StartOrdinal != 0 {
				t.Fatalf("restart range = %+v err=%v", input, err)
			}
			committed, err := restarted.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, at))
			if err != nil || committed.PostCommitFailure != nil || committed.Intent.NextChunkSize != 32 {
				t.Fatalf("successful reduced chunk must restore cap: %+v err=%v", committed, err)
			}
			intent, claim, found, err = restarted.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "recovered", BundleHash: fixture.bundleHash, Now: at.Add(time.Second), Lease: time.Minute})
			if err != nil || !found || intent.NextChunkSize != 32 || intent.Cursor != 1 {
				t.Fatalf("recovery claim: %+v found=%v err=%v", intent, found, err)
			}
			input, err = restarted.LoadFanOutEvaluation(ctx, claim)
			if err != nil || len(input.Items) != 32 || input.StartOrdinal != 1 {
				t.Fatalf("recovered range = %+v err=%v", input, err)
			}
			if _, err := restarted.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 1, 32, at.Add(time.Second))); err != nil {
				t.Fatal(err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 33, 33)
		})
	}
}

func TestFanOutForkResetsOnlyOperationalBudgetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			owner, _, db, _ := newFanOutOwnerPairForTest(t, backend)
			for _, prefix := range []int{0, 1, 3} {
				t.Run(fmt.Sprintf("prefix%d", prefix), func(t *testing.T) {
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					ctx, fixture := seedDeclaredNumericForkFanOutFixture(t, backend, authorActivityReceiptFixture{db: db, store: owner.(authorActivityReceiptStore)}, 3, at, false)
					if prefix > 0 {
						_, claim, found, err := claimFanOutForRun(t, ctx, owner, fixture.runID, fixture.bundleHash, at.Add(time.Second))
						if err != nil || !found {
							t.Fatalf("claim source found=%v err=%v", found, err)
						}
						if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, prefix, at.Add(time.Second))); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET next_chunk_size=1 WHERE run_id=$1`, fixture.runID); err != nil {
						t.Fatal(err)
					}
					postgres := backend == "postgres"
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, postgres)
					point := historicalLineageCheckpoint(t, db, fixture.runID, postgres, at.Add(2*time.Second))
					forkOwner := owner.(interface {
						MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
					})
					request := runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: point, OriginalLoopCarriage: originalCarriageForRun(t, owner, fixture.runID)}
					child, err := forkOwner.MaterializeRunFork(ctx, request)
					if err != nil {
						t.Fatal(err)
					}
					var budget, cursor, outcomes, sourceBudget int
					if err := db.QueryRowContext(ctx, `SELECT next_chunk_size,cursor,(SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1) FROM fan_out_intents WHERE run_id=$1`, child.ForkRunID).Scan(&budget, &cursor, &outcomes); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRowContext(ctx, `SELECT next_chunk_size FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&sourceBudget); err != nil {
						t.Fatal(err)
					}
					if budget != 32 || cursor != prefix || outcomes != prefix || sourceBudget != 1 {
						t.Fatalf("child budget/cursor/outcomes=%d/%d/%d source budget=%d", budget, cursor, outcomes, sourceBudget)
					}
				})
			}
		})
	}
}
