package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
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
				// Isolate sizing from the separately proven one-second readiness gate.
				if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET next_chunk_size=$1,retry_ready_at=NULL,retry_failure=NULL WHERE run_id=$2`, budget, fixture.runID); err != nil {
					t.Fatal(err)
				}
				at = at.Add(time.Second)
				intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "retry", BundleHash: fixture.bundleHash, Now: at, Lease: time.Minute})
				if err != nil || !found || intent.NextChunkSize != budget {
					t.Fatalf("claim budget%d: %+v found=%v err=%v", budget, intent, found, err)
				}
				if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: at, ObservedDuration: 0, Failure: fanOutRetryFailureForTest()}); err != nil {
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
			reader := restarted.(interface {
				ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
			})
			query := fanoutobligation.ListQuery{RunID: fixture.runID}
			beforeRestart, err := owner.(interface {
				ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
			}).ListFanOutIntents(ctx, query)
			if err != nil || len(beforeRestart.Intents) != 1 || beforeRestart.Intents[0].Retry == nil {
				t.Fatalf("persisted retry before owner reconstruction: %+v err=%v", beforeRestart, err)
			}
			// Restart does not waive the persisted retry wait.
			if _, _, found, err := restarted.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "restarted-early", BundleHash: fixture.bundleHash, Now: at, Lease: time.Minute}); err != nil || found {
				t.Fatalf("restart bypassed retry due time: found=%v err=%v", found, err)
			}
			afterRestart, err := reader.ListFanOutIntents(ctx, query)
			if err != nil || len(afterRestart.Intents) != 1 {
				t.Fatalf("reconstructed retry read: %+v err=%v", afterRestart, err)
			}
			beforeRow, afterRow := beforeRestart.Intents[0], afterRestart.Intents[0]
			afterRow.DurableState = beforeRow.DurableState
			if !reflect.DeepEqual(beforeRow, afterRow) || afterRow.ClaimOwner != "" || afterRow.LeaseExpiresAt != nil || afterRow.LastServedAt == nil || !reflect.DeepEqual(afterRow.Runtime, fanoutobligation.UnavailableRuntimeReadback()) {
				t.Fatalf("restart changed persisted retry cause/due/fairness or fabricated metrics: before=%+v after=%+v", beforeRow, afterRow)
			}
			time.Sleep(time.Second)
			intent, claim, found, err := restarted.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "restarted", BundleHash: fixture.bundleHash, Now: at, Lease: time.Minute})
			if err != nil || !found || intent.NextChunkSize != 1 {
				t.Fatalf("restart must retain valid floor tuning: %+v found=%v err=%v", intent, found, err)
			}
			input, err := restarted.LoadFanOutEvaluation(ctx, claim)
			if err != nil || len(input.Items) != 1 || input.StartOrdinal != 0 {
				t.Fatalf("restart range = %+v err=%v", input, err)
			}
			committed, err := restarted.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, at))
			if err != nil || committed.PostCommitFailure != nil || committed.Intent.NextChunkSize != 32 || committed.Intent.Retry != nil {
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
			for _, prefix := range []int{0, 1, 3} {
				t.Run(fmt.Sprintf("prefix%d", prefix), func(t *testing.T) {
					owner, _, db, _ := newFanOutOwnerPairForTest(t, backend)
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
					// Populate operational evidence through its mutation owner, rather
					// than asserting that a fork clears fields already empty at source.
					if prefix < 3 {
						_, claim, found, err := claimFanOutForRun(t, ctx, owner, fixture.runID, fixture.bundleHash, time.Now().UTC())
						if err != nil || !found {
							t.Fatalf("operational source claim: found=%v err=%v", found, err)
						}
						if prefix == 0 {
							if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
								t.Fatal(err)
							}
						}
					}
					read := func(runID string) fanoutobligation.IntentReadback {
						t.Helper()
						reader := owner.(interface {
							ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
						})
						query := fanoutobligation.ListQuery{RunID: runID}
						page, err := reader.ListFanOutIntents(ctx, query)
						if err != nil || len(page.Intents) != 1 {
							t.Fatalf("read fork operational evidence: %+v err=%v", page, err)
						}
						if err := page.Validate(query); err != nil {
							t.Fatal(err)
						}
						return page.Intents[0]
					}
					sourceBefore := read(fixture.runID)
					if prefix == 0 && (sourceBefore.Retry == nil || !reflect.DeepEqual(sourceBefore.Retry.Failure, fanOutRetryFailureForTest())) {
						t.Fatalf("source lacks exact retry cause/due evidence: %+v", sourceBefore)
					}
					if prefix == 1 && (sourceBefore.ClaimOwner == "" || sourceBefore.LeaseExpiresAt == nil || sourceBefore.ClaimGeneration == 0) {
						t.Fatalf("source lacks live claim evidence: %+v", sourceBefore)
					}
					if sourceBefore.LastServedAt == nil {
						t.Fatalf("source lacks nonzero fairness evidence: %+v", sourceBefore)
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
					childRead := read(child.ForkRunID)
					if childRead.Retry != nil || childRead.ClaimOwner != "" || childRead.ClaimGeneration != 0 || childRead.LeaseExpiresAt != nil || childRead.LastServedAt != nil {
						t.Fatalf("child inherited operational state: %+v", childRead)
					}
					if !reflect.DeepEqual(childRead.Runtime, fanoutobligation.UnavailableRuntimeReadback()) {
						t.Fatalf("unregistered child fabricated runtime metrics: %+v", childRead.Runtime)
					}
					after := read(fixture.runID)
					// Eligibility can change when database time crosses the preserved
					// retry due time. Compare persisted facts, not that derived state.
					after.DurableState = sourceBefore.DurableState
					if !reflect.DeepEqual(after, sourceBefore) {
						t.Fatalf("fork changed source operational facts: before=%+v after=%+v", sourceBefore, after)
					}
					if prefix < 3 {
						activation, err := owner.(interface {
							ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
						}).ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID, AllowSourceFreeze: true})
						if err != nil || !activation.Activated || !activation.SourceFrozen {
							t.Fatalf("ordinary fork activation: %+v err=%v", activation, err)
						}
						intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "fork-reset-child", BundleHash: fixture.bundleHash, Candidate: &childRead.Key, Now: time.Now().UTC(), Lease: time.Minute})
						if err != nil || !found || intent.Cursor != prefix {
							t.Fatalf("child suffix claim: %+v found=%v err=%v", intent, found, err)
						}
						integer, integerOK := intent.Request.Capsule.StateFields["integer"].(json.Number)
						decimal, decimalOK := intent.Request.Capsule.StateFields["decimal"].(json.Number)
						if !integerOK || integer.String() != "75" || !decimalOK || decimal.String() != "75.0" {
							t.Fatalf("fork changed numeric source lexemes: %+v", intent.Request.Capsule.StateFields)
						}
						input, err := owner.LoadFanOutEvaluation(ctx, claim)
						if err != nil || input.StartOrdinal != prefix || len(input.Items) != 3-prefix || input.Trigger.RunID() != fixture.runID || input.Trigger.ID() != fixture.eventID {
							t.Fatalf("fork suffix lost ancestor trigger or replayed prefix: %+v err=%v", input, err)
						}
						if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, prefix, 3-prefix, time.Now().UTC())); err != nil {
							t.Fatal(err)
						}
					}
					if final := read(child.ForkRunID); final.Status != fanoutobligation.StatusClosed || final.Cursor != 3 {
						t.Fatalf("fork did not preserve/complete exact terminal prefix: %+v", final)
					}
					var total, distinct, first, last int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT ordinal),MIN(ordinal),MAX(ordinal) FROM fan_out_outcomes WHERE run_id=$1`, child.ForkRunID).Scan(&total, &distinct, &first, &last); err != nil || total != 3 || distinct != 3 || first != 0 || last != 2 {
						t.Fatalf("fork ordinal coverage=%d/%d/%d/%d err=%v", total, distinct, first, last, err)
					}
					assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, prefix, prefix)
				})
			}
		})
	}
}
