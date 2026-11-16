package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func waitFanOutReadPacingForTest(t *testing.T, ctx context.Context, intent fanoutobligation.Intent) {
	t.Helper()
	if intent.ReadRetry == nil {
		t.Fatal("native claim did not retain its read admission floor")
	}
	timer := time.NewTimer(max(0, time.Until(intent.ReadRetry.NotBefore.Add(time.Millisecond))))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestFanOutReadPacingSurvivesClaimReleaseAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			ctx = originalStoreFixtureContext(t, ctx, owner)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 3, time.Now().UTC())
			request := pipeline.FanOutClaimRequest{Owner: "read-attempt", BundleHash: fixture.bundleHash, Now: time.Now().UTC(), Lease: time.Minute}
			intent, claim, found, err := owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || intent.ReadRetry == nil || intent.ReadRetry.Since != nil {
				t.Fatalf("admitted floor=%+v found=%v err=%v", intent.ReadRetry, found, err)
			}
			first := *intent.ReadRetry
			if settlement, err := owner.ReleaseFanOutClaim(ctx, claim); err != nil || !settlement.Acknowledged {
				t.Fatalf("release=%+v err=%v", settlement, err)
			}
			if _, _, found, err := restarted.ClaimFanOutIntent(ctx, request); err != nil || found {
				t.Fatalf("restart bypassed floor: found=%v err=%v", found, err)
			}
			time.Sleep(max(0, time.Until(first.NotBefore.Add(time.Millisecond))))
			intent, claim, found, err = restarted.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || intent.ReadRetry.Delay <= first.Delay {
				t.Fatalf("reconstructed attempt=%+v found=%v err=%v", intent.ReadRetry, found, err)
			}
			if settlement, err := restarted.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest(), ReadEvaluation: true}); err != nil || !settlement.Acknowledged {
				t.Fatalf("retry=%+v err=%v", settlement, err)
			}
			reader := owner.(interface {
				ListFanOutIntents(context.Context, fanoutobligation.ListQuery) (fanoutobligation.ListPage, error)
			})
			page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: fixture.runID})
			if err != nil || len(page.Intents) != 1 || page.Intents[0].ReadWait == nil || page.Intents[0].ReadWait.Since == nil || page.Intents[0].ReadWait.Failure == nil || page.Intents[0].Retry != nil {
				t.Fatalf("read evidence=%+v err=%v", page, err)
			}
			summary, err := owner.FanOutRunSummary(ctx, fixture.runID, time.Now().UTC())
			if err != nil || summary.ReadWaiting != 1 || summary.OldestReadWaitMS == nil || summary.Blocked != 0 || summary.Owed != 3 {
				t.Fatalf("summary=%+v err=%v", summary, err)
			}
			time.Sleep(max(0, time.Until(page.Intents[0].ReadWait.NotBefore.Add(time.Millisecond))))
			intent, claim, found, err = owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found || intent.ReadRetry.Since == nil || !intent.ReadRetry.Since.Equal(*page.Intents[0].ReadWait.Since) {
				t.Fatalf("claim reset failure age=%+v found=%v err=%v", intent.ReadRetry, found, err)
			}
			committed, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC()))
			if err != nil || committed.Intent.ReadRetry != nil {
				t.Fatalf("confirmed progress retained read episode: %+v err=%v", committed, err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
			// A confirmed read failure waits from its native observation, not
			// from the earlier claim admission which reserved the crash floor.
			_, claim, found, err = owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found {
				t.Fatalf("fresh first-read attempt: found=%v err=%v", found, err)
			}
			time.Sleep(100 * time.Millisecond)
			if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest(), ReadEvaluation: true}); err != nil {
				t.Fatal(err)
			}
			page, err = reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: fixture.runID})
			if err != nil || len(page.Intents) != 1 || page.Intents[0].ReadWait == nil || page.Intents[0].ReadWait.Since == nil {
				t.Fatalf("first read failure evidence=%+v err=%v", page, err)
			}
			wait := page.Intents[0].ReadWait
			if wait.NotBefore.Sub(*wait.Since) < fanoutobligation.InitialReadRetryDelay {
				t.Fatalf("first read retry waits %s from its observed failure, want at least %s", wait.NotBefore.Sub(*wait.Since), fanoutobligation.InitialReadRetryDelay)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
		})
	}
}

func TestFanOutOrdinaryResumeUsesExactNativeOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 30*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			ctx = originalStoreFixtureContext(t, ctx, selected)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 3, time.Now().UTC())
			_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			var turn capturedFanOutTurn
			select {
			case turn = <-executor.turns:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			_, claim, found, err := turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "pause-handler", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			if _, err := turn.owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			_, claim, found, err = turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "pause-handler", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("second claim found=%v err=%v", found, err)
			}
			if settlement, err := turn.owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil || !settlement.Acknowledged {
				t.Fatalf("block=%+v err=%v", settlement, err)
			}
			registration.Close()
			executor = &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
			registration, err = startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			resumed, err := registration.ResumeRunFanOut(ctx, fixture.runID)
			if err != nil || resumed.Observed != 1 || resumed.Reopened != 1 || resumed.Remaining != 0 {
				t.Fatalf("resume=%+v err=%v", resumed, err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
			resumed, err = registration.ResumeRunFanOut(ctx, fixture.runID)
			if err != nil || resumed.Observed != 0 {
				t.Fatalf("empty recheck=%+v err=%v", resumed, err)
			}
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID, ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			if turn.key != key {
				t.Fatal("resume changed exact handler identity")
			}
			_, claim, found, err = turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "hostile-prefix", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("hostile prefix claim found=%v err=%v", found, err)
			}
			if _, err := turn.owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `DELETE FROM fan_out_outcomes WHERE run_id=$1`, fixture.runID); err != nil {
				t.Fatal(err)
			}
			resumed, err = registration.ResumeRunFanOut(ctx, fixture.runID)
			if err == nil || resumed.Reopened != 0 || resumed.Remaining != 1 {
				t.Fatalf("missing prefix reopened or disappeared: %+v err=%v", resumed, err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 0)
			if err := grants[0].Retire(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := registration.ResumeRunFanOut(ctx, fixture.runID); err == nil {
				t.Fatal("empty inventory manufactured retired authority")
			}
		})
	}
}
