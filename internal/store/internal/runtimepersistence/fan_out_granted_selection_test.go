package runtimepersistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type fanOutSelectionExecutor struct {
	bundle    string
	started   chan capturedFanOutTurn
	completed chan fanoutobligation.Intent
	errors    chan error
	hold      <-chan struct{}
}

func (e *fanOutSelectionExecutor) ReportFanOutServingError(_ context.Context, err error) {
	select {
	case e.errors <- err:
	default:
	}
}

func (e *fanOutSelectionExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	e.started <- capturedFanOutTurn{owner: owner, key: key}
	if e.hold != nil {
		select {
		case <-e.hold:
		case <-ctx.Done():
			return pipeline.FanOutTurnResult{}, nil
		}
	}
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, e.bundle))
	intent, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "granted-selection", BundleHash: e.bundle, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil {
		return pipeline.FanOutTurnResult{}, err
	}
	if !found {
		return pipeline.FanOutTurnResult{}, fmt.Errorf("selected exact intent disappeared: %+v", key)
	}
	input, err := owner.LoadFanOutEvaluation(ctx, claim)
	if err != nil {
		return pipeline.FanOutTurnResult{}, err
	}
	if len(input.Items) != intent.ChunkEndOrdinal()-intent.Cursor {
		return pipeline.FanOutTurnResult{}, fmt.Errorf("selected source range mismatch")
	}
	committed, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), time.Now().UTC()))
	if err != nil {
		return pipeline.FanOutTurnResult{}, err
	}
	e.completed <- committed.Intent
	return pipeline.FanOutTurnResult{Refill: true}, committed.PostCommitFailure
}

func TestFanOutGrantedCompleteSetAndGlobalFairnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			old := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 33, time.Now().UTC())
			newer := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 1, time.Now().UTC(), storeTestSourceArtifact("fan-out-second-granted-source"))
			process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{old, newer})
			started := make(chan capturedFanOutTurn, 8)
			completed := make(chan fanoutobligation.Intent, 8)
			errors := make(chan error, 8)
			workers := 1
			first := &fanOutSelectionExecutor{bundle: old.bundleHash, started: started, completed: completed, errors: errors}
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, first)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			// Admit-before-register is a legal startup window. Observe one
			// actual scan: it must wait without errors or subset dispatch.
			scanned := make(chan struct{}, 1)
			registration.SetTestScanObserver(func(_ startupownership.FanOutCandidate, found bool, err error) {
				if found || err != nil {
					return // The executor/error arms below report any violation.
				}
				select {
				case scanned <- struct{}{}:
				default:
				}
			})
			registration.Wake()
			select {
			case <-scanned:
			case err := <-errors:
				t.Fatalf("legal registration window reported an invariant error: %v", err)
			case turn := <-started:
				t.Fatalf("incomplete set served a candidate: %+v", turn.key)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			pendingPage := fanoutobligation.ListPage{RunID: old.runID, Intents: []fanoutobligation.IntentReadback{{
				Key: fanoutobligation.IntentKey{RunID: old.runID, TriggeringDeliveryID: old.deliveryID,
					ElementRef: contracts.FanOutElementRef{FlowPath: old.flowPath, Family: "fan_out", SemanticPath: old.semanticPath}},
				BundleHash: old.bundleHash,
			}}}
			pendingRead, err := startupownership.ObserveFanOutRuntimePage(ctx, process, pendingPage)
			if err != nil {
				t.Fatal(err)
			}
			if runtime := pendingRead.Intents[0].Runtime; runtime.Reason != "registration_pending" || runtime.Availability != "unavailable" || runtime.Eligible != nil {
				t.Fatalf("pending registration gained eligibility: %+v", runtime)
			}
			second := &fanOutSelectionExecutor{bundle: newer.bundleHash, started: started, completed: completed, errors: errors}
			registration2, err := startupownership.StartFanOutServing(ctx, grants[1], occurrences[1], &workers, second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration2.Close)
			for index, want := range []struct {
				run    string
				cursor int
			}{{old.runID, 32}, {newer.runID, 1}, {old.runID, 33}} {
				select {
				case got := <-completed:
					if got.Request.Key.RunID != want.run || got.Cursor != want.cursor {
						t.Fatalf("global fairness turn %d: got=%s/%d want=%s/%d", index, got.Request.Key.RunID, got.Cursor, want.run, want.cursor)
					}
				case err := <-errors:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, old, 33, 33)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, newer, 1, 1)
		})
	}
}

func TestFanOutGrantedExactExclusionsDoNotStrandSameRunCapacityPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
	defer cancel()
	selected, _, db, postgres := newFanOutOwnerPairForTest(t, "postgres")
	first := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, time.Now().UTC())
	second := seedFanOutOwnerIntent(t, ctx, db, first, 1, time.Now().UTC())
	_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{first})
	started := make(chan capturedFanOutTurn, 8)
	completed := make(chan fanoutobligation.Intent, 8)
	errors := make(chan error, 8)
	hold := make(chan struct{})
	executor := &fanOutSelectionExecutor{bundle: first.bundleHash, started: started, completed: completed, errors: errors, hold: hold}
	workers := 2
	registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	var keys []fanoutobligation.IntentKey
	for range 2 {
		select {
		case turn := <-started:
			keys = append(keys, turn.key)
		case err := <-errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("same-run work stranded behind a reserved candidate: ", ctx.Err())
		}
	}
	if keys[0] == keys[1] || keys[0].RunID != first.runID || keys[1].RunID != first.runID {
		t.Fatalf("reservation exclusions lost structured identity: %+v", keys)
	}
	close(hold)
	for range 2 {
		select {
		case <-completed:
		case err := <-errors:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	assertFanOutCursorAndOutcomeCount(t, ctx, db, first, 1, 1)
	assertFanOutCursorAndOutcomeCount(t, ctx, db, second, 1, 1)
}
