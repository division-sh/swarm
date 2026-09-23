package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestFanOutGrantedGlobalReadinessIsNotOwedSuffixBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 64, time.Now().UTC())
			if err := acknowledgePipelineEventFixture(ctx, selected, fixture.eventID); err != nil {
				t.Fatal(err)
			}
			obligations := selected.(interface {
				PipelineObligations() pipelineobligation.Store
			}).PipelineObligations()
			assertReady := func(want bool) {
				presence, err := obligations.GlobalWorkPresence(ctx)
				if err != nil || presence.ProcessingEligible != want {
					t.Fatalf("fan-out global eligibility=%v want=%v err=%v", presence.ProcessingEligible, want, err)
				}
				summary, err := selected.FanOutRunSummary(ctx, fixture.runID, time.Now().UTC())
				if err != nil || summary.Owed != 64 || !summary.BlocksCompletion() {
					t.Fatalf("readiness erased owed suffix: %+v err=%v", summary, err)
				}
			}
			assertReady(false) // A bundle alone is not execution authority.
			owner, _, grant, _ := grantedFanOutOwnerForTest(t, ctx, selected, fixture)
			assertReady(true)
			key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID,
				ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
			request := pipeline.FanOutClaimRequest{Owner: "readiness-proof", BundleHash: fixture.bundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute}
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, request)
			if err != nil || !found {
				t.Fatalf("claim: found=%v err=%v", found, err)
			}
			assertReady(false)
			if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			assertReady(false)
			var rawDue any
			if err := db.QueryRowContext(ctx, `SELECT retry_ready_at FROM fan_out_intents WHERE run_id=$1`, fixture.runID).Scan(&rawDue); err != nil {
				t.Fatal(err)
			}
			due, _, err := sqliteTimeValue(rawDue)
			if err != nil {
				t.Fatal(err)
			}
			if wait := time.Until(due.Add(time.Millisecond)); wait > 0 {
				time.Sleep(wait)
			}
			assertReady(true)
			control := selected.(interface {
				PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				ContinueRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
			})
			controlCtx := testAuthorActivityContextForBundle(fixture.bundleHash)
			transition := runcontrol.TransitionRequest{RunID: fixture.runID, Now: time.Now().UTC(), Reason: "readiness-proof", ControlledBy: "test"}
			if outcome, err := control.PauseRunControlOutcome(controlCtx, transition); err != nil || !outcome.Acknowledged {
				t.Fatalf("pause outcome=%+v err=%v", outcome, err)
			}
			assertReady(false)
			if _, _, found, err := owner.ClaimFanOutIntent(ctx, request); err != nil || found {
				t.Fatalf("paused new turn: found=%v err=%v", found, err)
			}
			if outcome, err := control.ContinueRunControlOutcome(controlCtx, transition); err != nil || !outcome.Acknowledged {
				t.Fatalf("continue outcome=%+v err=%v", outcome, err)
			}
			assertReady(true)
			// A materialized selected binding reserves the run before any
			// selected execution/grant exists. Ordinary serving must skip it.
			source := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 0, time.Now().UTC(), fixture.artifact)
			if err := acknowledgePipelineEventFixture(ctx, selected, source.eventID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_bindings (binding_id,fork_run_id,source_run_id,fork_event_id,mode,created_at) VALUES ($1,$2,$3,$4,'selected_contracts',$5)`, uuid.NewString(), fixture.runID, source.runID, source.eventID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			assertReady(false)
			if _, _, found, err := owner.ClaimFanOutIntent(ctx, request); err != nil || found {
				t.Fatalf("ordinary grant claimed selected reservation: found=%v err=%v", found, err)
			}
			if err := grant.Retire(ctx); err != nil {
				t.Fatal(err)
			}
			assertReady(false)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutGrantedClockTiesUseStructuredIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			at := time.Now().UTC().Truncate(time.Microsecond)
			first := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, at)
			second := seedFanOutOwnerIntent(t, ctx, db, first, 1, at)
			// Insert the bytewise-later key first, with a locale-sensitive tie.
			for index, fixture := range []*fanOutOwnerFixture{&first, &second} {
				semanticPath := []string{"root.a", "root.Z"}[index]
				if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET semantic_path=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND flow_path=$4 AND declaration_family='fan_out' AND semantic_path=$5`, semanticPath, fixture.runID, fixture.deliveryID, fixture.flowPath, fixture.semanticPath); err != nil {
					t.Fatal(err)
				}
				fixture.semanticPath = semanticPath
			}
			_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{first})
			executor := &fanOutSelectionExecutor{bundle: first.bundleHash, started: make(chan capturedFanOutTurn, 4), completed: make(chan fanoutobligation.Intent, 4), errors: make(chan error, 4)}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			want := []string{first.semanticPath, second.semanticPath}
			if want[0] > want[1] {
				want[0], want[1] = want[1], want[0]
			}
			for _, semanticPath := range want {
				select {
				case intent := <-executor.completed:
					if intent.Request.Key.ElementRef.SemanticPath != semanticPath {
						t.Fatalf("clock tie used insertion/registration order: got=%s want=%s", intent.Request.Key.ElementRef.SemanticPath, semanticPath)
					}
				case err := <-executor.errors:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, first, 1, 1)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, second, 1, 1)
		})
	}
}
