package runtimepersistence

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/google/uuid"
)

func TestFanOutGrantedBatchObservationReadOnlyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 64, time.Now().UTC())
			unregistered := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 1, time.Now().UTC(), storeTestSourceArtifact("unregistered-observed-source"))
			process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			var turn capturedFanOutTurn
			select {
			case turn = <-executor.turns:
			case err := <-executor.errors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			page := fanoutobligation.ListPage{RunID: fixture.runID, Intents: make([]fanoutobligation.IntentReadback, 500)}
			for i := range page.Intents {
				page.Intents[i].Key = turn.key
				page.Intents[i].BundleHash = fixture.bundleHash
				page.Intents[i].Key.ElementRef.SemanticPath = fmt.Sprintf("missing-%03d", i)
			}
			page.Intents[0].Key = turn.key
			read, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page)
			if err != nil || len(read.Intents) != 500 {
				t.Fatalf("bounded runtime observation: count=%d err=%v", len(read.Intents), err)
			}
			for i, row := range read.Intents {
				if row.Key != page.Intents[i].Key {
					t.Fatal("runtime observation changed exact key/order")
				}
				if i == 0 {
					if row.Runtime.Eligible == nil || !*row.Runtime.Eligible || row.Runtime.Reason != "eligible" {
						t.Fatalf("live runtime eligibility: %+v", row.Runtime)
					}
				} else if row.Runtime.Availability != "unavailable" || row.Runtime.Eligible != nil || row.Runtime.Reason == "" {
					t.Fatalf("missing/unregistered runtime acquired authority: %+v", row.Runtime)
				}
			}
			unregisteredPage := fanoutobligation.ListPage{RunID: unregistered.runID, Intents: []fanoutobligation.IntentReadback{{Key: turn.key, BundleHash: unregistered.bundleHash}}}
			unregisteredKey := &unregisteredPage.Intents[0].Key
			unregisteredKey.RunID = unregistered.runID
			unregisteredKey.TriggeringDeliveryID = unregistered.deliveryID
			unregisteredKey.ElementRef.FlowPath = unregistered.flowPath
			unregisteredKey.ElementRef.SemanticPath = unregistered.semanticPath
			unregisteredRead, err := startupownership.ObserveFanOutRuntimePage(ctx, process, unregisteredPage)
			if err != nil {
				t.Fatal(err)
			}
			if runtime := unregisteredRead.Intents[0].Runtime; runtime.Availability != "unavailable" || runtime.Eligible != nil || runtime.Reason != "runtime_unregistered" {
				t.Fatalf("unregistered runtime acquired authority: %+v", runtime)
			}
			page.Intents = page.Intents[:1]
			assertReason := func(reason string, eligible bool) {
				t.Helper()
				readCtx, stop := context.WithTimeout(ctx, time.Second)
				defer stop()
				read, err := startupownership.ObserveFanOutRuntimePage(readCtx, process, page)
				if err != nil {
					t.Fatal(err)
				}
				got := read.Intents[0].Runtime
				if got.Reason != reason || got.Eligible == nil || *got.Eligible != eligible {
					t.Fatalf("runtime observation=%+v want=%s/%v", got, reason, eligible)
				}
			}
			_, claim, found, err := turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "read-snapshot-proof", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim: found=%v err=%v", found, err)
			}
			// Hold the canonical mutation fence. Runtime inspection, global
			// readiness and evaluation must remain reads, not wait behind it.
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := generationauthority.FenceMutation(ctx, tx, !postgres); err != nil {
				t.Fatal(err)
			}
			assertReason("claim_in_flight", false)
			readCtx, stop := context.WithTimeout(ctx, time.Second)
			input, err := turn.owner.LoadFanOutEvaluation(readCtx, claim)
			stop()
			if err != nil || len(input.Items) != 32 {
				t.Fatalf("evaluation acquired mutation fence: items=%d err=%v", len(input.Items), err)
			}
			obligations := selected.(interface {
				PipelineObligations() pipelineobligation.Store
			}).PipelineObligations()
			readCtx, stop = context.WithTimeout(ctx, time.Second)
			_, err = obligations.GlobalWorkPresence(readCtx)
			stop()
			if err != nil {
				t.Fatalf("global work acquired mutation fence: %v", err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := turn.owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatal(err)
			}
			assertReason("eligible", true)
			control := selected.(interface {
				PauseRunControl(context.Context, runcontrol.TransitionRequest) (runcontrol.State, error)
				ContinueRunControl(context.Context, runcontrol.TransitionRequest) (runcontrol.State, error)
			})
			controlCtx := testAuthorActivityContextForBundle(fixture.bundleHash)
			transition := runcontrol.TransitionRequest{RunID: fixture.runID, Now: time.Now().UTC(), Reason: "batch-observation", ControlledBy: "test"}
			if _, err := control.PauseRunControl(controlCtx, transition); err != nil {
				t.Fatal(err)
			}
			assertReason("run_paused", false)
			if _, err := control.ContinueRunControl(controlCtx, transition); err != nil {
				t.Fatal(err)
			}
			assertReason("eligible", true)
			_, claim, found, err = turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "retry-observation", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("retry claim: found=%v err=%v", found, err)
			}
			if err := turn.owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			assertReason("retry_wait", false)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutGrantedSelectorObservationBypassesMutationFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 0, time.Now().UTC())
			_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 4), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if err := generationauthority.FenceMutation(ctx, tx, !postgres); err != nil {
				t.Fatal(err)
			}
			scanned := make(chan error, 1)
			registration.SetTestScanObserver(func(_ startupownership.FanOutCandidate, found bool, err error) {
				if found {
					err = fmt.Errorf("closed intent became a serving candidate")
				}
				select {
				case scanned <- err:
				default:
				}
			})
			registration.Wake()
			select {
			case err := <-scanned:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("candidate observation waited behind the mutation fence")
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutGrantedSelectedFamilyDoesNotBlockOrdinaryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, time.Now().UTC())
			process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			selectedFixture := newSelectedCompletionFixtureWithProcess(t, selected.(selectedCompletionAuthorityStore), db, !postgres, process)
			selectedGrant := selectedMutationFenceGrant(t, ctx, selectedFixture)
			defer selectedGrant.Retire(ctx)
			executor := &fanOutSelectionExecutor{bundle: fixture.bundleHash, started: make(chan capturedFanOutTurn, 2), completed: make(chan fanoutobligation.Intent, 2), errors: make(chan error, 8)}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			select {
			case <-executor.completed:
			case err := <-executor.errors:
				t.Fatalf("selected family poisoned ordinary complete-set observation: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
		})
	}
}

func TestFanOutGrantedAmbiguousOrdinaryExecutionRefusedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, time.Now().UTC())
			process, grants, occurrences, plan := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			evidence, err := grants[0].Evidence()
			if err != nil {
				t.Fatal(err)
			}
			other, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fixture.bundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := other.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := other.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			select {
			case turn := <-executor.turns:
				t.Fatalf("ambiguous grant-id order selected a winner: %+v", turn.key)
			case err := <-executor.errors:
				if !strings.Contains(err.Error(), "ambiguous") {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			obligations := selected.(interface {
				PipelineObligations() pipelineobligation.Store
			}).PipelineObligations()
			if _, err := obligations.GlobalWorkPresence(ctx); err == nil || !strings.Contains(err.Error(), "ambiguous") {
				t.Fatalf("global work accepted ambiguous execution: %v", err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
		})
	}
}

func TestFanOutGrantedSourceRefreshRegistrationWindowBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			fixture := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, time.Now().UTC())
			extra := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 0, time.Now().UTC(), storeTestSourceArtifact("refresh-extra-source"))
			process, grants, occurrences, originalPlan := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
			executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
			workers := 1
			registration, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Close)
			var turn capturedFanOutTurn
			select {
			case turn = <-executor.turns:
			case err := <-executor.errors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			_, claim, found, err := turn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "source-refresh-predecessor", BundleHash: fixture.bundleHash, Candidate: &turn.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("predecessor claim: found=%v err=%v", found, err)
			}
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fixture.bundleHash}, {BundleHash: extra.bundleHash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: originalPlan.Revision, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			evidence, err := grants[0].Evidence()
			if err != nil {
				t.Fatal(err)
			}
			successor, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fixture.bundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := successor.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := successor.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			page := fanoutobligation.ListPage{RunID: fixture.runID, Intents: []fanoutobligation.IntentReadback{{Key: turn.key, BundleHash: fixture.bundleHash}}}
			read, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page)
			if err != nil {
				t.Fatalf("legal off-head predecessor/current-successor overlap failed: %v", err)
			}
			if runtime := read.Intents[0].Runtime; runtime.Reason != "registration_pending" || runtime.Availability != "unavailable" || runtime.Eligible != nil {
				t.Fatalf("replacement window acquired execution: %+v", runtime)
			}
			if _, err := turn.owner.LoadFanOutEvaluation(ctx, claim); err == nil {
				t.Fatal("off-head predecessor retained evaluation authority")
			}
			if err := grants[0].Retire(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-executor.done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := turn.owner.ReleaseFanOutClaim(ctx, claim); err != nil {
				t.Fatalf("retired off-head predecessor could not discard its exact claim: %v", err)
			}
			next := &fanOutSelectionExecutor{bundle: fixture.bundleHash, started: make(chan capturedFanOutTurn, 2), completed: make(chan fanoutobligation.Intent, 2), errors: make(chan error, 8)}
			nextRegistration, err := startupownership.StartFanOutServing(ctx, successor, occurrences[0], &workers, next)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nextRegistration.Close)
			select {
			case <-next.completed:
			case err := <-next.errors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 1, 1)
		})
	}
}
