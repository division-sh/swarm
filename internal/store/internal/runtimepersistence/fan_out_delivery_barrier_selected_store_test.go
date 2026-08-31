package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestFanOutDeliveryBarrierMixedDispositionLifecycleOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 4, base)
			handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)

			routes := [][]events.DeliveryRoute{
				nil,
				{fanOutBarrierRoute("single")},
				{fanOutBarrierRoute("multi-a"), fanOutBarrierRoute("multi-b"), fanOutBarrierRoute("multi-c")},
			}
			eventsByOrdinal := make([]events.Event, 3)
			for ordinal := range routes {
				event := fanOutBarrierChildEvent(t, fixture, ordinal, base.Add(time.Duration(ordinal+1)*time.Second))
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes[ordinal]); err != nil {
					t.Fatalf("commit ordinal %d event: %v", ordinal, err)
				}
				eventsByOrdinal[ordinal] = event
			}
			seedFanOutBarrierOutcomes(t, ctx, db, fixture, []string{eventsByOrdinal[0].ID(), eventsByOrdinal[1].ID(), eventsByOrdinal[2].ID()}, true, base.Add(5*time.Second))

			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(6*time.Second))
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusArmed, nil, "")
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)

			settleFanOutBarrierRouteSuccess(t, ctx, selected, eventsByOrdinal[1], routes[1][0])
			settleFanOutBarrierRouteDeadLetter(t, ctx, selected, eventsByOrdinal[2], routes[2][0])
			settleFanOutBarrierRouteSuccess(t, ctx, selected, eventsByOrdinal[2], routes[2][1])
			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(7*time.Second))
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusArmed, nil, "")

			settleFanOutBarrierRouteSuccess(t, ctx, selected, eventsByOrdinal[2], routes[2][2])
			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(8*time.Second))
			want := fanoutbarrier.Summary{Total: 4, Succeeded: 1, DeadLettered: 1, NoRoute: 1, SemanticRejected: 1}
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)

			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(9*time.Second))
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)

			summary, err := owner.FanOutRunSummary(ctx, fixture.runID, base.Add(10*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if summary.BarrierArmed != 0 || summary.BarrierPending != 1 || summary.BarrierTerminal != 0 || !summary.BlocksCompletion() {
				t.Fatalf("mixed barrier public summary = %#v", summary)
			}
		})
	}
}

func TestFanOutDeliveryBarrierCardinalityBoundariesOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cardinality := range []int{0, 1, 31, 32, 33, 1000} {
			t.Run(fmt.Sprintf("%s/N=%d", backend, cardinality), func(t *testing.T) {
				ctx := testAuthorActivityContext()
				owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				selected := owner.(storeTestDurableEventBusStore)
				base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, cardinality, base)
				handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)
				now := base.Add(time.Second)
				for {
					intent, claim, found, err := owner.ClaimFanOutIntent(ctx, runtimepipeline.FanOutClaimRequest{
						Owner: "barrier-boundary", BundleHash: fixture.bundleHash, Now: now, Lease: time.Hour,
					})
					if err != nil {
						t.Fatal(err)
					}
					if !found {
						break
					}
					input, err := owner.LoadFanOutEvaluation(ctx, claim)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, intent.Cursor, len(input.Items), now.Add(time.Millisecond))); err != nil {
						t.Fatal(err)
					}
					now = now.Add(time.Second)
				}
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, now)
				want := fanoutbarrier.Summary{Total: cardinality, SemanticRejected: cardinality}
				assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
				assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)
			})
		}
	}
}

func TestFanOutDeliveryBarrierFoldsOnlyExactIntentNotNestedDescendantsOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			parent := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, base)
			child := seedFanOutOwnerChildFixture(t, ctx, db, owner, postgres, parent, 1, base.Add(time.Second))
			handle := seedFanOutDeliveryBarrier(t, ctx, db, parent, base)

			_, claim, found, err := owner.ClaimFanOutIntent(ctx, runtimepipeline.FanOutClaimRequest{
				Owner: "parent-only", BundleHash: parent.bundleHash, Now: base.Add(2 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found || claim.Key.ElementRef.ElementID != parent.elementID {
				t.Fatalf("claim parent intent = %#v found=%v err=%v", claim, found, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, base.Add(3*time.Second))); err != nil {
				t.Fatal(err)
			}
			advanceFanOutBarriersForTest(t, ctx, selected, db, parent.runID, base.Add(4*time.Second))
			want := fanoutbarrier.Summary{Total: 1, SemanticRejected: 1}
			assertFanOutBarrierState(t, ctx, db, parent.runID, parent.deliveryID, parent.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
			assertFanOutCursorAndOutcomeCount(t, ctx, db, child, 0, 0)

			summary, err := owner.FanOutRunSummary(ctx, parent.runID, base.Add(5*time.Second))
			if err != nil || summary.Owed != 1 || summary.BarrierPending != 1 {
				t.Fatalf("parent exact/nested summary = %#v err=%v", summary, err)
			}
		})
	}
}

func TestFanOutDeliveryBarrierRestartAndExactIntentIsolationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			restartedSelected := restarted.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			closed := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			openSibling := seedFanOutOwnerIntent(t, ctx, db, closed, 1, base.Add(time.Second))
			closedHandle := seedFanOutDeliveryBarrier(t, ctx, db, closed, base)
			seedFanOutDeliveryBarrier(t, ctx, db, openSibling, base.Add(time.Second))

			// A separately constructed owner simulates restart after registration.
			advanceFanOutBarriersForTest(t, ctx, restartedSelected, db, closed.runID, base.Add(2*time.Second))
			closedSummary := fanoutbarrier.Summary{Total: 0}
			assertFanOutBarrierState(t, ctx, db, closed.runID, closed.deliveryID, closed.elementID, fanoutbarrier.StatusClosedPending, &closedSummary, closedHandle.TaskID())
			assertFanOutBarrierState(t, ctx, db, openSibling.runID, openSibling.deliveryID, openSibling.elementID, fanoutbarrier.StatusArmed, nil, "")
			assertFanOutBarrierTimerCount(t, ctx, db, closed.runID, 1)

			_, claim, found, err := restarted.ClaimFanOutIntent(ctx, runtimepipeline.FanOutClaimRequest{
				Owner: "restart-isolation", BundleHash: openSibling.bundleHash, Now: base.Add(3 * time.Second), Lease: time.Minute,
			})
			if err != nil || !found || claim.Key != (fanoutobligation.IntentKey{
				RunID: openSibling.runID, TriggeringDeliveryID: openSibling.deliveryID,
				ElementRef: runtimecontracts.FanOutElementRef{PackageKey: openSibling.packageKey, ElementID: openSibling.elementID},
			}) {
				t.Fatalf("claim exact sibling after restart = %#v found=%v err=%v", claim, found, err)
			}
			if _, err := restarted.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 1, base.Add(4*time.Second))); err != nil {
				t.Fatal(err)
			}
			advanceFanOutBarriersForTest(t, ctx, restartedSelected, db, closed.runID, base.Add(5*time.Second))
			siblingSummary := fanoutbarrier.Summary{Total: 1, SemanticRejected: 1}
			assertFanOutBarrierState(t, ctx, db, openSibling.runID, openSibling.deliveryID, openSibling.elementID, fanoutbarrier.StatusClosedPending, &siblingSummary, mustFanOutBarrierTaskID(t, ctx, db, openSibling))
			assertFanOutBarrierTimerCount(t, ctx, db, closed.runID, 2)
		})
	}
}

func TestFanOutDeliveryBarrierCandidateReturnsExactPostCommitScheduleActivationOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)

			operationOwner := selected.(runtimerunlifecycle.OperationOwner)
			if disposition, err := operationOwner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(fixture.runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("request barrier completion candidate = %s/%v", disposition, err)
			}
			result, err := executeRunCompletionCandidateForRun(
				ctx,
				selected.(runtimerunlifecycle.CandidateStore),
				fixture.bundleHash,
				fixture.runID,
				runtimerunlifecycle.NewTerminalCatalog([]string{"completed"}, nil),
			)
			if err != nil {
				t.Fatalf("execute barrier completion candidate: %v", err)
			}
			if result.Outcome != runtimerunlifecycle.OutcomeAwaitMutation || len(result.GenericScheduleActivations) != 1 {
				t.Fatalf("barrier completion result = %#v, want await_mutation with one activation", result)
			}
			var persistedActivationID string
			if err := db.QueryRowContext(ctx, `SELECT timer_id FROM timers WHERE run_id=$1`, fixture.runID).Scan(&persistedActivationID); err != nil {
				t.Fatal(err)
			}
			if got := result.GenericScheduleActivations[0].ID(); got != persistedActivationID {
				t.Fatalf("committed activation handoff = %s, want persisted %s", got, persistedActivationID)
			}
		})
	}
}

func TestFanOutDeliveryBarrierCorruptFactsFailBeforeCloseOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, corruption := range []string{"missing_ordinal", "contradictory_handle", "contradictory_plan"} {
			t.Run(backend+"/"+corruption, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
				selected := restarted.(storeTestDurableEventBusStore)
				base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, base)
				seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)

				switch corruption {
				case "missing_ordinal":
					if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET cursor=1,status='closed' WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
						t.Fatal(err)
					}
				case "contradictory_handle":
					if _, err := db.ExecContext(ctx, `UPDATE fan_out_obligation_barriers SET timer_handle=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND package_key=$4 AND element_id=$5`, `{}`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
						t.Fatal(err)
					}
				case "contradictory_plan":
					if _, err := db.ExecContext(ctx, `UPDATE fan_out_obligation_barriers SET semantic_digest=$1 WHERE run_id=$2 AND triggering_delivery_id=$3 AND package_key=$4 AND element_id=$5`, "sha256:"+strings.Repeat("f", 64), fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
						t.Fatal(err)
					}
				}

				if err := advanceFanOutBarriersAttempt(ctx, selected, fixture.runID, base.Add(time.Second)); err == nil {
					t.Fatalf("%s corruption advanced barrier", corruption)
				}
				assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusArmed, nil, "")
				assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)
			})
		}
	}
}

func TestFanOutDeliveryBarrierCancellationSuppressesOutcomeOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 3, base)
			seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)

			if err := owner.CancelRunFanOut(ctx, fixture.runID, "operator canceled", base.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			want := fanoutbarrier.Summary{Total: 3, Canceled: 3}
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedRunTerminal, &want, "")
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)
			summary, err := owner.FanOutRunSummary(ctx, fixture.runID, base.Add(2*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if summary.BarrierArmed != 0 || summary.BarrierPending != 0 || summary.BarrierTerminal != 1 || summary.BlocksCompletion() {
				t.Fatalf("canceled barrier public summary = %#v", summary)
			}
		})
	}
}

func TestFanOutDeliveryBarrierGenerationSupersessionOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, closeFirst := range []bool{false, true} {
			name := "armed"
			if closeFirst {
				name = "closed_pending"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				selected := owner.(storeTestDurableEventBusStore)
				base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
				handle, entityID, activation := seedFanOutDeliveryBarrierForLoop(t, ctx, db, fixture, base)
				seedFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base)

				if closeFirst {
					advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(time.Second))
					want := fanoutbarrier.Summary{Total: 0}
					assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
					activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
					assertFanOutBarrierScheduleLifecycle(t, ctx, db, activationID, "active", "", false)
				}
				if _, err := activation.Repeat("work", uuid.NewString(), base.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
				updateFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base.Add(2*time.Second))
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(3*time.Second))

				if closeFirst {
					want := fanoutbarrier.Summary{Total: 0}
					assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedGenerationSuperseded, &want, handle.TaskID())
					activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
					assertFanOutBarrierScheduleLifecycle(t, ctx, db, activationID, "cancelled", "fan_out_generation_superseded", false)
					assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)
				} else {
					assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedGenerationSuperseded, nil, "")
					assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)
				}
			})
		}
	}
}

func TestFanOutDeliveryBarrierCompletionAndSupersessionWinnerMatrixOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, completionWins := range []bool{false, true} {
			name := "supersession_wins_after_publication"
			if completionWins {
				name = "completion_commit_wins"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				selected := owner.(storeTestDurableEventBusStore)
				base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
				handle, entityID, activation := seedFanOutDeliveryBarrierForLoop(t, ctx, db, fixture, base)
				seedFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base)
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(time.Second))
				summary := fanoutbarrier.Summary{Total: 0}
				activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)

				route := fanOutBarrierRoute("completion-race")
				occurrenceID := runtimegenericschedule.OccurrenceEventID(activationID, base.Add(time.Second))
				occurrence := fanOutBarrierChildEventWithID(t, fixture, occurrenceID, 700, base.Add(2*time.Second))
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, occurrence, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				markFanOutBarrierScheduleFired(t, ctx, db, activationID, occurrence.ID(), base.Add(2*time.Second))

				if completionWins {
					claimed, err := claimDeliveryFixture(ctx, selected, occurrence, route)
					if err != nil {
						t.Fatal(err)
					}
					completion := fanoutbarrier.Completion{Handle: handle, Summary: summary}
					commitFanOutBarrierCompletionForTest(t, ctx, selected, runtimepipeline.WorkflowEngineMutationCommand{
						EntitylessTarget: route.Target, EntitylessRunID: fixture.runID, FanOutBarrierCompletion: &completion,
						DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
							Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Millisecond,
							RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
						},
					})
				}

				if _, err := activation.Repeat("work", uuid.NewString(), base.Add(3*time.Second)); err != nil {
					t.Fatal(err)
				}
				updateFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base.Add(3*time.Second))
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(4*time.Second))

				wantStatus := fanoutbarrier.StatusSuppressedGenerationSuperseded
				if completionWins {
					wantStatus = fanoutbarrier.StatusFired
				}
				assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, wantStatus, &summary, handle.TaskID())
				assertFanOutBarrierScheduleLifecycle(t, ctx, db, activationID, "fired", "", true)
				runSummary, err := owner.FanOutRunSummary(ctx, fixture.runID, base.Add(5*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if runSummary.BarrierArmed != 0 || runSummary.BarrierPending != 0 || runSummary.BarrierTerminal != 1 || runSummary.BlocksCompletion() {
					t.Fatalf("race winner run summary = %#v, want one nonblocking terminal barrier", runSummary)
				}
			})
		}
	}
}

func TestFanOutDeliveryBarrierConcurrentGenerationSupersessionCancelsExactlyOnceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			handle, entityID, activation := seedFanOutDeliveryBarrierForLoop(t, ctx, db, fixture, base)
			seedFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base)
			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(time.Second))
			activationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
			if _, err := activation.Repeat("work", uuid.NewString(), base.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			updateFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base.Add(2*time.Second))

			start := make(chan struct{})
			errs := make(chan error, 2)
			var workers sync.WaitGroup
			for range 2 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					errs <- advanceFanOutBarriersAttempt(ctx, selected, fixture.runID, base.Add(3*time.Second))
				}()
			}
			close(start)
			workers.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			want := fanoutbarrier.Summary{Total: 0}
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedGenerationSuperseded, &want, handle.TaskID())
			assertFanOutBarrierScheduleLifecycle(t, ctx, db, activationID, "cancelled", "fan_out_generation_superseded", false)
		})
	}
}

func TestFanOutDeliveryBarrierConcurrentCandidatesCloseExactlyOnceOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)

			start := make(chan struct{})
			errs := make(chan error, 2)
			var workers sync.WaitGroup
			for range 2 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					errs <- advanceFanOutBarriersAttempt(ctx, selected, fixture.runID, base.Add(time.Second))
				}()
			}
			close(start)
			workers.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}

			want := fanoutbarrier.Summary{Total: 0}
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusClosedPending, &want, handle.TaskID())
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)
		})
	}
}

func TestFanOutDeliveryBarrierCompletionFiresIdempotentlyOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
			handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)
			advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(time.Second))
			summary := fanoutbarrier.Summary{Total: 0}
			completion := fanoutbarrier.Completion{Handle: handle, Summary: summary}
			route := fanOutBarrierRoute("completion")
			commands := make([]runtimepipeline.WorkflowEngineMutationCommand, 0, 2)
			for attempt := range 2 {
				event := fanOutBarrierChildEvent(t, fixture, 100+attempt, base.Add(time.Duration(attempt+2)*time.Second))
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				claimed, err := claimDeliveryFixture(ctx, selected, event, route)
				if err != nil {
					t.Fatal(err)
				}
				commands = append(commands, runtimepipeline.WorkflowEngineMutationCommand{
					EntitylessTarget:        route.Target,
					EntitylessRunID:         fixture.runID,
					FanOutBarrierCompletion: &completion,
					DeliverySuccess: &runtimepipeline.WorkflowEngineDeliverySuccess{
						Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, Duration: time.Millisecond,
						RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
					},
				})
			}
			start := make(chan struct{})
			errs := make(chan error, len(commands))
			var workers sync.WaitGroup
			for _, command := range commands {
				command := command
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					switch store := selected.(type) {
					case *PostgresStore:
						_, err := store.CommitWorkflowEngineMutation(ctx, command)
						errs <- err
					case *SQLiteRuntimeStore:
						_, err := store.CommitWorkflowEngineMutation(ctx, command)
						errs <- err
					default:
						errs <- fmt.Errorf("unsupported store %T", selected)
					}
				}()
			}
			close(start)
			workers.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusFired, &summary, handle.TaskID())
			assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)
		})
	}
}

func TestFanOutDeliveryBarrierOutcomeFailureTerminalizesOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, noRoute := range []bool{true, false} {
			name := "dead_letter"
			if noRoute {
				name = "no_route"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				selected := owner.(storeTestDurableEventBusStore)
				base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 0, base)
				handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, base)
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(time.Second))
				event := fanOutBarrierChildEvent(t, fixture, 200, base.Add(2*time.Second))
				var routes []events.DeliveryRoute
				if !noRoute {
					routes = []events.DeliveryRoute{fanOutBarrierRoute("failed-outcome")}
				}
				if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
					t.Fatal(err)
				}
				if !noRoute {
					settleFanOutBarrierRouteDeadLetter(t, ctx, selected, event, routes[0])
				}
				if _, err := db.ExecContext(ctx, `UPDATE timers SET status='fired',occurrence_event_id=$1,occurrence_admitted_at=$2,fired_at=$2,accepted_at=$2 WHERE run_id=$3 AND schedule_key=$4`, event.ID(), base.Add(3*time.Second), fixture.runID, handle.TaskID()); err != nil {
					t.Fatal(err)
				}
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(4*time.Second))
				want := fanoutbarrier.Summary{Total: 0}
				assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusOutcomeDeadLettered, &want, handle.TaskID())
			})
		}
	}
}

func TestRunForkFanOutDeliveryBarrierFixedRevisionStateMatrixOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			selected := owner.(storeTestDurableEventBusStore)
			forkOwner := owner.(interface {
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			cases := []struct {
				name        string
				status      fanoutbarrier.Status
				cardinality int
			}{
				{name: "armed", status: fanoutbarrier.StatusArmed, cardinality: 1},
				{name: "closed_pending", status: fanoutbarrier.StatusClosedPending},
				{name: "fired", status: fanoutbarrier.StatusFired},
				{name: "outcome_dead_lettered", status: fanoutbarrier.StatusOutcomeDeadLettered},
				{name: "suppressed_run_terminal", status: fanoutbarrier.StatusSuppressedRunTerminal, cardinality: 1},
				{name: "suppressed_generation", status: fanoutbarrier.StatusSuppressedGenerationSuperseded, cardinality: 1},
			}
			for index, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					at := base.Add(time.Duration(index) * 10 * time.Minute)
					fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, tc.cardinality, at)
					handle := seedFanOutDeliveryBarrier(t, ctx, db, fixture, at)
					var summary *fanoutbarrier.Summary
					var sourceScheduleActivationID string
					switch tc.status {
					case fanoutbarrier.StatusClosedPending, fanoutbarrier.StatusFired, fanoutbarrier.StatusOutcomeDeadLettered:
						advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, at.Add(time.Second))
						value := fanoutbarrier.Summary{Total: 0}
						summary = &value
						if tc.status == fanoutbarrier.StatusClosedPending {
							sourceScheduleActivationID = mustFanOutBarrierScheduleActivationID(t, ctx, db, fixture)
						}
						if tc.status != fanoutbarrier.StatusClosedPending {
							if _, err := db.ExecContext(ctx, `UPDATE fan_out_obligation_barriers SET status=$1,updated_at=$2 WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6`, string(tc.status), at.Add(2*time.Second), fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
								t.Fatal(err)
							}
						}
					case fanoutbarrier.StatusSuppressedRunTerminal:
						if err := owner.CancelRunFanOut(ctx, fixture.runID, "fork terminal", at.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						value := fanoutbarrier.Summary{Total: 1, Canceled: 1}
						summary = &value
					case fanoutbarrier.StatusSuppressedGenerationSuperseded:
						if _, err := db.ExecContext(ctx, `UPDATE fan_out_obligation_barriers SET status=$1,updated_at=$2 WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6`, string(tc.status), at.Add(time.Second), fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID); err != nil {
							t.Fatal(err)
						}
					}

					forkPointID := uuid.NewString()
					forkPoint := eventtest.ExistingRunRootIngressWithRoutingSource(
						forkPointID, events.EventType("fork.barrier."+tc.name), "operator", "", []byte(`{}`), 0,
						fixture.runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(uuid.NewString()), at.Add(3*time.Second),
					)
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, fixture.bundleHash, postgres)
					if err := insertCanonicalEventRecordFixture(ctx, owner, forkPoint); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, db, fixture.runID, fixture.bundleHash, postgres)
					materialized, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointID})
					if err != nil {
						t.Fatalf("materialize %s barrier fork: %v", tc.name, err)
					}
					repeated, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: fixture.runID, At: forkPointID})
					if err != nil || repeated.ForkRunID != materialized.ForkRunID {
						t.Fatalf("repeat %s barrier fork = %#v err=%v, want %s", tc.name, repeated, err, materialized.ForkRunID)
					}
					assertFanOutBarrierState(t, ctx, db, materialized.ForkRunID, fixture.deliveryID, fixture.elementID, tc.status, summary, expectedForkBarrierSchedule(tc.status, handle.TaskID()))
					assertForkBarrierScheduleState(t, ctx, db, materialized.ForkRunID, tc.status)
					if tc.status == fanoutbarrier.StatusClosedPending {
						childFixture := fixture
						childFixture.runID = materialized.ForkRunID
						childScheduleActivationID := mustFanOutBarrierScheduleActivationID(t, ctx, db, childFixture)
						if childScheduleActivationID == sourceScheduleActivationID {
							t.Fatalf("fork reused source schedule activation %s", sourceScheduleActivationID)
						}
						var childTimerID string
						if err := db.QueryRowContext(ctx, `SELECT timer_id FROM timers WHERE run_id=$1 AND schedule_key=$2`, materialized.ForkRunID, handle.TaskID()).Scan(&childTimerID); err != nil {
							t.Fatal(err)
						}
						if childTimerID != childScheduleActivationID {
							t.Fatalf("fork barrier activation = %s, child-local timer = %s", childScheduleActivationID, childTimerID)
						}
					}
				})
			}
		})
	}
}

func seedFanOutDeliveryBarrier(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, at time.Time) timeridentity.TimerHandle {
	t.Helper()
	return seedFanOutDeliveryBarrierRecord(t, ctx, db, fixture, uuid.NewString(), attemptgeneration.Generation{}, at)
}

func seedFanOutDeliveryBarrierForLoop(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, at time.Time) (timeridentity.TimerHandle, string, loopruntime.Activation) {
	t.Helper()
	entityID := uuid.NewString()
	activation, err := loopruntime.New(fixture.runID, entityID, "", "retry", "revision_id", fixture.eventID, "work", 3, at)
	if err != nil {
		t.Fatal(err)
	}
	handle := seedFanOutDeliveryBarrierRecord(t, ctx, db, fixture, entityID, activation.Generation(), at)
	return handle, entityID, activation
}

func seedFanOutDeliveryBarrierRecord(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, entityID string, generation attemptgeneration.Generation, at time.Time) timeridentity.TimerHandle {
	t.Helper()
	digest := "sha256:" + strings.Repeat("2", 64)
	ref, err := timeridentity.NewFanOutDeliveryJoinRef(
		mustPersistenceRootNode("fan-out-source"), "items.ready", "all-items-delivered",
		fixture.packageKey, fixture.elementID, fixture.bundleHash, digest,
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindFanOutIntent(fixture.deliveryID, generation)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	routingSource, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	routingRaw, _ := json.Marshal(routingSource)
	handleRaw, _ := json.Marshal(handle)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	mustExecRunForkRevisionMatrix(t, ctx, tx, `
		INSERT INTO fan_out_obligation_barriers (
			run_id,triggering_delivery_id,package_key,element_id,bundle_hash,semantic_digest,
			target_package_key,target_flow_id,target_node_id,handler_event,join_id,
			route_scope_key,route_instance_id,route_instance_path,entity_id,routing_source,
			execution_mode,timer_handle,status,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'root','root','root',$12,$13,$14,$15,'armed',$16,$16)
	`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, fixture.bundleHash, digest,
		ref.PackageKey(), ref.FlowID(), ref.NodeID(), ref.HandlerEvent(), ref.JoinID(), entityID,
		string(routingRaw), string(executionmode.Live), string(handleRaw), at)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return handle
}

func seedFanOutBarrierLoopState(t *testing.T, ctx context.Context, db *sql.DB, runID, entityID string, activation loopruntime.Activation, at time.Time) {
	t.Helper()
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(buckets)
	if _, err := db.ExecContext(ctx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1,$2,'root','fan_out_fixture','work','{}','{"entity_type":"fan_out_fixture"}','{}',$3,1,$4,$4,$4)`, runID, entityID, string(raw), at); err != nil {
		t.Fatal(err)
	}
}

func updateFanOutBarrierLoopState(t *testing.T, ctx context.Context, db *sql.DB, runID, entityID string, activation loopruntime.Activation, at time.Time) {
	t.Helper()
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(buckets)
	if _, err := db.ExecContext(ctx, `UPDATE entity_state SET accumulator=$1,revision=revision+1,updated_at=$2 WHERE run_id=$3 AND entity_id=$4`, string(raw), at, runID, entityID); err != nil {
		t.Fatal(err)
	}
}

func fanOutBarrierRoute(nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode(nodeID)),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "root", FlowInstance: "root"}),
	}
}

func fanOutBarrierChildEvent(t *testing.T, fixture fanOutOwnerFixture, ordinal int, at time.Time) events.Event {
	t.Helper()
	return fanOutBarrierChildEventWithID(t, fixture, uuid.NewString(), ordinal, at)
}

func fanOutBarrierChildEventWithID(t *testing.T, fixture fanOutOwnerFixture, eventID string, ordinal int, at time.Time) events.Event {
	t.Helper()
	source, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.ChildForProducerWithRoutingSource(
		eventID, events.EventType("items.child"), eventtest.Producer(events.EventProducerPlatform, "fan-out-test"), "",
		[]byte(fmt.Sprintf(`{"ordinal":%d}`, ordinal)), 0,
		events.EventLineage{RunID: fixture.runID, ParentEventID: fixture.eventID, ExecutionMode: executionmode.Live},
		events.EventEnvelope{Scope: events.EventScopeGlobal}, source, at,
	)
}

func seedFanOutBarrierOutcomes(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture, eventIDs []string, appendRejection bool, at time.Time) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for ordinal, eventID := range eventIDs {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,event_id,created_at) VALUES ($1,$2,$3,$4,$5,'committed',$6,$7)`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, ordinal, eventID, at)
	}
	if appendRejection {
		failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassSchemaInvalid, "fixture_rejection", "test", "fan_out", map[string]any{"path": "$.fixture"}))
		if !ok {
			t.Fatal("construct typed semantic rejection fixture")
		}
		failureRaw, err := runtimefailures.MarshalEnvelope(failure)
		if err != nil {
			t.Fatal(err)
		}
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,$3,$4,$5,'semantic_rejected',$6,$7)`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, len(eventIDs), string(failureRaw), at)
	}
	cursor := len(eventIDs)
	if appendRejection {
		cursor++
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `UPDATE fan_out_intents SET cursor=$1,status='closed',updated_at=$2 WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6`, cursor, at, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func settleFanOutBarrierRouteSuccess(t *testing.T, ctx context.Context, store storeTestDurableEventBusStore, event events.Event, route events.DeliveryRoute) {
	t.Helper()
	claimed, err := claimDeliveryFixture(ctx, store, event, route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatal(err)
	}
}

func settleFanOutBarrierRouteDeadLetter(t *testing.T, ctx context.Context, store storeTestDurableEventBusStore, event events.Event, route events.DeliveryRoute) {
	t.Helper()
	claimed, err := claimDeliveryFixture(ctx, store, event, route)
	if err != nil {
		t.Fatal(err)
	}
	failure := testFailureEnvelope(runtimefailures.ClassLifecycleConflict, "fan_out_barrier_fixture", nil)
	if _, err := store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter, ReasonCode: "fixture_exhausted", Failure: &failure,
		RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
	}); err != nil {
		t.Fatal(err)
	}
}

func advanceFanOutBarriersForTest(t *testing.T, ctx context.Context, selected storeTestDurableEventBusStore, db *sql.DB, runID string, at time.Time) {
	t.Helper()
	if err := advanceFanOutBarriersAttempt(ctx, selected, runID, at); err != nil {
		t.Fatal(err)
	}
}

func advanceFanOutBarriersAttempt(ctx context.Context, selected storeTestDurableEventBusStore, runID string, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runPostgresRuntimeMutation(ctx, func(txctx context.Context, tx *sql.Tx) error {
			effects := runforkrevision.NewEffects()
			if _, err := store.pipelinePostgresOwner.AdvanceFanOutDeliveryBarriersTx(txctx, tx, effects, runID, at); err != nil {
				return err
			}
			_, err := runforkrevision.FinalizePostgres(txctx, tx, effects)
			return err
		})
	case *SQLiteRuntimeStore:
		return store.runRuntimeMutation(ctx, "sqlite fan-out barrier candidate proof", func(txctx context.Context, tx *sql.Tx) error {
			effects := runforkrevision.NewEffects()
			if _, err := store.pipelineSQLiteOwner.AdvanceFanOutDeliveryBarriersTx(txctx, tx, effects, runID, at); err != nil {
				return err
			}
			_, err := runforkrevision.FinalizeSQLite(txctx, tx, effects)
			return err
		})
	default:
		return fmt.Errorf("unsupported fan-out barrier store %T", selected)
	}
}

func assertFanOutBarrierState(t *testing.T, ctx context.Context, db *sql.DB, runID, triggeringDeliveryID, elementID string, wantStatus fanoutbarrier.Status, wantSummary *fanoutbarrier.Summary, wantSchedule string) {
	t.Helper()
	var status, schedule string
	var summaryRaw any
	if err := db.QueryRowContext(ctx, `SELECT status,COALESCE(summary,'null'),COALESCE(schedule_key,'') FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key='root' AND element_id=$3`, runID, triggeringDeliveryID, elementID).Scan(&status, &summaryRaw, &schedule); err != nil {
		t.Fatal(err)
	}
	if fanoutbarrier.Status(status) != wantStatus || schedule != wantSchedule {
		t.Fatalf("barrier state = status:%s schedule:%q, want %s/%q", status, schedule, wantStatus, wantSchedule)
	}
	raw := []byte(fmt.Sprint(summaryRaw))
	if bytes, ok := summaryRaw.([]byte); ok {
		raw = bytes
	}
	if wantSummary == nil {
		if string(raw) != "null" {
			t.Fatalf("barrier summary = %s, want null", raw)
		}
		return
	}
	var got fanoutbarrier.Summary
	if err := json.Unmarshal(raw, &got); err != nil || got != *wantSummary {
		t.Fatalf("barrier summary = %#v err=%v, want %#v", got, err, *wantSummary)
	}
}

func assertFanOutBarrierTimerCount(t *testing.T, ctx context.Context, db *sql.DB, runID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id=$1 AND task_type='timer'`, runID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("fan-out barrier timers = %d, want %d", got, want)
	}
}

func mustFanOutBarrierTaskID(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture) string {
	t.Helper()
	var taskID string
	if err := db.QueryRowContext(ctx, `SELECT schedule_key FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(taskID) == "" {
		t.Fatal("fan-out barrier schedule key is empty")
	}
	return taskID
}

func mustFanOutBarrierScheduleActivationID(t *testing.T, ctx context.Context, db *sql.DB, fixture fanOutOwnerFixture) string {
	t.Helper()
	var activationID string
	if err := db.QueryRowContext(ctx, `SELECT schedule_activation_id FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID).Scan(&activationID); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(activationID); err != nil {
		t.Fatalf("fan-out barrier schedule activation ID = %q: %v", activationID, err)
	}
	return activationID
}

func assertFanOutBarrierScheduleLifecycle(t *testing.T, ctx context.Context, db *sql.DB, activationID, wantStatus, wantCause string, wantOccurrence bool) {
	t.Helper()
	var status, cause string
	var occurrence sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status,COALESCE(cancel_cause,''),occurrence_event_id FROM timers WHERE timer_id=$1`, activationID).Scan(&status, &cause, &occurrence); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || cause != wantCause || occurrence.Valid != wantOccurrence {
		t.Fatalf("fan-out barrier activation %s = status:%s cause:%q occurrence:%v, want %s/%q/%v", activationID, status, cause, occurrence.Valid, wantStatus, wantCause, wantOccurrence)
	}
}

func markFanOutBarrierScheduleFired(t *testing.T, ctx context.Context, db *sql.DB, activationID, occurrenceEventID string, at time.Time) {
	t.Helper()
	result, err := db.ExecContext(ctx, `UPDATE timers SET status='fired',occurrence_event_id=$1,occurrence_admitted_at=$2,fired_at=$2,accepted_at=$2 WHERE timer_id=$3 AND status='active'`, occurrenceEventID, at, activationID)
	if err != nil {
		t.Fatal(err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("mark fan-out barrier activation fired affected %d rows: %v", affected, err)
	}
}

func commitFanOutBarrierCompletionForTest(t *testing.T, ctx context.Context, selected storeTestDurableEventBusStore, command runtimepipeline.WorkflowEngineMutationCommand) {
	t.Helper()
	switch store := selected.(type) {
	case *PostgresStore:
		if _, err := store.CommitWorkflowEngineMutation(ctx, command); err != nil {
			t.Fatal(err)
		}
	case *SQLiteRuntimeStore:
		if _, err := store.CommitWorkflowEngineMutation(ctx, command); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported fan-out barrier store %T", selected)
	}
}

func captureFanOutBarrierForkRevision(t *testing.T, ctx context.Context, db *sql.DB, runID, bundleHash string, postgres bool) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `INSERT INTO bundles (bundle_hash,content_yaml,parsed_json,metadata) VALUES ($1,'api_version: swarm.test.bundle.v1','{}','{"source":"fan-out-barrier-test"}') ON CONFLICT (bundle_hash) DO NOTHING`, bundleHash); err != nil {
		t.Fatalf("admit fan-out barrier fork bundle: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	effects, err := runforkrevision.ForRun(runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatal(err)
	}
	if postgres {
		_, err = runforkrevision.FinalizePostgres(ctx, tx, effects)
	} else {
		_, err = runforkrevision.FinalizeSQLite(ctx, tx, effects)
	}
	if err != nil {
		t.Fatalf("capture fan-out barrier fork revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func expectedForkBarrierSchedule(status fanoutbarrier.Status, sourceTaskID string) string {
	switch status {
	case fanoutbarrier.StatusClosedPending, fanoutbarrier.StatusFired, fanoutbarrier.StatusOutcomeDeadLettered:
		return sourceTaskID
	default:
		return ""
	}
}

func assertForkBarrierScheduleState(t *testing.T, ctx context.Context, db *sql.DB, runID string, status fanoutbarrier.Status) {
	t.Helper()
	var total, pending int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN status IN ('active','claimed') THEN 1 ELSE 0 END),0) FROM timers WHERE run_id=$1`, runID).Scan(&total, &pending); err != nil {
		t.Fatal(err)
	}
	switch status {
	case fanoutbarrier.StatusClosedPending:
		if total != 1 || pending != 1 {
			t.Fatalf("closed-pending fork schedules = total:%d pending:%d, want 1/1", total, pending)
		}
	case fanoutbarrier.StatusArmed, fanoutbarrier.StatusSuppressedRunTerminal, fanoutbarrier.StatusSuppressedGenerationSuperseded:
		if total != 0 || pending != 0 {
			t.Fatalf("unscheduled fork barrier %s has timers total:%d pending:%d", status, total, pending)
		}
	default:
		if pending != 0 {
			t.Fatalf("terminal fork barrier %s retained pending timer", status)
		}
	}
}
