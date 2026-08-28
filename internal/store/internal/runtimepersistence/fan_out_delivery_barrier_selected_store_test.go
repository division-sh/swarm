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
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
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
				}
				if _, err := activation.Repeat("work", uuid.NewString(), base.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
				updateFanOutBarrierLoopState(t, ctx, db, fixture.runID, entityID, activation, base.Add(2*time.Second))
				advanceFanOutBarriersForTest(t, ctx, selected, db, fixture.runID, base.Add(3*time.Second))

				if closeFirst {
					want := fanoutbarrier.Summary{Total: 0}
					assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedGenerationSuperseded, &want, handle.TaskID())
					assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)
				} else {
					assertFanOutBarrierState(t, ctx, db, fixture.runID, fixture.deliveryID, fixture.elementID, fanoutbarrier.StatusSuppressedGenerationSuperseded, nil, "")
					assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)
				}
			})
		}
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
	digest := "sha256:" + strings.Repeat("3", 64)
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
	source, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.ChildForProducerWithRoutingSource(
		uuid.NewString(), events.EventType("items.child"), eventtest.Producer(events.EventProducerPlatform, "fan-out-test"), "",
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
		failure := `{"class":"invalid_input","code":"fixture_rejection","owner":"test","operation":"fan_out","message":"fixture rejection"}`
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,package_key,element_id,ordinal,outcome_kind,failure,created_at) VALUES ($1,$2,$3,$4,$5,'semantic_rejected',$6,$7)`, fixture.runID, fixture.deliveryID, fixture.packageKey, fixture.elementID, len(eventIDs), failure, at)
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
			if err := store.pipelinePostgresOwner.AdvanceFanOutDeliveryBarriersTx(txctx, tx, effects, runID, at); err != nil {
				return err
			}
			_, err := runforkrevision.FinalizePostgres(txctx, tx, effects)
			return err
		})
	case *SQLiteRuntimeStore:
		return store.runRuntimeMutation(ctx, "sqlite fan-out barrier candidate proof", func(txctx context.Context, tx *sql.Tx) error {
			effects := runforkrevision.NewEffects()
			if err := store.pipelineSQLiteOwner.AdvanceFanOutDeliveryBarriersTx(txctx, tx, effects, runID, at); err != nil {
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
