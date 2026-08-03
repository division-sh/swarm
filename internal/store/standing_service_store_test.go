package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedeliverycontinuation "github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type standingSignalMutationRunner struct {
	delegate       storeTestRuntimeMutationOwner
	afterCommitErr error
	rollbackErr    error
	beforeCommit   func() error
}

type standingSignalCoordinatorDispatcher struct {
	dispatched atomic.Int32
}

func (d *standingSignalCoordinatorDispatcher) DispatchDeliveryContinuation(context.Context, events.Event, events.DeliveryRoute) error {
	d.dispatched.Add(1)
	return nil
}

func (r *standingSignalMutationRunner) RunRuntimeMutationContext(ctx context.Context, fn func(context.Context) error) error {
	if r == nil || r.delegate == nil {
		return errors.New("standing signal mutation delegate is required")
	}
	err := r.delegate.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		if err := fn(txctx); err != nil {
			return err
		}
		if r.beforeCommit != nil {
			if err := r.beforeCommit(); err != nil {
				return err
			}
		}
		if r.rollbackErr != nil {
			return r.rollbackErr
		}
		return nil
	})
	if err == nil && r.afterCommitErr != nil {
		return r.afterCommitErr
	}
	return err
}

func TestStandingServiceTerminalizationSignalUsesCallbackTimeRegistrationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"commit", "rollback"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				process := worklifetime.NewProcess()
				occurrence := newRunLifecycleExecutorOccurrence(t, process)
				ctx := worklifetime.WithRuntimeOccurrence(testAuthorActivityRuntimeContext(), occurrence)
				t.Cleanup(func() {
					retireRunLifecycleExecutorOccurrence(t, occurrence)
					retireRunLifecycleProcess(t, process)
				})

				var (
					db            *sql.DB
					base          storeTestRuntimeMutationOwner
					selected      workflowTestSelectedStore
					deliveryStore runtimedelivery.Store
					workflowStore *runtimepipeline.PipelineCoordinator
					dialect       runtimeauthoractivity.Dialect
					adapter       *runtimedelivery.Adapter
				)
				if backend == "sqlite" {
					sqliteSelected := newBootstrappedSQLiteRuntimeStoreForTest(t)
					db, base, deliveryStore = sqliteSelected.DB, sqliteSelected, sqliteSelected
					selected = sqliteSelected
					dialect, adapter = runtimeauthoractivity.DialectSQLite, sqliteDeliveryAdapter
				} else {
					_, opened, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					postgresSelected := admitTestPostgresStore(t, opened)
					db, base, deliveryStore = opened, postgresSelected, postgresSelected
					selected = postgresSelected
					dialect, adapter = runtimeauthoractivity.DialectPostgres, postgresDeliveryAdapter
				}
				runner := &standingSignalMutationRunner{delegate: base}
				if backend == "sqlite" {
					workflowStore = newSQLiteWorkflowTestCoordinator(t, db, selected)
				} else {
					workflowStore = newPostgresWorkflowTestCoordinator(t, db, selected)
				}

				candidate := runtimepipeline.StandingServiceCandidate{
					ServiceID:  runtimeflowidentity.StandingServiceID("project", "signal-registration-order"),
					PackageKey: "project", FlowID: "signal-registration-order",
					InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
					Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("9", 64)),
				}
				seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
				created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
				if err != nil || len(created) != 1 {
					t.Fatalf("seed standing service = %#v, %v", created, err)
				}

				predecessorAuthority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-registration-owner", 1)
				if err != nil {
					t.Fatal(err)
				}
				successorAuthority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-registration-owner", 2)
				if err != nil {
					t.Fatal(err)
				}
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("standing-registration-node")}
				commit := func(eventType events.EventType, authority runtimedelivery.ExecutionAuthority) (events.Event, runtimedelivery.DurableHandoffProof) {
					evt := eventtest.RuntimeControl(uuid.NewString(), eventType, "test", candidate.EntityID, []byte(`{}`), 0, created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC())
					if err := eventfixture.Insert(ctx, db, dialect, evt); err != nil {
						t.Fatalf("insert registration-order event: %v", err)
					}
					var proofs []runtimedelivery.DurableHandoffProof
					run := func(txctx context.Context, tx *sql.Tx) error {
						var err error
						proofs, err = adapter.CommitInitial(txctx, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, authority)
						return err
					}
					if backend == "sqlite" {
						if err := deliveryStore.(*SQLiteRuntimeStore).runEventTransaction(ctx, run); err != nil {
							t.Fatalf("commit SQLite registration-order delivery: %v", err)
						}
					} else if err := deliveryStore.(*PostgresStore).runEventTransaction(ctx, run); err != nil {
						t.Fatalf("commit PostgreSQL registration-order delivery: %v", err)
					}
					return evt, proofs[0]
				}
				predecessorEvent, predecessorProof := commit("standing.signal.predecessor", predecessorAuthority)
				successorEvent, successorProof := commit("standing.signal.successor", successorAuthority)

				predecessorCoordinator, err := runtimedeliverycontinuation.New(deliveryStore, predecessorAuthority, occurrence, &standingSignalCoordinatorDispatcher{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				successorCoordinator, err := runtimedeliverycontinuation.New(deliveryStore, successorAuthority, occurrence, &standingSignalCoordinatorDispatcher{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, configured := range []struct {
					coordinator *runtimedeliverycontinuation.Coordinator
					proof       runtimedelivery.DurableHandoffProof
				}{{predecessorCoordinator, predecessorProof}, {successorCoordinator, successorProof}} {
					if err := configured.coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{configured.proof}); err != nil {
						t.Fatalf("accept registration-order handoff: %v", err)
					}
					if err := configured.coordinator.Start(ctx); err != nil {
						t.Fatalf("start registration-order coordinator: %v", err)
					}
					coordinator := configured.coordinator
					t.Cleanup(func() {
						retireCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()
						if err := coordinator.Retire(retireCtx); err != nil {
							t.Errorf("retire registration-order coordinator: %v", err)
						}
					})
				}
				predecessorDeliveryID, _ := runtimedelivery.DeliveryID(predecessorEvent.ID(), route)
				successorDeliveryID, _ := runtimedelivery.DeliveryID(successorEvent.ID(), route)
				predecessorCarrier, err := predecessorCoordinator.Acquire(predecessorDeliveryID)
				if err != nil {
					t.Fatalf("acquire predecessor carrier: %v", err)
				}

				var predecessorSignals, successorSignals atomic.Int32
				predecessorRegistration, err := workflowStore.RegisterDeliveryContinuationSignal(predecessorAuthority, func() {
					predecessorSignals.Add(1)
					predecessorCoordinator.Signal()
				})
				if err != nil {
					t.Fatal(err)
				}
				var successorRegistration *runtimepipeline.DeliveryContinuationSignalRegistration
				runner.beforeCommit = func() error {
					predecessorRegistration.Release()
					var err error
					successorRegistration, err = workflowStore.RegisterDeliveryContinuationSignal(successorAuthority, func() {
						successorSignals.Add(1)
						successorCoordinator.Signal()
					})
					return err
				}
				if outcome == "rollback" {
					runner.rollbackErr = errors.New("injected registration-order rollback")
				}
				operationErr := runner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
					_, err := workflowStore.SuspendStandingService(txctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"})
					return err
				})
				if outcome == "rollback" && !errors.Is(operationErr, runner.rollbackErr) {
					t.Fatalf("registration-order rollback = %v", operationErr)
				}
				if outcome == "commit" && operationErr != nil {
					t.Fatalf("registration-order commit: %v", operationErr)
				}
				if successorRegistration == nil {
					t.Fatal("successor registration was not installed before commit callback")
				}
				t.Cleanup(successorRegistration.Release)
				if got := predecessorSignals.Load(); got != 0 {
					t.Fatalf("released predecessor signals = %d, want 0", got)
				}
				wantSuccessorSignals := int32(1)
				if outcome == "rollback" {
					wantSuccessorSignals = 0
				}
				if got := successorSignals.Load(); got != wantSuccessorSignals {
					t.Fatalf("current successor signals = %d, want %d", got, wantSuccessorSignals)
				}

				if outcome == "rollback" {
					if resolution, err := predecessorCarrier.Resolve(ctx, worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
						t.Fatalf("rolled-back predecessor carrier = %d, %v", resolution, err)
					}
					successorCarrier, err := successorCoordinator.Acquire(successorDeliveryID)
					if err != nil {
						t.Fatalf("acquire rolled-back successor carrier: %v", err)
					}
					if resolution, err := successorCarrier.Resolve(ctx, worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
						t.Fatalf("rolled-back successor carrier = %d, %v", resolution, err)
					}
					return
				}

				awaitStandingTerminalResolution(t, successorCoordinator, successorDeliveryID, nil)
				resolution, err := predecessorCarrier.Resolve(ctx, worklifetime.DeliveryContinuationReturn)
				if err != nil {
					t.Fatalf("resolve unsignaled predecessor carrier: %v", err)
				}
				if resolution == worklifetime.DeliveryContinuationReturned {
					predecessorCoordinator.Signal()
					awaitStandingTerminalResolution(t, predecessorCoordinator, predecessorDeliveryID, nil)
				} else if resolution != worklifetime.DeliveryContinuationTerminal {
					t.Fatalf("unsignaled predecessor carrier = %d, want returned or terminal", resolution)
				}
			})
		}
	}
}

func TestStandingServiceTerminalizationBeforeRegistrationIsRecoveredByStartupScanParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			process := worklifetime.NewProcess()
			occurrence := newRunLifecycleExecutorOccurrence(t, process)
			ctx := worklifetime.WithRuntimeOccurrence(testAuthorActivityRuntimeContext(), occurrence)
			t.Cleanup(func() {
				retireRunLifecycleExecutorOccurrence(t, occurrence)
				retireRunLifecycleProcess(t, process)
			})

			var (
				db            *sql.DB
				selected      workflowTestSelectedStore
				deliveryStore runtimedelivery.Store
				workflowStore *runtimepipeline.PipelineCoordinator
				dialect       runtimeauthoractivity.Dialect
				adapter       *runtimedelivery.Adapter
			)
			if backend == "sqlite" {
				sqliteSelected := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db, deliveryStore = sqliteSelected.DB, sqliteSelected
				selected = sqliteSelected
				dialect, adapter = runtimeauthoractivity.DialectSQLite, sqliteDeliveryAdapter
			} else {
				_, opened, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				postgresSelected := admitTestPostgresStore(t, opened)
				db, deliveryStore = opened, postgresSelected
				selected = postgresSelected
				dialect, adapter = runtimeauthoractivity.DialectPostgres, postgresDeliveryAdapter
			}
			if backend == "sqlite" {
				workflowStore = newSQLiteWorkflowTestCoordinator(t, db, selected)
			} else {
				workflowStore = newPostgresWorkflowTestCoordinator(t, db, selected)
			}

			candidate := runtimepipeline.StandingServiceCandidate{
				ServiceID:  runtimeflowidentity.StandingServiceID("project", "signal-startup-order"),
				PackageKey: "project", FlowID: "signal-startup-order",
				InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
				Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("7", 64)),
			}
			seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
			created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
			if err != nil || len(created) != 1 {
				t.Fatalf("seed standing service = %#v, %v", created, err)
			}
			authority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-startup-owner", 1)
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("standing-startup-node")}
			evt := eventtest.RuntimeControl(uuid.NewString(), "standing.signal.startup", "test", candidate.EntityID, []byte(`{}`), 0, created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := eventfixture.Insert(ctx, db, dialect, evt); err != nil {
				t.Fatalf("insert startup-order event: %v", err)
			}
			var proofs []runtimedelivery.DurableHandoffProof
			commit := func(txctx context.Context, tx *sql.Tx) error {
				var err error
				proofs, err = adapter.CommitInitial(txctx, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, authority)
				return err
			}
			if backend == "sqlite" {
				err = deliveryStore.(*SQLiteRuntimeStore).runEventTransaction(ctx, commit)
			} else {
				err = deliveryStore.(*PostgresStore).runEventTransaction(ctx, commit)
			}
			if err != nil {
				t.Fatalf("commit startup-order delivery: %v", err)
			}

			if _, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"}); err != nil {
				t.Fatalf("terminalize before registration: %v", err)
			}

			coordinator, err := runtimedeliverycontinuation.New(deliveryStore, authority, occurrence, &standingSignalCoordinatorDispatcher{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.AcceptCommitted(proofs); err != nil {
				t.Fatalf("accept startup-order handoff: %v", err)
			}
			var signals atomic.Int32
			registration, err := workflowStore.RegisterDeliveryContinuationSignal(authority, func() {
				signals.Add(1)
				coordinator.Signal()
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Release)
			if err := coordinator.Start(ctx); err != nil {
				t.Fatalf("startup scan after pre-registration callback: %v", err)
			}
			t.Cleanup(func() {
				retireCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := coordinator.Retire(retireCtx); err != nil {
					t.Errorf("retire startup-order coordinator: %v", err)
				}
			})
			if got := signals.Load(); got != 0 {
				t.Fatalf("callback executed before registration replayed %d signals", got)
			}
			deliveryID, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Acquire(deliveryID); err == nil {
				t.Fatal("startup scan retained a terminal delivery continuation")
			}
		})
	}
}

func TestStandingServiceTerminalizationSignalFollowsTransactionOutcomeParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []string{"orphan", "replacement", "suspend", "reset"} {
			for _, outcome := range []string{"committed_cleanup_failure", "rollback"} {
				t.Run(backend+"/"+operation+"/"+outcome, func(t *testing.T) {
					process := worklifetime.NewProcess()
					occurrence := newRunLifecycleExecutorOccurrence(t, process)
					ctx := worklifetime.WithRuntimeOccurrence(testAuthorActivityRuntimeContext(), occurrence)
					t.Cleanup(func() {
						retireRunLifecycleExecutorOccurrence(t, occurrence)
						retireRunLifecycleProcess(t, process)
					})
					var (
						db             *sql.DB
						base           storeTestRuntimeMutationOwner
						selected       workflowTestSelectedStore
						deliveryStore  runtimedelivery.Store
						workflowStore  *runtimepipeline.PipelineCoordinator
						dialect        runtimeauthoractivity.Dialect
						commitDelivery func(events.Event, events.DeliveryRoute, runtimedelivery.ExecutionAuthority) []runtimedelivery.DurableHandoffProof
					)
					if backend == "sqlite" {
						sqliteSelected := newBootstrappedSQLiteRuntimeStoreForTest(t)
						db = sqliteSelected.DB
						base = sqliteSelected
						selected = sqliteSelected
						deliveryStore = sqliteSelected
						dialect = runtimeauthoractivity.DialectSQLite
						commitDelivery = func(evt events.Event, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) []runtimedelivery.DurableHandoffProof {
							if err := eventfixture.Insert(ctx, db, dialect, evt); err != nil {
								t.Fatalf("insert standing delivery event: %v", err)
							}
							var proofs []runtimedelivery.DurableHandoffProof
							if err := sqliteSelected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
								var err error
								proofs, err = sqliteDeliveryAdapter.CommitInitial(txctx, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, authority)
								return err
							}); err != nil {
								t.Fatalf("commit standing delivery: %v", err)
							}
							return proofs
						}
					} else {
						_, opened, cleanup := testutil.StartPostgres(t)
						t.Cleanup(cleanup)
						postgresSelected := admitTestPostgresStore(t, opened)
						db = opened
						base = postgresSelected
						selected = postgresSelected
						deliveryStore = postgresSelected
						dialect = runtimeauthoractivity.DialectPostgres
						commitDelivery = func(evt events.Event, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) []runtimedelivery.DurableHandoffProof {
							if err := eventfixture.Insert(ctx, db, dialect, evt); err != nil {
								t.Fatalf("insert standing delivery event: %v", err)
							}
							var proofs []runtimedelivery.DurableHandoffProof
							if err := postgresSelected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
								var err error
								proofs, err = postgresDeliveryAdapter.CommitInitial(txctx, tx, evt.ID(), evt.RunID(), []events.DeliveryRoute{route}, authority)
								return err
							}); err != nil {
								t.Fatalf("commit standing delivery: %v", err)
							}
							return proofs
						}
					}
					runner := &standingSignalMutationRunner{delegate: base}
					if backend == "sqlite" {
						workflowStore = newSQLiteWorkflowTestCoordinator(t, db, selected)
					} else {
						workflowStore = newPostgresWorkflowTestCoordinator(t, db, selected)
					}
					candidate := runtimepipeline.StandingServiceCandidate{
						ServiceID:  runtimeflowidentity.StandingServiceID("project", "signal-"+operation),
						PackageKey: "project", FlowID: "signal-" + operation,
						InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
						Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("8", 64)),
					}
					seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
					created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
					if err != nil || len(created) != 1 {
						t.Fatalf("seed standing service = %#v, %v", created, err)
					}
					var signals atomic.Int32
					authority, err := runtimedelivery.NewNormalExecutionAuthority(candidate.Source, "standing-signal-owner", 1)
					if err != nil {
						t.Fatalf("build delivery continuation signal authority: %v", err)
					}
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient("standing-signal-node")}
					coordinatorEvent := eventtest.RuntimeControl(
						uuid.NewString(), "standing.signal.coordinator", "test", candidate.EntityID, []byte(`{}`), 0,
						created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC(),
					)
					carrierEvent := eventtest.RuntimeControl(
						uuid.NewString(), "standing.signal.carrier", "test", candidate.EntityID, []byte(`{}`), 0,
						created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC(),
					)
					proofs := append(
						commitDelivery(coordinatorEvent, route, authority),
						commitDelivery(carrierEvent, route, authority)...,
					)
					dispatcher := &standingSignalCoordinatorDispatcher{}
					coordinator, err := runtimedeliverycontinuation.New(deliveryStore, authority, occurrence, dispatcher, nil)
					if err != nil {
						t.Fatalf("construct standing delivery coordinator: %v", err)
					}
					if err := coordinator.AcceptCommitted(proofs); err != nil {
						t.Fatalf("accept standing delivery handoffs: %v", err)
					}
					if err := coordinator.Start(ctx); err != nil {
						t.Fatalf("start standing delivery coordinator: %v", err)
					}
					t.Cleanup(func() {
						retireCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()
						if err := coordinator.Retire(retireCtx); err != nil {
							t.Errorf("retire standing delivery coordinator: %v", err)
						}
					})
					carrierDeliveryID, err := runtimedelivery.DeliveryID(carrierEvent.ID(), route)
					if err != nil {
						t.Fatalf("derive standing carrier delivery id: %v", err)
					}
					carrier, err := coordinator.Acquire(carrierDeliveryID)
					if err != nil {
						t.Fatalf("acquire standing carrier continuation: %v", err)
					}
					coordinatorDeliveryID, err := runtimedelivery.DeliveryID(coordinatorEvent.ID(), route)
					if err != nil {
						t.Fatalf("derive standing coordinator delivery id: %v", err)
					}
					registration, err := workflowStore.RegisterDeliveryContinuationSignal(authority, func() {
						signals.Add(1)
						coordinator.Signal()
					})
					if err != nil {
						t.Fatalf("register delivery continuation signal: %v", err)
					}
					t.Cleanup(registration.Release)
					injectedErr := errors.New("injected standing mutation outcome failure")
					if outcome == "rollback" {
						runner.rollbackErr = injectedErr
					} else {
						runner.afterCommitErr = injectedErr
					}
					err = runner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
						switch operation {
						case "orphan":
							_, err = workflowStore.ReconcileStandingServiceSet(txctx, nil)
						case "replacement":
							_, err = workflowStore.ReconcileStandingServiceReplacement(txctx, []runtimepipeline.StandingServiceCandidate{candidate}, nil)
						case "suspend":
							_, err = workflowStore.SuspendStandingService(txctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"})
						case "reset":
							_, err = workflowStore.ResetStandingService(txctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"})
						}
						return err
					})
					if !errors.Is(err, injectedErr) {
						t.Fatalf("terminalization error = %v, want injected outcome failure", err)
					}
					wantSignals := int32(1)
					if outcome == "rollback" {
						wantSignals = 0
					}
					if got := signals.Load(); got != wantSignals {
						t.Fatalf("delivery continuation signals = %d, want %d", got, wantSignals)
					}
					if outcome == "rollback" {
						if resolution, err := carrier.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
							t.Fatalf("rolled-back carrier resolution = %d, %v; want returned", resolution, err)
						}
						pending, err := coordinator.Acquire(coordinatorDeliveryID)
						if err != nil {
							t.Fatalf("rolled-back coordinator continuation was released: %v", err)
						}
						if resolution, err := pending.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil || resolution != worklifetime.DeliveryContinuationReturned {
							t.Fatalf("rolled-back coordinator resolution = %d, %v; want returned", resolution, err)
						}
					} else {
						awaitStandingTerminalResolution(t, coordinator, carrierDeliveryID, carrier)
						awaitStandingTerminalResolution(t, coordinator, coordinatorDeliveryID, nil)
						if err := coordinator.Release(carrierDeliveryID); err != nil {
							t.Fatalf("repeat carrier terminal release: %v", err)
						}
					}

					runner.afterCommitErr = nil
					runner.rollbackErr = nil
					statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
					if err != nil || len(statuses) != 1 {
						t.Fatalf("load standing service after outcome = %#v, %v", statuses, err)
					}
					if outcome == "rollback" {
						if !statuses[0].DeclarationPresent || statuses[0].EffectiveState != "active" || statuses[0].Generation != 1 {
							t.Fatalf("rolled-back terminalization leaked state: %#v", statuses[0])
						}
					} else if operation == "orphan" || operation == "replacement" {
						if statuses[0].DeclarationPresent || statuses[0].EffectiveState != "orphaned" {
							t.Fatalf("committed orphan terminalization missing: %#v", statuses[0])
						}
					} else if operation == "suspend" {
						if statuses[0].EffectiveState != "suspended" {
							t.Fatalf("committed suspend terminalization missing: %#v", statuses[0])
						}
					} else if statuses[0].Generation != 2 {
						t.Fatalf("committed reset generation = %d, want 2", statuses[0].Generation)
					}
				})
			}
		}
	}
}

func awaitStandingTerminalResolution(
	t *testing.T,
	coordinator *runtimedeliverycontinuation.Coordinator,
	deliveryID string,
	capability worklifetime.DeliveryContinuation,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if capability == nil {
			var err error
			capability, err = coordinator.Acquire(deliveryID)
			if err != nil {
				return
			}
		}
		resolution, err := capability.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn)
		if err != nil {
			t.Fatalf("resolve standing terminal continuation %s: %v", deliveryID, err)
		}
		if resolution == worklifetime.DeliveryContinuationTerminal {
			return
		}
		if resolution != worklifetime.DeliveryContinuationReturned {
			t.Fatalf("standing continuation %s resolved as %d, want returned or terminal", deliveryID, resolution)
		}
		if time.Now().After(deadline) {
			t.Fatalf("standing continuation %s did not observe terminalization", deliveryID)
		}
		capability = nil
		time.Sleep(time.Millisecond)
	}
}

func TestSQLiteStandingServiceReconcileCreatesPublishesAndRepairsRestartAbandon(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	packageKey := "project"
	flowID := "ingress"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	instanceID := uuid.NewString()
	entityID := uuid.NewString()
	firstHash := "bundle-v1:sha256:" + strings.Repeat("1", 64)
	secondHash := "bundle-v1:sha256:" + strings.Repeat("2", 64)
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: packageKey, FlowID: flowID,
		InstanceID: instanceID, EntityID: entityID,
		Source: mustStoreTestPersistedBundleSourceFact(firstHash),
	}
	seedStoreTestPersistedBundle(t, store.DB, firstHash)

	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService(create): %v", err)
	}
	if created.Transition != "created" || created.Generation != 1 || created.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 1) {
		t.Fatalf("created reconciliation = %#v", created)
	}
	sequence, err := workflowStore.PublishStandingService(ctx, serviceID, created.RunID, created.Generation)
	if err != nil || sequence != 1 {
		t.Fatalf("PublishStandingService = %d, %v", sequence, err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, fields, gates, accumulator, entered_state_at, created_at, updated_at)
		VALUES (?, ?, ?, 'default', 'ready', '{"name":"preserved"}', '{}', '{}', ?, ?, ?)
	`, created.RunID, entityID, "ingress/"+instanceID, time.Now().UTC(), time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if _, err := markRunTerminalStatusForTest(
		ctx, store, created.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC(),
	); err != nil {
		t.Fatalf("cancel standing run: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, stopped_at, updated_at) VALUES (?, 'stopped', 'server_restart_abandon', 'swarm.serve.abandon_active_runs', ?, ?)`, created.RunID, time.Now().UTC(), time.Now().UTC()); err != nil {
		t.Fatalf("seed restart-abandon provenance: %v", err)
	}
	candidate.Source = mustStoreTestPersistedBundleSourceFact(secondHash)
	seedStoreTestPersistedBundle(t, store.DB, secondHash)
	repaired, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService(repair): %v", err)
	}
	if repaired.Transition != "repaired" || repaired.Generation != 2 || repaired.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("repaired reconciliation = %#v", repaired)
	}
	var state, name string
	if err := store.DB.QueryRowContext(ctx, `SELECT current_state, json_extract(fields, '$.name') FROM entity_state WHERE run_id = ? AND entity_id = ?`, repaired.RunID, entityID).Scan(&state, &name); err != nil {
		t.Fatalf("load repaired entity state: %v", err)
	}
	if state != "ready" || name != "preserved" {
		t.Fatalf("repaired entity state = %s/%s", state, name)
	}
	var oldStatus, retiredReason string
	if err := store.DB.QueryRowContext(ctx, `
		SELECT r.status, COALESCE(g.retired_reason, '')
		FROM runs r JOIN standing_service_generations g ON g.run_id = r.run_id
		WHERE r.run_id = ?
	`, created.RunID).Scan(&oldStatus, &retiredReason); err != nil {
		t.Fatalf("load predecessor lineage: %v", err)
	}
	if oldStatus != "cancelled" || retiredReason != "server_restart_abandon" {
		t.Fatalf("predecessor = %s/%s", oldStatus, retiredReason)
	}
}

func TestSQLiteStandingServiceReconcileRejectsUnknownTerminalityWithCommand(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("3", 64)),
	}
	seedStoreTestPersistedBundle(t, store.DB, candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := markRunTerminalStatusForTest(
		ctx, store, created.RunID, string(runtimerunlifecycle.StateCancelled), nil, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	_, err = workflowStore.ReconcileStandingService(ctx, candidate)
	if err == nil || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("error = %v, want teaching reset command", err)
	}
}

func TestSQLiteStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("4", 64)),
	}
	seedStoreTestPersistedBundle(t, store.DB, candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workflowStore.PublishStandingService(ctx, serviceID, created.RunID, created.Generation); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	agentID := "standing-agent"
	sessionID := uuid.NewString()
	timerID := uuid.NewString()
	fixtureCtx := testAuthorActivityContextForBundle(candidate.Source.BundleHash())
	workEvent := eventtest.PersistedProjection(
		eventID, events.EventType("standing.work"), "test", "", json.RawMessage(`{}`), 0,
		created.RunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	identity := testAgentIdentity(t, agentID, "standing/ingress")
	fields := testAgentIdentityStorageFields(t, identity)
	workRoute := testAgentDeliveryRoute(t, agentID, "standing/ingress")
	if err := commitSemanticEventFixtureWithRoutes(fixtureCtx, store, workEvent, []events.DeliveryRoute{workRoute}); err != nil {
		t.Fatal(err)
	}
	if err := commitSemanticEventFixture(fixtureCtx, store, eventtest.PersistedProjection(
		unsettledEventID, events.EventType("standing.unsettled"), "test", "", json.RawMessage(`{}`), 0,
		created.RunID, "", events.EventEnvelope{}, time.Now().UTC(),
	)); err != nil {
		t.Fatal(err)
	}
	seedTestAgentRow(t, ctx, store.DB, false, identity, "active")
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, conversation, runtime_state, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'authored', '[]', '{}', 'active')
	`, sessionID, created.RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(fixtureCtx, store, workEvent, workRoute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BindAgentSession(fixtureCtx, claimed.Claim, sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB.ExecContext(ctx, `INSERT INTO timers (timer_id, timer_name, run_id, fire_event, routing_source, fire_at, owner_kind, status) VALUES (?, ?, ?, 'timer.fire', '{"kind":"platform_control","route":{}}', ?, 'system', 'active')`, timerID, aggregateWorkflowTimerTaskID(timerID), created.RunID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" || suspended.Transition != "suspended" {
		t.Fatalf("suspended = %#v", suspended)
	}
	var runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason, timerStatus string
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `
		SELECT status, reason_code
		FROM event_deliveries
		WHERE event_id = ? AND subscriber_type = 'agent' AND subscriber_id = ?
	`, eventID, agentID).Scan(&deliveryStatus, &deliveryReason); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status, termination_reason FROM agent_sessions WHERE session_id = ?`, sessionID).Scan(&sessionStatus, &sessionReason); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM timers WHERE timer_id = ?`, timerID).Scan(&timerStatus); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := store.DB.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || deliveryReason != "standing_suspended" || sessionStatus != "terminated" || sessionReason != "cancelled" || timerStatus != "cancelled" {
		t.Fatalf("suspend state = run:%s delivery:%s/%s session:%s/%s timer:%s", runStatus, deliveryStatus, deliveryReason, sessionStatus, sessionReason, timerStatus)
	}
	if pipelineOutcome != "dead_letter" || pipelineReason != "standing_suspended" {
		t.Fatalf("unsettled pipeline receipt = %s/%s", pipelineOutcome, pipelineReason)
	}
	statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil || len(statuses) != 1 {
		t.Fatalf("ListStandingServiceStatuses = %#v, %v", statuses, err)
	}
	if statuses[0].OverrideActor != "tester" || statuses[0].OverrideReason != "maintenance" || statuses[0].OverrideAt.IsZero() {
		t.Fatalf("suspended status = %#v", statuses[0])
	}

	resumed, err := workflowStore.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResumeStandingService: %v", err)
	}
	if resumed.EffectiveState != "active" || resumed.RunID != created.RunID || resumed.Generation != created.Generation {
		t.Fatalf("resumed = %#v", resumed)
	}

	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResetStandingService: %v", err)
	}
	if reset.Transition != "reset" || reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("reset = %#v", reset)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if runStatus != "cancelled" {
		t.Fatalf("reset predecessor status = %s, want cancelled", runStatus)
	}
}

func TestSQLiteStandingServiceSetOrphansRemovedDeclaration(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("5", 64)),
	}
	seedStoreTestPersistedBundle(t, store.DB, candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("create set = %#v, %v", created, err)
	}
	results, err := workflowStore.ReconcileStandingServiceSet(ctx, nil)
	if err != nil {
		t.Fatalf("orphan set: %v", err)
	}
	if len(results) != 1 || results[0].Transition != "orphaned" || results[0].EffectiveState != "orphaned" {
		t.Fatalf("orphan results = %#v", results)
	}
	var declarationPresent bool
	var effectiveState, runStatus string
	if err := store.DB.QueryRowContext(ctx, `SELECT declaration_present, effective_state FROM standing_services WHERE service_id = ?`, serviceID).Scan(&declarationPresent, &effectiveState); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if declarationPresent || effectiveState != "orphaned" || runStatus != "paused" {
		t.Fatalf("orphan state = declared:%t state:%s run:%s", declarationPresent, effectiveState, runStatus)
	}
}

func TestSQLiteStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	testStandingServiceReplacementIsScopedAndAtomic(t, store.DB, workflowStore)
}

func TestPostgresStandingServiceReplacementIsScopedAndAtomic(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := admitTestPostgresStore(t, db)
	workflowStore := newPostgresWorkflowTestCoordinator(t, db, selected)
	testStandingServiceReplacementIsScopedAndAtomic(t, db, workflowStore)
}

func testStandingServiceReplacementIsScopedAndAtomic(t *testing.T, db *sql.DB, workflowStore *runtimepipeline.PipelineCoordinator) {
	t.Helper()
	ctx := testAuthorActivityRuntimeContext()
	makeCandidate := func(flowID, hashDigit string) runtimepipeline.StandingServiceCandidate {
		return runtimepipeline.StandingServiceCandidate{
			ServiceID: runtimeflowidentity.StandingServiceID("project", flowID), PackageKey: "project", FlowID: flowID,
			InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
			Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat(hashDigit, 64)),
		}
	}
	retained := makeCandidate("retained", "1")
	removed := makeCandidate("removed", "2")
	unrelated := makeCandidate("unrelated", "3")
	for _, candidate := range []runtimepipeline.StandingServiceCandidate{retained, removed, unrelated} {
		seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
	}
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed, unrelated})
	if err != nil || len(created) != 3 {
		t.Fatalf("seed standing services = %#v, %v", created, err)
	}
	initialRunID := map[string]string{}
	for _, result := range created {
		initialRunID[result.ServiceID] = result.RunID
	}

	revised := retained
	revised.Source = mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("4", 64))
	seedStoreTestPersistedBundle(t, db, revised.Source.BundleHash())
	missing := makeCandidate("missing", "5")
	if _, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{missing}, []runtimepipeline.StandingServiceCandidate{revised}); err == nil || !strings.Contains(err.Error(), "is not persisted") {
		t.Fatalf("replacement with missing predecessor error = %v", err)
	}
	statuses, err := workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range statuses {
		if status.ServiceID == retained.ServiceID && status.BundleHash != retained.Source.BundleHash() {
			t.Fatalf("failed replacement leaked retained revision: %#v", status)
		}
	}

	added := makeCandidate("added", "6")
	seedStoreTestPersistedBundle(t, db, added.Source.BundleHash())
	results, err := workflowStore.ReconcileStandingServiceReplacement(ctx, []runtimepipeline.StandingServiceCandidate{retained, removed}, []runtimepipeline.StandingServiceCandidate{revised, added})
	if err != nil {
		t.Fatalf("ReconcileStandingServiceReplacement: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("replacement results = %#v, want revised, created, and orphaned", results)
	}
	statuses, err = workflowStore.ListStandingServiceStatuses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]runtimepipeline.StandingServiceStatus, len(statuses))
	for _, status := range statuses {
		byID[status.ServiceID] = status
	}
	if got := byID[retained.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != revised.Source.BundleHash() || got.RunID != initialRunID[retained.ServiceID] || got.Transition != "revised" {
		t.Fatalf("retained service = %#v", got)
	}
	if got := byID[removed.ServiceID]; got.DeclarationPresent || got.EffectiveState != "orphaned" || got.Transition != "orphaned" {
		t.Fatalf("removed service = %#v", got)
	}
	if got := byID[added.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.Transition != "created" {
		t.Fatalf("added service = %#v", got)
	}
	if got := byID[unrelated.ServiceID]; !got.DeclarationPresent || got.EffectiveState != "active" || got.BundleHash != unrelated.Source.BundleHash() || got.RunID != initialRunID[unrelated.ServiceID] {
		t.Fatalf("unrelated service = %#v", got)
	}
}

func TestPostgresStandingServiceOperatorLifecycleQuiescesAndPersistsDesiredState(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	selected := admitTestPostgresStore(t, db)
	workflowStore := newPostgresWorkflowTestCoordinator(t, db, selected)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress",
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("6", 64)),
	}
	seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
	fixtureCtx := testAuthorActivityContextForBundle(candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{candidate})
	if err != nil || len(created) != 1 {
		t.Fatalf("ReconcileStandingServiceSet = %#v, %v", created, err)
	}
	eventID := uuid.NewString()
	unsettledEventID := uuid.NewString()
	agentID := "standing-agent"
	timerID := uuid.NewString()
	var workEvent events.Event
	for _, fixture := range []struct {
		id        string
		eventType events.EventType
	}{
		{id: eventID, eventType: "standing.work"},
		{id: unsettledEventID, eventType: "standing.unsettled"},
	} {
		event := eventtest.PersistedProjection(
			fixture.id, fixture.eventType, "test", "", json.RawMessage(`{}`), 0,
			created[0].RunID, "", events.EventEnvelope{}, time.Now().UTC(),
		)
		if fixture.id == eventID {
			workEvent = event
			continue
		}
		if err := commitSemanticEventFixture(fixtureCtx, selected, event); err != nil {
			t.Fatal(err)
		}
	}
	identity := testAgentIdentity(t, agentID, "standing/ingress")
	fields := testAgentIdentityStorageFields(t, identity)
	workRoute := testAgentDeliveryRoute(t, agentID, "standing/ingress")
	if err := commitSemanticEventFixtureWithRoutes(fixtureCtx, selected, workEvent, []events.DeliveryRoute{workRoute}); err != nil {
		t.Fatal(err)
	}
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source, conversation, runtime_state, status
		) VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored', '[]', '{}', 'active')
	`, uuid.NewString(), created[0].RunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath); err != nil {
		t.Fatal(err)
	}
	if _, err := claimDeliveryFixture(fixtureCtx, selected, workEvent, workRoute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO timers (timer_id, timer_name, run_id, fire_event, routing_source, fire_at, owner_kind, status) VALUES ($1::uuid, $2, $3::uuid, 'timer.fire', '{"kind":"platform_control","route":{}}'::jsonb, $4, 'system', 'active')`, timerID, aggregateWorkflowTimerTaskID(timerID), created[0].RunID, time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	suspended, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester", Reason: "maintenance"})
	if err != nil {
		t.Fatalf("SuspendStandingService: %v", err)
	}
	if suspended.EffectiveState != "suspended" {
		t.Fatalf("suspended = %#v", suspended)
	}
	var runStatus, deliveryStatus, sessionStatus, timerStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, created[0].RunID).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM event_deliveries WHERE event_id = $1::uuid`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM agent_sessions WHERE run_id = $1::uuid`, created[0].RunID).Scan(&sessionStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM timers WHERE run_id = $1::uuid`, created[0].RunID).Scan(&timerStatus); err != nil {
		t.Fatal(err)
	}
	var pipelineOutcome, pipelineReason string
	if err := db.QueryRowContext(ctx, `SELECT outcome, reason_code FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, unsettledEventID).Scan(&pipelineOutcome, &pipelineReason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "paused" || deliveryStatus != "dead_letter" || sessionStatus != "terminated" || timerStatus != "cancelled" {
		t.Fatalf("suspend state = %s/%s/%s/%s", runStatus, deliveryStatus, sessionStatus, timerStatus)
	}
	if pipelineOutcome != "dead_letter" || pipelineReason != "standing_suspended" {
		t.Fatalf("unsettled pipeline receipt = %s/%s", pipelineOutcome, pipelineReason)
	}
	if _, err := workflowStore.ResumeStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"}); err != nil {
		t.Fatalf("ResumeStandingService: %v", err)
	}
	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "tester"})
	if err != nil {
		t.Fatalf("ResetStandingService: %v", err)
	}
	if reset.Generation != 2 || reset.RunID != runtimeflowidentity.StandingGenerationRunID(serviceID, 2) {
		t.Fatalf("reset = %#v", reset)
	}
}

func TestSQLiteRunStopRefusesCurrentStandingGenerationWithTeachingCommand(t *testing.T) {
	ctx := testAuthorActivityRuntimeContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	workflowStore := newSQLiteWorkflowTestCoordinator(t, store.DB, store)
	serviceID := runtimeflowidentity.StandingServiceID("project", "ingress")
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: "project", FlowID: "ingress", InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("7", 64)),
	}
	seedStoreTestPersistedBundle(t, store.DB, candidate.Source.BundleHash())
	created, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.StopRunControl(ctx, runtimeruncontrol.TransitionRequest{RunID: created.RunID})
	if err == nil || !strings.Contains(err.Error(), "swarm standing suspend "+serviceID) || !strings.Contains(err.Error(), "swarm standing reset "+serviceID) {
		t.Fatalf("StopRunControl error = %v", err)
	}
	var status string
	if err := store.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, created.RunID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("standing run status = %s, want running", status)
	}
}
