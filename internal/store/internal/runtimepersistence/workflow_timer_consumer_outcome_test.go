package runtimepersistence

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type timerConsumerOutcomeStore struct {
	workflowTestSelectedStore
	injected   error
	calls      atomic.Int32
	reconciled chan runtimepipeline.WorkflowTimerActivation
}

func (s *timerConsumerOutcomeStore) LoadWorkflowTimerActivation(ctx context.Context, id string) (runtimepipeline.WorkflowTimerActivation, bool, error) {
	activation, found, err := s.workflowTestSelectedStore.LoadWorkflowTimerActivation(ctx, id)
	if s.calls.Load() > 0 && found && err == nil {
		select {
		case s.reconciled <- activation:
		default:
		}
	}
	return activation, found, err
}

func (s *timerConsumerOutcomeStore) CommitWorkflowTimerOccurrence(ctx context.Context, command runtimepipeline.WorkflowTimerOccurrenceCommand) (runtimepipeline.CommittedWorkflowTimerOccurrence, error) {
	s.calls.Add(1)
	result, err := s.workflowTestSelectedStore.CommitWorkflowTimerOccurrence(ctx, command)
	if result.Outcome == runtimepipeline.WorkflowTimerOccurrenceCommitted {
		err = errors.Join(err, s.injected)
	}
	return result, err
}

type timerConsumerOutcomeBus struct {
	*runtimebus.EventBus
	dispatched chan error
	finalizes  atomic.Int32
	releases   atomic.Int32
	errors     atomic.Int32
}

func (b *timerConsumerOutcomeBus) EngineDispatcher() runtimeengine.PostCommitDispatcher { return b }
func (b *timerConsumerOutcomeBus) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	err := b.EventBus.EngineDispatcher().DispatchPostCommit(ctx, intents)
	b.dispatched <- err
	return err
}
func (b *timerConsumerOutcomeBus) FinalizeEnginePublications(ctx context.Context, publications []runtimeengine.CommittedDurablePublication) error {
	b.finalizes.Add(1)
	return b.EventBus.FinalizeEnginePublications(ctx, publications)
}
func (b *timerConsumerOutcomeBus) ReleaseEnginePublications(ctx context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	b.releases.Add(1)
	return b.EventBus.ReleaseEnginePublications(ctx, plans)
}
func (b *timerConsumerOutcomeBus) LogRuntime(ctx context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	if entry.Action == "workflow_timer_fire_failed" {
		b.errors.Add(1)
	}
	return b.EventBus.LogRuntime(ctx, entry)
}

func TestWorkflowTimerSchedulerConsumesCommittedErrorOnBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		for _, fail := range []bool{false, true} {
			phase := "healthy"
			if fail {
				phase = "postcommit_error"
			}
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				selected, db, ctx := backend.open(t)
				ctx = authorGenericScheduleConsumerContext(runtimecorrelation.RunIDFromContext(ctx))
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
				store := &timerConsumerOutcomeStore{workflowTestSelectedStore: selected.(workflowTestSelectedStore), reconciled: make(chan runtimepipeline.WorkflowTimerActivation, 8)}
				if fail {
					store.injected = errors.New("injected timer cleanup after real COMMIT")
				}
				owner := storeTestWorkOwner(t)
				eventBus, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), runtimebus.EventBusOptions{WorkOwner: owner})
				if err != nil {
					t.Fatal(err)
				}
				bus := &timerConsumerOutcomeBus{EventBus: eventBus, dispatched: make(chan error, 8)}
				scheduler := runtimepipeline.NewSchedulerWithWorkOwner(owner)
				bundle := runControlTimerBundle()
				bundle.Semantics.Timers[0].Event = "test.node_emitted"
				bundle.Semantics.Timers[0].AdvancesTo = ""
				bundle.Semantics.Timers[0].Recurring = true
				options := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(store), store)
				options.Module = runControlTimerWorkflowModule{source: semanticview.Wrap(bundle)}
				options.TimerScheduler, options.WorkOwner = scheduler, owner
				coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, options)
				if coordinator == nil {
					t.Fatal("construct timer coordinator")
				}
				t.Cleanup(func() { _ = coordinator.StopWorkflowTimerLifecycle(context.Background()); scheduler.Stop() })
				runID := runtimecorrelation.RunIDFromContext(ctx)
				scope := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.RouteForInstancePath(runID)}
				enteredAt := time.Now().UTC().Add(-time.Hour - time.Second)
				_, err = coordinator.MaterializeInitialEntry(ctx, scope, runtimepipeline.WorkflowInstance{
					InstanceID: runID, StorageRef: runID, EntityID: uuid.NewString(), WorkflowName: ".", WorkflowVersion: "1",
					CurrentState: "waiting", EnteredStageAt: enteredAt, CreatedAt: enteredAt, EntityType: "test_entity",
				}, enteredAt)
				if err != nil {
					t.Fatal(err)
				}
				if err := coordinator.ArmInitialEntryTimers(ctx, scope); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-bus.dispatched:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("timer callback did not dispatch: commits=%d logged_errors=%d", store.calls.Load(), bus.errors.Load())
				}
				select {
				case next := <-store.reconciled:
					if next.Status != "active" || !next.FireAt.After(time.Now()) {
						t.Fatalf("recurrence not projected: %+v", next)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("committed recurrence was not reloaded by scheduler consumer")
				}
				if err := coordinator.StopWorkflowTimerLifecycle(context.Background()); err != nil {
					t.Fatal(err)
				}
				if store.calls.Load() != 1 || bus.finalizes.Load() != 1 || bus.releases.Load() != 0 {
					t.Fatalf("commits=%d finalizes=%d releases=%d", store.calls.Load(), bus.finalizes.Load(), bus.releases.Load())
				}
				wantErrors := int32(0)
				if fail {
					wantErrors = 1
				}
				if bus.errors.Load() != wantErrors {
					t.Fatalf("logged errors=%d want=%d", bus.errors.Load(), wantErrors)
				}
				query := `SELECT COUNT(*) FROM events WHERE run_id=? AND event_name='test.node_emitted'`
				if backend.name == "postgres" {
					query = `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name='test.node_emitted'`
				}
				var count int
				if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("event rows=%d error=%v", count, err)
				}
			})
		}
	}
}
