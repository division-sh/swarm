package runtimepersistence

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

type scheduleConsumerOutcomeStore struct {
	runtimegenericschedule.Store
	postCommitErr error
	calls         atomic.Int32
	reads         atomic.Int32
	commitErr     error
}

func (s *scheduleConsumerOutcomeStore) CommitGenericScheduleOccurrence(ctx context.Context, command runtimegenericschedule.CommitCommand) (runtimegenericschedule.CommitResult, error) {
	s.calls.Add(1)
	result, err := s.Store.CommitGenericScheduleOccurrence(ctx, command)
	if result.Outcome != "" {
		err = errors.Join(err, s.postCommitErr)
	}
	s.commitErr = err
	return result, err
}

func (s *scheduleConsumerOutcomeStore) LoadGenericScheduleActivation(ctx context.Context, id string) (runtimegenericschedule.Activation, bool, error) {
	s.reads.Add(1)
	return s.Store.LoadGenericScheduleActivation(ctx, id)
}

type scheduleConsumerOutcomePlanner struct {
	runtimegenericschedule.PublicationPlanner
	releases, finalizes int
}

func (p *scheduleConsumerOutcomePlanner) ReleaseEnginePublications(ctx context.Context, plans []runtimeengine.DurablePublicationPlan) error {
	p.releases++
	return p.PublicationPlanner.ReleaseEnginePublications(ctx, plans)
}

func (p *scheduleConsumerOutcomePlanner) FinalizeEnginePublications(ctx context.Context, plans []runtimeengine.CommittedDurablePublication) error {
	p.finalizes++
	return p.PublicationPlanner.FinalizeEnginePublications(ctx, plans)
}

func TestGenericScheduleSchedulerConsumesCommittedErrorOnBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		for _, fail := range []bool{false, true} {
			phase := "healthy"
			if fail {
				phase = "postcommit_error"
			}
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				selected, db, ctx := backend.open(t)
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
				bus, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
				if err != nil {
					t.Fatal(err)
				}
				store := &scheduleConsumerOutcomeStore{Store: selected}
				injected := errors.New("injected after selected schedule COMMIT")
				if fail {
					store.postCommitErr = injected
				}
				planner := &scheduleConsumerOutcomePlanner{PublicationPlanner: bus}
				scheduler := &selectedStoreLifecycleScheduler{}
				dispatcher := &terminalScheduleDispatcherProbe{}
				lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, planner, dispatcher, nil, executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := lifecycle.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				command := testRootGenericScheduleCommand(t, runtimecorrelation.RunIDFromContext(ctx), uuid.NewString(), "consumer-outcome", runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(-time.Second)))
				command.EventType = "test.node_emitted"
				admitted, err := lifecycle.Admit(ctx, command)
				if err != nil {
					t.Fatal(err)
				}
				if len(scheduler.registered) != 1 {
					t.Fatalf("wakeups=%d", len(scheduler.registered))
				}
				store.reads.Store(0)
				scheduler.callback(ctx, scheduler.registered[0])
				// handleWakeup must project the terminal result despite the error,
				// not start the generic mutation recovery loop.
				if planner.releases != 0 || planner.finalizes != 1 || dispatcher.calls != 1 || len(scheduler.retired) != 1 {
					t.Fatalf("releases=%d finalizes=%d dispatches=%d retired=%d commit_error=%v", planner.releases, planner.finalizes, dispatcher.calls, len(scheduler.retired), store.commitErr)
				}
				if err := lifecycle.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
				if store.calls.Load() != 1 || store.reads.Load() != 2 {
					t.Fatalf("commit calls=%d activation reads=%d, want one fire and one terminal projection", store.calls.Load(), store.reads.Load())
				}
				activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
				if err != nil || !found || activation.Status != runtimegenericschedule.StatusFired {
					t.Fatalf("activation=%+v found=%t error=%v", activation, found, err)
				}
				query := `SELECT COUNT(*) FROM events WHERE event_id=?`
				if backend.name == "postgres" {
					query = `SELECT COUNT(*) FROM events WHERE event_id=$1::uuid`
				}
				var count int
				if err := db.QueryRowContext(ctx, query, activation.CurrentEventID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("event rows=%d error=%v", count, err)
				}
			})
		}
	}
}
