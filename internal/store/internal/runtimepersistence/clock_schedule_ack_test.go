package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/google/uuid"
)

type clockAdmissionReceiptFault struct {
	genericschedule.Store
	calls int
	fault error
}

type instanceClockReceiptDispatcher struct {
	engine.PostCommitDispatcher
	handoff engine.CommittedPublicationDispatcher
	calls   int
}

func (d *instanceClockReceiptDispatcher) DispatchCommittedPublication(ctx context.Context, publication engine.CommittedDurablePublication) error {
	d.calls++
	return d.handoff.DispatchCommittedPublication(ctx, publication)
}

func TestInstanceClockSchedulerConsumesCommittedReceiptBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		for _, phase := range []string{"healthy", "preparation_postcommit_error", "settlement_postcommit_error", "cancel_after_commit"} {
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				selected, _, seed := backend.open(t)
				ctx := clockAcknowledgmentContext(t, seed)
				runID := correlation.RunIDFromContext(ctx)
				registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
				publisher, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
				if err != nil {
					t.Fatal(err)
				}
				store := &scheduleConsumerOutcomeStore{Store: selected}
				injected := errors.New("instance occurrence committed with follow-up diagnostic")
				if phase == "preparation_postcommit_error" {
					store.preparePostCommitErr = injected
				}
				if phase == "settlement_postcommit_error" {
					store.postCommitErr = injected
				}
				fireCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				if phase == "cancel_after_commit" {
					store.afterCommit = cancel
				}
				planner := &scheduleConsumerOutcomePlanner{PublicationPlanner: publisher}
				dispatch := publisher.EngineDispatcher()
				dispatcher := &instanceClockReceiptDispatcher{PostCommitDispatcher: dispatch, handoff: dispatch.(engine.CommittedPublicationDispatcher)}
				scheduler := &selectedStoreLifecycleScheduler{}
				lifecycle, err := genericschedule.NewLifecycle(store, scheduler, planner, dispatcher, nil, executionposture.Live)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := lifecycle.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				})
				source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
				if err != nil {
					t.Fatal(err)
				}
				setGenericScheduleClock(t, selected, func() time.Time { return time.Now().UTC().Add(-time.Minute) })
				admitted, err := lifecycle.Admit(ctx, genericschedule.AdmissionCommand{
					ScheduleKey: "poll", RunID: runID, FlowInstance: runID, OwnerKind: genericschedule.OwnerInstance, OwnerID: ".",
					EventType: "test.node_emitted", Payload: semanticvalue.EmptyObject(), RoutingSource: source,
					ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Minute),
				})
				if err != nil || len(scheduler.registered) != 1 {
					t.Fatalf("instance clock admission=%+v registered=%+v err=%v", admitted, scheduler.registered, err)
				}
				setGenericScheduleClock(t, selected, time.Now)
				wake := scheduler.registered[0]
				scheduler.callback(fireCtx, wake)
				if planner.releases != 0 || planner.finalizes != 1 || dispatcher.calls != 1 || store.calls.Load() != 1 || len(scheduler.registered) != 2 {
					t.Fatalf("receipt projection releases/finalizes/handoffs/commits/wakes=%d/%d/%d/%d/%d error=%v", planner.releases, planner.finalizes, dispatcher.calls, store.calls.Load(), len(scheduler.registered), store.commitErr)
				}
				before, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
				if err != nil || !found || !before.CurrentDueAt.Equal(wake.DueAt().Add(time.Minute)) || before.Status != genericschedule.StatusActive {
					t.Fatalf("committed recurrence=%+v found=%t err=%v", before, found, err)
				}
				if phase == "cancel_after_commit" && !errors.Is(fireCtx.Err(), context.Canceled) {
					t.Fatal("receipt test did not cancel the committing caller")
				}
				scheduler.callback(ctx, wake)
				after, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
				if err != nil || !found || !reflect.DeepEqual(before, after) || planner.finalizes != 1 || dispatcher.calls != 1 || store.calls.Load() != 1 {
					t.Fatalf("old wake resent or advanced accepted occurrence: before=%+v after=%+v handoffs=%d err=%v", before, after, dispatcher.calls, err)
				}
				eventID := genericschedule.OccurrenceEventID(admitted.Activation.ID, wake.DueAt())
				read, found, err := selected.(bus.PreparedPublishEventReader).LoadPreparedPublishEvent(ctx, eventID)
				if err != nil || !found || read.Event.Class() != events.EventAdmissionInstancePublication || read.Event.Event().RoutingSource() != source {
					t.Fatalf("accepted clock publication lost exact authority=%+v found=%t err=%v", read, found, err)
				}
			})
		}
	}
}

func (s *clockAdmissionReceiptFault) AdmitGenericScheduleOutcome(ctx context.Context, command genericschedule.AdmissionCommand) (genericschedule.AdmissionCommit, error) {
	s.calls++
	commit, err := s.Store.AdmitGenericScheduleOutcome(ctx, command)
	if s.calls == 2 && commit.Acknowledged {
		return genericschedule.AdmissionCommit{}, errors.Join(err, s.fault)
	}
	return commit, err
}

func clockAcknowledgmentContext(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	process := worklifetime.NewProcess()
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("clock receipt proof requires its admitted source")
	}
	runtime, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	standing, err := runtime.NewStanding(ctx, worklifetime.StandingIdentity{ServiceID: uuid.NewString(), RunID: correlation.RunIDFromContext(ctx), Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := standing.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Done(); err != nil {
			t.Error(err)
		}
		join, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := standing.RetireAndWait(join); err != nil {
			t.Error(err)
		}
		if _, err := runtime.RetireAndWait(join); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(join); err != nil {
			t.Error(err)
		}
	})
	return lease.Context()
}

func TestInstanceClockPartialAdmissionAndLostAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, seedCtx := backend.open(t)
			ctx := clockAcknowledgmentContext(t, seedCtx)
			runID := correlation.RunIDFromContext(ctx)
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: runID})
			if err != nil {
				t.Fatal(err)
			}
			command := genericschedule.AdmissionCommand{ScheduleKey: "first", RunID: runID, FlowInstance: runID,
				OwnerKind: genericschedule.OwnerInstance, OwnerID: ".", EventType: "poll.tick", Payload: semanticvalue.EmptyObject(),
				RoutingSource: source, ExecutionMode: executionmode.Live, Due: genericschedule.EveryDue(time.Hour)}
			fault := errors.New("lost second clock admission receipt after durable COMMIT")
			store := &clockAdmissionReceiptFault{Store: selected, fault: fault}
			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := genericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := lifecycle.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			})
			first, err := lifecycle.Admit(ctx, command)
			if err != nil || first.Outcome != genericschedule.AdmissionCreated || len(scheduler.registered) != 1 {
				t.Fatalf("first clock admission=%+v registered=%+v err=%v", first, scheduler.registered, err)
			}
			command.ScheduleKey = "second"
			unknown, err := lifecycle.Admit(ctx, command)
			if !errors.Is(err, fault) || !reflect.DeepEqual(unknown, genericschedule.AdmissionResult{}) || len(scheduler.registered) != 1 {
				t.Fatalf("lost receipt acquired execution authority: result=%+v registered=%+v err=%v", unknown, scheduler.registered, err)
			}
			if count, err := CountInstanceClockActivationsForTest(ctx, selected); err != nil || count != 2 {
				t.Fatalf("partial admission was silently rolled back/replaced: count=%d err=%v", count, err)
			}
			stored, err := selected.ListActiveGenericScheduleActivations(ctx)
			if err != nil || len(stored) != 2 {
				t.Fatalf("exact persisted partial admission=%+v err=%v", stored, err)
			}
			var second genericschedule.Activation
			for _, activation := range stored {
				if activation.Command.ScheduleKey == "second" {
					second = activation
				}
			}
			replay, err := lifecycle.Admit(ctx, command)
			if err != nil || replay.Outcome != genericschedule.AdmissionExactReplay || !reflect.DeepEqual(replay.Activation, second) || len(scheduler.registered) != 2 {
				t.Fatalf("receipt recovery rearmed/replaced the clock: result=%+v stored=%+v registered=%+v err=%v", replay, second, scheduler.registered, err)
			}
			command.ScheduleKey = "invalid"
			command.RunID = uuid.NewString()
			if _, err := lifecycle.Admit(ctx, command); err == nil {
				t.Fatal("foreign generation acquired admission authority")
			}
			if count, err := CountInstanceClockActivationsForTest(ctx, selected); err != nil || count != 2 || len(scheduler.registered) != 2 {
				t.Fatalf("refused sibling mutated storage/wakeups: count=%d registered=%+v err=%v", count, scheduler.registered, err)
			}
		})
	}
}
