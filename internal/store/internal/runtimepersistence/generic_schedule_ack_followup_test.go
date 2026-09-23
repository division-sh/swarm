package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/google/uuid"
)

type postCommitScheduleFaultStore struct {
	runtimegenericschedule.Store
	fault    error
	acks     int
	onAck    func()
	loadErrs chan error
}

func (s *postCommitScheduleFaultStore) AdmitGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error) {
	commit, err := s.Store.AdmitGenericScheduleOutcome(ctx, command)
	if commit.Acknowledged {
		s.acks++
		err = errors.Join(err, s.fault)
		if s.onAck != nil {
			s.onAck()
		}
	}
	return commit, err
}

func (s *postCommitScheduleFaultStore) CancelGenericScheduleOutcome(ctx context.Context, command runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelCommit, error) {
	commit, err := s.Store.CancelGenericScheduleOutcome(ctx, command)
	if commit.Acknowledged {
		s.acks++
		err = errors.Join(err, s.fault)
		if s.onAck != nil {
			s.onAck()
		}
	}
	return commit, err
}

func (s *postCommitScheduleFaultStore) LoadGenericScheduleActivation(ctx context.Context, activationID string) (runtimegenericschedule.Activation, bool, error) {
	if s.loadErrs != nil {
		select {
		case s.loadErrs <- ctx.Err():
		default:
		}
	}
	return s.Store.LoadGenericScheduleActivation(ctx, activationID)
}

func TestGenericScheduleAcknowledgedCleanupFaultPreservesFollowUpBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, seedCtx := backend.open(t)
			ctx := authorGenericScheduleConsumerContext(runtimecorrelation.RunIDFromContext(seedCtx))
			fault := errors.New("injected after acknowledged schedule COMMIT")
			store := &postCommitScheduleFaultStore{Store: selected, fault: fault}
			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })

			command := testRootGenericScheduleCommand(t, runtimecorrelation.RunIDFromContext(ctx), uuid.NewString(), "schedule-ack", runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)))
			admitted, err := lifecycle.Admit(ctx, command)
			if !errors.Is(err, fault) || admitted.Outcome != runtimegenericschedule.AdmissionCreated || len(scheduler.registered) != 1 || scheduler.registered[0].ActivationID() != admitted.Activation.ID {
				t.Fatalf("admission=%+v err=%v registered=%+v", admitted, err, scheduler.registered)
			}
			cancelled, err := lifecycle.Cancel(ctx, runtimegenericschedule.CancelCommand{ActivationID: admitted.Activation.ID, Cause: "ack_fault_test", CancelledAt: time.Now().UTC()})
			if !errors.Is(err, fault) || cancelled.Outcome != runtimegenericschedule.CancelChanged || len(scheduler.retired) != 1 || scheduler.retired[0].ActivationID() != admitted.Activation.ID {
				t.Fatalf("cancellation=%+v err=%v retired=%+v", cancelled, err, scheduler.retired)
			}
			if store.acks != 2 {
				t.Fatalf("acknowledged operations=%d, want 2", store.acks)
			}
			activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
			if err != nil || !found || activation.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("durable cancellation=%+v found=%v err=%v", activation, found, err)
			}
		})
	}
}

func TestGenericScheduleAcknowledgedCommitReconcilesAfterCallerCancellationBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, seedCtx := backend.open(t)
			ctx := authorGenericScheduleConsumerContext(runtimecorrelation.RunIDFromContext(seedCtx))
			fault := errors.New("post-commit handoff failed")
			store := &postCommitScheduleFaultStore{Store: selected, fault: fault, loadErrs: make(chan error, 2)}
			scheduler := &selectedStoreLifecycleScheduler{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(store, scheduler, &terminalSchedulePlannerProbe{}, &terminalScheduleDispatcherProbe{}, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })

			admitCtx, cancelAdmit := context.WithCancel(ctx)
			store.onAck = cancelAdmit
			command := testRootGenericScheduleCommand(t, runtimecorrelation.RunIDFromContext(ctx), uuid.NewString(), "schedule-cancel-after-ack", runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)))
			admitted, err := lifecycle.Admit(admitCtx, command)
			if !errors.Is(err, fault) || !errors.Is(admitCtx.Err(), context.Canceled) || admitted.Outcome != runtimegenericschedule.AdmissionCreated || len(scheduler.registered) != 1 {
				t.Fatalf("admission=%+v err=%v context=%v registered=%+v", admitted, err, admitCtx.Err(), scheduler.registered)
			}
			select {
			case loadErr := <-store.loadErrs:
				if loadErr != nil {
					t.Fatalf("post-admission load used canceled caller context: %v", loadErr)
				}
			default:
				t.Fatal("acknowledged admission did not load activation for reconciliation")
			}

			cancelCtx, cancelCancel := context.WithCancel(ctx)
			store.onAck = cancelCancel
			cancelled, err := lifecycle.Cancel(cancelCtx, runtimegenericschedule.CancelCommand{ActivationID: admitted.Activation.ID, Cause: "ack_cancel_test", CancelledAt: time.Now().UTC()})
			if !errors.Is(err, fault) || !errors.Is(cancelCtx.Err(), context.Canceled) || cancelled.Outcome != runtimegenericschedule.CancelChanged || len(scheduler.retired) != 1 {
				t.Fatalf("cancellation=%+v err=%v context=%v retired=%+v", cancelled, err, cancelCtx.Err(), scheduler.retired)
			}
			select {
			case loadErr := <-store.loadErrs:
				if loadErr != nil {
					t.Fatalf("post-cancellation load used canceled caller context: %v", loadErr)
				}
			default:
				t.Fatal("acknowledged cancellation did not load activation for reconciliation")
			}
		})
	}
}
