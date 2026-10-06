package genericschedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type unacknowledgedScheduleStore struct {
	Store
	activation Activation
	err        error
}

type ordinaryOnlyClockDispatcher struct{ calls int }

func (d *ordinaryOnlyClockDispatcher) DispatchPostCommit(context.Context, []engine.EmitIntent) error {
	d.calls++
	return nil
}

func TestInstanceClockRequiresHandoffOwnerBeforeAdmissionOrOccurrence(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	standing, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
	lease, err := standing.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Done() }()
	var order []string
	store := &lifecycleProofStore{activation: activation, order: &order}
	planner := &lifecycleProofPlanner{}
	dispatcher := &ordinaryOnlyClockDispatcher{}
	scheduler := &lifecycleProofScheduler{}
	lifecycle, err := NewLifecycle(store, scheduler, planner, dispatcher, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	defer stopLifecycleProof(t, lifecycle)
	if _, err := lifecycle.Admit(lease.Context(), activation.Command); err == nil || len(order) != 0 || len(scheduler.registered) != 0 {
		t.Fatalf("missing handoff owner admitted a clock: order=%v err=%v", order, err)
	}
	wake, err := activation.Wakeup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycle.fire(lease.Context(), wake); err == nil || store.commitCalls != 0 || planner.prepareCalls != 0 || dispatcher.calls != 0 {
		t.Fatalf("restored clock bypassed handoff admission: commits=%d prepares=%d dispatches=%d err=%v", store.commitCalls, planner.prepareCalls, dispatcher.calls, err)
	}
}

func (s *unacknowledgedScheduleStore) AdmitGenericScheduleOutcome(context.Context, AdmissionCommand) (AdmissionCommit, error) {
	return AdmissionCommit{Result: AdmissionResult{Outcome: AdmissionCreated, Activation: s.activation}}, s.err
}

func (s *unacknowledgedScheduleStore) CancelGenericScheduleOutcome(context.Context, CancelCommand) (CancelCommit, error) {
	return CancelCommit{Result: CancelResult{Outcome: CancelChanged, Activation: s.activation}}, s.err
}

func TestLifecycleRejectsUnacknowledgedScheduleValuesWithoutProjection(t *testing.T) {
	now := time.Now().UTC()
	activation := testGlobalActivation(t, AbsoluteDue(now.Add(time.Hour)), now, now.Add(time.Hour))
	fault := errors.New("commit not acknowledged")
	store := &unacknowledgedScheduleStore{Store: &lifecycleProofStore{activation: activation}, activation: activation, err: fault}
	scheduler := &lifecycleProofScheduler{}
	lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lifecycle.Stop(context.Background()) })

	admitted, err := lifecycle.Admit(context.Background(), activation.Command)
	if !errors.Is(err, fault) || admitted.Outcome != "" || len(scheduler.registered) != 0 {
		t.Fatalf("unacknowledged admission=%+v err=%v registered=%+v", admitted, err, scheduler.registered)
	}
	cancelled, err := lifecycle.Cancel(context.Background(), CancelCommand{ActivationID: activation.ID, Cause: "test", CancelledAt: now})
	if !errors.Is(err, fault) || cancelled.Outcome != "" || len(scheduler.retired) != 0 {
		t.Fatalf("unacknowledged cancellation=%+v err=%v retired=%+v", cancelled, err, scheduler.retired)
	}
}

func TestInstanceClockUnacknowledgedAdmissionCannotProjectWakeup(t *testing.T) {
	fault := errors.New("instance clock commit acknowledgment lost")
	for _, commitErr := range []error{nil, fault} {
		activation := instanceRecoveryActivation(t)
		standing, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
		lease, err := standing.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := lease.Done(); err != nil {
				t.Error(err)
			}
		}()
		store := &unacknowledgedScheduleStore{Store: &lifecycleProofStore{activation: activation}, activation: activation, err: commitErr}
		scheduler := &lifecycleProofScheduler{}
		lifecycle, err := NewLifecycle(store, scheduler, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
		if err != nil {
			t.Fatal(err)
		}
		defer stopLifecycleProof(t, lifecycle)
		result, admitErr := lifecycle.Admit(lease.Context(), activation.Command)
		if admitErr == nil || result.Outcome != "" || len(scheduler.registered) != 0 ||
			(commitErr != nil && !errors.Is(admitErr, fault)) {
			t.Fatalf("unacknowledged instance clock projected authority: result=%+v err=%v registered=%+v", result, admitErr, scheduler.registered)
		}
	}
}
