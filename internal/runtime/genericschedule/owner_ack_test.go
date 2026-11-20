package genericschedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type unacknowledgedScheduleStore struct {
	Store
	activation Activation
	err        error
}

type ordinaryOnlyClockDispatcher struct{ calls int }

type heldInstanceClockPreparation struct {
	*lifecycleProofStore
	entered chan struct{}
	release chan struct{}
	cause   error
}

func (s *heldInstanceClockPreparation) PrepareGenericScheduleOccurrence(ctx context.Context, _ Wakeup) (PreparationCommit, error) {
	close(s.entered)
	select {
	case <-s.release:
		return PreparationCommit{}, s.cause
	case <-ctx.Done():
		return PreparationCommit{}, ctx.Err()
	}
}

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

func TestInstanceClockFireRefusesBeforePreparationUnderRunFence(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	owner, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
	ctx := worklifetime.WithOccurrence(correlation.WithRunID(t.Context(), activation.Command.RunID), owner)
	store := &lifecycleProofStore{activation: activation}
	lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
	wakeup, err := activation.Wakeup()
	if err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	release, err := owner.BeginRunTransition(wait, activation.Command.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := lifecycle.fire(ctx, wakeup); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("fenced instance fire = %v, want native admission refusal", err)
	}
	if store.prepareCalls != 0 {
		t.Fatalf("fenced instance fire prepared %d durable occurrences", store.prepareCalls)
	}
	release()
	if _, err := lifecycle.fire(ctx, wakeup); err == nil || store.prepareCalls != 1 {
		t.Fatalf("released instance fire did not reach preparation: calls=%d err=%v", store.prepareCalls, err)
	}
}

func TestInstanceClockAdmittedFireJoinsRunTransition(t *testing.T) {
	activation := instanceRecoveryActivation(t)
	owner, _ := instanceRecoveryWorkOwner(t, activation.Command.RunID)
	ctx := worklifetime.WithOccurrence(correlation.WithRunID(t.Context(), activation.Command.RunID), owner)
	cause := errors.New("held instance clock preparation")
	store := &heldInstanceClockPreparation{
		lifecycleProofStore: &lifecycleProofStore{activation: activation},
		entered:             make(chan struct{}), release: make(chan struct{}), cause: cause,
	}
	released := false
	defer func() {
		if !released {
			close(store.release)
		}
	}()
	lifecycle, err := NewLifecycle(store, &lifecycleProofScheduler{}, &lifecycleProofPlanner{}, &lifecycleProofDispatcher{}, nil, executionposture.Live)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopLifecycleProof(t, lifecycle) })
	wakeup, err := activation.Wakeup()
	if err != nil {
		t.Fatal(err)
	}
	fired := make(chan error, 1)
	go func() {
		_, err := lifecycle.fire(ctx, wakeup)
		fired <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("clock fire never entered native preparation")
	}
	transitionCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	type transitionResult struct {
		done func()
		err  error
	}
	joined := make(chan transitionResult, 1)
	go func() {
		done, err := owner.BeginRunTransition(transitionCtx, activation.Command.RunID)
		joined <- transitionResult{done: done, err: err}
	}()
	select {
	case result := <-joined:
		if result.done != nil {
			result.done()
		}
		t.Fatalf("run transition crossed an admitted clock fire: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	released = true
	if err := <-fired; !errors.Is(err, cause) {
		t.Fatalf("clock fire lost its original preparation failure: %v", err)
	}
	result := <-joined
	if result.err != nil || result.done == nil {
		t.Fatalf("run transition did not join the clock fire: %v", result.err)
	}
	result.done()
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
		lease, err := standing.Begin(correlation.WithRunID(t.Context(), activation.Command.RunID))
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
