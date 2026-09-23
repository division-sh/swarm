package genericschedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type unacknowledgedScheduleStore struct {
	Store
	activation Activation
	err        error
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
