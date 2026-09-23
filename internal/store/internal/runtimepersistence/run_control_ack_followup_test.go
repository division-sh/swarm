package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/google/uuid"
)

type postCommitRunControlFaultStore struct {
	runtimeruncontrol.Store
	fault error
	acks  int
}

func (s *postCommitRunControlFaultStore) StopRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	outcome, err := s.Store.StopRunControlOutcome(ctx, req)
	if outcome.Acknowledged {
		s.acks++
		err = errors.Join(err, s.fault)
	}
	return outcome, err
}

func (s *postCommitRunControlFaultStore) PauseRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	outcome, err := s.Store.PauseRunControlOutcome(ctx, req)
	if outcome.Acknowledged {
		s.acks++
		err = errors.Join(err, s.fault)
	}
	return outcome, err
}

func (s *postCommitRunControlFaultStore) ContinueRunControlOutcome(ctx context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.StoreTransition, error) {
	outcome, err := s.Store.ContinueRunControlOutcome(ctx, req)
	if outcome.Acknowledged {
		s.acks++
		err = errors.Join(err, s.fault)
	}
	return outcome, err
}

type postCommitRunControlQueue struct{ released []string }

func (*postCommitRunControlQueue) PreflightRunQueue(context.Context, string) error { return nil }
func (*postCommitRunControlQueue) BeginRunStop(context.Context, string) (runtimeruncontrol.StopTransition, error) {
	return nil, errors.New("unexpected stop queue transition")
}
func (q *postCommitRunControlQueue) ReleaseRunQueue(_ context.Context, runID string, _ int) (runtimepipelineobligation.SweepResult, error) {
	q.released = append(q.released, runID)
	return runtimepipelineobligation.SweepResult{Exhausted: true}, nil
}

type postCommitTimerRecorder struct {
	refs []runtimetimercancellation.Ref
}

func (r *postCommitTimerRecorder) Reconcile(_ context.Context, refs []runtimetimercancellation.Ref) error {
	r.refs = append(r.refs, refs...)
	return nil
}

func TestRunControlAcknowledgedCleanupFaultPreservesFollowUpBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, seedCtx := backend.open(t)
			registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
			runID := runtimecorrelation.RunIDFromContext(seedCtx)
			ctx := authorGenericScheduleConsumerContext(runID)
			fault := errors.New("injected after acknowledged run control COMMIT")
			store := &postCommitRunControlFaultStore{Store: selected.(runtimeruncontrol.Store), fault: fault}
			request := runtimeruncontrol.TransitionRequest{RunID: runID, Reason: "ack_fault", ControlledBy: "test", Now: time.Now().UTC()}

			paused, err := runtimeruncontrol.NewController(store, nil, runtimeruncontrol.Options{}).Pause(ctx, request)
			if !errors.Is(err, fault) || paused.RunID != runID || paused.Status != runtimeruncontrol.StatusPaused {
				t.Fatalf("pause result=%+v err=%v", paused, err)
			}
			queue := &postCommitRunControlQueue{}
			continued, err := runtimeruncontrol.NewController(store, queue, runtimeruncontrol.Options{}).Continue(ctx, request)
			if !errors.Is(err, fault) || continued.RunID != runID || continued.Status != runtimeruncontrol.StatusRunning || len(queue.released) != 1 || queue.released[0] != runID {
				t.Fatalf("continue result=%+v err=%v queue=%+v", continued, err, queue.released)
			}

			admitted, err := selected.AdmitGenericScheduleOutcome(ctx, testRootGenericScheduleCommand(t, runID, uuid.NewString(), "run-control-ack", runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour))))
			if err != nil || !admitted.Acknowledged {
				t.Fatalf("timer admission=%+v err=%v", admitted, err)
			}
			reconciler := &postCommitTimerRecorder{}
			stopped, err := runtimeruncontrol.NewController(store, nil, runtimeruncontrol.Options{TimerCancellations: reconciler}).Stop(ctx, request)
			if !errors.Is(err, fault) || stopped.RunID != runID || stopped.Status != runtimeruncontrol.StatusCancelled {
				t.Fatalf("stop result=%+v err=%v", stopped, err)
			}
			if store.acks != 3 || len(reconciler.refs) != 1 || reconciler.refs[0].ActivationID != admitted.Result.Activation.ID {
				t.Fatalf("acknowledged transitions=%d timer refs=%+v", store.acks, reconciler.refs)
			}
			activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Result.Activation.ID)
			if err != nil || !found || activation.Status != runtimegenericschedule.StatusCancelled {
				t.Fatalf("durable timer=%+v found=%v err=%v", activation, found, err)
			}
		})
	}
}
