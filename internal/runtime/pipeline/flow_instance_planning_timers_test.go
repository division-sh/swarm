package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type planningInitialTimerReader struct {
	reads int
	fault error
}

func (r *planningInitialTimerReader) ListFlowInstancePlanningInitialTimers(ctx context.Context, _ flowidentity.RunScopedFlowInstance, _ string) ([]WorkflowTimerActivation, error) {
	r.reads++
	return nil, errors.Join(ctx.Err(), r.fault)
}

type planningOrdinaryTimerReader struct {
	WorkflowTimerActivationPersistence
	reads int
}

func (r *planningOrdinaryTimerReader) ListWorkflowTimerActivations(context.Context, string, string, bool) ([]WorkflowTimerActivation, error) {
	r.reads++
	return nil, nil
}

func TestFlowInstancePlanningTimerCompositionIsInitialEntryOnly(t *testing.T) {
	initial := &planningInitialTimerReader{}
	ordinary := &planningOrdinaryTimerReader{}
	pc := &PipelineCoordinator{workflowStore: &workflowInstanceStore{timerActivations: ordinary}}
	ctx, err := WithFlowInstancePlanningInitialTimerReader(context.Background(), initial)
	if err != nil {
		t.Fatal(err)
	}
	owner := testRunScopedWorkflowInstanceForRun("11111111-1111-1111-1111-111111111111", "receiver")
	for _, kind := range []workflowTimerCauseKind{workflowTimerCauseInitial, workflowTimerCauseEvent, workflowTimerCauseTransition} {
		if _, err := pc.readWorkflowPlanningTimers(ctx, owner.RunID, owner.Route, "entity", workflowTimerCause{Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	if initial.reads != 1 || ordinary.reads != 2 {
		t.Fatalf("planning reader widened into ordinary timer authority: initial=%d ordinary=%d", initial.reads, ordinary.reads)
	}
	fault := errors.New("finite initial-entry timer refusal")
	initial.fault = fault
	if _, err := pc.readWorkflowPlanningTimers(ctx, owner.RunID, owner.Route, "entity", workflowTimerCause{Kind: workflowTimerCauseInitial}); !errors.Is(err, fault) || ordinary.reads != 2 {
		t.Fatalf("failed scoped timer read fell back to the pool: ordinary=%d err=%v", ordinary.reads, err)
	}
	if _, err := WithFlowInstancePlanningInitialTimerReader(context.Background(), nil); err == nil {
		t.Fatal("missing timer reader was admitted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := WithFlowInstancePlanningInitialTimerReader(canceled, initial); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled timer reader was admitted: %v", err)
	}
}
