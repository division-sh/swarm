package runforkpersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func TestRunForkWorkflowTimerReadbackSeparatesCutFromContinuation(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	source.Recurring, source.RecurrenceInterval = true, time.Hour
	plan := workflowTimerHistoryPlan(t, source)
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, source.CreatedAt.Add(2*time.Hour))
	if err != nil || len(projected) != 1 {
		t.Fatalf("project source timer: %v", err)
	}
	for _, tc := range []struct {
		name              string
		edit              func(*pipeline.WorkflowTimerActivation)
		cut, continuation bool
	}{
		{"unstarted", func(*pipeline.WorkflowTimerActivation) {}, true, true},
		{"recurring_cannot_finish_as_fired", func(a *pipeline.WorkflowTimerActivation) {
			a.Status = "fired"
			a.FiredAt = a.CreatedAt.Add(time.Second)
		}, false, false},
		{"cancelled", func(a *pipeline.WorkflowTimerActivation) { a.Status = "cancelled" }, false, true},
		{"advanced_recurrence", func(a *pipeline.WorkflowTimerActivation) {
			a.FireAt = a.FireAt.Add(a.RecurrenceInterval)
			a.FiredAt = a.CreatedAt.Add(time.Second)
		}, false, true},
		{"wrong_due", func(a *pipeline.WorkflowTimerActivation) { a.FireAt = a.FireAt.Add(time.Microsecond) }, false, false},
		{"changed_payload", func(a *pipeline.WorkflowTimerActivation) { a.Payload = []byte(`{"changed":true}`) }, false, false},
		{"changed_source", func(a *pipeline.WorkflowTimerActivation) { a.SourceTimerID = "66666666-6666-4666-8666-666666666666" }, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := projected[0].activation.Canonical()
			tc.edit(&actual)
			for _, phase := range []runForkWorkflowTimerReadbackPhase{runForkWorkflowTimerAtCut, runForkWorkflowTimerContinuing} {
				want := tc.cut
				if phase == runForkWorkflowTimerContinuing {
					want = tc.continuation
				}
				err := requireExactRunForkWorkflowTimerInventory(projected, []pipeline.WorkflowTimerActivation{actual}, phase)
				if (err == nil) != want {
					t.Fatalf("phase=%d admitted=%t want=%t err=%v", phase, err == nil, want, err)
				}
			}
		})
	}
}

func TestRunForkWorkflowTimerReadbackRejectsExtraAndDuplicateInventory(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	plan := workflowTimerHistoryPlan(t, source)
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, source.CreatedAt.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	actual := projected[0].activation.Canonical()
	for _, rows := range [][]pipeline.WorkflowTimerActivation{nil, {actual, actual}} {
		if err := requireExactRunForkWorkflowTimerInventory(projected, rows, runForkWorkflowTimerContinuing); err == nil {
			t.Fatal("partial or extra inventory admitted")
		}
	}
	if err := requireExactRunForkWorkflowTimerInventory(nil, []pipeline.WorkflowTimerActivation{actual}, runForkWorkflowTimerAtCut); err == nil {
		t.Fatal("absent source history hid an extra child timer")
	}
	if err := requireExactRunForkWorkflowTimerInventory([]runForkWorkflowTimerProjection{projected[0], projected[0]}, []pipeline.WorkflowTimerActivation{actual, actual}, runForkWorkflowTimerAtCut); err == nil {
		t.Fatal("duplicate inventory admitted")
	}
}

func TestRunForkWorkflowTimerReadbackPreservesCompletedOneShot(t *testing.T) {
	source := workflowTimerProjectionSource(t, true)
	plan := workflowTimerHistoryPlan(t, source)
	projected, err := prepareRunForkWorkflowTimers(plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, source.CreatedAt.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	actual := projected[0].activation.Canonical()
	actual.Status, actual.FiredAt = "fired", actual.CreatedAt.Add(time.Second)
	if err := requireExactRunForkWorkflowTimerInventory(projected, []pipeline.WorkflowTimerActivation{actual}, runForkWorkflowTimerAtCut); err == nil {
		t.Fatal("completed child one-shot admitted before activation")
	}
	if err := requireExactRunForkWorkflowTimerInventory(projected, []pipeline.WorkflowTimerActivation{actual}, runForkWorkflowTimerContinuing); err != nil {
		t.Fatalf("completed one-shot was rearmed or rejected: %v", err)
	}
	owner := &workflowTimerMaterializerOwner{rows: map[string]pipeline.WorkflowTimerActivation{actual.Ref.ActivationID: actual}}
	arrivals := &arrivalJoinInventoryOwner{}
	withWorkflowTimerReadbackAttempt(t, func(ctx context.Context, attempt *mutationprotocol.Attempt) {
		bornAt := source.CreatedAt.Add(2 * time.Hour)
		cut, err := requireMaterializedRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun,
			workflowTimerMaterializerSelection{}, owner, arrivals, bornAt, plan.ReplayResumeAdmission)
		if err == nil || !reflect.DeepEqual(cut, plan.ReplayResumeAdmission) {
			t.Fatal("unified readback admitted completed child one-shot before activation or changed source admission")
		}
		continuing, err := requireContinuingRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun,
			workflowTimerMaterializerSelection{}, owner, arrivals, bornAt, plan.ReplayResumeAdmission)
		if err != nil || len(continuing.UnsupportedBlockers) != 0 || continuing.StateOnlyExecutionReady {
			t.Fatalf("unified continuing readback rearmed or rejected completed one-shot: admission=%+v err=%v", continuing, err)
		}
		assertWorkflowTimerAppliedDisposition(t, continuing, runfork.RunForkReplayResumeDispositionReconstruct)
	})
	if owner.reads != 2 || owner.writes != 0 || owner.cancels != 0 || arrivals.reads == 0 {
		t.Fatal("unified phase readback skipped inventory or mutated completed timer history")
	}
}
