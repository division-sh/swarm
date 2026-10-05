package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestWorkflowTimerInterruptionRetryRequiresOnlyContextCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"canceled", context.Canceled, true},
		{"deadline", fmt.Errorf("transition step: %w", context.DeadlineExceeded), true},
		{"both_context_causes", errors.Join(context.Canceled, context.DeadlineExceeded), true},
		{"nil", nil, false},
		{"wrong_owner", errors.Join(context.Canceled, errors.New("wrong timer owner")), false},
		{"schema", errors.Join(context.DeadlineExceeded, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "schema_invalid", "timer", "transition", nil)), false},
		{"revoked_run", runtimefailures.Wrap(runtimefailures.ClassAuthorizationDenied, "run_revoked", "timer", "transition", nil, context.Canceled), false},
		{"cleanup", errors.Join(context.Canceled, errors.New("independent cleanup failure")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := runtimefailures.IsContextInterruption(tc.err); got != tc.want {
				t.Fatalf("context-only interruption=%v, want %v", got, tc.want)
			}
		})
	}
}

type timerResultControlOwner struct {
	result CommittedWorkflowTimerOccurrence
	calls  int
}

type timerEntityFenceCommitOwner struct {
	WorkflowEngineMutationOwner
	coordinator *PipelineCoordinator
	test        *testing.T
	calls       int
}

func (o *timerEntityFenceCommitOwner) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	o.calls++
	o.coordinator.entityLockMu.Lock()
	lock := o.coordinator.entityLocks[command.State.EntityID]
	o.coordinator.entityLockMu.Unlock()
	if lock == nil {
		o.test.Fatal("timer transition reached commit without the entity fence")
	}
	if lock.TryLock() {
		lock.Unlock()
		o.test.Fatal("timer transition released the entity fence before commit")
	}
	return o.WorkflowEngineMutationOwner.CommitWorkflowEngineMutation(ctx, command)
}

func VerifyWorkflowTimerTransitionEntityFenceForTest(t *testing.T, factory WorkflowTimerCauseReplayFactoryForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml": "name: timer-fence-proof\nstages:\n  waiting:\n    initial: true\n    timers:\n      - {id: waiting.timeout, after: 1h, advances_to: done}\n  done: {terminal: true}\n",
			})
			bundle.Semantics.Timers[0].Recurring = true
			f := factory(t, backend, bundle)
			ctx, pc := f.Context, f.Coordinator
			runID := runtimecorrelation.RunIDFromContext(ctx)
			identity := testRunScopedWorkflowInstanceFromContext(ctx, runID)
			at := canonicalWorkflowTimerTime(time.Now().Add(-2 * time.Hour))
			f.CommitConstruction(ctx, identity, WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: ".", WorkflowVersion: semanticview.Wrap(bundle).WorkflowVersion(),
				Mode: "static", CurrentState: "waiting", StageDefined: true, Fields: map[string]any{}, CreatedAt: at, EnteredStageAt: at,
			}, at)
			owner := &timerEntityFenceCommitOwner{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, coordinator: pc, test: t}
			pc.workflowStore.engineMutations = owner
			projected := 0
			pc.workflowTimers.testAfterWakeupLoad = func() {
				if owner.calls == 0 {
					return
				}
				pc.entityLockMu.Lock()
				lock := pc.entityLocks[runID]
				pc.entityLockMu.Unlock()
				if lock == nil || !lock.TryLock() {
					t.Fatal("timer retained the entity fence across post-commit projection")
				}
				lock.Unlock()
				projected++
			}
			rows := listTimerCauseReplayActivationsForTest(t, pc.workflowStore.timerActivations, ctx, runID)
			if len(rows) != 1 {
				t.Fatalf("initial timer rows=%+v", rows)
			}
			if outcome, err := fireWorkflowTimerTestWakeup(ctx, pc, rows[0]); err != nil || outcome != WorkflowTimerFireCommitted {
				t.Fatalf("fire fenced timer: outcome=%s error=%v", outcome, err)
			}
			if owner.calls != 1 || projected == 0 {
				t.Fatalf("commit calls=%d post-commit projections=%d", owner.calls, projected)
			}
		})
	}
}

func (o *timerResultControlOwner) CommitWorkflowTimerOccurrence(context.Context, WorkflowTimerOccurrenceCommand) (CommittedWorkflowTimerOccurrence, error) {
	o.calls++
	return o.result, nil
}

func TestWorkflowTimerMissingOrInvalidCommitResultDoesNotDispatch(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, phase := range []string{"zero_nil_error", "invalid_acknowledged"} {
			t.Run(backend.name+"/"+phase, func(t *testing.T) {
				store, ctx := backend.open(t)
				bus := &recordingPipelineBus{}
				pc, _, activation := seedWorkflowTimerOwnerActivation(t, store, ctx, bus, false)
				defer pc.StopWorkflowTimerLifecycle(context.Background())
				owner := &timerResultControlOwner{}
				want := WorkflowTimerFireRetry
				if phase == "invalid_acknowledged" {
					owner.result.Outcome, want = WorkflowTimerOccurrenceCommitted, WorkflowTimerFireTerminal
				}
				pc.workflowTimers.storeOwner.timerOccurrences = owner
				planner := &outcomeEnginePlanner{recordingPipelineBus: bus}
				pc.workflowTimers.publication = planner
				// This control calls Fire directly: suppress asynchronous recovery
				// so one malformed owner response cannot race fixture teardown.
				pc.workflowTimers.workOwner = nil
				pc.workflowTimers.scheduler = nil
				wakeup, err := newWorkflowTimerWakeup(activation)
				if err != nil {
					t.Fatal(err)
				}
				outcome, recurrence, err := pc.workflowTimers.fireWakeup(ctx, wakeup)
				if err == nil || outcome != want || recurrence || owner.calls != 1 || planner.releases != 1 || planner.finalizes != 0 || bus.publishedCount() != 0 {
					t.Fatalf("outcome=%s recurrence=%t err=%v calls=%d releases=%d finalizes=%d published=%d", outcome, recurrence, err, owner.calls, planner.releases, planner.finalizes, bus.publishedCount())
				}
			})
		}
	}
}
