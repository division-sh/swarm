package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type missingAcknowledgementEngineOwner struct{ calls int }

func (o *missingAcknowledgementEngineOwner) CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	o.calls++
	return CommittedWorkflowEngineMutation{}, nil
}

type acknowledgedEngineOwner struct {
	result      CommittedWorkflowEngineMutation
	err         error
	afterCommit func()
}

func (o *acknowledgedEngineOwner) CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	if o.afterCommit != nil {
		o.afterCommit()
	}
	return o.result, o.err
}

type outcomeEnginePlanner struct {
	*recordingPipelineBus
	releases, finalizes int
	finalizeErr         error
	finalizePanic       bool
}

type outcomeTerminalReservation struct {
	commits, aborts int
	commitPanic     bool
	abortPanic      bool
}

func (r *outcomeTerminalReservation) Commit() error {
	r.commits++
	if r.commitPanic {
		panic("terminal cleanup panic")
	}
	return nil
}

func (r *outcomeTerminalReservation) Abort() error {
	r.aborts++
	if r.abortPanic {
		panic("terminal reservation abort panic")
	}
	return nil
}

func (p *outcomeEnginePlanner) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	p.releases++
	return nil
}
func (p *outcomeEnginePlanner) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	p.finalizes++
	if p.finalizePanic {
		panic("publication cleanup panic")
	}
	return p.finalizeErr
}

func outcomeConstructedMutation(t *testing.T) (runtimeengine.EngineMutation, WorkflowEngineMutationCommand) {
	t.Helper()
	runID := eventtest.UUID("outcome-run")
	flow := runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: runtimeflowidentity.RouteForInstancePath(runID)}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	command := WorkflowEngineMutationCommand{State: WorkflowEngineStateRecord{
		Identity: flow, EntityID: eventtest.UUID("outcome-entity"), WorkflowName: ".", WorkflowVersion: "v1",
		Mode: "static", Status: "active", CurrentState: "pending", StageDefined: true,
		Fields: json.RawMessage(`{}`), Bookkeeping: json.RawMessage(`{}`), Gates: json.RawMessage(`{}`),
		Accumulator: json.RawMessage(`{}`), Config: json.RawMessage(`{}`), InitialFields: json.RawMessage(`{}`),
		EnteredStageAt: at, CreatedAt: at, UpdatedAt: at, ExpectedState: "pending", ExpectedRevision: 1,
		Transition: WorkflowEngineStateTransitionUpdateStateAndCompanion,
	}}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	return runtimeengine.EngineMutation{Address: runtimeengine.StateAddress{FlowInstance: flow}}, command
}

func TestConstructedEngineMissingAcknowledgementDoesNotFinalize(t *testing.T) {
	storeOwner := &missingAcknowledgementEngineOwner{}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
	mutation, command := outcomeConstructedMutation(t)
	result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no acknowledged result") || result.Committed || storeOwner.calls != 1 || planner.releases != 1 || planner.finalizes != 0 {
		t.Fatalf("result=%+v error=%v commits=%d releases=%d finalizes=%d", result, err, storeOwner.calls, planner.releases, planner.finalizes)
	}
}

func TestConstructedEngineKeepsAcknowledgementThroughCleanupFailureAndPanic(t *testing.T) {
	for _, test := range []struct {
		name     string
		postErr  error
		panicNow bool
	}{
		{name: "cleanup_error", postErr: errors.New("cleanup failed")},
		{name: "cleanup_panic", panicNow: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}}
			planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizeErr: test.postErr, finalizePanic: test.panicNow}
			owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
			mutation, command := outcomeConstructedMutation(t)
			result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, nil, nil)
			if !result.Committed || planner.finalizes != 1 || planner.releases != 0 || err == nil {
				t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
			}
			if test.postErr != nil && !errors.Is(err, test.postErr) {
				t.Fatalf("cleanup cause lost: %v", err)
			}
			if test.panicNow && !strings.Contains(err.Error(), "publication cleanup panic") {
				t.Fatalf("panic cause lost: %v", err)
			}
		})
	}
}

func TestConstructedEngineKeepsAcknowledgementAfterCommitCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}, afterCommit: cancel}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizeErr: context.Canceled}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
	mutation, command := outcomeConstructedMutation(t)
	result, err := owner.commitPreparedEngineMutation(ctx, mutation, command, nil, nil)
	if !result.Committed || !errors.Is(err, context.Canceled) || planner.finalizes != 1 || planner.releases != 0 {
		t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
	}
}

func TestCommittedEngineAttemptsIndependentFinalizersAfterPublicationPanic(t *testing.T) {
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed: true,
		Lifecycle: CommittedWorkflowLifecycleMutation{Wakeups: []timeridentity.WorkflowTimerActivationRef{{}}},
	}}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizePanic: true}
	owner := pipelineEngineMutationOwner{
		store: &workflowInstanceStore{engineMutations: storeOwner},
		state: pipelineEngineStateRepo{coordinator: &PipelineCoordinator{}}, publication: planner,
	}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, nil)
	if !result.Committed || planner.finalizes != 1 || planner.releases != 0 || err == nil ||
		!strings.Contains(err.Error(), "publication cleanup panic") || !strings.Contains(err.Error(), "wakeup evidence 0 is invalid") {
		t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
	}
}

func TestCommittedEngineAttemptsIndependentFinalizersAfterTerminalPanic(t *testing.T) {
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed:  true,
		PostCommit: WorkflowEnginePostCommitPlan{FlowDeactivation: &WorkflowEngineFlowDeactivation{}},
		Lifecycle:  CommittedWorkflowLifecycleMutation{Wakeups: []timeridentity.WorkflowTimerActivationRef{{}}},
	}}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
	terminal := &outcomeTerminalReservation{commitPanic: true}
	owner := pipelineEngineMutationOwner{
		store: &workflowInstanceStore{engineMutations: storeOwner},
		state: pipelineEngineStateRepo{coordinator: &PipelineCoordinator{}}, publication: planner,
	}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, terminal)
	if result.FlowDeactivation == nil {
		t.Fatal("committed terminal reservation was not transferred to unlocked execution")
	}
	err = errors.Join(err, result.FlowDeactivation.FinalizeFlowDeactivation(context.Background()))
	if !result.Committed || terminal.commits != 1 || terminal.aborts != 1 || planner.finalizes != 1 || err == nil ||
		!strings.Contains(err.Error(), "terminal cleanup panic") || !strings.Contains(err.Error(), "wakeup evidence 0 is invalid") {
		t.Fatalf("result=%+v error=%v terminal=%+v finalizes=%d", result, err, terminal, planner.finalizes)
	}
}

func TestUnacknowledgedEngineAbortsTerminalReservation(t *testing.T) {
	terminal := &outcomeTerminalReservation{abortPanic: true}
	owner := pipelineEngineMutationOwner{
		store: &workflowInstanceStore{engineMutations: &missingAcknowledgementEngineOwner{}},
	}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, terminal)
	if result.Committed || err == nil || !strings.Contains(err.Error(), "terminal reservation abort panic") || terminal.commits != 0 || terminal.aborts != 1 {
		t.Fatalf("result=%+v error=%v terminal=%+v", result, err, terminal)
	}
}
