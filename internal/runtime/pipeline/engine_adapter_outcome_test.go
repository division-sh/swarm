package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
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
	releaseErr          error
	finalizeErr         error
	finalizePanic       bool
}

func (p *outcomeEnginePlanner) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	p.releases++
	return p.releaseErr
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
	header, err := WorkflowInstanceHeaderPayloadForRoute(flow.Route, "v1")
	if err != nil {
		t.Fatal(err)
	}
	config, err := canonicaljson.MarshalPreservingNumberKinds(header)
	if err != nil {
		t.Fatal(err)
	}
	command := WorkflowEngineMutationCommand{State: WorkflowEngineStateRecord{
		Identity: flow, EntityID: eventtest.UUID("outcome-entity"), WorkflowName: ".", WorkflowVersion: "v1",
		Mode: "static", Status: "active", CurrentState: "pending", StageDefined: true,
		Fields: json.RawMessage(`{}`), Bookkeeping: json.RawMessage(`{}`), Gates: json.RawMessage(`{}`),
		Accumulator: json.RawMessage(`{}`), Config: config, InitialFields: json.RawMessage(`{}`),
		EnteredStageAt: at, CreatedAt: at, UpdatedAt: at, ExpectedState: "pending", ExpectedRevision: 1,
		Transition: WorkflowEngineStateTransitionUpdateStateAndCompanion,
	}}
	if err := command.Validate(); err != nil {
		t.Fatal(err)
	}
	return runtimeengine.EngineMutation{Address: runtimeengine.StateAddress{FlowInstance: flow}}, command
}

func TestOutcomeConstructedMutationRetainsExactRecordedHeader(t *testing.T) {
	mutation, command := outcomeConstructedMutation(t)
	header, err := DecodeWorkflowInstanceRecordedHeader(command.State.Identity.Route, command.State.Config)
	if err != nil || header.WorkflowVersion() != command.State.WorkflowVersion || !header.ParentRoute().Empty() {
		t.Fatalf("constructed root header = %+v, error = %v", header, err)
	}
	if mutation.Address.FlowInstance != command.State.Identity {
		t.Fatalf("mutation changed the constructed receiver: %+v", mutation.Address)
	}
	invalid := command
	invalid.State.Config = json.RawMessage(`{}`)
	if err := invalid.Validate(); err == nil {
		t.Fatal("constructed mutation accepted an absent recorded identity")
	}
}

func outcomeCommittedStage(t *testing.T) runtimeengine.CommittedStage {
	t.Helper()
	_, command := outcomeConstructedMutation(t)
	stage, err := CommittedWorkflowStage(command.State)
	if err != nil {
		t.Fatal(err)
	}
	return stage
}

func TestConstructedEngineMissingAcknowledgementDoesNotFinalize(t *testing.T) {
	storeOwner := &missingAcknowledgementEngineOwner{}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
	mutation, command := outcomeConstructedMutation(t)
	result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, nil)
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
			storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Stage: outcomeCommittedStage(t), Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}}
			planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizeErr: test.postErr, finalizePanic: test.panicNow}
			owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
			mutation, command := outcomeConstructedMutation(t)
			result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, nil)
			if !result.Committed || planner.finalizes != 1 || planner.releases != 0 || err == nil {
				t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
			}
			if result.Stage == nil || *result.Stage != storeOwner.result.Stage {
				t.Fatalf("cleanup lost the exact committed stage: %+v", result)
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
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Stage: outcomeCommittedStage(t), Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}, afterCommit: cancel}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizeErr: context.Canceled}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
	mutation, command := outcomeConstructedMutation(t)
	result, err := owner.commitPreparedEngineMutation(ctx, mutation, command, nil)
	if !result.Committed || !errors.Is(err, context.Canceled) || planner.finalizes != 1 || planner.releases != 0 {
		t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
	}
	if result.Stage == nil || *result.Stage != storeOwner.result.Stage {
		t.Fatalf("cancellation lost the exact committed stage: %+v", result)
	}
}

func TestCommittedEngineAttemptsIndependentFinalizersAfterPublicationPanic(t *testing.T) {
	storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed: true,
		Stage:     outcomeCommittedStage(t),
		Lifecycle: CommittedWorkflowLifecycleMutation{Wakeups: []timeridentity.WorkflowTimerActivationRef{{}}},
	}}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}, finalizePanic: true}
	owner := pipelineEngineMutationOwner{
		store: &workflowInstanceStore{engineMutations: storeOwner},
		state: pipelineEngineStateRepo{coordinator: &PipelineCoordinator{}}, publication: planner,
	}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil)
	if !result.Committed || planner.finalizes != 1 || planner.releases != 0 || err == nil ||
		!strings.Contains(err.Error(), "publication cleanup panic") || !strings.Contains(err.Error(), "wakeup evidence 0 is invalid") {
		t.Fatalf("result=%+v error=%v finalizes=%d releases=%d", result, err, planner.finalizes, planner.releases)
	}
}

func TestConstructedEngineRejectsForeignOrMissingCommittedStageWithoutRepeatingCommit(t *testing.T) {
	for _, variant := range []string{"missing", "foreign_instance", "wrong_stage", "wrong_revision"} {
		t.Run(variant, func(t *testing.T) {
			stage := outcomeCommittedStage(t)
			switch variant {
			case "missing":
				stage = runtimeengine.CommittedStage{}
			case "foreign_instance":
				stage.Instance.RunID = eventtest.UUID("other-run")
			case "wrong_stage":
				stage.Stage = "other-stage"
			case "wrong_revision":
				stage.Revision++
			}
			storeOwner := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{Committed: true, Stage: stage, Lifecycle: CommittedWorkflowLifecycleMutation{Committed: true}}}
			planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
			owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
			mutation, command := outcomeConstructedMutation(t)
			result, err := owner.commitPreparedEngineMutation(context.Background(), mutation, command, nil)
			if err == nil || !result.Committed || result.Stage != nil || planner.releases != 0 || planner.finalizes != 1 {
				t.Fatalf("invalid receipt reused or guessed: %+v err=%v releases=%d finalizes=%d", result, err, planner.releases, planner.finalizes)
			}
		})
	}
}
