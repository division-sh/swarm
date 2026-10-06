package engine

import (
	"context"
	"errors"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type finalStageMutationOwner struct {
	locker    *executionBoundaryLocker
	cancel    context.CancelFunc
	postErr   error
	ack       bool
	mutations []EngineMutation
}

func (o *finalStageMutationOwner) CommitEngineMutation(_ context.Context, mutation EngineMutation) (CommittedEngineMutation, error) {
	if !o.locker.held {
		return CommittedEngineMutation{}, errors.New("business commit escaped its entity fence")
	}
	if err := mutation.ValidateTransitionEvidence(); err != nil {
		return CommittedEngineMutation{}, err
	}
	o.mutations = append(o.mutations, mutation)
	if o.cancel != nil {
		o.cancel()
	}
	return CommittedEngineMutation{Committed: o.ack}, o.postErr
}

func TestIssue2564M29FinalStageRetainsBusinessCommitAndAcknowledgement(t *testing.T) {
	for _, name := range []string{"success", "cleanup_error", "canceled_after_commit", "commit_refused"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			locker := &executionBoundaryLocker{}
			owner := &finalStageMutationOwner{locker: locker, ack: name != "commit_refused"}
			sentinel := errors.New("independent publication cleanup failure")
			switch name {
			case "cleanup_error":
				owner.postErr = sentinel
			case "canceled_after_commit":
				owner.cancel, owner.postErr = cancel, context.Canceled
			case "commit_refused":
				owner.postErr = errors.New("commit refused before acknowledgement")
			}
			node := testFlowExecutableNode(t, "orders", "worker")
			handler := runtimecontracts.SystemNodeEventHandler{AdvancesTo: "done"}
			graph := runtimecontracts.BuildWorkflowStageTopology("orders", "ready", []string{"ready", "done"}, []string{"done"},
				[]runtimecontracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: "done"}}, nil, nil)
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
				StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{"orders": graph},
			}})
			snapshot := testStateSnapshot("ready", map[string]any{}, nil, nil)
			executor, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: &recordingStateRepo{snapshot: &snapshot},
				MutationOwner: owner, Locker: locker, WorkflowLifecycle: &testWorkflowLifecycleOwner{}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteSemanticFixture(ctx, transitionTestRequest(t, node, "work.requested", handler, "ready"))
			if len(owner.mutations) != 1 || owner.mutations[0].State.NextState != "done" || result.Committed != owner.ack || locker.held {
				t.Fatalf("final-stage commit boundary: result=%+v err=%v mutations=%+v lock=%v", result, err, owner.mutations, locker.held)
			}
			if name == "success" && err != nil || name != "success" && err == nil {
				t.Fatalf("commit disposition=%v name=%s", err, name)
			}
			if name == "cleanup_error" && !errors.Is(err, sentinel) || name == "canceled_after_commit" && !errors.Is(err, context.Canceled) {
				t.Fatalf("acknowledged error cause disappeared: %v", err)
			}
		})
	}
}
