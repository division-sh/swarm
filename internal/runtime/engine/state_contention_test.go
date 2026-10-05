package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type contentionMutationOwner struct {
	repo             *recordingStateRepo
	remaining, calls int
	acknowledged     bool
	cancel           context.CancelFunc
	err              error
}

func (o *contentionMutationOwner) CommitEngineMutation(_ context.Context, m EngineMutation) (CommittedEngineMutation, error) {
	o.calls++
	if m.EvaluatedState.Revision != o.repo.snapshot.Revision || m.State.Fields["marker"] != o.repo.snapshot.Fields["marker"] {
		return CommittedEngineMutation{}, errors.New("attempt did not evaluate fresh state")
	}
	if o.remaining > 0 {
		o.remaining--
		o.repo.snapshot.Revision++
		o.repo.snapshot.Fields["marker"] = o.repo.snapshot.Revision
		if o.cancel != nil {
			o.cancel()
		}
		return CommittedEngineMutation{Committed: o.acknowledged}, o.err
	}
	return CommittedEngineMutation{Committed: true}, nil
}

func TestExecutorReevaluatesOnlyUncommittedStateContention(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		remaining            int
		acknowledged, cancel bool
		err                  error
		wantCalls            int
	}{
		{"more than delivery budget", 9, false, false, failures.New(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "test", "commit", nil), 10},
		{"cancel during reevaluation", 9, false, true, failures.New(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "test", "commit", nil), 1},
		{"acknowledged conflict-like cleanup", 1, true, false, failures.New(failures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "test", "commit", nil), 1},
		{"different refusal", 1, false, false, failures.New(failures.ClassAuthorizationDenied, "cross_flow_write_forbidden", "test", "commit", nil), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := testStateSnapshot("pending", map[string]any{"marker": int64(1)}, nil, nil)
			snapshot.Revision = 1
			repo := &recordingStateRepo{snapshot: &snapshot}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			owner := &contentionMutationOwner{repo: repo, remaining: tc.remaining, acknowledged: tc.acknowledged, err: tc.err}
			if tc.cancel {
				owner.cancel = cancel
			}
			source := sourceWithFixtureStages(stubSource(), "flow-1", "pending", "pending")
			executor, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: repo, MutationOwner: owner, Locker: &executionBoundaryLocker{}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteSemanticFixture(ctx, ExecutionRequest{
				EntityID: "entity-1", Node: testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:   eventtest.RunCreatingRootIngress("evt-contention", "task.completed", "test", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
				Handler: runtimecontracts.SystemNodeEventHandler{SetsGate: &runtimecontracts.GateSpec{Name: "ready"}},
			})
			if owner.calls != tc.wantCalls {
				t.Fatalf("calls=%d want=%d err=%v", owner.calls, tc.wantCalls, err)
			}
			if tc.cancel {
				if result.Committed || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel result=%+v err=%v", result, err)
				}
			} else if tc.acknowledged {
				if !result.Committed || err == nil {
					t.Fatalf("committed result lost: %+v %v", result, err)
				}
			} else if tc.name == "different refusal" {
				if result.Committed || err == nil {
					t.Fatalf("refusal hidden: %+v %v", result, err)
				}
			} else if !result.Committed || err != nil {
				t.Fatalf("contention exhausted delivery: %+v %v", result, err)
			}
		})
	}
}
