package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

type terminalHandoffMutationOwner struct {
	locker  *executionBoundaryLocker
	handoff *terminalHandoffProbe
	cancel  context.CancelFunc
	postErr error
	ack     bool
	calls   int
}

func (o *terminalHandoffMutationOwner) CommitEngineMutation(context.Context, EngineMutation) (CommittedEngineMutation, error) {
	o.calls++
	if !o.locker.held {
		return CommittedEngineMutation{}, errors.New("business commit escaped its entity fence")
	}
	if o.cancel != nil {
		o.cancel()
	}
	return CommittedEngineMutation{Committed: o.ack, FlowDeactivation: o.handoff}, o.postErr
}

type terminalHandoffProbe struct {
	locker *executionBoundaryLocker
	calls  int
	err    error
	panics bool
}

func (p *terminalHandoffProbe) FinalizeFlowDeactivation(context.Context) error {
	p.calls++
	if p.locker.held {
		return errors.New("terminal cleanup remained under the entity fence")
	}
	if p.panics {
		panic("terminal cleanup probe panic")
	}
	return p.err
}

func TestIssue2564M29ExecutorConsumesTerminalHandoffAfterUnlock(t *testing.T) {
	for _, name := range []string{"success", "cleanup_error", "cleanup_panic", "canceled_after_commit", "commit_refused"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			locker := &executionBoundaryLocker{}
			handoff := &terminalHandoffProbe{locker: locker}
			owner := &terminalHandoffMutationOwner{locker: locker, handoff: handoff, ack: name != "commit_refused"}
			sentinel := errors.New("terminal cleanup error")
			switch name {
			case "cleanup_error":
				handoff.err = sentinel
			case "cleanup_panic":
				handoff.panics = true
			case "canceled_after_commit":
				owner.cancel, owner.postErr = cancel, context.Canceled
			case "commit_refused":
				owner.postErr = errors.New("commit refused before acknowledgement")
			}
			snapshot := testStateSnapshot("pending", map[string]any{}, nil, nil)
			repo := &recordingStateRepo{snapshot: &snapshot}
			source := sourceWithFixtureStages(stubSource(), "flow-1", "pending", "pending")
			executor, err := NewExecutor(RuntimeDependencies{Source: source, StateRepo: repo, MutationOwner: owner, Locker: locker}, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.ExecuteSemanticFixture(ctx, ExecutionRequest{
				EntityID: "entity-1", Node: testFlowExecutableNode(t, "flow-1", "node-1"),
				Event:   eventtest.RunCreatingRootIngress("evt-terminal-handoff", "task.completed", "test", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
				Handler: runtimecontracts.SystemNodeEventHandler{SetsGate: &runtimecontracts.GateSpec{Name: "ready"}},
			})
			want := 1
			if !owner.ack {
				want = 0
			}
			if owner.calls != 1 || handoff.calls != want || result.Committed != owner.ack || locker.held {
				t.Fatalf("ack/cleanup boundary: result=%+v err=%v commits=%d cleanup=%d want=%d lock=%v", result, err, owner.calls, handoff.calls, want, locker.held)
			}
			if name == "success" && err != nil || name != "success" && err == nil {
				t.Fatalf("cleanup disposition=%v name=%s", err, name)
			}
			if name == "cleanup_error" && !errors.Is(err, sentinel) || name == "canceled_after_commit" && !errors.Is(err, context.Canceled) {
				t.Fatalf("acknowledged error cause disappeared: %v", err)
			}
		})
	}
}
