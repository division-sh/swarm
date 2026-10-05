package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type issue2564TerminalFenceProbe struct {
	lock            *sync.Mutex
	commits, aborts int
}

func (p *issue2564TerminalFenceProbe) Commit() error {
	p.commits++
	if !p.lock.TryLock() {
		return fmt.Errorf("terminal handoff would reacquire its still-held entity fence")
	}
	p.lock.Unlock()
	return nil
}

func (p *issue2564TerminalFenceProbe) Abort() error { p.aborts++; return nil }

func TestIssue2564M29AcknowledgedTerminalHandoffLeavesEntityFence(t *testing.T) {
	pc := &PipelineCoordinator{}
	store := &acknowledgedEngineOwner{result: CommittedWorkflowEngineMutation{
		Committed: true, PostCommit: WorkflowEnginePostCommitPlan{FlowDeactivation: &WorkflowEngineFlowDeactivation{}},
	}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: store}, state: pipelineEngineStateRepo{coordinator: pc}}
	entity := identity.NormalizeEntityID("11111111-1111-1111-1111-111111111111")
	unlock := pc.lockWorkflowEntity(entity.String())
	terminal := &issue2564TerminalFenceProbe{lock: pc.entityLocks[entity.String()]}
	result, err := owner.commitPreparedEngineMutation(context.Background(), runtimeengine.EngineMutation{}, WorkflowEngineMutationCommand{}, nil, terminal)
	unlock()
	if !result.Committed || err != nil || terminal.commits != 0 || terminal.aborts != 0 {
		t.Fatalf("terminal execution occurred before the executor released its fence: ack=%v err=%v commits=%d aborts=%d", result.Committed, err, terminal.commits, terminal.aborts)
	}
	if result.FlowDeactivation == nil {
		t.Fatal("acknowledged terminal reservation disappeared before unlocked execution")
	}
	if err := result.FlowDeactivation.FinalizeFlowDeactivation(context.Background()); err != nil || terminal.commits != 1 || terminal.aborts != 0 {
		t.Fatalf("unlocked terminal handoff did not finish exactly once: err=%v commits=%d aborts=%d", err, terminal.commits, terminal.aborts)
	}
}
