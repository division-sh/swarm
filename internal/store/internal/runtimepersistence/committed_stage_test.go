package runtimepersistence

import (
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestWorkflowEngineCommittedStageReceiptBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			record.CurrentState = record.ExpectedState
			record.EnteredStageAt = record.CreatedAt
			owner := f.store.(runtimepipeline.WorkflowEngineMutationOwner)
			first, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
			if err != nil || !first.Committed {
				t.Fatalf("first commit: %+v err=%v", first, err)
			}
			want, err := runtimepipeline.CommittedWorkflowStage(record)
			if err != nil || first.Stage != want {
				t.Fatalf("receipt=%+v want=%+v err=%v", first.Stage, want, err)
			}
			next := record
			next.ExpectedState, next.ExpectedRevision = record.CurrentState, want.Revision
			next.UpdatedAt = record.UpdatedAt.Add(time.Second)
			second, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: next})
			if err != nil || !second.Committed || second.Stage.Stage != "active" || second.Stage.Revision != want.Revision+1 {
				t.Fatalf("second commit: %+v err=%v", second, err)
			}
			if first.Stage != want {
				t.Fatalf("later commit changed original receipt: %+v", first.Stage)
			}
			stale, err := owner.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
			if err == nil || stale.Committed || stale.Stage.Validate() == nil {
				t.Fatalf("stale CAS exposed stage evidence: %+v err=%v", stale, err)
			}
		})
	}
}
