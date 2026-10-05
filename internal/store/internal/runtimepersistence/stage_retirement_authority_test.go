package runtimepersistence

import (
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestStageMutationCannotDeclareResourceRetirementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			owner := f.store.(workflowTestSelectedStore)
			record.CurrentState = record.ExpectedState
			command := runtimepipeline.WorkflowEngineMutationCommand{
				State: record,
				PostCommit: runtimepipeline.WorkflowEnginePostCommitPlan{
					FlowDeactivation: &runtimepipeline.WorkflowEngineFlowDeactivation{
						Identity: record.Identity, EntityID: record.EntityID, NextState: record.CurrentState,
					},
				},
			}
			result, err := owner.CommitWorkflowEngineMutation(f.ctx, command)
			if err == nil || result.Committed {
				t.Fatalf("stage mutation declared an operational resource retirement: %+v err=%v", result, err)
			}
			var stage, status string
			var revision int64
			if err := f.db.QueryRowContext(f.ctx, `SELECT current_state,status,revision FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, record.Identity.RunID, record.EntityID).Scan(&stage, &status, &revision); err != nil {
				t.Fatal(err)
			}
			if stage != record.ExpectedState || status != "active" || revision != record.ExpectedRevision {
				t.Fatalf("refused retirement changed workflow: stage=%s status=%s revision=%d", stage, status, revision)
			}
		})
	}
}
