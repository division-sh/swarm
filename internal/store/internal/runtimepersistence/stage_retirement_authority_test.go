package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"
	"time"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestIssue2564M29StageMutationCannotDeclareResourceRetirementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			before, found, err := f.workflows.Load(f.ctx, record.Identity)
			if err != nil || !found {
				t.Fatalf("read constructed receiver: found=%v err=%v", found, err)
			}
			record.CurrentState = record.ExpectedState
			command := runtimepipeline.WorkflowEngineMutationCommand{State: record,
				PostCommit: runtimepipeline.WorkflowEnginePostCommitPlan{FlowDeactivation: &runtimepipeline.WorkflowEngineFlowDeactivation{
					Identity: record.Identity, EntityID: record.EntityID, NextState: record.CurrentState,
				}}}
			result, err := f.store.(workflowTestSelectedStore).CommitWorkflowEngineMutation(f.ctx, command)
			if err == nil || !strings.Contains(err.Error(), "stage mutation cannot declare operational flow retirement") || result.Committed {
				t.Fatalf("stage mutation declared an operational resource retirement: %+v err=%v", result, err)
			}
			after, found, err := f.workflows.Load(f.ctx, record.Identity)
			if err != nil || !found || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused retirement changed canonical workflow state: before=%+v after=%+v found=%v err=%v", before, after, found, err)
			}
		})
	}
}
