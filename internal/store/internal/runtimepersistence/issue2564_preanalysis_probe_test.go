package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestIssue2564AgentWriteMustAdvanceCanonicalRevisionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, record := constructWorkflowMutationFixture(t, backend, "review", time.Now().UTC())
			selected := f.store.(workflowTestSelectedStore)
			opts := completeWorkflowTestCoordinatorOptions(pipeline.NewWorkflowPersistence(selected), selected)
			opts.Module = runForkGateWorkflowModule{source: semanticview.Wrap(f.bundle)}
			writer := pipeline.NewPipelineCoordinatorWithOptions(workflowTestBus{}, opts)
			result, err := writer.ApplyEntityFieldMutation(f.ctx, pipeline.EntityFieldMutation{
				RunID: record.Identity.RunID, EntityID: record.EntityID, Owner: record.Identity, FlowID: "review",
				Mutation: entityruntime.Mutation{Target: "entity.account_id", Value: "agent-committed"},
				Source:   semanticview.Wrap(f.bundle), Writer: mutationlog.Writer{Type: "agent", ID: "probe", HandlerStep: "save_entity_field"},
			})
			if err != nil || !result.Acknowledged {
				t.Fatalf("agent save: %+v %v", result, err)
			}
			var headerRevision, fieldsRevision int64
			query := `SELECT f.revision, e.revision FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.entity_id=$2`
			if err := f.db.QueryRowContext(f.ctx, query, record.Identity.RunID, record.EntityID).Scan(&headerRevision, &fieldsRevision); err != nil {
				t.Fatal(err)
			}
			if headerRevision <= record.ExpectedRevision || headerRevision != fieldsRevision {
				t.Fatalf("acknowledged agent save bypassed canonical CAS: evaluated=%d header=%d fields=%d tool=%d", record.ExpectedRevision, headerRevision, fieldsRevision, result.Revision)
			}
		})
	}
}
