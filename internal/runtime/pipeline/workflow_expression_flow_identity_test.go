package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func VerifyPipelineExpressionPreservesExactExecutionFlowOnBothStoresForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{
		"schema.yaml":         "name: display-name-is-not-a-flow\nstages:\n  ready: {}\n",
		"entities.yaml":       "root_entity:\n  root_only: text\n",
		"events.yaml":         "query.requested:\n  value: text\n",
		"child/schema.yaml":   "name: child\nstages:\n  ready: {}\n",
		"child/entities.yaml": "child_entity:\n  child_only: text\n",
		"child/events.yaml":   "query.requested:\n  value: text\n",
	})
	fixture := open(t, source)
	ctx := correlation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{
		Module: staticSemanticWorkflowModule{source: source},
	})
	evaluator := pipelineEngineEvaluator{evaluator: newWorkflowExpressionEvaluator(), coordinator: pc}
	for _, row := range []struct{ flow, field string }{{semanticview.RootExecutionFlowID(source), "root_only"}, {"child", "child_only"}} {
		resolution := semanticview.ResolveEventSchema(source, row.flow, "query.requested")
		if !resolution.HasStructural {
			t.Fatal("flow query requires its declared payload schema")
		}
		ok, err := evaluator.EvalBool("query_entities("+row.field+" == payload.value).count == 0", engine.BaseContext{
			FlowID: row.flow, Event: values.Wrap(map[string]any{"run_id": testPipelineRunID}),
			Payload: values.Wrap(map[string]any{"value": "absent"}),
		}, workflowexpr.ValueExpressionOptions{PayloadType: &resolution.StructuralType})
		if err != nil || !ok {
			t.Fatalf("flow %q lost its exact entity contract: ok=%t err=%v", row.flow, ok, err)
		}
	}
}
