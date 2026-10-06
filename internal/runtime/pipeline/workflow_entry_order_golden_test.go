package pipeline

import (
	"reflect"
	"testing"
)

// These two source families required explicit declaration movement; generated
// output, not the rewrite's lexer metadata, must preserve their old entry facts.
func TestWorkflowEntryGoldensProtectMovedGateAndJoinGeneratedSources(t *testing.T) {
	gate := gateLifecycleBundle(t)
	if graph, ok := gate.WorkflowStageTopology("."); !ok || graph.InitialStage != "awaiting_review" || !reflect.DeepEqual(graph.StageIDs(), []string{"awaiting_review", "drafting", "operating"}) || len(graph.FinalStageIDs()) != 0 {
		t.Fatalf("moved gate source changed entry/order/finals: %+v", graph)
	}
	for _, review := range []bool{false, true} {
		for _, loop := range []string{"", "revision"} {
			bundle := workflowJoinLifecycleBundleWithOptions(t, review, loop)
			graph, ok := bundle.WorkflowStageTopology("orders")
			want := []string{"awaiting", "dispatching", "ready", "attention"}
			if review {
				want = []string{"awaiting", "reviewing", "dispatching", "ready", "attention"}
			}
			finals := []string{"attention", "ready"}
			if loop != "" {
				finals = []string{"attention"}
			}
			if !ok || graph.InitialStage != "awaiting" || !reflect.DeepEqual(graph.StageIDs(), want) || !reflect.DeepEqual(graph.FinalStageIDs(), finals) {
				t.Fatalf("moved join generator review=%t loop=%s: %+v", review, loop, graph)
			}
		}
	}
}
