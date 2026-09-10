package semanticview

import (
	"reflect"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
)

func TestCompiledTransitionProjectionMutationIsolation(t *testing.T) {
	node := identitytest.RootNode(t, "worker")
	graph := runtimecontracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "working", "done"}, []string{"done"},
		[]runtimecontracts.HandlerTransitionSemantic{{Node: node, EventType: "work.requested", AdvancesTo: "done"}}, nil, nil)
	bundle := &runtimecontracts.WorkflowContractBundle{Semantics: runtimecontracts.WorkflowSemanticView{
		StageTopologies: map[string]runtimecontracts.WorkflowStageTopology{".": graph},
	}}
	source := Wrap(bundle)
	before, ok := WorkflowStageTopology(source, ".")
	if !ok || len(before.Edges) != 2 || len(before.Handlers) != 1 {
		t.Fatalf("missing canonical carrier projection: %#v", before)
	}
	for _, read := range []func() (runtimecontracts.WorkflowStageTopology, bool){
		func() (runtimecontracts.WorkflowStageTopology, bool) { return WorkflowStageTopology(source, ".") },
		func() (runtimecontracts.WorkflowStageTopology, bool) { return bundle.WorkflowStageTopology(".") },
	} {
		projection, _ := read()
		projection.Stages[0] = "foreign"
		projection.TerminalStages[0] = "foreign"
		projection.Edges[0].To = "foreign"
		projection.Handlers[0].Stages[0] = "foreign"
		projection.Handlers[0].EventType = "foreign"
		after, _ := WorkflowStageTopology(source, ".")
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("returned projection changed canonical owner: before=%#v after=%#v", before, after)
		}
	}
	if _, ok := WorkflowStageTopology(source, "child"); ok {
		t.Fatal("missing child borrowed root topology")
	}
}
