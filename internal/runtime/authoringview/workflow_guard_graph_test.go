package authoringview

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestStageGraphDistinguishesGuardPossibilitiesFromAuthoredEdges(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	graph := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "done", "killed"}, []string{"done", "killed"},
		[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work", AdvancesTo: "done", Guard: &contracts.GuardSpec{ID: "check", Check: "false", OnFail: "kill"}}}, nil, nil)
	bundle := &contracts.WorkflowContractBundle{RootSchema: &contracts.FlowSchemaDocument{StageDeclarations: contracts.FlowStageDeclarations{Declared: true}},
		Semantics: contracts.WorkflowSemanticView{InitialStage: "ready", StageTopologies: map[string]contracts.WorkflowStageTopology{".": graph}}}
	view, err := Build(context.Background(), semanticviewtest.WrapRootAgents(bundle), BuildOptions{IncludeStageGraph: true})
	if err != nil || len(view.StageGraphs) != 1 {
		t.Fatalf("graph build: %#v %v", view, err)
	}
	got := view.StageGraphs[0]
	if len(got.Edges) != 1 || got.Edges[0].To != "done" || len(got.GuardTerminations) != 1 || got.GuardTerminations[0].To != "killed" || got.GuardTerminations[0].GuardID != "check" {
		t.Fatalf("guard possibility confused with authored carrier: %#v", got)
	}
	raw, err := json.Marshal(view)
	if err != nil || !strings.Contains(string(raw), `"guard_terminations"`) {
		t.Fatalf("supported JSON readback lost distinct cause: %s %v", raw, err)
	}
}
