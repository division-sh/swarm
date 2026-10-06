package authoringview

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestStageGraphPreservesEffectiveGuardIdentity(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	for _, tc := range []struct {
		name  string
		guard contracts.GuardSpec
		want  []string
	}{
		{"unnamed", contracts.GuardSpec{Check: " payload.score >= 70 "}, []string{"payload.score >= 70"}},
		{"unnamed chain", contracts.GuardSpec{Checks: []contracts.GuardCheck{{Check: "true"}, {Check: "false"}}}, []string{"false", "true"}},
		{"mixed chain", contracts.GuardSpec{Checks: []contracts.GuardCheck{{}, {ID: " first ", Check: "true"}, {Check: "false"}}}, []string{"false", "first"}},
		{"named registry", contracts.GuardSpec{ID: " registered "}, []string{"registered"}},
		{"on fail only", contracts.GuardSpec{}, nil},
		{"empty check", contracts.GuardSpec{Checks: []contracts.GuardCheck{{}}}, nil},
		{"policy only", contracts.GuardSpec{PolicyRef: "threshold"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.guard.OnFail = "kill"
			graph := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "killed"}, []string{"killed"}, []contracts.HandlerTransitionSemantic{{Node: node, EventType: "work", Guard: &tc.guard}}, nil, nil)
			bundle := &contracts.WorkflowContractBundle{RootSchema: &contracts.FlowSchemaDocument{StageDeclarations: contracts.FlowStageDeclarations{Declared: true, Entries: []contracts.FlowStageDeclaration{{ID: "ready"}, {ID: "killed", Final: true}}}}, Semantics: contracts.WorkflowSemanticView{StageTopologies: map[string]contracts.WorkflowStageTopology{".": graph}}}
			view, err := Build(context.Background(), semanticviewtest.WrapRootAgents(bundle), BuildOptions{IncludeStageGraph: true})
			if err != nil || len(view.StageGraphs) != 1 {
				t.Fatalf("readback failed: %v", err)
			}
			var ids []string
			for _, possibility := range view.StageGraphs[0].GuardTerminations {
				ids = append(ids, possibility.GuardID)
			}
			if !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("readback identities=%v, want %v", ids, tc.want)
			}
		})
	}
}

func TestStageGraphDistinguishesGuardPossibilitiesFromAuthoredEdges(t *testing.T) {
	node := identitytest.RootNode(t, "router")
	graph := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "done", "killed"}, []string{"done", "killed"},
		[]contracts.HandlerTransitionSemantic{{Node: node, EventType: "work", AdvancesTo: "done", Guard: &contracts.GuardSpec{ID: "check", Check: "false", OnFail: "kill"}}}, nil, nil)
	bundle := &contracts.WorkflowContractBundle{RootSchema: &contracts.FlowSchemaDocument{StageDeclarations: contracts.FlowStageDeclarations{Declared: true}},
		Semantics: contracts.WorkflowSemanticView{StageTopologies: map[string]contracts.WorkflowStageTopology{".": graph}}}
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
