package contracts

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"gopkg.in/yaml.v3"
)

func TestCompiledTransitionRootTopologyUsesExactSchema(t *testing.T) {
	decode := func(raw string) FlowSchemaDocument {
		t.Helper()
		var schema FlowSchemaDocument
		if err := yaml.Unmarshal([]byte(raw), &schema); err != nil {
			t.Fatal(err)
		}
		return schema
	}
	root := decode("stages: {queued: {initial: true}, shared: {}, done: {terminal: true}}")
	child := decode("stages: {queued: {initial: true}, shared: {terminal: true}, child_only: {}, killed: {terminal: true}}")
	for _, order := range [][]string{{"child", "outer/inner"}, {"outer/inner", "child"}} {
		bundle := &WorkflowContractBundle{RootSchema: &root, FlowSchemas: map[string]FlowSchemaDocument{}, Nodes: map[string]SystemNodeContract{
			"worker": {EventHandlers: map[string]SystemNodeEventHandler{"advance": {AdvancesTo: "done"}}},
		}}
		for _, flow := range order {
			bundle.FlowSchemas[flow] = child
		}
		if err := CompileWorkflowSemantics(bundle); err != nil {
			t.Fatal(err)
		}
		graph, ok := bundle.WorkflowStageTopology(".")
		if !ok || graph.InitialStage != "queued" || !reflect.DeepEqual(graph.Stages, []string{"done", "queued", "shared"}) || !reflect.DeepEqual(graph.TerminalStages, []string{"done"}) {
			t.Fatalf("root graph contaminated by child metadata: %#v", graph)
		}
		if graph.GuardTerminationTarget() != "" {
			t.Fatal("root borrowed child kill target")
		}
		site := WorkflowTransitionSite{Node: identitytest.RootNode(t, "worker"), HandlerEvent: "advance", AdvanceCarrier: HandlerAdvanceCarrierHandler}
		for _, from := range []string{"queued", "shared"} {
			if _, err := graph.AdmitTransition(site, from, "done"); err != nil {
				t.Fatalf("child terminal blocked legal root source %s: %v", from, err)
			}
		}
		for _, from := range []string{"child_only", "killed", "done"} {
			if _, err := graph.AdmitTransition(site, from, "done"); err == nil {
				t.Fatalf("root admitted foreign/terminal source %s", from)
			}
		}
		for _, flow := range order {
			childGraph, found := bundle.WorkflowStageTopology(flow)
			if !found || !reflect.DeepEqual(childGraph.Stages, []string{"child_only", "killed", "queued", "shared"}) || !reflect.DeepEqual(childGraph.TerminalStages, []string{"killed", "shared"}) {
				t.Fatalf("lost scoped child metadata: %#v", childGraph)
			}
		}
		if len(bundle.WorkflowStages()) != 11 {
			t.Fatalf("phase metadata collapsed duplicate stage names: %#v", bundle.WorkflowStages())
		}
	}
}

func TestCompiledTransitionRootTopologyNeverUsesAggregateFallback(t *testing.T) {
	for _, root := range []*FlowSchemaDocument{nil, {StageDeclarations: FlowStageDeclarations{Declared: true}}} {
		semantics := WorkflowSemanticView{
			InitialStage:   "root-descriptor-initial",
			Stages:         []WorkflowStageContract{{ID: "foreign", Phase: "child"}},
			TerminalStages: []string{"foreign"},
			FlowStates:     map[string][]string{".": {"foreign"}, "child": {"foreign"}},
			FlowInitial:    map[string]string{".": "foreign", "child": "foreign"},
			FlowTerminal:   map[string][]string{".": {"foreign"}, "child": {"foreign"}},
		}
		graph := deriveWorkflowStageTopologies(root, semantics)["."]
		bundle := &WorkflowContractBundle{RootSchema: root, Semantics: semantics}
		if len(bundle.FlowStates(".")) != 0 || len(bundle.FlowTerminalStages(".")) != 0 {
			t.Fatal("root accessor rescued membership from aggregate metadata")
		}
		if bundle.FlowInitialStage(".") != semantics.InitialStage {
			t.Fatal("root descriptor initial-stage accessor was lost")
		}
		if len(graph.Stages) != 0 || len(graph.TerminalStages) != 0 || len(graph.Edges) != 0 {
			t.Fatalf("root rescued from aggregate or scoped-map override: %#v", graph)
		}
		if graph.InitialStage != semantics.InitialStage {
			t.Fatalf("root descriptor initial stage dropped: %#v", graph)
		}
	}
}
