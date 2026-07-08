package contracts

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func TestFlowStatesRootScopeExcludesChildFlowStates(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Package: ProjectPackageDocument{Name: "root-workflow", Version: "1.0.0"},
		RootSchema: &FlowSchemaDocument{
			InitialState: "root-new",
			States:       []string{"root-new", "root-done"},
		},
		Paths: ContractPaths{Flows: []FlowContractPaths{
			{ID: "child", Flow: "child"},
		}},
		FlowSchemas: map[string]FlowSchemaDocument{
			"child": {
				InitialState: "child-new",
				States:       []string{"child-new", "child-done"},
			},
		},
	}
	populateWorkflowSemantics(bundle)

	if got, want := bundle.FlowStates(""), []string{"root-new", "root-done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowStates(\"\") = %#v, want %#v", got, want)
	}
	if got, want := bundle.FlowStates(bundle.WorkflowName()), []string{"root-new", "root-done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowStates(workflow name) = %#v, want %#v", got, want)
	}
	if got, want := bundle.FlowStates("child"), []string{"child-new", "child-done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowStates(child) = %#v, want %#v", got, want)
	}
	if got := bundle.WorkflowStages(); len(got) != 4 {
		t.Fatalf("WorkflowStages length = %d, want aggregate root+child states", len(got))
	}
}

func TestAuthoredStagesLowerPerFlowScopedLifecycle(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Package: ProjectPackageDocument{Name: "root-workflow", Version: "1.0.0"},
		RootSchema: &FlowSchemaDocument{
			StageDeclarations: FlowStageDeclarations{
				Declared: true,
				Entries: []FlowStageDeclaration{
					{ID: "ready", Initial: true},
					{ID: "done", Terminal: true},
				},
			},
		},
		Paths: ContractPaths{Flows: []FlowContractPaths{
			{ID: "child", Flow: "child"},
		}},
		FlowSchemas: map[string]FlowSchemaDocument{
			"child": {
				StageDeclarations: FlowStageDeclarations{
					Declared: true,
					Entries: []FlowStageDeclaration{
						{ID: "ready", Initial: true},
						{ID: "done", Terminal: true},
					},
				},
			},
		},
	}
	populateWorkflowSemantics(bundle)

	if got, want := bundle.FlowInitialStage(""), "ready"; got != want {
		t.Fatalf("FlowInitialStage(root) = %q, want %q", got, want)
	}
	if got, want := bundle.FlowStates(""), []string{"ready", "done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowStates(root) = %#v, want %#v", got, want)
	}
	if got, want := bundle.FlowTerminalStages("child"), []string{"done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FlowTerminalStages(child) = %#v, want %#v", got, want)
	}
	stages := bundle.WorkflowStages()
	if len(stages) != 4 {
		t.Fatalf("WorkflowStages length = %d, want root+child scoped duplicates", len(stages))
	}
	var childReady bool
	for _, stage := range stages {
		if stage.ID == "ready" && stage.Phase == "child" {
			childReady = true
		}
	}
	if !childReady {
		t.Fatalf("WorkflowStages = %#v, want child-scoped ready stage", stages)
	}
}

func TestResolveFlowInputAutoWire_DoesNotInferSiblingProducerForUniquePinMatch(t *testing.T) {
	producer := FlowContractView{
		Paths: FlowContractPaths{ID: "producer", Flow: "producer"},
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Outputs: FlowOutputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "producer",
	}
	consumer := FlowContractView{
		Paths: FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Inputs: FlowInputPins{Events: []string{"scan.requested"}},
			},
		},
		Path: "consumer",
	}
	root := FlowContractView{Children: []FlowContractView{producer, consumer}}
	bundle := &WorkflowContractBundle{
		FlowTree: flowmodel.Tree[FlowContractView]{
			Root: &root,
			ByID: map[string]*FlowContractView{
				"producer": &root.Children[0],
				"consumer": &root.Children[1],
			},
		},
	}

	resolution := bundle.ResolveFlowInputAutoWire("consumer", "scan.requested")
	if len(resolution.ProducerFlows) != 0 {
		t.Fatalf("ProducerFlows = %#v, want none for retired sibling auto-wire", resolution.ProducerFlows)
	}
	if len(resolution.Patterns) != 0 {
		t.Fatalf("Patterns = %#v, want none for retired sibling auto-wire", resolution.Patterns)
	}
}

func TestResolveFlowInputAutoWire_DoesNotExposeAmbiguousSiblingProducerFallback(t *testing.T) {
	producerA := FlowContractView{
		Paths: FlowContractPaths{ID: "producer_a", Flow: "producer_a"},
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Outputs: FlowOutputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "producer_a",
	}
	producerB := FlowContractView{
		Paths: FlowContractPaths{ID: "producer_b", Flow: "producer_b"},
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Outputs: FlowOutputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "producer_b",
	}
	consumer := FlowContractView{
		Paths: FlowContractPaths{ID: "consumer", Flow: "consumer"},
		Schema: FlowSchemaDocument{
			Pins: FlowPins{
				Inputs: FlowInputPins{Events: []string{"ticket.ready"}},
			},
		},
		Path: "consumer",
	}
	root := FlowContractView{Children: []FlowContractView{producerA, producerB, consumer}}
	bundle := &WorkflowContractBundle{
		FlowTree: flowmodel.Tree[FlowContractView]{
			Root: &root,
			ByID: map[string]*FlowContractView{
				"producer_a": &root.Children[0],
				"producer_b": &root.Children[1],
				"consumer":   &root.Children[2],
			},
		},
	}

	resolution := bundle.ResolveFlowInputAutoWire("consumer", "ticket.ready")
	if len(resolution.ProducerFlows) != 0 {
		t.Fatalf("ProducerFlows = %#v, want none for retired sibling auto-wire", resolution.ProducerFlows)
	}
	if len(resolution.Patterns) != 0 {
		t.Fatalf("Patterns = %#v, want none for retired sibling auto-wire", resolution.Patterns)
	}
}
