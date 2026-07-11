package runstart

import (
	"reflect"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDeriveRootInputSetRequiresDeclaredAndRoutableRootInput(t *testing.T) {
	bundle := rootInputTestBundle("scan.corpus_file_requested")
	set, err := DeriveRootInputSet(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("DeriveRootInputSet: %v", err)
	}
	if got, want := set.Declared, []string{"scan.corpus_file_requested"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("declared = %#v, want %#v", got, want)
	}
	if got, want := set.Routable, []string{"scan.corpus_file_requested"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("routable = %#v, want %#v", got, want)
	}
	_, err = ValidateInputEvents(semanticview.Wrap(bundle), []string{"scan.requested"})
	diagnostic, ok := AsRootInputValidationError(err)
	if !ok {
		t.Fatal("expected retired undeclared root input to fail")
	}
	if diagnostic.EventName != "scan.requested" || diagnostic.Reason != RootInputNotDeclared {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
	if !reflect.DeepEqual(diagnostic.Inputs.Declared, []string{"scan.corpus_file_requested"}) ||
		!reflect.DeepEqual(diagnostic.Inputs.Routable, []string{"scan.corpus_file_requested"}) {
		t.Fatalf("diagnostic inputs = %#v", diagnostic.Inputs)
	}
}

func TestValidateInputEventsRejectsDeclaredUnroutableRootInput(t *testing.T) {
	const eventName = "scan.unroutable_requested"
	bundle := rootInputTestBundle(eventName)
	bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"] = runtimecontracts.SystemNodeContract{
		ID:           "scan-orchestrator",
		SubscribesTo: []string{"scan.other_requested"},
	}
	bundle.Nodes["scan-orchestrator"] = bundle.FlowTree.Root.Children[0].Nodes["scan-orchestrator"]

	_, err := ValidateInputEvents(semanticview.Wrap(bundle), []string{eventName})
	diagnostic, ok := AsRootInputValidationError(err)
	if !ok {
		t.Fatal("expected declared but unroutable root input to fail")
	}
	if diagnostic.EventName != eventName || diagnostic.Reason != RootInputNotRoutable {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
	if !reflect.DeepEqual(diagnostic.Inputs.Declared, []string{eventName}) || len(diagnostic.Inputs.Routable) != 0 {
		t.Fatalf("diagnostic inputs = %#v", diagnostic.Inputs)
	}
	if !strings.Contains(diagnostic.Error(), "routable root inputs: none") {
		t.Fatalf("diagnostic error = %q", diagnostic.Error())
	}
}

func TestRootInputValidationErrorOwnsNormalizedSnapshot(t *testing.T) {
	inputs := RootInputSet{
		Declared: []string{"z.event", "a.event", "z.event"},
		Routable: []string{"z.event"},
	}
	diagnostic := newRootInputValidationError(" missing.event ", RootInputNotDeclared, inputs)
	inputs.Declared[0] = "changed.event"
	inputs.Routable[0] = "changed.event"

	if diagnostic.EventName != "missing.event" {
		t.Fatalf("event name = %q", diagnostic.EventName)
	}
	if !reflect.DeepEqual(diagnostic.Inputs.Declared, []string{"a.event", "z.event"}) ||
		!reflect.DeepEqual(diagnostic.Inputs.Routable, []string{"z.event"}) {
		t.Fatalf("diagnostic inputs = %#v", diagnostic.Inputs)
	}
	copy, ok := AsRootInputValidationError(diagnostic)
	if !ok {
		t.Fatal("AsRootInputValidationError did not recognize typed error")
	}
	copy.Inputs.Declared[0] = "copy.changed"
	if diagnostic.Inputs.Declared[0] != "a.event" {
		t.Fatalf("typed extraction aliased original inputs: %#v", diagnostic.Inputs)
	}
}

func TestValidateInputEventsTreatsAbsentRootSchemaAsEmptyDomain(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	set, err := ValidateInputEvents(semanticview.Wrap(bundle), []string{"flow/local.event"})
	diagnostic, ok := AsRootInputValidationError(err)
	if !ok {
		t.Fatalf("ValidateInputEvents error = %v, want typed root-input rejection", err)
	}
	if diagnostic.EventName != "flow/local.event" || diagnostic.Reason != RootInputNotDeclared {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
	if set.Declared == nil || set.Routable == nil || len(set.Declared) != 0 || len(set.Routable) != 0 {
		t.Fatalf("root-input set = %#v, want explicit empty domains", set)
	}
	if diagnostic.Inputs.Declared == nil || diagnostic.Inputs.Routable == nil {
		t.Fatalf("diagnostic inputs = %#v, want explicit empty domains", diagnostic.Inputs)
	}
}

func rootInputTestBundle(eventName string) *runtimecontracts.WorkflowContractBundle {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "discovery", Flow: "discovery"},
		Path:  "discovery",
		Schema: runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID:           "scan-orchestrator",
				SubscribesTo: []string{eventName},
			},
		},
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}}
	return &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": flow.Nodes["scan-orchestrator"],
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{
				Inputs: runtimecontracts.FlowInputPins{Events: []string{eventName}},
			},
		},
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"discovery": &root.Children[0],
			},
		},
	}
}
