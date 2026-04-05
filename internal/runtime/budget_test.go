package runtime

import (
	"reflect"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestBudgetTracker_KeepsTerminalStatesInstanceOwned(t *testing.T) {
	trackerA := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"done"},
		},
	}))
	trackerB := NewBudgetTracker(nil, nil, nil, nil, nil, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			TerminalStages: []string{"closed"},
		},
	}))

	if got, want := trackerA.TerminalInstanceStates(), []string{"done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerA.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
	if got, want := trackerB.TerminalInstanceStates(), []string{"closed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("trackerB.TerminalInstanceStates() = %#v, want %#v", got, want)
	}
}
