package contracts_test

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestStageReferenceHasNoExternalAuthorityFields(t *testing.T) {
	graph := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	ready, err := graph.ResolveStage("ready")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < reflect.TypeOf(ready).NumField(); i++ {
		field := reflect.TypeOf(ready).Field(i)
		if field.IsExported() {
			t.Fatalf("stage reference exposes writable authority field %q", field.Name)
		}
	}
	graph.InitialStage = "Ready"
	graph.Stages[0] = "unknown"
	graph.TerminalStages[0] = "ready"
	if err := graph.RequireStage(ready); err != nil || ready.IsTerminal() {
		t.Fatalf("public projection changed sealed stage reference: ref=%#v err=%v", ready, err)
	}
	initial, err := graph.InitialStageRef()
	if err != nil || initial.ID() != "ready" {
		t.Fatalf("public projection changed compiled initial: ref=%#v err=%v", initial, err)
	}
	other := contracts.BuildWorkflowStageTopology(".", "ready", []string{"ready", "Ready"}, []string{"Ready"}, nil, nil, nil)
	if err := other.RequireStage(ready); err == nil {
		t.Fatal("another selected source accepted a foreign stage reference")
	}
	relabelled := graph
	relabelled.FlowID = "child"
	if err := relabelled.RequireStage(ready); err == nil {
		t.Fatal("relabelled topology accepted a reference from its original flow")
	}
}
