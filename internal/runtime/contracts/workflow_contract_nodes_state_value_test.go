package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeStateAndGateSchemasFromSource(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("state_schema:\n  fields:\n    seen: {type: boolean, default: false}\ngate_state:\n  gates:\n    ready: {description: complete}\n"))
	if err != nil {
		t.Fatal(err)
	}
	root := snapshot.Document("nodes.yaml").Root()
	state, err := root.Lookup("state_schema")
	if err != nil {
		t.Fatal(err)
	}
	projectedState, err := projectNodeStateSchemaValue(state.Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(projectedState.Fields) != 1 || projectedState.Fields[0].Name != "seen" || projectedState.Fields[0].Type == "" {
		t.Fatalf("wrong node state: %#v", projectedState)
	}
	gate, err := root.Lookup("gate_state")
	if err != nil {
		t.Fatal(err)
	}
	projectedGate, err := projectNodeGateStateValue(gate.Value)
	if err != nil {
		t.Fatal(err)
	}
	if len(projectedGate.Gates) != 1 || projectedGate.Gates[0].Name != "ready" || projectedGate.Gates[0].Description != "complete" {
		t.Fatalf("wrong node gate state: %#v", projectedGate)
	}
}
