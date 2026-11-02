package contracts

import (
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func TestProjectNodeGateSchemaFromSource(t *testing.T) {
	snapshot, err := yamlsource.Load([]byte("gate_state:\n  gates:\n    ready: {description: complete}\n"))
	if err != nil {
		t.Fatal(err)
	}
	root := snapshot.Document("nodes.yaml").Root()
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
