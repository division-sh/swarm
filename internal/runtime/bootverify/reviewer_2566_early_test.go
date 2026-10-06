package bootverify

import (
	"context"
	"testing"
)

func TestReviewer2566LoopStartFromFinalIsRejectedBeforeRuntime(t *testing.T) {
	bundle := loopValidationBundle()
	if findings := loopValidationFindings(bundle); len(findings) != 0 {
		t.Fatalf("control: %+v", findings)
	}
	for i := range bundle.RootSchema.StageDeclarations.Entries {
		if bundle.RootSchema.StageDeclarations.Entries[i].ID == "research" {
			bundle.RootSchema.StageDeclarations.Entries[i].Final = true
		}
	}
	refreshLoopValidationTopology(bundle)
	c := newCheckerContext(context.Background(), compileBootverifySchemasPreservingPlans(bundle), Options{})
	findings := append(c.stateMachineCoherence(), checkLoopValidation(c)...)
	t.Logf("findings: %+v", findings)
	graph := bundle.Semantics.StageTopologies["."]
	for _, edge := range graph.Edges {
		if edge.From == "research" && edge.To == "drafting" {
			_, err := graph.AdmitTransition(edge.Site(), edge.From, edge.To)
			t.Logf("compiled edge %s -> %s; runtime admission: %v", edge.From, edge.To, err)
		}
	}
	if len(findings) == 0 {
		t.Fatal("loop start exits final research but both coherence and loop validation accept it")
	}
}
