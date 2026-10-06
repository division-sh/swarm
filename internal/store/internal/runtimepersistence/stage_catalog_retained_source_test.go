package runtimepersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func assertRetainedStageCatalogsForFork(t *testing.T, original, retained semanticview.Source) {
	t.Helper()
	bundle, ok := semanticview.Bundle(original)
	if !ok {
		t.Fatal("source fixture lost its admitted bundle")
	}
	for flow, expected := range bundle.Semantics.StageTopologies {
		actual, ok := semanticview.WorkflowStageTopology(retained, flow)
		if !ok || !actual.ValidStageCatalog() || !reflect.DeepEqual(expected.StageIDs(), actual.StageIDs()) || !reflect.DeepEqual(expected.FinalStageIDs(), actual.FinalStageIDs()) {
			t.Fatalf("retained fork changed ordered stage/final facts for %s: expected=%+v actual=%+v", flow, expected, actual)
		}
		before, err := expected.InitialStoredStage()
		if err != nil {
			t.Fatal(err)
		}
		after, err := actual.InitialStoredStage()
		if err != nil || before.ID() != after.ID() || before.IsFinal() != after.IsFinal() || before.IsStatelessPosture() != after.IsStatelessPosture() {
			t.Fatalf("retained fork changed entry for %s: %v", flow, err)
		}
	}
}
