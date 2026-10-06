package contracts

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFinalStageAdmissionRetiresMarkerPresence(t *testing.T) {
	for _, key := range []string{"initial", "terminal"} {
		for _, value := range []string{"true", "false", "null", "''", "[]", "{}", "'true'", "&flag true"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := admitSchemaFragment(fmt.Sprintf("stages:\n  waiting: {%s: %s}\n", key, value))
				if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "Valid fields:") || !strings.Contains(err.Error(), "final") {
					t.Fatalf("retired key was not rejected with canonical vocabulary: %v", err)
				}
			})
		}
	}
	for _, source := range []string{
		"stages: {waiting: &flag {initial: true}, other: *flag}",
		"stages:\n  waiting: {<<: &flag {terminal: false}}",
	} {
		if _, err := admitSchemaFragment(source); err == nil || !strings.Contains(err.Error(), "Valid fields:") {
			t.Fatalf("aliased/merged retired field admitted: %v", err)
		}
	}
}

func TestFinalStageBooleanAndOrderedEntry(t *testing.T) {
	for _, source := range []string{
		"stages: {waiting: {}, done: {final: true}}",
		"stages: &declarations {waiting: {}, done: {final: true}}",
		"stages:\n  waiting: {}\n  <<: &remaining {done: {final: true}}",
	} {
		doc, err := admitSchemaFragment(source)
		if err != nil || doc.LoweredInitialState() != "waiting" || !reflect.DeepEqual(doc.LoweredFinalStates(), []string{"done"}) {
			t.Fatalf("ordered declaration entry/final choice changed: %#v, %v", doc, err)
		}
	}
	for _, value := range []string{"null", "''", "[]", "{}", "'true'", "1", "1.5"} {
		if _, err := admitSchemaFragment("stages: {waiting: {final: " + value + "}}"); err == nil {
			t.Fatalf("non-boolean final %s admitted", value)
		}
	}
	for _, source := range []string{"stages: {waiting: {}}", "stages: {waiting: {final: false}}"} {
		doc, err := admitSchemaFragment(source)
		if err != nil || doc.LoweredInitialState() != "waiting" || len(doc.LoweredFinalStates()) != 0 {
			t.Fatalf("optional no-final service declaration rejected: %v", err)
		}
	}
	for _, row := range []string{"emit: ping", "advances_to: done", "emit: ping, advances_to: done"} {
		_, err := admitSchemaFragment("stages: {done: {final: true, timers: [{after: 1s, " + row + "}]}}")
		if err == nil || !strings.Contains(err.Error(), "cannot own executable timers") {
			t.Fatalf("final stage timer admitted (%s): %v", row, err)
		}
	}
}

func TestFinalStageCatalogOwnsEntryAndEndReadout(t *testing.T) {
	root, err := admitSchemaFragment("stages: {registered: {}, cooling: {}, Done: {final: true}}")
	if err != nil {
		t.Fatal(err)
	}
	child, err := admitSchemaFragment("stages: {registered: {}, Done: {final: false}}")
	if err != nil {
		t.Fatal(err)
	}
	bundle := &WorkflowContractBundle{RootSchema: &root, FlowSchemas: map[string]FlowSchemaDocument{"child": child}}
	if err := CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	graph, ok := bundle.WorkflowStageTopology(".")
	if !ok {
		t.Fatal("missing canonical root")
	}
	entry, err := graph.InitialStoredStage()
	if err != nil || entry.ID() != "registered" || entry.IsFinal() {
		t.Fatalf("entry followed sorted IDs instead of declaration order: %#v, %v", entry, err)
	}
	root.StageDeclarations.Entries[0].ID = "foreign"
	root.StageDeclarations.Entries[2].Final = false
	child.StageDeclarations.Entries[0].ID = "foreign"
	bundle.FlowSchemas["child"] = child
	graph.InitialStage = "cooling"
	graph.FinalStages[0] = "registered"
	graph.Stages[0] = "foreign"
	if bundle.FlowInitialStage(".") != "registered" || bundle.FlowInitialStage("child") != "registered" || !reflect.DeepEqual(bundle.FlowFinalStages("."), []string{"Done"}) || len(bundle.FlowFinalStages("child")) != 0 {
		t.Fatal("mutable declaration/projection changed canonical entry or final readout")
	}
	if !reflect.DeepEqual(bundle.FlowStates("."), []string{"registered", "cooling", "Done"}) {
		t.Fatal("catalog lost effective source declaration order")
	}
	for _, unknown := range []string{"done", "DONE", "pending", "foreign"} {
		if _, err := graph.ResolveStoredStage(unknown); err == nil {
			t.Fatalf("unknown/case-folded stored stage %q admitted", unknown)
		}
	}
	if len(bundle.FlowStates("")) != 0 || len(bundle.FlowFinalStages("")) != 0 || bundle.FlowInitialStage("") != "" {
		t.Fatal("retired empty root alias restored")
	}
}
