package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func q10SharedCoordinatorCurrentSource(row recipe) (string, bool, error) {
	if row.File != "internal/runtime/final_flow_instance_authoring_runtime_test.go" || row.Function != "TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance" {
		return "", false, nil
	}
	const original = "\t\tSELECT COUNT(*) FROM entity_state\n\t\tWHERE flow_instance LIKE 'account/%'\n\t\t  AND current_state = 'reviewed'\n\t\t  AND fields @> $1::jsonb\n"
	const current = "\t\tSELECT COUNT(*) FROM entity_state es JOIN flow_instances fi\n\t\t  ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id\n\t\tWHERE es.flow_instance LIKE 'account/%'\n\t\t  AND fi.current_state = 'reviewed'\n\t\t  AND es.fields @> $1::jsonb\n"
	if row.Family != "native-shared-coordinator-authority" || strings.Count(row.Successor, original) != 1 {
		return "", true, fmt.Errorf("missing exact original account stage/field readback")
	}
	return strings.Replace(row.Successor, original, current, 1), true, nil
}

func TestQ10SharedCoordinatorReadbackRetainsExactIdentityAndLifecycle(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		current, found, err := q10SharedCoordinatorCurrentSource(row)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			continue
		}
		matched++
		guard := recipe{File: row.File, Function: row.Function, Before: current, After: current}
		for _, edit := range [][2]string{
			{"fi.run_id=es.run_id AND ", ""}, {"fi.instance_path=es.flow_instance AND ", ""},
			{"fi.entity_id=es.entity_id", "fi.entity_id=fi.entity_id"},
			{"fi.current_state = 'reviewed'", "es.current_state = 'reviewed'"},
			{"es.fields @> $1::jsonb", "true"}, {"es.flow_instance LIKE 'account/%'", "true"},
			{"`, 1, `", "`, 0, `"}, {"loaded.CurrentState != \"reviewed\"", "false"},
			{"loaded.Fields[\"score\"] != \"91\"", "false"},
		} {
			mutant := strings.Replace(current, edit[0], edit[1], 1)
			if mutant == current {
				t.Fatalf("missing exact account readback control: %q", edit[0])
			}
			if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
				t.Fatalf("account readback lost identity, stage, fields or cardinality: %q", edit[0])
			}
		}
		if _, changed, err := rewriteFunction(row.File, []byte("package proof\n"+row.Successor), guard); err == nil || changed {
			t.Fatal("retired data-row stage readback accepted as current")
		}
	}
	if matched != 1 {
		t.Fatalf("Q10 coordinator readbacks=%d, want1", matched)
	}
}
