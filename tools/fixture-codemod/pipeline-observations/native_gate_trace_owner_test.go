package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func originalGateTraceSource(source string) string {
	source = strings.Replace(source, "selected, reconstructed := storetest.StartSQLiteRuntimeStorePair(t)", "selected := storetest.StartSQLiteRuntimeStore(t)", 1)
	source = strings.Replace(source, "\treconstructed := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
	source = strings.Replace(source, "trace: reconstructed", "trace: selected", 1)
	return strings.Replace(source, "\treturn result\n", "\tif result.trace != selected {\n\t\tt.Fatal(\"gate trace must retain its exact original selected owner\")\n\t}\n\treturn result\n", 1)
}

func TestGateTraceFactoriesRetainOnlyOriginalSelectedReader(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-gate-original-trace-owner" {
			continue
		}
		matched++
		want, err := canonicalFunction(originalGateTraceSource(row.Before))
		got, afterErr := canonicalFunction(row.After)
		actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || afterErr != nil || actualErr != nil || want != got || got != actual {
			t.Fatalf("gate source, role, construction or cleanup changed: %s", row.Function)
		}
		for _, cut := range []string{"trace: selected", "result.trace != selected", "persistence: persistence"} {
			mutant, mutantErr := canonicalFunction(strings.Replace(row.After, cut, "unreviewed", 1))
			if mutantErr == nil && mutant == want {
				t.Fatalf("gate trace reconstruction/role substitution admitted: %s", cut)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("gate trace factories=%d,want2", matched)
	}
}
